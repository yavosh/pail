// Package diff checks pail against AWS S3, SQS, and SNS as a black box. Record
// mode writes golden files from AWS; replay mode, the default, sends the same
// requests to an in-process pail and compares. See README.md here.
package diff

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/server"
)

var record = flag.Bool("record", false, "record golden files against AWS S3, SQS, and SNS in us-east-1")

const (
	goldenDir      = "testdata/golden"
	knownDiffsFile = "testdata/known-diffs.txt"
	pendingFile    = "testdata/pending.txt"
	region         = "us-east-1"
)

func TestDiff(t *testing.T) {
	var tg *target
	if *record {
		tg = awsTarget(t)
	} else {
		tg = pailTarget(t)
	}

	known := readListFile(t, knownDiffsFile, readKnownDiffs)
	pending := readListFile(t, pendingFile, readPending)

	names := map[string]bool{}
	steps := map[string]bool{}
	for _, sc := range scenarios() {
		names[sc.name] = true
		for _, st := range sc.steps {
			steps[sc.name+"/"+st.name] = true
		}
	}
	for _, key := range slices.Sorted(maps.Keys(pending)) {
		if !steps[key] {
			t.Errorf("%s lists %q, but no scenario has that step", pendingFile, key)
		}
	}
	seen := map[string]bool{}
	ran := map[string]bool{}
	for _, sc := range scenarios() {
		t.Run(sc.name, func(t *testing.T) {
			path := filepath.Join(goldenDir, sc.name+".json")
			if *record {
				recordScenario(t, tg, sc, path)
				return
			}
			want, err := readGolden(path)
			if os.IsNotExist(err) {
				t.Skipf("no golden file %s: record it with go test ./test/diff -record", path)
			}
			if err != nil {
				t.Fatal(err)
			}
			// ran is set only after replay returns, so a step that fails fatally
			// does not also report its listed differences as stale.
			unexpected, got := replayScenario(t, tg, sc, want, known, pending)
			ran[sc.name] = true
			for _, key := range got {
				seen[key] = true
			}
			for _, d := range unexpected {
				t.Error(d)
			}
		})
	}

	if *record {
		return
	}
	// A pending step that matches AWS fails, so the list shrinks as pail grows.
	for _, key := range slices.Sorted(maps.Keys(pending)) {
		scenarioName, _, _ := strings.Cut(key, "/")
		if steps[key] && ran[scenarioName] && !seen[key] {
			t.Errorf("%s lists %q, but that step has no unlisted difference now: remove the line", pendingFile, key)
		}
	}
	// A listed difference that no longer occurs fails, so the list stays current.
	for _, key := range slices.Sorted(maps.Keys(known)) {
		scenarioName, _, _ := strings.Cut(key, "/")
		if !names[scenarioName] {
			t.Errorf("%s lists %q, but no scenario is named %q", knownDiffsFile, key, scenarioName)
		}
		if ran[scenarioName] && !seen[key] {
			t.Errorf("%s lists %q, but that difference no longer occurs: remove the line", knownDiffsFile, key)
		}
	}
}

// recordScenario runs sc against AWS and writes its golden file.
func recordScenario(t *testing.T, tg *target, sc scenario, path string) {
	bucket := newBucketName()
	if slices.ContainsFunc(sc.steps, func(st step) bool { return st.service == "" }) {
		t.Cleanup(func() { cleanupBucket(t, tg, bucket) })
	}
	for _, q := range sc.queues {
		name := strings.ReplaceAll(q, "{name}", bucket)
		t.Cleanup(func() { cleanupQueue(t, tg, name) })
	}
	vars := map[string]string{"name": bucket}
	for _, topic := range sc.topics {
		name := strings.ReplaceAll(topic, "{name}", bucket)
		t.Cleanup(func() { cleanupTopic(t, tg, name, vars["topicArn"]) })
	}
	g := runScenario(t, tg, sc, bucket, vars)
	if err := os.MkdirAll(goldenDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeGolden(path, g); err != nil {
		t.Fatal(err)
	}
	t.Logf("recorded %s", path)
}

// runScenario sends every step of sc to tg and returns the normalized results.
func runScenario(t *testing.T, tg *target, sc scenario, bucket string, vars map[string]string) golden {
	t.Helper()
	g := golden{Scenario: sc.name}
	prev := "no earlier step ran"
	for _, st := range sc.steps {
		expanded, missing := st.withVars(vars)
		// Recording an error golden for a step that never had its input is wrong.
		if *record && st.service != "" && len(missing) > 0 {
			t.Fatalf("step %s uses {%s}, which no earlier response set (%s): record again", st.name, missing[0], prev)
		}
		resp, err := tg.do(t.Context(), expanded, bucket)
		if err != nil {
			t.Fatalf("step %s (%s): %v", st.name, describe(st), err)
		}
		captureVars(vars, resp.body)
		prev = st.name + " answered " + failure(resp, nil)
		g.Exchanges = append(g.Exchanges, normalize(st, bucket, resp))
	}
	return g
}

// TestReplayIsDeterministic records every scenario against pail and replays it
// against pail with another bucket. Any difference means normalization missed
// a value that changes between runs.
func TestReplayIsDeterministic(t *testing.T) {
	if *record {
		t.Skip("record mode")
	}
	tg := pailTarget(t)
	for _, sc := range scenarios() {
		t.Run(sc.name, func(t *testing.T) {
			name := newBucketName()
			want := runScenario(t, tg, sc, name, map[string]string{"name": name})
			unexpected, seen := replayScenario(t, tg, sc, want, nil, nil)
			for _, d := range unexpected {
				t.Error(d)
			}
			if len(seen) != 0 {
				t.Errorf("known differences seen = %v, want none", seen)
			}
		})
	}
}

// replayScenario runs sc against pail. It returns every difference not listed
// in known or pending, and the known and pending keys it saw.
func replayScenario(t *testing.T, tg *target, sc scenario, want golden, known, pending map[string]string) (unexpected, seen []string) {
	if len(want.Exchanges) != len(sc.steps) {
		t.Fatalf("golden file has %d steps, scenario has %d: record it again", len(want.Exchanges), len(sc.steps))
	}
	bucket := newBucketName()
	vars := map[string]string{"name": bucket}
	for i, st := range sc.steps {
		w := want.Exchanges[i]
		if w.Step != st.name || w.Request != describe(st) || w.Fingerprint != fingerprint(st) {
			t.Fatalf("step %d %q (%s) changed since the golden file was recorded: record it again", i, st.name, describe(st))
		}
		expanded, _ := st.withVars(vars)
		resp, err := tg.do(t.Context(), expanded, bucket)
		if err != nil {
			// Earlier differences often explain a later failure, so keep them.
			msg := fmt.Sprintf("step %s (%s): %v", st.name, describe(st), err)
			if len(unexpected) > 0 {
				msg += "\nearlier differences:\n" + strings.Join(unexpected, "\n")
			}
			t.Fatal(msg)
		}
		captureVars(vars, resp.body)
		got := normalize(st, bucket, resp)
		diffs := compare(w, got)
		stepKey := sc.name + "/" + st.name
		for _, key := range slices.Sorted(maps.Keys(diffs)) {
			full := sc.name + "/" + key
			if _, ok := known[full]; ok {
				seen = append(seen, full)
				continue
			}
			if reason, ok := pending[stepKey]; ok {
				seen = append(seen, stepKey)
				t.Logf("%s: pending, %s\n%s", full, reason, diffs[key])
				continue
			}
			unexpected = append(unexpected, fmt.Sprintf("%s: %s\n%s", full, w.Request, diffs[key]))
		}
	}
	return unexpected, seen
}

// readListFile opens and parses one of the testdata lists.
func readListFile(t *testing.T, path string, parse func(io.Reader) (map[string]string, error)) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	m, err := parse(f)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return m
}

func newBucketName() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "pail-diff-" + hex.EncodeToString(b)
}

// awsTarget reads credentials from the environment. For a profile or SSO, run
// eval "$(aws configure export-credentials --format env)" first.
func awsTarget(t *testing.T) *target {
	t.Helper()
	creds := aws.Credentials{
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		t.Fatal(`record mode needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY; for a profile, run eval "$(aws configure export-credentials --format env)"`)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	return &target{
		region:      region,
		creds:       creds,
		client:      &http.Client{Timeout: 30 * time.Second, Transport: transport, CheckRedirect: noRedirect},
		scheme:      "https",
		host:        func(bucket string) string { return bucket + ".s3." + region + ".amazonaws.com" },
		serviceHost: func(service string) string { return service + "." + region + ".amazonaws.com" },
	}
}

// pailTarget starts pail in-process and sends every request to it, whatever
// the virtual-hosted host name says.
func pailTarget(t *testing.T) *target {
	t.Helper()
	cfg := config.Config{
		Addr:            "127.0.0.1:0",
		DataDir:         t.TempDir(),
		AccessKeyID:     "AKIAPAILDIFFTEST0000",
		SecretAccessKey: "pail-diff-secret",
		Region:          region,
		Domain:          "localhost",
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv := server.New(cfg)
	if err := srv.Listen(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("pail shutdown: %v", err)
		}
	})

	addr := srv.Addr().String()
	_, port, _ := net.SplitHostPort(addr)
	dialer := &net.Dialer{}
	return &target{
		region: region,
		creds:  aws.Credentials{AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey},
		client: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: noRedirect,
			Transport: &http.Transport{
				DisableCompression: true,
				DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					return dialer.DialContext(ctx, network, addr)
				},
			},
		},
		scheme:      "http",
		host:        func(bucket string) string { return bucket + ".localhost:" + port },
		serviceHost: func(string) string { return "localhost:" + port },
	}
}

// noRedirect returns a redirect as the response. Following it would resend a
// request signed for another host and hide what the server answered.
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
