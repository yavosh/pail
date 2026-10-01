// Package diff checks pail against AWS S3 as a black box. Record mode sends
// each scenario to AWS and writes golden files; replay mode, the default, sends
// the same requests to an in-process pail and compares. See README.md here.
package diff

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/server"
)

var record = flag.Bool("record", false, "record golden files against AWS S3 in us-east-1")

const (
	goldenDir      = "testdata/golden"
	knownDiffsFile = "testdata/known-diffs.txt"
	region         = "us-east-1"
)

func TestDiff(t *testing.T) {
	var tg *target
	if *record {
		tg = awsTarget(t)
	} else {
		tg = pailTarget(t)
	}

	f, err := os.Open(knownDiffsFile)
	if err != nil {
		t.Fatal(err)
	}
	known, err := readKnownDiffs(f)
	f.Close()
	if err != nil {
		t.Fatalf("%s: %v", knownDiffsFile, err)
	}

	var mu sync.Mutex
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
			mu.Lock()
			ran[sc.name] = true
			mu.Unlock()
			for _, key := range replayScenario(t, tg, sc, want, known) {
				mu.Lock()
				seen[key] = true
				mu.Unlock()
			}
		})
	}

	if *record {
		return
	}
	// A listed difference that no longer occurs fails, so the list stays current.
	for _, key := range slices.Sorted(func(yield func(string) bool) {
		for k := range known {
			if !yield(k) {
				return
			}
		}
	}) {
		scenarioName, _, _ := strings.Cut(key, "/")
		if ran[scenarioName] && !seen[key] {
			t.Errorf("%s lists %q, but that difference no longer occurs: remove the line", knownDiffsFile, key)
		}
	}
}

// recordScenario runs sc against AWS and writes its golden file.
func recordScenario(t *testing.T, tg *target, sc scenario, path string) {
	bucket := newBucketName()
	t.Cleanup(func() { cleanupBucket(t, tg, bucket) })
	g := runScenario(t, tg, sc, bucket)
	if err := os.MkdirAll(goldenDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeGolden(path, g); err != nil {
		t.Fatal(err)
	}
	t.Logf("recorded %s", path)
}

// runScenario sends every step of sc to tg and returns the normalized results.
func runScenario(t *testing.T, tg *target, sc scenario, bucket string) golden {
	t.Helper()
	g := golden{Scenario: sc.name}
	for _, st := range sc.steps {
		resp, err := tg.do(t.Context(), st, bucket)
		if err != nil {
			t.Fatalf("step %s (%s): %v", st.name, describe(st), err)
		}
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
			want := runScenario(t, tg, sc, newBucketName())
			if seen := replayScenario(t, tg, sc, want, nil); len(seen) != 0 {
				t.Errorf("known differences seen = %v, want none", seen)
			}
		})
	}
}

// replayScenario runs sc against pail and reports every difference not listed
// in known. It returns the known-difference keys it saw.
func replayScenario(t *testing.T, tg *target, sc scenario, want golden, known map[string]string) []string {
	if len(want.Exchanges) != len(sc.steps) {
		t.Fatalf("golden file has %d steps, scenario has %d: record it again", len(want.Exchanges), len(sc.steps))
	}
	bucket := newBucketName()
	var seen []string
	for i, st := range sc.steps {
		w := want.Exchanges[i]
		if w.Step != st.name || w.Request != describe(st) {
			t.Fatalf("step %d is %q (%s) in the golden file and %q (%s) in the scenario: record it again", i, w.Step, w.Request, st.name, describe(st))
		}
		resp, err := tg.do(t.Context(), st, bucket)
		if err != nil {
			t.Fatalf("step %s (%s): %v", st.name, describe(st), err)
		}
		got := normalize(st, bucket, resp)
		diffs := compare(w, got)
		for _, key := range slices.Sorted(func(yield func(string) bool) {
			for k := range diffs {
				if !yield(k) {
					return
				}
			}
		}) {
			full := sc.name + "/" + key
			if _, ok := known[full]; ok {
				seen = append(seen, full)
				continue
			}
			t.Errorf("%s: %s\n%s", full, w.Request, diffs[key])
		}
	}
	return seen
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
	return &target{
		name:   "aws",
		region: region,
		creds:  creds,
		client: &http.Client{Timeout: 30 * time.Second},
		scheme: "https",
		host:   func(bucket string) string { return bucket + ".s3." + region + ".amazonaws.com" },
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
		name:   "pail",
		region: region,
		creds:  aws.Credentials{AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey},
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, addr)
			}},
		},
		scheme: "http",
		host:   func(bucket string) string { return bucket + ".localhost:" + port },
	}
}
