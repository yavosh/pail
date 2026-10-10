package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/topic"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

func TestNotifierExists(t *testing.T) {
	ctx := context.Background()
	fsys, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	queues, err := queue.Open(ctx, fsys, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	topics, err := topic.Open(ctx, fsys, "us-east-1", queues)
	if err != nil {
		t.Fatal(err)
	}
	if err := queues.CreateQueue(ctx, "q", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := topics.CreateTopic(ctx, "t", nil, nil); err != nil {
		t.Fatal(err)
	}
	n := notifier{region: "us-east-1", queues: queues, topics: topics}
	tests := []struct {
		arn  string
		want bool
	}{
		{"arn:aws:sqs:us-east-1:000000000000:q", true},
		{"arn:aws:sns:us-east-1:000000000000:t", true},
		{"arn:aws:sqs:us-east-1:000000000000:missing", false},
		{"arn:aws:sns:us-east-1:000000000000:missing", false},
		{"arn:aws:sqs:eu-west-1:000000000000:q", false},
		{"arn:aws:sqs:us-east-1:111111111111:q", false},
		{"arn:aws:lambda:us-east-1:000000000000:q", false},
		{"q", false},
	}
	for _, tt := range tests {
		if got := n.Exists(ctx, tt.arn); got != tt.want {
			t.Errorf("Exists(%q) = %v, want %v", tt.arn, got, tt.want)
		}
	}
}

// signedS3 sends one signed S3 request to srv.
func signedS3(t *testing.T, srv *httptest.Server, method, path, body string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	creds := aws.Credentials{AccessKeyID: "AKIAPAILTEST00000000", SecretAccessKey: "pail-test-secret"}
	if err := v4.NewSigner().SignHTTP(t.Context(), creds, req, hash, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestPutObjectEventReachesQueue(t *testing.T) {
	ctx := context.Background()
	fsys, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	st, err := store.Open(ctx, fsys)
	if err != nil {
		t.Fatal(err)
	}
	queues, err := queue.Open(ctx, fsys, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	topics, err := topic.Open(ctx, fsys, "us-east-1", queues)
	if err != nil {
		t.Fatal(err)
	}
	if err := queues.CreateQueue(ctx, "events", nil, nil); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewHandler(config.Config{
		AccessKeyID: "AKIAPAILTEST00000000", SecretAccessKey: "pail-test-secret", Region: "us-east-1",
	}, st, queues, topics))
	t.Cleanup(srv.Close)

	doc := `<NotificationConfiguration><QueueConfiguration><Id>all</Id><Queue>arn:aws:sqs:us-east-1:000000000000:events</Queue>` +
		`<Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>`
	for _, step := range []struct{ method, path, body string }{
		{http.MethodPut, "/bkt", ""},
		{http.MethodPut, "/bkt?notification", doc},
		{http.MethodPut, "/bkt/key.txt", "hello"},
	} {
		if status := signedS3(t, srv, step.method, step.path, step.body); status != http.StatusOK {
			t.Fatalf("%s %s = %d, want 200", step.method, step.path, status)
		}
	}
	wait := 0
	msgs, err := queues.Receive(ctx, "events", queue.ReceiveInput{Max: 10, WaitTimeSeconds: &wait})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("messages = %d, want 2 (test event and put event)", len(msgs))
	}
	if !strings.Contains(msgs[0].Body, `"s3:TestEvent"`) {
		t.Errorf("first message = %s, want the test event", msgs[0].Body)
	}
	for _, want := range []string{`"eventName":"ObjectCreated:Put"`, `"configurationId":"all"`, `"key":"key.txt"`, `"size":5`, `"eTag":"5d41402abc4b2a76b9719d911017c592"`} {
		if !strings.Contains(msgs[1].Body, want) {
			t.Errorf("event %s lacks %s", msgs[1].Body, want)
		}
	}
}
