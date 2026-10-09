package server

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/yavosh/pail/internal/config"
)

func TestServeClosesConnectionsOnListenerFailure(t *testing.T) {
	s := listen(t, "127.0.0.1:0")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var serveErr error
	go func() {
		defer close(done)
		serveErr = s.Serve(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := io.WriteString(conn, "GET /_pail/health HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", resp.StatusCode)
	}
	if err := s.ln.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	if !errors.Is(serveErr, net.ErrClosed) {
		t.Fatalf("Serve error = %v, want net.ErrClosed", serveErr)
	}
	// Bound a regression failure if the server leaves the connection open.
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Peek(1); !errors.Is(err, io.EOF) {
		t.Errorf("connection read after Serve returned = %v, want EOF", err)
	}
}

func listen(t *testing.T, addr string) *Server {
	t.Helper()
	s := New(config.Config{Addr: addr, DataDir: t.TempDir()})
	if err := s.Listen(t.Context()); err != nil {
		t.Fatalf("Listen(%q) error = %v", addr, err)
	}
	t.Cleanup(func() { s.ln.Close(); _ = s.fs.Close() })
	return s
}

func TestListenPortConflict(t *testing.T) {
	first := listen(t, "127.0.0.1:0")
	addr := first.Addr().String()

	err := New(config.Config{Addr: addr, DataDir: t.TempDir()}).Listen(t.Context())
	if err == nil || !strings.Contains(err.Error(), "listen on "+addr) {
		t.Errorf("Listen(%q) error = %v, want it to contain %q", addr, err, "listen on "+addr)
	}
}

func TestServeStopsOnCancel(t *testing.T) {
	s := listen(t, "127.0.0.1:0")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()

	url := "http://" + s.Addr().String() + "/_pail/health"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s error = %v", url, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET %s status = %d, want %d", url, resp.StatusCode, http.StatusOK)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve() after cancel = %v, want nil", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Serve() did not return after cancel")
	}
}

// postSQS sends a signed SQS request to addr and returns the status and body.
// When running is set, the request sends Expect: 100-continue and running is
// closed on the server's 100 Continue, which the handler sends on its first body read.
func postSQS(ctx context.Context, addr, op, body string, running chan<- struct{}) (int, string, error) {
	if running != nil {
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{Got100Continue: func() { close(running) }})
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/", strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	if running != nil {
		req.Header.Set("Expect", "100-continue")
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+op)
	sum := sha256.Sum256([]byte(body))
	creds := aws.Credentials{AccessKeyID: "AKIAPAILTEST00000000", SecretAccessKey: "pail-test-secret"}
	if err := v4.NewSigner().SignHTTP(ctx, creds, req, hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now()); err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), err
}

func TestServeEndsLongPollsOnShutdown(t *testing.T) {
	s := New(config.Config{
		Addr: "127.0.0.1:0", DataDir: t.TempDir(), Region: "us-east-1",
		AccessKeyID: "AKIAPAILTEST00000000", SecretAccessKey: "pail-test-secret",
	})
	if err := s.Listen(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.ln.Close(); _ = s.fs.Close() })
	addr := s.Addr().String()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()

	if status, body, err := postSQS(ctx, addr, "CreateQueue", `{"QueueName":"q"}`, nil); err != nil || status != http.StatusOK {
		t.Fatalf("CreateQueue = %d %s, %v; want 200", status, body, err)
	}
	type result struct {
		status int
		body   string
		err    error
	}
	poll := make(chan result, 1)
	running := make(chan struct{})
	go func() {
		status, body, err := postSQS(context.WithoutCancel(ctx), addr, "ReceiveMessage",
			`{"QueueUrl":"http://`+addr+`/000000000000/q","WaitTimeSeconds":20}`, running)
		poll <- result{status, body, err}
	}()
	select {
	case <-running:
	case <-time.After(5 * time.Second):
		t.Fatal("the server did not start reading the long poll")
	}

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve() after cancel = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve() did not return while a long poll waited")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("shutdown took %v, want well under the 20 s poll", took)
	}
	select {
	case r := <-poll:
		if r.err != nil || r.status != http.StatusOK || r.body != "{}" {
			t.Errorf("long poll = %d %q, %v; want 200 {}", r.status, r.body, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long poll did not return")
	}
}
