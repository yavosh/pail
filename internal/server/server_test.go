package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

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
