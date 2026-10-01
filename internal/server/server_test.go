package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yavosh/pail/internal/config"
)

func listen(t *testing.T, addr string) *Server {
	t.Helper()
	s := New(config.Config{Addr: addr})
	if err := s.Listen(t.Context()); err != nil {
		t.Fatalf("Listen(%q) error = %v", addr, err)
	}
	t.Cleanup(func() { s.ln.Close() })
	return s
}

func TestListenPortConflict(t *testing.T) {
	first := listen(t, "127.0.0.1:0")
	addr := first.Addr().String()

	err := New(config.Config{Addr: addr}).Listen(t.Context())
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
