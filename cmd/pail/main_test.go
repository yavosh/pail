package main

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func noEnv(string) string { return "" }

func TestRunVersion(t *testing.T) {
	var out bytes.Buffer
	if err := run(t.Context(), []string{"--version"}, noEnv, &out); err != nil {
		t.Fatalf("run(--version) error = %v, want nil", err)
	}
	if !strings.HasPrefix(out.String(), "pail ") {
		t.Errorf("run(--version) output = %q, want prefix %q", out.String(), "pail ")
	}
}

func TestRunMissingKeys(t *testing.T) {
	var out bytes.Buffer
	err := run(t.Context(), nil, noEnv, &out)
	if err == nil || !strings.Contains(err.Error(), "PAIL_ACCESS_KEY_ID") {
		t.Errorf("run() error = %v, want it to mention %q", err, "PAIL_ACCESS_KEY_ID")
	}
}

// healthServer serves status on /_pail/health and returns its host:port.
func healthServer(t *testing.T, status int) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/_pail/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// freeAddr returns a loopback address with nothing listening on it.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return addr
}

func TestRunHealthcheck(t *testing.T) {
	tests := []struct {
		name    string
		addr    func(*testing.T) string
		wantErr string
	}{
		{"healthy", func(t *testing.T) string { return healthServer(t, http.StatusOK) }, ""},
		{"server error", func(t *testing.T) string { return healthServer(t, http.StatusInternalServerError) }, "500"},
		{"nothing listening", freeAddr, "healthcheck"},
		{"bad address", func(*testing.T) string { return "no-port" }, "invalid address"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{"PAIL_ADDR": tt.addr(t)}
			getenv := func(k string) string { return env[k] }
			err := run(t.Context(), []string{"--healthcheck"}, getenv, &bytes.Buffer{})
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("run(--healthcheck, %v) error = %v, want nil", env, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("run(--healthcheck, %v) error = %v, want it to contain %q", env, err, tt.wantErr)
			}
		})
	}
}

func TestRunHealthcheckFlagAddr(t *testing.T) {
	addr := healthServer(t, http.StatusOK)
	if err := run(t.Context(), []string{"--healthcheck", "--addr", addr}, noEnv, &bytes.Buffer{}); err != nil {
		t.Errorf("run(--healthcheck --addr %s) error = %v, want nil", addr, err)
	}
}

func TestHealthURL(t *testing.T) {
	tests := []struct {
		addr, want string
	}{
		{"127.0.0.1:9000", "http://127.0.0.1:9000/_pail/health"},
		{"0.0.0.0:9000", "http://127.0.0.1:9000/_pail/health"},
		{":9000", "http://127.0.0.1:9000/_pail/health"},
		{"[::]:9000", "http://[::1]:9000/_pail/health"},
		{"localhost:9000", "http://localhost:9000/_pail/health"},
		{"[::1]:9000", "http://[::1]:9000/_pail/health"},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			got, err := healthURL(tt.addr)
			if err != nil || got != tt.want {
				t.Errorf("healthURL(%q) = %q, %v, want %q", tt.addr, got, err, tt.want)
			}
		})
	}
}
