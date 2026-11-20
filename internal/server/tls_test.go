package server

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yavosh/pail/internal/config"
)

func TestServeHTTPAndHTTPS(t *testing.T) {
	s, tlsCfg := listenWithTLS(t)
	stop, wait := startServe(t, s)
	transport := &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: true}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport}

	httpURL := "http://" + s.Addr().String()
	httpsURL := "https://" + s.TLSAddr().String()
	tests := []struct{ queue, create, get string }{
		{"plain", httpURL, httpsURL},
		{"secure", httpsURL, httpURL},
	}
	for _, tt := range tests {
		t.Run(tt.queue, func(t *testing.T) {
			body := "{\"QueueName\":\"" + tt.queue + "\"}"
			status, got, err := postSQS(t.Context(), client, tt.create, "CreateQueue", body, nil)
			want := "{\"QueueUrl\":\"" + tt.create + "/000000000000/" + tt.queue + "\"}"
			if err != nil || status != http.StatusOK || got != want {
				t.Fatalf("CreateQueue via %s = %d %q, %v; want 200 %q", tt.create, status, got, err, want)
			}

			status, got, err = postSQS(t.Context(), client, tt.get, "GetQueueUrl", body, nil)
			want = "{\"QueueUrl\":\"" + tt.get + "/000000000000/" + tt.queue + "\"}"
			if err != nil || status != http.StatusOK || got != want {
				t.Errorf("GetQueueUrl via %s = %d %q, %v; want 200 %q", tt.get, status, got, err, want)
			}
		})
	}

	stop()
	if err := wait(); err != nil {
		t.Fatalf("Serve after cancellation = %v, want nil", err)
	}
	var dialer net.Dialer
	for _, addr := range []net.Addr{s.Addr(), s.TLSAddr()} {
		conn, err := dialer.DialContext(t.Context(), "tcp", addr.String())
		if err == nil {
			_ = conn.Close()
			t.Errorf("dial %s after shutdown succeeded, want error", addr)
		}
	}
}

func TestServeClosesOtherListenerOnFailure(t *testing.T) {
	for _, failTLS := range []bool{false, true} {
		name := "http"
		if failTLS {
			name = "https"
		}
		t.Run(name, func(t *testing.T) {
			s, tlsCfg := listenWithTLS(t)
			_, wait := startServe(t, s)

			var conn net.Conn
			var err error
			failed := s.ln
			if failTLS {
				failed = s.tlsLn
				var dialer net.Dialer
				conn, err = dialer.DialContext(t.Context(), "tcp", s.Addr().String())
			} else {
				dialer := &tls.Dialer{Config: tlsCfg}
				conn, err = dialer.DialContext(t.Context(), "tcp", s.TLSAddr().String())
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(conn, "GET /_pail/health HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			resp, err := http.ReadResponse(reader, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if _, err := io.Copy(io.Discard, resp.Body); err != nil || resp.StatusCode != http.StatusOK {
				t.Fatalf("health response = %d, %v; want 200 and nil error", resp.StatusCode, err)
			}

			if err := failed.Close(); err != nil {
				t.Fatal(err)
			}
			if err := wait(); !errors.Is(err, net.ErrClosed) {
				t.Errorf("Serve after %s listener failure = %v, want net.ErrClosed", name, err)
			}
			if _, err := reader.Peek(1); !errors.Is(err, io.EOF) {
				t.Errorf("other connection read after %s listener failure = %v, want EOF", name, err)
			}
		})
	}
}

func TestListenReleasesHTTPPortOnTLSBindFailure(t *testing.T) {
	cfg, _ := testTLSConfig(t)
	var lc net.ListenConfig
	probe, err := lc.Listen(t.Context(), "tcp", cfg.Addr)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Addr = probe.Addr().String()
	cfg.TLSAddr = cfg.Addr
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	s := New(cfg)
	if err := s.Listen(t.Context()); err == nil || !strings.Contains(err.Error(), "listen for tls on "+cfg.TLSAddr) {
		t.Fatalf("Listen with the same HTTP and HTTPS address = %v, want TLS bind error", err)
	}
	rebound, err := lc.Listen(t.Context(), "tcp", cfg.Addr)
	if err != nil {
		t.Fatalf("rebind HTTP address after failed startup = %v, want nil", err)
	}
	t.Cleanup(func() { _ = rebound.Close() })
}

func TestListenRejectsInvalidTLSCertificate(t *testing.T) {
	cfg, _ := testTLSConfig(t)
	if err := os.WriteFile(cfg.TLSCert, []byte("invalid certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.DataDir = filepath.Join(cfg.DataDir, "unopened")
	err := New(cfg).Listen(t.Context())
	if err == nil || !strings.Contains(err.Error(), "load tls key pair") {
		t.Errorf("Listen with invalid TLS certificate = %v, want key pair error", err)
	}
	if _, err := os.Stat(cfg.DataDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stat data directory after invalid certificate = %v, want os.ErrNotExist", err)
	}
}

func testTLSConfig(t *testing.T) (config.Config, *tls.Config) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg := config.Config{
		Addr: "127.0.0.1:0", TLSAddr: "127.0.0.1:0", DataDir: filepath.Join(dir, "data"),
		Region: "us-east-1", AccessKeyID: "AKIAPAILTEST00000000", SecretAccessKey: "pail-test-secret",
		TLSCert: filepath.Join(dir, "server.pem"), TLSKey: filepath.Join(dir, "server-key.pem"),
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	if err := os.WriteFile(cfg.TLSCert, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.TLSKey, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("failed to trust test certificate")
	}
	return cfg, &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
}

func listenWithTLS(t *testing.T) (*Server, *tls.Config) {
	t.Helper()
	cfg, tlsCfg := testTLSConfig(t)
	s := New(cfg)
	if err := s.Listen(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = s.ln.Close()
		_ = s.tlsLn.Close()
		_ = s.fs.Close()
	})
	return s, tlsCfg
}

func startServe(t *testing.T, s *Server) (context.CancelFunc, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var serveErr error
	go func() {
		defer close(done)
		serveErr = s.Serve(ctx)
	}()
	wait := func() error {
		select {
		case <-done:
			return serveErr
		case <-time.After(15 * time.Second):
			return errors.New("serve did not finish")
		}
	}
	t.Cleanup(func() {
		cancel()
		_ = wait()
	})
	return cancel, wait
}
