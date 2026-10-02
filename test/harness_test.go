// Package test runs pail in-process and talks to it with aws-sdk-go-v2, the
// way users' code does. Every feature adds its client-level tests here.
package test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/s3api"
	"github.com/yavosh/pail/internal/server"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

const (
	testKey    = "AKIAPAILSDKTEST00000"
	testSecret = "pail-sdk-test-secret"
	testRegion = "us-east-1"
)

// pail is one running server for a test.
type pail struct {
	port     string
	endpoint string // the SDK's BaseEndpoint
	dataDir  string // the server's data directory, for tests that inspect storage
	// httpClient sends every host, including <bucket>.localhost, to the
	// server, so virtual-hosted and presigned URLs need no DNS or proxy.
	httpClient *http.Client
}

// startPail runs pail on a free loopback port with a fresh data directory and
// stops it when the test ends.
func startPail(t *testing.T) *pail {
	t.Helper()
	cfg := config.Config{
		Addr:            "127.0.0.1:0",
		DataDir:         t.TempDir(),
		AccessKeyID:     testKey,
		SecretAccessKey: testSecret,
		Region:          testRegion,
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
			t.Errorf("pail shutdown: %v (an unread response body may hold a connection open)", err)
		}
	})

	addr := srv.Addr().String()
	_, port, _ := net.SplitHostPort(addr)
	dialer := &net.Dialer{}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
		// The SDK's own client waits for 100-continue on large PUTs; match it.
		ExpectContinueTimeout: awshttp.DefaultHTTPTransportExpectContinueTimeout,
	}
	// Registered after the server's cleanup, so it runs first: a parked idle
	// connection would otherwise hold Shutdown for seconds.
	t.Cleanup(transport.CloseIdleConnections)
	return &pail{
		port:       port,
		endpoint:   "http://localhost:" + port,
		dataDir:    cfg.DataDir,
		httpClient: &http.Client{Timeout: 30 * time.Second, Transport: transport},
	}
}

// startPailTLS is startPail over HTTPS, which the SDK needs before it sends
// aws-chunked uploads. httptest's certificate covers example.com and its
// subdomains, so both addressing styles verify.
func startPailTLS(t *testing.T) *pail {
	t.Helper()
	dataDir := t.TempDir()
	fsys, err := localdisk.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	st, err := store.Open(context.Background(), fsys)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(s3api.New(s3api.Options{
		Domain:          "example.com",
		AccessKeyID:     testKey,
		SecretAccessKey: testSecret,
		Region:          testRegion,
		Store:           st,
	}))
	srv.StartTLS()
	t.Cleanup(srv.Close)

	addr := srv.Listener.Addr().String()
	_, port, _ := net.SplitHostPort(addr)
	base, _ := srv.Client().Transport.(*http.Transport)
	transport := base.Clone() // keeps the test certificate's root
	dialer := &net.Dialer{}
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}
	transport.ExpectContinueTimeout = awshttp.DefaultHTTPTransportExpectContinueTimeout
	t.Cleanup(transport.CloseIdleConnections)
	return &pail{
		port:       port,
		endpoint:   "https://example.com:" + port,
		dataDir:    dataDir,
		httpClient: &http.Client{Timeout: 30 * time.Second, Transport: transport},
	}
}

// style is an S3 addressing style.
type style struct {
	name      string
	pathStyle bool
}

var styles = []style{{"path-style", true}, {"virtual-hosted", false}}

// client returns an SDK client for p.
func (p *pail) client(st style) *s3.Client {
	return p.clientWith(st, nil)
}

// clientWith is client, with wrap applied to the HTTP transport when set.
func (p *pail) clientWith(st style, wrap func(http.RoundTripper) http.RoundTripper) *s3.Client {
	httpClient := p.httpClient
	if wrap != nil {
		httpClient = &http.Client{Timeout: p.httpClient.Timeout, Transport: wrap(p.httpClient.Transport)}
	}
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(p.endpoint),
		UsePathStyle: st.pathStyle,
		Region:       testRegion,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}, nil
		}),
		HTTPClient: httpClient,
		// s3.New skips the config loader that turns default checksums on, so
		// set them here, as users get them.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenSupported,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenSupported,
		// A retry would hide the first answer.
		RetryMaxAttempts: 1,
	})
}

// bucketURL is the URL of bucket in the given style, for raw requests.
func (p *pail) bucketURL(st style, bucket string) string {
	if st.pathStyle {
		return "http://localhost:" + p.port + "/" + bucket
	}
	return "http://" + bucket + ".localhost:" + p.port
}

// forEachStyle runs fn once per addressing style against a fresh pail.
func forEachStyle(t *testing.T, fn func(t *testing.T, p *pail, st style, c *s3.Client)) {
	t.Helper()
	for _, st := range styles {
		t.Run(st.name, func(t *testing.T) {
			p := startPail(t)
			fn(t, p, st, p.client(st))
		})
	}
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// headerRecorder captures the host and headers of each request it forwards.
type headerRecorder struct {
	next    http.RoundTripper
	hosts   []string
	headers []http.Header
}

func (h *headerRecorder) RoundTrip(r *http.Request) (*http.Response, error) {
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	h.hosts = append(h.hosts, host)
	h.headers = append(h.headers, r.Header.Clone())
	return h.next.RoundTrip(r)
}
