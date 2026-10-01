// Package test runs pail in-process and talks to it with aws-sdk-go-v2, the
// way users' code does. Every feature adds its client-level tests here.
package test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/yavosh/pail/internal/config"
	"github.com/yavosh/pail/internal/server"
)

const (
	testKey    = "AKIAPAILSDKTEST00000"
	testSecret = "pail-sdk-test-secret"
	testRegion = "us-east-1"
)

// pail is one running server for a test.
type pail struct {
	addr    string // host:port the server listens on
	port    string
	dataDir string
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
			t.Errorf("pail shutdown: %v (an unread response body holds a connection open)", err)
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
		addr:       addr,
		port:       port,
		dataDir:    cfg.DataDir,
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
		BaseEndpoint: aws.String("http://localhost:" + p.port),
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

// headerRecorder captures the headers of each request it forwards.
type headerRecorder struct {
	next    http.RoundTripper
	host    string
	headers []http.Header
}

func (h *headerRecorder) RoundTrip(r *http.Request) (*http.Response, error) {
	h.host = r.Host
	if h.host == "" {
		h.host = r.URL.Host
	}
	h.headers = append(h.headers, r.Header.Clone())
	return h.next.RoundTrip(r)
}
