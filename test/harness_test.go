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
	addr string // host:port the server listens on
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
			t.Errorf("pail shutdown: %v", err)
		}
	})
	return &pail{addr: srv.Addr().String()}
}

// style is an S3 addressing style.
type style struct {
	name      string
	pathStyle bool
}

var styles = []style{{"path-style", true}, {"virtual-hosted", false}}

// client returns an SDK client for p. The dialer sends every host, including
// <bucket>.localhost, to p, so virtual-hosted requests need no DNS.
func (p *pail) client(st style) *s3.Client {
	return p.clientWith(st, nil)
}

// clientWith is client, with wrap applied to the HTTP transport when set.
func (p *pail) clientWith(st style, wrap func(http.RoundTripper) http.RoundTripper) *s3.Client {
	_, port, _ := net.SplitHostPort(p.addr)
	dialer := &net.Dialer{}
	var transport http.RoundTripper = &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, p.addr)
	}}
	if wrap != nil {
		transport = wrap(transport)
	}
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: transport}
	return s3.New(s3.Options{
		BaseEndpoint: aws.String("http://localhost:" + port),
		UsePathStyle: st.pathStyle,
		Region:       testRegion,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}, nil
		}),
		HTTPClient: httpClient,
		// A retry would hide the first answer.
		RetryMaxAttempts: 1,
	})
}

// forEachStyle runs fn once per addressing style against a fresh pail.
func forEachStyle(t *testing.T, fn func(t *testing.T, c *s3.Client)) {
	t.Helper()
	for _, st := range styles {
		t.Run(st.name, func(t *testing.T) {
			fn(t, startPail(t).client(st))
		})
	}
}
