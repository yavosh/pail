package test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// get sends an unsigned GET through p's client and returns the status and body.
func get(t *testing.T, p *pail, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func TestUnsignedRequestIsDenied(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, st style, _ *s3.Client) {
		url := p.bucketURL(st, "bkt") + "/?list-type=2"
		if status, body := get(t, p, url); status != http.StatusForbidden || !strings.Contains(body, "<Code>AccessDenied</Code>") {
			t.Errorf("unsigned GET %s = %d %s, want 403 AccessDenied", url, status, body)
		}
	})
}

// TestVirtualHostedRouting proves the server routes by host: /_pail/health on a
// bucket host is an S3 key, so it needs a signature, while path-style it is
// pail's open health endpoint.
func TestVirtualHostedRouting(t *testing.T) {
	p := startPail(t)
	tests := []struct {
		url        string
		wantStatus int
	}{
		{"http://localhost:" + p.port + "/_pail/health", http.StatusOK},
		{"http://bkt.localhost:" + p.port + "/_pail/health", http.StatusForbidden},
	}
	for _, tt := range tests {
		if status, body := get(t, p, tt.url); status != tt.wantStatus {
			t.Errorf("unsigned GET %s = %d %s, want %d", tt.url, status, body, tt.wantStatus)
		}
	}
}

// TestSDKParsesErrors proves the SDK reads pail's XML errors: a signed call to
// an operation that does not exist yet surfaces its S3 code.
func TestSDKParsesErrors(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		_, err := c.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{Bucket: aws.String("bkt")})
		apiErr, ok := errors.AsType[smithy.APIError](err)
		if !ok || apiErr.ErrorCode() != "NotImplemented" {
			t.Errorf("GetBucketLocation error = %v, want an API error with code NotImplemented", err)
		}
	})
}

// TestClientSettings checks what the SDK client sends: the host for each
// style, and the default checksum headers users' clients send.
func TestClientSettings(t *testing.T) {
	p := startPail(t)
	tests := []struct {
		st       style
		wantHost string
	}{
		{styles[0], "localhost:" + p.port},
		{styles[1], "bkt.localhost:" + p.port},
	}
	for _, tt := range tests {
		rec := &headerRecorder{}
		c := p.clientWith(tt.st, func(next http.RoundTripper) http.RoundTripper {
			rec.next = next
			return rec
		})
		ctx := context.Background()
		_, _ = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("bkt"), Key: aws.String("k"), Body: strings.NewReader("hello")})
		_, _ = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("bkt"), Key: aws.String("k")})
		if len(rec.headers) != 2 {
			t.Fatalf("%s: %d requests recorded, want 2", tt.st.name, len(rec.headers))
		}
		for i, host := range rec.hosts {
			if host != tt.wantHost {
				t.Errorf("%s request %d host = %q, want %q", tt.st.name, i, host, tt.wantHost)
			}
		}
		if got := rec.headers[0].Get("X-Amz-Checksum-Crc32"); got == "" {
			t.Errorf("%s PutObject sent no x-amz-checksum-crc32, want the SDK default checksum", tt.st.name)
		}
		if got := rec.headers[1].Get("X-Amz-Checksum-Mode"); got != "ENABLED" {
			t.Errorf("%s GetObject x-amz-checksum-mode = %q, want ENABLED", tt.st.name, got)
		}
	}
}
