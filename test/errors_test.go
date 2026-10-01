package test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func TestUnsignedRequestIsDenied(t *testing.T) {
	p := startPail(t)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+p.addr+"/bkt?list-type=2", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), "<Code>AccessDenied</Code>") {
		t.Errorf("unsigned GET /bkt = %d %s, want 403 AccessDenied", resp.StatusCode, body)
	}
}

// TestSDKParsesErrors proves the SDK reads pail's XML errors: a signed call to
// an operation that does not exist yet surfaces its S3 code.
func TestSDKParsesErrors(t *testing.T) {
	forEachStyle(t, func(t *testing.T, c *s3.Client) {
		_, err := c.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{Bucket: aws.String("bkt")})
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "NotImplemented" {
			t.Errorf("GetBucketLocation error = %v, want an API error with code NotImplemented", err)
		}
	})
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestAddressingStyles checks the host each style sends, so the virtual-hosted
// runs really exercise virtual-hosted routing.
func TestAddressingStyles(t *testing.T) {
	p := startPail(t)
	_, port, _ := net.SplitHostPort(p.addr)
	tests := []struct {
		st       style
		wantHost string
	}{
		{styles[0], "localhost:" + port},
		{styles[1], "bkt.localhost:" + port},
	}
	for _, tt := range tests {
		var host string
		c := p.clientWith(tt.st, func(next http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(r *http.Request) (*http.Response, error) {
				host = r.Host
				if host == "" {
					host = r.URL.Host
				}
				return next.RoundTrip(r)
			})
		})
		_, _ = c.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{Bucket: aws.String("bkt")})
		if host != tt.wantHost {
			t.Errorf("%s request host = %q, want %q", tt.st.name, host, tt.wantHost)
		}
	}
}
