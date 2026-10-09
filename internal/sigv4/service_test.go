package sigv4

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// serviceSigner escapes the path twice, as SDK signers do for non-S3 services.
var serviceSigner = v4.NewSigner()

// signedService builds a request to a non-S3 service and signs it like an SDK.
func signedService(t *testing.T, service, contentType, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(body))
	r.Header.Set("Content-Type", contentType)
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	if err := serviceSigner.SignHTTP(r.Context(), creds, r, sha256Hex(body), service, "eu-west-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestVerifyService(t *testing.T) {
	const jsonBody = `{"QueueNamePrefix":"a"}`
	const formBody = "Action=ListTopics&Version=2010-03-31"
	tests := []struct {
		name    string
		request func(t *testing.T) *http.Request
		service string
		verify  func(v *Verifier, r *http.Request, service string) error
		want    error
		asErr   bool // want a *http.MaxBytesError
	}{
		{
			name: "sqs json",
			request: func(t *testing.T) *http.Request {
				return signedService(t, "sqs", "application/x-amz-json-1.0", jsonBody)
			},
			service: "sqs",
		},
		{
			name: "sns form",
			request: func(t *testing.T) *http.Request {
				return signedService(t, "sns", "application/x-www-form-urlencoded", formBody)
			},
			service: "sns",
		},
		{
			name: "body changed after signing",
			request: func(t *testing.T) *http.Request {
				r := signedService(t, "sqs", "application/x-amz-json-1.0", jsonBody)
				r.Body = io.NopCloser(strings.NewReader(`{"QueueNamePrefix":"b"}`))
				return r
			},
			service: "sqs",
			want:    ErrSignatureMismatch,
		},
		{
			name: "s3 scope to sqs",
			request: func(t *testing.T) *http.Request {
				return signedService(t, "s3", "application/x-amz-json-1.0", jsonBody)
			},
			service: "sqs",
			want:    ErrMalformedAuth,
		},
		{
			name: "sqs scope to s3 verify",
			request: func(t *testing.T) *http.Request {
				return signedService(t, "sqs", "application/x-amz-json-1.0", jsonBody)
			},
			service: "sqs",
			verify:  func(v *Verifier, r *http.Request, _ string) error { return v.Verify(r) },
			want:    ErrMalformedAuth,
		},
		{
			name: "no authorization",
			request: func(*testing.T) *http.Request {
				return httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(jsonBody))
			},
			service: "sqs",
			want:    ErrMissingAuth,
		},
		{
			name: "presigned query",
			request: func(*testing.T) *http.Request {
				return httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/?X-Amz-Algorithm=AWS4-HMAC-SHA256", strings.NewReader(jsonBody))
			},
			service: "sqs",
			want:    ErrNotImplemented,
		},
		{
			name: "body over the cap",
			request: func(t *testing.T) *http.Request {
				r := signedService(t, "sqs", "application/x-amz-json-1.0", jsonBody)
				r.Body = http.MaxBytesReader(httptest.NewRecorder(), r.Body, 5)
				return r
			},
			service: "sqs",
			asErr:   true,
		},
	}
	v := New(testKey, testSecret)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.request(t)
			verify := tt.verify
			if verify == nil {
				verify = (*Verifier).VerifyService
			}
			err := verify(v, r, tt.service)
			if tt.asErr {
				if _, ok := errors.AsType[*http.MaxBytesError](err); !ok {
					t.Fatalf("VerifyService() = %v, want *http.MaxBytesError", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("VerifyService() = %v, want %v", err, tt.want)
			}
			if tt.want != nil {
				return
			}
			got, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			if want := map[string]string{"sqs": jsonBody, "sns": formBody}[tt.service]; string(got) != want {
				t.Errorf("body after VerifyService = %q, want %q", got, want)
			}
		})
	}
}

func TestService(t *testing.T) {
	const scope = "AKID/20261009/us-east-1/%s/aws4_request"
	auth := func(service string) string {
		return "AWS4-HMAC-SHA256 Credential=" + strings.Replace(scope, "%s", service, 1) + ", SignedHeaders=host, Signature=abc"
	}
	tests := []struct {
		name, target, authorization, want string
	}{
		{"sqs header", "/", auth("sqs"), "sqs"},
		{"sns header", "/", auth("sns"), "sns"},
		{"s3 presigned query", "/b/k?X-Amz-Credential=AKID%2F20261009%2Fus-east-1%2Fs3%2Faws4_request", "", "s3"},
		{"no auth", "/", "", ""},
		{"short credential", "/", "AWS4-HMAC-SHA256 Credential=AKID/20261009/sqs, SignedHeaders=host, Signature=abc", ""},
		{"not sigv4", "/", "AWS AKID:sig", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, tt.target, nil)
			if tt.authorization != "" {
				r.Header.Set("Authorization", tt.authorization)
			}
			if got := Service(r); got != tt.want {
				t.Errorf("Service(%q, %q) = %q, want %q", tt.target, tt.authorization, got, tt.want)
			}
		})
	}
}
