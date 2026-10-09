package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouter(t *testing.T) {
	const auth = "AWS4-HMAC-SHA256 Credential=AKID/20261009/us-east-1/%s/aws4_request, SignedHeaders=host, Signature=abc"
	named := func(name string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(name)) })
	}
	rt := &router{s3: named("s3"), sqs: named("sqs"), sns: named("sns")}

	tests := []struct {
		name, target, authorization, amzTarget, want string
	}{
		{"sqs scope", "/", fmt.Sprintf(auth, "sqs"), "", "sqs"},
		{"sns scope", "/", fmt.Sprintf(auth, "sns"), "", "sns"},
		{"s3 scope", "/b/k", fmt.Sprintf(auth, "s3"), "", "s3"},
		{"other scope", "/", fmt.Sprintf(auth, "sts"), "", "s3"},
		{"unsigned sqs target", "/", "", "AmazonSQS.ListQueues", "sqs"},
		{"unsigned root", "/", "", "", "s3"},
		{"presigned s3", "/b/k?X-Amz-Credential=AKID%2F20261009%2Fus-east-1%2Fs3%2Faws4_request", "", "", "s3"},
		{"health", "/_pail/health", "", "", "s3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, tt.target, nil)
			if tt.authorization != "" {
				r.Header.Set("Authorization", tt.authorization)
			}
			if tt.amzTarget != "" {
				r.Header.Set("X-Amz-Target", tt.amzTarget)
			}
			w := httptest.NewRecorder()
			rt.ServeHTTP(w, r)
			if got := w.Body.String(); got != tt.want {
				t.Errorf("route(%q, %q, %q) = %q, want %q", tt.target, tt.authorization, tt.amzTarget, got, tt.want)
			}
		})
	}
}
