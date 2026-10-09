package sqsapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	testKey    = "AKIAPAILTEST00000000"
	testSecret = "pail-test-secret"
	listBody   = `{}`
)

func TestHandler(t *testing.T) {
	tests := []struct {
		name      string
		sign      string // secret to sign with; "" leaves the request unsigned
		body      string
		wantCode  int
		wantType  string
		wantQuery string
	}{
		{"signed", testSecret, listBody, 400, "com.amazonaws.sqs#UnsupportedOperation", "AWS.SimpleQueueService.UnsupportedOperation;Sender"},
		{"unsigned", "", listBody, 403, "com.amazon.coral.service#MissingAuthenticationTokenException", "MissingAuthenticationToken;Sender"},
		{"wrong secret", "wrong", listBody, 403, "com.amazon.coral.service#InvalidSignatureException", "SignatureDoesNotMatch;Sender"},
		{"over the cap", testSecret, strings.Repeat("x", maxRequestBytes+1), 413, "com.amazon.coral.service#RequestEntityTooLargeException", "RequestEntityTooLarge;Sender"},
	}
	h := New(Options{AccessKeyID: testKey, SecretAccessKey: testSecret})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(tt.body))
			r.Header.Set("Content-Type", "application/x-amz-json-1.0")
			r.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
			if tt.sign != "" {
				sum := sha256.Sum256([]byte(tt.body))
				creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: tt.sign}
				if err := v4.NewSigner().SignHTTP(r.Context(), creds, r, hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if w.Code != tt.wantCode {
				t.Errorf("status = %d, want %d; body %s", w.Code, tt.wantCode, w.Body)
			}
			var body struct {
				Type    string `json:"__type"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("body %q: %v", w.Body, err)
			}
			if body.Type != tt.wantType {
				t.Errorf("__type = %q, want %q", body.Type, tt.wantType)
			}
			if got := w.Header().Get("x-amzn-query-error"); got != tt.wantQuery {
				t.Errorf("x-amzn-query-error = %q, want %q", got, tt.wantQuery)
			}
			if got, want := w.Header().Get("Content-Type"), "application/x-amz-json-1.0"; got != want {
				t.Errorf("Content-Type = %q, want %q", got, want)
			}
			if w.Header().Get("x-amzn-RequestId") == "" {
				t.Error("x-amzn-RequestId is empty")
			}
		})
	}
}
