package snsapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
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
	listBody   = "Action=ListTopics&Version=2010-03-31"
)

func TestHandler(t *testing.T) {
	tests := []struct {
		name     string
		sign     string // secret to sign with; "" leaves the request unsigned
		body     string
		wantCode int
		want     string
		wantMsg  string // checked when set
	}{
		{"unimplemented action", testSecret, "Action=ConfirmSubscription&Version=2010-03-31", 400, "InvalidAction", "ConfirmSubscription is not supported"},
		{"unknown action", testSecret, "Action=Bogus&Version=2010-03-31", 400, "InvalidAction", "Bogus is not supported"},
		{"no action", testSecret, "Version=2010-03-31", 400, "InvalidAction", "the action is not supported"},
		{"unsigned", "", listBody, 403, "MissingAuthenticationToken", ""},
		{"wrong secret", "wrong", listBody, 403, "SignatureDoesNotMatch", ""},
		{"over the cap", testSecret, "Action=" + strings.Repeat("x", maxRequestBytes), 413, "RequestEntityTooLarge", ""},
	}
	h := New(Options{AccessKeyID: testKey, SecretAccessKey: testSecret})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(tt.body))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
			if tt.sign != "" {
				sum := sha256.Sum256([]byte(tt.body))
				creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: tt.sign}
				if err := v4.NewSigner().SignHTTP(r.Context(), creds, r, hex.EncodeToString(sum[:]), "sns", "us-east-1", time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if w.Code != tt.wantCode {
				t.Errorf("status = %d, want %d; body %s", w.Code, tt.wantCode, w.Body)
			}
			var body struct {
				Type      string `xml:"Error>Type"`
				Code      string `xml:"Error>Code"`
				Message   string `xml:"Error>Message"`
				RequestID string `xml:"RequestId"`
			}
			if err := xml.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("body %q: %v", w.Body, err)
			}
			if body.Code != tt.want {
				t.Errorf("Code = %q, want %q", body.Code, tt.want)
			}
			if tt.wantMsg != "" && body.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", body.Message, tt.wantMsg)
			}
			if body.Type != "Sender" {
				t.Errorf("Type = %q, want Sender", body.Type)
			}
			if got := w.Header().Get("x-amzn-RequestId"); got == "" || got != body.RequestID {
				t.Errorf("x-amzn-RequestId = %q, body RequestId = %q, want equal and non-empty", got, body.RequestID)
			}
			if got, want := w.Header().Get("Content-Type"), "text/xml"; got != want {
				t.Errorf("Content-Type = %q, want %q", got, want)
			}
		})
	}
}
