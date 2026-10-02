package s3api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	testKey    = "AKIAPAILTEST00000000"
	testSecret = "pail-test-secret"
)

func testOptions(domain string) Options {
	return Options{Domain: domain, AccessKeyID: testKey, SecretAccessKey: testSecret}
}

// signRequest signs req the way the S3 SDKs do, with an empty payload.
func signRequest(t *testing.T, req *http.Request, at time.Time) {
	t.Helper()
	signPayload(t, req, at, "")
}

// signPayload signs req with the SHA-256 of body as its payload hash.
func signPayload(t *testing.T, req *http.Request, at time.Time, body string) {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	if err := signer.SignHTTP(req.Context(), creds, req, hash, "s3", "us-east-1", at); err != nil {
		t.Fatal(err)
	}
}

// do sends one request to srv and returns the status, the headers, and the body.
func do(t *testing.T, srv *httptest.Server, method, path, host string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if host != "" {
		req.Host = host
	}
	signRequest(t, req, time.Now())
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, body
}

func TestNotImplementedError(t *testing.T) {
	srv := httptest.NewServer(New(testOptions("")))
	t.Cleanup(srv.Close)

	status, header, body := do(t, srv, http.MethodGet, "/bkt?tagging", "")
	if status != http.StatusNotImplemented {
		t.Fatalf("GET /bkt?tagging status = %d, want %d", status, http.StatusNotImplemented)
	}
	if got := header.Get("Content-Type"); got != "application/xml" {
		t.Errorf("Content-Type = %q, want application/xml", got)
	}
	var e errorBody
	if err := xml.Unmarshal(body, &e); err != nil {
		t.Fatalf("unmarshal %q: %v", body, err)
	}
	if e.Code != "NotImplemented" || e.Resource != "/bkt" {
		t.Errorf("error = %+v, want Code NotImplemented and Resource /bkt", e)
	}
	if id := header.Get("x-amz-request-id"); id == "" || e.RequestID != id {
		t.Errorf("RequestId = %q, header x-amz-request-id = %q, want equal and non-empty", e.RequestID, id)
	}
}

func TestHeadErrorHasNoBody(t *testing.T) {
	srv, _ := storeServer(t, "")

	status, _, body := do(t, srv, http.MethodHead, "/bkt/key", "")
	if status != http.StatusNotFound || len(body) != 0 {
		t.Errorf("HEAD /bkt/key = %d with %d body bytes, want %d with none", status, len(body), http.StatusNotFound)
	}
}

func TestRouting(t *testing.T) {
	srv, _ := storeServer(t, "localhost:9000")

	tests := []struct {
		name       string
		method     string
		path       string
		host       string
		wantStatus int
		wantBody   *string // nil skips the body check
	}{
		{"unclean path is not redirected", http.MethodGet, "/bkt/a//b", "", http.StatusNotFound, nil},
		{"dot segment is not redirected", http.MethodGet, "/bkt/./x", "", http.StatusNotFound, nil},
		{"dot-dot segment gets a bare 400, as on AWS", http.MethodGet, "/bkt/../x", "", http.StatusBadRequest, new("")},
		{"health", http.MethodGet, "/_pail/health", "", http.StatusOK, new("ok\n")},
		{"health head", http.MethodHead, "/_pail/health", "", http.StatusOK, new("")},
		{"virtual-hosted _pail is a key", http.MethodGet, "/_pail/health", "bkt.localhost", http.StatusNotFound, nil},
		{"domain with port still matches", http.MethodGet, "/_pail/health", "bkt.localhost:9000", http.StatusNotFound, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _, body := do(t, srv, tt.method, tt.path, tt.host)
			if status != tt.wantStatus {
				t.Errorf("%s %s (host %q) status = %d, want %d", tt.method, tt.path, tt.host, status, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusNotFound && !strings.Contains(string(body), "<Code>NoSuchBucket</Code>") {
				t.Errorf("%s %s body = %q, want an S3 NoSuchBucket error, not a mux 404", tt.method, tt.path, body)
			}
			if tt.wantBody != nil && string(body) != *tt.wantBody {
				t.Errorf("%s %s body = %q, want %q", tt.method, tt.path, body, *tt.wantBody)
			}
		})
	}
}

func TestRequestIDs(t *testing.T) {
	srv, _ := storeServer(t, "")

	var ids []string
	for _, path := range []string{"/_pail/health", "/bkt"} {
		_, h, _ := do(t, srv, http.MethodGet, path, "")
		if h.Get("x-amz-request-id") == "" || h.Get("x-amz-id-2") == "" {
			t.Errorf("GET %s: x-amz-request-id = %q, x-amz-id-2 = %q, want both set", path, h.Get("x-amz-request-id"), h.Get("x-amz-id-2"))
		}
		ids = append(ids, h.Get("x-amz-request-id"))
	}
	if ids[0] == ids[1] {
		t.Errorf("two requests share x-amz-request-id %q, want unique", ids[0])
	}
}

func TestRecorderKeepsFirstStatus(t *testing.T) {
	rec := &recorder{ResponseWriter: httptest.NewRecorder(), status: http.StatusOK}
	rec.WriteHeader(http.StatusNotFound)
	rec.WriteHeader(http.StatusInternalServerError)
	if rec.status != http.StatusNotFound {
		t.Errorf("WriteHeader(404) then WriteHeader(500): status = %d, want %d", rec.status, http.StatusNotFound)
	}
}

func TestAuthErrors(t *testing.T) {
	srv := httptest.NewServer(New(testOptions("")))
	t.Cleanup(srv.Close)

	tests := []struct {
		name     string
		prepare  func(*http.Request)
		wantCode string
		wantHTTP int
	}{
		{"unsigned", func(*http.Request) {}, "AccessDenied", http.StatusForbidden},
		{"sigv2", func(r *http.Request) { r.Header.Set("Authorization", "AWS "+testKey+":c2ln") }, "InvalidRequest", http.StatusBadRequest},
		{"bad signature", func(r *http.Request) {
			signRequest(t, r, time.Now())
			r.Host = "changed.example"
		}, "SignatureDoesNotMatch", http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/bkt", nil)
			if err != nil {
				t.Fatal(err)
			}
			tt.prepare(req)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			var e errorBody
			body, _ := io.ReadAll(resp.Body)
			if err := xml.Unmarshal(body, &e); err != nil {
				t.Fatalf("unmarshal %q: %v", body, err)
			}
			if resp.StatusCode != tt.wantHTTP || e.Code != tt.wantCode {
				t.Errorf("GET /bkt (%s) = %d %s, want %d %s", tt.name, resp.StatusCode, e.Code, tt.wantHTTP, tt.wantCode)
			}
		})
	}
}

func TestPresignedAuthErrors(t *testing.T) {
	srv, st := storeServer(t, "")
	if err := st.CreateBucket(context.Background(), "bkt"); err != nil {
		t.Fatal(err)
	}
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}

	tests := []struct {
		name       string
		signedAt   time.Duration // relative to now
		expires    string
		tamper     func(url.Values)
		wantStatus int
		wantCode   string
	}{
		{"valid, missing key", 0, "900", nil, http.StatusNotFound, "NoSuchKey"},
		{"expired", -2 * time.Hour, "3600", nil, http.StatusForbidden, "AccessDenied"},
		{"expires too long", 0, "604801", nil, http.StatusBadRequest, "AuthorizationQueryParametersError"},
		{"missing date", 0, "900", func(q url.Values) { q.Del("X-Amz-Date") }, http.StatusBadRequest, "AuthorizationQueryParametersError"},
		{"bad credential service", 0, "900", func(q url.Values) {
			q.Set("X-Amz-Credential", strings.Replace(q.Get("X-Amz-Credential"), "/s3/", "/ec2/", 1))
		}, http.StatusBadRequest, "AuthorizationQueryParametersError"},
		{"tampered", 0, "900", func(q url.Values) { q.Set("x-id", "Tampered") }, http.StatusForbidden, "SignatureDoesNotMatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/bkt/missing?X-Amz-Expires="+tt.expires, nil)
			if err != nil {
				t.Fatal(err)
			}
			signedURL, _, err := signer.PresignHTTP(req.Context(), creds, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now().Add(tt.signedAt))
			if err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(signedURL)
			if err != nil {
				t.Fatal(err)
			}
			if tt.tamper != nil {
				q := u.Query()
				tt.tamper(q)
				u.RawQuery = q.Encode()
			}
			req, err = http.NewRequestWithContext(context.Background(), http.MethodGet, u.String(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if status, code := send(t, req); status != tt.wantStatus || code != tt.wantCode {
				t.Errorf("presigned GET (%s) = %d %s, want %d %s", tt.name, status, code, tt.wantStatus, tt.wantCode)
			}
		})
	}
}

func TestHealthNeedsNoCredentials(t *testing.T) {
	srv := httptest.NewServer(New(testOptions("")))
	t.Cleanup(srv.Close)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/_pail/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("unsigned GET /_pail/health = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}
