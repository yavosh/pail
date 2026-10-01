package s3api

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
	srv := httptest.NewServer(New(Options{}))
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
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

	status, _, body := do(t, srv, http.MethodHead, "/bkt/key", "")
	if status != http.StatusNotImplemented || len(body) != 0 {
		t.Errorf("HEAD /bkt/key = %d with %d body bytes, want %d with none", status, len(body), http.StatusNotImplemented)
	}
}

func TestRouting(t *testing.T) {
	srv := httptest.NewServer(New(Options{Domain: "localhost:9000"}))
	t.Cleanup(srv.Close)

	tests := []struct {
		name       string
		method     string
		path       string
		host       string
		wantStatus int
		wantBody   *string // nil skips the body check
	}{
		{"unclean path is not redirected", http.MethodGet, "/bkt/a//b", "", http.StatusNotImplemented, nil},
		{"dot segment is not redirected", http.MethodGet, "/bkt/../x", "", http.StatusNotImplemented, nil},
		{"health", http.MethodGet, "/_pail/health", "", http.StatusOK, new("ok\n")},
		{"health head", http.MethodHead, "/_pail/health", "", http.StatusOK, new("")},
		{"virtual-hosted _pail is a key", http.MethodGet, "/_pail/health", "bkt.localhost", http.StatusNotImplemented, nil},
		{"domain with port still matches", http.MethodGet, "/_pail/health", "bkt.localhost:9000", http.StatusNotImplemented, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _, body := do(t, srv, tt.method, tt.path, tt.host)
			if status != tt.wantStatus {
				t.Errorf("%s %s (host %q) status = %d, want %d", tt.method, tt.path, tt.host, status, tt.wantStatus)
			}
			if tt.wantBody != nil && string(body) != *tt.wantBody {
				t.Errorf("%s %s body = %q, want %q", tt.method, tt.path, body, *tt.wantBody)
			}
		})
	}
}

func TestRequestIDs(t *testing.T) {
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

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
