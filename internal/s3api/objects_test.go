package s3api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yavosh/pail/internal/store"
)

func TestParseRange(t *testing.T) {
	tests := []struct {
		spec            string
		size            int64
		first, last     int64
		ok, satisfiable bool
	}{
		{"bytes=0-4", 11, 0, 4, true, true},
		{"bytes=6-", 11, 6, 10, true, true},
		{"bytes=-5", 11, 6, 10, true, true},
		{"bytes=-50", 11, 0, 10, true, true},
		{"bytes=5-100", 11, 5, 10, true, true},
		{"bytes=10-10", 11, 10, 10, true, true},
		{"bytes=11-", 11, 0, 0, true, false},
		{"bytes=100-200", 11, 0, 0, true, false},
		{"bytes=-0", 11, 0, 0, true, false},
		{"bytes=0-", 0, 0, 0, true, false},
		{"bytes=5-2", 11, 0, 0, false, false},
		{"bytes=0-1,4-5", 11, 0, 0, false, false},
		{"items=0-4", 11, 0, 0, false, false},
		{"bytes=a-b", 11, 0, 0, false, false},
		{"bytes=4", 11, 0, 0, false, false},
	}
	for _, tt := range tests {
		first, last, ok, sat := parseRange(tt.spec, tt.size)
		if first != tt.first || last != tt.last || ok != tt.ok || sat != tt.satisfiable {
			t.Errorf("parseRange(%q, %d) = %d, %d, %v, %v, want %d, %d, %v, %v",
				tt.spec, tt.size, first, last, ok, sat, tt.first, tt.last, tt.ok, tt.satisfiable)
		}
	}
}

func TestCheckConditions(t *testing.T) {
	modified := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	before := modified.Add(-time.Hour).Format(http.TimeFormat)
	after := modified.Add(time.Hour).Format(http.TimeFormat)
	const etag = `"abc"`
	tests := []struct {
		name   string
		header map[string]string
		want   int
	}{
		{"none", nil, 0},
		{"if-match hit", map[string]string{"If-Match": `"abc"`}, 0},
		{"if-match in list", map[string]string{"If-Match": `"x", "abc"`}, 0},
		{"if-match star", map[string]string{"If-Match": "*"}, 0},
		{"if-match unquoted", map[string]string{"If-Match": "abc"}, 0},
		{"if-match miss", map[string]string{"If-Match": `"nope"`}, http.StatusPreconditionFailed},
		{"if-unmodified-since before", map[string]string{"If-Unmodified-Since": before}, http.StatusPreconditionFailed},
		{"if-unmodified-since after", map[string]string{"If-Unmodified-Since": after}, 0},
		{"if-match hit overrides if-unmodified-since", map[string]string{"If-Match": etag, "If-Unmodified-Since": before}, 0},
		{"if-none-match hit", map[string]string{"If-None-Match": etag}, http.StatusNotModified},
		{"if-none-match weak hit", map[string]string{"If-None-Match": `W/"abc"`}, http.StatusNotModified},
		{"if-none-match miss", map[string]string{"If-None-Match": `"nope"`}, 0},
		{"if-modified-since after", map[string]string{"If-Modified-Since": after}, http.StatusNotModified},
		{"if-modified-since equal", map[string]string{"If-Modified-Since": modified.Format(http.TimeFormat)}, http.StatusNotModified},
		{"if-modified-since before", map[string]string{"If-Modified-Since": before}, 0},
		{"if-none-match miss overrides if-modified-since", map[string]string{"If-None-Match": `"nope"`, "If-Modified-Since": after}, 0},
		{"bad date is ignored", map[string]string{"If-Modified-Since": "yesterday"}, 0},
		{"412 wins over 304", map[string]string{"If-Match": `"nope"`, "If-None-Match": etag}, http.StatusPreconditionFailed},
	}
	for _, tt := range tests {
		h := http.Header{}
		for k, v := range tt.header {
			h.Set(k, v)
		}
		if got := checkConditions(h, etag, modified); got != tt.want {
			t.Errorf("%s: checkConditions(%v) = %d, want %d", tt.name, tt.header, got, tt.want)
		}
	}
}

func TestPutObjectErrors(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, path string
		header     map[string]string
		wantStatus int
		wantCode   string
	}{
		{"bad Content-MD5", "/bkt/k", map[string]string{"Content-MD5": "nope"}, http.StatusBadRequest, "InvalidDigest"},
		{"wrong Content-MD5", "/bkt/k", map[string]string{"Content-MD5": "AAAAAAAAAAAAAAAAAAAAAA=="}, http.StatusBadRequest, "BadDigest"},
		{"metadata too large", "/bkt/k", map[string]string{"X-Amz-Meta-Big": strings.Repeat("x", maxUserMetadata)}, http.StatusBadRequest, "MetadataTooLarge"},
		{"key too long", "/bkt/" + strings.Repeat("k", maxKeyLen+1), nil, http.StatusBadRequest, "KeyTooLongError"},
		{"if-none-match etag", "/bkt/k", map[string]string{"If-None-Match": `"abc"`}, http.StatusNotImplemented, "NotImplemented"},
		{"missing bucket", "/nope/k", nil, http.StatusNotFound, "NoSuchBucket"},
	}
	for _, tt := range tests {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, srv.URL+tt.path, strings.NewReader("body"))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range tt.header {
			req.Header.Set(k, v)
		}
		signPayload(t, req, time.Now(), "body")
		status, code := send(t, req)
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: PUT %.40s = %d %q, want %d %q", tt.name, tt.path, status, code, tt.wantStatus, tt.wantCode)
		}
	}
	if _, err := st.HeadObject(ctx, "bkt", "k"); !errors.Is(err, store.ErrNoSuchKey) {
		t.Errorf("a failed PutObject stored the object: HeadObject error = %v", err)
	}
}

// TestTamperedBodyStoresNothing checks the SigV4 payload check fails a put
// whose body changed after signing, before anything is committed.
func TestTamperedBodyStoresNothing(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, srv.URL+"/bkt/k", strings.NewReader("evil"))
	if err != nil {
		t.Fatal(err)
	}
	signPayload(t, req, time.Now(), "good") // same length, different bytes
	status, code := send(t, req)
	if status != http.StatusBadRequest || code != "XAmzContentSHA256Mismatch" {
		t.Errorf("PUT with a tampered body = %d %q, want 400 XAmzContentSHA256Mismatch", status, code)
	}
	if _, err := st.HeadObject(ctx, "bkt", "k"); !errors.Is(err, store.ErrNoSuchKey) {
		t.Errorf("a tampered PutObject stored the object: HeadObject error = %v", err)
	}
}

// TestDefaultContentType uses a raw request: the SDK always sends a type.
func TestDefaultContentType(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, srv.URL+"/bkt/k", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	signPayload(t, req, time.Now(), "x")
	if status, code := send(t, req); status != http.StatusOK {
		t.Fatalf("PUT without Content-Type = %d %q, want 200", status, code)
	}
	info, err := st.HeadObject(ctx, "bkt", "k")
	if err != nil || info.Metadata["Content-Type"] != defaultType {
		t.Errorf("stored Content-Type = %q, %v, want %q", info.Metadata["Content-Type"], err, defaultType)
	}
}
