package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

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
		{"Bytes=0-4", 11, 0, 4, true, true},
		{"bytes=+0-+4", 11, 0, 0, false, false},
		{"bytes=9223372036854775807-", 11, 0, 0, true, false},
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
		{"if-match weak never matches", map[string]string{"If-Match": `W/"abc"`}, http.StatusPreconditionFailed},
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
		{"empty Content-MD5", "/bkt/k", map[string]string{"Content-MD5": ""}, http.StatusBadRequest, "InvalidDigest"},
		{"empty metadata name", "/bkt/k", map[string]string{"X-Amz-Meta-": "x"}, http.StatusBadRequest, "InvalidArgument"},
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

// TestErrorResponsesCarryNoObjectHeaders guards against a 416 that keeps the
// object's Content-Encoding: clients would try to decompress the XML error.
func TestErrorResponsesCarryNoObjectHeaders(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	meta := map[string]string{"Content-Encoding": "gzip", "Cache-Control": "max-age=3600", "X-Amz-Meta-Color": "blue"}
	if _, err := st.PutObject(ctx, "bkt", "k", strings.NewReader("hello world"), store.PutOptions{Metadata: meta}); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		method, rng, ifMatch string
		wantStatus           int
		wantCR               string
	}{
		{http.MethodGet, "bytes=100-200", "", http.StatusRequestedRangeNotSatisfiable, ""},
		{http.MethodHead, "bytes=100-200", "", http.StatusRequestedRangeNotSatisfiable, ""},
		{http.MethodGet, "bytes=0-4", `"nope"`, http.StatusPreconditionFailed, ""},
		{http.MethodHead, "bytes=0-4", "", http.StatusPartialContent, "bytes 0-4/11"},
	}
	for _, tt := range tests {
		req, err := http.NewRequestWithContext(ctx, tt.method, srv.URL+"/bkt/k", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Range", tt.rng)
		if tt.ifMatch != "" {
			req.Header.Set("If-Match", tt.ifMatch)
		}
		req.Header.Set("Accept-Encoding", "identity")
		signRequest(t, req, time.Now())
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tt.wantStatus || resp.Header.Get("Content-Range") != tt.wantCR {
			t.Errorf("%s Range %s = %d %q, want %d %q", tt.method, tt.rng, resp.StatusCode, resp.Header.Get("Content-Range"), tt.wantStatus, tt.wantCR)
		}
		if tt.wantStatus >= 400 {
			// As recorded from AWS: an error carries no object headers at all.
			for _, h := range []string{"Content-Encoding", "Cache-Control", "X-Amz-Meta-Color", "ETag", "Last-Modified"} {
				if v := resp.Header.Get(h); v != "" {
					t.Errorf("%s Range %s error response has %s: %q, want none", tt.method, tt.rng, h, v)
				}
			}
		}
		if tt.wantStatus == http.StatusPartialContent && resp.Header.Get("Content-Length") != "5" {
			t.Errorf("HEAD Range %s Content-Length = %q, want 5", tt.rng, resp.Header.Get("Content-Length"))
		}
	}

	// A 304 keeps the caching headers a 200 would have carried (RFC 9110).
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/bkt/k", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("If-None-Match", "*")
	signRequest(t, req, time.Now())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotModified || resp.Header.Get("Cache-Control") != "max-age=3600" || resp.Header.Get("X-Amz-Meta-Color") != "blue" || resp.Header.Get("ETag") == "" {
		t.Errorf("GET If-None-Match * = %d, Cache-Control %q, X-Amz-Meta-Color %q, ETag %q, want 304 with all three", resp.StatusCode, resp.Header.Get("Cache-Control"), resp.Header.Get("X-Amz-Meta-Color"), resp.Header.Get("ETag"))
	}
	if v := resp.Header.Get("Content-Encoding"); v != "" {
		t.Errorf("GET If-None-Match * Content-Encoding = %q, want none: AWS sends no content headers with a 304", v)
	}
}

func TestIncompleteBody(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Sign a 10-byte body, send 5 bytes, then half-close.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, srv.URL+"/bkt/k", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.ContentLength = 10
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	if err := signer.SignHTTP(ctx, creds, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.WriteString("PUT /bkt/k HTTP/1.1\r\nHost: " + req.Host + "\r\nContent-Length: 10\r\n")
	for _, h := range []string{"Authorization", "X-Amz-Date", "X-Amz-Content-Sha256"} {
		b.WriteString(h + ": " + req.Header.Get(h) + "\r\n")
	}
	b.WriteString("\r\nhello")
	if _, err := conn.Write([]byte(b.String())); err != nil {
		t.Fatal(err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reply, _ := io.ReadAll(conn)
	if !strings.Contains(string(reply), "<Code>IncompleteBody</Code>") {
		t.Errorf("short body reply = %.200q, want IncompleteBody", reply)
	}
	if _, err := st.HeadObject(ctx, "bkt", "k"); !errors.Is(err, store.ErrNoSuchKey) {
		t.Errorf("a short body stored the object: HeadObject error = %v", err)
	}
}

func TestPutObjectChecksumHeaders(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		header     map[string]string
		wantStatus int
		wantCode   string
		wantAlg    string
	}{
		{"default", nil, http.StatusOK, "", "CRC64NVME"},
		{"crc32 value", map[string]string{"x-amz-checksum-crc32": "DUoRhQ=="}, http.StatusOK, "", "CRC32"},
		{"algorithm without a value", map[string]string{"x-amz-sdk-checksum-algorithm": "sha256"}, http.StatusBadRequest, "InvalidRequest", ""},
		{"algorithm with its value", map[string]string{"x-amz-sdk-checksum-algorithm": "crc32", "x-amz-checksum-crc32": "DUoRhQ=="}, http.StatusOK, "", "CRC32"},
		{"empty value", map[string]string{"x-amz-checksum-crc32": ""}, http.StatusBadRequest, "InvalidRequest", ""},
		{"x-amz-checksum-algorithm is not a PutObject header", map[string]string{"x-amz-checksum-algorithm": "SHA1"}, http.StatusOK, "", "CRC64NVME"},
		{"wrong value", map[string]string{"x-amz-checksum-crc32": "AAAAAA=="}, http.StatusBadRequest, "BadDigest", ""},
		{"bad base64", map[string]string{"x-amz-checksum-crc32": "nope!"}, http.StatusBadRequest, "InvalidRequest", ""},
		{"wrong length", map[string]string{"x-amz-checksum-crc32": "AAAAAAAAAAA="}, http.StatusBadRequest, "InvalidRequest", ""},
		{"two values", map[string]string{"x-amz-checksum-crc32": "DUoRhQ==", "x-amz-checksum-sha1": "Kq5sNclPz7QV2+lfQIuc6R7oRu0="}, http.StatusBadRequest, "InvalidRequest", ""},
		{"algorithm disagrees with value", map[string]string{"x-amz-sdk-checksum-algorithm": "SHA1", "x-amz-checksum-crc32": "DUoRhQ=="}, http.StatusBadRequest, "InvalidRequest", ""},
		{"unknown algorithm", map[string]string{"x-amz-sdk-checksum-algorithm": "BOGUS"}, http.StatusBadRequest, "InvalidRequest", ""},
		{"trailer without aws-chunked", map[string]string{"x-amz-trailer": "x-amz-checksum-crc32"}, http.StatusBadRequest, "InvalidRequest", ""},
		{"sha512 value", map[string]string{"x-amz-checksum-sha512": "MJ7MSJwS1utMxA9QyQLytNDtd+5RGnx6m808qG1M2G+YndNbxf9JlnDaNCVbRbDP2DDoH2Bdz33FVC6TrpzXbw=="}, http.StatusOK, "", "SHA512"},
		{"md5 value", map[string]string{"x-amz-checksum-md5": "XrY7u+Ae7tCTyyK7j1rNww=="}, http.StatusOK, "", "MD5"},
		{"xxhash64 value", map[string]string{"x-amz-checksum-xxhash64": "RatnNLIeaWg="}, http.StatusOK, "", "XXHASH64"},
		{"xxhash64 named by the sdk", map[string]string{"x-amz-sdk-checksum-algorithm": "xxhash64", "x-amz-checksum-xxhash64": "RatnNLIeaWg="}, http.StatusOK, "", "XXHASH64"},
		{"xxhash64 wrong value", map[string]string{"x-amz-checksum-xxhash64": "AAAAAAAAAAA="}, http.StatusBadRequest, "BadDigest", ""},
		{"md5 wrong length", map[string]string{"x-amz-checksum-md5": "AAAAAAAAAAA="}, http.StatusBadRequest, "InvalidRequest", ""},
		{"xxhash3 header", map[string]string{"x-amz-checksum-xxhash3": "AAAAAAAAAAA="}, http.StatusNotImplemented, "NotImplemented", ""},
		{"xxhash128 header", map[string]string{"x-amz-checksum-xxhash128": "AAAAAAAAAAAAAAAAAAAAAA=="}, http.StatusNotImplemented, "NotImplemented", ""},
		{"xxhash3 named by the sdk", map[string]string{"x-amz-sdk-checksum-algorithm": "XXHASH3"}, http.StatusNotImplemented, "NotImplemented", ""},
		{"unknown checksum header", map[string]string{"x-amz-checksum-bogus": "AAAAAA=="}, http.StatusBadRequest, "InvalidRequest", ""},
		{"checksum mode is not a checksum", map[string]string{"x-amz-checksum-mode": "ENABLED"}, http.StatusOK, "", "CRC64NVME"},
	}
	for _, tt := range tests {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, srv.URL+"/bkt/k", strings.NewReader("hello world"))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range tt.header {
			req.Header.Set(k, v)
		}
		signPayload(t, req, time.Now(), "hello world")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var e errorBody
		_ = xml.Unmarshal(body, &e)
		if resp.StatusCode != tt.wantStatus || e.Code != tt.wantCode {
			t.Errorf("%s: PUT = %d %q, want %d %q", tt.name, resp.StatusCode, e.Code, tt.wantStatus, tt.wantCode)
			continue
		}
		if tt.wantAlg != "" {
			header := "x-amz-checksum-" + strings.ToLower(tt.wantAlg)
			if resp.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" || resp.Header.Get(header) == "" {
				t.Errorf("%s: PUT response = type %q, %s %q; want FULL_OBJECT and a value", tt.name, resp.Header.Get("x-amz-checksum-type"), header, resp.Header.Get(header))
			}
		}
	}
}

func TestPutObjectStreaming(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	const crc32 = "x-amz-checksum-crc32" // "DUoRhQ==" is the CRC32 of "hello world"
	tests := []struct {
		name       string
		header     http.Header
		trailer    string // a trailer line, without CRLF
		wantStatus int
		wantCode   string
		wantAlg    string
		wantEnc    string
	}{
		{"checksum trailer", http.Header{"X-Amz-Trailer": {crc32}}, crc32 + ":DUoRhQ==", http.StatusOK, "", "CRC32", ""},
		{"no trailer", nil, "", http.StatusOK, "", "CRC64NVME", ""},
		{"gzip under aws-chunked", http.Header{"Content-Encoding": {"gzip", "aws-chunked"}}, "", http.StatusOK, "", "CRC64NVME", "gzip"},
		{"one list value", http.Header{"Content-Encoding": {"gzip, aws-chunked"}}, "", http.StatusOK, "", "CRC64NVME", "gzip"},
		{"named algorithm", http.Header{"X-Amz-Trailer": {crc32}, "X-Amz-Sdk-Checksum-Algorithm": {"CRC32"}}, crc32 + ":DUoRhQ==", http.StatusOK, "", "CRC32", ""},
		{"wrong trailer value", http.Header{"X-Amz-Trailer": {crc32}}, crc32 + ":AAAAAA==", http.StatusBadRequest, "BadDigest", "", ""},
		{"bad trailer base64", http.Header{"X-Amz-Trailer": {crc32}}, crc32 + ":nope!", http.StatusBadRequest, "InvalidRequest", "", ""},
		{"trailer is not a checksum", http.Header{"X-Amz-Trailer": {"x-amz-meta-color"}}, "x-amz-meta-color:blue", http.StatusBadRequest, "InvalidRequest", "", ""},
		{"two trailers", http.Header{"X-Amz-Trailer": {crc32 + ",x-amz-checksum-sha1"}}, crc32 + ":DUoRhQ==", http.StatusBadRequest, "InvalidRequest", "", ""},
		{"header and trailer", http.Header{"X-Amz-Trailer": {crc32}, "X-Amz-Checksum-Crc32": {"DUoRhQ=="}}, crc32 + ":DUoRhQ==", http.StatusBadRequest, "InvalidRequest", "", ""},
		{"algorithm disagrees with trailer", http.Header{"X-Amz-Trailer": {crc32}, "X-Amz-Sdk-Checksum-Algorithm": {"SHA1"}}, crc32 + ":DUoRhQ==", http.StatusBadRequest, "InvalidRequest", "", ""},
		{"missing decoded length", http.Header{"X-Amz-Decoded-Content-Length": nil}, "", http.StatusLengthRequired, "MissingContentLength", "", ""},
		{"decoded length over 5 GiB", http.Header{"X-Amz-Decoded-Content-Length": {"5368709121"}}, "", http.StatusBadRequest, "EntityTooLarge", "", ""},
		{"decoded length mismatch", http.Header{"X-Amz-Decoded-Content-Length": {"12"}}, "", http.StatusBadRequest, "IncompleteBody", "", ""},
	}
	for _, tt := range tests {
		const body = "hello world"
		enc := "6\r\nhello \r\n5\r\nworld\r\n0\r\n"
		if tt.trailer != "" {
			enc += tt.trailer + "\r\n"
		}
		enc += "\r\n"
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, srv.URL+"/bkt/k", strings.NewReader(enc))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Decoded-Content-Length", "11")
		for k, v := range tt.header {
			req.Header[k] = v
			if v == nil {
				req.Header.Del(k)
			}
		}
		req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
		creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
		if err := signer.SignHTTP(ctx, creds, req, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}
		_ = st.DeleteObject(ctx, "bkt", "k", store.DeleteOptions{})
		status, code := send(t, req)
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: PUT = %d %q, want %d %q", tt.name, status, code, tt.wantStatus, tt.wantCode)
			continue
		}
		info, err := st.HeadObject(ctx, "bkt", "k")
		if tt.wantStatus != http.StatusOK {
			if !errors.Is(err, store.ErrNoSuchKey) {
				t.Errorf("%s: a failed PUT stored the object: HeadObject error = %v", tt.name, err)
			}
			continue
		}
		if err != nil || info.Size != int64(len(body)) || info.ChecksumAlgorithm != tt.wantAlg || info.Metadata["Content-Encoding"] != tt.wantEnc {
			t.Errorf("%s: stored size %d, checksum %q, Content-Encoding %q, %v; want %d, %q, %q",
				tt.name, info.Size, info.ChecksumAlgorithm, info.Metadata["Content-Encoding"], err, len(body), tt.wantAlg, tt.wantEnc)
		}
	}
}

// Only an aws-chunked upload loses the aws-chunked coding; others store it as sent.
func TestPlainPutKeepsContentEncoding(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, srv.URL+"/bkt/k", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Encoding", "gzip, aws-chunked")
	signPayload(t, req, time.Now(), "x")
	if status, code := send(t, req); status != http.StatusOK {
		t.Fatalf("PUT = %d %q, want 200", status, code)
	}
	info, err := st.HeadObject(ctx, "bkt", "k")
	if got := info.Metadata["Content-Encoding"]; err != nil || got != "gzip, aws-chunked" {
		t.Errorf("stored Content-Encoding = %q, %v, want %q", got, err, "gzip, aws-chunked")
	}
}
