package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yavosh/pail/internal/store"
)

// sendWith sends one signed request with body and extra headers.
func sendWith(t *testing.T, srv *httptest.Server, method, path, body string, header map[string]string) (int, string, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	signPayload(t, req, time.Now(), body)
	return sendRaw(t, req)
}

func TestParseCopySource(t *testing.T) {
	tests := []struct {
		raw        string
		want       target
		wantStatus int
		wantCode   string
	}{
		{"bkt/key", target{bucket: "bkt", key: "key"}, 0, ""},
		{"/bkt/key", target{bucket: "bkt", key: "key"}, 0, ""},
		{"bkt/dir/key", target{bucket: "bkt", key: "dir/key"}, 0, ""},
		{"bkt/a%20b", target{bucket: "bkt", key: "a b"}, 0, ""},
		{"bkt/a+b", target{bucket: "bkt", key: "a+b"}, 0, ""},
		{"bkt/a%3Fb", target{bucket: "bkt", key: "a?b"}, 0, ""},
		{"bkt/key?versionId=null", target{bucket: "bkt", key: "key"}, 0, ""},
		{"bkt/key?versionId=v1", target{}, http.StatusNotImplemented, "NotImplemented"},
		{"bkt/key?versionId=null&versionId=v1", target{}, http.StatusNotImplemented, "NotImplemented"},
		{"bkt/key?versionId=null&versionId=null", target{bucket: "bkt", key: "key"}, 0, ""},
		{"bkt/key?other=1", target{}, http.StatusBadRequest, "InvalidArgument"},
		{"bkt/key?%zz", target{}, http.StatusBadRequest, "InvalidArgument"},
		{"bkt", target{}, http.StatusBadRequest, "InvalidArgument"},
		{"bkt/", target{}, http.StatusBadRequest, "InvalidArgument"},
		{"/key", target{}, http.StatusBadRequest, "InvalidArgument"},
		{"bkt/a%zz", target{}, http.StatusBadRequest, "InvalidArgument"},
		{"bkt/%ff", target{}, http.StatusBadRequest, "InvalidArgument"},
		{"BAD_BUCKET/key", target{}, http.StatusBadRequest, "InvalidBucketName"},
		{"bkt/" + strings.Repeat("k", maxKeyLen+1), target{}, http.StatusBadRequest, "KeyTooLongError"},
	}
	for _, tt := range tests {
		got, apiErr, ok := parseCopySource(tt.raw)
		if ok != (tt.wantCode == "") || got != tt.want || apiErr.Code != tt.wantCode || apiErr.Status != tt.wantStatus {
			t.Errorf("parseCopySource(%.40q) = %+v, %d %q, %v; want %+v, %d %q", tt.raw, got, apiErr.Status, apiErr.Code, ok, tt.want, tt.wantStatus, tt.wantCode)
		}
	}
}

func TestCopyObject(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	for _, b := range []string{"bkt", "other"} {
		if err := st.CreateBucket(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	put := func(bucket, key string) store.ObjectInfo {
		t.Helper()
		info, err := st.PutObject(ctx, bucket, key, strings.NewReader("hello world"), store.PutOptions{
			Metadata:          map[string]string{"Content-Type": "text/plain", "Content-Disposition": "attachment", "X-Amz-Meta-Color": "blue"},
			ChecksumAlgorithm: "CRC32",
		})
		if err != nil {
			t.Fatal(err)
		}
		return info
	}
	srcInfo := put("bkt", "src")
	put("bkt", "a b?c")
	etag := quoteETag(srcInfo.ETag)
	past := srcInfo.LastModified.Add(-time.Hour).UTC().Format(http.TimeFormat)
	future := srcInfo.LastModified.Add(time.Hour).UTC().Format(http.TimeFormat)

	tests := []struct {
		name       string
		dst        string // bucket/key
		header     map[string]string
		wantStatus int
		wantCode   string
		wantType   string // checked on success
		wantColor  string
		wantDisp   string
		wantAlg    string
	}{
		{"default copies metadata", "bkt/dst", map[string]string{"x-amz-copy-source": "/bkt/src"}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"COPY without a leading slash", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-metadata-directive": "COPY"}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"COPY ignores request metadata", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-metadata-directive": "COPY", "Content-Type": "x/y", "X-Amz-Meta-Color": "red"}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"other bucket", "other/dst", map[string]string{"x-amz-copy-source": "bkt/src"}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"REPLACE uses request metadata", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-metadata-directive": "REPLACE", "Content-Type": "application/json", "X-Amz-Meta-Color": "red"}, 200, "", "application/json", "red", "", "CRC32"},
		{"REPLACE without metadata", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-metadata-directive": "REPLACE"}, 200, "", defaultType, "", "", "CRC32"},
		{"REPLACE with too much metadata", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-metadata-directive": "REPLACE", "X-Amz-Meta-Big": strings.Repeat("x", maxUserMetadata)}, 400, "MetadataTooLarge", "", "", "", ""},
		{"unknown directive", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-metadata-directive": "MERGE"}, 400, "InvalidArgument", "", "", "", ""},
		{"lowercase directive", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-metadata-directive": "replace"}, 400, "InvalidArgument", "", "", "", ""},
		{"onto itself", "bkt/src", map[string]string{"x-amz-copy-source": "bkt/src"}, 400, "InvalidRequest", "", "", "", ""},
		{"onto itself with COPY", "bkt/src", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-metadata-directive": "COPY"}, 400, "InvalidRequest", "", "", "", ""},
		{"onto itself with REPLACE", "bkt/src", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-metadata-directive": "REPLACE", "Content-Type": "application/json"}, 200, "", "application/json", "", "", "CRC32"},
		{"if-match hit", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-match": etag}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"if-match miss", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-match": `"nope"`}, 412, "PreconditionFailed", "", "", "", ""},
		{"if-none-match hit", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-none-match": etag}, 412, "PreconditionFailed", "", "", "", ""},
		{"if-none-match miss", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-none-match": `"nope"`}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"if-modified-since past", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-modified-since": past}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"if-modified-since future is ignored", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-modified-since": future}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"if-modified-since now", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-modified-since": "{now}"}, 412, "PreconditionFailed", "", "", "", ""},
		{"if-unmodified-since future", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-unmodified-since": future}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"if-unmodified-since past", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-unmodified-since": past}, 412, "PreconditionFailed", "", "", "", ""},
		{"source without a key", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt"}, 400, "InvalidArgument", "", "", "", ""},
		{"source with a bad escape", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/a%zz"}, 400, "InvalidArgument", "", "", "", ""},
		{"missing source key", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/nope"}, 404, "NoSuchKey", "", "", "", ""},
		{"missing source bucket", "bkt/dst", map[string]string{"x-amz-copy-source": "nope/src"}, 404, "NoSuchBucket", "", "", "", ""},
		{"missing destination bucket", "nope/dst", map[string]string{"x-amz-copy-source": "bkt/src"}, 404, "NoSuchBucket", "", "", "", ""},
		{"encoded space and question mark", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/" + url.PathEscape("a b?c")}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"unencoded question mark is a query", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/a b?c"}, 400, "InvalidArgument", "", "", "", ""},
		{"versionId null", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src?versionId=null"}, 200, "", "text/plain", "blue", "attachment", "CRC32"},
		{"other versionId", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src?versionId=v1"}, 501, "NotImplemented", "", "", "", ""},
		{"checksum algorithm", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-checksum-algorithm": "SHA256"}, 200, "", "text/plain", "blue", "attachment", "SHA256"},
		{"xxhash128 checksum algorithm", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-checksum-algorithm": "XXHASH128"}, 501, "NotImplemented", "", "", "", ""},
		{"unknown checksum algorithm", "bkt/dst", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-checksum-algorithm": "BOGUS"}, 400, "InvalidRequest", "", "", "", ""},
	}
	for _, tt := range tests {
		put("bkt", "src") // the onto-itself REPLACE case overwrites it
		_ = st.DeleteObject(ctx, "bkt", "dst", store.DeleteOptions{})
		_ = st.DeleteObject(ctx, "other", "dst", store.DeleteOptions{})
		// {now} is read after the put above, so it is never before the source's Last-Modified.
		header := maps.Clone(tt.header)
		for k, v := range header {
			header[k] = strings.ReplaceAll(v, "{now}", time.Now().UTC().Format(http.TimeFormat))
		}
		status, code, body := sendWith(t, srv, http.MethodPut, "/"+tt.dst, "", header)
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: PUT /%s = %d %q, want %d %q", tt.name, tt.dst, status, code, tt.wantStatus, tt.wantCode)
			continue
		}
		bucket, key, _ := strings.Cut(tt.dst, "/")
		info, err := st.HeadObject(ctx, bucket, key)
		if tt.wantCode != "" {
			if key == "dst" && !errors.Is(err, store.ErrNoSuchKey) && !errors.Is(err, store.ErrNoSuchBucket) {
				t.Errorf("%s: a failed copy stored the destination: HeadObject error = %v", tt.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: HeadObject error = %v", tt.name, err)
			continue
		}
		if got := info.Metadata["Content-Type"]; got != tt.wantType {
			t.Errorf("%s: Content-Type = %q, want %q", tt.name, got, tt.wantType)
		}
		if got := info.Metadata["X-Amz-Meta-Color"]; got != tt.wantColor {
			t.Errorf("%s: X-Amz-Meta-Color = %q, want %q", tt.name, got, tt.wantColor)
		}
		if got := info.Metadata["Content-Disposition"]; got != tt.wantDisp {
			t.Errorf("%s: Content-Disposition = %q, want %q", tt.name, got, tt.wantDisp)
		}
		if info.ChecksumAlgorithm != tt.wantAlg || info.ETag != srcInfo.ETag || info.Size != srcInfo.Size {
			t.Errorf("%s: stored = %s, %s, %d bytes; want %s, %s, %d bytes", tt.name, info.ChecksumAlgorithm, info.ETag, info.Size, tt.wantAlg, srcInfo.ETag, srcInfo.Size)
		}

		var result struct {
			ETag, LastModified, ChecksumType string
			Checksum                         string `xml:",any"`
		}
		if err := xml.Unmarshal(body, &result); err != nil {
			t.Errorf("%s: CopyObjectResult = %q, %v", tt.name, body, err)
			continue
		}
		if result.ETag != etag || result.LastModified != info.LastModified.UTC().Format(timeFormat) || result.ChecksumType != "FULL_OBJECT" {
			t.Errorf("%s: CopyObjectResult = %+v, want ETag %s, LastModified %s, ChecksumType FULL_OBJECT", tt.name, result, etag, info.LastModified.UTC().Format(timeFormat))
		}
		if !strings.Contains(string(body), "<Checksum"+tt.wantAlg+">"+info.Checksum+"</Checksum"+tt.wantAlg+">") {
			t.Errorf("%s: CopyObjectResult = %s, want a Checksum%s element", tt.name, body, tt.wantAlg)
		}
	}
}

func TestUploadPartCopy(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutObject(ctx, "bkt", "src", strings.NewReader("0123456789"), store.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutObject(ctx, "bkt", "cold", strings.NewReader("x"), store.PutOptions{StorageClass: "GLACIER"}); err != nil {
		t.Fatal(err)
	}
	up, err := st.CreateUpload(ctx, "bkt", "dst", store.UploadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const wholeETag = "781e5e245d69b566979b86e28d23f2c7" // MD5 of "0123456789"
	const rangeETag = "81b073de9370ea873f548e31b8adc081" // MD5 of "2345"

	tests := []struct {
		name       string
		query      string
		header     map[string]string
		wantStatus int
		wantCode   string
		wantETag   string
	}{
		{"whole object", "partNumber=1&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/src"}, 200, "", wholeETag},
		{"range", "partNumber=2&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "/bkt/src", "x-amz-copy-source-range": "bytes=2-5"}, 200, "", rangeETag},
		{"range to the last byte", "partNumber=3&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-range": "bytes=9-9"}, 200, "", ""},
		{"range past the end", "partNumber=4&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-range": "bytes=5-100"}, 400, "InvalidArgument", ""},
		{"range without unit", "partNumber=4&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-range": "2-5"}, 400, "InvalidArgument", ""},
		{"open-ended range", "partNumber=4&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-range": "bytes=2-"}, 400, "InvalidArgument", ""},
		{"reversed range", "partNumber=4&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-range": "bytes=5-2"}, 400, "InvalidArgument", ""},
		{"if-match miss", "partNumber=4&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-if-match": `"nope"`}, 412, "PreconditionFailed", ""},
		{"missing source", "partNumber=4&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/missing"}, 404, "NoSuchKey", ""},
		{"archived source", "partNumber=4&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/cold"}, 403, "InvalidObjectState", ""},
		{"bad source", "partNumber=4&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt"}, 400, "InvalidArgument", ""},
		{"source owner mismatch", "partNumber=4&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-source-expected-bucket-owner": "123456789012"}, 403, "AccessDenied", ""},
		{"unknown upload", "partNumber=4&uploadId=bogus", map[string]string{"x-amz-copy-source": "bkt/src"}, 404, "NoSuchUpload", ""},
		{"part number zero", "partNumber=0&uploadId=" + up.ID, map[string]string{"x-amz-copy-source": "bkt/src"}, 400, "InvalidArgument", ""},
	}
	for _, tt := range tests {
		r := call(t, srv, http.MethodPut, "/bkt/dst?"+tt.query, "", tt.header)
		if r.status != tt.wantStatus || r.code != tt.wantCode {
			t.Errorf("%s: status, code = %d, %q; want %d, %q (body %s)", tt.name, r.status, r.code, tt.wantStatus, tt.wantCode, r.body)
			continue
		}
		var got struct {
			ETag string
		}
		if tt.wantStatus == 200 {
			if err := xml.Unmarshal([]byte(r.body), &got); err != nil {
				t.Fatalf("%s: decode %q: %v", tt.name, r.body, err)
			}
		}
		if tt.wantETag != "" && got.ETag != `"`+tt.wantETag+`"` {
			t.Errorf("%s: ETag = %s, want %q", tt.name, got.ETag, tt.wantETag)
		}
	}

	_, parts, err := st.ListParts(ctx, "bkt", "dst", up.ID)
	if err != nil {
		t.Fatal(err)
	}
	var sizes []int64
	for _, p := range parts {
		sizes = append(sizes, p.Size)
	}
	if want := []int64{10, 4, 1}; !slices.Equal(sizes, want) {
		t.Errorf("part sizes = %v, want %v", sizes, want)
	}
}

func TestParseCopyRange(t *testing.T) {
	tests := []struct {
		spec             string
		size             int64
		wantFirst, wantN int64
		wantOK           bool
	}{
		{"bytes=0-9", 10, 0, 10, true},
		{"bytes=2-5", 10, 2, 4, true},
		{"bytes=9-9", 10, 9, 1, true},
		{"bytes=0-10", 10, 0, 0, false},
		{"bytes=5-2", 10, 0, 0, false},
		{"bytes=2-", 10, 0, 0, false},
		{"bytes=-2", 10, 0, 0, false},
		{"2-5", 10, 0, 0, false},
		{"bytes=a-b", 10, 0, 0, false},
		{"bytes=0-0", 0, 0, 0, false},
	}
	for _, tt := range tests {
		first, n, ok := parseCopyRange(tt.spec, tt.size)
		if first != tt.wantFirst || n != tt.wantN || ok != tt.wantOK {
			t.Errorf("parseCopyRange(%q, %d) = %d, %d, %v; want %d, %d, %v", tt.spec, tt.size, first, n, ok, tt.wantFirst, tt.wantN, tt.wantOK)
		}
	}
}

func TestPartWritesRejectSSEC(t *testing.T) {
	srv, _ := storeServer(t, "")
	sendWith(t, srv, http.MethodPut, "/bkt", "", nil)
	sendWith(t, srv, http.MethodPut, "/bkt/src", "data", nil)
	_, _, body := sendWith(t, srv, http.MethodPost, "/bkt/dst?uploads", "", nil)
	var created struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	part := "/bkt/dst?partNumber=1&uploadId=" + created.UploadID
	tests := []struct {
		name   string
		header map[string]string
	}{
		{"UploadPart with a customer key", map[string]string{"x-amz-server-side-encryption-customer-algorithm": "AES256"}},
		{"UploadPartCopy with a customer key", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-server-side-encryption-customer-algorithm": "AES256"}},
		{"UploadPartCopy with a source key", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-copy-source-server-side-encryption-customer-algorithm": "AES256"}},
	}
	for _, tt := range tests {
		if status, code, _ := sendWith(t, srv, http.MethodPut, part, "data", tt.header); status != http.StatusForbidden || code != "AccessDenied" {
			t.Errorf("%s: PUT %s = %d %q, want 403 AccessDenied", tt.name, part, status, code)
		}
	}
}
