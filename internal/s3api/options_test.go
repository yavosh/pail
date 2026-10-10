package s3api

import (
	"maps"
	"net/http"
	"strings"
	"testing"
)

func TestWriteOptionErrors(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	_ = call(t, srv, http.MethodPut, "/bkt/src", "x", nil)
	tests := []struct {
		name       string
		header     map[string]string
		wantStatus int
		wantCode   string
	}{
		{"no options", nil, 200, ""},
		{"sse aes256", map[string]string{"x-amz-server-side-encryption": "AES256"}, 200, ""},
		{"sse kms", map[string]string{"x-amz-server-side-encryption": "aws:kms"}, 200, ""},
		{"sse kms dsse", map[string]string{"x-amz-server-side-encryption": "aws:kms:dsse"}, 200, ""},
		{"sse unknown", map[string]string{"x-amz-server-side-encryption": "bogus"}, 400, "InvalidArgument"},
		{"sse lower case", map[string]string{"x-amz-server-side-encryption": "aes256"}, 400, "InvalidArgument"},
		{"sse-c algorithm", map[string]string{"x-amz-server-side-encryption-customer-algorithm": "AES256"}, 403, "AccessDenied"},
		{"sse-c key", map[string]string{"x-amz-server-side-encryption-customer-key": "a2V5"}, 403, "AccessDenied"},
		{"sse-c copy source", map[string]string{"x-amz-copy-source-server-side-encryption-customer-algorithm": "AES256"}, 403, "AccessDenied"},
		{"object lock mode", map[string]string{"x-amz-object-lock-mode": "GOVERNANCE"}, 400, "InvalidRequest"},
		{"object lock retain until", map[string]string{"x-amz-object-lock-retain-until-date": "2030-01-01T00:00:00Z"}, 400, "InvalidRequest"},
		{"object lock legal hold", map[string]string{"x-amz-object-lock-legal-hold": "ON"}, 400, "InvalidRequest"},
		{"storage class standard", map[string]string{"x-amz-storage-class": "STANDARD"}, 200, ""},
		{"storage class deep archive", map[string]string{"x-amz-storage-class": "DEEP_ARCHIVE"}, 200, ""},
		{"storage class glacier ir", map[string]string{"x-amz-storage-class": "GLACIER_IR"}, 200, ""},
		{"storage class unknown", map[string]string{"x-amz-storage-class": "BOGUS"}, 400, "InvalidStorageClass"},
		{"storage class lower case", map[string]string{"x-amz-storage-class": "standard_ia"}, 400, "InvalidStorageClass"},
		{"redirect path", map[string]string{"x-amz-website-redirect-location": "/target"}, 200, ""},
		{"redirect http", map[string]string{"x-amz-website-redirect-location": "http://example.com/x"}, 200, ""},
		{"redirect https", map[string]string{"x-amz-website-redirect-location": "https://example.com/x"}, 200, ""},
		{"redirect relative", map[string]string{"x-amz-website-redirect-location": "target"}, 400, "InvalidRedirectLocation"},
		{"redirect ftp", map[string]string{"x-amz-website-redirect-location": "ftp://example.com/x"}, 400, "InvalidRedirectLocation"},
	}
	ops := []struct {
		name, method, target string
		copySource           bool
	}{
		{"PutObject", http.MethodPut, "/bkt/dst", false},
		{"CopyObject", http.MethodPut, "/bkt/dst", true},
		{"CreateMultipartUpload", http.MethodPost, "/bkt/dst?uploads", false},
	}
	for _, op := range ops {
		for _, tt := range tests {
			header := map[string]string{}
			maps.Copy(header, tt.header)
			if op.copySource {
				header["x-amz-copy-source"] = "bkt/src"
			}
			r := call(t, srv, op.method, op.target, "", header)
			if r.status != tt.wantStatus || r.code != tt.wantCode {
				t.Errorf("%s with %s: %s = %d %q, want %d %q", op.name, tt.name, op.method, r.status, r.code, tt.wantStatus, tt.wantCode)
			}
		}
	}
}

func TestCreateBucketObjectLock(t *testing.T) {
	srv, _ := storeServer(t, "")
	tests := []struct {
		name, bucket, value string
		wantStatus          int
		wantCode            string
	}{
		{"enabled", "lock-on", "true", 501, "NotImplemented"},
		{"enabled in upper case", "lock-upper", "TRUE", 501, "NotImplemented"},
		{"disabled", "lock-off", "false", 200, ""},
	}
	for _, tt := range tests {
		r := call(t, srv, http.MethodPut, "/"+tt.bucket, "", map[string]string{"x-amz-bucket-object-lock-enabled": tt.value})
		if r.status != tt.wantStatus || r.code != tt.wantCode {
			t.Errorf("CreateBucket with object lock %s = %d %q, want %d %q", tt.name, r.status, r.code, tt.wantStatus, tt.wantCode)
		}
	}
}

func TestStoredOptions(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	puts := []struct {
		key    string
		header map[string]string
		want   http.Header // the headers of the PutObject response and of HEAD
	}{
		{"plain", nil, http.Header{"X-Amz-Server-Side-Encryption": {"AES256"}, "X-Amz-Storage-Class": {""}}},
		{"kms", map[string]string{"x-amz-server-side-encryption": "aws:kms"}, http.Header{"X-Amz-Server-Side-Encryption": {"aws:kms"}}},
		{"standard", map[string]string{"x-amz-storage-class": "STANDARD"}, http.Header{"X-Amz-Storage-Class": {""}}},
		{"ia", map[string]string{"x-amz-storage-class": "STANDARD_IA"}, http.Header{"X-Amz-Storage-Class": {"STANDARD_IA"}}},
		{"redirect", map[string]string{"x-amz-website-redirect-location": "/target"}, http.Header{"X-Amz-Website-Redirect-Location": {"/target"}}},
	}
	for _, tt := range puts {
		put := call(t, srv, http.MethodPut, "/bkt/"+tt.key, "data", tt.header)
		if put.status != 200 {
			t.Fatalf("PUT %s = %d %q, want 200", tt.key, put.status, put.code)
		}
		if got := put.header.Get("x-amz-website-redirect-location"); got != "" {
			t.Errorf("PUT %s echoed the website redirect %q, want none", tt.key, got)
		}
		for _, method := range []string{http.MethodHead, http.MethodGet} {
			r := call(t, srv, method, "/bkt/"+tt.key, "", nil)
			for name, want := range tt.want {
				if got := r.header.Get(name); got != want[0] {
					t.Errorf("%s %s header %s = %q, want %q", method, tt.key, name, got, want[0])
				}
			}
		}
		for name, want := range tt.want {
			if name != "X-Amz-Website-Redirect-Location" && put.header.Get(name) != want[0] {
				t.Errorf("PUT %s header %s = %q, want %q", tt.key, name, put.header.Get(name), want[0])
			}
		}
	}
}

func TestArchivedObjects(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	tests := []struct {
		class                  string
		wantGet, wantCopy      int
		wantGetCode, wantCopyC string
	}{
		{"STANDARD_IA", 200, 200, "", ""},
		{"GLACIER_IR", 200, 200, "", ""},
		{"GLACIER", 403, 403, "InvalidObjectState", "InvalidObjectState"},
		{"DEEP_ARCHIVE", 403, 403, "InvalidObjectState", "InvalidObjectState"},
	}
	for _, tt := range tests {
		key := strings.ToLower(tt.class)
		_ = call(t, srv, http.MethodPut, "/bkt/"+key, "data", map[string]string{"x-amz-storage-class": tt.class})
		if r := call(t, srv, http.MethodHead, "/bkt/"+key, "", nil); r.status != 200 || r.header.Get("x-amz-storage-class") != tt.class {
			t.Errorf("HEAD %s = %d, class %q, want 200 and %s", tt.class, r.status, r.header.Get("x-amz-storage-class"), tt.class)
		}
		if r := call(t, srv, http.MethodGet, "/bkt/"+key, "", nil); r.status != tt.wantGet || r.code != tt.wantGetCode {
			t.Errorf("GET %s = %d %q, want %d %q", tt.class, r.status, r.code, tt.wantGet, tt.wantGetCode)
		}
		r := call(t, srv, http.MethodPut, "/bkt/copy-"+key, "", map[string]string{"x-amz-copy-source": "bkt/" + key})
		if r.status != tt.wantCopy || r.code != tt.wantCopyC {
			t.Errorf("CopyObject from %s = %d %q, want %d %q", tt.class, r.status, r.code, tt.wantCopy, tt.wantCopyC)
		}
	}
}

func TestCopyOptions(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	_ = call(t, srv, http.MethodPut, "/bkt/src", "data", map[string]string{
		"x-amz-storage-class": "STANDARD_IA", "x-amz-server-side-encryption": "aws:kms", "x-amz-website-redirect-location": "/target",
	})
	tests := []struct {
		name                                    string
		dst                                     string
		header                                  map[string]string
		wantStatus                              int
		wantEncryption, wantClass, wantRedirect string
	}{
		{"keeps nothing by default", "d1", nil, 200, "AES256", "", ""},
		{"takes the request options", "d2", map[string]string{"x-amz-storage-class": "ONEZONE_IA", "x-amz-server-side-encryption": "AES256"}, 200, "AES256", "ONEZONE_IA", ""},
		{"replace takes the redirect", "d3", map[string]string{"x-amz-metadata-directive": "REPLACE", "x-amz-website-redirect-location": "/other"}, 200, "AES256", "", "/other"},
		{"copy directive keeps the request redirect", "d4", map[string]string{"x-amz-website-redirect-location": "/other"}, 200, "AES256", "", "/other"},
		{"onto itself without changes", "src", nil, 400, "", "", ""},
		{"onto itself with a new class", "src", map[string]string{"x-amz-storage-class": "STANDARD"}, 200, "AES256", "", ""},
		{"onto itself with a new redirect", "src", map[string]string{"x-amz-website-redirect-location": "/again"}, 200, "AES256", "", "/again"},
	}
	for _, tt := range tests {
		header := map[string]string{"x-amz-copy-source": "bkt/src"}
		maps.Copy(header, tt.header)
		r := call(t, srv, http.MethodPut, "/bkt/"+tt.dst, "", header)
		if r.status != tt.wantStatus {
			t.Errorf("%s: CopyObject = %d %q, want %d", tt.name, r.status, r.code, tt.wantStatus)
			continue
		}
		if tt.wantStatus != 200 {
			continue
		}
		if got := r.header.Get("x-amz-server-side-encryption"); got != tt.wantEncryption {
			t.Errorf("%s: CopyObject encryption = %q, want %q", tt.name, got, tt.wantEncryption)
		}
		h := call(t, srv, http.MethodHead, "/bkt/"+tt.dst, "", nil)
		if h.header.Get("x-amz-server-side-encryption") != tt.wantEncryption || h.header.Get("x-amz-storage-class") != tt.wantClass || h.header.Get("x-amz-website-redirect-location") != tt.wantRedirect {
			t.Errorf("%s: HEAD = encryption %q, class %q, redirect %q; want %q, %q, %q", tt.name,
				h.header.Get("x-amz-server-side-encryption"), h.header.Get("x-amz-storage-class"), h.header.Get("x-amz-website-redirect-location"),
				tt.wantEncryption, tt.wantClass, tt.wantRedirect)
		}
	}
}

func TestListingsShowStorageClass(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	_ = call(t, srv, http.MethodPut, "/bkt/plain", "data", nil)
	_ = call(t, srv, http.MethodPut, "/bkt/cold", "data", map[string]string{"x-amz-storage-class": "GLACIER"})
	for _, query := range []string{"?list-type=2", ""} {
		r := call(t, srv, http.MethodGet, "/bkt"+query, "", nil)
		if !strings.Contains(r.body, "<Key>cold</Key>") || strings.Count(r.body, "<StorageClass>GLACIER</StorageClass>") != 1 || strings.Count(r.body, "<StorageClass>STANDARD</StorageClass>") != 1 {
			t.Errorf("list /bkt%s = %s, want one GLACIER and one STANDARD object", query, r.body)
		}
	}
}

func TestMultipartOptions(t *testing.T) {
	srv, _ := storeServer(t, "")
	id := startUpload(t, srv, "k", map[string]string{
		"x-amz-storage-class": "STANDARD_IA", "x-amz-server-side-encryption": "aws:kms", "x-amz-website-redirect-location": "/target",
	})
	plain := startUpload(t, srv, "plain", nil)
	if r := call(t, srv, http.MethodPost, "/bkt/plain2?uploads", "", nil); r.header.Get("x-amz-server-side-encryption") != "AES256" {
		t.Errorf("CreateMultipartUpload without encryption returned %q, want AES256", r.header.Get("x-amz-server-side-encryption"))
	}
	if r := call(t, srv, http.MethodPost, "/bkt/k2?uploads", "", map[string]string{"x-amz-server-side-encryption": "AES256"}); r.header.Get("x-amz-server-side-encryption") != "AES256" {
		t.Errorf("CreateMultipartUpload with encryption echoed %q, want AES256", r.header.Get("x-amz-server-side-encryption"))
	}

	part := putPart(t, srv, "k", id, 1, "data", nil)
	if part.header.Get("x-amz-server-side-encryption") != "aws:kms" || part.header.Get("x-amz-storage-class") != "STANDARD_IA" {
		t.Errorf("UploadPart headers = encryption %q, class %q, want aws:kms and STANDARD_IA", part.header.Get("x-amz-server-side-encryption"), part.header.Get("x-amz-storage-class"))
	}
	plainPart := putPart(t, srv, "plain", plain, 1, "data", nil)
	if plainPart.header.Get("x-amz-server-side-encryption") != "AES256" || plainPart.header.Get("x-amz-storage-class") != "" {
		t.Errorf("UploadPart headers without options = encryption %q, class %q, want AES256 and none", plainPart.header.Get("x-amz-server-side-encryption"), plainPart.header.Get("x-amz-storage-class"))
	}
	if r := call(t, srv, http.MethodGet, "/bkt?uploads", "", nil); !strings.Contains(r.body, "<StorageClass>STANDARD_IA</StorageClass>") {
		t.Errorf("ListMultipartUploads = %s, want a STANDARD_IA upload", r.body)
	}
	if r := call(t, srv, http.MethodGet, "/bkt/k?uploadId="+id, "", nil); !strings.Contains(r.body, "<StorageClass>STANDARD_IA</StorageClass>") {
		t.Errorf("ListParts = %s, want StorageClass STANDARD_IA", r.body)
	}

	etag := part.header.Get("ETag")
	done := call(t, srv, http.MethodPost, "/bkt/k?uploadId="+id, "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"+etag+"</ETag></Part></CompleteMultipartUpload>", nil)
	if done.status != 200 || done.header.Get("x-amz-server-side-encryption") != "aws:kms" {
		t.Errorf("CompleteMultipartUpload = %d, encryption %q, want 200 and aws:kms", done.status, done.header.Get("x-amz-server-side-encryption"))
	}
	head := call(t, srv, http.MethodHead, "/bkt/k", "", nil)
	if head.header.Get("x-amz-server-side-encryption") != "aws:kms" || head.header.Get("x-amz-storage-class") != "STANDARD_IA" || head.header.Get("x-amz-website-redirect-location") != "/target" {
		t.Errorf("HEAD after complete = encryption %q, class %q, redirect %q; want aws:kms, STANDARD_IA, /target",
			head.header.Get("x-amz-server-side-encryption"), head.header.Get("x-amz-storage-class"), head.header.Get("x-amz-website-redirect-location"))
	}
}
