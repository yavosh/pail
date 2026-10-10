package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestParseTarget(t *testing.T) {
	tests := []struct {
		name   string
		host   string
		path   string
		domain string
		want   target
	}{
		{"service", "127.0.0.1:9000", "/", "", target{}},
		{"bucket", "127.0.0.1:9000", "/bkt", "", target{bucket: "bkt"}},
		{"bucket trailing slash", "127.0.0.1:9000", "/bkt/", "", target{bucket: "bkt"}},
		{"key", "127.0.0.1:9000", "/bkt/a/b.txt", "", target{bucket: "bkt", key: "a/b.txt"}},
		{"double slash", "127.0.0.1:9000", "/bkt/a//b", "", target{bucket: "bkt", key: "a//b"}},
		{"dot dot", "127.0.0.1:9000", "/bkt/../x", "", target{bucket: "bkt", key: "../x"}},
		{"encoded slash", "127.0.0.1:9000", "/bkt/a%2Fb", "", target{bucket: "bkt", key: "a/b"}},
		{"plus", "127.0.0.1:9000", "/bkt/a+b", "", target{bucket: "bkt", key: "a+b"}},
		{"space", "127.0.0.1:9000", "/bkt/a%20b", "", target{bucket: "bkt", key: "a b"}},
		{"unicode", "127.0.0.1:9000", "/bkt/%E2%9C%93", "", target{bucket: "bkt", key: "✓"}},
		{"folder marker", "127.0.0.1:9000", "/bkt/dir/", "", target{bucket: "bkt", key: "dir/"}},
		{"virtual host", "bkt.localhost:9000", "/a//b", "localhost", target{bucket: "bkt", key: "a//b", virtualHost: true}},
		{"virtual host no port", "bkt.localhost", "/k", "localhost", target{bucket: "bkt", key: "k", virtualHost: true}},
		{"virtual host bucket only", "bkt.localhost", "/", "localhost", target{bucket: "bkt", virtualHost: true}},
		{"virtual host dotted bucket", "my.bkt.localhost", "/k", "localhost", target{bucket: "my.bkt", key: "k", virtualHost: true}},
		{"virtual host uppercase", "BKT.LocalHost:9000", "/k", "localhost", target{bucket: "bkt", key: "k", virtualHost: true}},
		{"host equals domain", "localhost:9000", "/bkt/k", "localhost", target{bucket: "bkt", key: "k"}},
		{"ip host with domain", "127.0.0.1:9000", "/bkt/k", "localhost", target{bucket: "bkt", key: "k"}},
		{"domain off", "bkt.localhost:9000", "/b2/k", "", target{bucket: "b2", key: "k"}},
		{"virtual host trailing dot", "bkt.localhost.:9000", "/k", "localhost", target{bucket: "bkt", key: "k", virtualHost: true}},
		{"empty bucket segment", "127.0.0.1:9000", "//k", "", target{key: "k"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+tt.host+tt.path, nil)
			if got := parseTarget(r, tt.domain); got != tt.want {
				t.Errorf("parseTarget(host %q, path %q, domain %q) = %+v, want %+v", tt.host, tt.path, tt.domain, got, tt.want)
			}
		})
	}
}

func TestNormalizeDomain(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"localhost", "localhost"},
		{".localhost", "localhost"},
		{"localhost.", "localhost"},
		{"LocalHost:9000", "localhost"},
		{"s3.example.com", "s3.example.com"},
	}
	for _, tt := range tests {
		if got := normalizeDomain(tt.in); got != tt.want {
			t.Errorf("normalizeDomain(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestResolve(t *testing.T) {
	bucket := target{bucket: "bkt"}
	object := target{bucket: "bkt", key: "k"}
	copySrc := http.Header{"X-Amz-Copy-Source": {"/src/k"}}
	tests := []struct {
		method string
		t      target
		query  string
		header http.Header
		want   operation
	}{
		{http.MethodGet, target{}, "", nil, opListBuckets},
		{http.MethodPut, target{}, "", nil, ""},
		{http.MethodGet, target{key: "k"}, "", nil, ""},
		{http.MethodPut, bucket, "", nil, opCreateBucket},
		{http.MethodHead, bucket, "", nil, opHeadBucket},
		{http.MethodDelete, bucket, "", nil, opDeleteBucket},
		{http.MethodGet, bucket, "location", nil, opGetBucketLocation},
		{http.MethodGet, bucket, "uploads", nil, opListMultipartUploads},
		{http.MethodGet, bucket, "list-type=2&prefix=a", nil, opListObjectsV2},
		{http.MethodGet, bucket, "prefix=a&marker=b", nil, opListObjects},
		{http.MethodPost, bucket, "delete", nil, opDeleteObjects},
		{http.MethodGet, bucket, "tagging", nil, ""},
		{http.MethodGet, bucket, "versioning", nil, ""},
		{http.MethodPut, bucket, "acl", nil, opPutBucketACL},
		{http.MethodGet, bucket, "location&tagging", nil, ""},
		{http.MethodPut, object, "", nil, opPutObject},
		{http.MethodPut, object, "x-id=PutObject", nil, opPutObject},
		{http.MethodPut, object, "", copySrc, opCopyObject},
		{http.MethodPut, object, "partNumber=1&uploadId=u", nil, opUploadPart},
		{http.MethodPut, object, "partNumber=1&uploadId=u", copySrc, opUploadPartCopy},
		{http.MethodPut, object, "uploadId=u", nil, ""},
		{http.MethodPut, object, "partNumber=1", nil, ""},
		{http.MethodPut, object, "tagging", nil, ""},
		{http.MethodGet, object, "", nil, opGetObject},
		{http.MethodGet, object, "response-content-type=text%2Fplain", nil, opGetObject},
		{http.MethodGet, object, "partNumber=1", nil, ""},
		{http.MethodGet, object, "uploadId=u", nil, opListParts},
		{http.MethodGet, object, "acl", nil, opGetObjectACL},
		{http.MethodGet, object, "versionId=v", nil, opGetObject},
		{http.MethodGet, object, "versionId=null", nil, opGetObject},
		{http.MethodGet, object, "acl&versionId=null", nil, opGetObjectACL},
		{http.MethodHead, object, "versionId=null", nil, opHeadObject},
		{http.MethodDelete, object, "versionId=null", nil, opDeleteObject},
		{http.MethodGet, object, "uploadId=u&versionId=null", nil, ""},
		{http.MethodPut, object, "versionId=null", nil, ""},
		{http.MethodGet, object, "tagging&versionId=null", nil, ""},
		{http.MethodGet, bucket, "versionId=null", nil, ""},
		{http.MethodHead, object, "", nil, opHeadObject},
		{http.MethodHead, object, "partNumber=1", nil, ""},
		{http.MethodDelete, object, "", nil, opDeleteObject},
		{http.MethodDelete, object, "uploadId=u", nil, opAbortMultipartUpload},
		{http.MethodPost, object, "uploads", nil, opCreateMultipartUpload},
		{http.MethodPost, object, "uploadId=u", nil, opCompleteMultipartUpload},
		{http.MethodPost, object, "", nil, ""},
		{http.MethodPatch, object, "", nil, ""},
	}
	for _, tt := range tests {
		r := httptest.NewRequestWithContext(context.Background(), tt.method, "http://h/?"+tt.query, nil)
		if got := resolve(tt.method, tt.t, r.URL.Query(), tt.header); got != tt.want {
			t.Errorf("resolve(%s, %+v, %q, %v) = %q, want %q", tt.method, tt.t, tt.query, tt.header, got, tt.want)
		}
	}
}

func TestCheckVersionID(t *testing.T) {
	tests := []struct {
		name  string
		op    operation
		query string
		want  bool
	}{
		{"no versionId", opGetObject, "", true},
		{"null", opGetObject, "versionId=null", true},
		{"null twice", opDeleteObject, "versionId=null&versionId=null", true},
		{"bogus on get", opGetObject, "versionId=bogus", false},
		{"bogus on head", opHeadObject, "versionId=bogus", false},
		{"bogus on delete", opDeleteObject, "versionId=bogus", false},
		{"bogus on acl", opGetObjectACL, "versionId=bogus", false},
		{"null then bogus", opGetObject, "versionId=null&versionId=bogus", false},
		{"empty", opGetObject, "versionId=", false},
		{"case differs", opGetObject, "versionId=NULL", false},
		{"unversioned operation", opListObjects, "versionId=bogus", true},
	}
	for _, tt := range tests {
		q, _ := url.ParseQuery(tt.query)
		apiErr, got := checkVersionID(tt.op, q)
		if got != tt.want || !got && apiErr.Code != "InvalidArgument" {
			t.Errorf("%s: checkVersionID(%s, %q) = %q, %v, want ok=%v with InvalidArgument", tt.name, tt.op, tt.query, apiErr.Code, got, tt.want)
		}
	}
}
