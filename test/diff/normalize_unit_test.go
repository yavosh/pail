package diff

import (
	"net/http"
	"strings"
	"testing"
)

func TestNormalizeError(t *testing.T) {
	r := response{
		status: http.StatusNotFound,
		header: http.Header{
			"Content-Type":     {"application/xml"},
			"Content-Length":   {"312"},
			"X-Amz-Request-Id": {"ABC"},
			"Date":             {"Wed, 01 Oct 2026 10:00:00 GMT"},
			"Server":           {"AmazonS3"},
		},
		body: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message><BucketName>pail-diff-1</BucketName><RequestId>ABC</RequestId><HostId>xyz</HostId></Error>`),
	}
	got := normalize(step{name: "s", method: http.MethodGet}, "pail-diff-1", r)
	if got.Body != "Error\n  Code: NoSuchBucket\n" {
		t.Errorf("body = %q, want the code only", got.Body)
	}
	want := map[string]string{"Content-Type": "application/xml", "X-Amz-Request-Id": "<present>"}
	if len(got.Headers) != len(want) {
		t.Errorf("headers = %v, want %v", got.Headers, want)
	}
	for k, v := range want {
		if got.Headers[k] != v {
			t.Errorf("header %s = %q, want %q", k, got.Headers[k], v)
		}
	}
}

func TestNormalizeListing(t *testing.T) {
	r := response{
		status: http.StatusOK,
		header: http.Header{"Content-Type": {"application/xml"}},
		body: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>pail-diff-1</Name><KeyCount>1</KeyCount>
<Contents><Key>a b</Key><LastModified>2026-10-01T10:00:00.000Z</LastModified><ETag>&quot;e&quot;</ETag><Size>3</Size></Contents></ListBucketResult>`),
	}
	got := normalize(step{name: "s", method: http.MethodGet, query: "list-type=2"}, "pail-diff-1", r)
	want := `ListBucketResult
  Name: {bucket}
  KeyCount: 1
  Contents
    Key: a b
    LastModified: <volatile>
    ETag: "e"
    Size: 3
`
	if got.Body != want {
		t.Errorf("body =\n%s\nwant\n%s", got.Body, want)
	}
	if got.Request != "GET /?list-type=2" {
		t.Errorf("request = %q, want %q", got.Request, "GET /?list-type=2")
	}
}

func TestNormalizeObject(t *testing.T) {
	r := response{
		status: http.StatusOK,
		header: http.Header{
			"Etag":                         {`"abc"`},
			"Content-Length":               {"5"},
			"Last-Modified":                {"Wed, 01 Oct 2026 10:00:00 GMT"},
			"X-Amz-Meta-Color":             {"blue"},
			"X-Amz-Server-Side-Encryption": {"AES256"},
		},
		body: []byte("hello"),
	}
	got := normalize(step{name: "s", method: http.MethodGet, key: "k"}, "pail-diff-1", r)
	want := map[string]string{`Etag`: `"abc"`, "Content-Length": "5", "Last-Modified": "<present>", "X-Amz-Meta-Color": "blue"}
	if got.Body != "hello" || len(got.Headers) != len(want) {
		t.Errorf("normalize = %+v, want body hello and headers %v", got, want)
	}
	for k, v := range want {
		if got.Headers[k] != v {
			t.Errorf("header %s = %q, want %q", k, got.Headers[k], v)
		}
	}
}

func TestCompare(t *testing.T) {
	want := exchange{Step: "get", Status: 200, Headers: map[string]string{"Etag": `"a"`}, Body: "x"}
	got := exchange{Step: "get", Status: 404, Headers: map[string]string{"Content-Type": "text/plain"}, Body: "y"}
	diffs := compare(want, got)
	for _, key := range []string{"get status", "get header:Etag", "get header:Content-Type", "get body"} {
		if _, ok := diffs[key]; !ok {
			t.Errorf("compare missing %q in %v", key, diffs)
		}
	}
	if d := compare(want, want); len(d) != 0 {
		t.Errorf("compare(x, x) = %v, want none", d)
	}
}

func TestReadKnownDiffs(t *testing.T) {
	in := "# comment\n\nobject-basics/get header:Etag pail does not quote yet (#8)\n"
	known, err := readKnownDiffs(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if got := known["object-basics/get header:Etag"]; got != "pail does not quote yet (#8)" {
		t.Errorf("reason = %q, want the rest of the line", got)
	}
	for _, bad := range []string{"no-slash status reason\n", "a/b status\n"} {
		if _, err := readKnownDiffs(strings.NewReader(bad)); err == nil {
			t.Errorf("readKnownDiffs(%q) error = nil, want an error", bad)
		}
	}
}

func TestS3Escape(t *testing.T) {
	tests := []struct{ in, want string }{
		{"a/b.txt", "a/b.txt"},
		{"a b", "a%20b"},
		{"a+b", "a%2Bb"},
		{"✓", "%E2%9C%93"},
		{"a//b", "a//b"},
	}
	for _, tt := range tests {
		if got := s3Escape(tt.in); got != tt.want {
			t.Errorf("s3Escape(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
