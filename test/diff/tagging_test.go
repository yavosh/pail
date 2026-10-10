package diff

import (
	"fmt"
	"maps"
	"net/http"
	"strings"
)

// taggingScenario records object and bucket tagging, tags on writes and
// copies, the tag count header, and the tag validation errors.
func taggingScenario() scenario {
	const upload = "uploadId={uploadId}"
	tagging := func(pairs ...string) string {
		var b strings.Builder
		b.WriteString("<Tagging><TagSet>")
		for i := 0; i < len(pairs); i += 2 {
			b.WriteString("<Tag><Key>" + pairs[i] + "</Key><Value>" + pairs[i+1] + "</Value></Tag>")
		}
		b.WriteString("</TagSet></Tagging>")
		return b.String()
	}
	putTags := func(name, key, body string) step {
		return step{name: name, method: http.MethodPut, key: key, query: "tagging", body: body, header: map[string]string{"Content-MD5": md5Base64(body)}}
	}
	count := []string{"X-Amz-Tagging-Count"}
	var eleven []string
	for i := range 11 {
		eleven = append(eleven, fmt.Sprint("k", i), "v")
	}
	copyTo := func(name, key string, extra map[string]string) step {
		h := map[string]string{"x-amz-copy-source": "{bucket}/k"}
		maps.Copy(h, extra)
		return step{name: name, method: http.MethodPut, key: key, header: h}
	}
	complete := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etagOf("mpu") + `</ETag></Part></CompleteMultipartUpload>`
	bucketTags := tagging("team", "storage")
	return scenario{name: "object-tagging", steps: []step{
		createBucket(),
		{name: "put-with-tagging", method: http.MethodPut, key: "k", body: "x", header: map[string]string{"x-amz-tagging": "color=blue&size=large"}},
		{name: "head-tag-count", method: http.MethodHead, key: "k", compare: count},
		{name: "get-tag-count", method: http.MethodGet, key: "k", compare: count},
		{name: "get-tagging", method: http.MethodGet, key: "k", query: "tagging"},
		putTags("put-tagging", "k", tagging("a", "1", "b", "")),
		{name: "get-tagging-after", method: http.MethodGet, key: "k", query: "tagging"},
		putTags("put-tagging-too-many", "k", tagging(eleven...)),
		putTags("put-tagging-duplicate", "k", tagging("a", "1", "a", "2")),
		putTags("put-tagging-aws-prefix", "k", tagging("aws:x", "1")),
		putTags("put-tagging-long-key", "k", tagging(strings.Repeat("k", 129), "1")),
		putTags("put-tagging-long-value", "k", tagging("k", strings.Repeat("v", 257))),
		{name: "put-tagging-no-md5", method: http.MethodPut, key: "k", query: "tagging", body: tagging("c", "3")},
		putTags("put-tagging-malformed", "k", "<Tagging>"),
		{name: "put-header-aws-prefix", method: http.MethodPut, key: "bad", body: "x", header: map[string]string{"x-amz-tagging": "aws:k=v"}},
		{name: "put-header-no-value", method: http.MethodPut, key: "novalue", body: "x", header: map[string]string{"x-amz-tagging": "a"}},
		{name: "get-tagging-novalue", method: http.MethodGet, key: "novalue", query: "tagging"},
		copyTo("copy-default", "k2", nil),
		{name: "get-tagging-copy", method: http.MethodGet, key: "k2", query: "tagging"},
		copyTo("copy-replace", "k3", map[string]string{"x-amz-tagging-directive": "REPLACE", "x-amz-tagging": "x=y"}),
		{name: "get-tagging-replace", method: http.MethodGet, key: "k3", query: "tagging"},
		copyTo("copy-bad-directive", "k4", map[string]string{"x-amz-tagging-directive": "BOGUS"}),
		{name: "delete-tagging", method: http.MethodDelete, key: "k", query: "tagging"},
		{name: "get-tagging-deleted", method: http.MethodGet, key: "k", query: "tagging"},
		{name: "head-after-delete-tagging", method: http.MethodHead, key: "k", compare: count},
		{name: "get-tagging-missing-key", method: http.MethodGet, key: "missing", query: "tagging"},
		{name: "create-upload-tagged", method: http.MethodPost, key: "mpu", query: "uploads", header: map[string]string{"x-amz-tagging": "m=1"}},
		{name: "put-part", method: http.MethodPut, key: "mpu", query: "partNumber=1&" + upload, body: "mpu"},
		{name: "complete-upload", method: http.MethodPost, key: "mpu", query: upload, body: complete},
		{name: "get-tagging-upload", method: http.MethodGet, key: "mpu", query: "tagging"},
		{name: "get-bucket-tagging-none", method: http.MethodGet, query: "tagging"},
		{name: "put-bucket-tagging", method: http.MethodPut, query: "tagging", body: bucketTags, header: map[string]string{"Content-MD5": md5Base64(bucketTags)}},
		{name: "get-bucket-tagging", method: http.MethodGet, query: "tagging"},
		{name: "delete-bucket-tagging", method: http.MethodDelete, query: "tagging"},
		{name: "get-bucket-tagging-deleted", method: http.MethodGet, query: "tagging"},
		{name: "delete-objects", method: http.MethodPost, query: "delete", body: deleteBody("k", "k2", "k3", "novalue", "mpu"),
			header: map[string]string{"Content-MD5": md5Base64(deleteBody("k", "k2", "k3", "novalue", "mpu"))}},
		deleteBucket(),
	}}
}

// deleteBody is a DeleteObjects body for keys.
func deleteBody(keys ...string) string {
	var b strings.Builder
	b.WriteString("<Delete>")
	for _, k := range keys {
		b.WriteString("<Object><Key>" + k + "</Key></Object>")
	}
	b.WriteString("</Delete>")
	return b.String()
}
