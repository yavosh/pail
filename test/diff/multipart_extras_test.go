package diff

import "net/http"

// multipartExtrasScenario records x-amz-mp-object-size, encoding-type on
// ListMultipartUploads, and versionId=null on an unversioned bucket.
func multipartExtrasScenario() scenario {
	const upload = "uploadId={uploadId}"
	complete := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etagOf("abc") + `</ETag></Part></CompleteMultipartUpload>`
	completeWith := func(name, size string) step {
		return step{name: name, method: http.MethodPost, key: "sized", query: upload, body: complete, header: map[string]string{"x-amz-mp-object-size": size}}
	}
	const odd = "odd key+&=%"
	return scenario{name: "multipart-extras", steps: []step{
		createBucket(),
		{name: "create-sized", method: http.MethodPost, key: "sized", query: "uploads"},
		{name: "put-part", method: http.MethodPut, key: "sized", query: "partNumber=1&" + upload, body: "abc"},
		completeWith("complete-wrong-size", "999"),
		completeWith("complete-bad-size", "abc"),
		completeWith("complete-right-size", "3"),
		{name: "head-sized", method: http.MethodHead, key: "sized"},
		{name: "create-odd", method: http.MethodPost, key: odd, query: "uploads"},
		{name: "list-uploads-url", method: http.MethodGet, query: "encoding-type=url&uploads"},
		{name: "list-uploads-plain", method: http.MethodGet, query: "uploads"},
		{name: "list-uploads-bad-encoding", method: http.MethodGet, query: "encoding-type=bogus&uploads"},
		{name: "abort-odd", method: http.MethodDelete, key: odd, query: upload},
		{name: "put-plain", method: http.MethodPut, key: "plain", body: "plain"},
		{name: "get-null-version", method: http.MethodGet, key: "plain", query: "versionId=null"},
		{name: "head-null-version", method: http.MethodHead, key: "plain", query: "versionId=null"},
		{name: "get-bogus-version", method: http.MethodGet, key: "plain", query: "versionId=bogus"},
		{name: "get-null-acl", method: http.MethodGet, key: "plain", query: "acl&versionId=null"},
		{name: "delete-null-version", method: http.MethodDelete, key: "plain", query: "versionId=null"},
		{name: "get-after-null-delete", method: http.MethodGet, key: "plain"},
		{name: "delete-sized", method: http.MethodDelete, key: "sized"},
		deleteBucket(),
	}}
}
