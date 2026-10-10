package diff

import (
	"fmt"
	"maps"
	"net/http"
)

// uploadPartCopyScenario records UploadPartCopy: a whole source, a byte range,
// and its errors.
func uploadPartCopyScenario() scenario {
	const upload = "uploadId={uploadId}"
	copyPart := func(name string, n int, extra map[string]string) step {
		h := map[string]string{"x-amz-copy-source": "{bucket}/src"}
		maps.Copy(h, extra)
		return step{name: name, method: http.MethodPut, key: "dst", query: fmt.Sprintf("partNumber=%d&%s", n, upload), header: h}
	}
	complete := `<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>` + etagOf("2345") + `</ETag></Part></CompleteMultipartUpload>`
	return scenario{name: "upload-part-copy", steps: []step{
		createBucket(),
		{name: "put-src", method: http.MethodPut, key: "src", body: "0123456789"},
		{name: "create-upload", method: http.MethodPost, key: "dst", query: "uploads"},
		copyPart("copy-part-full", 1, nil),
		copyPart("copy-part-range", 2, map[string]string{"x-amz-copy-source-range": "bytes=2-5"}),
		copyPart("copy-part-range-past-end", 3, map[string]string{"x-amz-copy-source-range": "bytes=5-100"}),
		copyPart("copy-part-range-malformed", 3, map[string]string{"x-amz-copy-source-range": "2-5"}),
		copyPart("copy-part-if-match-wrong", 3, map[string]string{"x-amz-copy-source-if-match": `"wrong"`}),
		{name: "copy-part-missing-source", method: http.MethodPut, key: "dst", query: "partNumber=3&" + upload, header: map[string]string{"x-amz-copy-source": "{bucket}/missing"}},
		{name: "copy-part-bad-upload", method: http.MethodPut, key: "dst", query: "partNumber=3&uploadId=bogus", header: map[string]string{"x-amz-copy-source": "{bucket}/src"}},
		{name: "list-parts", method: http.MethodGet, key: "dst", query: upload},
		{name: "complete-upload", method: http.MethodPost, key: "dst", query: upload, body: complete},
		{name: "get-dst", method: http.MethodGet, key: "dst"},
		{name: "delete-src", method: http.MethodDelete, key: "src"},
		{name: "delete-dst", method: http.MethodDelete, key: "dst"},
		deleteBucket(),
	}}
}
