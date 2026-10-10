package diff

import (
	"maps"
	"net/http"
	"strings"
)

// partReadsScenario records GET and HEAD with partNumber, and GetObjectAttributes.
func partReadsScenario() scenario {
	const upload = "uploadId={uploadId}"
	big := strings.Repeat("a", 5<<20)
	const tail = "tail"
	crc := func(body string) map[string]string {
		return map[string]string{"x-amz-checksum-crc32": crc32Base64(body)}
	}
	complete := `<CompleteMultipartUpload>` +
		`<Part><PartNumber>1</PartNumber><ETag>` + etagOf(big) + `</ETag><ChecksumCRC32>` + crc32Base64(big) + `</ChecksumCRC32></Part>` +
		`<Part><PartNumber>2</PartNumber><ETag>` + etagOf(tail) + `</ETag><ChecksumCRC32>` + crc32Base64(tail) + `</ChecksumCRC32></Part>` +
		`</CompleteMultipartUpload>`
	parts := []string{"X-Amz-Mp-Parts-Count"}
	attrs := func(name, key, list string, extra map[string]string) step {
		h := map[string]string{}
		if list != "" {
			h["x-amz-object-attributes"] = list
		}
		maps.Copy(h, extra)
		return step{name: name, method: http.MethodGet, key: key, query: "attributes", header: h}
	}
	const all = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize"
	return scenario{name: "part-reads-attributes", steps: []step{
		createBucket(),
		{name: "put-simple", method: http.MethodPut, key: "simple", body: "hello"},
		{name: "head-simple-part1", method: http.MethodHead, key: "simple", query: "partNumber=1", compare: parts},
		{name: "get-simple-part1", method: http.MethodGet, key: "simple", query: "partNumber=1", compare: parts},
		{name: "get-simple-part2", method: http.MethodGet, key: "simple", query: "partNumber=2", compare: parts},
		{name: "create-multi", method: http.MethodPost, key: "multi", query: "uploads", header: map[string]string{"x-amz-checksum-algorithm": "CRC32"}},
		{name: "put-part1", method: http.MethodPut, key: "multi", query: "partNumber=1&" + upload, body: big, header: crc(big)},
		{name: "put-part2", method: http.MethodPut, key: "multi", query: "partNumber=2&" + upload, body: tail, header: crc(tail)},
		{name: "complete-multi", method: http.MethodPost, key: "multi", query: upload, body: complete},
		{name: "head-multi-part1", method: http.MethodHead, key: "multi", query: "partNumber=1", compare: parts},
		{name: "get-multi-part2", method: http.MethodGet, key: "multi", query: "partNumber=2", compare: parts},
		{name: "head-multi-part3", method: http.MethodHead, key: "multi", query: "partNumber=3", compare: parts},
		{name: "get-multi-part-zero", method: http.MethodGet, key: "multi", query: "partNumber=0", compare: parts},
		{name: "get-part-with-range", method: http.MethodGet, key: "multi", query: "partNumber=2", header: map[string]string{"Range": "bytes=0-1"}, compare: parts},
		{name: "head-multi", method: http.MethodHead, key: "multi", compare: parts},
		attrs("attributes-simple", "simple", all, nil),
		attrs("attributes-multi", "multi", all, nil),
		attrs("attributes-multi-max-parts", "multi", "ObjectParts", map[string]string{"x-amz-max-parts": "1"}),
		attrs("attributes-multi-marker", "multi", "ObjectParts", map[string]string{"x-amz-part-number-marker": "1"}),
		attrs("attributes-no-header", "simple", "", nil),
		attrs("attributes-bogus", "simple", "Bogus", nil),
		attrs("attributes-missing-key", "missing", all, nil),
		{name: "delete-simple", method: http.MethodDelete, key: "simple"},
		{name: "delete-multi", method: http.MethodDelete, key: "multi"},
		deleteBucket(),
	}}
}
