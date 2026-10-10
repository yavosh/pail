package diff

import (
	"crypto/md5"
	"crypto/sha512"
	"encoding/base64"
	"maps"
	"net/http"
	"strings"
)

// optionHeaders are the object option headers that objectOptionsScenario compares.
var optionHeaders = []string{
	"X-Amz-Server-Side-Encryption", "X-Amz-Server-Side-Encryption-Customer-Algorithm",
	"X-Amz-Storage-Class", "X-Amz-Website-Redirect-Location",
	"X-Amz-Checksum-Sha512", "X-Amz-Checksum-Md5",
	"X-Amz-Checksum-Xxhash64", "X-Amz-Checksum-Xxhash3", "X-Amz-Checksum-Xxhash128",
}

// objectOptionsScenario records the write options that pail used to ignore:
// encryption, Object Lock, newer checksum algorithms, storage classes, and
// website redirects (findings C and D, and the section 2 metadata gaps).
func objectOptionsScenario() scenario {
	const body = "hello options"
	sha512Sum := sha512.Sum512([]byte(body))
	customerKey := []byte(strings.Repeat("k", 32))
	customerKeyMD5 := md5.Sum(customerKey)
	zeros := func(n int) string { return base64.StdEncoding.EncodeToString(make([]byte, n)) }
	put := func(name, key string, header map[string]string) step {
		return step{name: name, method: http.MethodPut, key: key, body: body, header: header, compare: optionHeaders}
	}
	read := func(name, method, key string) step {
		return step{name: name, method: method, key: key, compare: optionHeaders}
	}
	copyOf := func(name, key, source string, extra map[string]string) step {
		h := map[string]string{"x-amz-copy-source": "{bucket}/" + source}
		maps.Copy(h, extra)
		return step{name: name, method: http.MethodPut, key: key, header: h, compare: optionHeaders}
	}
	keys := []string{"sse", "plain", "upload", "ssec", "sha512", "md5sum", "ia", "glacier", "redirect", "copy-ia", "copy-redirect"}
	var batch strings.Builder
	batch.WriteString("<Delete>")
	for _, k := range keys {
		batch.WriteString("<Object><Key>" + k + "</Key></Object>")
	}
	batch.WriteString("</Delete>")
	const upload = "uploadId={uploadId}"
	return scenario{name: "object-options", steps: []step{
		createBucket(),
		// Server-side encryption.
		put("put-sse-s3", "sse", map[string]string{"x-amz-server-side-encryption": "AES256"}),
		read("head-sse-s3", http.MethodHead, "sse"),
		put("put-default", "plain", nil),
		read("get-default", http.MethodGet, "plain"),
		put("put-sse-kms", "kms", map[string]string{"x-amz-server-side-encryption": "aws:kms"}),
		// An SSE-KMS ETag is not an MD5 digest, so the listing below must not include it.
		{name: "delete-kms", method: http.MethodDelete, key: "kms"},
		put("put-sse-bogus", "bogus", map[string]string{"x-amz-server-side-encryption": "bogus"}),
		put("put-sse-c", "ssec", map[string]string{
			"x-amz-server-side-encryption-customer-algorithm": "AES256",
			"x-amz-server-side-encryption-customer-key":       base64.StdEncoding.EncodeToString(customerKey),
			"x-amz-server-side-encryption-customer-key-MD5":   base64.StdEncoding.EncodeToString(customerKeyMD5[:]),
		}),
		// Object Lock on a bucket without it.
		put("put-legal-hold", "locked", map[string]string{"x-amz-object-lock-legal-hold": "ON"}),
		put("put-retention", "locked", map[string]string{
			"x-amz-object-lock-mode":              "GOVERNANCE",
			"x-amz-object-lock-retain-until-date": "2030-01-01T00:00:00Z",
		}),
		// Checksum algorithms beyond CRC32, CRC32C, CRC64NVME, SHA-1, and SHA-256.
		put("put-sha512", "sha512", map[string]string{"x-amz-checksum-sha512": base64.StdEncoding.EncodeToString(sha512Sum[:])}),
		{name: "get-sha512", method: http.MethodGet, key: "sha512", header: map[string]string{"x-amz-checksum-mode": "ENABLED"}, compare: optionHeaders},
		put("put-sha512-wrong", "bad", map[string]string{"x-amz-checksum-sha512": zeros(64)}),
		put("put-md5-checksum", "md5sum", map[string]string{"x-amz-checksum-md5": md5Base64(body)}),
		put("put-xxhash64-wrong", "bad", map[string]string{"x-amz-checksum-xxhash64": zeros(8)}),
		put("put-xxhash3-wrong", "bad", map[string]string{"x-amz-checksum-xxhash3": zeros(8)}),
		put("put-xxhash128-wrong", "bad", map[string]string{"x-amz-checksum-xxhash128": zeros(16)}),
		put("put-unknown-checksum", "bad", map[string]string{"x-amz-checksum-bogus": zeros(4)}),
		read("head-bad", http.MethodHead, "bad"),
		// Storage classes.
		put("put-standard-ia", "ia", map[string]string{"x-amz-storage-class": "STANDARD_IA"}),
		read("head-standard-ia", http.MethodHead, "ia"),
		put("put-glacier", "glacier", map[string]string{"x-amz-storage-class": "GLACIER"}),
		read("head-glacier", http.MethodHead, "glacier"),
		read("get-glacier", http.MethodGet, "glacier"),
		put("put-storage-bogus", "bogus", map[string]string{"x-amz-storage-class": "BOGUS"}),
		{name: "list-classes", method: http.MethodGet, query: "list-type=2"},
		// Website redirects.
		put("put-redirect", "redirect", map[string]string{"x-amz-website-redirect-location": "/target"}),
		read("head-redirect", http.MethodHead, "redirect"),
		put("put-redirect-invalid", "bogus", map[string]string{"x-amz-website-redirect-location": "target"}),
		// What a copy keeps.
		copyOf("copy-ia", "copy-ia", "ia", nil),
		read("head-copy-ia", http.MethodHead, "copy-ia"),
		copyOf("copy-redirect", "copy-redirect", "redirect", nil),
		read("head-copy-redirect", http.MethodHead, "copy-redirect"),
		// Multipart initiation.
		{name: "create-upload-ia", method: http.MethodPost, key: "upload", query: "uploads", compare: optionHeaders,
			header: map[string]string{"x-amz-storage-class": "STANDARD_IA", "x-amz-server-side-encryption": "AES256"}},
		{name: "list-uploads", method: http.MethodGet, query: "uploads"},
		{name: "upload-part", method: http.MethodPut, key: "upload", query: "partNumber=1&" + upload, body: body, compare: optionHeaders},
		{name: "complete-upload", method: http.MethodPost, key: "upload", query: upload, compare: optionHeaders,
			body: "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + etagOf(body) + "</ETag></Part></CompleteMultipartUpload>"},
		read("head-upload", http.MethodHead, "upload"),
		{name: "create-upload-aborted", method: http.MethodPost, key: "aborted", query: "uploads"},
		{name: "abort-upload", method: http.MethodDelete, key: "aborted", query: upload},
		{name: "delete-objects", method: http.MethodPost, query: "delete", body: batch.String(), header: map[string]string{"Content-MD5": md5Base64(batch.String())}},
		deleteBucket(),
	}}
}
