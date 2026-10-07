package diff

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"maps"
	"net/http"
	"strings"
)

// step is one raw S3 request. key empty addresses the bucket. query is
// already encoded. "{bucket}" in query and header values becomes the bucket.
// "{uploadId}" in the query, header values, and body becomes the ID that the
// latest CreateMultipartUpload in the scenario returned.
type step struct {
	form         map[string]string
	postMutation string
	name         string
	method       string
	key          string
	query        string
	header       map[string]string
	body         string
	auth         authMode
	// stream sends the body aws-chunked in this x-amz-content-sha256 mode.
	stream      string
	chunk       int    // chunk size; 0 means 8 KiB
	trailer     string // a trailing header line, such as "x-amz-checksum-crc32:<value>"
	badChunkSig bool   // corrupt the first chunk signature
}

// scenario runs its steps in order against one fresh bucket name. Scenarios
// never call ListBuckets: on AWS it would expose the account's other buckets.
type scenario struct {
	name  string
	steps []step
}

func etagOf(body string) string {
	sum := md5.Sum([]byte(body))
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// crc32Base64 is the CRC32 checksum value of body, as S3 sends it.
func crc32Base64(body string) string {
	sum := binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE([]byte(body)))
	return base64.StdEncoding.EncodeToString(sum)
}

// crc32Trailer is the CRC32 trailer line of body.
func crc32Trailer(body string) string {
	return "x-amz-checksum-crc32:" + crc32Base64(body)
}

// md5Base64 is the Content-MD5 value of body.
func md5Base64(body string) string {
	sum := md5.Sum([]byte(body))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// withUploadID returns st with "{uploadId}" replaced by id. The recorded
// request and fingerprint come from st itself, so they keep the placeholder.
func (st step) withUploadID(id string) step {
	const placeholder = "{uploadId}"
	st.query = strings.ReplaceAll(st.query, placeholder, id)
	st.body = strings.ReplaceAll(st.body, placeholder, id)
	if st.header != nil {
		header := make(map[string]string, len(st.header))
		for k, v := range st.header {
			header[k] = strings.ReplaceAll(v, placeholder, id)
		}
		st.header = header
	}
	return st
}

func createBucket() step { return step{name: "create-bucket", method: http.MethodPut} }
func deleteBucket() step { return step{name: "delete-bucket", method: http.MethodDelete} }

// specialKeys are keys that path handling tends to break.
var specialKeys = []struct{ name, key string }{
	{"double-slash", "a//b"},
	{"dot-dot", "../x"},
	{"plus", "a+b"},
	{"space", "a b"},
	{"unicode", "✓"},
	{"folder-marker", "dir/"},
}

func scenarios() []scenario {
	const body = "hello world"
	all := []scenario{
		corsScenario(),
		lifecycleRulesScenario(),
		aclScenario(),
		postScenario(),
		sigV2Scenario(),
		conditionalDeleteScenario(),
		{name: "auth-errors", steps: []step{
			createBucket(),
			{name: "no-credentials", method: http.MethodGet, query: "list-type=2", auth: authNone},
			{name: "unknown-key", method: http.MethodGet, query: "list-type=2", auth: authUnknownKey},
			{name: "bad-signature", method: http.MethodGet, query: "list-type=2", auth: authBadSignature},
			{name: "clock-skew", method: http.MethodGet, query: "list-type=2", auth: authSkewed},
			deleteBucket(),
		}},
		{name: "bucket-lifecycle", steps: []step{
			{name: "head-missing", method: http.MethodHead},
			{name: "delete-missing", method: http.MethodDelete},
			createBucket(),
			{name: "create-again", method: http.MethodPut},
			{name: "head", method: http.MethodHead},
			{name: "location", method: http.MethodGet, query: "location"},
			{name: "put-object", method: http.MethodPut, key: "k", body: "x"},
			{name: "delete-not-empty", method: http.MethodDelete},
			{name: "delete-object", method: http.MethodDelete, key: "k"},
			deleteBucket(),
			{name: "head-after-delete", method: http.MethodHead},
		}},
		{name: "object-basics", steps: []step{
			createBucket(),
			{name: "put", method: http.MethodPut, key: "docs/hello.txt", body: body, header: map[string]string{
				"Content-Type":        "text/plain",
				"Cache-Control":       "max-age=60",
				"Content-Disposition": "attachment",
				"X-Amz-Meta-Color":    "blue",
			}},
			{name: "head", method: http.MethodHead, key: "docs/hello.txt"},
			{name: "get", method: http.MethodGet, key: "docs/hello.txt"},
			{name: "get-range", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"Range": "bytes=0-4"}},
			{name: "get-suffix-range", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"Range": "bytes=-5"}},
			{name: "get-range-unsatisfiable", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"Range": "bytes=100-200"}},
			{name: "get-if-none-match", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"If-None-Match": etagOf(body)}},
			{name: "get-if-match-mismatch", method: http.MethodGet, key: "docs/hello.txt", header: map[string]string{"If-Match": `"nope"`}},
			{name: "get-override", method: http.MethodGet, key: "docs/hello.txt", query: "response-content-type=application%2Fjson"},
			{name: "put-if-none-match-star", method: http.MethodPut, key: "docs/hello.txt", body: "again", header: map[string]string{"If-None-Match": "*"}},
			{name: "get-missing", method: http.MethodGet, key: "missing"},
			{name: "put-xml", method: http.MethodPut, key: "document.xml", body: `<ID permission="read"> original </ID>`, header: map[string]string{"Content-Type": "application/xml"}},
			{name: "get-xml", method: http.MethodGet, key: "document.xml"},
			{name: "delete-xml", method: http.MethodDelete, key: "document.xml"},
			{name: "delete", method: http.MethodDelete, key: "docs/hello.txt"},
			{name: "delete-again", method: http.MethodDelete, key: "docs/hello.txt"},
			deleteBucket(),
		}},
	}

	keys := scenario{name: "special-keys", steps: []step{createBucket()}}
	for _, k := range specialKeys {
		keys.steps = append(keys.steps,
			step{name: "put-" + k.name, method: http.MethodPut, key: k.key, body: k.key},
			step{name: "get-" + k.name, method: http.MethodGet, key: k.key},
		)
	}
	keys.steps = append(keys.steps, step{name: "list", method: http.MethodGet, query: "list-type=2"})
	for _, k := range specialKeys {
		keys.steps = append(keys.steps, step{name: "delete-" + k.name, method: http.MethodDelete, key: k.key})
	}
	keys.steps = append(keys.steps, deleteBucket())
	return append(all, keys, listingScenario(), streamingScenario(), presignedScenario(), copyAndDeleteScenario(), multipartScenario(), multipartValidationScenario())
}

// streamingScenario records aws-chunked uploads in each signing mode, and
// how AWS rejects broken ones.
func streamingScenario() scenario {
	body := strings.Repeat("0123456789abcdef", 1280) // three 8 KiB chunks
	crc := crc32Trailer(body)
	checksumMode := map[string]string{"x-amz-checksum-mode": "ENABLED"}
	sc := scenario{name: "streaming", steps: []step{
		createBucket(),
		{name: "put-unsigned-trailer", method: http.MethodPut, key: "unsigned-trailer", body: body, stream: streamUnsignedTrailer, trailer: crc},
		{name: "head-unsigned-trailer", method: http.MethodHead, key: "unsigned-trailer", header: checksumMode},
		{name: "put-signed", method: http.MethodPut, key: "signed", body: body, stream: streamSigned},
		{name: "head-signed", method: http.MethodHead, key: "signed", header: checksumMode},
		{name: "put-signed-trailer", method: http.MethodPut, key: "signed-trailer", body: body, stream: streamSignedTrailer, trailer: crc},
		{name: "head-signed-trailer", method: http.MethodHead, key: "signed-trailer", header: checksumMode},
		{name: "put-gzip", method: http.MethodPut, key: "gzip", body: body, stream: streamUnsignedTrailer, trailer: crc,
			header: map[string]string{"Content-Encoding": "gzip,aws-chunked"}},
		{name: "head-gzip", method: http.MethodHead, key: "gzip"},
		{name: "put-small-chunks", method: http.MethodPut, key: "small-chunks", body: body, stream: streamSigned, chunk: 1 << 10},
		{name: "put-bad-chunk-signature", method: http.MethodPut, key: "bad", body: body, stream: streamSigned, badChunkSig: true},
		{name: "put-trailer-checksum-mismatch", method: http.MethodPut, key: "bad", body: body, stream: streamUnsignedTrailer, trailer: crc32Trailer("other")},
		{name: "put-decoded-length-mismatch", method: http.MethodPut, key: "bad", body: body, stream: streamSigned,
			header: map[string]string{"X-Amz-Decoded-Content-Length": "20481"}},
		{name: "put-missing-decoded-length", method: http.MethodPut, key: "bad", body: body, stream: streamSigned,
			header: map[string]string{"X-Amz-Decoded-Content-Length": ""}},
		{name: "head-bad", method: http.MethodHead, key: "bad"},
	}}
	for _, key := range []string{"unsigned-trailer", "signed", "signed-trailer", "gzip", "small-chunks"} {
		sc.steps = append(sc.steps, step{name: "delete-" + key, method: http.MethodDelete, key: key})
	}
	sc.steps = append(sc.steps, deleteBucket())
	return sc
}

// presignedScenario records how AWS answers query-string authentication:
// each operation, an expired and a tampered URL, and out-of-range lifetimes.
func presignedScenario() scenario {
	const key = "dir/a b.txt"
	return scenario{name: "presigned", steps: []step{
		createBucket(),
		{name: "put", method: http.MethodPut, key: key, body: "hello presigned", auth: authPresigned},
		{name: "get", method: http.MethodGet, key: key, auth: authPresigned},
		{name: "get-content-disposition", method: http.MethodGet, key: key, auth: authPresigned,
			query: "response-content-disposition=attachment%3B%20filename%3D%22a.txt%22"},
		{name: "head", method: http.MethodHead, key: key, auth: authPresigned},
		{name: "get-expired", method: http.MethodGet, key: key, auth: authPresignedExpired},
		{name: "get-tampered", method: http.MethodGet, key: key, auth: authPresignedTampered},
		{name: "get-expires-too-long", method: http.MethodGet, key: key, auth: authPresigned, query: "X-Amz-Expires=604801"},
		{name: "get-expires-zero", method: http.MethodGet, key: key, auth: authPresigned, query: "X-Amz-Expires=0"},
		{name: "get-future", method: http.MethodGet, key: key, auth: authPresignedFuture},
		{name: "get-bad-credential", method: http.MethodGet, key: key, auth: authPresignedBadCredential},
		{name: "get-missing-param", method: http.MethodGet, key: key, auth: authPresignedMissingParam},
		{name: "delete", method: http.MethodDelete, key: key, auth: authPresigned},
		deleteBucket(),
	}}
}

// listingKeys exercise delimiters, nesting, and characters that encoding-type=url changes.
var listingKeys = []struct{ name, key string }{
	{"a", "a"},
	{"a-b", "a/b"},
	{"a-c-d", "a/c/d"},
	{"dir", "dir/"},
	{"dir-x", "dir/x"},
	{"e-1", "e-1"},
	{"e-2", "e-2"},
	{"space-plus", "sp ace+plus/k"},
}

// listingScenario records how AWS pages, groups, and encodes listings.
func listingScenario() scenario {
	sc := scenario{name: "listing", steps: []step{createBucket()}}
	for _, k := range listingKeys {
		sc.steps = append(sc.steps, step{name: "put-" + k.name, method: http.MethodPut, key: k.key})
	}
	lists := []struct{ name, query string }{
		{"v2-root-folders", "list-type=2&delimiter=%2F"},
		{"v2-inside-a-folder", "list-type=2&prefix=a%2F&delimiter=%2F"},
		{"v2-other-delimiter", "list-type=2&delimiter=-"},
		{"v2-first-page", "list-type=2&max-keys=1"},
		{"v2-start-after-inside-a-prefix", "list-type=2&delimiter=%2F&start-after=a%2Fb"},
		{"v2-url-encoding", "list-type=2&encoding-type=url"},
		{"v2-max-keys-zero", "list-type=2&max-keys=0"},
		{"v2-max-keys-over-limit", "list-type=2&max-keys=5000"},
		{"v2-fetch-owner", "list-type=2&prefix=e&fetch-owner=true"},
		{"v1-page-with-delimiter", "max-keys=2&delimiter=%2F"},
		{"v1-page-without-delimiter", "max-keys=2"},
		{"v1-url-encoding-and-marker", "encoding-type=url&marker=a%2Fb"},
	}
	for _, l := range lists {
		sc.steps = append(sc.steps, step{name: l.name, method: http.MethodGet, query: l.query})
	}
	for _, k := range listingKeys {
		sc.steps = append(sc.steps, step{name: "delete-" + k.name, method: http.MethodDelete, key: k.key})
	}
	sc.steps = append(sc.steps, deleteBucket())
	return sc
}

// deleteXML is a DeleteObjects body for keys, which need no XML escaping.
func deleteXML(quiet bool, keys ...string) string {
	var b strings.Builder
	b.WriteString("<Delete>")
	if quiet {
		b.WriteString("<Quiet>true</Quiet>")
	}
	for _, k := range keys {
		b.WriteString("<Object><Key>" + k + "</Key></Object>")
	}
	b.WriteString("</Delete>")
	return b.String()
}

// copyAndDeleteScenario records CopyObject with each metadata directive and
// condition, its errors, and DeleteObjects with its checksum rules and limits.
func copyAndDeleteScenario() scenario {
	const body = "hello world"
	copyFrom := func(source string, extra map[string]string) map[string]string {
		h := map[string]string{"x-amz-copy-source": source}
		maps.Copy(h, extra)
		return h
	}
	src := "{bucket}/src"
	meta := map[string]string{"Content-Type": "text/plain", "X-Amz-Meta-Color": "blue"}
	manyKeys := make([]string, 1001)
	for i := range manyKeys {
		manyKeys[i] = fmt.Sprintf("k%04d", i)
	}
	del := func(name, body string, header map[string]string) step {
		return step{name: name, method: http.MethodPost, query: "delete", body: body, header: header}
	}
	md5Of := func(body string) map[string]string { return map[string]string{"Content-MD5": md5Base64(body)} }
	d1 := deleteXML(false, "src", "dst", "missing")
	d2 := deleteXML(true, "dst-replace", "a b", "a+b")
	d3 := deleteXML(false, "space-copy", "plus-copy", "cond")
	d4 := deleteXML(false, "src")
	d5 := deleteXML(false, manyKeys...)
	return scenario{name: "copy-and-delete", steps: []step{
		createBucket(),
		{name: "put-src", method: http.MethodPut, key: "src", body: body, header: meta},
		{name: "copy", method: http.MethodPut, key: "dst", header: copyFrom(src, nil)},
		{name: "head-copy", method: http.MethodHead, key: "dst"},
		{name: "copy-replace", method: http.MethodPut, key: "dst-replace", header: copyFrom(src, map[string]string{
			"x-amz-metadata-directive": "REPLACE", "Content-Type": "application/json", "X-Amz-Meta-Color": "red"})},
		{name: "head-copy-replace", method: http.MethodHead, key: "dst-replace"},
		{name: "copy-onto-itself", method: http.MethodPut, key: "src", header: copyFrom(src, nil)},
		{name: "copy-onto-itself-replace", method: http.MethodPut, key: "src", header: copyFrom(src, map[string]string{
			"x-amz-metadata-directive": "REPLACE", "Content-Type": "text/plain", "X-Amz-Meta-Color": "blue"})},
		{name: "copy-if-match-fails", method: http.MethodPut, key: "cond", header: copyFrom(src, map[string]string{"x-amz-copy-source-if-match": `"nope"`})},
		{name: "copy-if-none-match-fails", method: http.MethodPut, key: "cond", header: copyFrom(src, map[string]string{"x-amz-copy-source-if-none-match": etagOf(body)})},
		{name: "copy-if-modified-since-future", method: http.MethodPut, key: "cond", header: copyFrom(src, map[string]string{"x-amz-copy-source-if-modified-since": "Fri, 01 Jan 2100 00:00:00 GMT"})},
		{name: "copy-if-unmodified-since-past", method: http.MethodPut, key: "cond", header: copyFrom(src, map[string]string{"x-amz-copy-source-if-unmodified-since": "Thu, 01 Jan 2015 00:00:00 GMT"})},
		{name: "copy-if-match-passes", method: http.MethodPut, key: "cond", header: copyFrom(src, map[string]string{"x-amz-copy-source-if-match": etagOf(body)})},
		{name: "copy-missing-source", method: http.MethodPut, key: "cond2", header: copyFrom("{bucket}/missing", nil)},
		{name: "copy-bad-source", method: http.MethodPut, key: "cond2", header: copyFrom("{bucket}", nil)},
		{name: "copy-unknown-directive", method: http.MethodPut, key: "cond2", header: copyFrom(src, map[string]string{"x-amz-metadata-directive": "MERGE"})},
		{name: "put-space-key", method: http.MethodPut, key: "a b", body: body},
		{name: "copy-space-key", method: http.MethodPut, key: "space-copy", header: copyFrom("{bucket}/a%20b", nil)},
		{name: "put-plus-key", method: http.MethodPut, key: "a+b", body: body},
		{name: "copy-plus-key", method: http.MethodPut, key: "plus-copy", header: copyFrom("{bucket}/a+b", nil)},
		{name: "head-plus-copy", method: http.MethodHead, key: "plus-copy"},
		del("delete-objects", d1, md5Of(d1)),
		del("delete-objects-quiet", d2, md5Of(d2)),
		del("delete-objects-crc32", d3, map[string]string{"x-amz-checksum-crc32": crc32Base64(d3)}),
		del("delete-objects-no-checksum", d4, nil),
		del("delete-objects-bad-md5", d4, map[string]string{"Content-MD5": md5Base64("other")}),
		del("delete-objects-1001", d5, md5Of(d5)),
		deleteBucket(),
	}}
}

// completeXML is a CompleteMultipartUpload body. Each part is the content of its Part element.
func completeXML(parts ...string) string {
	var b strings.Builder
	b.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		b.WriteString("<Part>" + p + "</Part>")
	}
	b.WriteString("</CompleteMultipartUpload>")
	return b.String()
}

// partXML is the content of a Part element for number n and its body.
// checksum, if set, is the part's CRC32.
func partXML(n int, body string, checksum bool) string {
	s := fmt.Sprintf("<PartNumber>%d</PartNumber><ETag>%s</ETag>", n, etagOf(body))
	if checksum {
		s += "<ChecksumCRC32>" + crc32Base64(body) + "</ChecksumCRC32>"
	}
	return s
}

// multipartScenario records multipart uploads: listings, each completion
// error, the 5 MiB minimum, part number limits, and the checksum rules.
func multipartScenario() scenario {
	big := strings.Repeat("a", 5<<20) // the smallest part that is not the last
	tail := strings.Repeat("b", 1<<10)
	const upload = "uploadId={uploadId}"
	partQuery := func(n int) string { return fmt.Sprintf("partNumber=%d&%s", n, upload) }
	crcHeader := func(body string) map[string]string {
		return map[string]string{"x-amz-checksum-crc32": crc32Base64(body)}
	}
	createWith := func(name, key string, header map[string]string) step {
		return step{name: name, method: http.MethodPost, key: key, query: "uploads", header: header}
	}
	put := func(name, key string, n int, body string, header map[string]string) step {
		return step{name: name, method: http.MethodPut, key: key, query: partQuery(n), body: body, header: header}
	}
	complete := func(name, key, body string, header map[string]string) step {
		return step{name: name, method: http.MethodPost, key: key, query: upload, body: body, header: header}
	}
	abort := func(name, key string) step {
		return step{name: name, method: http.MethodDelete, key: key, query: upload}
	}
	checksumMode := map[string]string{"x-amz-checksum-mode": "ENABLED"}
	crc32Algorithm := map[string]string{"x-amz-checksum-algorithm": "CRC32"}
	fullObject := map[string]string{"x-amz-checksum-algorithm": "CRC32", "x-amz-checksum-type": "FULL_OBJECT"}
	wrongETag := "<PartNumber>1</PartNumber><ETag>" + etagOf("other") + "</ETag>"
	return scenario{name: "multipart", steps: []step{
		createBucket(),
		createWith("create-upload", "big", map[string]string{"Content-Type": "text/plain", "X-Amz-Meta-Color": "blue"}),
		put("upload-part-1", "big", 1, big, nil),
		put("upload-part-2", "big", 2, tail, nil),
		{name: "list-parts", method: http.MethodGet, key: "big", query: upload},
		{name: "list-parts-first-page", method: http.MethodGet, key: "big", query: upload + "&max-parts=1"},
		{name: "list-parts-after-marker", method: http.MethodGet, key: "big", query: upload + "&part-number-marker=1"},
		{name: "list-uploads", method: http.MethodGet, query: "uploads"},
		{name: "list-uploads-other-prefix", method: http.MethodGet, query: "uploads&prefix=other"},
		complete("complete-bad-order", "big", completeXML(partXML(2, tail, false), partXML(1, big, false)), nil),
		complete("complete-bad-etag", "big", completeXML(wrongETag, partXML(2, tail, false)), nil),
		complete("complete-missing-part", "big", completeXML(partXML(1, big, false), partXML(3, tail, false)), nil),
		complete("complete-no-parts", "big", completeXML(), nil),
		complete("complete", "big", completeXML(partXML(1, big, false), partXML(2, tail, false)), nil),
		{name: "head-big", method: http.MethodHead, key: "big", header: checksumMode},
		abort("abort-completed", "big"),
		{name: "list-uploads-after-complete", method: http.MethodGet, query: "uploads"},

		createWith("create-upload-small", "small", nil),
		put("upload-small-1", "small", 1, tail, nil),
		put("upload-small-2", "small", 2, tail, nil),
		complete("complete-small", "small", completeXML(partXML(1, tail, false), partXML(2, tail, false)), nil),
		abort("abort-small", "small"),
		abort("abort-small-again", "small"),

		createWith("create-upload-numbers", "numbers", nil),
		put("upload-part-0", "numbers", 0, tail, nil),
		put("upload-part-10001", "numbers", 10001, tail, nil),
		put("upload-part-10000", "numbers", 10000, tail, nil),
		abort("abort-numbers", "numbers"),

		createWith("create-crc32", "crc32", crc32Algorithm),
		put("upload-crc32-part-1", "crc32", 1, big, crcHeader(big)),
		put("upload-crc32-part-2", "crc32", 2, tail, crcHeader(tail)),
		put("upload-crc32-part-wrong-checksum", "crc32", 3, tail, crcHeader("other")),
		put("upload-crc32-part-other-algorithm", "crc32", 3, tail, map[string]string{"x-amz-checksum-sha1": "Kq5sNclPz7QV2+lfQIuc6R7oRu0="}),
		complete("complete-crc32-wrong-part-checksum", "crc32", completeXML(partXML(1, big, true), "<PartNumber>2</PartNumber><ETag>"+etagOf(tail)+"</ETag><ChecksumCRC32>"+crc32Base64("other")+"</ChecksumCRC32>"), nil),
		complete("complete-crc32", "crc32", completeXML(partXML(1, big, true), partXML(2, tail, true)), nil),
		{name: "head-crc32", method: http.MethodHead, key: "crc32", header: checksumMode},
		createWith("create-crc64nvme-composite", "bad", map[string]string{"x-amz-checksum-algorithm": "CRC64NVME", "x-amz-checksum-type": "COMPOSITE"}),
		createWith("create-sha256-full-object", "bad", map[string]string{"x-amz-checksum-algorithm": "SHA256", "x-amz-checksum-type": "FULL_OBJECT"}),
		createWith("create-type-without-algorithm", "bad", map[string]string{"x-amz-checksum-type": "FULL_OBJECT"}),

		createWith("create-crc32-full-object", "crc32-full", fullObject),
		put("upload-full-part-1", "crc32-full", 1, big, crcHeader(big)),
		put("upload-full-part-2", "crc32-full", 2, tail, crcHeader(tail)),
		complete("complete-full-wrong-checksum", "crc32-full", completeXML(partXML(1, big, true), partXML(2, tail, true)),
			map[string]string{"x-amz-checksum-crc32": crc32Base64("other"), "x-amz-checksum-type": "FULL_OBJECT"}),
		complete("complete-full", "crc32-full", completeXML(partXML(1, big, true), partXML(2, tail, true)),
			map[string]string{"x-amz-checksum-crc32": crc32Base64(big + tail), "x-amz-checksum-type": "FULL_OBJECT"}),
		{name: "head-full", method: http.MethodHead, key: "crc32-full", header: checksumMode},

		{name: "delete-big", method: http.MethodDelete, key: "big"},
		{name: "delete-crc32", method: http.MethodDelete, key: "crc32"},
		{name: "delete-crc32-full", method: http.MethodDelete, key: "crc32-full"},
		// Last, because AWS deletes a bucket that still has a pending upload.
		createWith("create-leftover-upload", "leftover", nil),
		{name: "delete-bucket-with-upload", method: http.MethodDelete},
	}}
}

// multipartValidationScenario checks completion failures with one small part.
func multipartValidationScenario() scenario {
	const body = "checksum probe"
	const upload = "uploadId={uploadId}"
	return scenario{name: "multipart-validation", steps: []step{
		createBucket(),
		{name: "create-upload", method: http.MethodPost, key: "k", query: "uploads",
			header: map[string]string{"x-amz-checksum-algorithm": "CRC32", "x-amz-checksum-type": "COMPOSITE"}},
		{name: "upload-part-1", method: http.MethodPut, key: "k", query: "partNumber=1&" + upload, body: body,
			header: map[string]string{"x-amz-checksum-crc32": crc32Base64(body)}},
		{name: "upload-part-2", method: http.MethodPut, key: "k", query: "partNumber=2&" + upload, body: body,
			header: map[string]string{"x-amz-checksum-crc32": crc32Base64(body)}},
		{name: "complete-missing-checksum", method: http.MethodPost, key: "k", query: upload, body: completeXML(partXML(1, body, false))},
		{name: "complete-nonconsecutive", method: http.MethodPost, key: "k", query: upload, body: completeXML(partXML(2, body, true))},
		{name: "complete-wrong-type", method: http.MethodPost, key: "k", query: upload, body: completeXML(partXML(1, body, true)),
			header: map[string]string{"x-amz-checksum-type": "FULL_OBJECT"}},
		{name: "complete-corrected", method: http.MethodPost, key: "k", query: upload, body: completeXML(partXML(1, body, true))},
		{name: "get", method: http.MethodGet, key: "k"},
		{name: "delete", method: http.MethodDelete, key: "k"},
		deleteBucket(),
	}}
}
