package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/yavosh/pail/internal/store"
)

// reply is one response from the test server.
type reply struct {
	status int
	code   string // the S3 error code, if the body is an error
	header http.Header
	body   string
}

// call sends one signed request. headers are set before signing.
func call(t *testing.T, srv *httptest.Server, method, target, body string, headers map[string]string) reply {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	signPayload(t, req, time.Now(), body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var e errorBody
	_ = xml.Unmarshal(b, &e)
	return reply{status: resp.StatusCode, code: e.Code, header: resp.Header, body: string(b)}
}

// startUpload creates bucket bkt, if needed, and an upload for key.
func startUpload(t *testing.T, srv *httptest.Server, key string, headers map[string]string) string {
	t.Helper()
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	r := call(t, srv, http.MethodPost, "/bkt/"+key+"?uploads", "", headers)
	var res struct {
		Bucket   string `xml:"Bucket"`
		Key      string `xml:"Key"`
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal([]byte(r.body), &res); err != nil || r.status != http.StatusOK || res.UploadID == "" || res.Bucket != "bkt" || res.Key != key {
		t.Fatalf("POST /bkt/%s?uploads = %d %s (%v), want an InitiateMultipartUploadResult for bkt/%s", key, r.status, r.body, err, key)
	}
	return res.UploadID
}

func putPart(t *testing.T, srv *httptest.Server, key, id string, n int, body string, headers map[string]string) reply {
	t.Helper()
	return call(t, srv, http.MethodPut, fmt.Sprintf("/bkt/%s?partNumber=%d&uploadId=%s", key, n, id), body, headers)
}

func TestUploadChecksum(t *testing.T) {
	tests := []struct {
		name, algorithm, typ string
		wantAlg, wantTyp     string
		wantOK               bool
	}{
		{"none", "", "", "", "", true},
		{"type without algorithm", "", "FULL_OBJECT", "", "", false},
		{"CRC64NVME default", "CRC64NVME", "", "CRC64NVME", "FULL_OBJECT", true},
		{"CRC64NVME full object", "CRC64NVME", "FULL_OBJECT", "CRC64NVME", "FULL_OBJECT", true},
		{"CRC64NVME composite", "CRC64NVME", "COMPOSITE", "", "", false},
		{"CRC32 default", "CRC32", "", "CRC32", "COMPOSITE", true},
		{"CRC32 full object", "CRC32", "FULL_OBJECT", "CRC32", "FULL_OBJECT", true},
		{"CRC32C composite", "CRC32C", "COMPOSITE", "CRC32C", "COMPOSITE", true},
		{"CRC32C full object", "CRC32C", "FULL_OBJECT", "CRC32C", "FULL_OBJECT", true},
		{"SHA1 default", "SHA1", "", "SHA1", "COMPOSITE", true},
		{"SHA256 composite", "SHA256", "COMPOSITE", "SHA256", "COMPOSITE", true},
		{"SHA256 full object", "SHA256", "FULL_OBJECT", "", "", false},
		{"SHA1 full object", "SHA1", "FULL_OBJECT", "", "", false},
		// The AWS rules for these three are unverified: they follow SHA1 and SHA256.
		{"SHA512 default", "SHA512", "", "SHA512", "COMPOSITE", true},
		{"MD5 composite", "MD5", "COMPOSITE", "MD5", "COMPOSITE", true},
		{"XXHASH64 full object", "XXHASH64", "FULL_OBJECT", "", "", false},
		{"XXHASH3 is not supported", "XXHASH3", "", "", "", false},
		{"lower-case algorithm", "crc32", "", "CRC32", "COMPOSITE", true},
		{"unknown algorithm", "BOGUS", "", "", "", false},
		{"unknown type", "CRC32", "PARTIAL", "", "", false},
	}
	for _, tt := range tests {
		h := http.Header{}
		if tt.algorithm != "" {
			h.Set("x-amz-checksum-algorithm", tt.algorithm)
		}
		if tt.typ != "" {
			h.Set("x-amz-checksum-type", tt.typ)
		}
		alg, typ, ok := uploadChecksum(h)
		if ok != tt.wantOK || ok && (alg != tt.wantAlg || typ != tt.wantTyp) {
			t.Errorf("%s: uploadChecksum(%q, %q) = %q, %q, %v, want %q, %q, %v", tt.name, tt.algorithm, tt.typ, alg, typ, ok, tt.wantAlg, tt.wantTyp, tt.wantOK)
		}
	}
}

func TestMultipartErrors(t *testing.T) {
	srv, _ := storeServer(t, "")
	id := startUpload(t, srv, "k", nil)
	// 1 KiB parts are below the minimum, so completing them before the last fails.
	p1 := putPart(t, srv, "k", id, 1, strings.Repeat("a", 1024), nil).header.Get("ETag")
	p2 := putPart(t, srv, "k", id, 2, "tail", nil).header.Get("ETag")
	crcID := startUpload(t, srv, "crc", map[string]string{"x-amz-checksum-algorithm": "CRC32"})
	crcETag := putPart(t, srv, "crc", crcID, 1, "x", nil).header.Get("ETag")
	complete := func(parts ...string) string {
		var b strings.Builder
		b.WriteString("<CompleteMultipartUpload>")
		for _, p := range parts {
			b.WriteString("<Part>" + p + "</Part>")
		}
		b.WriteString("</CompleteMultipartUpload>")
		return b.String()
	}
	part := func(n int, etag string) string {
		return fmt.Sprintf("<PartNumber>%d</PartNumber><ETag>%s</ETag>", n, etag)
	}
	missing := `"00000000000000000000000000000000"`
	unknown := strings.Repeat("0", 32)

	tests := []struct {
		name, method, target, body string
		headers                    map[string]string
		wantStatus                 int
		wantCode                   string
	}{
		{"create in a missing bucket", http.MethodPost, "/nope/k?uploads", "", nil, 404, "NoSuchBucket"},
		{"create in an invalid bucket", http.MethodPost, "/Bad_Name/k?uploads", "", nil, 400, "InvalidBucketName"},
		{"create with an unknown algorithm", http.MethodPost, "/bkt/k?uploads", "", map[string]string{"x-amz-checksum-algorithm": "BOGUS"}, 400, "InvalidRequest"},
		{"create with a type but no algorithm", http.MethodPost, "/bkt/k?uploads", "", map[string]string{"x-amz-checksum-type": "FULL_OBJECT"}, 400, "InvalidRequest"},
		{"create CRC64NVME composite", http.MethodPost, "/bkt/k?uploads", "", map[string]string{"x-amz-checksum-algorithm": "CRC64NVME", "x-amz-checksum-type": "COMPOSITE"}, 400, "InvalidRequest"},
		{"create SHA256 full object", http.MethodPost, "/bkt/k?uploads", "", map[string]string{"x-amz-checksum-algorithm": "SHA256", "x-amz-checksum-type": "FULL_OBJECT"}, 400, "InvalidRequest"},
		{"create with too much metadata", http.MethodPost, "/bkt/k?uploads", "", map[string]string{"x-amz-meta-big": strings.Repeat("x", 3000)}, 400, "MetadataTooLarge"},

		{"part 0", http.MethodPut, "/bkt/k?partNumber=0&uploadId=" + id, "x", nil, 400, "InvalidArgument"},
		{"part 10001", http.MethodPut, "/bkt/k?partNumber=10001&uploadId=" + id, "x", nil, 400, "InvalidArgument"},
		{"part -1", http.MethodPut, "/bkt/k?partNumber=-1&uploadId=" + id, "x", nil, 400, "InvalidArgument"},
		{"part +1", http.MethodPut, "/bkt/k?partNumber=%2B1&uploadId=" + id, "x", nil, 400, "InvalidArgument"},
		{"part abc", http.MethodPut, "/bkt/k?partNumber=abc&uploadId=" + id, "x", nil, 400, "InvalidArgument"},
		{"part empty", http.MethodPut, "/bkt/k?partNumber=&uploadId=" + id, "x", nil, 400, "InvalidArgument"},
		{"part 10000", http.MethodPut, "/bkt/k?partNumber=10000&uploadId=" + id, "x", nil, 200, ""},
		{"part to an unknown upload", http.MethodPut, "/bkt/k?partNumber=1&uploadId=" + unknown, "x", nil, 404, "NoSuchUpload"},
		{"part to another key", http.MethodPut, "/bkt/other?partNumber=1&uploadId=" + id, "x", nil, 404, "NoSuchUpload"},
		{"part to a missing bucket", http.MethodPut, "/nope/k?partNumber=1&uploadId=" + id, "x", nil, 404, "NoSuchBucket"},
		{"part with an invalid MD5", http.MethodPut, "/bkt/k?partNumber=3&uploadId=" + id, "x", map[string]string{"Content-MD5": "nope"}, 400, "InvalidDigest"},
		{"part with a wrong MD5", http.MethodPut, "/bkt/k?partNumber=3&uploadId=" + id, "x", map[string]string{"Content-MD5": "AAAAAAAAAAAAAAAAAAAAAA=="}, 400, "BadDigest"},
		{"part with a wrong CRC32", http.MethodPut, "/bkt/crc?partNumber=3&uploadId=" + crcID, "x", map[string]string{"x-amz-checksum-crc32": "AAAAAA=="}, 400, "BadDigest"},
		{"part with another algorithm", http.MethodPut, "/bkt/crc?partNumber=3&uploadId=" + crcID, "x", map[string]string{"x-amz-checksum-sha1": "Kq5sNclPz7QV2+lfQIuc6R7oRu0="}, 400, "InvalidRequest"},
		{"part with a malformed checksum", http.MethodPut, "/bkt/crc?partNumber=3&uploadId=" + crcID, "x", map[string]string{"x-amz-checksum-crc32": "nope!"}, 400, "InvalidRequest"},
		{"part with a trailer but no aws-chunked", http.MethodPut, "/bkt/crc?partNumber=3&uploadId=" + crcID, "x", map[string]string{"x-amz-trailer": "x-amz-checksum-crc32"}, 400, "InvalidRequest"},
		{"part copy source is not implemented", http.MethodPut, "/bkt/k?partNumber=3&uploadId=" + id, "", map[string]string{"x-amz-copy-source": "/bkt/k"}, 501, "NotImplemented"},

		{"complete with an empty body", http.MethodPost, "/bkt/k?uploadId=" + id, "", nil, 400, "MalformedXML"},
		{"complete with no parts", http.MethodPost, "/bkt/k?uploadId=" + id, complete(), nil, 400, "MalformedXML"},
		{"complete with the wrong root", http.MethodPost, "/bkt/k?uploadId=" + id, "<Foo/>", nil, 400, "MalformedXML"},
		{"complete with a bad part number", http.MethodPost, "/bkt/k?uploadId=" + id, complete("<PartNumber>x</PartNumber><ETag>a</ETag>"), nil, 400, "MalformedXML"},
		{"complete with text after the root", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(1, p1)) + "junk", nil, 400, "MalformedXML"},
		{"complete out of order", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(2, p2), part(1, p1)), nil, 400, "InvalidPartOrder"},
		{"complete with a repeated part", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(1, p1), part(1, p1)), nil, 400, "InvalidPartOrder"},
		{"complete with a wrong ETag", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(1, missing)), nil, 400, "InvalidPart"},
		{"complete with a part never uploaded", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(5, p1)), nil, 400, "InvalidPart"},
		{"complete with a small part first", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(1, p1), part(2, p2)), nil, 400, "EntityTooSmall"},
		{"complete an unknown upload", http.MethodPost, "/bkt/k?uploadId=" + unknown, complete(part(1, p1)), nil, 404, "NoSuchUpload"},
		{"complete an invalid upload ID", http.MethodPost, "/bkt/k?uploadId=../x", complete(part(1, p1)), nil, 404, "NoSuchUpload"},
		{"complete under another key", http.MethodPost, "/bkt/other?uploadId=" + id, complete(part(1, p1)), nil, 404, "NoSuchUpload"},
		{"complete with an unsupported if-none-match", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(2, p2)), map[string]string{"If-None-Match": `"abc"`}, 501, "NotImplemented"},
		{"complete with a malformed checksum", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(2, p2)), map[string]string{"x-amz-checksum-crc32": "nope!"}, 400, "InvalidRequest"},
		{"complete with a checksum trailer", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(2, p2)), map[string]string{"x-amz-trailer": "x-amz-checksum-crc32"}, 400, "InvalidRequest"},
		{"complete with a checksum for a composite upload", http.MethodPost, "/bkt/crc?uploadId=" + crcID, complete(part(1, crcETag)), map[string]string{"x-amz-checksum-crc32": "AAAAAA=="}, 400, "InvalidRequest"},

		{"list parts of an unknown upload", http.MethodGet, "/bkt/k?uploadId=" + unknown, "", nil, 404, "NoSuchUpload"},
		{"list parts under another key", http.MethodGet, "/bkt/other?uploadId=" + id, "", nil, 404, "NoSuchUpload"},
		{"list parts with a bad max-parts", http.MethodGet, "/bkt/k?uploadId=" + id + "&max-parts=abc", "", nil, 400, "InvalidArgument"},
		{"list parts with a negative max-parts", http.MethodGet, "/bkt/k?uploadId=" + id + "&max-parts=-1", "", nil, 400, "InvalidArgument"},
		{"list parts with a bad marker", http.MethodGet, "/bkt/k?uploadId=" + id + "&part-number-marker=x", "", nil, 400, "InvalidArgument"},
		{"list uploads with a bad max-uploads", http.MethodGet, "/bkt?uploads&max-uploads=abc", "", nil, 400, "InvalidArgument"},
		{"list uploads of a missing bucket", http.MethodGet, "/nope?uploads", "", nil, 404, "NoSuchBucket"},
		{"list uploads of an invalid bucket", http.MethodGet, "/Bad_Name?uploads", "", nil, 400, "InvalidBucketName"},

		{"abort an unknown upload", http.MethodDelete, "/bkt/k?uploadId=" + unknown, "", nil, 404, "NoSuchUpload"},
		{"abort under another key", http.MethodDelete, "/bkt/other?uploadId=" + id, "", nil, 404, "NoSuchUpload"},
		{"abort in a missing bucket", http.MethodDelete, "/nope/k?uploadId=" + id, "", nil, 404, "NoSuchBucket"},
		{"abort", http.MethodDelete, "/bkt/k?uploadId=" + id, "", nil, 204, ""},
		{"abort again", http.MethodDelete, "/bkt/k?uploadId=" + id, "", nil, 204, ""},
		{"part after abort", http.MethodPut, "/bkt/k?partNumber=1&uploadId=" + id, "x", nil, 404, "NoSuchUpload"},
		{"complete after abort", http.MethodPost, "/bkt/k?uploadId=" + id, complete(part(2, p2)), nil, 404, "NoSuchUpload"},
	}
	for _, tt := range tests {
		r := call(t, srv, tt.method, tt.target, tt.body, tt.headers)
		if r.status != tt.wantStatus || r.code != tt.wantCode {
			t.Errorf("%s: %s %s = %d %q, want %d %q (%s)", tt.name, tt.method, tt.target, r.status, r.code, tt.wantStatus, tt.wantCode, r.body)
		}
	}
}

// A part over 5 GiB fails before its body is read.
func TestUploadPartTooLarge(t *testing.T) {
	srv, _ := storeServer(t, "")
	id := startUpload(t, srv, "k", nil)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, srv.URL+"/bkt/k?partNumber=1&uploadId="+id, strings.NewReader("0\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", "5368709121")
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	if err := signer.SignHTTP(context.Background(), creds, req, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if status, code := send(t, req); status != http.StatusBadRequest || code != "EntityTooLarge" {
		t.Errorf("PUT of a 5 GiB + 1 part = %d %q, want 400 EntityTooLarge", status, code)
	}
}

func TestMultipartFlow(t *testing.T) {
	srv, st := storeServer(t, "localhost")
	id := startUpload(t, srv, "dir/big file", map[string]string{"Content-Type": "text/plain", "x-amz-meta-color": "blue", "x-amz-checksum-algorithm": "CRC32"})
	// 5 MiB makes part 1 large enough to precede another part.
	big := strings.Repeat("a", store.MinPartSize)
	// "DUoRhQ==" is the CRC32 of "hello world".
	r := putPart(t, srv, "dir/big%20file", id, 2, "hello world", map[string]string{"x-amz-checksum-crc32": "DUoRhQ=="})
	if r.status != http.StatusOK || r.header.Get("x-amz-checksum-crc32") != "DUoRhQ==" || r.header.Get("ETag") != `"5eb63bbbe01eeed093cb22bb8f5acdc3"` {
		t.Fatalf("UploadPart = %d, ETag %s, CRC32 %s, want 200, the MD5 ETag, and the CRC32", r.status, r.header.Get("ETag"), r.header.Get("x-amz-checksum-crc32"))
	}
	r = putPart(t, srv, "dir/big%20file", id, 1, big, nil)
	etag1 := r.header.Get("ETag")
	if r.status != http.StatusOK || r.header.Get("x-amz-checksum-crc32") == "" {
		t.Fatalf("UploadPart without a checksum = %d, CRC32 %q, want 200 and the computed CRC32", r.status, r.header.Get("x-amz-checksum-crc32"))
	}

	body := fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag><ChecksumCRC32>%s</ChecksumCRC32></Part><Part><PartNumber>2</PartNumber><ETag>\"5eb63bbbe01eeed093cb22bb8f5acdc3\"</ETag><ChecksumCRC32>DUoRhQ==</ChecksumCRC32></Part></CompleteMultipartUpload>",
		etag1, r.header.Get("x-amz-checksum-crc32"))
	r = call(t, srv, http.MethodPost, "/bkt/dir/big%20file?uploadId="+id, body, nil)
	var res struct {
		Location     string `xml:"Location"`
		Bucket       string `xml:"Bucket"`
		Key          string `xml:"Key"`
		ETag         string `xml:"ETag"`
		ChecksumCRC  string `xml:"ChecksumCRC32"`
		ChecksumType string `xml:"ChecksumType"`
	}
	if err := xml.Unmarshal([]byte(r.body), &res); err != nil || r.status != http.StatusOK {
		t.Fatalf("CompleteMultipartUpload = %d %s (%v), want 200", r.status, r.body, err)
	}
	if !strings.HasSuffix(res.ETag, `-2"`) || !strings.HasSuffix(res.ChecksumCRC, "-2") || res.ChecksumType != "COMPOSITE" ||
		res.Bucket != "bkt" || res.Key != "dir/big file" || !strings.HasSuffix(res.Location, "/bkt/dir/big%20file") {
		t.Errorf("CompleteMultipartUpload = %+v, want a 2-part ETag, a composite CRC32, and the object's location", res)
	}
	info, err := st.HeadObject(context.Background(), "bkt", "dir/big file")
	if err != nil || info.Metadata["X-Amz-Meta-Color"] != "blue" || info.Metadata["Content-Type"] != "text/plain" || info.Size != int64(len(big)+11) {
		t.Errorf("stored object = %+v, %v, want the upload's metadata and %d bytes", info, err, len(big)+11)
	}
	g := call(t, srv, http.MethodHead, "/bkt/dir/big%20file", "", map[string]string{"x-amz-checksum-mode": "ENABLED"})
	if g.header.Get("x-amz-checksum-type") != "COMPOSITE" || !strings.HasSuffix(g.header.Get("x-amz-checksum-crc32"), "-2") {
		t.Errorf("HEAD = type %q, CRC32 %q, want COMPOSITE and a value ending in -2", g.header.Get("x-amz-checksum-type"), g.header.Get("x-amz-checksum-crc32"))
	}

	// Virtual-hosted style puts the key right after the host in Location.
	vid := startUpload(t, srv, "v", nil)
	etag := putPart(t, srv, "v", vid, 1, "x", nil).header.Get("ETag")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/v?uploadId="+vid, strings.NewReader("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"+etag+"</ETag></Part></CompleteMultipartUpload>"))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "bkt.localhost"
	signPayload(t, req, time.Now(), "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"+etag+"</ETag></Part></CompleteMultipartUpload>")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if want := "<Location>http://bkt.localhost/v</Location>"; !strings.Contains(string(b), want) {
		t.Errorf("virtual-hosted CompleteMultipartUpload = %s, want %s", b, want)
	}
}

func TestCreateMultipartUploadHeaders(t *testing.T) {
	srv, _ := storeServer(t, "")
	tests := []struct {
		name         string
		headers      map[string]string
		wantAlg, typ string
	}{
		{"no checksum", nil, "", ""},
		{"CRC32", map[string]string{"x-amz-checksum-algorithm": "CRC32"}, "CRC32", "COMPOSITE"},
		{"CRC32 full object", map[string]string{"x-amz-checksum-algorithm": "CRC32", "x-amz-checksum-type": "FULL_OBJECT"}, "CRC32", "FULL_OBJECT"},
		{"CRC64NVME", map[string]string{"x-amz-checksum-algorithm": "CRC64NVME"}, "CRC64NVME", "FULL_OBJECT"},
		{"SHA256", map[string]string{"x-amz-checksum-algorithm": "SHA256"}, "SHA256", "COMPOSITE"},
	}
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	for _, tt := range tests {
		r := call(t, srv, http.MethodPost, "/bkt/k?uploads", "", tt.headers)
		if ct, ok := r.header["Content-Type"]; ok {
			t.Errorf("%s: create sent Content-Type %q, want none, as AWS", tt.name, ct)
		}
		if r.status != http.StatusOK || r.header.Get("x-amz-checksum-algorithm") != tt.wantAlg || r.header.Get("x-amz-checksum-type") != tt.typ {
			t.Errorf("%s: create = %d, algorithm %q, type %q, want 200, %q, %q", tt.name, r.status, r.header.Get("x-amz-checksum-algorithm"), r.header.Get("x-amz-checksum-type"), tt.wantAlg, tt.typ)
		}
	}
}

func TestListPartsPaging(t *testing.T) {
	srv, _ := storeServer(t, "")
	id := startUpload(t, srv, "k", map[string]string{"x-amz-checksum-algorithm": "SHA1"})
	for _, n := range []int{5, 1, 3, 2, 4} {
		putPart(t, srv, "k", id, n, strings.Repeat("x", n), nil)
	}
	type listing struct {
		UploadID             string `xml:"UploadId"`
		PartNumberMarker     int    `xml:"PartNumberMarker"`
		NextPartNumberMarker int    `xml:"NextPartNumberMarker"`
		MaxParts             int    `xml:"MaxParts"`
		IsTruncated          bool   `xml:"IsTruncated"`
		StorageClass         string `xml:"StorageClass"`
		ChecksumAlgorithm    string `xml:"ChecksumAlgorithm"`
		ChecksumType         string `xml:"ChecksumType"`
		Parts                []struct {
			PartNumber int    `xml:"PartNumber"`
			Size       int    `xml:"Size"`
			ChecksumSH string `xml:"ChecksumSHA1"`
		} `xml:"Part"`
	}
	tests := []struct {
		query         string
		wantParts     []int
		wantTruncated bool
		wantNext      int
		wantMax       int
	}{
		{"", []int{1, 2, 3, 4, 5}, false, 5, 1000},
		{"&max-parts=2", []int{1, 2}, true, 2, 2},
		{"&max-parts=2&part-number-marker=2", []int{3, 4}, true, 4, 2},
		{"&max-parts=2&part-number-marker=4", []int{5}, false, 5, 2},
		{"&max-parts=5", []int{1, 2, 3, 4, 5}, false, 5, 5},
		{"&part-number-marker=5", nil, false, 0, 1000},
		{"&max-parts=0", nil, false, 0, 0},
		{"&max-parts=5000", []int{1, 2, 3, 4, 5}, false, 5, 5000},
	}
	for _, tt := range tests {
		r := call(t, srv, http.MethodGet, "/bkt/k?uploadId="+id+tt.query, "", nil)
		var l listing
		if err := xml.Unmarshal([]byte(r.body), &l); err != nil || r.status != http.StatusOK {
			t.Errorf("ListParts%s = %d %s (%v), want 200", tt.query, r.status, r.body, err)
			continue
		}
		var got []int
		for _, p := range l.Parts {
			got = append(got, p.PartNumber)
			if p.Size != p.PartNumber || p.ChecksumSH == "" {
				t.Errorf("ListParts%s part %d = size %d, SHA1 %q, want size %d and a SHA1", tt.query, p.PartNumber, p.Size, p.ChecksumSH, p.PartNumber)
			}
		}
		if !slices.Equal(got, tt.wantParts) || l.IsTruncated != tt.wantTruncated || l.NextPartNumberMarker != tt.wantNext || l.MaxParts != tt.wantMax {
			t.Errorf("ListParts%s = parts %v, truncated %v, next %d, max %d; want %v, %v, %d, %d",
				tt.query, got, l.IsTruncated, l.NextPartNumberMarker, l.MaxParts, tt.wantParts, tt.wantTruncated, tt.wantNext, tt.wantMax)
		}
		if l.UploadID != id || l.StorageClass != "STANDARD" || l.ChecksumAlgorithm != "SHA1" || l.ChecksumType != "COMPOSITE" {
			t.Errorf("ListParts%s = upload %s, class %s, checksum %s %s; want %s, STANDARD, SHA1 COMPOSITE", tt.query, l.UploadID, l.StorageClass, l.ChecksumAlgorithm, l.ChecksumType, id)
		}
	}
}

func TestListMultipartUploads(t *testing.T) {
	srv, _ := storeServer(t, "")
	var order []string
	for _, key := range []string{"b", "docs/x", "a", "docs/y", "a"} {
		order = append(order, key+":"+startUpload(t, srv, key, nil))
	}
	type listing struct {
		KeyMarker          string `xml:"KeyMarker"`
		UploadIDMarker     string `xml:"UploadIdMarker"`
		NextKeyMarker      string `xml:"NextKeyMarker"`
		NextUploadIDMarker string `xml:"NextUploadIdMarker"`
		Prefix             string `xml:"Prefix"`
		Delimiter          string `xml:"Delimiter"`
		MaxUploads         int    `xml:"MaxUploads"`
		IsTruncated        bool   `xml:"IsTruncated"`
		Uploads            []struct {
			Key          string `xml:"Key"`
			UploadID     string `xml:"UploadId"`
			StorageClass string `xml:"StorageClass"`
			Initiated    string `xml:"Initiated"`
			Initiator    struct {
				ID string `xml:"ID"`
			} `xml:"Initiator"`
		} `xml:"Upload"`
		Prefixes []string `xml:"CommonPrefixes>Prefix"`
	}
	list := func(query string) listing {
		t.Helper()
		r := call(t, srv, http.MethodGet, "/bkt?uploads"+query, "", nil)
		var l listing
		if err := xml.Unmarshal([]byte(r.body), &l); err != nil || r.status != http.StatusOK {
			t.Fatalf("ListMultipartUploads%s = %d %s (%v), want 200", query, r.status, r.body, err)
		}
		return l
	}
	keys := func(l listing) []string {
		var out []string
		for _, u := range l.Uploads {
			out = append(out, u.Key+":"+u.UploadID)
		}
		return out
	}
	// By key. Uploads of one key list by start time, which can tie, so the
	// order of the two "a" uploads comes from the server.
	as := keys(list("&prefix=a"))
	if !slices.Equal(slices.Sorted(slices.Values(as)), slices.Sorted(slices.Values([]string{order[2], order[4]}))) {
		t.Fatalf("ListMultipartUploads with prefix a = %v, want %v and %v", as, order[2], order[4])
	}
	byKey := []string{as[0], as[1], order[0], order[1], order[3]}
	// wantNext is the last entry on the page, which AWS sends even when the page is not truncated.
	tests := []struct {
		query         string
		want          []string
		wantPrefixes  []string
		wantNext      string
		wantTruncated bool
	}{
		{"", byKey, nil, byKey[4], false},
		{"&prefix=docs/", []string{order[1], order[3]}, nil, order[3], false},
		{"&delimiter=/", []string{as[0], as[1], order[0]}, []string{"docs/"}, "docs/:", false},
		{"&prefix=docs/&delimiter=/", []string{order[1], order[3]}, nil, order[3], false},
		{"&max-uploads=2", byKey[:2], nil, byKey[1], true},
		{"&max-uploads=2&key-marker=a&upload-id-marker=" + id(byKey[0]), byKey[1:3], nil, byKey[2], true},
		{"&max-uploads=2&key-marker=b", byKey[3:], nil, byKey[4], false},
		{"&key-marker=a", byKey[2:], nil, byKey[4], false},
		{"&delimiter=/&max-uploads=3", byKey[:3], nil, byKey[2], true},
		{"&delimiter=/&key-marker=docs/", nil, nil, ":", false},
		{"&max-uploads=0", nil, nil, ":", false},
	}
	for _, tt := range tests {
		l := list(tt.query)
		next := l.NextKeyMarker + ":" + l.NextUploadIDMarker
		if !slices.Equal(keys(l), tt.want) || !slices.Equal(l.Prefixes, tt.wantPrefixes) || next != tt.wantNext || l.IsTruncated != tt.wantTruncated {
			t.Errorf("ListMultipartUploads%s = %v, prefixes %v, next %q, truncated %v; want %v, %v, %q, %v", tt.query, keys(l), l.Prefixes, next, l.IsTruncated, tt.want, tt.wantPrefixes, tt.wantNext, tt.wantTruncated)
		}
	}
	l := list("&max-uploads=2000&prefix=a&delimiter=%2F&key-marker=")
	if l.MaxUploads != 2000 || l.Prefix != "a" || l.Delimiter != "/" || len(l.Uploads) != 2 || l.Uploads[0].StorageClass != "STANDARD" || l.Uploads[0].Initiator.ID == "" || l.Uploads[0].Initiated == "" {
		t.Errorf("ListMultipartUploads echo = %+v, want MaxUploads 2000, prefix a, delimiter /, 2 uploads with initiator and start time", l)
	}
	// A completed or aborted upload leaves the list.
	if r := call(t, srv, http.MethodDelete, "/bkt/a?uploadId="+id(order[2]), "", nil); r.status != http.StatusNoContent {
		t.Fatalf("abort = %d, want 204", r.status)
	}
	if got := keys(list("&prefix=a")); !slices.Equal(got, []string{order[4]}) {
		t.Errorf("ListMultipartUploads after an abort = %v, want %v", got, []string{order[4]})
	}
}

func id(entry string) string { _, id, _ := strings.Cut(entry, ":"); return id }

func TestPageUploads(t *testing.T) {
	var all []store.UploadInfo
	for i, key := range []string{"a", "a", "docs/x", "docs/y", "docs/y", "z"} {
		all = append(all, store.UploadInfo{Key: key, ID: "id" + strconv.Itoa(i+1)})
	}
	type entry = string // "key:id"
	tests := []struct {
		name                           string
		prefix, delimiter, key, marker string
		limit                          int
		want                           []entry
		wantPrefixes                   []string
		wantTruncated                  bool
		wantLast                       entry
	}{
		{"all", "", "", "", "", 1000, []entry{"a:id1", "a:id2", "docs/x:id3", "docs/y:id4", "docs/y:id5", "z:id6"}, nil, false, "z:id6"},
		{"first page", "", "", "", "", 2, []entry{"a:id1", "a:id2"}, nil, true, "a:id2"},
		{"resume inside a key", "", "", "a", "id1", 2, []entry{"a:id2", "docs/x:id3"}, nil, true, "docs/x:id3"},
		{"key marker alone skips the key", "", "", "a", "", 2, []entry{"docs/x:id3", "docs/y:id4"}, nil, true, "docs/y:id4"},
		{"resume at the last upload of a key", "", "", "docs/y", "id4", 1000, []entry{"docs/y:id5", "z:id6"}, nil, false, "z:id6"},
		{"unknown marker ID skips the key", "", "", "a", "gone", 1000, []entry{"docs/x:id3", "docs/y:id4", "docs/y:id5", "z:id6"}, nil, false, "z:id6"},
		{"delimiter", "", "/", "", "", 1000, []entry{"a:id1", "a:id2", "z:id6"}, []string{"docs/"}, false, "z:id6"},
		{"delimiter page counts prefixes", "", "/", "", "", 3, []entry{"a:id1", "a:id2"}, []string{"docs/"}, true, "docs/:"},
		{"marker on a prefix skips it", "", "/", "docs/", "", 1000, []entry{"z:id6"}, nil, false, "z:id6"},
		{"prefix inside a folder", "docs/", "/", "", "", 1000, []entry{"docs/x:id3", "docs/y:id4", "docs/y:id5"}, nil, false, "docs/y:id5"},
		{"zero limit", "", "", "", "", 0, nil, nil, false, ":"},
	}
	for _, tt := range tests {
		filtered := slices.DeleteFunc(slices.Clone(all), func(u store.UploadInfo) bool { return !strings.HasPrefix(u.Key, tt.prefix) })
		p := pageUploads(filtered, tt.prefix, tt.delimiter, tt.key, tt.marker, tt.limit)
		var got []entry
		for _, u := range p.uploads {
			got = append(got, u.Key+":"+u.ID)
		}
		if !slices.Equal(got, tt.want) || !slices.Equal(p.prefixes, tt.wantPrefixes) || p.truncated != tt.wantTruncated || p.lastKey+":"+p.lastID != tt.wantLast {
			t.Errorf("%s: pageUploads = %v, %v, truncated %v, last %s:%s; want %v, %v, %v, %s",
				tt.name, got, p.prefixes, p.truncated, p.lastKey, p.lastID, tt.want, tt.wantPrefixes, tt.wantTruncated, tt.wantLast)
		}
	}
}

// As on AWS, deleting a bucket discards its pending uploads.
func TestDeleteBucketDiscardsUploads(t *testing.T) {
	srv, _ := storeServer(t, "")
	id := startUpload(t, srv, "k", nil)
	if status, code, _ := sendWith(t, srv, http.MethodDelete, "/bkt", "", nil); status != http.StatusNoContent {
		t.Fatalf("DELETE /bkt with an upload = %d %q, want 204", status, code)
	}
	if status, code, _ := sendWith(t, srv, http.MethodGet, "/bkt/k?uploadId="+id, "", nil); status != http.StatusNotFound || code != "NoSuchBucket" {
		t.Errorf("ListParts after DeleteBucket = %d %q, want 404 NoSuchBucket", status, code)
	}
}
