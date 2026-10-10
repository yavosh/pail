package s3api

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/yavosh/pail/internal/store"
)

// deleteBody builds a Delete document. A key "k@v" carries the version ID "v".
func deleteBody(quiet string, keys ...string) string {
	var b strings.Builder
	b.WriteString("<Delete>")
	if quiet != "" {
		b.WriteString("<Quiet>" + quiet + "</Quiet>")
	}
	for _, k := range keys {
		k, version, _ := strings.Cut(k, "@")
		b.WriteString("<Object><Key>" + k + "</Key>")
		if version != "" {
			b.WriteString("<VersionId>" + version + "</VersionId>")
		}
		b.WriteString("</Object>")
	}
	b.WriteString("</Delete>")
	return b.String()
}

func md5Header(body string) map[string]string {
	sum := md5.Sum([]byte(body))
	return map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(sum[:])}
}

func crc32Header(body string) map[string]string {
	sum := binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE([]byte(body)))
	return map[string]string{"x-amz-checksum-crc32": base64.StdEncoding.EncodeToString(sum)}
}

func TestDeleteObjects(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	manyKeys := make([]string, maxDeleteKeys+1)
	for i := range manyKeys {
		manyKeys[i] = fmt.Sprintf("k%04d", i)
	}
	long := strings.Repeat("k", maxKeyLen+1)

	tests := []struct {
		name        string
		path        string
		body        string
		header      func(string) map[string]string
		wantStatus  int
		wantCode    string
		wantDeleted []string          // reported in Deleted
		wantErrors  map[string]string // key to code
		wantKept    []string          // seeded keys that must remain
	}{
		{"Content-MD5", "/bkt?delete", deleteBody("", "a", "b", "missing"), md5Header, 200, "", []string{"a", "b", "missing"}, nil, nil},
		{"crc32 header", "/bkt?delete", deleteBody("", "a", "b"), crc32Header, 200, "", []string{"a", "b"}, nil, nil},
		{"quiet", "/bkt?delete", deleteBody("true", "a", "b"), md5Header, 200, "", nil, nil, nil},
		{"quiet false", "/bkt?delete", deleteBody("false", "a"), md5Header, 200, "", []string{"a"}, nil, []string{"b"}},
		{"quiet with an error", "/bkt?delete", deleteBody("true", "a", "b@v1"), md5Header, 200, "", nil, map[string]string{"b": "InvalidArgument"}, []string{"b"}},
		{"null version", "/bkt?delete", deleteBody("", "a@null"), md5Header, 200, "", []string{"a"}, nil, []string{"b"}},
		{"malformed version", "/bkt?delete", deleteBody("", "a", "b@v1"), md5Header, 200, "", []string{"a"}, map[string]string{"b": "InvalidArgument"}, []string{"b"}},
		{"key too long", "/bkt?delete", deleteBody("", "a", long), md5Header, 200, "", []string{"a"}, map[string]string{long: "KeyTooLongError"}, []string{"b"}},
		{"1000 keys", "/bkt?delete", deleteBody("true", manyKeys[:maxDeleteKeys]...), md5Header, 200, "", nil, nil, []string{"a", "b"}},
		{"1001 keys", "/bkt?delete", deleteBody("", manyKeys...), md5Header, 400, "MalformedXML", nil, nil, []string{"a", "b"}},
		{"no keys", "/bkt?delete", deleteBody(""), md5Header, 400, "MalformedXML", nil, nil, []string{"a", "b"}},
		{"empty key", "/bkt?delete", deleteBody("", "a", ""), md5Header, 400, "MalformedXML", nil, nil, []string{"a", "b"}},
		{"bad Quiet", "/bkt?delete", deleteBody("maybe", "a"), md5Header, 400, "MalformedXML", nil, nil, []string{"a", "b"}},
		{"wrong root", "/bkt?delete", "<Remove><Object><Key>a</Key></Object></Remove>", md5Header, 400, "MalformedXML", nil, nil, []string{"a", "b"}},
		{"not XML", "/bkt?delete", "a,b", md5Header, 400, "MalformedXML", nil, nil, []string{"a", "b"}},
		{"bad Content-MD5", "/bkt?delete", deleteBody("", "a"), func(string) map[string]string { return map[string]string{"Content-MD5": "AAAAAAAAAAAAAAAAAAAAAA=="} }, 400, "BadDigest", nil, nil, []string{"a", "b"}},
		{"invalid Content-MD5", "/bkt?delete", deleteBody("", "a"), func(string) map[string]string { return map[string]string{"Content-MD5": "nope"} }, 400, "InvalidDigest", nil, nil, []string{"a", "b"}},
		{"wrong crc32", "/bkt?delete", deleteBody("", "a"), func(string) map[string]string { return map[string]string{"x-amz-checksum-crc32": "AAAAAA=="} }, 400, "BadDigest", nil, nil, []string{"a", "b"}},
		{"no checksum", "/bkt?delete", deleteBody("", "a"), func(string) map[string]string { return nil }, 400, "InvalidRequest", nil, nil, []string{"a", "b"}},
		{"algorithm without a value", "/bkt?delete", deleteBody("", "a"), func(string) map[string]string { return map[string]string{"x-amz-sdk-checksum-algorithm": "CRC32"} }, 400, "InvalidRequest", nil, nil, []string{"a", "b"}},
		{"trailer without aws-chunked", "/bkt?delete", deleteBody("", "a"), func(string) map[string]string { return map[string]string{"x-amz-trailer": "x-amz-checksum-crc32"} }, 400, "InvalidRequest", nil, nil, []string{"a", "b"}},
		{"missing bucket", "/nope?delete", deleteBody("", "a"), md5Header, 404, "NoSuchBucket", nil, nil, nil},
		{"invalid bucket", "/BAD_BUCKET?delete", deleteBody("", "a"), md5Header, 400, "InvalidBucketName", nil, nil, nil},
	}
	for _, tt := range tests {
		for _, k := range []string{"a", "b"} {
			if _, err := st.PutObject(ctx, "bkt", k, strings.NewReader(k), store.PutOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		status, code, body := sendWith(t, srv, http.MethodPost, tt.path, tt.body, tt.header(tt.body))
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: POST %s = %d %q, want %d %q", tt.name, tt.path, status, code, tt.wantStatus, tt.wantCode)
			continue
		}
		for _, k := range tt.wantKept {
			if _, err := st.HeadObject(ctx, "bkt", k); err != nil {
				t.Errorf("%s: %q was deleted: HeadObject error = %v", tt.name, k, err)
			}
		}
		if tt.wantCode != "" {
			continue
		}
		var result struct {
			XMLName xml.Name `xml:"DeleteResult"`
			Deleted []struct{ Key string }
			Error   []struct{ Key, Code, Message string }
		}
		if err := xml.Unmarshal(body, &result); err != nil {
			t.Errorf("%s: DeleteResult = %q, %v", tt.name, body, err)
			continue
		}
		var deleted []string
		for _, d := range result.Deleted {
			deleted = append(deleted, d.Key)
		}
		gotErrors := map[string]string{}
		for _, e := range result.Error {
			gotErrors[e.Key] = e.Code
			if e.Message == "" {
				t.Errorf("%s: Error entry for %q has no Message", tt.name, e.Key)
			}
		}
		if !slices.Equal(deleted, tt.wantDeleted) || len(gotErrors) != len(tt.wantErrors) {
			t.Errorf("%s: Deleted = %v, Error = %v; want %v, %v", tt.name, deleted, gotErrors, tt.wantDeleted, tt.wantErrors)
		}
		for k, c := range tt.wantErrors {
			if gotErrors[k] != c {
				t.Errorf("%s: Error code for %.20q = %q, want %q", tt.name, k, gotErrors[k], c)
			}
		}
		for _, k := range deleted {
			if _, err := st.HeadObject(ctx, "bkt", k); !errors.Is(err, store.ErrNoSuchKey) {
				t.Errorf("%s: %q reported Deleted, HeadObject error = %v", tt.name, k, err)
			}
		}
	}
}

// TestDeleteObjectsStreaming sends the body aws-chunked with a checksum trailer.
// The SDK does not do this for DeleteObjects, but the S3 API allows it.
func TestDeleteObjectsStreaming(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	doc := deleteBody("", "a")
	good := crc32Header(doc)["x-amz-checksum-crc32"]
	tests := []struct {
		name       string
		trailer    string
		wantStatus int
		wantCode   string
		wantKept   bool
	}{
		{"matching trailer", "x-amz-checksum-crc32:" + good, http.StatusOK, "", false},
		{"wrong trailer", "x-amz-checksum-crc32:AAAAAA==", http.StatusBadRequest, "BadDigest", true},
		{"missing trailer", "", http.StatusBadRequest, "IncompleteBody", true},
	}
	for _, tt := range tests {
		if _, err := st.PutObject(ctx, "bkt", "a", strings.NewReader("a"), store.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		enc := fmt.Sprintf("%x\r\n%s\r\n0\r\n", len(doc), doc)
		if tt.trailer != "" {
			enc += tt.trailer + "\r\n"
		}
		enc += "\r\n"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/bkt?delete", strings.NewReader(enc))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(doc)))
		req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
		req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
		creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
		if err := signer.SignHTTP(ctx, creds, req, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}
		status, code := send(t, req)
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: POST = %d %q, want %d %q", tt.name, status, code, tt.wantStatus, tt.wantCode)
		}
		if _, err := st.HeadObject(ctx, "bkt", "a"); errors.Is(err, store.ErrNoSuchKey) == tt.wantKept {
			t.Errorf("%s: HeadObject error = %v, want the object kept = %v", tt.name, err, tt.wantKept)
		}
	}
}

// TestDeleteObjectsDeepXML sends a body that nests elements far past any
// valid request, which would otherwise hold one decoder stack entry each.
func TestDeleteObjectsDeepXML(t *testing.T) {
	srv, st := storeServer(t, "")
	if err := st.CreateBucket(context.Background(), "bkt"); err != nil {
		t.Fatal(err)
	}
	// Well-formed, so only the depth limit rejects it.
	body := "<Delete><Object><Key>a</Key></Object>" + strings.Repeat("<a>", 1<<16) + strings.Repeat("</a>", 1<<16) + "</Delete>"
	status, code, _ := sendWith(t, srv, http.MethodPost, "/bkt?delete", body, md5Header(body))
	if status != http.StatusBadRequest || code != "MalformedXML" {
		t.Errorf("POST with %d nested elements = %d %q, want 400 MalformedXML", 1<<16, status, code)
	}
}
