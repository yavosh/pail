package s3api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yavosh/pail/internal/store"
)

// partsServer stores "simple" (5 bytes) and "multi" (a 5 MiB part and "tail",
// both with CRC32) in bucket bkt, and returns the multi object's part checksums.
func partsServer(t *testing.T) (srv *httptest.Server, crc1, crc2 string) {
	t.Helper()
	srv, _ = storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	if r := call(t, srv, http.MethodPut, "/bkt/simple", "hello", nil); r.status != http.StatusOK {
		t.Fatalf("PUT simple = %d, want 200", r.status)
	}
	id := startUpload(t, srv, "multi", map[string]string{"x-amz-checksum-algorithm": "CRC32"})
	big := strings.Repeat("a", store.MinPartSize)
	r1 := putPart(t, srv, "multi", id, 1, big, nil)
	r2 := putPart(t, srv, "multi", id, 2, "tail", nil)
	crc1, crc2 = r1.header.Get("x-amz-checksum-crc32"), r2.header.Get("x-amz-checksum-crc32")
	body := fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag><ChecksumCRC32>%s</ChecksumCRC32></Part>"+
		"<Part><PartNumber>2</PartNumber><ETag>%s</ETag><ChecksumCRC32>%s</ChecksumCRC32></Part></CompleteMultipartUpload>",
		r1.header.Get("ETag"), crc1, r2.header.Get("ETag"), crc2)
	if r := call(t, srv, http.MethodPost, "/bkt/multi?uploadId="+id, body, nil); r.status != http.StatusOK {
		t.Fatalf("complete multi = %d %s, want 200", r.status, r.body)
	}
	return srv, crc1, crc2
}

func TestPartReads(t *testing.T) {
	srv, _, _ := partsServer(t)
	_ = call(t, srv, http.MethodPut, "/bkt/cold", "x", map[string]string{"x-amz-storage-class": "GLACIER"})
	total := store.MinPartSize + 4
	tests := []struct {
		name, method, key, query string
		header                   map[string]string
		wantStatus               int
		wantCode                 string
		wantRange, wantCount     string
		wantLength               int
		wantBody                 string
	}{
		{name: "simple part 1", method: http.MethodGet, key: "simple", query: "partNumber=1", wantStatus: 206, wantRange: "bytes 0-4/5", wantLength: 5, wantBody: "hello"},
		{name: "simple part 1 head", method: http.MethodHead, key: "simple", query: "partNumber=1", wantStatus: 206, wantRange: "bytes 0-4/5", wantLength: 5},
		{name: "simple part 2", method: http.MethodGet, key: "simple", query: "partNumber=2", wantStatus: 416, wantCode: "InvalidPartNumber"},
		{name: "multi part 1 head", method: http.MethodHead, key: "multi", query: "partNumber=1", wantStatus: 206, wantCount: "2", wantLength: store.MinPartSize,
			wantRange: fmt.Sprintf("bytes 0-%d/%d", store.MinPartSize-1, total)},
		{name: "multi part 2", method: http.MethodGet, key: "multi", query: "partNumber=2", wantStatus: 206, wantCount: "2", wantLength: 4, wantBody: "tail",
			wantRange: fmt.Sprintf("bytes %d-%d/%d", store.MinPartSize, total-1, total)},
		{name: "multi part 3 head", method: http.MethodHead, key: "multi", query: "partNumber=3", wantStatus: 416},
		{name: "multi part 3", method: http.MethodGet, key: "multi", query: "partNumber=3", wantStatus: 416, wantCode: "InvalidPartNumber"},
		{name: "part 0", method: http.MethodGet, key: "multi", query: "partNumber=0", wantStatus: 400, wantCode: "InvalidArgument"},
		{name: "part abc", method: http.MethodGet, key: "multi", query: "partNumber=abc", wantStatus: 400, wantCode: "InvalidArgument"},
		{name: "part -1", method: http.MethodGet, key: "multi", query: "partNumber=-1", wantStatus: 400, wantCode: "InvalidArgument"},
		{name: "part with range", method: http.MethodGet, key: "multi", query: "partNumber=2", header: map[string]string{"Range": "bytes=0-1"}, wantStatus: 400, wantCode: "InvalidRequest"},
		{name: "part missing key", method: http.MethodGet, key: "none", query: "partNumber=1", wantStatus: 404, wantCode: "NoSuchKey"},
		{name: "if-match miss", method: http.MethodGet, key: "multi", query: "partNumber=2", header: map[string]string{"If-Match": `"nope"`}, wantStatus: 412, wantCode: "PreconditionFailed"},
		{name: "archived", method: http.MethodGet, key: "cold", query: "partNumber=1", wantStatus: 403, wantCode: "InvalidObjectState"},
		{name: "response override", method: http.MethodGet, key: "simple", query: "partNumber=1&response-content-type=text%2Fplain", wantStatus: 206, wantRange: "bytes 0-4/5", wantLength: 5, wantBody: "hello"},
		{name: "no part", method: http.MethodHead, key: "multi", wantStatus: 200, wantLength: total},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := call(t, srv, tt.method, "/bkt/"+tt.key+"?"+tt.query, "", tt.header)
			if r.status != tt.wantStatus || r.code != tt.wantCode {
				t.Fatalf("%s %s?%s = %d %q, want %d %q", tt.method, tt.key, tt.query, r.status, r.code, tt.wantStatus, tt.wantCode)
			}
			if got := r.header.Get("Content-Range"); got != tt.wantRange {
				t.Errorf("Content-Range = %q, want %q", got, tt.wantRange)
			}
			if got := r.header.Get("x-amz-mp-parts-count"); got != tt.wantCount {
				t.Errorf("x-amz-mp-parts-count = %q, want %q", got, tt.wantCount)
			}
			if tt.wantStatus < 300 {
				if got := r.header.Get("Content-Length"); got != fmt.Sprint(tt.wantLength) {
					t.Errorf("Content-Length = %q, want %d", got, tt.wantLength)
				}
			}
			if tt.method == http.MethodGet && tt.wantStatus < 300 && tt.wantBody != "" && r.body != tt.wantBody {
				t.Errorf("body = %q, want %q", r.body, tt.wantBody)
			}
		})
	}
	if r := call(t, srv, http.MethodGet, "/bkt/simple?partNumber=1&response-content-type=text%2Fplain", "", nil); r.header.Get("Content-Type") != "text/plain" {
		t.Errorf("Content-Type with response override = %q, want text/plain", r.header.Get("Content-Type"))
	}
}

func TestPartSpan(t *testing.T) {
	multi := store.ObjectInfo{Size: 30, Parts: []store.ObjectPart{{PartNumber: 1, Size: 10}, {PartNumber: 3, Size: 20}}}
	tests := []struct {
		name        string
		info        store.ObjectInfo
		n           int64
		first, last int64
		ok          bool
	}{
		{"first part", multi, 1, 0, 9, true},
		{"second part by its number", multi, 3, 10, 29, true},
		{"missing number", multi, 2, 0, 0, false},
		{"no layout is one part", store.ObjectInfo{Size: 7}, 1, 0, 6, true},
		{"no layout part 2", store.ObjectInfo{Size: 7}, 2, 0, 0, false},
	}
	for _, tt := range tests {
		first, last, ok := partSpan(tt.info, tt.n)
		if first != tt.first || last != tt.last || ok != tt.ok {
			t.Errorf("%s: partSpan(%d) = %d, %d, %v, want %d, %d, %v", tt.name, tt.n, first, last, ok, tt.first, tt.last, tt.ok)
		}
	}
}

func TestGetObjectAttributes(t *testing.T) {
	srv, crc1, crc2 := partsServer(t)
	const all = "ETag,Checksum,ObjectParts,StorageClass,ObjectSize"
	part1 := "<Part><PartNumber>1</PartNumber><Size>5242880</Size><ChecksumCRC32>" + crc1 + "</ChecksumCRC32></Part>"
	part2 := "<Part><PartNumber>2</PartNumber><Size>4</Size><ChecksumCRC32>" + crc2 + "</ChecksumCRC32></Part>"
	tests := []struct {
		name, key  string
		header     map[string]string
		wantStatus int
		wantCode   string
		want       []string // substrings of the body, in order
		notWant    []string
	}{
		{name: "simple all", key: "simple", header: map[string]string{"x-amz-object-attributes": all}, wantStatus: 200,
			want:    []string{"<ETag>5d41402abc4b2a76b9719d911017c592</ETag>", "<ChecksumType>FULL_OBJECT</ChecksumType>", "<StorageClass>STANDARD</StorageClass>", "<ObjectSize>5</ObjectSize>"},
			notWant: []string{"ObjectParts"}},
		{name: "multi all", key: "multi", header: map[string]string{"x-amz-object-attributes": all}, wantStatus: 200,
			want: []string{"<ETag>", "<ChecksumType>COMPOSITE</ChecksumType>", "<PartsCount>2</PartsCount><PartNumberMarker>0</PartNumberMarker><NextPartNumberMarker>2</NextPartNumberMarker><MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated>" + part1 + part2,
				"<StorageClass>STANDARD</StorageClass>", "<ObjectSize>5242884</ObjectSize>"}},
		{name: "only size", key: "multi", header: map[string]string{"x-amz-object-attributes": "ObjectSize"}, wantStatus: 200,
			want: []string{"<ObjectSize>5242884</ObjectSize>"}, notWant: []string{"ETag", "Checksum", "ObjectParts", "StorageClass"}},
		{name: "split list with spaces", key: "simple", header: map[string]string{"x-amz-object-attributes": "ETag, ObjectSize"}, wantStatus: 200,
			want: []string{"<ETag>", "<ObjectSize>5</ObjectSize>"}, notWant: []string{"StorageClass"}},
		{name: "max parts", key: "multi", header: map[string]string{"x-amz-object-attributes": "ObjectParts", "x-amz-max-parts": "1"}, wantStatus: 200,
			want: []string{"<PartNumberMarker>0</PartNumberMarker><NextPartNumberMarker>1</NextPartNumberMarker><MaxParts>1</MaxParts><IsTruncated>true</IsTruncated>" + part1}, notWant: []string{"<PartNumber>2"}},
		{name: "marker", key: "multi", header: map[string]string{"x-amz-object-attributes": "ObjectParts", "x-amz-part-number-marker": "1"}, wantStatus: 200,
			want: []string{"<PartNumberMarker>1</PartNumberMarker><NextPartNumberMarker>2</NextPartNumberMarker><MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated>" + part2}, notWant: []string{"<PartNumber>1<"}},
		{name: "marker past the end", key: "multi", header: map[string]string{"x-amz-object-attributes": "ObjectParts", "x-amz-part-number-marker": "2"}, wantStatus: 200,
			want: []string{"<PartNumberMarker>2</PartNumberMarker><NextPartNumberMarker>0</NextPartNumberMarker>", "<IsTruncated>false</IsTruncated>"}, notWant: []string{"<Part>"}},
		{name: "bad max parts", key: "multi", header: map[string]string{"x-amz-object-attributes": "ObjectParts", "x-amz-max-parts": "x"}, wantStatus: 400, wantCode: "InvalidArgument"},
		{name: "bad marker", key: "multi", header: map[string]string{"x-amz-object-attributes": "ObjectParts", "x-amz-part-number-marker": "-1"}, wantStatus: 400, wantCode: "InvalidArgument"},
		{name: "no header", key: "simple", wantStatus: 400, wantCode: "InvalidRequest"},
		{name: "empty header", key: "simple", header: map[string]string{"x-amz-object-attributes": " , "}, wantStatus: 400, wantCode: "InvalidRequest"},
		{name: "unknown attribute", key: "simple", header: map[string]string{"x-amz-object-attributes": "ETag,Bogus"}, wantStatus: 400, wantCode: "InvalidArgument"},
		{name: "lower case attribute", key: "simple", header: map[string]string{"x-amz-object-attributes": "etag"}, wantStatus: 400, wantCode: "InvalidArgument"},
		{name: "missing key", key: "none", header: map[string]string{"x-amz-object-attributes": all}, wantStatus: 404, wantCode: "NoSuchKey"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := call(t, srv, http.MethodGet, "/bkt/"+tt.key+"?attributes", "", tt.header)
			if r.status != tt.wantStatus || r.code != tt.wantCode {
				t.Fatalf("GET %s?attributes = %d %q %s, want %d %q", tt.key, r.status, r.code, r.body, tt.wantStatus, tt.wantCode)
			}
			if tt.wantStatus != http.StatusOK {
				return
			}
			if r.header.Get("Last-Modified") == "" {
				t.Error("Last-Modified is empty, want a date")
			}
			rest := r.body
			for _, s := range tt.want {
				i := strings.Index(rest, s)
				if i < 0 {
					t.Fatalf("body = %s, want %q in order", r.body, s)
				}
				rest = rest[i+len(s):]
			}
			for _, s := range tt.notWant {
				if strings.Contains(r.body, s) {
					t.Errorf("body = %s, want no %q", r.body, s)
				}
			}
		})
	}
}
