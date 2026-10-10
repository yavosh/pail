package s3api

import (
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
)

func tagDoc(pairs ...string) string {
	var b strings.Builder
	b.WriteString("<Tagging><TagSet>")
	for i := 0; i < len(pairs); i += 2 {
		b.WriteString("<Tag><Key>" + pairs[i] + "</Key><Value>" + pairs[i+1] + "</Value></Tag>")
	}
	b.WriteString("</TagSet></Tagging>")
	return b.String()
}

func TestPutObjectTagging(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	_ = call(t, srv, http.MethodPut, "/bkt/k", "x", nil)
	var eleven []string
	for i := range 11 {
		eleven = append(eleven, fmt.Sprint("k", i), "v")
	}
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"two tags", tagDoc("a", "1", "b", ""), 200, ""},
		{"empty set", tagDoc(), 200, ""},
		{"ten tags", tagDoc(eleven[:20]...), 200, ""},
		{"eleven tags", tagDoc(eleven...), 400, "BadRequest"},
		{"duplicate key", tagDoc("a", "1", "a", "2"), 400, "InvalidTag"},
		{"aws prefix", tagDoc("aws:a", "1"), 400, "InvalidTag"},
		{"empty key", tagDoc("", "1"), 400, "InvalidTag"},
		{"key 129", tagDoc(strings.Repeat("k", 129), "1"), 400, "InvalidTag"},
		{"value 257", tagDoc("k", strings.Repeat("v", 257)), 400, "InvalidTag"},
		{"malformed", "<Tagging>", 400, "MalformedXML"},
		{"empty body", "", 400, "MalformedXML"},
	}
	for _, tt := range tests {
		r := call(t, srv, http.MethodPut, "/bkt/k?tagging", tt.body, nil)
		if r.status != tt.wantStatus || r.code != tt.wantCode {
			t.Errorf("%s: PutObjectTagging = %d %q, want %d %q", tt.name, r.status, r.code, tt.wantStatus, tt.wantCode)
		}
	}
	if r := call(t, srv, http.MethodPut, "/bkt/missing?tagging", tagDoc("a", "1"), nil); r.status != 404 || r.code != "NoSuchKey" {
		t.Errorf("PutObjectTagging on a missing key = %d %q, want 404 NoSuchKey", r.status, r.code)
	}
}

func TestObjectTaggingRoundTrip(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	put := call(t, srv, http.MethodPut, "/bkt/k", "x", map[string]string{"x-amz-tagging": "color=blue&size=large"})
	etag := put.header.Get("ETag")
	head := call(t, srv, http.MethodHead, "/bkt/k", "", nil)
	if got := head.header.Get("x-amz-tagging-count"); got != "2" {
		t.Errorf("x-amz-tagging-count = %q, want 2", got)
	}
	modified := head.header.Get("Last-Modified")
	want := xml.Header + `<Tagging xmlns="` + s3Namespace + `"><TagSet><Tag><Key>color</Key><Value>blue</Value></Tag><Tag><Key>size</Key><Value>large</Value></Tag></TagSet></Tagging>`
	get := call(t, srv, http.MethodGet, "/bkt/k?tagging", "", nil)
	if get.status != 200 || get.body != want {
		t.Errorf("GetObjectTagging = %d %q, want 200 %q", get.status, get.body, want)
	}
	if _, set := get.header["Content-Type"]; set {
		t.Errorf("GetObjectTagging Content-Type = %q, want none", get.header.Get("Content-Type"))
	}
	if r := call(t, srv, http.MethodPut, "/bkt/k?tagging", tagDoc("c", "3"), nil); r.status != 200 || r.body != "" {
		t.Errorf("PutObjectTagging = %d %q, want 200 with no body", r.status, r.body)
	}
	head = call(t, srv, http.MethodHead, "/bkt/k", "", nil)
	if head.header.Get("ETag") != etag || head.header.Get("Last-Modified") != modified || head.header.Get("x-amz-tagging-count") != "1" {
		t.Errorf("HEAD after PutObjectTagging: ETag %q, Last-Modified %q, count %q, want %q, %q, 1",
			head.header.Get("ETag"), head.header.Get("Last-Modified"), head.header.Get("x-amz-tagging-count"), etag, modified)
	}
	if r := call(t, srv, http.MethodDelete, "/bkt/k?tagging", "", nil); r.status != 204 {
		t.Errorf("DeleteObjectTagging = %d, want 204", r.status)
	}
	empty := xml.Header + `<Tagging xmlns="` + s3Namespace + `"><TagSet></TagSet></Tagging>`
	if r := call(t, srv, http.MethodGet, "/bkt/k?tagging", "", nil); r.status != 200 || r.body != empty {
		t.Errorf("GetObjectTagging after delete = %d %q, want 200 %q", r.status, r.body, empty)
	}
	if got := call(t, srv, http.MethodGet, "/bkt/k", "", nil).header.Get("x-amz-tagging-count"); got != "" {
		t.Errorf("x-amz-tagging-count after delete = %q, want none", got)
	}
}

func TestTaggingOnWrites(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	_ = call(t, srv, http.MethodPut, "/bkt/src", "x", map[string]string{"x-amz-tagging": "s=1"})
	tests := []struct {
		name       string
		header     map[string]string
		wantStatus int
		wantCode   string
		wantTags   string // the stored set, as the count and first key
	}{
		{"default copies the source", nil, 200, "", "s"},
		{"COPY ignores the header", map[string]string{"x-amz-tagging-directive": "COPY", "x-amz-tagging": "n=2"}, 200, "", "s"},
		{"REPLACE takes the header", map[string]string{"x-amz-tagging-directive": "REPLACE", "x-amz-tagging": "n=2"}, 200, "", "n"},
		{"REPLACE with no header clears", map[string]string{"x-amz-tagging-directive": "REPLACE"}, 200, "", ""},
		{"unknown directive", map[string]string{"x-amz-tagging-directive": "MERGE"}, 400, "InvalidArgument", ""},
		{"lower-case directive", map[string]string{"x-amz-tagging-directive": "replace"}, 400, "InvalidArgument", ""},
		{"aws prefix", map[string]string{"x-amz-tagging-directive": "REPLACE", "x-amz-tagging": "aws:x=1"}, 400, "InvalidTag", ""},
	}
	for _, tt := range tests {
		h := map[string]string{"x-amz-copy-source": "bkt/src"}
		maps.Copy(h, tt.header)
		r := call(t, srv, http.MethodPut, "/bkt/dst", "", h)
		if r.status != tt.wantStatus || r.code != tt.wantCode {
			t.Errorf("%s: CopyObject = %d %q, want %d %q", tt.name, r.status, r.code, tt.wantStatus, tt.wantCode)
			continue
		}
		if tt.wantStatus != 200 {
			continue
		}
		get := call(t, srv, http.MethodGet, "/bkt/dst?tagging", "", nil)
		if has := strings.Contains(get.body, "<Key>"+tt.wantTags+"</Key>"); tt.wantTags != "" && !has || tt.wantTags == "" && strings.Contains(get.body, "<Tag>") {
			t.Errorf("%s: tags = %q, want key %q", tt.name, get.body, tt.wantTags)
		}
	}
}

func TestMultipartKeepsTags(t *testing.T) {
	srv, _ := storeServer(t, "")
	id := startUpload(t, srv, "mpu", map[string]string{"x-amz-tagging": "m=1"})
	part := call(t, srv, http.MethodPut, "/bkt/mpu?partNumber=1&uploadId="+id, "x", nil)
	body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + part.header.Get("ETag") + `</ETag></Part></CompleteMultipartUpload>`
	if r := call(t, srv, http.MethodPost, "/bkt/mpu?uploadId="+id, body, nil); r.status != 200 {
		t.Fatalf("CompleteMultipartUpload = %d %q", r.status, r.code)
	}
	if got := call(t, srv, http.MethodHead, "/bkt/mpu", "", nil).header.Get("x-amz-tagging-count"); got != "1" {
		t.Errorf("x-amz-tagging-count = %q, want 1", got)
	}
}

func TestBucketTagging(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	if r := call(t, srv, http.MethodGet, "/bkt?tagging", "", nil); r.status != 404 || r.code != "NoSuchTagSet" {
		t.Errorf("GetBucketTagging with none = %d %q, want 404 NoSuchTagSet", r.status, r.code)
	}
	var fifty []string
	for i := range 50 {
		fifty = append(fifty, fmt.Sprint("k", i), "v")
	}
	fiftyOne := append(slices.Clone(fifty), "extra", "v")
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"one tag", tagDoc("team", "storage"), 204, ""},
		{"fifty tags", tagDoc(fifty...), 204, ""},
		{"fifty-one tags", tagDoc(fiftyOne...), 400, "BadRequest"},
		{"aws prefix", tagDoc("aws:x", "1"), 400, "InvalidTag"},
		{"malformed", "<Tagging>", 400, "MalformedXML"},
	}
	for _, tt := range tests {
		r := call(t, srv, http.MethodPut, "/bkt?tagging", tt.body, nil)
		if r.status != tt.wantStatus || r.code != tt.wantCode {
			t.Errorf("%s: PutBucketTagging = %d %q, want %d %q", tt.name, r.status, r.code, tt.wantStatus, tt.wantCode)
		}
	}
	_ = call(t, srv, http.MethodPut, "/bkt?tagging", tagDoc("team", "storage"), nil)
	want := xml.Header + `<Tagging xmlns="` + s3Namespace + `"><TagSet><Tag><Key>team</Key><Value>storage</Value></Tag></TagSet></Tagging>`
	if r := call(t, srv, http.MethodGet, "/bkt?tagging", "", nil); r.status != 200 || r.body != want {
		t.Errorf("GetBucketTagging = %d %q, want 200 %q", r.status, r.body, want)
	}
	if r := call(t, srv, http.MethodDelete, "/bkt?tagging", "", nil); r.status != 204 {
		t.Errorf("DeleteBucketTagging = %d, want 204", r.status)
	}
	if r := call(t, srv, http.MethodGet, "/bkt?tagging", "", nil); r.status != 404 || r.code != "NoSuchTagSet" {
		t.Errorf("GetBucketTagging after delete = %d %q, want 404 NoSuchTagSet", r.status, r.code)
	}
	if r := call(t, srv, http.MethodGet, "/nobkt?tagging", "", nil); r.status != 404 || r.code != "NoSuchBucket" {
		t.Errorf("GetBucketTagging on a missing bucket = %d %q, want 404 NoSuchBucket", r.status, r.code)
	}
}

func TestLifecycleTagFilterValidation(t *testing.T) {
	srv, _ := storeServer(t, "")
	_ = call(t, srv, http.MethodPut, "/bkt", "", nil)
	rule := func(filter string, extra string) string {
		return `<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><Filter>` + filter + `</Filter>` + extra + `</Rule></LifecycleConfiguration>`
	}
	const expire = `<Expiration><Days>1</Days></Expiration>`
	const abort = `<AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload>`
	tag := func(k, v string) string { return "<Tag><Key>" + k + "</Key><Value>" + v + "</Value></Tag>" }
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"one tag", rule(tag("a", "1"), expire), 200, ""},
		{"and of tags", rule("<And>"+tag("a", "1")+tag("b", "2")+"</And>", expire), 200, ""},
		{"and of prefix and tag", rule("<And><Prefix>p/</Prefix>"+tag("a", "1")+"</And>", expire), 200, ""},
		{"two direct tags", rule(tag("a", "1")+tag("b", "2"), expire), 400, "MalformedXML"},
		{"and of one tag", rule("<And>"+tag("a", "1")+"</And>", expire), 400, "MalformedXML"},
		{"duplicate tags", rule("<And>"+tag("a", "1")+tag("a", "2")+"</And>", expire), 400, "InvalidTag"},
		{"tag with abort", rule(tag("a", "1"), abort), 400, "InvalidArgument"},
		{"tag with transition", rule(tag("a", "1"), `<Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>`), 501, "NotImplemented"},
	}
	for _, tt := range tests {
		r := call(t, srv, http.MethodPut, "/bkt?lifecycle", tt.body, nil)
		if r.status != tt.wantStatus || r.code != tt.wantCode {
			t.Errorf("%s: PutBucketLifecycleConfiguration = %d %q, want %d %q", tt.name, r.status, r.code, tt.wantStatus, tt.wantCode)
		}
	}
}
