package s3api

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

func TestValidBucketName(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"abc", true},
		{"my-bucket.v2", true},
		{"a1b", true},
		{strings.Repeat("a", 63), true},
		{"ab", false},
		{strings.Repeat("a", 64), false},
		{"Bucket", false},
		{"my_bucket", false},
		{"-abc", false},
		{"abc-", false},
		{".abc", false},
		{"a..b", false},
		{"192.168.5.4", false},
		{"xn--abc", false},
		{"sthree-abc", false},
		{"amzn-s3-demo-abc", false},
		{"abc-s3alias", false},
		{"abc--ol-s3", false},
		{"abc.mrap", false},
		{"abc--x-s3", false},
		{"abc--table-s3", false},
		{"a b", false},
		{"999.999.999.999", false},
		{"192.168.05.004", false},
		{"1.2.3", true},
		{"1.2.3.a", true},
	}
	for _, tt := range tests {
		if got := validBucketName(tt.name); got != tt.want {
			t.Errorf("validBucketName(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// storeServer serves the S3 API on a real store in a temp directory.
func storeServer(t *testing.T, domain string) (*httptest.Server, *store.Store) {
	t.Helper()
	return storeServerIn(t, domain, "us-east-1")
}

// storeServerIn is storeServer for a given region.
func storeServerIn(t *testing.T, domain, region string) (*httptest.Server, *store.Store) {
	t.Helper()
	fsys, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	st, err := store.Open(context.Background(), fsys)
	if err != nil {
		t.Fatal(err)
	}
	opts := testOptions(domain)
	opts.Region, opts.Store = region, st
	srv := httptest.NewServer(New(opts))
	t.Cleanup(srv.Close)
	return srv, st
}

// doBody sends one signed request with body and returns the status, the S3
// error code, and the raw response body.
func doBody(t *testing.T, srv *httptest.Server, method, path, body string) (int, string, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	signPayload(t, req, time.Now(), body)
	return sendRaw(t, req)
}

// send sends a signed request and returns the status and the S3 error code.
func send(t *testing.T, req *http.Request) (int, string) {
	t.Helper()
	status, code, _ := sendRaw(t, req)
	return status, code
}

func sendRaw(t *testing.T, req *http.Request) (int, string, []byte) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var e errorBody
	_ = xml.Unmarshal(b, &e)
	return resp.StatusCode, e.Code, b
}

func TestBucketErrors(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := context.Background()
	if err := st.CreateBucket(ctx, "full"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutObject(ctx, "full", "k", strings.NewReader("x"), store.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name, method, path, body string
		wantStatus               int
		wantCode                 string
	}{
		{"create invalid name", http.MethodPut, "/Bad_Name", "", http.StatusBadRequest, "InvalidBucketName"},
		{"create IP name", http.MethodPut, "/10.0.0.1", "", http.StatusBadRequest, "InvalidBucketName"},
		{"create malformed config", http.MethodPut, "/new-bucket", "<CreateBucketConfiguration>", http.StatusBadRequest, "MalformedXML"},
		{"create in another region", http.MethodPut, "/cfg-bucket", "<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>", http.StatusBadRequest, "IllegalLocationConstraintException"},
		{"create in this region", http.MethodPut, "/cfg-bucket", "<CreateBucketConfiguration><LocationConstraint>us-east-1</LocationConstraint></CreateBucketConfiguration>", http.StatusOK, ""},
		{"create with wrong root", http.MethodPut, "/root-bucket", "<Foo/>", http.StatusBadRequest, "MalformedXML"},
		{"create with leading content", http.MethodPut, "/root-bucket", "junk<CreateBucketConfiguration/>", http.StatusBadRequest, "MalformedXML"},
		{"create with prolog", http.MethodPut, "/prolog-bucket", "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>", http.StatusOK, ""},
		{"create with trailing content", http.MethodPut, "/root-bucket", "<CreateBucketConfiguration/>junk<<<", http.StatusBadRequest, "MalformedXML"},
		{"create with trailing space", http.MethodPut, "/space-bucket", "<CreateBucketConfiguration/>\n  ", http.StatusOK, ""},
		{"list with bad max-buckets", http.MethodGet, "/?max-buckets=0", "", http.StatusBadRequest, "InvalidArgument"},
		{"list with bad token", http.MethodGet, "/?continuation-token=%21%21%21", "", http.StatusBadRequest, "InvalidArgument"},
		{"create existing in us-east-1", http.MethodPut, "/full", "", http.StatusOK, ""},
		{"delete not empty", http.MethodDelete, "/full", "", http.StatusConflict, "BucketNotEmpty"},
		{"delete missing", http.MethodDelete, "/missing", "", http.StatusNotFound, "NoSuchBucket"},
		{"location missing", http.MethodGet, "/missing?location", "", http.StatusNotFound, "NoSuchBucket"},
		{"head missing", http.MethodHead, "/missing", "", http.StatusNotFound, ""},
		{"head invalid name", http.MethodHead, "/Bad_Name", "", http.StatusBadRequest, ""},
	}
	for _, tt := range tests {
		status, code, _ := doBody(t, srv, tt.method, tt.path, tt.body)
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: %s %s = %d %q, want %d %q", tt.name, tt.method, tt.path, status, code, tt.wantStatus, tt.wantCode)
		}
	}
	if _, err := st.HeadBucket(ctx, "new-bucket"); err == nil {
		t.Error("a CreateBucket with malformed XML created the bucket")
	}
}

func TestListBucketsParameters(t *testing.T) {
	srv, st := storeServer(t, "")
	for _, b := range []string{"aaa", "bbb", "bbc", "ccc"} {
		if err := st.CreateBucket(context.Background(), b); err != nil {
			t.Fatal(err)
		}
	}
	type page struct {
		Names             []string `xml:"Buckets>Bucket>Name"`
		ContinuationToken string   `xml:"ContinuationToken"`
	}
	list := func(query string) page {
		status, code, body := doBody(t, srv, http.MethodGet, "/?"+query, "")
		if status != http.StatusOK {
			t.Fatalf("GET /?%s = %d %s", query, status, code)
		}
		var p page
		if err := xml.Unmarshal(body, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	tests := []struct {
		query string
		want  []string
	}{
		{"", []string{"aaa", "bbb", "bbc", "ccc"}},
		{"prefix=bb", []string{"bbb", "bbc"}},
		{"bucket-region=eu-west-1", nil},
		{"bucket-region=us-east-1&prefix=c", []string{"ccc"}},
	}
	for _, tt := range tests {
		if got := list(tt.query).Names; !slices.Equal(got, tt.want) {
			t.Errorf("ListBuckets ?%s = %v, want %v", tt.query, got, tt.want)
		}
	}

	// Pages of two walk every bucket exactly once.
	var all []string
	query := "max-buckets=2"
	for range 10 {
		p := list(query)
		all = append(all, p.Names...)
		if p.ContinuationToken == "" {
			break
		}
		query = "max-buckets=2&continuation-token=" + p.ContinuationToken
	}
	if want := []string{"aaa", "bbb", "bbc", "ccc"}; !slices.Equal(all, want) {
		t.Errorf("paged ListBuckets = %v, want %v", all, want)
	}
}

// TestCreateExistingOutsideUSEast1 checks the non-legacy answer: only us-east-1
// lets an owner re-create a bucket.
func TestCreateExistingOutsideUSEast1(t *testing.T) {
	srv, st := storeServerIn(t, "", "eu-west-1")
	if err := st.CreateBucket(context.Background(), "bkt"); err != nil {
		t.Fatal(err)
	}
	if status, code, _ := doBody(t, srv, http.MethodPut, "/bkt", ""); status != http.StatusConflict || code != "BucketAlreadyOwnedByYou" {
		t.Errorf("PUT existing bucket in eu-west-1 = %d %q, want 409 BucketAlreadyOwnedByYou", status, code)
	}
}

func TestDecodeXMLDocumentDepth(t *testing.T) {
	nested := func(n int) string { return strings.Repeat("<a>", n) + strings.Repeat("</a>", n) }
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"at the limit", nested(maxXMLDepth), false},
		{"past the limit", nested(maxXMLDepth + 1), true},
		{"siblings are not nesting", "<a>" + strings.Repeat("<b/>", 100) + "</a>", false},
		{"unclosed past the limit", strings.Repeat("<a>", maxXMLDepth+1), true},
	}
	for _, tt := range tests {
		var v struct{ XMLName xml.Name }
		if err := decodeXMLDocument([]byte(tt.body), &v); (err != nil) != tt.wantErr {
			t.Errorf("%s: decodeXMLDocument(%.30q...) error = %v, want error = %v", tt.name, tt.body, err, tt.wantErr)
		}
	}
}
