package s3api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

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
	}
	for _, tt := range tests {
		if got := validBucketName(tt.name); got != tt.want {
			t.Errorf("validBucketName(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// storeServer serves the S3 API on a real store in a temp directory.
func storeServer(t *testing.T) (*httptest.Server, *store.Store) {
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
	opts := testOptions("")
	opts.Region, opts.Store = "us-east-1", st
	srv := httptest.NewServer(New(opts))
	t.Cleanup(srv.Close)
	return srv, st
}

// doBody sends one signed request with body and returns the status and the S3 error code.
func doBody(t *testing.T, srv *httptest.Server, method, path, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	if err := signer.SignHTTP(req.Context(), creds, req, hash, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var e errorBody
	_ = xml.Unmarshal(b, &e)
	return resp.StatusCode, e.Code
}

func TestBucketErrors(t *testing.T) {
	srv, st := storeServer(t)
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
		{"create with config", http.MethodPut, "/cfg-bucket", "<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>", http.StatusOK, ""},
		{"create existing", http.MethodPut, "/full", "", http.StatusConflict, "BucketAlreadyOwnedByYou"},
		{"delete not empty", http.MethodDelete, "/full", "", http.StatusConflict, "BucketNotEmpty"},
		{"delete missing", http.MethodDelete, "/missing", "", http.StatusNotFound, "NoSuchBucket"},
		{"location missing", http.MethodGet, "/missing?location", "", http.StatusNotFound, "NoSuchBucket"},
		{"head missing", http.MethodHead, "/missing", "", http.StatusNotFound, ""},
		{"head invalid name", http.MethodHead, "/Bad_Name", "", http.StatusBadRequest, ""},
	}
	for _, tt := range tests {
		status, code := doBody(t, srv, tt.method, tt.path, tt.body)
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: %s %s = %d %q, want %d %q", tt.name, tt.method, tt.path, status, code, tt.wantStatus, tt.wantCode)
		}
	}
	if _, err := st.HeadBucket(ctx, "new-bucket"); err == nil {
		t.Error("a CreateBucket with malformed XML created the bucket")
	}
}
