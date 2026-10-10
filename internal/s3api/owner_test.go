package s3api

import (
	"errors"
	"io"
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/yavosh/pail/internal/account"
	"github.com/yavosh/pail/internal/store"
)

func TestCopyDestinationConditions(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := t.Context()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"src", "dest"} {
		if _, err := st.PutObject(ctx, "bkt", key, strings.NewReader(key), store.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	dest, err := st.HeadObject(ctx, "bkt", "dest")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		key        string
		header     map[string]string
		wantStatus int
		wantCode   string
		wantBody   string // destination body afterwards; empty means no object
	}{
		{"if-none-match star on existing", "dest", map[string]string{"If-None-Match": "*"}, 412, "PreconditionFailed", "dest"},
		{"if-none-match star on new", "fresh", map[string]string{"If-None-Match": "*"}, 200, "", "src"},
		{"if-match wrong", "dest", map[string]string{"If-Match": `"wrong"`}, 412, "PreconditionFailed", "dest"},
		{"if-match right", "dest", map[string]string{"If-Match": quoteETag(dest.ETag)}, 200, "", "src"},
		{"if-match on missing", "missing", map[string]string{"If-Match": quoteETag(dest.ETag)}, 404, "NoSuchKey", ""},
		{"if-none-match with an ETag", "dest", map[string]string{"If-None-Match": quoteETag(dest.ETag)}, 501, "NotImplemented", "dest"},
	}
	for _, tt := range tests {
		// Reset the destination so each case starts from the same state.
		_, _ = st.DeleteObject(ctx, "bkt", "fresh", store.DeleteOptions{})
		if _, err := st.PutObject(ctx, "bkt", "dest", strings.NewReader("dest"), store.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		header := map[string]string{"x-amz-copy-source": "bkt/src"}
		maps.Copy(header, tt.header)
		status, code, _ := sendWith(t, srv, http.MethodPut, "/bkt/"+tt.key, "", header)
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: copy = %d %q, want %d %q", tt.name, status, code, tt.wantStatus, tt.wantCode)
		}
		f, _, err := st.GetObject(ctx, "bkt", tt.key)
		got := ""
		if err == nil {
			b, _ := io.ReadAll(f)
			_ = f.Close()
			got = string(b)
		}
		if got != tt.wantBody {
			t.Errorf("%s: destination body = %q, want %q", tt.name, got, tt.wantBody)
		}
	}
}

func TestExpectedBucketOwner(t *testing.T) {
	srv, st := storeServer(t, "")
	ctx := t.Context()
	if err := st.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutObject(ctx, "bkt", "src", strings.NewReader("src"), store.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	const wrong = "111111111111"
	owner := func(v string) map[string]string { return map[string]string{"x-amz-expected-bucket-owner": v} }
	tests := []struct {
		name       string
		method     string
		path       string
		header     map[string]string
		wantStatus int
		wantCode   string
	}{
		{"get right owner", http.MethodGet, "/bkt/src", owner(account.ID), 200, ""},
		{"get wrong owner", http.MethodGet, "/bkt/src", owner(wrong), 403, "AccessDenied"},
		{"get malformed owner", http.MethodGet, "/bkt/src", owner("abc"), 400, "InvalidBucketOwnerAWSAccountID"},
		{"get 13 digits", http.MethodGet, "/bkt/src", owner("0000000000000"), 400, "InvalidBucketOwnerAWSAccountID"},
		{"get empty owner", http.MethodGet, "/bkt/src", owner(""), 400, "InvalidBucketOwnerAWSAccountID"},
		{"put wrong owner", http.MethodPut, "/bkt/new", owner(wrong), 403, "AccessDenied"},
		{"list wrong owner", http.MethodGet, "/bkt?list-type=2", owner(wrong), 403, "AccessDenied"},
		{"location wrong owner", http.MethodGet, "/bkt?location", owner(wrong), 403, "AccessDenied"},
		{"ownership wrong owner", http.MethodGet, "/bkt?ownershipControls", owner(wrong), 403, "AccessDenied"},
		{"create upload wrong owner", http.MethodPost, "/bkt/mpu?uploads", owner(wrong), 403, "AccessDenied"},
		{"copy wrong destination owner", http.MethodPut, "/bkt/copied", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-expected-bucket-owner": wrong}, 403, "AccessDenied"},
		{"copy wrong source owner", http.MethodPut, "/bkt/copied", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-source-expected-bucket-owner": wrong}, 403, "AccessDenied"},
		{"copy malformed source owner", http.MethodPut, "/bkt/copied", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-source-expected-bucket-owner": "abc"}, 400, "InvalidBucketOwnerAWSAccountID"},
		{"copy right owners", http.MethodPut, "/bkt/copied", map[string]string{"x-amz-copy-source": "bkt/src", "x-amz-source-expected-bucket-owner": account.ID, "x-amz-expected-bucket-owner": account.ID}, 200, ""},
		{"delete wrong owner", http.MethodDelete, "/bkt/src", owner(wrong), 403, "AccessDenied"},
	}
	for _, tt := range tests {
		status, code, _ := sendWith(t, srv, tt.method, tt.path, "", tt.header)
		if status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: %s %s = %d %q, want %d %q", tt.name, tt.method, tt.path, status, code, tt.wantStatus, tt.wantCode)
		}
	}
	// Failed checks change nothing: the source survives and no new key exists.
	for key, want := range map[string]error{"src": nil, "new": store.ErrNoSuchKey, "mpu": store.ErrNoSuchKey} {
		if _, err := st.HeadObject(ctx, "bkt", key); !errors.Is(err, want) {
			t.Errorf("HeadObject(%q) error = %v, want %v", key, err, want)
		}
	}
}
