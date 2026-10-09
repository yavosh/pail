package s3api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yavosh/pail/internal/acl"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

// replacingStore replaces the object after the authorization metadata read.
type replacingStore struct {
	Store
	afterHead func() error
}

func (s *replacingStore) HeadObject(ctx context.Context, bucket, key string) (store.ObjectInfo, error) {
	info, err := s.Store.HeadObject(ctx, bucket, key)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	if s.afterHead != nil {
		replace := s.afterHead
		s.afterHead = nil
		if err := replace(); err != nil {
			return store.ObjectInfo{}, err
		}
	}
	return info, nil
}

func TestAnonymousReadReplacement(t *testing.T) {
	private := acl.Private(strings.Repeat("a", 64))
	public := acl.Private(private.Owner.ID)
	public.Grants = append(public.Grants, acl.Grant{Grantee: acl.Grantee{Type: "Group", URI: acl.AllUsers}, Permission: "READ"})
	tests := []struct {
		name   string
		policy *acl.Policy
		status int
	}{
		{"private", &private, http.StatusForbidden},
		{"missing ACL", nil, http.StatusForbidden},
		{"public", &public, http.StatusOK},
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		for _, tt := range tests {
			t.Run(method+"/"+tt.name, func(t *testing.T) {
				fsys, err := localdisk.Open(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = fsys.Close() })
				st, err := store.Open(t.Context(), fsys)
				if err != nil {
					t.Fatal(err)
				}
				if err := st.CreateBucket(t.Context(), "acl-race"); err != nil {
					t.Fatal(err)
				}
				if _, err := st.PutObject(t.Context(), "acl-race", "key", strings.NewReader("public"), store.PutOptions{ACL: &public}); err != nil {
					t.Fatal(err)
				}
				opts := testOptions("")
				opts.Store = &replacingStore{Store: st, afterHead: func() error {
					_, err := st.PutObject(t.Context(), "acl-race", "key", strings.NewReader("replacement"), store.PutOptions{ACL: tt.policy})
					return err
				}}
				srv := httptest.NewTestServer(t, New(opts))
				srv.Start()
				req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+"/acl-race/key", nil)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := srv.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != tt.status {
					t.Fatalf("%s replacement status = %d, want %d", method, resp.StatusCode, tt.status)
				}
				if tt.status == http.StatusForbidden && (resp.Header.Get("ETag") != "" || strings.Contains(string(body), "replacement")) {
					t.Fatalf("%s denied replacement exposed ETag or body: %v, %q", method, resp.Header, body)
				}
				if tt.status == http.StatusOK && method == http.MethodGet && string(body) != "replacement" {
					t.Errorf("public replacement body = %q, want replacement", body)
				}
			})
		}
	}
}
