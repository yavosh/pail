package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yavosh/pail/internal/acl"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/vfs/localdisk"
)

func TestOwnershipValueAndACLHeaders(t *testing.T) {
	values := []struct {
		in   string
		want bool
	}{
		{"BucketOwnerEnforced", true}, {"BucketOwnerPreferred", true}, {"ObjectWriter", true},
		{"", false}, {"Bogus", false}, {"objectwriter", false},
	}
	for _, tt := range values {
		if got := validOwnership(tt.in); got != tt.want {
			t.Errorf("validOwnership(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
	headers := []struct {
		name   string
		header http.Header
		want   bool
	}{
		{"none", http.Header{}, true},
		{"private", http.Header{"X-Amz-Acl": {"private"}}, true},
		{"owner full control", http.Header{"X-Amz-Acl": {"bucket-owner-full-control"}}, true},
		{"public read", http.Header{"X-Amz-Acl": {"public-read"}}, false},
		{"owner read", http.Header{"X-Amz-Acl": {"bucket-owner-read"}}, false},
		{"grant", http.Header{"X-Amz-Grant-Read": {`id="x"`}}, false},
	}
	for _, tt := range headers {
		t.Run(tt.name, func(t *testing.T) {
			if _, got := aclHeadersAllowed(tt.header); got != tt.want {
				t.Errorf("aclHeadersAllowed(%v) = %v, want %v", tt.header, got, tt.want)
			}
		})
	}
}

// TestEnforcedOwnerDeniesStoredGrants checks that a public-read ACL stored before
// the bucket switched to BucketOwnerEnforced no longer allows anonymous reads.
func TestEnforcedOwnerDeniesStoredGrants(t *testing.T) {
	tests := []struct {
		ownership string
		want      int
	}{
		{"", http.StatusOK},
		{"ObjectWriter", http.StatusOK},
		{"BucketOwnerPreferred", http.StatusOK},
		{"BucketOwnerEnforced", http.StatusForbidden},
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		for _, tt := range tests {
			t.Run(method+"/"+tt.ownership, func(t *testing.T) {
				fsys, err := localdisk.Open(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = fsys.Close() })
				st, err := store.Open(t.Context(), fsys)
				if err != nil {
					t.Fatal(err)
				}
				if err := st.CreateBucket(t.Context(), "owned"); err != nil {
					t.Fatal(err)
				}
				public := acl.Private(strings.Repeat("a", 64))
				public.Grants = append(public.Grants, acl.Grant{Grantee: acl.Grantee{Type: "Group", URI: acl.AllUsers}, Permission: "READ"})
				if _, err := st.PutObject(t.Context(), "owned", "key", strings.NewReader("x"), store.PutOptions{ACL: &public}); err != nil {
					t.Fatal(err)
				}
				opts := testOptions("")
				opts.Store = st
				h := New(opts).(*handler)
				if tt.ownership != "" {
					if err := h.putOwnership(t.Context(), "owned", tt.ownership); err != nil {
						t.Fatal(err)
					}
				}
				srv := httptest.NewTestServer(t, h)
				srv.Start()
				req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+"/owned/key", nil)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := srv.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != tt.want {
					t.Errorf("%s with ownership %q = %d, want %d", method, tt.ownership, resp.StatusCode, tt.want)
				}
			})
		}
	}
}

func TestOwnershipRejectsStrayInput(t *testing.T) {
	srv, _ := storeServer(t, "")
	if status, code, _ := sendWith(t, srv, http.MethodPut, "/enf", "", map[string]string{"x-amz-object-ownership": "BucketOwnerEnforced"}); status != http.StatusOK {
		t.Fatalf("create enforced bucket = %d %s, want 200", status, code)
	}
	controls := func(inner string) string { return `<OwnershipControls>` + inner + `</OwnershipControls>` }
	tests := []struct {
		name, method, path, body string
		header                   map[string]string
		wantStatus               int
		wantCode                 string
	}{
		{"re-create with a public ACL", http.MethodPut, "/enf", "", map[string]string{"x-amz-acl": "public-read"}, 400, "AccessControlListNotSupported"},
		{"re-create with a grant", http.MethodPut, "/enf", "", map[string]string{"x-amz-grant-read": `uri="http://acs.amazonaws.com/groups/global/AllUsers"`}, 400, "AccessControlListNotSupported"},
		{"re-create without ACL headers", http.MethodPut, "/enf", "", nil, 200, ""},
		{"extra root element", http.MethodPut, "/enf?ownershipControls", controls(`<Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule><Extra>x</Extra>`), nil, 400, "MalformedXML"},
		{"repeated ObjectOwnership", http.MethodPut, "/enf?ownershipControls", controls(`<Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule>`), nil, 400, "MalformedXML"},
	}
	for _, tt := range tests {
		if status, code, _ := sendWith(t, srv, tt.method, tt.path, tt.body, tt.header); status != tt.wantStatus || code != tt.wantCode {
			t.Errorf("%s: %s %s = %d %q, want %d %q", tt.name, tt.method, tt.path, status, code, tt.wantStatus, tt.wantCode)
		}
	}
	if status, _, body := sendWith(t, srv, http.MethodGet, "/enf?ownershipControls", "", nil); status != http.StatusOK || !strings.Contains(string(body), "BucketOwnerEnforced") {
		t.Errorf("ownership after rejected writes = %d %s, want BucketOwnerEnforced kept", status, body)
	}
}
