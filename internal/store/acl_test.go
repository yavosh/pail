package store

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/yavosh/pail/internal/acl"
)

func TestObjectACLPersistence(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "bucket")
	before := mustPut(t, s, "bucket", "key", "body")
	policy := acl.Private(strings.Repeat("a", 64))
	policy.Grants = append(policy.Grants, acl.Grant{Grantee: acl.Grantee{Type: "Group", URI: acl.AllUsers}, Permission: "READ"})
	if err := s.PutObjectACL(t.Context(), "bucket", "key", "", policy); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), fsys)
	if err != nil {
		t.Fatal(err)
	}
	body, after := mustGet(t, reopened, "bucket", "key")
	if body != "body" || after.ETag != before.ETag || !after.LastModified.Equal(before.LastModified) || after.Checksum != before.Checksum || after.ACL == nil || !reflect.DeepEqual(*after.ACL, policy) {
		t.Fatalf("ACL update changed object: before %+v, after %+v, body %q", before, after, body)
	}
}

func TestAnonymousCommitChecksCurrentOwner(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "bucket")
	anonymous := acl.Private(acl.AnonymousID)
	account := acl.Private(strings.Repeat("a", 64))
	body := &replaceDuringRead{replace: func() {
		if _, err := s.PutObject(t.Context(), "bucket", "key", strings.NewReader("account"), PutOptions{ACL: &account}); err != nil {
			t.Fatal(err)
		}
	}, Reader: strings.NewReader("anonymous")}
	if _, err := s.PutObject(t.Context(), "bucket", "key", body, PutOptions{Anonymous: true, ACL: &anonymous}); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("anonymous race error = %v, want ErrAccessDenied", err)
	}
	got, _ := mustGet(t, s, "bucket", "key")
	if got != "account" {
		t.Fatalf("anonymous race body = %q, want account", got)
	}
}

type replaceDuringRead struct {
	Reader  *strings.Reader
	replace func()
}

func (r *replaceDuringRead) Read(p []byte) (int, error) {
	if r.replace != nil {
		r.replace()
		r.replace = nil
	}
	return r.Reader.Read(p)
}
