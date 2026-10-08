package store

import (
	"crypto/md5"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/yavosh/pail/internal/vfs"
)

func TestConditionalDelete(t *testing.T) {
	const body = "original"
	etag := fmt.Sprintf("%x", md5.Sum([]byte(body)))
	for _, tc := range []struct {
		name    string
		exists  bool
		ifMatch *string
		want    error
	}{
		{"unconditional existing", true, nil, nil},
		{"matching quoted", true, new(`"` + etag + `"`), nil},
		{"matching unquoted", true, new(etag), nil},
		{"wildcard existing", true, new("*"), nil},
		{"mismatch", true, new(`"wrong"`), ErrPreconditionFailed},
		{"empty condition", true, new(""), ErrPreconditionFailed},
		{"empty ETag", true, new(`""`), ErrPreconditionFailed},
		{"weak ETag", true, new(`W/"` + etag + `"`), ErrPreconditionFailed},
		{"unconditional missing", false, nil, nil},
		{"ETag missing", false, new(etag), ErrNoSuchKey},
		{"wildcard missing", false, new("*"), ErrNoSuchKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, fsys := newStore(t)
			mustCreate(t, s, "bucket")
			var before ObjectInfo
			if tc.exists {
				before = mustPut(t, s, "bucket", "key", body)
			}
			if err := s.DeleteObject(t.Context(), "bucket", "key", DeleteOptions{IfMatch: tc.ifMatch}); !errors.Is(err, tc.want) {
				t.Fatalf("DeleteObject(IfMatch %v, exists %v) = %v, want %v", tc.ifMatch, tc.exists, err, tc.want)
			}
			if tc.exists && tc.want != nil {
				got, after := mustGet(t, s, "bucket", "key")
				if got != body || after.ETag != before.ETag || !after.LastModified.Equal(before.LastModified) || after.Checksum != before.Checksum {
					t.Errorf("failed delete changed object: body %q, info %+v, want %q, %+v", got, after, body, before)
				}
				if n := dirLen(t, fsys, "buckets/bucket/blobs"); n != 1 {
					t.Errorf("blobs after failed delete = %d, want 1", n)
				}
				return
			}
			if _, err := s.HeadObject(t.Context(), "bucket", "key"); !errors.Is(err, ErrNoSuchKey) {
				t.Errorf("HeadObject after delete = %v, want ErrNoSuchKey", err)
			}
			if n := dirLen(t, fsys, "buckets/bucket/blobs"); n != 0 {
				t.Errorf("blobs after delete = %d, want 0", n)
			}
		})
	}
}

func TestConditionalDeleteUsesCurrentETag(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "bucket")
	old := mustPut(t, s, "bucket", "key", "old")
	mustPut(t, s, "bucket", "key", "replacement")
	if err := s.DeleteObject(t.Context(), "bucket", "key", DeleteOptions{IfMatch: new(old.ETag)}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("delete with stale ETag = %v, want ErrPreconditionFailed", err)
	}
	if got, _ := mustGet(t, s, "bucket", "key"); got != "replacement" {
		t.Errorf("body after stale delete = %q, want replacement", got)
	}
}

func TestConditionalDeleteDuringOverwrite(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "bucket")
	old := mustPut(t, s, "bucket", "key", "old")
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := s.PutObject(t.Context(), "bucket", "key", strings.NewReader("replacement"), PutOptions{}); err != nil {
			t.Errorf("overwrite error = %v, want nil", err)
		}
	})
	for range 16 {
		wg.Go(func() {
			err := s.DeleteObject(t.Context(), "bucket", "key", DeleteOptions{IfMatch: new(old.ETag)})
			if err != nil && !errors.Is(err, ErrNoSuchKey) && !errors.Is(err, ErrPreconditionFailed) {
				t.Errorf("concurrent delete error = %v, want nil, ErrNoSuchKey, or ErrPreconditionFailed", err)
			}
		})
	}
	wg.Wait()
	if got, _ := mustGet(t, s, "bucket", "key"); got != "replacement" {
		t.Errorf("body after overwrite and stale deletes = %q, want replacement", got)
	}
	if n := dirLen(t, fsys, "buckets/bucket/blobs"); n != 1 {
		t.Errorf("blobs after concurrent deletes = %d, want 1", n)
	}
}

func TestConditionalDeleteHoldsKeyLock(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "bucket")
	info := mustPut(t, s, "bucket", "key", "body")
	fsys := &deleteLockFS{FS: s.fs, metadata: metaFile("bucket", "key"), lock: s.keyLock("bucket", "key")}
	s.fs = fsys
	if err := s.DeleteObject(t.Context(), "bucket", "key", DeleteOptions{IfMatch: new(info.ETag)}); err != nil {
		t.Fatal(err)
	}
	if !fsys.readLocked || !fsys.removeLocked {
		t.Errorf("key lock during metadata read/delete = %v/%v, want true/true", fsys.readLocked, fsys.removeLocked)
	}
}

// deleteLockFS observes lock coverage without scheduling a competing writer.
type deleteLockFS struct {
	vfs.FS
	metadata                 string
	lock                     *sync.Mutex
	readLocked, removeLocked bool
}

func (f *deleteLockFS) locked() bool {
	if f.lock.TryLock() {
		f.lock.Unlock()
		return false
	}
	return true
}

func (f *deleteLockFS) Open(name string) (vfs.File, error) {
	if name == f.metadata {
		f.readLocked = f.locked()
	}
	return f.FS.Open(name)
}

func (f *deleteLockFS) Remove(name string) error {
	if name == f.metadata {
		f.removeLocked = f.locked()
	}
	return f.FS.Remove(name)
}
