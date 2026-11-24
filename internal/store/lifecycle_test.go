package store

import (
	"cmp"
	"encoding/xml"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/lifecycle"
	"github.com/yavosh/pail/internal/vfs"
)

func putLifecycle(t *testing.T, s *Store, bucket string, rules []lifecycle.Rule) {
	t.Helper()
	body, err := xml.Marshal(lifecycle.Configuration{Rules: rules})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutBucketConfiguration(t.Context(), bucket, "lifecycle", &BucketConfiguration{XML: body}); err != nil {
		t.Fatal(err)
	}
}

func TestSweepLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, fsys := newStore(t)
		mustCreate(t, s, "bucket")
		days := 1
		prefix := "tmp/"
		old := mustPut(t, s, "bucket", "tmp/old", "old")
		mustPut(t, s, "bucket", "keep", "keep")
		mustPut(t, s, "bucket", "tmp/empty", "")
		upload, err := s.CreateUpload(t.Context(), "bucket", "tmp/incomplete", UploadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		putLifecycle(t, s, "bucket", []lifecycle.Rule{
			{ID: "expire", Status: "Enabled", Filter: &lifecycle.Filter{And: &lifecycle.Filter{Prefix: &prefix, Greater: new(int64(0))}}, Expiration: &lifecycle.Expiration{Days: &days}},
			{ID: "abort", Status: "Enabled", Prefix: &prefix, Abort: &lifecycle.Abort{Days: 1}},
		})
		deadline := lifecycle.Deadline(old.LastModified, 1)
		time.Sleep(time.Until(deadline.Add(-time.Nanosecond)))
		if err := s.SweepLifecycle(t.Context(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := s.HeadObject(t.Context(), "bucket", "tmp/old"); err != nil {
			t.Fatalf("expired before UTC boundary: %v", err)
		}
		mustPut(t, s, "bucket", "tmp/replaced", "old")
		mustPut(t, s, "bucket", "tmp/replaced", "new")
		time.Sleep(time.Nanosecond)
		// Reopening the store preserves configurations and timestamps.
		reopened, err := Open(t.Context(), fsys)
		if err != nil {
			t.Fatal(err)
		}
		if err := reopened.SweepLifecycle(t.Context(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := reopened.HeadObject(t.Context(), "bucket", "tmp/old"); !errors.Is(err, ErrNoSuchKey) {
			t.Fatalf("expired object error = %v, want ErrNoSuchKey", err)
		}
		for _, key := range []string{"keep", "tmp/empty", "tmp/replaced"} {
			if _, err := reopened.HeadObject(t.Context(), "bucket", key); err != nil {
				t.Errorf("retained object %q: %v", key, err)
			}
		}
		if _, _, err := reopened.ListParts(t.Context(), "bucket", upload.Key, upload.ID); !errors.Is(err, ErrNoSuchUpload) {
			t.Fatalf("expired upload error = %v, want ErrNoSuchUpload", err)
		}
		if err := reopened.AbortUpload(t.Context(), "bucket", upload.Key, upload.ID); err != nil {
			t.Fatalf("repeat abort: %v", err)
		}
	})
}

func TestLifecycleDateAndDisabled(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "bucket")
	old := mustPut(t, s, "bucket", "expired", "body")
	mustPut(t, s, "bucket", "disabled", "body")
	expiredPrefix, disabledPrefix := "expired", "disabled"
	putLifecycle(t, s, "bucket", []lifecycle.Rule{
		{Status: "Enabled", Prefix: &expiredPrefix, Expiration: &lifecycle.Expiration{Date: "2000-01-01T00:00:00Z"}},
		{Status: "Disabled", Prefix: &disabledPrefix, Expiration: &lifecycle.Expiration{Date: "2000-01-01T00:00:00Z"}},
	})
	if err := s.SweepLifecycle(t.Context(), old.LastModified); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HeadObject(t.Context(), "bucket", "expired"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("date expiration = %v", err)
	}
	if _, err := s.HeadObject(t.Context(), "bucket", "disabled"); err != nil {
		t.Fatalf("disabled rule deleted object: %v", err)
	}
}

// lockCoverFS records changes to one metadata file made without its key lock.
type lockCoverFS struct {
	vfs.FS
	meta     string
	lock     *sync.Mutex
	unlocked []string
}

func (f *lockCoverFS) check(op string) {
	if f.lock.TryLock() {
		f.lock.Unlock()
		f.unlocked = append(f.unlocked, op)
	}
}

func (f *lockCoverFS) Remove(name string) error {
	if name == f.meta {
		f.check("remove")
	}
	return f.FS.Remove(name)
}

func (f *lockCoverFS) CreateTemp() (vfs.TempFile, error) {
	tf, err := f.FS.CreateTemp()
	return &lockCoverFile{TempFile: tf, fs: f}, err
}

type lockCoverFile struct {
	vfs.TempFile
	fs *lockCoverFS
}

func (f *lockCoverFile) Commit(name string) error {
	if name == f.fs.meta {
		f.fs.check("commit")
	}
	return f.TempFile.Commit(name)
}

// Expiry changes a key under its key lock, so a version reader, which holds
// only that lock, never sees the key halfway through an expiry.
func TestSweepLifecycleHoldsKeyLock(t *testing.T) {
	for _, status := range []string{"", VersioningEnabled, VersioningSuspended} {
		t.Run(cmp.Or(status, "Unversioned"), func(t *testing.T) {
			s, _ := newStore(t)
			mustCreate(t, s, "b")
			mustPut(t, s, "b", "k", "body")
			if status != "" {
				setVersioning(t, s, "b", status)
			}
			days := 1
			putLifecycle(t, s, "b", []lifecycle.Rule{{ID: "expire", Status: "Enabled", Expiration: &lifecycle.Expiration{Days: &days}}})
			fsys := &lockCoverFS{FS: s.fs, meta: metaFile("b", "k"), lock: s.keyLock("b", "k")}
			s.fs = fsys
			if err := s.SweepLifecycle(t.Context(), time.Now().AddDate(0, 0, 3)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.HeadObject(t.Context(), "b", "k"); !errors.Is(err, ErrNoSuchKey) {
				t.Fatalf("HeadObject after expiry error = %v, want ErrNoSuchKey", err)
			}
			if len(fsys.unlocked) > 0 {
				t.Errorf("expiry changed the metadata without the key lock: %v", fsys.unlocked)
			}
		})
	}
}
