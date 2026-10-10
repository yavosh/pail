package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/acl"
	"github.com/yavosh/pail/internal/lifecycle"
	"github.com/yavosh/pail/internal/tag"
	"github.com/yavosh/pail/internal/vfs"
)

func setVersioning(t *testing.T, s *Store, bucket, status string) {
	t.Helper()
	if err := s.PutBucketConfiguration(t.Context(), bucket, "versioning", &BucketConfiguration{Status: status}); err != nil {
		t.Fatalf("set versioning %q: %v", status, err)
	}
}

func mustDelete(t *testing.T, s *Store, bucket, key string, opts DeleteOptions) DeleteResult {
	t.Helper()
	res, err := s.DeleteObject(t.Context(), bucket, key, opts)
	if err != nil {
		t.Fatalf("DeleteObject(%q, %+v) error = %v", key, opts, err)
	}
	return res
}

// versionList renders ListVersions as "key:version" entries, newest first,
// with "*" on the latest and "DM" on a delete marker. body names a version by its content.
func versionList(t *testing.T, s *Store, bucket string, names map[string]string) []string {
	t.Helper()
	all, err := s.ListVersions(t.Context(), bucket, "", "")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, v := range all {
		label := cmpName(names, v.VersionID)
		if v.DeleteMarker {
			label = "DM" + label
		}
		if v.IsLatest {
			label += "*"
		}
		out = append(out, v.Key+":"+label)
	}
	return out
}

func cmpName(names map[string]string, id string) string {
	if id == "" {
		return "null"
	}
	if n, ok := names[id]; ok {
		return n
	}
	return id
}

func TestEnabledPutsKeepVersions(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	pre := mustPut(t, s, "b", "k", "pre")
	if pre.VersionID != "" {
		t.Fatalf("PutObject before versioning VersionID = %q, want empty", pre.VersionID)
	}
	setVersioning(t, s, "b", VersioningEnabled)
	v1, v2 := mustPut(t, s, "b", "k", "one"), mustPut(t, s, "b", "k", "two")
	for _, id := range []string{v1.VersionID, v2.VersionID} {
		if len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" {
			t.Errorf("version ID %q, want 32 lowercase hex characters", id)
		}
	}
	if v1.VersionID >= v2.VersionID {
		t.Errorf("version IDs %q, %q do not sort by age", v1.VersionID, v2.VersionID)
	}
	names := map[string]string{v1.VersionID: "v1", v2.VersionID: "v2"}
	if got, want := versionList(t, s, "b", names), []string{"k:v2*", "k:v1", "k:null"}; !slices.Equal(got, want) {
		t.Errorf("ListVersions = %v, want %v", got, want)
	}
	tests := []struct {
		version string
		want    string
		wantErr error
	}{
		{"", "two", nil},
		{v2.VersionID, "two", nil},
		{v1.VersionID, "one", nil},
		{"null", "pre", nil},
		{"0123456789abcdef0123456789abcdef", "", ErrNoSuchVersion},
	}
	for _, tt := range tests {
		f, _, err := s.GetObjectVersion(t.Context(), "b", "k", tt.version)
		if !errors.Is(err, tt.wantErr) {
			t.Errorf("GetObjectVersion(%q) error = %v, want %v", tt.version, err, tt.wantErr)
			continue
		}
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(f)
		_ = f.Close()
		if string(body) != tt.want {
			t.Errorf("GetObjectVersion(%q) = %q, want %q", tt.version, body, tt.want)
		}
	}
	for _, tt := range []struct {
		key, version string
		want         error
	}{{"missing", "null", ErrNoSuchKey}, {"missing", "0123456789abcdef0123456789abcdef", ErrNoSuchVersion}} {
		if _, err := s.HeadObjectVersion(t.Context(), "b", tt.key, tt.version); !errors.Is(err, tt.want) {
			t.Errorf("HeadObjectVersion(%q, %q) error = %v, want %v", tt.key, tt.version, err, tt.want)
		}
	}
}

func TestVersioningNeverReturnsToUnversioned(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	if got, err := s.BucketVersioning(t.Context(), "b"); err != nil || got != "" {
		t.Fatalf("BucketVersioning = %q, %v, want empty", got, err)
	}
	setVersioning(t, s, "b", VersioningEnabled)
	setVersioning(t, s, "b", VersioningSuspended)
	if got, err := s.BucketVersioning(t.Context(), "b"); err != nil || got != VersioningSuspended {
		t.Errorf("BucketVersioning = %q, %v, want Suspended", got, err)
	}
	if _, err := s.BucketVersioning(t.Context(), "nope"); !errors.Is(err, ErrNoSuchBucket) {
		t.Errorf("BucketVersioning on a missing bucket error = %v, want ErrNoSuchBucket", err)
	}
}

func TestSuspendedWritesReplaceNullVersion(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	v1 := mustPut(t, s, "b", "k", "one")
	setVersioning(t, s, "b", VersioningSuspended)
	n1 := mustPut(t, s, "b", "k", "null1")
	n2 := mustPut(t, s, "b", "k", "null2")
	if n1.VersionID != "" || n2.VersionID != "" {
		t.Errorf("suspended writes returned version IDs %q, %q, want none", n1.VersionID, n2.VersionID)
	}
	names := map[string]string{v1.VersionID: "v1"}
	if got, want := versionList(t, s, "b", names), []string{"k:null*", "k:v1"}; !slices.Equal(got, want) {
		t.Errorf("ListVersions = %v, want %v", got, want)
	}
	if body, _ := mustGet(t, s, "b", "k"); body != "null2" {
		t.Errorf("current body = %q, want null2", body)
	}
	if n := dirLen(t, fsys, blobsDir("b")); n != 2 {
		t.Errorf("blobs = %d, want 2 (the replaced null body is removed)", n)
	}

	// A null version that was made noncurrent is replaced by the next suspended write.
	setVersioning(t, s, "b", VersioningEnabled)
	v2 := mustPut(t, s, "b", "k", "two")
	names[v2.VersionID] = "v2"
	if got, want := versionList(t, s, "b", names), []string{"k:v2*", "k:null", "k:v1"}; !slices.Equal(got, want) {
		t.Errorf("ListVersions after enabling = %v, want %v", got, want)
	}
	setVersioning(t, s, "b", VersioningSuspended)
	mustPut(t, s, "b", "k", "null3")
	if got, want := versionList(t, s, "b", names), []string{"k:null*", "k:v2", "k:v1"}; !slices.Equal(got, want) {
		t.Errorf("ListVersions after the next null write = %v, want %v", got, want)
	}
	if n := dirLen(t, fsys, blobsDir("b")); n != 3 {
		t.Errorf("blobs = %d, want 3", n)
	}
}

func TestDeleteMarkers(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	v1 := mustPut(t, s, "b", "k", "one")
	res := mustDelete(t, s, "b", "k", DeleteOptions{})
	if !res.Versioned || !res.DeleteMarker || len(res.VersionID) != 32 {
		t.Fatalf("DeleteObject = %+v, want a delete marker with a version ID", res)
	}
	names := map[string]string{v1.VersionID: "v1", res.VersionID: "m1"}

	_, err := s.HeadObject(t.Context(), "b", "k")
	marker, ok := errors.AsType[*DeleteMarkerError](err)
	if !ok || !errors.Is(err, ErrNoSuchKey) || marker.VersionID != res.VersionID || marker.Specific {
		t.Errorf("HeadObject under a marker error = %v, want a latest DeleteMarkerError for %q", err, res.VersionID)
	}
	_, _, err = s.GetObjectVersion(t.Context(), "b", "k", res.VersionID)
	if marker, ok := errors.AsType[*DeleteMarkerError](err); !ok || !marker.Specific {
		t.Errorf("GetObjectVersion of a marker error = %v, want a specific DeleteMarkerError", err)
	}
	if body := mustGetVersion(t, s, "b", "k", v1.VersionID); body != "one" {
		t.Errorf("version under the marker = %q, want one", body)
	}
	if objects, err := s.ListObjects(t.Context(), "b", "", ""); err != nil || len(objects) != 0 {
		t.Errorf("ListObjects = %v, %v, want none: the key is deleted", objects, err)
	}
	if got, want := versionList(t, s, "b", names), []string{"k:DMm1*", "k:v1"}; !slices.Equal(got, want) {
		t.Errorf("ListVersions = %v, want %v", got, want)
	}
	if n := dirLen(t, fsys, blobsDir("b")); n != 1 {
		t.Errorf("blobs = %d, want 1: a marker has no body", n)
	}

	// A second delete stacks another marker, also for a key that never existed.
	res2 := mustDelete(t, s, "b", "k", DeleteOptions{})
	res3 := mustDelete(t, s, "b", "never", DeleteOptions{})
	if !res2.DeleteMarker || res2.VersionID == res.VersionID || !res3.DeleteMarker {
		t.Errorf("further deletes = %+v, %+v, want new delete markers", res2, res3)
	}

	// Suspended: the marker is the null version and replaces a null object.
	setVersioning(t, s, "b", VersioningSuspended)
	mustPut(t, s, "b", "n", "body")
	res4 := mustDelete(t, s, "b", "n", DeleteOptions{})
	if !res4.Versioned || !res4.DeleteMarker || res4.VersionID != "" {
		t.Errorf("suspended DeleteObject = %+v, want a null marker", res4)
	}
	all, _ := s.ListVersions(t.Context(), "b", "n", "")
	all = slices.DeleteFunc(all, func(v VersionInfo) bool { return v.Key != "n" })
	if len(all) != 1 || !all[0].DeleteMarker || all[0].VersionID != "" {
		t.Errorf("ListVersions(n) = %+v, want only a null marker", all)
	}
}

func mustGetVersion(t *testing.T, s *Store, bucket, key, version string) string {
	t.Helper()
	f, _, err := s.GetObjectVersion(t.Context(), bucket, key, version)
	if err != nil {
		t.Fatalf("GetObjectVersion(%q, %q) error = %v", key, version, err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestPermanentDelete(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	mustPut(t, s, "b", "k", "pre")
	setVersioning(t, s, "b", VersioningEnabled)
	v1, v2 := mustPut(t, s, "b", "k", "one"), mustPut(t, s, "b", "k", "two")
	marker := mustDelete(t, s, "b", "k", DeleteOptions{})
	names := map[string]string{v1.VersionID: "v1", v2.VersionID: "v2", marker.VersionID: "m"}

	// Removing the marker restores the object.
	res := mustDelete(t, s, "b", "k", DeleteOptions{VersionID: marker.VersionID})
	if !res.DeleteMarker || res.VersionID != marker.VersionID || !res.Versioned {
		t.Errorf("removing a marker = %+v", res)
	}
	if body, info := mustGet(t, s, "b", "k"); body != "two" || info.VersionID != v2.VersionID {
		t.Errorf("after removing the marker: %q, %q, want two, %q", body, info.VersionID, v2.VersionID)
	}

	// Removing a noncurrent version leaves the current one and drops the blob.
	mustDelete(t, s, "b", "k", DeleteOptions{VersionID: v1.VersionID})
	if got, want := versionList(t, s, "b", names), []string{"k:v2*", "k:null"}; !slices.Equal(got, want) {
		t.Errorf("ListVersions = %v, want %v", got, want)
	}
	if n := dirLen(t, fsys, blobsDir("b")); n != 2 {
		t.Errorf("blobs = %d, want 2", n)
	}

	// Removing the current version promotes the newest remaining one.
	mustDelete(t, s, "b", "k", DeleteOptions{VersionID: v2.VersionID})
	if body, info := mustGet(t, s, "b", "k"); body != "pre" || info.VersionID != "" {
		t.Errorf("after removing the current version: %q, %q, want pre, null", body, info.VersionID)
	}
	if got, want := versionList(t, s, "b", names), []string{"k:null*"}; !slices.Equal(got, want) {
		t.Errorf("ListVersions = %v, want %v", got, want)
	}

	// Removing the last version removes the key, and an unknown version succeeds.
	res = mustDelete(t, s, "b", "k", DeleteOptions{VersionID: "null"})
	if res.DeleteMarker || res.VersionID != "" || !res.Versioned {
		t.Errorf("removing the null version = %+v", res)
	}
	mustDelete(t, s, "b", "k", DeleteOptions{VersionID: v1.VersionID})
	if _, err := s.HeadObject(t.Context(), "b", "k"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("HeadObject after the last version error = %v, want ErrNoSuchKey", err)
	}
	if n := dirLen(t, fsys, blobsDir("b")); n != 0 {
		t.Errorf("blobs = %d, want 0", n)
	}
	if err := s.DeleteBucket(t.Context(), "b"); err != nil {
		t.Errorf("DeleteBucket after removing every version error = %v", err)
	}
}

func TestPermanentDeleteConditions(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	v1 := mustPut(t, s, "b", "k", "one")
	mustPut(t, s, "b", "k", "two")
	tests := []struct {
		name    string
		version string
		ifMatch string
		want    error
	}{
		{"mismatch", v1.VersionID, `"other"`, ErrPreconditionFailed},
		{"missing version", "0123456789abcdef0123456789abcdef", "*", ErrNoSuchKey},
		{"match", v1.VersionID, v1.ETag, nil},
	}
	for _, tt := range tests {
		_, err := s.DeleteObject(t.Context(), "b", "k", DeleteOptions{VersionID: tt.version, IfMatch: &tt.ifMatch})
		if !errors.Is(err, tt.want) {
			t.Errorf("%s: DeleteObject error = %v, want %v", tt.name, err, tt.want)
		}
	}
}

func TestVersionedBucketNotEmpty(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	mustPut(t, s, "b", "k", "x")
	res := mustDelete(t, s, "b", "k", DeleteOptions{})
	if err := s.DeleteBucket(t.Context(), "b"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket with a delete marker error = %v, want ErrBucketNotEmpty", err)
	}
	mustDelete(t, s, "b", "k", DeleteOptions{VersionID: res.VersionID})
	if err := s.DeleteBucket(t.Context(), "b"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket with a noncurrent version error = %v, want ErrBucketNotEmpty", err)
	}
}

func TestWriteConditionsSeeCurrentVersion(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	v1 := mustPut(t, s, "b", "k", "one")
	put := func(opts PutOptions) error {
		_, err := s.PutObject(t.Context(), "b", "k", strings.NewReader("x"), opts)
		return err
	}
	if err := put(PutOptions{IfNoneMatch: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("If-None-Match on an existing key error = %v, want ErrPreconditionFailed", err)
	}
	mustDelete(t, s, "b", "k", DeleteOptions{})
	if err := put(PutOptions{IfMatch: v1.ETag}); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("If-Match under a delete marker error = %v, want ErrNoSuchKey", err)
	}
	if _, err := s.DeleteObject(t.Context(), "b", "k", DeleteOptions{IfMatch: new("*")}); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("conditional delete under a delete marker error = %v, want ErrNoSuchKey", err)
	}
	if err := put(PutOptions{IfNoneMatch: true}); err != nil {
		t.Errorf("If-None-Match under a delete marker error = %v, want success", err)
	}
}

func TestACLAndTagsPerVersion(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	v1 := mustPut(t, s, "b", "k", "one")
	v2 := mustPut(t, s, "b", "k", "two")
	policy := acl.Private(strings.Repeat("a", 64))
	policy.Grants = append(policy.Grants, acl.Grant{Grantee: acl.Grantee{Type: "Group", URI: acl.AllUsers}, Permission: "READ"})
	if err := s.PutObjectACL(t.Context(), "b", "k", v1.VersionID, policy); err != nil {
		t.Fatal(err)
	}
	if err := s.PutObjectTags(t.Context(), "b", "k", v1.VersionID, []tag.Tag{{Key: "a", Value: "1"}}); err != nil {
		t.Fatal(err)
	}
	old, _ := s.HeadObjectVersion(t.Context(), "b", "k", v1.VersionID)
	cur, _ := s.HeadObject(t.Context(), "b", "k")
	if old.ACL == nil || len(old.ACL.Grants) != 2 || len(old.Tags) != 1 {
		t.Errorf("noncurrent version = ACL %v, tags %v, want the new ACL and one tag", old.ACL, old.Tags)
	}
	if cur.VersionID != v2.VersionID || len(cur.Tags) != 0 || cur.ACL != nil && len(cur.ACL.Grants) != 0 {
		t.Errorf("current version = %+v, want it untouched", cur)
	}
	if err := s.PutObjectTags(t.Context(), "b", "k", "0123456789abcdef0123456789abcdef", nil); !errors.Is(err, ErrNoSuchVersion) {
		t.Errorf("PutObjectTags on an unknown version error = %v, want ErrNoSuchVersion", err)
	}
}

func TestListVersionsOrder(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	mustPut(t, s, "b", "b", "b0")
	setVersioning(t, s, "b", VersioningEnabled)
	for _, key := range []string{"b", "a/x", "a", "c"} {
		mustPut(t, s, "b", key, key)
	}
	mustDelete(t, s, "b", "c", DeleteOptions{})
	all, err := s.ListVersions(t.Context(), "b", "", "")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, v := range all {
		got = append(got, fmt.Sprintf("%s/%t/%t", v.Key, v.IsLatest, v.DeleteMarker))
	}
	want := []string{"a/true/false", "a/x/true/false", "b/true/false", "b/false/false", "c/true/true", "c/false/false"}
	if !slices.Equal(got, want) {
		t.Errorf("ListVersions = %v, want %v", got, want)
	}
	from, err := s.ListVersions(t.Context(), "b", "", "b")
	if err != nil || len(from) != 4 || from[0].Key != "b" {
		t.Errorf("ListVersions(fromKey b) = %d entries, %v, want 4 starting at b", len(from), err)
	}
	pre, err := s.ListVersions(t.Context(), "b", "a", "")
	if err != nil || len(pre) != 2 {
		t.Errorf("ListVersions(prefix a) = %d entries, %v, want 2", len(pre), err)
	}
}

func TestUnversionedBucketListsNullVersions(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	mustPut(t, s, "b", "k", "x")
	if got, want := versionList(t, s, "b", nil), []string{"k:null*"}; !slices.Equal(got, want) {
		t.Errorf("ListVersions = %v, want %v", got, want)
	}
	res := mustDelete(t, s, "b", "k", DeleteOptions{})
	if res.Versioned || res.DeleteMarker {
		t.Errorf("DeleteObject on an unversioned bucket = %+v, want no version", res)
	}
}

func TestCompleteUploadVersions(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	up, err := s.CreateUpload(t.Context(), "b", "k", UploadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.PutPart(t.Context(), "b", "k", up.ID, 1, strings.NewReader("part"), PartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "b", "k", "first")
	info, err := s.CompleteUpload(t.Context(), "b", "k", up.ID, []CompletePart{{PartNumber: 1, ETag: part.ETag}}, CompleteOptions{})
	if err != nil || len(info.VersionID) != 32 {
		t.Fatalf("CompleteUpload = %+v, %v, want a version ID", info, err)
	}
	if got := len(versionList(t, s, "b", nil)); got != 2 {
		t.Errorf("versions = %d, want 2", got)
	}
}

func TestConcurrentVersionedPuts(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	const n = 20
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if _, err := s.PutObject(t.Context(), "b", "k", strings.NewReader(fmt.Sprint(i)), PutOptions{}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	all, err := s.ListVersions(t.Context(), "b", "", "")
	if err != nil {
		t.Fatal(err)
	}
	latest, ids := 0, map[string]bool{}
	for _, v := range all {
		ids[v.VersionID] = true
		if v.IsLatest {
			latest++
		}
	}
	if len(all) != n || len(ids) != n || latest != 1 || !all[0].IsLatest {
		t.Errorf("versions = %d (%d distinct, %d latest), want %d, one latest first", len(all), len(ids), latest, n)
	}
	if got := dirLen(t, fsys, blobsDir("b")); got != n {
		t.Errorf("blobs = %d, want %d", got, n)
	}
}

func TestLifecycleExpiryAddsDeleteMarker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, fsys := newStore(t)
		mustCreate(t, s, "b")
		setVersioning(t, s, "b", VersioningEnabled)
		old := mustPut(t, s, "b", "k", "body")
		prefix, days := "", 1
		putLifecycle(t, s, "b", []lifecycle.Rule{{Status: "Enabled", Prefix: &prefix, Expiration: &lifecycle.Expiration{Days: &days}}})
		time.Sleep(time.Until(lifecycle.Deadline(old.LastModified, 1)))
		for range 2 { // the second sweep finds only a marker and adds no more
			if err := s.SweepLifecycle(t.Context(), time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.HeadObject(t.Context(), "b", "k"); !errors.Is(err, ErrNoSuchKey) {
			t.Errorf("HeadObject after expiry error = %v, want ErrNoSuchKey", err)
		}
		names := map[string]string{old.VersionID: "v1"}
		all := versionList(t, s, "b", names)
		if len(all) != 2 || !strings.HasPrefix(all[0], "k:DM") || all[1] != "k:v1" {
			t.Errorf("ListVersions = %v, want a delete marker over v1", all)
		}
		if n := dirLen(t, fsys, blobsDir("b")); n != 1 {
			t.Errorf("blobs = %d, want 1: expiry keeps the version", n)
		}
	})
}

// failCommitFS fails a Commit to a file that fail names.
type failCommitFS struct {
	vfs.FS
	fail func(name string) bool
}

func (f *failCommitFS) CreateTemp() (vfs.TempFile, error) {
	tf, err := f.FS.CreateTemp()
	return &failCommitFile{TempFile: tf, fail: f.fail}, err
}

type failCommitFile struct {
	vfs.TempFile
	fail func(name string) bool
}

func (f *failCommitFile) Commit(name string) error {
	if f.fail(name) {
		return errors.New("injected commit failure")
	}
	return f.TempFile.Commit(name)
}

func TestInterruptedWriteKeepsCurrentVersion(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	v1 := mustPut(t, s, "b", "k", "one")

	// The old version is saved before the current file changes, so a failed
	// commit leaves the object readable and its duplicate invisible.
	armed := false
	broken := &Store{fs: &failCommitFS{FS: fsys, fail: func(name string) bool { return armed && name == metaFile("b", "k") }}, buckets: s.buckets}
	armed = true
	if _, err := broken.PutObject(ctx, "b", "k", strings.NewReader("two"), PutOptions{}); err == nil {
		t.Fatal("PutObject with a failing commit succeeded")
	}
	armed = false
	if body, info := mustGet(t, s, "b", "k"); body != "one" || info.VersionID != v1.VersionID {
		t.Errorf("after a failed write: %q, %q, want one, %q", body, info.VersionID, v1.VersionID)
	}
	if got := versionList(t, s, "b", map[string]string{v1.VersionID: "v1"}); !slices.Equal(got, []string{"k:v1*"}) {
		t.Errorf("ListVersions after a failed write = %v, want one version", got)
	}
	// Open removes the duplicate and the orphan blob.
	if _, err := Open(ctx, fsys); err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.Stat(keyVersionsDir("b", "k")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("duplicate version directory after Open: Stat error = %v, want not exist", err)
	}
	if n := dirLen(t, fsys, blobsDir("b")); n != 1 {
		t.Errorf("blobs after Open = %d, want 1", n)
	}
}

func TestInterruptedPromotionIsRepaired(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	v1 := mustPut(t, s, "b", "k", "one")
	v2 := mustPut(t, s, "b", "k", "two")

	// Crash after the promoted version became current and before its own file
	// and the removed version's blob went: v1 is current and noncurrent, v2's blob is orphaned.
	cur, err := s.readRecord("b", "k")
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.noncurrent("b", cur)
	if err != nil || len(old) != 1 {
		t.Fatalf("noncurrent = %v, %v", old, err)
	}
	if err := s.writeJSON(metaFile("b", "k"), old[0]); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, fsys)
	if err != nil {
		t.Fatal(err)
	}
	if body, info := mustGet(t, reopened, "b", "k"); body != "one" || info.VersionID != v1.VersionID {
		t.Errorf("after repair: %q, %q, want one, %q", body, info.VersionID, v1.VersionID)
	}
	if _, err := reopened.HeadObjectVersion(ctx, "b", "k", v2.VersionID); !errors.Is(err, ErrNoSuchVersion) {
		t.Errorf("removed version error = %v, want ErrNoSuchVersion", err)
	}
	if n := dirLen(t, fsys, blobsDir("b")); n != 1 {
		t.Errorf("blobs after repair = %d, want 1", n)
	}
}

func TestOpenPromotesKeyWithoutCurrentRecord(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	v1 := mustPut(t, s, "b", "k", "one")
	mustPut(t, s, "b", "k", "two")
	// The current record is gone, as if a crash lost it: the newest noncurrent version takes over.
	if err := fsys.Remove(metaFile("b", "k")); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, fsys)
	if err != nil {
		t.Fatal(err)
	}
	if _, info := mustGet(t, reopened, "b", "k"); info.VersionID != v1.VersionID {
		t.Errorf("promoted version = %q, want %q", info.VersionID, v1.VersionID)
	}
	if n := dirLen(t, fsys, blobsDir("b")); n != 1 {
		t.Errorf("blobs after repair = %d, want 1", n)
	}
}

func TestOpenDropsDuplicateNullVersion(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	mustPut(t, s, "b", "k", "pre")
	mustPut(t, s, "b", "k", "two")
	setVersioning(t, s, "b", VersioningSuspended)
	// Crash after the new null version became current, before the old null file went.
	cur, _ := s.readRecord("b", "k")
	older, _ := s.noncurrent("b", cur)
	nullRec := older[len(older)-1]
	nullRec.VersionID = ""
	nullRec.Seq = 1
	if err := s.writeVersion("b", nullRec); err != nil {
		t.Fatal(err)
	}
	cur.VersionID, cur.Seq = "", 5
	if err := s.writeJSON(metaFile("b", "k"), cur); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, fsys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.HeadObjectVersion(ctx, "b", "k", "null"); err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.Stat(versionFile("b", "k", "")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("noncurrent null file after Open: Stat error = %v, want not exist", err)
	}
}

func TestVersionFilesStayUnderVersionsDir(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	setVersioning(t, s, "b", VersioningEnabled)
	key := "dir/../weird key?"
	mustPut(t, s, "b", key, "one")
	mustPut(t, s, "b", key, "two")
	entries, err := fsys.ReadDir(keyVersionsDir("b", key))
	if err != nil || len(entries) != 1 || !strings.HasPrefix(path.Join(keyVersionsDir("b", key), entries[0].Name()), versionsDir("b")) {
		t.Errorf("version files = %v, %v, want one under versions/", entries, err)
	}
}

func TestFailedCommitDoesNotResurrectDeletedVersion(t *testing.T) {
	for _, tt := range []struct {
		name  string
		older bool
	}{{"with older versions", true}, {"only the current version", false}} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s, fsys := newStore(t)
			mustCreate(t, s, "b")
			setVersioning(t, s, "b", VersioningEnabled)
			var older ObjectInfo
			if tt.older {
				older = mustPut(t, s, "b", "k", "zero")
			}
			cur := mustPut(t, s, "b", "k", "one")
			armed := true
			broken := &Store{fs: &failCommitFS{FS: fsys, fail: func(name string) bool { return armed && name == metaFile("b", "k") }}, buckets: s.buckets}
			if _, err := broken.PutObject(ctx, "b", "k", strings.NewReader("two"), PutOptions{}); err == nil {
				t.Fatal("PutObject with a failing commit succeeded")
			}
			armed = false
			mustDelete(t, s, "b", "k", DeleteOptions{VersionID: cur.VersionID})

			want := []string(nil)
			if tt.older {
				want = []string{"k:v0*"}
			}
			names := map[string]string{older.VersionID: "v0"}
			check := func(st *Store, when string) {
				t.Helper()
				if got := versionList(t, st, "b", names); !slices.Equal(got, want) {
					t.Errorf("%s: ListVersions = %v, want %v", when, got, want)
				}
				if tt.older {
					if body, _ := mustGet(t, st, "b", "k"); body != "zero" {
						t.Errorf("%s: body = %q, want zero", when, body)
					}
				} else if _, err := st.HeadObject(ctx, "b", "k"); !errors.Is(err, ErrNoSuchKey) {
					t.Errorf("%s: HeadObject error = %v, want ErrNoSuchKey", when, err)
				}
			}
			check(s, "after the delete")
			reopened, err := Open(ctx, fsys)
			if err != nil {
				t.Fatal(err)
			}
			check(reopened, "after Open")
		})
	}
}

func TestOpenRemovesEmptyVersionsDirectory(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	dir := keyVersionsDir("b", "gone")
	if err := fsys.MkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), fsys); err != nil {
		t.Fatalf("Open with an empty versions directory error = %v", err)
	}
	if _, err := fsys.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("empty directory after Open: Stat error = %v, want not exist", err)
	}
	_ = s
}
