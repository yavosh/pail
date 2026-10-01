package store

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/yavosh/pail/internal/vfs/localdisk"
)

func newStore(t *testing.T) (*Store, *localdisk.FS) {
	t.Helper()
	fsys, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	s, err := Open(context.Background(), fsys)
	if err != nil {
		t.Fatal(err)
	}
	return s, fsys
}

func mustCreate(t *testing.T, s *Store, bucket string) {
	t.Helper()
	if err := s.CreateBucket(context.Background(), bucket); err != nil {
		t.Fatalf("CreateBucket(%q) error = %v", bucket, err)
	}
}

func mustPut(t *testing.T, s *Store, bucket, key, body string) ObjectInfo {
	t.Helper()
	info, err := s.PutObject(context.Background(), bucket, key, strings.NewReader(body), PutOptions{})
	if err != nil {
		t.Fatalf("PutObject(%q, %q) error = %v", bucket, key, err)
	}
	return info
}

func mustGet(t *testing.T, s *Store, bucket, key string) (string, ObjectInfo) {
	t.Helper()
	f, info, err := s.GetObject(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("GetObject(%q, %q) error = %v", bucket, key, err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return string(b), info
}

// dirLen counts the entries in a store directory.
func dirLen(t *testing.T, fsys *localdisk.FS, dir string) int {
	t.Helper()
	entries, err := fsys.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", dir, err)
	}
	return len(entries)
}

func TestBucketLifecycle(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)

	if _, err := s.HeadBucket(ctx, "b1"); !errors.Is(err, ErrNoSuchBucket) {
		t.Errorf("HeadBucket(missing) error = %v, want ErrNoSuchBucket", err)
	}
	if err := s.DeleteBucket(ctx, "b1"); !errors.Is(err, ErrNoSuchBucket) {
		t.Errorf("DeleteBucket(missing) error = %v, want ErrNoSuchBucket", err)
	}
	mustCreate(t, s, "b2")
	mustCreate(t, s, "b1")
	if err := s.CreateBucket(ctx, "b1"); !errors.Is(err, ErrBucketExists) {
		t.Errorf("CreateBucket(existing) error = %v, want ErrBucketExists", err)
	}
	if b, err := s.HeadBucket(ctx, "b1"); err != nil || b.Name != "b1" || b.Created.IsZero() {
		t.Errorf("HeadBucket(b1) = %+v, %v, want name b1 and a creation time", b, err)
	}
	list, err := s.ListBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, b := range list {
		names = append(names, b.Name)
	}
	if want := []string{"b1", "b2"}; !slices.Equal(names, want) {
		t.Errorf("ListBuckets = %v, want %v", names, want)
	}

	mustPut(t, s, "b1", "k", "x")
	if err := s.DeleteBucket(ctx, "b1"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Errorf("DeleteBucket(non-empty) error = %v, want ErrBucketNotEmpty", err)
	}
	if err := s.DeleteObject(ctx, "b1", "k"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteBucket(ctx, "b1"); err != nil {
		t.Fatalf("DeleteBucket(empty) error = %v", err)
	}
	if _, err := s.HeadBucket(ctx, "b1"); !errors.Is(err, ErrNoSuchBucket) {
		t.Errorf("HeadBucket after delete error = %v, want ErrNoSuchBucket", err)
	}
	mustCreate(t, s, "b1") // a deleted name is free again
}

func TestInvalidNames(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "a\xffb"} {
		if err := s.CreateBucket(ctx, name); !errors.Is(err, ErrInvalidName) {
			t.Errorf("CreateBucket(%q) error = %v, want ErrInvalidName", name, err)
		}
	}
	for _, key := range []string{"", strings.Repeat("k", maxKeyLen+1), "a\xffb"} {
		if _, err := s.PutObject(ctx, "b", key, strings.NewReader("x"), PutOptions{}); !errors.Is(err, ErrInvalidName) {
			t.Errorf("PutObject(key of %d bytes) error = %v, want ErrInvalidName", len(key), err)
		}
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	meta := map[string]string{"Content-Type": "text/plain", "X-Amz-Meta-Color": "blue"}
	info, err := s.PutObject(ctx, "b", "docs/hello.txt", strings.NewReader("hello"), PutOptions{Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	wantETag := fmt.Sprintf("%x", md5.Sum([]byte("hello")))
	if info.ETag != wantETag || info.Size != 5 || info.Key != "docs/hello.txt" {
		t.Errorf("PutObject info = %+v, want ETag %s and size 5", info, wantETag)
	}
	body, got := mustGet(t, s, "b", "docs/hello.txt")
	if body != "hello" || got.ETag != wantETag || got.Metadata["X-Amz-Meta-Color"] != "blue" {
		t.Errorf("GetObject = %q, %+v, want hello with ETag and metadata", body, got)
	}
	head, err := s.HeadObject(ctx, "b", "docs/hello.txt")
	if err != nil || head.ETag != wantETag || !head.LastModified.Equal(info.LastModified) {
		t.Errorf("HeadObject = %+v, %v, want the PutObject info", head, err)
	}
}

func TestMissing(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"get missing key", getErr(s, "b", "nope"), ErrNoSuchKey},
		{"get missing bucket", getErr(s, "nope", "k"), ErrNoSuchBucket},
		{"head missing key", headErr(s, "b", "nope"), ErrNoSuchKey},
		{"put missing bucket", putErr(s, "nope", "k"), ErrNoSuchBucket},
		{"delete missing bucket", s.DeleteObject(ctx, "nope", "k"), ErrNoSuchBucket},
		{"delete missing key", s.DeleteObject(ctx, "b", "nope"), nil},
		{"list missing bucket", listErr(s, "nope"), ErrNoSuchBucket},
	}
	for _, tt := range tests {
		if !errors.Is(tt.err, tt.want) {
			t.Errorf("%s: error = %v, want %v", tt.name, tt.err, tt.want)
		}
	}
}

func getErr(s *Store, bucket, key string) error {
	f, _, err := s.GetObject(context.Background(), bucket, key)
	if f != nil {
		f.Close()
	}
	return err
}

func headErr(s *Store, bucket, key string) error {
	_, err := s.HeadObject(context.Background(), bucket, key)
	return err
}

func putErr(s *Store, bucket, key string) error {
	_, err := s.PutObject(context.Background(), bucket, key, strings.NewReader("x"), PutOptions{})
	return err
}

func listErr(s *Store, bucket string) error {
	_, err := s.ListObjects(context.Background(), bucket, "", "")
	return err
}

func TestKeysCoexist(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	keys := []string{"a", "a/b", "dir/", "dir/file", "a//b", "../x", strings.Repeat("k", maxKeyLen)}
	for _, k := range keys {
		mustPut(t, s, "b", k, "body of "+k)
	}
	for _, k := range keys {
		if body, _ := mustGet(t, s, "b", k); body != "body of "+k {
			t.Errorf("GetObject(%.20q) = %.30q, want %.30q", k, body, "body of "+k)
		}
	}
}

// failingReader returns its data, then err instead of io.EOF, like a body
// whose signature check fails at the end.
type failingReader struct {
	r   io.Reader
	err error
}

func (f *failingReader) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if errors.Is(err, io.EOF) {
		return n, f.err
	}
	return n, err
}

func TestFailedPutLeavesNoTrace(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	mustPut(t, s, "b", "k", "original")
	errSig := errors.New("signature mismatch")

	tests := []struct {
		name string
		body io.Reader
		opts PutOptions
		want error
	}{
		{"body error at EOF", &failingReader{r: strings.NewReader("new"), err: errSig}, PutOptions{}, errSig},
		{"bad content MD5", strings.NewReader("new"), PutOptions{ContentMD5: make([]byte, 16)}, ErrBadDigest},
		{"if-none-match on existing key", strings.NewReader("new"), PutOptions{IfNoneMatch: true}, ErrPreconditionFailed},
		{"if-match mismatch", strings.NewReader("new"), PutOptions{IfMatch: `"0000"`}, ErrPreconditionFailed},
		{"checksum mismatch", strings.NewReader("new"), PutOptions{ChecksumAlgorithm: "CRC32", Checksum: make([]byte, 4)}, ErrChecksumMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := s.PutObject(ctx, "b", "k", tt.body, tt.opts); !errors.Is(err, tt.want) {
				t.Errorf("PutObject error = %v, want %v", err, tt.want)
			}
			if body, _ := mustGet(t, s, "b", "k"); body != "original" {
				t.Errorf("object after failed put = %q, want %q", body, "original")
			}
			if n := dirLen(t, fsys, "buckets/b/blobs"); n != 1 {
				t.Errorf("blobs after failed put = %d, want 1", n)
			}
			if n := dirLen(t, fsys, "tmp"); n != 0 {
				t.Errorf("tmp entries after failed put = %d, want 0", n)
			}
		})
	}
}

func TestConditionalPut(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")

	if _, err := s.PutObject(ctx, "b", "k", strings.NewReader("v1"), PutOptions{IfMatch: `"abc"`}); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("If-Match on a missing key error = %v, want ErrNoSuchKey", err)
	}
	v1, err := s.PutObject(ctx, "b", "k", strings.NewReader("v1"), PutOptions{IfNoneMatch: true})
	if err != nil {
		t.Fatalf("If-None-Match on a missing key error = %v", err)
	}
	if _, err := s.PutObject(ctx, "b", "k", strings.NewReader("v2"), PutOptions{IfMatch: `"` + v1.ETag + `"`}); err != nil {
		t.Errorf("If-Match with the current ETag error = %v", err)
	}
	if body, _ := mustGet(t, s, "b", "k"); body != "v2" {
		t.Errorf("object = %q, want v2", body)
	}
}

func TestDeleteRemovesBlob(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	mustPut(t, s, "b", "k", "x")
	mustPut(t, s, "b", "k", "y") // the overwrite removes the first blob
	if n := dirLen(t, fsys, "buckets/b/blobs"); n != 1 {
		t.Errorf("blobs after overwrite = %d, want 1", n)
	}
	if err := s.DeleteObject(ctx, "b", "k"); err != nil {
		t.Fatal(err)
	}
	if n := dirLen(t, fsys, "buckets/b/blobs"); n != 0 {
		t.Errorf("blobs after delete = %d, want 0", n)
	}
	if _, err := s.HeadObject(ctx, "b", "k"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("HeadObject after delete error = %v, want ErrNoSuchKey", err)
	}
}

func TestListObjects(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	for _, k := range []string{"é", "b", "a/b", "a", "B", "a/c"} {
		mustPut(t, s, "b", k, k)
	}
	tests := []struct {
		prefix, startAfter string
		want               []string
	}{
		{"", "", []string{"B", "a", "a/b", "a/c", "b", "é"}},
		{"a/", "", []string{"a/b", "a/c"}},
		{"", "a/b", []string{"a/c", "b", "é"}},
		{"a", "a", []string{"a/b", "a/c"}},
		{"z", "", nil},
	}
	for _, tt := range tests {
		got, err := s.ListObjects(ctx, "b", tt.prefix, tt.startAfter)
		if err != nil {
			t.Fatal(err)
		}
		var keys []string
		for _, o := range got {
			keys = append(keys, o.Key)
		}
		if !slices.Equal(keys, tt.want) {
			t.Errorf("ListObjects(prefix %q, startAfter %q) = %q, want %q", tt.prefix, tt.startAfter, keys, tt.want)
		}
	}
}

func TestConcurrentPutsLeaveOneVersion(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	const writers = 32
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			body := strings.Repeat(fmt.Sprint(i%10), 4096)
			if _, err := s.PutObject(context.Background(), "b", "k", strings.NewReader(body), PutOptions{}); err != nil {
				t.Errorf("writer %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	body, info := mustGet(t, s, "b", "k")
	if len(body) != 4096 || strings.Count(body, body[:1]) != 4096 {
		t.Errorf("object is not one complete version: %d bytes starting %q", len(body), body[:1])
	}
	if want := fmt.Sprintf("%x", md5.Sum([]byte(body))); info.ETag != want {
		t.Errorf("ETag = %s, want %s for the stored body", info.ETag, want)
	}
	if n := dirLen(t, fsys, "buckets/b/blobs"); n != 1 {
		t.Errorf("blobs after concurrent puts = %d, want 1", n)
	}
}

func TestReaderDuringOverwrite(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	mustPut(t, s, "b", "k", "old")
	f, _, err := s.GetObject(context.Background(), "b", "k")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	mustPut(t, s, "b", "k", "new")
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "old" {
		t.Errorf("reader opened before overwrite read %q, want %q", b, "old")
	}
	if body, _ := mustGet(t, s, "b", "k"); body != "new" {
		t.Errorf("reader after overwrite read %q, want %q", body, "new")
	}
}

func TestOpenRemovesOrphanBlobs(t *testing.T) {
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	mustPut(t, s, "b", "k", "kept")
	tf, err := fsys.CreateTemp()
	if err != nil {
		t.Fatal(err)
	}
	if err := tf.Commit("buckets/b/blobs/orphan"); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), fsys); err != nil {
		t.Fatal(err)
	}
	if n := dirLen(t, fsys, "buckets/b/blobs"); n != 1 {
		t.Errorf("blobs after Open = %d, want 1", n)
	}
	if body, _ := mustGet(t, s, "b", "k"); body != "kept" {
		t.Errorf("object after Open = %q, want kept", body)
	}
}

func TestDeleteBucketWaitsForWriters(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			// Either the put wins and the delete sees an object, or the delete
			// wins and the put sees no bucket. Never a put into a deleted bucket.
			_, err := s.PutObject(ctx, "b", fmt.Sprint(i), strings.NewReader("x"), PutOptions{})
			if err != nil && !errors.Is(err, ErrNoSuchBucket) {
				t.Errorf("put %d: %v", i, err)
			}
		})
	}
	wg.Go(func() {
		err := s.DeleteBucket(ctx, "b")
		if err != nil && !errors.Is(err, ErrBucketNotEmpty) {
			t.Errorf("delete: %v", err)
		}
	})
	wg.Wait()
	if _, err := s.HeadBucket(ctx, "b"); errors.Is(err, ErrNoSuchBucket) {
		if _, err := fsys.Stat("buckets/b"); err == nil {
			t.Error("bucket directory exists after the bucket was deleted")
		}
	}
}

func TestSlowPutDoesNotBlockDeleteBucket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s, _ := newStore(t)
		mustCreate(t, s, "b")
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() {
			_, err := s.PutObject(ctx, "b", "slow", pr, PutOptions{})
			done <- err
		}()
		synctest.Wait() // the put is now blocked reading its body
		// If the slow put held the bucket lock, this would deadlock the bubble.
		if err := s.DeleteBucket(ctx, "b"); err != nil {
			t.Fatalf("DeleteBucket during a slow put error = %v", err)
		}
		_, _ = pw.Write([]byte("late"))
		_ = pw.Close()
		if err := <-done; !errors.Is(err, ErrNoSuchBucket) {
			t.Errorf("slow put after DeleteBucket error = %v, want ErrNoSuchBucket", err)
		}
	})
}

func TestGetDuringRapidOverwrites(t *testing.T) {
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	mustPut(t, s, "b", "k", strings.Repeat("0", 64))
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for range 100 {
				if _, err := s.PutObject(context.Background(), "b", "k", strings.NewReader(strings.Repeat(fmt.Sprint(w), 64)), PutOptions{}); err != nil {
					t.Errorf("put: %v", err)
				}
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for range 200 {
				f, _, err := s.GetObject(context.Background(), "b", "k")
				if err != nil {
					t.Errorf("GetObject during overwrites error = %v", err)
					return
				}
				b, err := io.ReadAll(f)
				f.Close()
				if err != nil || len(b) != 64 || strings.Count(string(b), string(b[:1])) != 64 {
					t.Errorf("GetObject during overwrites read %q, %v, want one complete version", b, err)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestRecreateAfterPartialDelete(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	mustPut(t, s, "b", "stale", "x")
	// Simulate a DeleteBucket that removed bucket.json and then failed.
	if err := fsys.Remove("buckets/b/bucket.json"); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, s, "b")
	if got, err := s.ListObjects(ctx, "b", "", ""); err != nil || len(got) != 0 {
		t.Errorf("ListObjects on a recreated bucket = %v, %v, want none", got, err)
	}
	if n := dirLen(t, fsys, "buckets/b/blobs"); n != 0 {
		t.Errorf("blobs in a recreated bucket = %d, want 0", n)
	}
}

func TestMissingBucketAddsNoLock(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	for i := range 10 {
		name := fmt.Sprint("nope-", i)
		_ = putErr(s, name, "k")
		_ = s.DeleteObject(ctx, name, "k")
		_ = s.DeleteBucket(ctx, name)
	}
	if n := len(s.buckets); n != 0 {
		t.Errorf("lock entries after operations on missing buckets = %d, want 0", n)
	}
}
