// Package store implements S3 storage semantics on a vfs.FS.
//
// Layout under the VFS root:
//
//	buckets/<bucket>/bucket.json
//	buckets/<bucket>/objects/<sha256(key)>.json   metadata; renaming it in commits a write
//	buckets/<bucket>/blobs/<id>                   object bytes, immutable once committed
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/yavosh/pail/internal/vfs"
)

// Errors callers check with errors.Is. Other errors are internal failures.
var (
	ErrInvalidName        = errors.New("invalid bucket name or key")
	ErrNoSuchBucket       = errors.New("no such bucket")
	ErrBucketExists       = errors.New("bucket already exists")
	ErrBucketNotEmpty     = errors.New("bucket not empty")
	ErrNoSuchKey          = errors.New("no such key")
	ErrPreconditionFailed = errors.New("precondition failed")
	ErrBadDigest          = errors.New("content MD5 does not match")
	ErrChecksumMismatch   = errors.New("checksum does not match")
)

const maxKeyLen = 1024

// Store keeps buckets and objects in a vfs.FS. One Store owns its FS.
type Store struct {
	fs vfs.FS

	mu      sync.Mutex
	buckets map[string]*sync.RWMutex
	// keys serializes writers per key, so a conditional check and its commit
	// are atomic. Striping bounds memory; unrelated keys rarely share a lock.
	keys [256]sync.Mutex
}

// BucketInfo describes a bucket.
type BucketInfo struct {
	Name    string    `json:"name"`
	Created time.Time `json:"created"`
}

// Open returns a Store on fsys and removes blobs that no metadata references,
// which a crash between the blob and metadata commits leaves behind.
func Open(ctx context.Context, fsys vfs.FS) (*Store, error) {
	if err := fsys.MkdirAll("buckets"); err != nil {
		return nil, fmt.Errorf("create buckets directory: %w", err)
	}
	s := &Store{fs: fsys, buckets: map[string]*sync.RWMutex{}}
	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	for _, b := range buckets {
		if err := s.removeOrphanBlobs(ctx, b.Name); err != nil {
			return nil, fmt.Errorf("recover bucket %s: %w", b.Name, err)
		}
	}
	return s, nil
}

// bucketLock returns the lock that DeleteBucket holds exclusively and every
// write to the bucket holds shared.
func (s *Store) bucketLock(name string) *sync.RWMutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.buckets[name]
	if !ok {
		l = &sync.RWMutex{}
		s.buckets[name] = l
	}
	return l
}

// CreateBucket creates an empty bucket.
func (s *Store) CreateBucket(ctx context.Context, name string) error {
	if err := checkBucketName(name); err != nil {
		return err
	}
	l := s.bucketLock(name)
	l.Lock()
	defer l.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := s.fs.Stat(bucketFile(name)); err == nil {
		return ErrBucketExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat bucket %s: %w", name, err)
	}
	// A failed DeleteBucket can leave objects behind bucket.json; a new bucket
	// must not inherit them.
	if err := s.fs.RemoveAll(bucketDir(name)); err != nil {
		return fmt.Errorf("clear bucket %s: %w", name, err)
	}
	for _, dir := range []string{objectsDir(name), blobsDir(name)} {
		if err := s.fs.MkdirAll(dir); err != nil {
			return fmt.Errorf("create bucket %s: %w", name, err)
		}
	}
	// bucket.json is written last, so a half-created bucket does not exist.
	return s.writeJSON(bucketFile(name), BucketInfo{Name: name, Created: time.Now().UTC()})
}

// HeadBucket describes a bucket.
func (s *Store) HeadBucket(ctx context.Context, name string) (BucketInfo, error) {
	if err := checkBucketName(name); err != nil {
		return BucketInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return BucketInfo{}, err
	}
	var b BucketInfo
	if err := s.readJSON(bucketFile(name), &b); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return BucketInfo{}, ErrNoSuchBucket
		}
		return BucketInfo{}, fmt.Errorf("read bucket %s: %w", name, err)
	}
	return b, nil
}

// ListBuckets returns every bucket, sorted by name.
func (s *Store) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	entries, err := s.fs.ReadDir("buckets")
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	var out []BucketInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		b, err := s.HeadBucket(ctx, e.Name())
		if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrInvalidName) {
			continue // a directory without bucket.json is not a bucket
		}
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// DeleteBucket removes an empty bucket.
func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	if _, err := s.HeadBucket(ctx, name); err != nil {
		return err // checked before bucketLock, so a missing bucket adds no lock entry
	}
	l := s.bucketLock(name)
	l.Lock()
	defer l.Unlock()
	if _, err := s.HeadBucket(ctx, name); err != nil {
		return err
	}
	entries, err := s.fs.ReadDir(objectsDir(name))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("list objects in %s: %w", name, err)
	}
	if len(entries) > 0 {
		return ErrBucketNotEmpty
	}
	// Remove bucket.json first: if RemoveAll fails partway, the rest is not a bucket.
	if err := s.fs.Remove(bucketFile(name)); err != nil {
		return fmt.Errorf("delete bucket %s: %w", name, err)
	}
	if err := s.fs.RemoveAll(bucketDir(name)); err != nil {
		return fmt.Errorf("delete bucket %s: %w", name, err)
	}
	return nil
}

// removeOrphanBlobs deletes blobs that no metadata file references.
func (s *Store) removeOrphanBlobs(ctx context.Context, bucket string) error {
	objects, err := s.readAllObjects(ctx, bucket)
	if err != nil {
		return err
	}
	used := map[string]bool{}
	for _, o := range objects {
		used[o.Blob] = true
	}
	blobs, err := s.fs.ReadDir(blobsDir(bucket))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for _, b := range blobs {
		if !used[b.Name()] {
			if err := s.fs.Remove(path.Join(blobsDir(bucket), b.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

// writeJSON commits v to name through a temp file.
func (s *Store) writeJSON(name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tf, err := s.fs.CreateTemp()
	if err != nil {
		return err
	}
	defer func() { _ = tf.Abort() }()
	if _, err := tf.Write(b); err != nil {
		return err
	}
	return tf.Commit(name)
}

func (s *Store) readJSON(name string, v any) error {
	f, err := s.fs.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// checkBucketName rejects names that are not one path element. The S3 naming
// rules are the API layer's job; this only keeps a name inside its directory.
func checkBucketName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\") || !utf8.ValidString(name) {
		return ErrInvalidName
	}
	return nil
}

func checkKey(key string) error {
	// JSON would replace invalid UTF-8, so the stored key would differ from its address.
	if key == "" || len(key) > maxKeyLen || !utf8.ValidString(key) {
		return ErrInvalidName
	}
	return nil
}

func bucketDir(name string) string  { return path.Join("buckets", name) }
func bucketFile(name string) string { return path.Join("buckets", name, "bucket.json") }
func objectsDir(name string) string { return path.Join("buckets", name, "objects") }
func blobsDir(name string) string   { return path.Join("buckets", name, "blobs") }

// keyHash names a key's metadata file, so any key up to 1024 bytes fits a filename.
func keyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func metaFile(bucket, key string) string {
	return path.Join(objectsDir(bucket), keyHash(key)+".json")
}

func newBlobID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// keyLock returns the striped lock for bucket and key. Hold at most one key
// lock per goroutine: two keys can share a stripe, and the mutex is not reentrant.
func (s *Store) keyLock(bucket, key string) *sync.Mutex {
	sum := sha256.Sum256([]byte(bucket + "/" + key))
	return &s.keys[sum[0]]
}

// sortedByKey sorts objects by key in UTF-8 byte order, as S3 lists them.
func sortedByKey(objects []record) []record {
	slices.SortFunc(objects, func(a, b record) int { return strings.Compare(a.Key, b.Key) })
	return objects
}
