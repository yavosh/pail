package store

import (
	"bytes"
	"cmp"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/checksum"
	"github.com/yavosh/pail/internal/vfs"
)

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"` // hex MD5, without quotes
	LastModified time.Time `json:"lastModified"`
	// Metadata holds the headers stored with the object. The store does not interpret them.
	Metadata map[string]string `json:"metadata,omitempty"`
	// ChecksumAlgorithm, Checksum (base64), and ChecksumType describe the
	// flexible checksum; objects stored before checksums had none.
	ChecksumAlgorithm string `json:"checksumAlgorithm,omitempty"`
	Checksum          string `json:"checksum,omitempty"`
	ChecksumType      string `json:"checksumType,omitempty"`
}

// record is the metadata file: the object description plus its blob name.
type record struct {
	ObjectInfo
	Blob string `json:"blob"`
}

// PutOptions are the optional parts of a PutObject.
type PutOptions struct {
	Metadata map[string]string
	// ContentMD5 is the digest the client sent; nil means none was sent. A
	// mismatch, including an empty slice, fails with ErrBadDigest.
	ContentMD5 []byte
	// IfMatch commits only if the current ETag equals it exactly; "*" and weak
	// ETags never match. A missing key fails with ErrNoSuchKey.
	IfMatch string
	// IfNoneMatch commits only if the key does not exist (If-None-Match: *).
	IfNoneMatch bool
	// ChecksumAlgorithm is computed over the body; empty or unknown means
	// checksum.Default, so callers validate names. A non-nil Checksum must
	// match, else ErrChecksumMismatch.
	ChecksumAlgorithm string
	Checksum          []byte
}

// PutObject stores body under key. A body read error, such as a signature
// mismatch detected at EOF, aborts the write and leaves nothing behind.
func (s *Store) PutObject(ctx context.Context, bucket, key string, body io.Reader, opts PutOptions) (ObjectInfo, error) {
	if err := checkBucketName(bucket); err != nil {
		return ObjectInfo{}, err
	}
	if err := checkKey(key); err != nil {
		return ObjectInfo{}, err
	}
	// Fail fast, but stream the body without the bucket lock: a slow upload
	// must not hold off DeleteBucket, which would then hold off every writer.
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return ObjectInfo{}, err
	}

	tf, err := s.fs.CreateTemp()
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("create temp file: %w", err)
	}
	defer func() { _ = tf.Abort() }()
	algorithm := cmp.Or(checksum.Canonical(opts.ChecksumAlgorithm), checksum.Default)
	flexible, _ := checksum.New(algorithm)
	sum := md5.New()
	n, err := io.Copy(io.MultiWriter(tf, sum, flexible), body)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("read body: %w", err)
	}
	digest := sum.Sum(nil)
	if opts.ContentMD5 != nil && !bytes.Equal(opts.ContentMD5, digest) {
		return ObjectInfo{}, ErrBadDigest
	}
	flexibleSum := flexible.Sum(nil)
	if opts.Checksum != nil && !bytes.Equal(opts.Checksum, flexibleSum) {
		return ObjectInfo{}, ErrChecksumMismatch
	}

	l := s.bucketLock(bucket)
	l.RLock()
	defer l.RUnlock()
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return ObjectInfo{}, err
	}
	blob := newBlobID()
	if err := tf.Commit(path.Join(blobsDir(bucket), blob)); err != nil {
		return ObjectInfo{}, fmt.Errorf("commit blob: %w", err)
	}

	rec := record{
		Key:          key,
		Size:         n,
		ETag:         hex.EncodeToString(digest),
		LastModified: time.Now().UTC(),
		Metadata:     opts.Metadata,
		Blob:         blob,

		ChecksumAlgorithm: algorithm,
		Checksum:          checksum.Encode(flexibleSum),
		ChecksumType:      checksum.FullObject,
	}
	if err := s.commitRecord(ctx, bucket, rec, opts); err != nil {
		_ = s.fs.Remove(path.Join(blobsDir(bucket), blob))
		return ObjectInfo{}, err
	}
	return rec.ObjectInfo, nil
}

// commitRecord checks the write conditions and commits rec under the key
// lock, then removes the blob it replaced.
func (s *Store) commitRecord(ctx context.Context, bucket string, rec record, opts PutOptions) error {
	kl := s.keyLock(bucket, rec.Key)
	kl.Lock()
	defer kl.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	old, err := s.readRecord(bucket, rec.Key)
	exists := err == nil
	if err != nil && !errors.Is(err, ErrNoSuchKey) {
		return err
	}
	switch {
	case opts.IfNoneMatch && exists:
		return ErrPreconditionFailed
	case opts.IfMatch != "" && !exists:
		return ErrNoSuchKey
	case opts.IfMatch != "" && strings.Trim(opts.IfMatch, `"`) != old.ETag:
		return ErrPreconditionFailed
	}
	if err := s.writeJSON(metaFile(bucket, rec.Key), rec); err != nil {
		return fmt.Errorf("commit metadata: %w", err)
	}
	if exists && old.Blob != rec.Blob {
		_ = s.fs.Remove(path.Join(blobsDir(bucket), old.Blob))
	}
	return nil
}

// GetObject opens key for reading. The caller closes the file.
func (s *Store) GetObject(ctx context.Context, bucket, key string) (vfs.File, ObjectInfo, error) {
	if err := s.checkObject(ctx, bucket, key); err != nil {
		return nil, ObjectInfo{}, err
	}
	// A concurrent write may remove the blob between reading the metadata and
	// opening it; then the metadata names a newer blob, so read again. The same
	// blob missing twice is real damage, not a race.
	var lastBlob string
	for range maxOpenAttempts {
		rec, err := s.readRecord(bucket, key)
		if err != nil {
			return nil, ObjectInfo{}, s.missing(ctx, bucket, err)
		}
		f, err := s.fs.Open(path.Join(blobsDir(bucket), rec.Blob))
		if err == nil {
			return f, rec.ObjectInfo, nil
		}
		if !errors.Is(err, fs.ErrNotExist) || rec.Blob == lastBlob {
			return nil, ObjectInfo{}, fmt.Errorf("open blob: %w", err)
		}
		lastBlob = rec.Blob
	}
	return nil, ObjectInfo{}, fmt.Errorf("open blob: overwritten %d times while opening", maxOpenAttempts)
}

const maxOpenAttempts = 16

// HeadObject describes key.
func (s *Store) HeadObject(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	if err := s.checkObject(ctx, bucket, key); err != nil {
		return ObjectInfo{}, err
	}
	rec, err := s.readRecord(bucket, key)
	if err != nil {
		return ObjectInfo{}, s.missing(ctx, bucket, err)
	}
	return rec.ObjectInfo, nil
}

// DeleteObject removes key. Removing a missing key succeeds, as on S3.
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := s.checkObject(ctx, bucket, key); err != nil {
		return err
	}
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return err // checked before bucketLock, so a missing bucket adds no lock entry
	}
	l := s.bucketLock(bucket)
	l.RLock()
	defer l.RUnlock()
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	kl := s.keyLock(bucket, key)
	kl.Lock()
	defer kl.Unlock()
	rec, err := s.readRecord(bucket, key)
	if errors.Is(err, ErrNoSuchKey) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := s.fs.Remove(metaFile(bucket, key)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete metadata: %w", err)
	}
	_ = s.fs.Remove(path.Join(blobsDir(bucket), rec.Blob))
	return nil
}

// ListObjects returns the objects whose keys start with prefix and sort after
// startAfter, in UTF-8 byte order. It reads every metadata file in the bucket,
// so a call costs O(objects); callers paginate the result.
func (s *Store) ListObjects(ctx context.Context, bucket, prefix, startAfter string) ([]ObjectInfo, error) {
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	records, err := s.readAllObjects(ctx, bucket)
	if err != nil {
		return nil, err
	}
	var out []ObjectInfo
	for _, r := range sortedByKey(records) {
		if r.Key > startAfter && strings.HasPrefix(r.Key, prefix) {
			out = append(out, r.ObjectInfo)
		}
	}
	return out, nil
}

// readAllObjects reads every metadata file in a bucket, in no order.
func (s *Store) readAllObjects(ctx context.Context, bucket string) ([]record, error) {
	entries, err := s.fs.ReadDir(objectsDir(bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list objects in %s: %w", bucket, err)
	}
	out := make([]record, 0, len(entries))
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var r record
		err := s.readJSON(path.Join(objectsDir(bucket), e.Name()), &r)
		if errors.Is(err, fs.ErrNotExist) {
			continue // deleted since ReadDir
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		out = append(out, r)
	}
	return out, nil
}

func (s *Store) readRecord(bucket, key string) (record, error) {
	var r record
	if err := s.readJSON(metaFile(bucket, key), &r); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return record{}, ErrNoSuchKey
		}
		return record{}, fmt.Errorf("read metadata: %w", err)
	}
	return r, nil
}

func (s *Store) checkObject(ctx context.Context, bucket, key string) error {
	if err := checkBucketName(bucket); err != nil {
		return err
	}
	if err := checkKey(key); err != nil {
		return err
	}
	return ctx.Err()
}

// missing turns a missing key into ErrNoSuchBucket when the bucket is gone too.
func (s *Store) missing(ctx context.Context, bucket string, err error) error {
	if !errors.Is(err, ErrNoSuchKey) {
		return err
	}
	if _, berr := s.HeadBucket(ctx, bucket); berr != nil {
		return berr
	}
	return ErrNoSuchKey
}
