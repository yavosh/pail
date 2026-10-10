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

	"github.com/yavosh/pail/internal/acl"
	"github.com/yavosh/pail/internal/checksum"
	"github.com/yavosh/pail/internal/tag"
	"github.com/yavosh/pail/internal/vfs"
)

// ObjectOptions are write options that the store keeps and returns. Only Tags
// have an effect: lifecycle rules match them. An empty field means the client set none.
type ObjectOptions struct {
	ServerSideEncryption string `json:"serverSideEncryption,omitempty"`
	// StorageClass is empty for STANDARD.
	StorageClass    string    `json:"storageClass,omitempty"`
	WebsiteRedirect string    `json:"websiteRedirect,omitempty"`
	Tags            []tag.Tag `json:"tags,omitempty"`
}

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	ObjectOptions
	ACL          *acl.Policy `json:"acl,omitempty"`
	Key          string      `json:"key"`
	Size         int64       `json:"size"`
	ETag         string      `json:"etag"` // hex MD5, without quotes
	LastModified time.Time   `json:"lastModified"`
	// Metadata holds the headers stored with the object. The store does not interpret them.
	Metadata map[string]string `json:"metadata,omitempty"`
	// ChecksumAlgorithm, Checksum (base64), and ChecksumType describe the
	// flexible checksum; objects stored before checksums had none.
	ChecksumAlgorithm string `json:"checksumAlgorithm,omitempty"`
	Checksum          string `json:"checksum,omitempty"`
	ChecksumType      string `json:"checksumType,omitempty"`
	// Parts is the layout of a completed multipart object. A simple object, or
	// one completed before pail kept the layout, has none.
	Parts []ObjectPart `json:"parts,omitempty"`
	// VersionID is empty for the null version, which every object in a
	// bucket that was never versioned is. A delete marker has no body.
	VersionID    string `json:"versionId,omitempty"`
	DeleteMarker bool   `json:"deleteMarker,omitempty"`
}

// ObjectPart is one part of a completed multipart object, in object order.
type ObjectPart struct {
	PartNumber int   `json:"partNumber"`
	Size       int64 `json:"size"`
	// ChecksumAlgorithm and Checksum (base64) are empty when the part has none.
	ChecksumAlgorithm string `json:"checksumAlgorithm,omitempty"`
	Checksum          string `json:"checksum,omitempty"`
}

// record is the metadata file: the object description plus its blob name.
type record struct {
	ObjectInfo
	Blob string `json:"blob"`
	// Seq orders the versions of a key; a newer version has a larger Seq.
	// Writes in a versioned bucket set it. Earlier objects have none.
	Seq int64 `json:"seq,omitempty"`
}

// PutOptions are the optional parts of a PutObject.
type PutOptions struct {
	// Anonymous prevents replacing an object owned by an authenticated account.
	Anonymous bool
	ACL       *acl.Policy
	Metadata  map[string]string
	ObjectOptions
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
		ACL:          opts.ACL,
		Blob:         blob,

		ObjectOptions:     opts.ObjectOptions,
		ChecksumAlgorithm: algorithm,
		Checksum:          checksum.Encode(flexibleSum),
		ChecksumType:      checksum.FullObject,
	}
	if err := s.commitRecord(ctx, bucket, &rec, opts); err != nil {
		_ = s.fs.Remove(path.Join(blobsDir(bucket), blob))
		return ObjectInfo{}, err
	}
	return rec.ObjectInfo, nil
}

// commitRecord checks the write conditions and commits rec under the key
// lock. The conditions apply to the current version; a delete marker is no object.
// It sets rec's version ID.
func (s *Store) commitRecord(ctx context.Context, bucket string, rec *record, opts PutOptions) error {
	kl := s.keyLock(bucket, rec.Key)
	kl.Lock()
	defer kl.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	status, err := s.versioningStatus(bucket)
	if err != nil {
		return err
	}
	old, err := s.readRecord(bucket, rec.Key)
	hasOld := err == nil
	if err != nil && !errors.Is(err, ErrNoSuchKey) {
		return err
	}
	exists := hasOld && !old.DeleteMarker
	switch {
	case opts.Anonymous && exists && (old.ACL == nil || old.ACL.Owner.ID != acl.AnonymousID):
		return ErrAccessDenied
	case opts.IfNoneMatch && exists:
		return ErrPreconditionFailed
	case opts.IfMatch != "" && !exists:
		return ErrNoSuchKey
	case opts.IfMatch != "" && strings.Trim(opts.IfMatch, `"`) != old.ETag:
		return ErrPreconditionFailed
	}
	return s.install(bucket, status, old, hasOld, rec)
}

// GetObject opens the current version of key for reading. The caller closes the file.
func (s *Store) GetObject(ctx context.Context, bucket, key string) (vfs.File, ObjectInfo, error) {
	return s.GetObjectVersion(ctx, bucket, key, "")
}

// GetObjectVersion opens a version of key; an empty versionID names the
// current one. A delete marker fails with a *DeleteMarkerError.
func (s *Store) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (vfs.File, ObjectInfo, error) {
	if err := s.checkObject(ctx, bucket, key); err != nil {
		return nil, ObjectInfo{}, err
	}
	// A concurrent write may remove the blob between reading the metadata and
	// opening it; then the metadata names a newer blob, so read again. The same
	// blob missing twice is real damage, not a race.
	var lastBlob string
	for range maxOpenAttempts {
		rec, _, err := s.lookupObject(bucket, key, versionID)
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

// HeadObject describes the current version of key.
func (s *Store) HeadObject(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	return s.HeadObjectVersion(ctx, bucket, key, "")
}

// HeadObjectVersion describes a version of key, as GetObjectVersion selects it.
func (s *Store) HeadObjectVersion(ctx context.Context, bucket, key, versionID string) (ObjectInfo, error) {
	if err := s.checkObject(ctx, bucket, key); err != nil {
		return ObjectInfo{}, err
	}
	rec, _, err := s.lookupObject(bucket, key, versionID)
	if err != nil {
		return ObjectInfo{}, s.missing(ctx, bucket, err)
	}
	return rec.ObjectInfo, nil
}

// DeleteOptions are the optional parts of a DeleteObject.
type DeleteOptions struct {
	// IfMatch requires a strong ETag match, or "*" for any existing object.
	// nil is unconditional; a conditional missing key fails with ErrNoSuchKey.
	// With a VersionID it applies to that version.
	IfMatch *string
	// VersionID removes that version for good; "null" names the null version.
	// Empty deletes the current version: a versioned bucket gets a delete marker.
	VersionID string
}

// DeleteObject checks opts and removes key under the key lock.
// Removing a missing key succeeds only when the delete is unconditional.
func (s *Store) DeleteObject(ctx context.Context, bucket, key string, opts DeleteOptions) (DeleteResult, error) {
	if err := s.checkObject(ctx, bucket, key); err != nil {
		return DeleteResult{}, err
	}
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return DeleteResult{}, err // checked before bucketLock, so a missing bucket adds no lock entry
	}
	l := s.bucketLock(bucket)
	l.RLock()
	defer l.RUnlock()
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return DeleteResult{}, err
	}
	kl := s.keyLock(bucket, key)
	kl.Lock()
	defer kl.Unlock()
	if err := ctx.Err(); err != nil {
		return DeleteResult{}, err
	}
	status, err := s.versioningStatus(bucket)
	if err != nil {
		return DeleteResult{}, err
	}
	cur, err := s.readRecord(bucket, key)
	hasCur := err == nil
	if err != nil && !errors.Is(err, ErrNoSuchKey) {
		return DeleteResult{}, err
	}
	if opts.VersionID != "" {
		return s.deleteVersion(bucket, status, cur, hasCur, opts, key)
	}
	exists := hasCur && !cur.DeleteMarker
	if !exists && opts.IfMatch == nil && status == "" {
		return DeleteResult{}, nil
	}
	if !exists && opts.IfMatch != nil {
		return DeleteResult{}, ErrNoSuchKey
	}
	if opts.IfMatch != nil && !etagMatches(*opts.IfMatch, cur.ETag) {
		return DeleteResult{}, ErrPreconditionFailed
	}
	if status == "" {
		if err := s.fs.Remove(metaFile(bucket, key)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return DeleteResult{}, fmt.Errorf("delete metadata: %w", err)
		}
		_ = s.fs.Remove(path.Join(blobsDir(bucket), cur.Blob))
		return DeleteResult{}, nil
	}
	marker := record{Key: key, LastModified: time.Now().UTC(), DeleteMarker: true}
	if err := s.install(bucket, status, cur, hasCur, &marker); err != nil {
		return DeleteResult{}, err
	}
	return DeleteResult{VersionID: marker.VersionID, DeleteMarker: true, Versioned: true}, nil
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
		if r.Key > startAfter && strings.HasPrefix(r.Key, prefix) && !r.DeleteMarker {
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

// missing turns a missing key or version into ErrNoSuchBucket when the bucket is gone too.
func (s *Store) missing(ctx context.Context, bucket string, err error) error {
	if !errors.Is(err, ErrNoSuchKey) && !errors.Is(err, ErrNoSuchVersion) {
		return err
	}
	if _, berr := s.HeadBucket(ctx, bucket); berr != nil {
		return berr
	}
	return err
}
