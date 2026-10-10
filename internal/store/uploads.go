package store

import (
	"bytes"
	"cmp"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yavosh/pail/internal/acl"
	"github.com/yavosh/pail/internal/checksum"
)

// Errors of the multipart operations, checked with errors.Is.
var (
	ErrNoSuchUpload              = errors.New("no such upload")
	ErrInvalidPart               = errors.New("invalid part")
	ErrInvalidPartOrder          = errors.New("parts not in ascending order")
	ErrEntityTooSmall            = errors.New("part smaller than the minimum size")
	ErrEntityTooLarge            = errors.New("object larger than the maximum size")
	ErrSizeMismatch              = errors.New("object size does not match the expected size")
	ErrChecksumAlgorithmMismatch = errors.New("checksum algorithm does not match the upload")
	ErrChecksumTypeMismatch      = errors.New("checksum type does not match the upload")
	ErrMissingPartChecksum       = errors.New("missing part checksum")
)

// Multipart limits, as on AWS.
const (
	MaxParts         = 10000
	MinPartSize      = 5 << 20              // every part but the last
	maxMultipartSize = MaxParts * (5 << 30) // 10,000 parts of 5 GiB
	uploadFileName   = "upload.json"
)

// UploadInfo describes a multipart upload.
type UploadInfo struct {
	ACL       *acl.Policy `json:"acl,omitempty"`
	ID        string      `json:"-"` // the directory name
	Key       string      `json:"key"`
	Initiated time.Time   `json:"initiated"`
	// Metadata is stored with the object that Complete creates.
	Metadata map[string]string `json:"metadata,omitempty"`
	ObjectOptions
	// ChecksumAlgorithm and ChecksumType are empty when the client chose none.
	ChecksumAlgorithm string `json:"checksumAlgorithm,omitempty"`
	ChecksumType      string `json:"checksumType,omitempty"`
}

// UploadOptions are the optional parts of a CreateUpload.
type UploadOptions struct {
	ACL      *acl.Policy
	Metadata map[string]string
	ObjectOptions
	ChecksumAlgorithm string
	ChecksumType      string
}

// PartInfo describes an uploaded part.
type PartInfo struct {
	PartNumber   int       `json:"partNumber"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"` // hex MD5, without quotes
	LastModified time.Time `json:"lastModified"`
	// ObjectOptions come from the upload. PutPart fills them; they are not stored with the part.
	ObjectOptions `json:"-"`
	// ChecksumAlgorithm and Checksum (base64) are empty when the part has none.
	ChecksumAlgorithm string `json:"checksumAlgorithm,omitempty"`
	Checksum          string `json:"checksum,omitempty"`
}

// partRecord is a part's metadata file: the description plus its data file.
type partRecord struct {
	PartInfo
	File string `json:"file"`
}

// PartOptions are the optional parts of a PutPart.
type PartOptions struct {
	// ContentMD5 and Checksum work as in PutOptions.
	ContentMD5 []byte
	// ChecksumAlgorithm is the client's algorithm. An upload that has one
	// accepts only that; otherwise this one, if set, is computed.
	ChecksumAlgorithm string
	Checksum          []byte
}

// CompletePart is one entry of a CompleteUpload request.
type CompletePart struct {
	PartNumber int
	ETag       string // with or without quotes
	// ChecksumAlgorithm and Checksum (base64) are the part's checksum as the
	// client sent it. A part that differs from the stored one is invalid.
	ChecksumAlgorithm string
	Checksum          string
}

// CompleteOptions are the optional parts of a CompleteUpload.
type CompleteOptions struct {
	// IfMatch and IfNoneMatch work as in PutOptions.
	IfMatch     string
	IfNoneMatch bool
	// ChecksumAlgorithm and FullObjectChecksum are the client's whole-object
	// checksum. Only a FULL_OBJECT upload, or one with no algorithm, has one.
	ChecksumAlgorithm  string
	FullObjectChecksum []byte
	ChecksumType       string
	// ExpectedSize is the x-amz-mp-object-size value. Nil means the client sent none.
	ExpectedSize *int64
}

func uploadsDir(bucket string) string { return path.Join("buckets", bucket, "uploads") }
func uploadDir(bucket, id string) string {
	return path.Join(uploadsDir(bucket), id)
}
func endedDir(bucket string) string       { return path.Join("buckets", bucket, "ended-uploads") }
func endedFile(bucket, id string) string  { return path.Join(endedDir(bucket), id) }
func uploadFile(bucket, id string) string { return path.Join(uploadDir(bucket, id), uploadFileName) }
func partFile(bucket, id string, n int) string {
	return path.Join(uploadDir(bucket, id), "part-"+strconv.Itoa(n)+".json")
}

// uploadLock returns the striped lock for an upload. Two uploads can share a
// stripe, so a goroutine holds at most one.
func (s *Store) uploadLock(bucket, id string) *sync.Mutex {
	sum := sha256.Sum256([]byte(bucket + "/" + id))
	return &s.uploads[sum[0]]
}

// validUploadID accepts only the IDs CreateUpload makes, so an ID never leaves its directory.
func validUploadID(id string) bool {
	return len(id) == 32 && strings.Trim(id, "0123456789abcdef") == ""
}

// readUpload returns the upload, or ErrNoSuchUpload when it is missing or
// belongs to another key.
func (s *Store) readUpload(ctx context.Context, bucket, key, id string) (UploadInfo, error) {
	if err := checkBucketName(bucket); err != nil {
		return UploadInfo{}, err
	}
	if err := checkKey(key); err != nil {
		return UploadInfo{}, err
	}
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return UploadInfo{}, err
	}
	if !validUploadID(id) {
		return UploadInfo{}, ErrNoSuchUpload
	}
	var up UploadInfo
	if err := s.readJSON(uploadFile(bucket, id), &up); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return UploadInfo{}, ErrNoSuchUpload
		}
		return UploadInfo{}, fmt.Errorf("read upload %s: %w", id, err)
	}
	if up.Key != key {
		return UploadInfo{}, ErrNoSuchUpload
	}
	up.ID = id
	return up, nil
}

// CreateUpload starts a multipart upload for key.
func (s *Store) CreateUpload(ctx context.Context, bucket, key string, opts UploadOptions) (UploadInfo, error) {
	if err := checkBucketName(bucket); err != nil {
		return UploadInfo{}, err
	}
	if err := checkKey(key); err != nil {
		return UploadInfo{}, err
	}
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return UploadInfo{}, err // checked before bucketLock, so a missing bucket adds no lock entry
	}
	l := s.bucketLock(bucket)
	l.RLock()
	defer l.RUnlock()
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return UploadInfo{}, err
	}
	up := UploadInfo{
		ID:                newBlobID(),
		Key:               key,
		Initiated:         time.Now().UTC(),
		Metadata:          opts.Metadata,
		ACL:               opts.ACL,
		ObjectOptions:     opts.ObjectOptions,
		ChecksumAlgorithm: checksum.Canonical(opts.ChecksumAlgorithm),
		ChecksumType:      opts.ChecksumType,
	}
	if err := s.fs.MkdirAll(uploadDir(bucket, up.ID)); err != nil {
		return UploadInfo{}, fmt.Errorf("create upload: %w", err)
	}
	// upload.json is written last, so a half-created upload does not exist.
	if err := s.writeJSON(uploadFile(bucket, up.ID), up); err != nil {
		_ = s.fs.RemoveAll(uploadDir(bucket, up.ID))
		return UploadInfo{}, fmt.Errorf("write upload: %w", err)
	}
	return up, nil
}

// PutPart stores body as part partNumber, which replaces an earlier part with
// that number. A body read error aborts the write and leaves nothing behind.
func (s *Store) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader, opts PartOptions) (PartInfo, error) {
	if partNumber < 1 || partNumber > MaxParts {
		return PartInfo{}, fmt.Errorf("part number %d out of range", partNumber)
	}
	// Fail fast, but stream the body without any lock, as PutObject does.
	up, err := s.readUpload(ctx, bucket, key, uploadID)
	if err != nil {
		return PartInfo{}, err
	}
	algorithm := checksum.Canonical(opts.ChecksumAlgorithm)
	if up.ChecksumAlgorithm != "" && algorithm != "" && algorithm != up.ChecksumAlgorithm {
		return PartInfo{}, ErrChecksumAlgorithmMismatch
	}
	algorithm = cmp.Or(up.ChecksumAlgorithm, algorithm)

	tf, err := s.fs.CreateTemp()
	if err != nil {
		return PartInfo{}, fmt.Errorf("create temp file: %w", err)
	}
	defer func() { _ = tf.Abort() }()
	sum := md5.New()
	writers := []io.Writer{tf, sum}
	var flexible hash.Hash
	if algorithm != "" {
		flexible, _ = checksum.New(algorithm)
		writers = append(writers, flexible)
	}
	n, err := io.Copy(io.MultiWriter(writers...), body)
	if err != nil {
		return PartInfo{}, fmt.Errorf("read body: %w", err)
	}
	digest := sum.Sum(nil)
	if opts.ContentMD5 != nil && !bytes.Equal(opts.ContentMD5, digest) {
		return PartInfo{}, ErrBadDigest
	}
	rec := partRecord{PartNumber: partNumber, Size: n, ETag: hex.EncodeToString(digest), ChecksumAlgorithm: algorithm}
	if flexible != nil {
		flexibleSum := flexible.Sum(nil)
		if opts.Checksum != nil && !bytes.Equal(opts.Checksum, flexibleSum) {
			return PartInfo{}, ErrChecksumMismatch
		}
		rec.Checksum = checksum.Encode(flexibleSum)
	}

	l := s.bucketLock(bucket)
	l.RLock()
	defer l.RUnlock()
	ul := s.uploadLock(bucket, uploadID)
	ul.Lock()
	defer ul.Unlock()
	// An abort or a complete may have removed the upload while the body streamed.
	if _, err := s.readUpload(ctx, bucket, key, uploadID); err != nil {
		return PartInfo{}, err
	}
	var old partRecord
	oldErr := s.readJSON(partFile(bucket, uploadID, partNumber), &old)
	if oldErr != nil && !errors.Is(oldErr, fs.ErrNotExist) {
		return PartInfo{}, fmt.Errorf("read part %d: %w", partNumber, oldErr)
	}
	rec.File = "part-" + strconv.Itoa(partNumber) + "-" + newBlobID()
	rec.LastModified = time.Now().UTC()
	data := path.Join(uploadDir(bucket, uploadID), rec.File)
	if err := tf.Commit(data); err != nil {
		return PartInfo{}, fmt.Errorf("commit part: %w", err)
	}
	if err := s.writeJSON(partFile(bucket, uploadID, partNumber), rec); err != nil {
		_ = s.fs.Remove(data)
		return PartInfo{}, fmt.Errorf("commit part metadata: %w", err)
	}
	if oldErr == nil {
		_ = s.fs.Remove(path.Join(uploadDir(bucket, uploadID), old.File))
	}
	rec.ObjectOptions = up.ObjectOptions
	return rec.PartInfo, nil
}

// readParts reads every part record of an upload, sorted by part number.
func (s *Store) readParts(bucket, id string) ([]partRecord, error) {
	entries, err := s.fs.ReadDir(uploadDir(bucket, id))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNoSuchUpload
	}
	if err != nil {
		return nil, fmt.Errorf("list parts of %s: %w", id, err)
	}
	var out []partRecord
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "part-") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var r partRecord
		err := s.readJSON(path.Join(uploadDir(bucket, id), e.Name()), &r)
		if errors.Is(err, fs.ErrNotExist) {
			continue // the upload ended since ReadDir
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b partRecord) int { return cmp.Compare(a.PartNumber, b.PartNumber) })
	return out, nil
}

// ListParts returns the upload and its parts, sorted by part number.
func (s *Store) ListParts(ctx context.Context, bucket, key, uploadID string) (UploadInfo, []PartInfo, error) {
	up, err := s.readUpload(ctx, bucket, key, uploadID)
	if err != nil {
		return UploadInfo{}, nil, err
	}
	records, err := s.readParts(bucket, uploadID)
	if err != nil {
		return UploadInfo{}, nil, err
	}
	parts := make([]PartInfo, len(records))
	for i, r := range records {
		parts[i] = r.PartInfo
	}
	return up, parts, nil
}

// ListUploads returns the bucket's uploads, sorted by key, then start time, then ID.
func (s *Store) ListUploads(ctx context.Context, bucket string) ([]UploadInfo, error) {
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	entries, err := s.fs.ReadDir(uploadsDir(bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list uploads in %s: %w", bucket, err)
	}
	var out []UploadInfo
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var up UploadInfo
		err := s.readJSON(uploadFile(bucket, e.Name()), &up)
		if errors.Is(err, fs.ErrNotExist) {
			continue // not started or already ended
		}
		if err != nil {
			return nil, fmt.Errorf("read upload %s: %w", e.Name(), err)
		}
		up.ID = e.Name()
		out = append(out, up)
	}
	slices.SortFunc(out, func(a, b UploadInfo) int {
		return cmp.Or(strings.Compare(a.Key, b.Key), a.Initiated.Compare(b.Initiated), strings.Compare(a.ID, b.ID))
	})
	return out, nil
}

// AbortUpload discards an upload and its parts. As on AWS, aborting an upload
// that was already completed or aborted succeeds.
func (s *Store) AbortUpload(ctx context.Context, bucket, key, uploadID string) error {
	if _, err := s.readUpload(ctx, bucket, key, uploadID); err != nil && !errors.Is(err, ErrNoSuchUpload) {
		return err
	}
	l := s.bucketLock(bucket)
	l.RLock()
	defer l.RUnlock()
	ul := s.uploadLock(bucket, uploadID)
	ul.Lock()
	defer ul.Unlock()
	_, err := s.readUpload(ctx, bucket, key, uploadID)
	if errors.Is(err, ErrNoSuchUpload) {
		// The tombstone does not know the key, so any key matches an ended ID.
		if ended, endedErr := s.isEnded(bucket, uploadID); endedErr != nil || ended {
			return endedErr
		}
	}
	if err != nil {
		return err
	}
	if err := s.markEnded(bucket, uploadID); err != nil {
		return err
	}
	return s.removeUpload(bucket, uploadID)
}

// markEnded writes the empty tombstone that makes a repeated abort succeed.
// Tombstones are never pruned: unlike AWS, pail keeps them until the bucket is deleted.
func (s *Store) markEnded(bucket, id string) error {
	if err := s.fs.MkdirAll(endedDir(bucket)); err != nil {
		return fmt.Errorf("create ended uploads directory: %w", err)
	}
	tf, err := s.fs.CreateTemp()
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() { _ = tf.Abort() }()
	if err := tf.Commit(endedFile(bucket, id)); err != nil {
		return fmt.Errorf("mark upload %s ended: %w", id, err)
	}
	return nil
}

// isEnded reports whether the upload was completed or aborted.
func (s *Store) isEnded(bucket, id string) (bool, error) {
	if !validUploadID(id) {
		return false, nil
	}
	_, err := s.fs.Stat(endedFile(bucket, id))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat ended upload %s: %w", id, err)
	}
	return true, nil
}

// removeUpload removes upload.json first, so a partial removal leaves no upload.
func (s *Store) removeUpload(bucket, id string) error {
	if err := s.fs.Remove(uploadFile(bucket, id)); err != nil {
		return fmt.Errorf("remove upload %s: %w", id, err)
	}
	if err := s.fs.RemoveAll(uploadDir(bucket, id)); err != nil {
		return fmt.Errorf("remove upload %s: %w", id, err)
	}
	return nil
}

// CompleteUpload joins the listed parts into one object, commits it as
// PutObject does, and ends the upload. A failed completion leaves the upload.
func (s *Store) CompleteUpload(ctx context.Context, bucket, key, uploadID string, parts []CompletePart, opts CompleteOptions) (ObjectInfo, error) {
	if _, err := s.readUpload(ctx, bucket, key, uploadID); err != nil {
		return ObjectInfo{}, err
	}
	l := s.bucketLock(bucket)
	l.RLock()
	defer l.RUnlock()
	// Held while the parts are copied, which blocks this upload's other writers.
	ul := s.uploadLock(bucket, uploadID)
	ul.Lock()
	defer ul.Unlock()
	up, err := s.readUpload(ctx, bucket, key, uploadID)
	if err != nil {
		return ObjectInfo{}, err
	}
	if opts.ChecksumType != "" && opts.ChecksumType != cmp.Or(up.ChecksumType, checksum.FullObject) {
		return ObjectInfo{}, ErrChecksumTypeMismatch
	}

	records, size, err := s.checkParts(bucket, up, parts)
	if err != nil {
		return ObjectInfo{}, err
	}
	if opts.ExpectedSize != nil {
		if size != *opts.ExpectedSize {
			return ObjectInfo{}, ErrSizeMismatch
		}
	}

	// A composite upload has no whole-object hash; an upload with no
	// algorithm gets the default, as a PutObject does.
	fullAlgorithm := ""
	switch {
	case up.ChecksumAlgorithm == "":
		fullAlgorithm = checksum.Default
	case up.ChecksumType == checksum.FullObject:
		fullAlgorithm = up.ChecksumAlgorithm
	}
	if opts.FullObjectChecksum != nil && (fullAlgorithm == "" || checksum.Canonical(opts.ChecksumAlgorithm) != fullAlgorithm) {
		return ObjectInfo{}, ErrChecksumAlgorithmMismatch
	}

	tf, err := s.fs.CreateTemp()
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("create temp file: %w", err)
	}
	defer func() { _ = tf.Abort() }()
	var full hash.Hash
	var dst io.Writer = tf
	if fullAlgorithm != "" {
		full, _ = checksum.New(fullAlgorithm)
		dst = io.MultiWriter(tf, full)
	}
	etagSum := md5.New()
	var composite hash.Hash
	if fullAlgorithm == "" {
		composite, _ = checksum.New(up.ChecksumAlgorithm)
	}
	var total int64
	for _, r := range records {
		if err := ctx.Err(); err != nil {
			return ObjectInfo{}, err
		}
		binary, _ := hex.DecodeString(r.ETag)
		etagSum.Write(binary)
		if composite != nil {
			partSum, ok := checksum.Decode(up.ChecksumAlgorithm, r.Checksum)
			if !ok {
				return ObjectInfo{}, fmt.Errorf("part %d has no %s checksum", r.PartNumber, up.ChecksumAlgorithm)
			}
			composite.Write(partSum)
		}
		n, err := s.copyPart(dst, bucket, uploadID, r)
		if err != nil {
			return ObjectInfo{}, err
		}
		total += n
	}

	info := ObjectInfo{
		Key:          key,
		Size:         total,
		ETag:         hex.EncodeToString(etagSum.Sum(nil)) + "-" + strconv.Itoa(len(records)),
		LastModified: time.Now().UTC(),
		Metadata:     up.Metadata,
		ACL:          up.ACL,

		ObjectOptions: up.ObjectOptions,
	}
	if full != nil {
		fullSum := full.Sum(nil)
		if opts.FullObjectChecksum != nil && !bytes.Equal(opts.FullObjectChecksum, fullSum) {
			return ObjectInfo{}, ErrChecksumMismatch
		}
		info.ChecksumAlgorithm, info.Checksum, info.ChecksumType = fullAlgorithm, checksum.Encode(fullSum), checksum.FullObject
	} else {
		info.ChecksumAlgorithm = up.ChecksumAlgorithm
		info.Checksum = checksum.Encode(composite.Sum(nil)) + "-" + strconv.Itoa(len(records))
		info.ChecksumType = checksum.Composite
	}

	blob := newBlobID()
	if err := tf.Commit(path.Join(blobsDir(bucket), blob)); err != nil {
		return ObjectInfo{}, fmt.Errorf("commit blob: %w", err)
	}
	if err := s.commitRecord(ctx, bucket, record{ObjectInfo: info, Blob: blob}, PutOptions{IfMatch: opts.IfMatch, IfNoneMatch: opts.IfNoneMatch}); err != nil {
		_ = s.fs.Remove(path.Join(blobsDir(bucket), blob))
		return ObjectInfo{}, err
	}
	// The object exists now. If this fails, or the process dies first, the upload
	// stays listed until it is aborted, and completing it again rewrites the object.
	if err := s.markEnded(bucket, uploadID); err == nil {
		_ = s.removeUpload(bucket, uploadID)
	}
	return info, nil
}

// checkParts matches the client's list against the stored parts and returns
// the stored records in list order, with their total size.
func (s *Store) checkParts(bucket string, up UploadInfo, parts []CompletePart) ([]partRecord, int64, error) {
	if len(parts) == 0 {
		return nil, 0, ErrInvalidPart
	}
	for i := 1; i < len(parts); i++ {
		if parts[i].PartNumber <= parts[i-1].PartNumber {
			return nil, 0, ErrInvalidPartOrder
		}
	}
	var (
		records []partRecord
		total   int64
	)
	for i, cp := range parts {
		if cp.PartNumber < 1 || cp.PartNumber > MaxParts {
			return nil, 0, ErrInvalidPart
		}
		var r partRecord
		err := s.readJSON(partFile(bucket, up.ID, cp.PartNumber), &r)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, 0, ErrInvalidPart
		}
		if err != nil {
			return nil, 0, fmt.Errorf("read part %d: %w", cp.PartNumber, err)
		}
		if strings.Trim(cp.ETag, `"`) != r.ETag {
			return nil, 0, ErrInvalidPart
		}
		if up.ChecksumType == checksum.Composite {
			if cp.Checksum == "" {
				return nil, 0, ErrMissingPartChecksum
			}
			if cp.PartNumber != i+1 {
				// AWS returns InternalError for nonconsecutive composite parts.
				return nil, 0, fmt.Errorf("checksum part number %d, want %d", cp.PartNumber, i+1)
			}
		}
		if cp.Checksum != "" && (checksum.Canonical(cp.ChecksumAlgorithm) != r.ChecksumAlgorithm || cp.Checksum != r.Checksum) {
			return nil, 0, ErrInvalidPart
		}
		if i < len(parts)-1 && r.Size < MinPartSize {
			return nil, 0, ErrEntityTooSmall
		}
		total += r.Size
		records = append(records, r)
	}
	if total > maxMultipartSize {
		return nil, 0, ErrEntityTooLarge
	}
	return records, total, nil
}

// copyPart appends a part's data file to dst.
func (s *Store) copyPart(dst io.Writer, bucket, id string, r partRecord) (int64, error) {
	f, err := s.fs.Open(path.Join(uploadDir(bucket, id), r.File))
	if err != nil {
		return 0, fmt.Errorf("open part %d: %w", r.PartNumber, err)
	}
	defer func() { _ = f.Close() }()
	n, err := io.Copy(dst, f)
	if err != nil {
		return 0, fmt.Errorf("copy part %d: %w", r.PartNumber, err)
	}
	if n != r.Size {
		return 0, fmt.Errorf("part %d has %d bytes, want %d", r.PartNumber, n, r.Size)
	}
	return n, nil
}

// removeOrphanUploadFiles removes upload directories without upload.json and
// part files that no part record names, which a crash can leave behind.
func (s *Store) removeOrphanUploadFiles(ctx context.Context, bucket string) error {
	dirs, err := s.fs.ReadDir(uploadsDir(bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, d := range dirs {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := d.Name()
		_, err := s.fs.Stat(uploadFile(bucket, id))
		if errors.Is(err, fs.ErrNotExist) {
			if err := s.fs.RemoveAll(uploadDir(bucket, id)); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		records, err := s.readParts(bucket, id)
		if err != nil {
			return err
		}
		used := map[string]bool{}
		for _, r := range records {
			used[r.File] = true
		}
		files, err := s.fs.ReadDir(uploadDir(bucket, id))
		if err != nil {
			return err
		}
		for _, f := range files {
			if name := f.Name(); !used[name] && !strings.HasSuffix(name, ".json") {
				if err := s.fs.Remove(path.Join(uploadDir(bucket, id), name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return err
				}
			}
		}
	}
	return nil
}
