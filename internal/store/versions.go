package store

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"
	"time"
)

// Versioning states of a bucket. A bucket that was never versioned has none,
// and cannot return to it.
const (
	VersioningEnabled   = "Enabled"
	VersioningSuspended = "Suspended"
)

// NullVersionID names the null version in requests and responses. The store
// keeps it as an empty VersionID.
const NullVersionID = "null"

// ErrNoSuchVersion means the key has no version with the requested ID.
var ErrNoSuchVersion = errors.New("no such version")

// DeleteMarkerError means the version a read named is a delete marker. It
// matches ErrNoSuchKey, which is what a read of a deleted key answers.
type DeleteMarkerError struct {
	VersionID    string // empty for the null version
	LastModified time.Time
	// Specific is true when the request named the marker by its version ID.
	Specific bool
}

func (e *DeleteMarkerError) Error() string { return "delete marker" }

// Is makes a delete marker match ErrNoSuchKey.
func (e *DeleteMarkerError) Is(target error) bool { return target == ErrNoSuchKey }

// VersionInfo is one entry of a version listing.
type VersionInfo struct {
	ObjectInfo
	IsLatest bool
}

// DeleteResult describes what a DeleteObject did.
type DeleteResult struct {
	// VersionID is the ID of the delete marker created, or of the version
	// removed. It is empty for the null version.
	VersionID    string
	DeleteMarker bool // a marker was created or removed
	Versioned    bool // the bucket has versioning, so the answer names a version
}

// BucketVersioning returns the versioning state of a bucket: "", "Enabled", or "Suspended".
func (s *Store) BucketVersioning(ctx context.Context, bucket string) (string, error) {
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return "", err
	}
	return s.versioningStatus(bucket)
}

func (s *Store) versioningStatus(bucket string) (string, error) {
	name, _ := configurationFile(bucket, "versioning")
	var cfg BucketConfiguration
	if err := s.readJSON(name, &cfg); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("read versioning configuration: %w", err)
	}
	return cfg.Status, nil
}

func versionsDir(bucket string) string { return path.Join("buckets", bucket, "versions") }

func keyVersionsDir(bucket, key string) string { return path.Join(versionsDir(bucket), keyHash(key)) }

// versionFile names a noncurrent version. The null version is null.json.
func versionFile(bucket, key, versionID string) string {
	return path.Join(keyVersionsDir(bucket, key), cmp.Or(versionID, NullVersionID)+".json")
}

// newVersionID returns 32 hex characters: the sequence number, so IDs sort by
// age, then random bytes.
func newVersionID(seq int64) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(seq))
	_, _ = rand.Read(b[8:])
	return hex.EncodeToString(b[:])
}

// lookup returns the record that versionID names, and whether it is the
// current one. An empty versionID names the current record, which can be a delete marker.
func (s *Store) lookup(bucket, key, versionID string) (rec record, current bool, err error) {
	cur, err := s.readRecord(bucket, key)
	if err != nil {
		if errors.Is(err, ErrNoSuchKey) && versionID != "" && versionID != NullVersionID {
			return record{}, false, ErrNoSuchVersion
		}
		return record{}, false, err
	}
	want := versionID
	if want == NullVersionID {
		want = ""
	}
	if versionID == "" || cur.VersionID == want {
		return cur, true, nil
	}
	err = s.readJSON(versionFile(bucket, key, want), &rec)
	if errors.Is(err, fs.ErrNotExist) {
		// A delete may have promoted this version to current since the first read.
		if cur, err = s.readRecord(bucket, key); err == nil && cur.VersionID == want {
			return cur, true, nil
		}
		return record{}, false, ErrNoSuchVersion
	}
	if err != nil {
		return record{}, false, fmt.Errorf("read version: %w", err)
	}
	return rec, false, nil
}

// lookupObject is lookup for readers: a delete marker is an error.
func (s *Store) lookupObject(bucket, key, versionID string) (record, bool, error) {
	rec, current, err := s.lookup(bucket, key, versionID)
	if err == nil && rec.DeleteMarker {
		return record{}, false, &DeleteMarkerError{VersionID: rec.VersionID, LastModified: rec.LastModified, Specific: versionID != ""}
	}
	return rec, current, err
}

// updateRecord rewrites rec in the file that holds it.
func (s *Store) updateRecord(bucket string, rec record, current bool) error {
	name := versionFile(bucket, rec.Key, rec.VersionID)
	if current {
		name = metaFile(bucket, rec.Key)
	}
	return s.writeJSON(name, rec)
}

// writeVersion saves rec as a noncurrent version.
func (s *Store) writeVersion(bucket string, rec record) error {
	if err := s.fs.MkdirAll(keyVersionsDir(bucket, rec.Key)); err != nil {
		return fmt.Errorf("create versions directory: %w", err)
	}
	if err := s.writeJSON(versionFile(bucket, rec.Key, rec.VersionID), rec); err != nil {
		return fmt.Errorf("write version: %w", err)
	}
	return nil
}

// noncurrent returns the noncurrent records of cur's key, newest first. A
// record that repeats cur is the leftover of an interrupted write, so it is skipped.
func (s *Store) noncurrent(bucket string, cur record) ([]record, error) {
	dir := keyVersionsDir(bucket, cur.Key)
	entries, err := s.fs.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list versions of %s: %w", cur.Key, err)
	}
	var out []record
	for _, e := range entries {
		var r record
		err := s.readJSON(path.Join(dir, e.Name()), &r)
		if errors.Is(err, fs.ErrNotExist) {
			continue // removed since ReadDir
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		if r.Seq != cur.Seq && (r.VersionID != "" || cur.VersionID != "") {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b record) int { return cmpSeq(b, a) })
	return out, nil
}

func cmpSeq(a, b record) int {
	switch {
	case a.Seq < b.Seq:
		return -1
	case a.Seq > b.Seq:
		return 1
	}
	return 0
}

// install makes rec the current version of its key and retires old, if hasOld.
// The caller holds the key lock or the exclusive bucket lock. The old version
// is saved first and replaced files go last, so recoverVersions can repair a crash.
func (s *Store) install(bucket, status string, old record, hasOld bool, rec *record) error {
	if status == "" {
		if err := s.writeJSON(metaFile(bucket, rec.Key), *rec); err != nil {
			return fmt.Errorf("commit metadata: %w", err)
		}
		if hasOld && old.Blob != "" && old.Blob != rec.Blob {
			_ = s.fs.Remove(path.Join(blobsDir(bucket), old.Blob))
		}
		return nil
	}
	rec.Seq = time.Now().UnixNano()
	if hasOld {
		rec.Seq = max(rec.Seq, old.Seq+1)
	}
	rec.VersionID = ""
	if status == VersioningEnabled {
		rec.VersionID = newVersionID(rec.Seq)
	}
	var replaced []record
	nullAside := false // the null version being replaced is a noncurrent one
	saved := false     // old was written to versions/
	oldIsNull := hasOld && old.VersionID == ""
	if hasOld {
		if status == VersioningSuspended && oldIsNull {
			replaced = append(replaced, old)
		} else if err := s.writeVersion(bucket, old); err != nil {
			return err
		} else {
			saved = true
		}
	}
	if status == VersioningSuspended && !oldIsNull {
		var null record
		err := s.readJSON(versionFile(bucket, rec.Key, ""), &null)
		if err == nil {
			replaced, nullAside = append(replaced, null), true
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("read null version: %w", err)
		}
	}
	if err := s.writeJSON(metaFile(bucket, rec.Key), *rec); err != nil {
		if saved {
			// old stays current, so its copy would resurrect it after a delete.
			_ = s.fs.Remove(versionFile(bucket, rec.Key, old.VersionID))
		}
		return fmt.Errorf("commit metadata: %w", err)
	}
	if nullAside {
		_ = s.fs.Remove(versionFile(bucket, rec.Key, ""))
	}
	for _, r := range replaced {
		if r.Blob != "" && r.Blob != rec.Blob {
			_ = s.fs.Remove(path.Join(blobsDir(bucket), r.Blob))
		}
	}
	return nil
}

// ListVersions returns the versions and delete markers of keys with prefix at
// or after fromKey, in key order and newest first within a key. An unversioned
// bucket lists each object as its null version. It reads every metadata file.
func (s *Store) ListVersions(ctx context.Context, bucket, prefix, fromKey string) ([]VersionInfo, error) {
	if _, err := s.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	records, err := s.readAllObjects(ctx, bucket)
	if err != nil {
		return nil, err
	}
	var out []VersionInfo
	for _, cur := range sortedByKey(records) {
		if cur.Key < fromKey || !strings.HasPrefix(cur.Key, prefix) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out = append(out, VersionInfo{ObjectInfo: cur.ObjectInfo, IsLatest: true})
		older, err := s.noncurrent(bucket, cur)
		if err != nil {
			return nil, err
		}
		for _, r := range older {
			out = append(out, VersionInfo{ObjectInfo: r.ObjectInfo})
		}
	}
	return out, nil
}

// deleteVersion permanently removes one version, or a delete marker. Removing
// the current version promotes the newest noncurrent one. The caller holds
// the key lock.
func (s *Store) deleteVersion(bucket, status string, cur record, hasCur bool, opts DeleteOptions, key string) (DeleteResult, error) {
	id := opts.VersionID
	if id == NullVersionID {
		id = ""
	}
	result := DeleteResult{VersionID: id, Versioned: status != ""}
	target, isCurrent := cur, hasCur && cur.VersionID == id
	if !isCurrent {
		err := s.readJSON(versionFile(bucket, key, id), &target)
		if errors.Is(err, fs.ErrNotExist) {
			if opts.IfMatch != nil {
				return DeleteResult{}, ErrNoSuchKey
			}
			return result, nil // deleting a version that is gone succeeds, as it does on AWS
		}
		if err != nil {
			return DeleteResult{}, fmt.Errorf("read version: %w", err)
		}
	}
	if opts.IfMatch != nil && !etagMatches(*opts.IfMatch, target.ETag) {
		return DeleteResult{}, ErrPreconditionFailed
	}
	result.DeleteMarker = target.DeleteMarker
	if isCurrent {
		// A current ID never has a versions/ file; a failed write may have left one.
		_ = s.fs.Remove(versionFile(bucket, key, cur.VersionID))
		older, err := s.noncurrent(bucket, cur)
		if err != nil {
			return DeleteResult{}, err
		}
		if len(older) == 0 {
			if err := s.fs.Remove(metaFile(bucket, key)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return DeleteResult{}, fmt.Errorf("delete metadata: %w", err)
			}
		} else {
			// The promoted version is written as current before its own file goes.
			if err := s.writeJSON(metaFile(bucket, key), older[0]); err != nil {
				return DeleteResult{}, fmt.Errorf("promote version: %w", err)
			}
			_ = s.fs.Remove(versionFile(bucket, key, older[0].VersionID))
		}
	} else if err := s.fs.Remove(versionFile(bucket, key, id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return DeleteResult{}, fmt.Errorf("delete version: %w", err)
	}
	if target.Blob != "" {
		_ = s.fs.Remove(path.Join(blobsDir(bucket), target.Blob))
	}
	_ = s.fs.Remove(keyVersionsDir(bucket, key)) // succeeds only when empty
	return result, nil
}

// etagMatches reports whether an If-Match value names etag: "*" or a strong ETag.
func etagMatches(ifMatch, etag string) bool {
	return ifMatch == "*" || ifMatch == etag || ifMatch == `"`+etag+`"`
}

// recoverVersions repairs what a crash in install or deleteVersion left: it
// removes a noncurrent record that repeats the current one, and promotes the
// newest noncurrent record of a key that has no current one.
func (s *Store) recoverVersions(ctx context.Context, bucket string) error {
	dirs, err := s.fs.ReadDir(versionsDir(bucket))
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
		if err := s.recoverKeyVersions(bucket, d.Name()); err != nil {
			return fmt.Errorf("recover versions %s: %w", d.Name(), err)
		}
	}
	return nil
}

func (s *Store) recoverKeyVersions(bucket, hash string) error {
	dir := path.Join(versionsDir(bucket), hash)
	entries, err := s.fs.ReadDir(dir)
	if err != nil {
		return err
	}
	var all []record
	for _, e := range entries {
		var r record
		if err := s.readJSON(path.Join(dir, e.Name()), &r); err != nil {
			return err
		}
		all = append(all, r)
	}
	slices.SortFunc(all, func(a, b record) int { return cmpSeq(b, a) })
	var cur record
	err = s.readJSON(path.Join(objectsDir(bucket), hash+".json"), &cur)
	switch {
	case errors.Is(err, fs.ErrNotExist) && len(all) == 0:
		return s.fs.Remove(dir) // an empty directory of no key
	case errors.Is(err, fs.ErrNotExist):
		if err := s.writeJSON(path.Join(objectsDir(bucket), hash+".json"), all[0]); err != nil {
			return err
		}
		cur, all = all[0], all[1:]
		if err := s.fs.Remove(versionFile(bucket, cur.Key, cur.VersionID)); err != nil {
			return err
		}
	case err != nil:
		return err
	}
	for _, r := range all {
		if r.Seq == cur.Seq || r.VersionID == "" && cur.VersionID == "" {
			if err := s.fs.Remove(versionFile(bucket, r.Key, r.VersionID)); err != nil {
				return err
			}
		}
	}
	_ = s.fs.Remove(dir)
	return nil
}

// readAllVersions reads every noncurrent record in a bucket, in no order.
func (s *Store) readAllVersions(ctx context.Context, bucket string) ([]record, error) {
	dirs, err := s.fs.ReadDir(versionsDir(bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list versions in %s: %w", bucket, err)
	}
	var out []record
	for _, d := range dirs {
		entries, err := s.fs.ReadDir(path.Join(versionsDir(bucket), d.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var r record
			err := s.readJSON(path.Join(versionsDir(bucket), d.Name(), e.Name()), &r)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", e.Name(), err)
			}
			out = append(out, r)
		}
	}
	return out, nil
}
