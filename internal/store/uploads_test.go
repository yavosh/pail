package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"path"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yavosh/pail/internal/checksum"
)

var (
	bigPart   = bytes.Repeat([]byte("a"), MinPartSize)
	smallPart = []byte(strings.Repeat("b", 1000))
)

func mustUpload(t *testing.T, s *Store, bucket, key string, opts UploadOptions) UploadInfo {
	t.Helper()
	up, err := s.CreateUpload(context.Background(), bucket, key, opts)
	if err != nil {
		t.Fatalf("CreateUpload(%q, %q) error = %v", bucket, key, err)
	}
	return up
}

func mustPart(t *testing.T, s *Store, bucket, key, id string, n int, body []byte, opts PartOptions) PartInfo {
	t.Helper()
	p, err := s.PutPart(context.Background(), bucket, key, id, n, bytes.NewReader(body), opts)
	if err != nil {
		t.Fatalf("PutPart(%q, %d) error = %v", id, n, err)
	}
	return p
}

// listed turns stored parts into the list a client would send to complete them.
func listed(parts ...PartInfo) []CompletePart {
	out := make([]CompletePart, len(parts))
	for i, p := range parts {
		out[i] = CompletePart{PartNumber: p.PartNumber, ETag: `"` + p.ETag + `"`}
	}
	return out
}

func md5Sum(b ...[]byte) []byte {
	h := md5.New()
	for _, p := range b {
		h.Write(p)
	}
	return h.Sum(nil)
}

func TestUploadLifecycle(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	meta := map[string]string{"Content-Type": "text/plain", "X-Amz-Meta-Color": "blue"}
	up := mustUpload(t, s, "b", "dir/big", UploadOptions{Metadata: meta})
	if len(up.ID) != 32 || up.Key != "dir/big" || up.Initiated.IsZero() {
		t.Fatalf("CreateUpload = %+v, want a 32-character ID, the key, and a start time", up)
	}
	p2 := mustPart(t, s, "b", "dir/big", up.ID, 2, smallPart, PartOptions{})
	p1 := mustPart(t, s, "b", "dir/big", up.ID, 1, bigPart, PartOptions{})
	if p1.ETag != hex.EncodeToString(md5Sum(bigPart)) || p1.Size != MinPartSize || p1.Checksum != "" {
		t.Errorf("PutPart(1) = %+v, want the MD5 ETag, size %d, and no checksum", p1, MinPartSize)
	}

	gotUp, parts, err := s.ListParts(ctx, "b", "dir/big", up.ID)
	if err != nil || gotUp.ID != up.ID || len(parts) != 2 || parts[0] != p1 || parts[1] != p2 {
		t.Errorf("ListParts = %+v, %+v, %v, want the upload and parts 1 and 2 in order", gotUp, parts, err)
	}
	uploads, err := s.ListUploads(ctx, "b")
	if err != nil || len(uploads) != 1 || uploads[0].ID != up.ID {
		t.Errorf("ListUploads = %+v, %v, want the one upload", uploads, err)
	}

	info, err := s.CompleteUpload(ctx, "b", "dir/big", up.ID, listed(p1, p2), CompleteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantETag := hex.EncodeToString(md5Sum(md5Sum(bigPart), md5Sum(smallPart))) + "-2"
	if info.ETag != wantETag || info.Size != int64(len(bigPart)+len(smallPart)) || info.Metadata["X-Amz-Meta-Color"] != "blue" {
		t.Errorf("CompleteUpload = %+v, want ETag %s, size %d, and the upload's metadata", info, wantETag, len(bigPart)+len(smallPart))
	}
	body, got := mustGet(t, s, "b", "dir/big")
	if body != string(bigPart)+string(smallPart) || got.ETag != wantETag {
		t.Errorf("GetObject = %d bytes, ETag %s, want the joined parts and ETag %s", len(body), got.ETag, wantETag)
	}
	if uploads, err := s.ListUploads(ctx, "b"); err != nil || len(uploads) != 0 {
		t.Errorf("ListUploads after complete = %+v, %v, want none", uploads, err)
	}
	if n := dirLen(t, fsys, "buckets/b/uploads"); n != 0 {
		t.Errorf("uploads directory has %d entries after complete, want 0", n)
	}
	if n := dirLen(t, fsys, "buckets/b/blobs"); n != 1 {
		t.Errorf("blobs directory has %d entries, want 1", n)
	}
	if _, err := s.CompleteUpload(ctx, "b", "dir/big", up.ID, listed(p1, p2), CompleteOptions{}); !errors.Is(err, ErrNoSuchUpload) {
		t.Errorf("second CompleteUpload error = %v, want ErrNoSuchUpload", err)
	}
}

// TestUploadChecksums compares each formula with a computation that uses only the standard library.
func TestUploadChecksums(t *testing.T) {
	whole := append(append([]byte{}, bigPart...), smallPart...)
	crc := func(sum uint32) []byte { return binary.BigEndian.AppendUint32(nil, sum) }
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	sha1Sum := func(b []byte) []byte { s := sha1.Sum(b); return s[:] }
	sha256Sum := func(b []byte) []byte { s := sha256.Sum256(b); return s[:] }
	type hashFunc func([]byte) []byte
	algorithms := []struct {
		name string
		sum  hashFunc
	}{
		{checksum.CRC32, func(b []byte) []byte { return crc(crc32.ChecksumIEEE(b)) }},
		{checksum.CRC32C, func(b []byte) []byte { return crc(crc32.Checksum(b, castagnoli)) }},
		{checksum.SHA1, sha1Sum},
		{checksum.SHA256, sha256Sum},
	}
	enc := base64.StdEncoding.EncodeToString
	for _, a := range algorithms {
		for _, typ := range []string{checksum.Composite, checksum.FullObject} {
			if typ == checksum.FullObject && a.name != checksum.CRC32 && a.name != checksum.CRC32C {
				continue // AWS allows FULL_OBJECT only for the CRCs
			}
			t.Run(a.name+"/"+typ, func(t *testing.T) {
				s, _ := newStore(t)
				mustCreate(t, s, "b")
				up := mustUpload(t, s, "b", "k", UploadOptions{ChecksumAlgorithm: a.name, ChecksumType: typ})
				p1 := mustPart(t, s, "b", "k", up.ID, 1, bigPart, PartOptions{})
				p2 := mustPart(t, s, "b", "k", up.ID, 2, smallPart, PartOptions{})
				if want := enc(a.sum(bigPart)); p1.Checksum != want || p1.ChecksumAlgorithm != a.name {
					t.Errorf("PutPart(1) = %s %s, want %s %s", p1.ChecksumAlgorithm, p1.Checksum, a.name, want)
				}
				info, err := s.CompleteUpload(context.Background(), "b", "k", up.ID, listed(p1, p2), CompleteOptions{})
				if err != nil {
					t.Fatal(err)
				}
				want := enc(a.sum(whole))
				if typ == checksum.Composite {
					want = enc(a.sum(append(a.sum(bigPart), a.sum(smallPart)...))) + "-2"
				}
				if info.Checksum != want || info.ChecksumAlgorithm != a.name || info.ChecksumType != typ {
					t.Errorf("CompleteUpload checksum = %s %s %s, want %s %s %s", info.ChecksumAlgorithm, info.Checksum, info.ChecksumType, a.name, want, typ)
				}
				if head, err := s.HeadObject(context.Background(), "b", "k"); err != nil || head.Checksum != want || head.ChecksumType != typ {
					t.Errorf("HeadObject = %+v, %v, want the completed checksum", head, err)
				}
			})
		}
	}

	t.Run("no algorithm gets the default of a PutObject", func(t *testing.T) {
		s, _ := newStore(t)
		mustCreate(t, s, "b")
		up := mustUpload(t, s, "b", "k", UploadOptions{})
		p1 := mustPart(t, s, "b", "k", up.ID, 1, whole, PartOptions{})
		info, err := s.CompleteUpload(context.Background(), "b", "k", up.ID, listed(p1), CompleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		put := mustPut(t, s, "b", "put", string(whole))
		if info.Checksum != put.Checksum || info.ChecksumAlgorithm != checksum.Default || info.ChecksumType != checksum.FullObject {
			t.Errorf("CompleteUpload checksum = %s %s %s, want %s %s %s like PutObject", info.ChecksumAlgorithm, info.Checksum, info.ChecksumType, checksum.Default, put.Checksum, checksum.FullObject)
		}
	})
}

func TestPutPartChecksum(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	plain := mustUpload(t, s, "b", "k", UploadOptions{})
	crc := mustUpload(t, s, "b", "k", UploadOptions{ChecksumAlgorithm: checksum.CRC32, ChecksumType: checksum.Composite})
	body := []byte("hello world")
	good := binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(body))

	tests := []struct {
		name         string
		id           string
		opts         PartOptions
		want         error
		wantAlg      string
		wantChecksum string
	}{
		{"upload without algorithm, none sent", plain.ID, PartOptions{}, nil, "", ""},
		{"upload without algorithm, client sends CRC32", plain.ID, PartOptions{ChecksumAlgorithm: "CRC32", Checksum: good}, nil, "CRC32", "DUoRhQ=="},
		{"upload with CRC32, none sent", crc.ID, PartOptions{}, nil, "CRC32", "DUoRhQ=="},
		{"upload with CRC32, same algorithm sent", crc.ID, PartOptions{ChecksumAlgorithm: "CRC32", Checksum: good}, nil, "CRC32", "DUoRhQ=="},
		{"upload with CRC32, trailer algorithm without a value", crc.ID, PartOptions{ChecksumAlgorithm: "CRC32"}, nil, "CRC32", "DUoRhQ=="},
		{"upload with CRC32, another algorithm sent", crc.ID, PartOptions{ChecksumAlgorithm: "SHA1"}, ErrChecksumAlgorithmMismatch, "", ""},
		{"wrong checksum", crc.ID, PartOptions{ChecksumAlgorithm: "CRC32", Checksum: make([]byte, 4)}, ErrChecksumMismatch, "", ""},
		{"wrong checksum, upload without algorithm", plain.ID, PartOptions{ChecksumAlgorithm: "CRC32", Checksum: make([]byte, 4)}, ErrChecksumMismatch, "", ""},
		{"wrong content MD5", plain.ID, PartOptions{ContentMD5: make([]byte, 16)}, ErrBadDigest, "", ""},
		{"right content MD5", plain.ID, PartOptions{ContentMD5: md5Sum(body)}, nil, "", ""},
	}
	for _, tt := range tests {
		p, err := s.PutPart(ctx, "b", "k", tt.id, 1, bytes.NewReader(body), tt.opts)
		if !errors.Is(err, tt.want) {
			t.Errorf("%s: PutPart error = %v, want %v", tt.name, err, tt.want)
			continue
		}
		if err == nil && (p.ChecksumAlgorithm != tt.wantAlg || p.Checksum != tt.wantChecksum) {
			t.Errorf("%s: PutPart checksum = %q %q, want %q %q", tt.name, p.ChecksumAlgorithm, p.Checksum, tt.wantAlg, tt.wantChecksum)
		}
	}
}

func TestPutPartReplacesPart(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	up := mustUpload(t, s, "b", "k", UploadOptions{})
	mustPart(t, s, "b", "k", up.ID, 1, []byte("first"), PartOptions{})
	p := mustPart(t, s, "b", "k", up.ID, 1, []byte("second"), PartOptions{})

	_, parts, err := s.ListParts(ctx, "b", "k", up.ID)
	if err != nil || len(parts) != 1 || parts[0] != p || parts[0].Size != 6 {
		t.Errorf("ListParts = %+v, %v, want only the second upload of part 1", parts, err)
	}
	// upload.json, part-1.json, and the one data file.
	if n := dirLen(t, fsys, path.Join("buckets/b/uploads", up.ID)); n != 3 {
		t.Errorf("upload directory has %d entries, want 3", n)
	}
	if _, err := s.CompleteUpload(ctx, "b", "k", up.ID, listed(p), CompleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if body, _ := mustGet(t, s, "b", "k"); body != "second" {
		t.Errorf("object = %q, want %q", body, "second")
	}
}

func TestCompleteUploadErrors(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	crc := mustUpload(t, s, "b", "k", UploadOptions{ChecksumAlgorithm: checksum.CRC32, ChecksumType: checksum.Composite})
	p1 := mustPart(t, s, "b", "k", crc.ID, 1, bigPart, PartOptions{})
	p2 := mustPart(t, s, "b", "k", crc.ID, 2, smallPart, PartOptions{})
	p3 := mustPart(t, s, "b", "k", crc.ID, 3, smallPart, PartOptions{})
	wrongETag := CompletePart{PartNumber: 1, ETag: `"` + strings.Repeat("0", 32) + `"`}
	tests := []struct {
		name  string
		parts []CompletePart
		want  error
	}{
		{"no parts", nil, ErrInvalidPart},
		{"descending", listed(p2, p1), ErrInvalidPartOrder},
		{"repeated", listed(p1, p1), ErrInvalidPartOrder},
		{"order is checked before the parts exist", []CompletePart{{PartNumber: 8}, {PartNumber: 7}}, ErrInvalidPartOrder},
		{"part never uploaded", []CompletePart{{PartNumber: 9, ETag: p1.ETag}}, ErrInvalidPart},
		{"part number zero", []CompletePart{{PartNumber: 0, ETag: p1.ETag}}, ErrInvalidPart},
		{"part number over the limit", []CompletePart{{PartNumber: MaxParts + 1, ETag: p1.ETag}}, ErrInvalidPart},
		{"wrong ETag", []CompletePart{wrongETag}, ErrInvalidPart},
		{"wrong checksum", []CompletePart{{PartNumber: 1, ETag: p1.ETag, ChecksumAlgorithm: "CRC32", Checksum: "AAAAAA=="}}, ErrInvalidPart},
		{"checksum of another algorithm", []CompletePart{{PartNumber: 1, ETag: p1.ETag, ChecksumAlgorithm: "CRC32C", Checksum: p1.Checksum}}, ErrInvalidPart},
		{"small part before the last", listed(p2, p3), ErrEntityTooSmall},
		{"small last part", listed(p1, p3), nil},
	}
	for _, tt := range tests {
		_, err := s.CompleteUpload(ctx, "b", "k", crc.ID, tt.parts, CompleteOptions{})
		if !errors.Is(err, tt.want) {
			t.Errorf("%s: CompleteUpload error = %v, want %v", tt.name, err, tt.want)
		}
		if tt.want != nil {
			if _, err := s.HeadObject(ctx, "b", "k"); !errors.Is(err, ErrNoSuchKey) {
				t.Errorf("%s: a failed CompleteUpload stored the object: HeadObject error = %v", tt.name, err)
			}
		}
	}

	// A matching client checksum, with or without quotes on the ETag, completes.
	up := mustUpload(t, s, "b", "k2", UploadOptions{ChecksumAlgorithm: checksum.CRC32, ChecksumType: checksum.Composite})
	p := mustPart(t, s, "b", "k2", up.ID, 1, smallPart, PartOptions{})
	_, err := s.CompleteUpload(ctx, "b", "k2", up.ID, []CompletePart{{PartNumber: 1, ETag: p.ETag, ChecksumAlgorithm: "CRC32", Checksum: p.Checksum}}, CompleteOptions{})
	if err != nil {
		t.Errorf("CompleteUpload with an unquoted ETag and the part's checksum error = %v, want nil", err)
	}
}

func TestCompleteFullObjectChecksum(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	body := []byte("hello world")
	good := binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(body))
	newUpload := func(opts UploadOptions) (UploadInfo, []CompletePart) {
		up := mustUpload(t, s, "b", "k", opts)
		return up, listed(mustPart(t, s, "b", "k", up.ID, 1, body, PartOptions{}))
	}
	full := UploadOptions{ChecksumAlgorithm: checksum.CRC32, ChecksumType: checksum.FullObject}
	composite := UploadOptions{ChecksumAlgorithm: checksum.CRC32, ChecksumType: checksum.Composite}
	tests := []struct {
		name string
		up   UploadOptions
		opts CompleteOptions
		want error
	}{
		{"full-object upload, right value", full, CompleteOptions{ChecksumAlgorithm: "CRC32", FullObjectChecksum: good}, nil},
		{"full-object upload, wrong value", full, CompleteOptions{ChecksumAlgorithm: "CRC32", FullObjectChecksum: make([]byte, 4)}, ErrChecksumMismatch},
		{"full-object upload, other algorithm", full, CompleteOptions{ChecksumAlgorithm: "CRC32C", FullObjectChecksum: good}, ErrChecksumAlgorithmMismatch},
		{"composite upload has no whole-object value", composite, CompleteOptions{ChecksumAlgorithm: "CRC32", FullObjectChecksum: good}, ErrChecksumAlgorithmMismatch},
		{"upload without algorithm, default algorithm", UploadOptions{}, CompleteOptions{ChecksumAlgorithm: "CRC64NVME", FullObjectChecksum: make([]byte, 8)}, ErrChecksumMismatch},
	}
	for _, tt := range tests {
		up, parts := newUpload(tt.up)
		_, err := s.CompleteUpload(ctx, "b", "k", up.ID, parts, tt.opts)
		if !errors.Is(err, tt.want) {
			t.Errorf("%s: CompleteUpload error = %v, want %v", tt.name, err, tt.want)
		}
		if tt.want != nil {
			// The upload survives a failed completion, so the client can retry.
			if _, _, err := s.ListParts(ctx, "b", "k", up.ID); err != nil {
				t.Errorf("%s: ListParts after a failed complete error = %v, want nil", tt.name, err)
			}
		}
	}
}

func TestCompleteConditions(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	old := mustPut(t, s, "b", "k", "old")
	newUpload := func(key string) (UploadInfo, []CompletePart) {
		up := mustUpload(t, s, "b", key, UploadOptions{})
		return up, listed(mustPart(t, s, "b", key, up.ID, 1, []byte("new"), PartOptions{}))
	}
	tests := []struct {
		name string
		key  string
		opts CompleteOptions
		want error
	}{
		{"if-none-match on an existing key", "k", CompleteOptions{IfNoneMatch: true}, ErrPreconditionFailed},
		{"if-none-match on a new key", "fresh", CompleteOptions{IfNoneMatch: true}, nil},
		{"if-match with another ETag", "k", CompleteOptions{IfMatch: `"0000"`}, ErrPreconditionFailed},
		{"if-match on a missing key", "missing", CompleteOptions{IfMatch: `"0000"`}, ErrNoSuchKey},
		{"if-match with the current ETag", "k", CompleteOptions{IfMatch: `"` + old.ETag + `"`}, nil},
	}
	for _, tt := range tests {
		up, parts := newUpload(tt.key)
		_, err := s.CompleteUpload(ctx, "b", tt.key, up.ID, parts, tt.opts)
		if !errors.Is(err, tt.want) {
			t.Errorf("%s: CompleteUpload error = %v, want %v", tt.name, err, tt.want)
		}
		if _, _, listErr := s.ListParts(ctx, "b", tt.key, up.ID); (listErr == nil) != (tt.want != nil) {
			t.Errorf("%s: upload exists after complete = %v, want %v", tt.name, listErr == nil, tt.want != nil)
		}
	}
	if body, _ := mustGet(t, s, "b", "k"); body != "new" {
		t.Errorf("object after the successful if-match = %q, want %q", body, "new")
	}
	// k and fresh each keep one blob; the failed completions left none.
	if n := dirLen(t, fsys, "buckets/b/blobs"); n != 2 {
		t.Errorf("blobs directory has %d entries, want 2", n)
	}
}

func TestUploadAddressing(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	up := mustUpload(t, s, "b", "k", UploadOptions{})
	mustPart(t, s, "b", "k", up.ID, 1, []byte("x"), PartOptions{})

	ops := map[string]func(bucket, key, id string) error{
		"PutPart": func(bucket, key, id string) error {
			_, err := s.PutPart(ctx, bucket, key, id, 1, strings.NewReader("x"), PartOptions{})
			return err
		},
		"ListParts": func(bucket, key, id string) error {
			_, _, err := s.ListParts(ctx, bucket, key, id)
			return err
		},
		"CompleteUpload": func(bucket, key, id string) error {
			_, err := s.CompleteUpload(ctx, bucket, key, id, []CompletePart{{PartNumber: 1}}, CompleteOptions{})
			return err
		},
		"AbortUpload": func(bucket, key, id string) error { return s.AbortUpload(ctx, bucket, key, id) },
	}
	tests := []struct {
		name            string
		bucket, key, id string
		want            error
	}{
		{"unknown ID", "b", "k", strings.Repeat("0", 32), ErrNoSuchUpload},
		{"empty ID", "b", "k", "", ErrNoSuchUpload},
		{"short ID", "b", "k", up.ID[:31], ErrNoSuchUpload},
		{"uppercase ID", "b", "k", strings.ToUpper(up.ID), ErrNoSuchUpload},
		{"path in ID", "b", "k", "../" + up.ID[3:], ErrNoSuchUpload},
		{"ID with a slash", "b", "k", up.ID[:16] + "/" + up.ID[17:], ErrNoSuchUpload},
		{"another key", "b", "other", up.ID, ErrNoSuchUpload},
		{"missing bucket", "nope", "k", up.ID, ErrNoSuchBucket},
	}
	for opName, op := range ops {
		for _, tt := range tests {
			if err := op(tt.bucket, tt.key, tt.id); !errors.Is(err, tt.want) {
				t.Errorf("%s with %s: error = %v, want %v", opName, tt.name, err, tt.want)
			}
		}
	}
	if _, err := s.CreateUpload(ctx, "nope", "k", UploadOptions{}); !errors.Is(err, ErrNoSuchBucket) {
		t.Errorf("CreateUpload in a missing bucket error = %v, want ErrNoSuchBucket", err)
	}
	if _, err := s.CreateUpload(ctx, "b", "", UploadOptions{}); !errors.Is(err, ErrInvalidName) {
		t.Errorf("CreateUpload with an empty key error = %v, want ErrInvalidName", err)
	}
	if _, err := s.ListUploads(ctx, "nope"); !errors.Is(err, ErrNoSuchBucket) {
		t.Errorf("ListUploads of a missing bucket error = %v, want ErrNoSuchBucket", err)
	}
	for _, n := range []int{0, -1, MaxParts + 1} {
		if _, err := s.PutPart(ctx, "b", "k", up.ID, n, strings.NewReader("x"), PartOptions{}); err == nil {
			t.Errorf("PutPart(part %d) error = nil, want an error", n)
		}
	}
	// None of that touched the real upload.
	if _, parts, err := s.ListParts(ctx, "b", "k", up.ID); err != nil || len(parts) != 1 {
		t.Errorf("ListParts = %+v, %v, want the one part", parts, err)
	}
}

func TestFailedPutPartLeavesNoTrace(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	up := mustUpload(t, s, "b", "k", UploadOptions{})
	p := mustPart(t, s, "b", "k", up.ID, 1, []byte("original"), PartOptions{})
	errSig := errors.New("signature mismatch")

	_, err := s.PutPart(ctx, "b", "k", up.ID, 1, &failingReader{r: strings.NewReader("new"), err: errSig}, PartOptions{})
	if !errors.Is(err, errSig) {
		t.Errorf("PutPart error = %v, want %v", err, errSig)
	}
	if _, err := s.PutPart(ctx, "b", "k", up.ID, 1, strings.NewReader("new"), PartOptions{ContentMD5: make([]byte, 16)}); !errors.Is(err, ErrBadDigest) {
		t.Errorf("PutPart with a wrong MD5 error = %v, want ErrBadDigest", err)
	}
	if _, parts, err := s.ListParts(ctx, "b", "k", up.ID); err != nil || len(parts) != 1 || parts[0] != p {
		t.Errorf("ListParts = %+v, %v, want the original part", parts, err)
	}
	if n := dirLen(t, fsys, path.Join("buckets/b/uploads", up.ID)); n != 3 {
		t.Errorf("upload directory has %d entries, want 3", n)
	}
	if n := dirLen(t, fsys, "tmp"); n != 0 {
		t.Errorf("temp directory has %d entries, want 0", n)
	}
}

func TestAbortUpload(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	up := mustUpload(t, s, "b", "k", UploadOptions{})
	mustPart(t, s, "b", "k", up.ID, 1, []byte("x"), PartOptions{})
	if err := s.AbortUpload(ctx, "b", "k", up.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.AbortUpload(ctx, "b", "k", up.ID); !errors.Is(err, ErrNoSuchUpload) {
		t.Errorf("second AbortUpload error = %v, want ErrNoSuchUpload", err)
	}
	if _, err := s.PutPart(ctx, "b", "k", up.ID, 2, strings.NewReader("x"), PartOptions{}); !errors.Is(err, ErrNoSuchUpload) {
		t.Errorf("PutPart after abort error = %v, want ErrNoSuchUpload", err)
	}
	if n := dirLen(t, fsys, "buckets/b/uploads"); n != 0 {
		t.Errorf("uploads directory has %d entries after abort, want 0", n)
	}
}

func TestListUploadsOrder(t *testing.T) {
	// The bubble's clock moves only on Sleep, so each upload starts at a distinct time.
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s, _ := newStore(t)
		mustCreate(t, s, "b")
		var created []UploadInfo
		for _, key := range []string{"b", "a", "b", "a/x", "a"} {
			created = append(created, mustUpload(t, s, "b", key, UploadOptions{}))
			time.Sleep(time.Millisecond)
		}
		got, err := s.ListUploads(ctx, "b")
		if err != nil {
			t.Fatal(err)
		}
		// By key, then by start time. The uploads were created in this order.
		want := []UploadInfo{created[1], created[4], created[3], created[0], created[2]}
		for i := range want {
			if i >= len(got) || got[i].ID != want[i].ID {
				t.Fatalf("ListUploads order = %v, want %v", ids(got), ids(want))
			}
		}
	})
}

func ids(uploads []UploadInfo) []string {
	out := make([]string, len(uploads))
	for i, u := range uploads {
		out[i] = u.Key + ":" + u.ID[:6]
	}
	return out
}

func TestDeleteBucketWithUpload(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	mustCreate(t, s, "b")
	up := mustUpload(t, s, "b", "k", UploadOptions{})
	if err := s.DeleteBucket(ctx, "b"); err != nil {
		t.Fatalf("DeleteBucket with an upload error = %v, want nil, as on AWS", err)
	}
	if err := s.AbortUpload(ctx, "b", "k", up.ID); !errors.Is(err, ErrNoSuchBucket) {
		t.Errorf("AbortUpload after DeleteBucket error = %v, want ErrNoSuchBucket", err)
	}
	mustCreate(t, s, "b")
	if uploads, err := s.ListUploads(ctx, "b"); err != nil || len(uploads) != 0 {
		t.Errorf("ListUploads in a re-created bucket = %+v, %v, want none", uploads, err)
	}
}

func TestOpenKeepsUploadsAndRemovesOrphans(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	up := mustUpload(t, s, "b", "k", UploadOptions{})
	p := mustPart(t, s, "b", "k", up.ID, 1, []byte("kept"), PartOptions{})

	// A crash can leave a directory without upload.json and a data file no part names.
	orphanDir := "buckets/b/uploads/" + strings.Repeat("f", 32)
	if err := fsys.MkdirAll(orphanDir); err != nil {
		t.Fatal(err)
	}
	orphanFile := path.Join("buckets/b/uploads", up.ID, "part-2-orphan")
	tf, err := fsys.CreateTemp()
	if err != nil {
		t.Fatal(err)
	}
	if err := tf.Commit(orphanFile); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, fsys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.Stat(orphanDir); err == nil {
		t.Errorf("orphan upload directory still exists after Open")
	}
	if _, err := fsys.Stat(orphanFile); err == nil {
		t.Errorf("orphan part file still exists after Open")
	}
	if _, parts, err := reopened.ListParts(ctx, "b", "k", up.ID); err != nil || len(parts) != 1 || parts[0] != p {
		t.Errorf("ListParts after Open = %+v, %v, want the kept part", parts, err)
	}
	if _, err := reopened.CompleteUpload(ctx, "b", "k", up.ID, listed(p), CompleteOptions{}); err != nil {
		t.Errorf("CompleteUpload after Open error = %v, want nil", err)
	}
}

// Parts, aborts, and completions of one upload race; every call ends with
// success or ErrNoSuchUpload and nothing is left behind.
func TestConcurrentUploadOperations(t *testing.T) {
	ctx := context.Background()
	s, fsys := newStore(t)
	mustCreate(t, s, "b")
	for round := range 20 {
		up := mustUpload(t, s, "b", "k", UploadOptions{})
		first := mustPart(t, s, "b", "k", up.ID, 1, []byte("seed"), PartOptions{})
		var wg sync.WaitGroup
		results := make(chan error, 16)
		for i := range 8 {
			wg.Go(func() {
				_, err := s.PutPart(ctx, "b", "k", up.ID, 1+i%2, strings.NewReader(fmt.Sprintf("body %d", i)), PartOptions{})
				results <- err
			})
		}
		wg.Go(func() { results <- s.AbortUpload(ctx, "b", "k", up.ID) })
		if round%2 == 0 {
			wg.Go(func() {
				_, err := s.CompleteUpload(ctx, "b", "k", up.ID, []CompletePart{{PartNumber: 1, ETag: first.ETag}}, CompleteOptions{})
				if errors.Is(err, ErrInvalidPart) {
					err = nil // part 1 was replaced first
				}
				results <- err
			})
		}
		wg.Wait()
		close(results)
		for err := range results {
			if err != nil && !errors.Is(err, ErrNoSuchUpload) {
				t.Errorf("round %d: error = %v, want nil or ErrNoSuchUpload", round, err)
			}
		}
		if _, _, err := s.ListParts(ctx, "b", "k", up.ID); !errors.Is(err, ErrNoSuchUpload) {
			t.Errorf("round %d: upload exists after abort, ListParts error = %v", round, err)
		}
		if n := dirLen(t, fsys, "buckets/b/uploads"); n != 0 {
			t.Errorf("round %d: uploads directory has %d entries, want 0", round, n)
		}
		_ = s.DeleteObject(ctx, "b", "k")
	}
	if n := dirLen(t, fsys, "buckets/b/blobs"); n != 0 {
		t.Errorf("blobs directory has %d entries, want 0", n)
	}
	if n := dirLen(t, fsys, "tmp"); n != 0 {
		t.Errorf("temp directory has %d entries, want 0", n)
	}
}

// A missing bucket must not leave an entry in the lock map.
func TestCreateUploadInMissingBucketAddsNoLock(t *testing.T) {
	s, _ := newStore(t)
	if _, err := s.CreateUpload(context.Background(), "nope", "k", UploadOptions{}); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("CreateUpload in a missing bucket error = %v, want ErrNoSuchBucket", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets["nope"]; ok {
		t.Errorf("s.buckets has an entry for the missing bucket, want none")
	}
}
