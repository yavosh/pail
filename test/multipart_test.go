package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const minPart = 5 << 20 // the smallest part that is not the last

// randomBytes returns n deterministic bytes, so a wrong join shows as a different hash.
func randomBytes(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(1, 2))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// uploadParts uploads each body as the next part and returns the completed list.
func uploadParts(t *testing.T, c *s3.Client, bucket, key, uploadID string, bodies ...[]byte) []types.CompletedPart {
	t.Helper()
	var parts []types.CompletedPart
	for i, body := range bodies {
		out, err := c.UploadPart(context.Background(), &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
			PartNumber: aws.Int32(int32(i + 1)), Body: bytes.NewReader(body),
		})
		if err != nil {
			t.Fatalf("UploadPart %d error = %v", i+1, err)
		}
		parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(int32(i + 1)), ETag: out.ETag, ChecksumCRC32: out.ChecksumCRC32})
	}
	return parts
}

func createUpload(t *testing.T, c *s3.Client, in *s3.CreateMultipartUploadInput) *s3.CreateMultipartUploadOutput {
	t.Helper()
	out, err := c.CreateMultipartUpload(context.Background(), in)
	if err != nil {
		t.Fatalf("CreateMultipartUpload(%s) error = %v", aws.ToString(in.Key), err)
	}
	return out
}

// uploader is an SDK helper that splits a large body into parts.
type uploader struct {
	name  string
	parts int // the part count for 20 MiB at the helper's default part size
	run   func(ctx context.Context, c *s3.Client, bucket, key string, body []byte) error
}

var uploaders = []uploader{
	// manager is deprecated for transfermanager, but most existing code still uses it.
	{"manager", 4, func(ctx context.Context, c *s3.Client, bucket, key string, body []byte) error {
		_, err := manager.NewUploader(c).Upload(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body)}) //nolint:staticcheck // see above
		return err
	}},
	{"transfermanager", 3, func(ctx context.Context, c *s3.Client, bucket, key string, body []byte) error {
		_, err := transfermanager.New(c).UploadObject(ctx, &transfermanager.UploadObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body)})
		return err
	}},
}

// uploadAndVerify uploads 20 MiB with the helper's default settings, then
// downloads it and checks the bytes and the composite checksum.
func uploadAndVerify(t *testing.T, c *s3.Client, u uploader) {
	t.Helper()
	ctx := context.Background()
	mustBucket(t, c, "bigfiles")
	body := randomBytes(20 << 20)
	if err := u.run(ctx, c, "bigfiles", "dir/big.bin", body); err != nil {
		t.Fatalf("%s upload of %d bytes error = %v", u.name, len(body), err)
	}

	got, _ := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("bigfiles"), Key: aws.String("dir/big.bin")})
	if sha256.Sum256([]byte(got)) != sha256.Sum256(body) {
		t.Errorf("GetObject returned %d bytes with another SHA-256, want the %d uploaded bytes", len(got), len(body))
	}
	// Both helpers send CRC32 per part, which makes the object checksum composite.
	suffix := fmt.Sprintf("-%d", u.parts)
	head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("bigfiles"), Key: aws.String("dir/big.bin"), ChecksumMode: types.ChecksumModeEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(aws.ToString(head.ETag), suffix+`"`) || !strings.HasSuffix(aws.ToString(head.ChecksumCRC32), suffix) || head.ChecksumType != types.ChecksumTypeComposite {
		t.Errorf("HeadObject = ETag %s, CRC32 %s, ChecksumType %s, want a %d-part ETag, a composite CRC32 ending in %s, and COMPOSITE",
			aws.ToString(head.ETag), aws.ToString(head.ChecksumCRC32), head.ChecksumType, u.parts, suffix)
	}
}

func TestMultipartUploader(t *testing.T) {
	for _, u := range uploaders {
		t.Run(u.name, func(t *testing.T) {
			forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
				uploadAndVerify(t, c, u)
			})
		})
	}
}

// Over HTTPS the helpers send each part aws-chunked, with the CRC32 in a trailer.
func TestMultipartUploaderOverHTTPS(t *testing.T) {
	for _, u := range uploaders {
		for _, st := range styles {
			t.Run(u.name+"/"+st.name, func(t *testing.T) {
				// The helpers send parts concurrently, so count with an atomic.
				var chunked atomic.Int32
				c := startPailTLS(t).clientWith(st, func(next http.RoundTripper) http.RoundTripper {
					return roundTripFunc(func(r *http.Request) (*http.Response, error) {
						if r.Header.Get("X-Amz-Content-Sha256") == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" && r.Header.Get("X-Amz-Trailer") != "" {
							chunked.Add(1)
						}
						return next.RoundTrip(r)
					})
				})
				uploadAndVerify(t, c, u)
				if got := int(chunked.Load()); got != u.parts {
					t.Errorf("%s sent %d aws-chunked parts, want %d", u.name, got, u.parts)
				}
			})
		}
	}
}

func TestMultipartLifecycle(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "mpu")
		bucket, key := aws.String("mpu"), aws.String("dir/object.bin")
		first, second := randomBytes(minPart), []byte("tail")

		up := createUpload(t, c, &s3.CreateMultipartUploadInput{
			Bucket: bucket, Key: key, ContentType: aws.String("application/x-test"),
			Metadata: map[string]string{"owner": "me"}, ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
		})
		if up.ChecksumAlgorithm != types.ChecksumAlgorithmCrc32 || up.ChecksumType != types.ChecksumTypeComposite || aws.ToString(up.UploadId) == "" {
			t.Fatalf("CreateMultipartUpload = %s %s %q, want CRC32, COMPOSITE, and an upload ID", up.ChecksumAlgorithm, up.ChecksumType, aws.ToString(up.UploadId))
		}
		parts := uploadParts(t, c, "mpu", "dir/object.bin", aws.ToString(up.UploadId), first, second)

		pl, err := c.ListParts(ctx, &s3.ListPartsInput{Bucket: bucket, Key: key, UploadId: up.UploadId, MaxParts: aws.Int32(1)})
		if err != nil {
			t.Fatal(err)
		}
		if len(pl.Parts) != 1 || !aws.ToBool(pl.IsTruncated) || aws.ToString(pl.NextPartNumberMarker) != "1" ||
			aws.ToInt64(pl.Parts[0].Size) != minPart || aws.ToString(pl.Parts[0].ETag) != aws.ToString(parts[0].ETag) || aws.ToString(pl.Parts[0].ChecksumCRC32) != aws.ToString(parts[0].ChecksumCRC32) {
			t.Errorf("ListParts max-parts=1 = %+v, want part 1 of %d bytes, truncated, next marker 1", pl.Parts, minPart)
		}
		pl, err = c.ListParts(ctx, &s3.ListPartsInput{Bucket: bucket, Key: key, UploadId: up.UploadId, PartNumberMarker: pl.NextPartNumberMarker})
		if err != nil || len(pl.Parts) != 1 || aws.ToInt32(pl.Parts[0].PartNumber) != 2 || aws.ToBool(pl.IsTruncated) {
			t.Errorf("ListParts after marker 1 = %+v, %v, want part 2 and no truncation", pl.Parts, err)
		}

		ul, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: bucket})
		if err != nil || len(ul.Uploads) != 1 || aws.ToString(ul.Uploads[0].Key) != aws.ToString(key) || aws.ToString(ul.Uploads[0].UploadId) != aws.ToString(up.UploadId) {
			t.Errorf("ListMultipartUploads = %+v, %v, want the one upload", ul.Uploads, err)
		}

		done, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: bucket, Key: key, UploadId: up.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(aws.ToString(done.ETag), `-2"`) || !strings.HasSuffix(aws.ToString(done.ChecksumCRC32), "-2") || done.ChecksumType != types.ChecksumTypeComposite {
			t.Errorf("CompleteMultipartUpload = ETag %s, CRC32 %s, ChecksumType %s, want a 2-part ETag, a composite CRC32 ending in -2, and COMPOSITE",
				aws.ToString(done.ETag), aws.ToString(done.ChecksumCRC32), done.ChecksumType)
		}
		got, obj := getBody(t, c, &s3.GetObjectInput{Bucket: bucket, Key: key})
		if got != string(first)+string(second) || aws.ToString(obj.ContentType) != "application/x-test" || obj.Metadata["owner"] != "me" || aws.ToString(obj.ETag) != aws.ToString(done.ETag) {
			t.Errorf("GetObject = %d bytes, Content-Type %q, metadata %v, ETag %s, want the joined bytes, the upload's headers, and the completed ETag",
				len(got), aws.ToString(obj.ContentType), obj.Metadata, aws.ToString(obj.ETag))
		}
		if ul, err = c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: bucket}); err != nil || len(ul.Uploads) != 0 {
			t.Errorf("ListMultipartUploads after Complete = %+v, %v, want none", ul.Uploads, err)
		}
		list, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket})
		if err != nil || len(list.Contents) != 1 || list.Contents[0].ChecksumType != types.ChecksumTypeComposite {
			t.Errorf("ListObjectsV2 = %+v, %v, want the object with ChecksumType COMPOSITE", list.Contents, err)
		}
	})
}

func TestMultipartFullObjectChecksum(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "mpu")
		first, second := randomBytes(minPart), []byte("tail")
		up := createUpload(t, c, &s3.CreateMultipartUploadInput{
			Bucket: aws.String("mpu"), Key: aws.String("full"), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32, ChecksumType: types.ChecksumTypeFullObject,
		})
		if up.ChecksumType != types.ChecksumTypeFullObject {
			t.Fatalf("CreateMultipartUpload ChecksumType = %s, want FULL_OBJECT", up.ChecksumType)
		}
		parts := uploadParts(t, c, "mpu", "full", aws.ToString(up.UploadId), first, second)
		sum := crc32.ChecksumIEEE(append(append([]byte{}, first...), second...))
		want := base64.StdEncoding.EncodeToString([]byte{byte(sum >> 24), byte(sum >> 16), byte(sum >> 8), byte(sum)})

		done, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String("mpu"), Key: aws.String("full"), UploadId: up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}, ChecksumCRC32: aws.String(want), ChecksumType: types.ChecksumTypeFullObject,
		})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToString(done.ChecksumCRC32) != want || done.ChecksumType != types.ChecksumTypeFullObject {
			t.Errorf("CompleteMultipartUpload = CRC32 %s, ChecksumType %s, want the CRC32 of the whole object %s and FULL_OBJECT", aws.ToString(done.ChecksumCRC32), done.ChecksumType, want)
		}
		head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("mpu"), Key: aws.String("full"), ChecksumMode: types.ChecksumModeEnabled})
		if err != nil || aws.ToString(head.ChecksumCRC32) != want {
			t.Errorf("HeadObject CRC32 = %s, %v, want %s", aws.ToString(head.ChecksumCRC32), err, want)
		}

		// A wrong whole-object checksum fails and leaves the upload.
		up = createUpload(t, c, &s3.CreateMultipartUploadInput{
			Bucket: aws.String("mpu"), Key: aws.String("wrong"), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32, ChecksumType: types.ChecksumTypeFullObject,
		})
		parts = uploadParts(t, c, "mpu", "wrong", aws.ToString(up.UploadId), []byte("a"))
		_, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String("mpu"), Key: aws.String("wrong"), UploadId: up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}, ChecksumCRC32: aws.String("AAAAAA=="), ChecksumType: types.ChecksumTypeFullObject,
		})
		if code := errorCode(err); code != "BadDigest" {
			t.Errorf("CompleteMultipartUpload with a wrong CRC32 error = %v, want BadDigest", err)
		}
	})
}

func TestMultipartErrors(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "mpu")
		bucket := aws.String("mpu")
		small := []byte(strings.Repeat("x", 1024))

		up := createUpload(t, c, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("k")})
		id := up.UploadId
		parts := uploadParts(t, c, "mpu", "k", aws.ToString(id), small, small)

		complete := func(key string, uploadID *string, p ...types.CompletedPart) error {
			_, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
				Bucket: bucket, Key: aws.String(key), UploadId: uploadID, MultipartUpload: &types.CompletedMultipartUpload{Parts: p},
			})
			return err
		}
		upload := func(key string, uploadID *string, number int32) error {
			_, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: bucket, Key: aws.String(key), UploadId: uploadID, PartNumber: aws.Int32(number), Body: strings.NewReader("x")})
			return err
		}
		wrongETag := types.CompletedPart{PartNumber: aws.Int32(1), ETag: aws.String(`"00000000000000000000000000000000"`)}
		tests := []struct {
			name string
			err  error
			want string
		}{
			{"complete with parts out of order", complete("k", id, parts[1], parts[0]), "InvalidPartOrder"},
			{"complete with a repeated part", complete("k", id, parts[0], parts[0]), "InvalidPartOrder"},
			{"complete with a wrong ETag", complete("k", id, wrongETag), "InvalidPart"},
			{"complete with a part never uploaded", complete("k", id, types.CompletedPart{PartNumber: aws.Int32(9), ETag: parts[0].ETag}), "InvalidPart"},
			{"complete with a small part before the last", complete("k", id, parts...), "EntityTooSmall"},
			{"complete with no parts", complete("k", id), "MalformedXML"},
			{"complete under another key", complete("other", id, parts[1]), "NoSuchUpload"},
			{"complete with an unknown upload", complete("k", aws.String("nope"), parts[0]), "NoSuchUpload"},
			{"upload part to an unknown upload", upload("k", aws.String(strings.Repeat("0", 32)), 1), "NoSuchUpload"},
			{"upload part under another key", upload("other", id, 1), "NoSuchUpload"},
			{"upload part 0", upload("k", id, 0), "InvalidArgument"},
			{"upload part 10001", upload("k", id, 10001), "InvalidArgument"},
			{"upload part 10000", upload("k", id, 10000), ""},
		}
		for _, tt := range tests {
			if got := errorCode(tt.err); got != tt.want {
				t.Errorf("%s: error = %v, want code %q", tt.name, tt.err, tt.want)
			}
		}

		_, err := c.ListParts(ctx, &s3.ListPartsInput{Bucket: bucket, Key: aws.String("k"), UploadId: aws.String("nope")})
		if code := errorCode(err); code != "NoSuchUpload" {
			t.Errorf("ListParts of an unknown upload error = %v, want NoSuchUpload", err)
		}
		_, err = c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String("missing"), Key: aws.String("k")})
		if code := errorCode(err); code != "NoSuchBucket" {
			t.Errorf("CreateMultipartUpload in a missing bucket error = %v, want NoSuchBucket", err)
		}
		for _, combo := range []struct {
			alg types.ChecksumAlgorithm
			typ types.ChecksumType
		}{
			{types.ChecksumAlgorithmCrc64nvme, types.ChecksumTypeComposite},
			{types.ChecksumAlgorithmSha256, types.ChecksumTypeFullObject},
			{types.ChecksumAlgorithmSha1, types.ChecksumTypeFullObject},
		} {
			_, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("k"), ChecksumAlgorithm: combo.alg, ChecksumType: combo.typ})
			if code := errorCode(err); code != "InvalidRequest" {
				t.Errorf("CreateMultipartUpload with %s and %s error = %v, want InvalidRequest", combo.alg, combo.typ, err)
			}
		}

		if _, err := c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: bucket, Key: aws.String("k"), UploadId: id}); err != nil {
			t.Fatalf("AbortMultipartUpload error = %v", err)
		}
		// As on AWS, aborting again succeeds.
		if _, err = c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: bucket, Key: aws.String("k"), UploadId: id}); err != nil {
			t.Errorf("second AbortMultipartUpload error = %v, want nil", err)
		}
		if err := complete("k", id, parts[0]); errorCode(err) != "NoSuchUpload" {
			t.Errorf("complete after abort error = %v, want NoSuchUpload", err)
		}
		// As on AWS, a pending upload does not keep the bucket.
		if _, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("left")}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket}); err != nil {
			t.Errorf("DeleteBucket with a pending upload error = %v, want nil", err)
		}
	})
}

func TestMultipartListUploads(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "mpu")
		bucket := aws.String("mpu")
		for _, key := range []string{"a", "docs/x", "docs/y", "docs/y", "z"} {
			createUpload(t, c, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String(key)})
		}
		type entry struct{ key, id string }
		var seen []entry
		in := &s3.ListMultipartUploadsInput{Bucket: bucket, MaxUploads: aws.Int32(2)}
		for pages := 0; ; pages++ {
			out, err := c.ListMultipartUploads(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			for _, u := range out.Uploads {
				seen = append(seen, entry{aws.ToString(u.Key), aws.ToString(u.UploadId)})
			}
			if !aws.ToBool(out.IsTruncated) {
				break
			}
			if pages > 5 {
				t.Fatalf("ListMultipartUploads did not end after %d pages", pages)
			}
			in.KeyMarker, in.UploadIdMarker = out.NextKeyMarker, out.NextUploadIdMarker
		}
		var keys []string
		for _, e := range seen {
			keys = append(keys, e.key)
		}
		if want := "a,docs/x,docs/y,docs/y,z"; strings.Join(keys, ",") != want {
			t.Errorf("paged ListMultipartUploads keys = %v, want %s", keys, want)
		}
		if seen[2].id == seen[3].id {
			t.Errorf("two uploads of docs/y share the ID %s, want distinct IDs", seen[2].id)
		}

		out, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: bucket, Delimiter: aws.String("/")})
		if err != nil || len(out.Uploads) != 2 || len(out.CommonPrefixes) != 1 || aws.ToString(out.CommonPrefixes[0].Prefix) != "docs/" {
			t.Errorf("ListMultipartUploads with a delimiter = %d uploads, %+v, %v, want 2 uploads and the prefix docs/", len(out.Uploads), out.CommonPrefixes, err)
		}
		out, err = c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: bucket, Prefix: aws.String("docs/")})
		if err != nil || len(out.Uploads) != 3 {
			t.Errorf("ListMultipartUploads with prefix docs/ = %d uploads, %v, want 3", len(out.Uploads), err)
		}
	})
}

// A presigned UploadPart carries no body signature, so any client can send the part.
func TestPresignedUploadPart(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "mpu")
		bucket, key := aws.String("mpu"), aws.String("presigned")
		up := createUpload(t, c, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: key})
		const body = "presigned part"

		ps, err := s3.NewPresignClient(c).PresignUploadPart(ctx, &s3.UploadPartInput{Bucket: bucket, Key: key, UploadId: up.UploadId, PartNumber: aws.Int32(1)})
		if err != nil {
			t.Fatal(err)
		}
		status, header, msg := sendPresigned(t, p, ps, body)
		if status != http.StatusOK || header.Get("ETag") == "" {
			t.Fatalf("presigned UploadPart = %d, ETag %q, %s, want 200 and an ETag", status, header.Get("ETag"), msg)
		}
		_, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: bucket, Key: key, UploadId: up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: aws.String(header.Get("ETag"))}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := getBody(t, c, &s3.GetObjectInput{Bucket: bucket, Key: key}); got != body {
			t.Errorf("GetObject = %q, want %q", got, body)
		}
	})
}

func TestMultipartCompletionChecksumValidation(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		mustBucket(t, c, "checksums")
		for _, tt := range []struct {
			name         string
			number       int32
			omitChecksum bool
			typ          types.ChecksumType
			want         string
		}{
			{"nonconsecutive", 2, false, types.ChecksumTypeComposite, "InternalError"},
			{"missing-checksum", 1, true, types.ChecksumTypeComposite, "InvalidRequest"},
			{"wrong-type", 1, false, types.ChecksumTypeFullObject, "InvalidRequest"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				ctx := t.Context()
				bucket, key := aws.String("checksums"), aws.String(tt.name)
				up := createUpload(t, c, &s3.CreateMultipartUploadInput{
					Bucket: bucket, Key: key, ChecksumAlgorithm: types.ChecksumAlgorithmCrc32, ChecksumType: types.ChecksumTypeComposite,
				})
				p, err := c.UploadPart(ctx, &s3.UploadPartInput{
					Bucket: bucket, Key: key, UploadId: up.UploadId, PartNumber: aws.Int32(tt.number), Body: strings.NewReader("checksum probe"),
				})
				if err != nil {
					t.Fatal(err)
				}
				part := types.CompletedPart{PartNumber: aws.Int32(tt.number), ETag: p.ETag, ChecksumCRC32: p.ChecksumCRC32}
				if tt.omitChecksum {
					part.ChecksumCRC32 = nil
				}
				_, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
					Bucket: bucket, Key: key, UploadId: up.UploadId, ChecksumType: tt.typ,
					MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{part}},
				})
				if got := errorCode(err); got != tt.want {
					t.Fatalf("CompleteMultipartUpload(%s) code = %q, want %q", tt.name, got, tt.want)
				}
				if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key}); errorCode(err) != "NotFound" {
					t.Errorf("HeadObject after rejected completion = %v, want NotFound", err)
				}
				// Rejection must leave the upload usable for a corrected completion.
				parts := uploadParts(t, c, "checksums", tt.name, aws.ToString(up.UploadId), []byte("corrected"))
				if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
					Bucket: bucket, Key: key, UploadId: up.UploadId,
					MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
				}); err != nil {
					t.Fatalf("corrected completion error = %v, want nil", err)
				}
			})
		}
	})
}
