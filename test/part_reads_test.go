package test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// putMultipart uploads bodies as parts with a CRC32 checksum and completes the upload.
func putMultipart(t *testing.T, c *s3.Client, bucket, key string, bodies ...[]byte) {
	t.Helper()
	up := createUpload(t, c, &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: &key, ChecksumAlgorithm: types.ChecksumAlgorithmCrc32})
	parts := uploadParts(t, c, bucket, key, aws.ToString(up.UploadId), bodies...)
	_, err := c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: &bucket, Key: &key, UploadId: up.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload(%s) error = %v", key, err)
	}
}

func TestGetObjectPartNumber(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "parts")
		first, second := randomBytes(minPart), []byte("tail")
		putMultipart(t, c, "parts", "multi", first, second)
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("parts"), Key: aws.String("simple"), Body: bytes.NewReader([]byte("hello"))}); err != nil {
			t.Fatal(err)
		}

		for _, tt := range []struct {
			key       string
			part      int32
			want      []byte
			wantRange string
			wantCount int32
		}{
			{"multi", 1, first, "bytes 0-5242879/5242884", 2},
			{"multi", 2, second, "bytes 5242880-5242883/5242884", 2},
			{"simple", 1, []byte("hello"), "bytes 0-4/5", 0},
		} {
			out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("parts"), Key: aws.String(tt.key), PartNumber: aws.Int32(tt.part)})
			if err != nil {
				t.Fatalf("GetObject(%s, part %d) error = %v", tt.key, tt.part, err)
			}
			got, err := io.ReadAll(out.Body)
			_ = out.Body.Close()
			if err != nil || !bytes.Equal(got, tt.want) || aws.ToString(out.ContentRange) != tt.wantRange || aws.ToInt32(out.PartsCount) != tt.wantCount {
				t.Errorf("GetObject(%s, part %d) = %d bytes, Content-Range %q, PartsCount %d, %v, want %d bytes, %q, %d",
					tt.key, tt.part, len(got), aws.ToString(out.ContentRange), aws.ToInt32(out.PartsCount), err, len(tt.want), tt.wantRange, tt.wantCount)
			}
			head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("parts"), Key: aws.String(tt.key), PartNumber: aws.Int32(tt.part)})
			if err != nil || aws.ToInt64(head.ContentLength) != int64(len(tt.want)) || aws.ToInt32(head.PartsCount) != tt.wantCount {
				t.Errorf("HeadObject(%s, part %d) = length %d, PartsCount %d, %v, want length %d, PartsCount %d",
					tt.key, tt.part, aws.ToInt64(head.ContentLength), aws.ToInt32(head.PartsCount), err, len(tt.want), tt.wantCount)
			}
		}

		_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("parts"), Key: aws.String("multi"), PartNumber: aws.Int32(3)})
		if apiErr, ok := errors.AsType[smithy.APIError](err); !ok || apiErr.ErrorCode() != "InvalidPartNumber" {
			t.Errorf("GetObject part 3 error = %v, want InvalidPartNumber", err)
		}
		_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("parts"), Key: aws.String("multi"), PartNumber: aws.Int32(2), Range: aws.String("bytes=0-1")})
		if apiErr, ok := errors.AsType[smithy.APIError](err); !ok || apiErr.ErrorCode() != "InvalidRequest" {
			t.Errorf("GetObject part with Range error = %v, want InvalidRequest", err)
		}
	})
}

func TestGetObjectAttributes(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "attrs")
		first, second := randomBytes(minPart), []byte("tail")
		putMultipart(t, c, "attrs", "multi", first, second)
		all := []types.ObjectAttributes{types.ObjectAttributesEtag, types.ObjectAttributesChecksum, types.ObjectAttributesObjectParts, types.ObjectAttributesStorageClass, types.ObjectAttributesObjectSize}

		out, err := c.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{Bucket: aws.String("attrs"), Key: aws.String("multi"), ObjectAttributes: all})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToInt64(out.ObjectSize) != minPart+4 || out.StorageClass != types.StorageClassStandard || out.LastModified == nil ||
			out.Checksum == nil || out.Checksum.ChecksumType != types.ChecksumTypeComposite || aws.ToString(out.Checksum.ChecksumCRC32) == "" ||
			out.ObjectParts == nil || aws.ToInt32(out.ObjectParts.TotalPartsCount) != 2 || len(out.ObjectParts.Parts) != 2 ||
			aws.ToInt64(out.ObjectParts.Parts[0].Size) != minPart || aws.ToInt64(out.ObjectParts.Parts[1].Size) != 4 || aws.ToString(out.ObjectParts.Parts[1].ChecksumCRC32) == "" {
			t.Errorf("GetObjectAttributes = %+v, %+v, %+v, want a composite 2-part object of %d bytes", out, out.Checksum, out.ObjectParts, minPart+4)
		}
		if aws.ToString(out.ETag) == "" || aws.ToString(out.ETag)[0] == '"' {
			t.Errorf("GetObjectAttributes ETag = %q, want a value without quotes", aws.ToString(out.ETag))
		}

		page, err := c.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String("attrs"), Key: aws.String("multi"), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts}, MaxParts: aws.Int32(1),
		})
		if err != nil || page.ObjectParts == nil || len(page.ObjectParts.Parts) != 1 || !aws.ToBool(page.ObjectParts.IsTruncated) || aws.ToString(page.ObjectParts.NextPartNumberMarker) != "1" || page.ETag != nil {
			t.Fatalf("GetObjectAttributes max-parts=1 = %+v, %v, want part 1, truncated, next marker 1, and no ETag", page, err)
		}
		page, err = c.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String("attrs"), Key: aws.String("multi"), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
			PartNumberMarker: aws.String("1"),
		})
		if err != nil || page.ObjectParts == nil || len(page.ObjectParts.Parts) != 1 || aws.ToInt32(page.ObjectParts.Parts[0].PartNumber) != 2 || aws.ToBool(page.ObjectParts.IsTruncated) {
			t.Errorf("GetObjectAttributes after marker 1 = %+v, %v, want part 2 and no truncation", page, err)
		}

		_, err = c.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{Bucket: aws.String("attrs"), Key: aws.String("missing"), ObjectAttributes: all})
		if apiErr, ok := errors.AsType[smithy.APIError](err); !ok || apiErr.ErrorCode() != "NoSuchKey" {
			t.Errorf("GetObjectAttributes of a missing key error = %v, want NoSuchKey", err)
		}
	})
}
