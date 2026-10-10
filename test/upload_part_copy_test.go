package test

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestUploadPartCopy(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "origin")
		small := []byte("0123456789")
		big := randomBytes(2 * minPart) // two ranges of the minimum part size
		for key, body := range map[string][]byte{"small": small, "big": big} {
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("origin"), Key: aws.String(key), Body: bytes.NewReader(body)}); err != nil {
				t.Fatal(err)
			}
		}
		up := createUpload(t, c, &s3.CreateMultipartUploadInput{Bucket: aws.String("origin"), Key: aws.String("joined")})

		// ranges lists the copies: source key and optional range, in part order.
		ranges := []struct{ key, rng string }{
			{"big", "bytes=0-5242879"},
			{"big", "bytes=5242880-10485759"},
			{"small", ""}, // the last part may be under the minimum size
		}
		var parts []types.CompletedPart
		for i, p := range ranges {
			in := &s3.UploadPartCopyInput{
				Bucket: aws.String("origin"), Key: aws.String("joined"), UploadId: up.UploadId,
				PartNumber: aws.Int32(int32(i + 1)), CopySource: aws.String("origin/" + p.key),
			}
			if p.rng != "" {
				in.CopySourceRange = aws.String(p.rng)
			}
			out, err := c.UploadPartCopy(ctx, in)
			if err != nil {
				t.Fatalf("UploadPartCopy %d (%+v) error = %v", i+1, p, err)
			}
			parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(int32(i + 1)), ETag: out.CopyPartResult.ETag})
		}
		if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String("origin"), Key: aws.String("joined"), UploadId: up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		}); err != nil {
			t.Fatalf("CompleteMultipartUpload error = %v", err)
		}

		// A single-part upload copies a range of a small source.
		one := createUpload(t, c, &s3.CreateMultipartUploadInput{Bucket: aws.String("origin"), Key: aws.String("slice")})
		out, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket: aws.String("origin"), Key: aws.String("slice"), UploadId: one.UploadId, PartNumber: aws.Int32(1),
			CopySource: aws.String("origin/small"), CopySourceRange: aws.String("bytes=2-5"),
		})
		if err != nil {
			t.Fatalf("UploadPartCopy of a range error = %v", err)
		}
		if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String("origin"), Key: aws.String("slice"), UploadId: one.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: out.CopyPartResult.ETag}}},
		}); err != nil {
			t.Fatalf("CompleteMultipartUpload of the slice error = %v", err)
		}
		if slice, _ := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("origin"), Key: aws.String("slice")}); slice != "2345" {
			t.Errorf("slice = %q, want %q", slice, "2345")
		}

		want := string(big) + "0123456789"
		got, _ := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("origin"), Key: aws.String("joined")})
		if got != want {
			t.Errorf("GetObject returned %d bytes that differ from the %d copied bytes", len(got), len(want))
		}
	})
}
