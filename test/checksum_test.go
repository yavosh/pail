package test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// checksumOf picks the output field for algorithm.
func checksumOf(out *s3.GetObjectOutput, algorithm types.ChecksumAlgorithm) string {
	switch algorithm {
	case types.ChecksumAlgorithmCrc32:
		return aws.ToString(out.ChecksumCRC32)
	case types.ChecksumAlgorithmCrc32c:
		return aws.ToString(out.ChecksumCRC32C)
	case types.ChecksumAlgorithmCrc64nvme:
		return aws.ToString(out.ChecksumCRC64NVME)
	case types.ChecksumAlgorithmSha1:
		return aws.ToString(out.ChecksumSHA1)
	case types.ChecksumAlgorithmSha256:
		return aws.ToString(out.ChecksumSHA256)
	}
	return ""
}

func TestChecksums(t *testing.T) {
	algorithms := []types.ChecksumAlgorithm{
		types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmCrc64nvme,
		types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256,
	}
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "sums")
		for _, alg := range algorithms {
			key := "k-" + strings.ToLower(string(alg))
			put, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("sums"), Key: aws.String(key), Body: strings.NewReader("hello world"), ChecksumAlgorithm: alg})
			if err != nil {
				t.Errorf("PutObject with %s error = %v", alg, err)
				continue
			}
			if put.ChecksumType != types.ChecksumTypeFullObject {
				t.Errorf("PutObject with %s ChecksumType = %q, want FULL_OBJECT", alg, put.ChecksumType)
			}
			// With checksum mode on, the SDK also validates the body against the value.
			got, out := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("sums"), Key: aws.String(key), ChecksumMode: types.ChecksumModeEnabled})
			if got != "hello world" || checksumOf(out, alg) == "" {
				t.Errorf("GetObject %s = %q, checksum %q, want the body and a checksum", alg, got, checksumOf(out, alg))
			}
		}

		// The SDK sends CRC32 by default; pail's own CRC64NVME default applies
		// only to clients that send no checksum (see internal/s3api tests).
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("sums"), Key: aws.String("plain"), Body: strings.NewReader("x")}); err != nil {
			t.Fatal(err)
		}
		head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("sums"), Key: aws.String("plain"), ChecksumMode: types.ChecksumModeEnabled})
		if err != nil || aws.ToString(head.ChecksumCRC32) == "" {
			t.Errorf("HeadObject after a default SDK put = CRC32 %q, %v, want the SDK's CRC32", aws.ToString(head.ChecksumCRC32), err)
		}

		_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("sums"), Key: aws.String("bad"), Body: strings.NewReader("hello world"), ChecksumCRC32: aws.String("AAAAAA==")})
		if code := errorCode(err); code != "BadDigest" {
			t.Errorf("PutObject with a wrong CRC32 error = %v, want BadDigest", err)
		}

		list, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("sums")})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range list.Contents {
			if len(o.ChecksumAlgorithm) != 1 || o.ChecksumType != types.ChecksumTypeFullObject {
				t.Errorf("listed %s checksum = %v %q, want one algorithm and FULL_OBJECT", aws.ToString(o.Key), o.ChecksumAlgorithm, o.ChecksumType)
			}
		}
	})
}
