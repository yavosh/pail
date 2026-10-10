package test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func tagSet(pairs ...string) []types.Tag {
	var out []types.Tag
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, types.Tag{Key: aws.String(pairs[i]), Value: aws.String(pairs[i+1])})
	}
	return out
}

// objectTags returns the tags of key as "k=v" strings.
func objectTags(t *testing.T, c *s3.Client, bucket, key string) []string {
	t.Helper()
	out, err := c.GetObjectTagging(t.Context(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObjectTagging(%s) error = %v", key, err)
	}
	var tags []string
	for _, tag := range out.TagSet {
		tags = append(tags, *tag.Key+"="+*tag.Value)
	}
	return tags
}

func TestObjectTagging(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		mustBucket(t, c, "tags")
		bucket, key := aws.String("tags"), aws.String("k")
		put, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("x"), Tagging: aws.String("color=blue&size=large")})
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(objectTags(t, c, "tags", "k"), ","); got != "color=blue,size=large" {
			t.Errorf("tags after PutObject = %q, want color=blue,size=large", got)
		}
		head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key})
		if err != nil || aws.ToInt32(head.TagCount) != 2 {
			t.Errorf("HeadObject TagCount = %d, %v, want 2", aws.ToInt32(head.TagCount), err)
		}
		if _, out := getBody(t, c, &s3.GetObjectInput{Bucket: bucket, Key: key}); aws.ToInt32(out.TagCount) != 2 {
			t.Errorf("GetObject TagCount = %d, want 2", aws.ToInt32(out.TagCount))
		}

		if _, err := c.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: bucket, Key: key, Tagging: &types.Tagging{TagSet: tagSet("a", "1", "b", "")}}); err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(objectTags(t, c, "tags", "k"), ","); got != "a=1,b=" {
			t.Errorf("tags after PutObjectTagging = %q, want a=1,b=", got)
		}
		after, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key})
		if err != nil || aws.ToString(after.ETag) != aws.ToString(put.ETag) || !after.LastModified.Equal(*head.LastModified) {
			t.Errorf("HeadObject after tagging = %v, %v, want the ETag %s and time %v", after, err, aws.ToString(put.ETag), head.LastModified)
		}

		for _, tt := range []struct {
			name string
			tags []types.Tag
			want string
		}{
			{"duplicate key", tagSet("a", "1", "a", "2"), "InvalidTag"},
			{"aws prefix", tagSet("aws:a", "1"), "InvalidTag"},
			{"long value", tagSet("a", strings.Repeat("v", 257)), "InvalidTag"},
		} {
			_, err := c.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: bucket, Key: key, Tagging: &types.Tagging{TagSet: tt.tags}})
			if errorCode(err) != tt.want {
				t.Errorf("%s: PutObjectTagging error = %v, want %s", tt.name, err, tt.want)
			}
		}
		_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("bad"), Body: strings.NewReader("x"), Tagging: aws.String("aws:k=v")})
		if errorCode(err) != "InvalidTag" {
			t.Errorf("PutObject with an aws: tag error = %v, want InvalidTag", err)
		}

		if _, err := c.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: bucket, Key: key}); err != nil {
			t.Fatal(err)
		}
		if got := objectTags(t, c, "tags", "k"); len(got) != 0 {
			t.Errorf("tags after DeleteObjectTagging = %v, want none", got)
		}
		if head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key}); err != nil || head.TagCount != nil {
			t.Errorf("HeadObject TagCount after delete = %v, %v, want none", head.TagCount, err)
		}
		_, err = c.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: bucket, Key: aws.String("missing")})
		if errorCode(err) != "NoSuchKey" {
			t.Errorf("GetObjectTagging on a missing key error = %v, want NoSuchKey", err)
		}
	})
}

func TestCopyObjectTaggingDirective(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		mustBucket(t, c, "copytags")
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("copytags"), Key: aws.String("src"), Body: strings.NewReader("x"), Tagging: aws.String("s=1")}); err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name      string
			directive types.TaggingDirective
			tagging   *string
			want      string
		}{
			{"default", "", nil, "s=1"},
			{"copy", types.TaggingDirectiveCopy, aws.String("n=2"), "s=1"},
			{"replace", types.TaggingDirectiveReplace, aws.String("n=2"), "n=2"},
			{"replace with none", types.TaggingDirectiveReplace, nil, ""},
		}
		for _, tt := range tests {
			_, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("copytags"), Key: aws.String(tt.name), CopySource: aws.String("copytags/src"), TaggingDirective: tt.directive, Tagging: tt.tagging})
			if err != nil {
				t.Errorf("%s: CopyObject error = %v", tt.name, err)
				continue
			}
			if got := strings.Join(objectTags(t, c, "copytags", tt.name), ","); got != tt.want {
				t.Errorf("%s: copy tags = %q, want %q", tt.name, got, tt.want)
			}
		}
		_, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("copytags"), Key: aws.String("bad"), CopySource: aws.String("copytags/src"), TaggingDirective: "MERGE"})
		if errorCode(err) != "InvalidArgument" {
			t.Errorf("CopyObject with an unknown directive error = %v, want InvalidArgument", err)
		}
	})
}

func TestMultipartUploadTagging(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		mustBucket(t, c, "mputags")
		bucket, key := aws.String("mputags"), aws.String("mpu")
		up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: key, Tagging: aws.String("m=1")})
		if err != nil {
			t.Fatal(err)
		}
		part, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: bucket, Key: key, UploadId: up.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("x")})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: bucket, Key: key, UploadId: up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag, ChecksumCRC32: part.ChecksumCRC32}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(objectTags(t, c, "mputags", "mpu"), ","); got != "m=1" {
			t.Errorf("tags after CompleteMultipartUpload = %q, want m=1", got)
		}
	})
}

func TestBucketTagging(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		mustBucket(t, c, "buckettags")
		bucket := aws.String("buckettags")
		_, err := c.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: bucket})
		if errorCode(err) != "NoSuchTagSet" {
			t.Errorf("GetBucketTagging with none error = %v, want NoSuchTagSet", err)
		}
		if _, err := c.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: bucket, Tagging: &types.Tagging{TagSet: tagSet("team", "storage")}}); err != nil {
			t.Fatal(err)
		}
		got, err := c.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: bucket})
		if err != nil || len(got.TagSet) != 1 || *got.TagSet[0].Key != "team" || *got.TagSet[0].Value != "storage" {
			t.Errorf("GetBucketTagging = %v, %v, want team=storage", got, err)
		}
		_, err = c.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: bucket, Tagging: &types.Tagging{TagSet: tagSet("aws:x", "1")}})
		if errorCode(err) != "InvalidTag" {
			t.Errorf("PutBucketTagging with an aws: tag error = %v, want InvalidTag", err)
		}
		if _, err := c.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		_, err = c.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: bucket})
		if errorCode(err) != "NoSuchTagSet" {
			t.Errorf("GetBucketTagging after delete error = %v, want NoSuchTagSet", err)
		}
	})
}
