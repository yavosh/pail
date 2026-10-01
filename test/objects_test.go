package test

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// mustBucket creates bucket through c.
func mustBucket(t *testing.T, c *s3.Client, bucket string) {
	t.Helper()
	if _, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket(%s) error = %v", bucket, err)
	}
}

// getBody reads an object and returns its body.
func getBody(t *testing.T, c *s3.Client, in *s3.GetObjectInput) (string, *s3.GetObjectOutput) {
	t.Helper()
	out, err := c.GetObject(context.Background(), in)
	if err != nil {
		t.Fatalf("GetObject(%s) error = %v", aws.ToString(in.Key), err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b), out
}

func TestObjectRoundTrip(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "docs")
		const body = "hello world"
		put, err := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket:             aws.String("docs"),
			Key:                aws.String("notes/hello.txt"),
			Body:               strings.NewReader(body),
			ContentType:        aws.String("text/plain"),
			CacheControl:       aws.String("max-age=60"),
			ContentDisposition: aws.String("attachment"),
			ContentLanguage:    aws.String("en"),
			Metadata:           map[string]string{"color": "blue"},
		})
		if err != nil {
			t.Fatalf("PutObject error = %v", err)
		}
		wantETag := fmt.Sprintf(`"%x"`, md5.Sum([]byte(body)))
		if aws.ToString(put.ETag) != wantETag {
			t.Errorf("PutObject ETag = %s, want %s", aws.ToString(put.ETag), wantETag)
		}

		got, out := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String("notes/hello.txt")})
		if got != body || aws.ToString(out.ETag) != wantETag {
			t.Errorf("GetObject = %q, ETag %s, want %q, %s", got, aws.ToString(out.ETag), body, wantETag)
		}
		checks := map[string][2]string{
			"Content-Type":        {aws.ToString(out.ContentType), "text/plain"},
			"Cache-Control":       {aws.ToString(out.CacheControl), "max-age=60"},
			"Content-Disposition": {aws.ToString(out.ContentDisposition), "attachment"},
			"Content-Language":    {aws.ToString(out.ContentLanguage), "en"},
			"x-amz-meta-color":    {out.Metadata["color"], "blue"},
			"Accept-Ranges":       {aws.ToString(out.AcceptRanges), "bytes"},
		}
		for name, c := range checks {
			if c[0] != c[1] {
				t.Errorf("GetObject %s = %q, want %q", name, c[0], c[1])
			}
		}
		if out.LastModified == nil || time.Since(*out.LastModified) > time.Minute {
			t.Errorf("GetObject Last-Modified = %v, want about now", out.LastModified)
		}

		head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("docs"), Key: aws.String("notes/hello.txt")})
		if err != nil || aws.ToInt64(head.ContentLength) != int64(len(body)) || aws.ToString(head.ETag) != wantETag {
			t.Errorf("HeadObject = length %d, ETag %s, %v, want %d, %s", aws.ToInt64(head.ContentLength), aws.ToString(head.ETag), err, len(body), wantETag)
		}
	})
}

func TestObjectRangesAndConditions(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "docs")
		const body = "hello world"
		put, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("docs"), Key: aws.String("k"), Body: strings.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		in := func(f func(*s3.GetObjectInput)) *s3.GetObjectInput {
			i := &s3.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String("k")}
			f(i)
			return i
		}

		ranges := []struct{ spec, want, contentRange string }{
			{"bytes=0-4", "hello", "bytes 0-4/11"},
			{"bytes=-5", "world", "bytes 6-10/11"},
			{"bytes=6-", "world", "bytes 6-10/11"},
		}
		for _, r := range ranges {
			got, out := getBody(t, c, in(func(i *s3.GetObjectInput) { i.Range = aws.String(r.spec) }))
			if got != r.want || aws.ToString(out.ContentRange) != r.contentRange {
				t.Errorf("GetObject Range %s = %q (%s), want %q (%s)", r.spec, got, aws.ToString(out.ContentRange), r.want, r.contentRange)
			}
		}
		_, err = c.GetObject(ctx, in(func(i *s3.GetObjectInput) { i.Range = aws.String("bytes=100-200") }))
		if code := errorCode(err); code != "InvalidRange" {
			t.Errorf("GetObject unsatisfiable Range error = %v, want InvalidRange", err)
		}

		errs := []struct {
			name string
			f    func(*s3.GetObjectInput)
			want string
		}{
			{"if-none-match hit", func(i *s3.GetObjectInput) { i.IfNoneMatch = put.ETag }, "NotModified"},
			{"if-match miss", func(i *s3.GetObjectInput) { i.IfMatch = aws.String(`"nope"`) }, "PreconditionFailed"},
			{"if-unmodified-since past", func(i *s3.GetObjectInput) { i.IfUnmodifiedSince = aws.Time(time.Now().Add(-time.Hour)) }, "PreconditionFailed"},
			{"if-modified-since future", func(i *s3.GetObjectInput) { i.IfModifiedSince = aws.Time(time.Now().Add(time.Hour)) }, "NotModified"},
		}
		for _, e := range errs {
			_, err := c.GetObject(ctx, in(e.f))
			if code := errorCode(err); code != e.want {
				t.Errorf("GetObject %s error = %v, want %s", e.name, err, e.want)
			}
		}
		if got, _ := getBody(t, c, in(func(i *s3.GetObjectInput) { i.IfMatch = put.ETag })); got != body {
			t.Errorf("GetObject If-Match hit = %q, want %q", got, body)
		}

		_, out := getBody(t, c, in(func(i *s3.GetObjectInput) {
			i.ResponseContentType = aws.String("application/json")
			i.ResponseContentDisposition = aws.String("inline")
		}))
		if aws.ToString(out.ContentType) != "application/json" || aws.ToString(out.ContentDisposition) != "inline" {
			t.Errorf("response overrides = %q, %q, want application/json, inline", aws.ToString(out.ContentType), aws.ToString(out.ContentDisposition))
		}
	})
}

func TestConditionalPutAndDelete(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "docs")
		put := func(body string, f func(*s3.PutObjectInput)) (*s3.PutObjectOutput, error) {
			in := &s3.PutObjectInput{Bucket: aws.String("docs"), Key: aws.String("k"), Body: strings.NewReader(body)}
			if f != nil {
				f(in)
			}
			return c.PutObject(ctx, in)
		}
		v1, err := put("v1", func(i *s3.PutObjectInput) { i.IfNoneMatch = aws.String("*") })
		if err != nil {
			t.Fatalf("PutObject If-None-Match * on a new key error = %v", err)
		}
		if _, err := put("v2", func(i *s3.PutObjectInput) { i.IfNoneMatch = aws.String("*") }); errorCode(err) != "PreconditionFailed" {
			t.Errorf("PutObject If-None-Match * on an existing key error = %v, want PreconditionFailed", err)
		}
		if _, err := put("v2", func(i *s3.PutObjectInput) { i.IfMatch = aws.String(`"nope"`) }); errorCode(err) != "PreconditionFailed" {
			t.Errorf("PutObject If-Match miss error = %v, want PreconditionFailed", err)
		}
		if _, err := put("v2", func(i *s3.PutObjectInput) { i.IfMatch = v1.ETag }); err != nil {
			t.Errorf("PutObject If-Match hit error = %v", err)
		}
		bad := md5.Sum([]byte("other"))
		if _, err := put("v3", func(i *s3.PutObjectInput) { i.ContentMD5 = aws.String(base64.StdEncoding.EncodeToString(bad[:])) }); errorCode(err) != "BadDigest" {
			t.Errorf("PutObject wrong Content-MD5 error = %v, want BadDigest", err)
		}
		if got, _ := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String("k")}); got != "v2" {
			t.Errorf("object after conditional puts = %q, want v2", got)
		}

		if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String("docs")}); errorCode(err) != "BucketNotEmpty" {
			t.Errorf("DeleteBucket(non-empty) error = %v, want BucketNotEmpty", err)
		}
		for range 2 { // deleting a missing key succeeds too
			if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("docs"), Key: aws.String("k")}); err != nil {
				t.Errorf("DeleteObject error = %v", err)
			}
		}
		_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String("k")})
		if _, ok := errorAs[*types.NoSuchKey](err); !ok {
			t.Errorf("GetObject after delete error = %v, want NoSuchKey", err)
		}
		_, err = c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("docs"), Key: aws.String("k")})
		if code := errorCode(err); code != "NotFound" {
			t.Errorf("HeadObject after delete error = %v, want NotFound", err)
		}
		_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("nope"), Key: aws.String("k")})
		if code := errorCode(err); code != "NoSuchBucket" {
			t.Errorf("GetObject in a missing bucket error = %v, want NoSuchBucket", err)
		}
	})
}

// specialKeys are keys that path handling tends to break.
var specialKeys = []string{"a//b", "../x", "a+b", "a b", "✓/ünïcode", "dir/", "100%", "a%2Fb", "?q#f"}

func TestSpecialKeys(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "keys")
		for _, key := range specialKeys {
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("keys"), Key: aws.String(key), Body: strings.NewReader("body of " + key)}); err != nil {
				t.Errorf("PutObject(%q) error = %v", key, err)
				continue
			}
			if got, _ := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("keys"), Key: aws.String(key)}); got != "body of "+key {
				t.Errorf("GetObject(%q) = %q, want %q", key, got, "body of "+key)
			}
		}
	})
}
