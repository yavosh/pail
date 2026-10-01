package test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// errorAs is errors.AsType, named for reading at call sites.
func errorAs[T error](err error) (T, bool) { return errors.AsType[T](err) }

// errorCode returns the S3 error code of err, or "" for none.
func errorCode(err error) string {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		return apiErr.ErrorCode()
	}
	return ""
}

func TestBucketLifecycle(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		bucket := aws.String("photos")

		if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket}); errorCode(err) != "NotFound" {
			t.Errorf("HeadBucket(missing) error = %v, want NotFound", err)
		}
		if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket}); errorCode(err) != "NoSuchBucket" {
			t.Errorf("DeleteBucket(missing) error = %v, want NoSuchBucket", err)
		}
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatalf("CreateBucket error = %v", err)
		}
		_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket})
		if _, ok := errors.AsType[*types.BucketAlreadyOwnedByYou](err); !ok {
			t.Errorf("CreateBucket(existing) error = %v, want BucketAlreadyOwnedByYou", err)
		}

		head, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket})
		if err != nil || aws.ToString(head.BucketRegion) != testRegion {
			t.Errorf("HeadBucket = region %q, %v, want %q", aws.ToString(head.BucketRegion), err, testRegion)
		}
		loc, err := c.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: bucket})
		if err != nil || loc.LocationConstraint != "" {
			t.Errorf("GetBucketLocation = %q, %v, want empty for us-east-1", loc.LocationConstraint, err)
		}

		// A configuration body is read, and its payload hash verified.
		_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket:                    aws.String("logs"),
			CreateBucketConfiguration: &types.CreateBucketConfiguration{},
		})
		if err != nil {
			t.Fatalf("CreateBucket with a configuration error = %v", err)
		}
		// pail serves one region, so another region's constraint is refused.
		_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket:                    aws.String("elsewhere"),
			CreateBucketConfiguration: &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraintEuWest1},
		})
		if code := errorCode(err); code != "IllegalLocationConstraintException" {
			t.Errorf("CreateBucket in eu-west-1 error = %v, want IllegalLocationConstraintException", err)
		}
		list, err := c.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, b := range list.Buckets {
			names = append(names, aws.ToString(b.Name))
			if b.CreationDate == nil || b.CreationDate.IsZero() {
				t.Errorf("bucket %s has no creation date", aws.ToString(b.Name))
			}
		}
		if want := []string{"logs", "photos"}; !slices.Equal(names, want) {
			t.Errorf("ListBuckets = %v, want %v", names, want)
		}
		if list.Owner == nil || aws.ToString(list.Owner.ID) == "" {
			t.Errorf("ListBuckets owner = %+v, want an ID", list.Owner)
		}

		for _, b := range []string{"photos", "logs"} {
			if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(b)}); err != nil {
				t.Errorf("DeleteBucket(%s) error = %v", b, err)
			}
		}
		if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket}); errorCode(err) != "NotFound" {
			t.Errorf("HeadBucket after delete error = %v, want NotFound", err)
		}
	})
}

func TestInvalidBucketNames(t *testing.T) {
	names := []string{"ab", strings.Repeat("a", 64), "Bucket", "my_bucket", "10.0.0.1", "a..b", "-abc"}
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		for _, name := range names {
			_, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(name)})
			if code := errorCode(err); code != "InvalidBucketName" {
				t.Errorf("CreateBucket(%q) error = %v, want InvalidBucketName", name, err)
			}
		}
	})
}

func TestListBucketsPaginator(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		want := []string{"b1-pages", "b2-pages", "b3-pages", "other"}
		for _, b := range want {
			if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
				t.Fatal(err)
			}
		}
		var got []string
		p := s3.NewListBucketsPaginator(c, &s3.ListBucketsInput{MaxBuckets: aws.Int32(1)})
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, b := range page.Buckets {
				got = append(got, aws.ToString(b.Name))
			}
		}
		if !slices.Equal(got, want) {
			t.Errorf("ListBuckets pages = %v, want %v", got, want)
		}
		page, err := c.ListBuckets(ctx, &s3.ListBucketsInput{Prefix: aws.String("b2")})
		if err != nil || len(page.Buckets) != 1 || aws.ToString(page.Buckets[0].Name) != "b2-pages" {
			t.Errorf("ListBuckets(prefix b2) = %v, %v, want [b2-pages]", page, err)
		}
	})
}
