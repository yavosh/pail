package test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestOwnershipControls(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, st style, c *s3.Client) {
		ctx := t.Context()
		bucket := aws.String("ownership")
		key := aws.String("object")
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket, ObjectOwnership: "Bogus"}); errorCode(err) != "InvalidArgument" {
			t.Fatalf("CreateBucket(Bogus) error = %v, want InvalidArgument", err)
		}
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: bucket}); errorCode(err) != "OwnershipControlsNotFoundError" {
			t.Fatalf("GetBucketOwnershipControls(default) error = %v, want OwnershipControlsNotFoundError", err)
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("data"), ACL: types.ObjectCannedACLPublicRead}); err != nil {
			t.Fatalf("PutObject(public-read) without controls error = %v", err)
		}
		endpoint := p.bucketURL(st, *bucket) + "/" + *key
		if status, _ := anonymousRequest(t, p, http.MethodGet, endpoint, ""); status != 200 {
			t.Fatalf("anonymous GET before enforcement = %d, want 200", status)
		}

		put := func(ownership types.ObjectOwnership) error {
			_, err := c.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{Bucket: bucket, OwnershipControls: &types.OwnershipControls{Rules: []types.OwnershipControlsRule{{ObjectOwnership: ownership}}}})
			return err
		}
		if err := put("Bogus"); errorCode(err) != "MalformedXML" {
			t.Errorf("PutBucketOwnershipControls(Bogus) error = %v, want MalformedXML", err)
		}
		if err := put(types.ObjectOwnershipBucketOwnerEnforced); err != nil {
			t.Fatal(err)
		}
		got, err := c.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: bucket})
		if err != nil || len(got.OwnershipControls.Rules) != 1 || got.OwnershipControls.Rules[0].ObjectOwnership != types.ObjectOwnershipBucketOwnerEnforced {
			t.Fatalf("GetBucketOwnershipControls = %+v, %v, want BucketOwnerEnforced", got, err)
		}
		if status, _ := anonymousRequest(t, p, http.MethodGet, endpoint, ""); status != 403 {
			t.Errorf("anonymous GET under BucketOwnerEnforced = %d, want 403", status)
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("data"), ACL: types.ObjectCannedACLPublicRead}); errorCode(err) != "AccessControlListNotSupported" {
			t.Errorf("PutObject(public-read) error = %v, want AccessControlListNotSupported", err)
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("data"), GrantRead: aws.String(`uri="http://acs.amazonaws.com/groups/global/AllUsers"`)}); errorCode(err) != "AccessControlListNotSupported" {
			t.Errorf("PutObject(grant) error = %v, want AccessControlListNotSupported", err)
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("data"), ACL: types.ObjectCannedACLBucketOwnerFullControl}); err != nil {
			t.Errorf("PutObject(bucket-owner-full-control) error = %v", err)
		}
		if _, err := c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: bucket, Key: key, ACL: types.ObjectCannedACLPrivate}); errorCode(err) != "AccessControlListNotSupported" {
			t.Errorf("PutObjectAcl error = %v, want AccessControlListNotSupported", err)
		}
		if _, err := c.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: bucket, ACL: types.BucketCannedACLPrivate}); errorCode(err) != "AccessControlListNotSupported" {
			t.Errorf("PutBucketAcl error = %v, want AccessControlListNotSupported", err)
		}
		acl, err := c.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: bucket, Key: key})
		if err != nil || len(acl.Grants) != 1 || acl.Grants[0].Permission != types.PermissionFullControl {
			t.Errorf("GetObjectAcl = %+v, %v, want owner FULL_CONTROL", acl, err)
		}

		if err := put(types.ObjectOwnershipObjectWriter); err != nil {
			t.Fatal(err)
		}
		if _, err := c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: bucket, Key: key, ACL: types.ObjectCannedACLPrivate}); err != nil {
			t.Errorf("PutObjectAcl under ObjectWriter error = %v", err)
		}
		if _, err := c.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: bucket}); errorCode(err) != "OwnershipControlsNotFoundError" {
			t.Errorf("GetBucketOwnershipControls(deleted) error = %v, want OwnershipControlsNotFoundError", err)
		}

		enforced := aws.String("ownership-enforced")
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: enforced, ObjectOwnership: types.ObjectOwnershipBucketOwnerEnforced}); err != nil {
			t.Fatal(err)
		}
		got, err = c.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: enforced})
		if err != nil || got.OwnershipControls.Rules[0].ObjectOwnership != types.ObjectOwnershipBucketOwnerEnforced {
			t.Errorf("GetBucketOwnershipControls(created enforced) = %+v, %v", got, err)
		}
	})
}
