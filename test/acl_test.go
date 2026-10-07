package test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func anonymousRequest(t *testing.T, p *pail, method, endpoint, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, data
}

func TestACLGrants(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, st style, c *s3.Client) {
		ctx := t.Context()
		bucket := aws.String("acl-grants")
		key := aws.String("object")
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		private, err := c.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: bucket})
		if err != nil {
			t.Fatal(err)
		}
		if len(private.Grants) != 1 || private.Grants[0].Permission != types.PermissionFullControl || aws.ToString(private.Owner.ID) == "" || aws.ToString(private.Grants[0].Grantee.ID) != aws.ToString(private.Owner.ID) {
			t.Fatalf("default bucket ACL = %+v", private)
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("private")}); err != nil {
			t.Fatal(err)
		}
		endpoint := p.bucketURL(st, *bucket) + "/" + *key
		if status, _ := anonymousRequest(t, p, http.MethodGet, endpoint, ""); status != 403 {
			t.Fatalf("private GET = %d, want 403", status)
		}
		if _, err := c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: bucket, Key: key, ACL: types.ObjectCannedACLPublicRead}); err != nil {
			t.Fatal(err)
		}
		public, err := c.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: bucket, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if len(public.Grants) != 2 || public.Grants[1].Grantee.Type != types.TypeGroup {
			t.Fatalf("public ACL = %+v", public)
		}
		if status, data := anonymousRequest(t, p, http.MethodGet, endpoint, ""); status != 200 || string(data) != "private" {
			t.Fatalf("public GET = %d, %q", status, data)
		}
		if status, _ := anonymousRequest(t, p, http.MethodGet, endpoint+"?X-Amz-Credential=partial", ""); status != 403 {
			t.Fatalf("partial authentication GET = %d, want 403", status)
		}
		if status, _ := anonymousRequest(t, p, http.MethodGet, endpoint+"?acl", ""); status != 403 {
			t.Fatalf("public-read ACL GET = %d, want 403", status)
		}
		if status, _ := anonymousRequest(t, p, http.MethodPut, endpoint+"?acl", "<AccessControlPolicy/>"); status != 403 {
			t.Fatalf("public-read ACL PUT = %d, want 403", status)
		}
		if status, _ := anonymousRequest(t, p, http.MethodGet, p.bucketURL(st, *bucket), ""); status != 403 {
			t.Fatalf("private bucket listing = %d, want 403", status)
		}
		if _, err := c.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: bucket, GrantRead: aws.String(`uri="http://acs.amazonaws.com/groups/global/AllUsers"`), GrantFullControl: aws.String(`id="` + aws.ToString(private.Owner.ID) + `"`)}); err != nil {
			t.Fatal(err)
		}
		if status, _ := anonymousRequest(t, p, http.MethodGet, p.bucketURL(st, *bucket), ""); status != 200 {
			t.Fatalf("public listing = %d, want 200", status)
		}
		// Replacing an object resets its ACL to private.
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("replacement")}); err != nil {
			t.Fatal(err)
		}
		if status, _ := anonymousRequest(t, p, http.MethodGet, endpoint, ""); status != 403 {
			t.Fatalf("replacement GET = %d, want 403", status)
		}
		policy := &types.AccessControlPolicy{Owner: private.Owner, Grants: []types.Grant{
			{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: private.Owner.ID}, Permission: types.PermissionFullControl},
			{Grantee: &types.Grantee{Type: types.TypeGroup, URI: aws.String("http://acs.amazonaws.com/groups/global/AllUsers")}, Permission: types.PermissionRead},
		}}
		if _, err := c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: bucket, Key: key, AccessControlPolicy: policy}); err != nil {
			t.Fatal(err)
		}
		if status, _ := anonymousRequest(t, p, http.MethodHead, endpoint, ""); status != 200 {
			t.Fatalf("XML ACL HEAD = %d, want 200", status)
		}
		if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: bucket, Key: aws.String("copy"), CopySource: aws.String(*bucket + "/" + *key)}); err != nil {
			t.Fatal(err)
		}
		if status, _ := anonymousRequest(t, p, http.MethodGet, p.bucketURL(st, *bucket)+"/copy", ""); status != 403 {
			t.Fatalf("copy ACL GET = %d, want 403", status)
		}
		if _, err := c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: bucket, Key: key, ACL: types.ObjectCannedACLPrivate, GrantRead: aws.String(`uri="http://acs.amazonaws.com/groups/global/AllUsers"`)}); errorCode(err) != "InvalidRequest" {
			t.Fatalf("mixed ACL error = %v", err)
		}
		if _, err := c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: bucket, Key: aws.String("missing"), ACL: types.ObjectCannedACLPrivate}); errorCode(err) != "NoSuchKey" {
			t.Fatalf("missing ACL error = %v", err)
		}
		// Multipart creation carries its ACL through completion.
		upload, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String("multipart"), ACL: types.ObjectCannedACLPublicRead})
		if err != nil {
			t.Fatal(err)
		}
		part, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: bucket, Key: aws.String("multipart"), UploadId: upload.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("multipart")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: bucket, Key: aws.String("multipart"), UploadId: upload.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}}}}); err != nil {
			t.Fatal(err)
		}
		if status, data := anonymousRequest(t, p, http.MethodGet, p.bucketURL(st, *bucket)+"/multipart", ""); status != 200 || string(data) != "multipart" {
			t.Fatalf("multipart ACL GET = %d, %q", status, data)
		}
		// WRITE on the bucket permits new anonymous objects, not owner object replacements.
		if _, err := c.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: bucket, ACL: types.BucketCannedACLPublicReadWrite}); err != nil {
			t.Fatal(err)
		}
		if status, _ := anonymousRequest(t, p, http.MethodPut, p.bucketURL(st, *bucket)+"/anonymous", "new"); status != 200 {
			t.Fatalf("anonymous PUT = %d, want 200", status)
		}
		status, _, data := sendForm(t, p, p.bucketURL(st, *bucket), map[string]string{"key": "anonymous-form", "acl": "public-read"}, "form", false)
		if status != 204 {
			t.Fatalf("public POST = %d: %s", status, data)
		}
		if status, data := anonymousRequest(t, p, http.MethodGet, p.bucketURL(st, *bucket)+"/anonymous-form", ""); status != 200 || string(data) != "form" {
			t.Fatalf("public POST GET = %d, %q", status, data)
		}
		status, _, data = sendForm(t, p, p.bucketURL(st, *bucket), map[string]string{"key": "partial-auth-form", "x-amz-credential": "partial"}, "body", false)
		if status != 400 {
			t.Fatalf("partial authentication POST = %d: %s", status, data)
		}
		if status, _ := anonymousRequest(t, p, http.MethodPut, endpoint, "forbidden"); status != 403 {
			t.Fatalf("anonymous overwrite = %d, want 403", status)
		}
		if status, _ := anonymousRequest(t, p, http.MethodDelete, endpoint, ""); status != 403 {
			t.Fatalf("anonymous delete = %d, want 403", status)
		}
		obj, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Body.Close()
		data, err = io.ReadAll(obj.Body)
		if err != nil || string(data) != "replacement" {
			t.Fatalf("owner object = %q, %v", data, err)
		}
	})
}
