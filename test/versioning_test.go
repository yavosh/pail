package test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// setVersioning sets the versioning state of bucket.
func setVersioning(t *testing.T, c *s3.Client, bucket string, status types.BucketVersioningStatus) {
	t.Helper()
	_, err := c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: status},
	})
	if err != nil {
		t.Fatalf("PutBucketVersioning(%s) error = %v", status, err)
	}
}

// putVersion writes body under key and returns the version ID of the write.
func putVersion(t *testing.T, c *s3.Client, bucket, key, body string) string {
	t.Helper()
	out, err := c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body)})
	if err != nil {
		t.Fatalf("PutObject(%s) error = %v", key, err)
	}
	return aws.ToString(out.VersionId)
}

func TestBucketVersioningConfiguration(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "vconf")
		got, err := c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String("vconf")})
		if err != nil || got.Status != "" {
			t.Fatalf("GetBucketVersioning on a new bucket = %q, %v, want empty", got.Status, err)
		}
		for _, status := range []types.BucketVersioningStatus{types.BucketVersioningStatusEnabled, types.BucketVersioningStatusSuspended} {
			setVersioning(t, c, "vconf", status)
			if got, err := c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String("vconf")}); err != nil || got.Status != status {
				t.Errorf("GetBucketVersioning = %q, %v, want %q", got.Status, err, status)
			}
		}
		_, err = c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String("vconf"), VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled, MFADelete: types.MFADeleteEnabled,
		}})
		if errorCode(err) != "AccessDenied" {
			t.Errorf("PutBucketVersioning with MFA delete error = %v, want AccessDenied", err)
		}
		_, err = c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String("nobucket")})
		if errorCode(err) != "NoSuchBucket" {
			t.Errorf("GetBucketVersioning on a missing bucket error = %v, want NoSuchBucket", err)
		}
	})
}

func TestVersionedObjects(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "vobj")
		bucket, key := aws.String("vobj"), aws.String("doc")
		pre := putVersion(t, c, "vobj", "doc", "pre")
		if pre != "" {
			t.Errorf("PutObject before versioning VersionId = %q, want none", pre)
		}
		setVersioning(t, c, "vobj", types.BucketVersioningStatusEnabled)
		v1, v2 := putVersion(t, c, "vobj", "doc", "one"), putVersion(t, c, "vobj", "doc", "two")
		if v1 == "" || v2 == "" || v1 == v2 {
			t.Fatalf("version IDs = %q, %q, want two different IDs", v1, v2)
		}

		for _, tt := range []struct{ version, want, wantID string }{
			{"", "two", v2}, {v1, "one", v1}, {"null", "pre", "null"},
		} {
			body, out := getBody(t, c, &s3.GetObjectInput{Bucket: bucket, Key: key, VersionId: versionPtr(tt.version)})
			if body != tt.want || aws.ToString(out.VersionId) != tt.wantID {
				t.Errorf("GetObject(version %q) = %q, version %q, want %q, %q", tt.version, body, aws.ToString(out.VersionId), tt.want, tt.wantID)
			}
		}
		_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key, VersionId: aws.String("0123456789abcdef0123456789abcdef")})
		if errorCode(err) != "NoSuchVersion" {
			t.Errorf("GetObject of an unknown version error = %v, want NoSuchVersion", err)
		}

		// A delete adds a marker; the object stays readable by version.
		del, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key})
		if err != nil || !aws.ToBool(del.DeleteMarker) || aws.ToString(del.VersionId) == "" {
			t.Fatalf("DeleteObject = %+v, %v, want a delete marker with a version ID", del, err)
		}
		if _, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key}); errorCode(err) != "NoSuchKey" {
			t.Errorf("GetObject under a marker error = %v, want NoSuchKey", err)
		}
		if _, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key, VersionId: del.VersionId}); errorCode(err) != "MethodNotAllowed" {
			t.Errorf("GetObject of the marker error = %v, want MethodNotAllowed", err)
		}
		if left := listAll(t, c, "vobj"); len(left) != 0 {
			t.Errorf("ListObjectsV2 under a marker = %v, want none", left)
		}

		// Copy from a version, which the marker hides from a plain copy.
		cp, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: bucket, Key: aws.String("copy"), CopySource: aws.String("vobj/doc?versionId=" + v1)})
		if err != nil || aws.ToString(cp.CopySourceVersionId) != v1 || aws.ToString(cp.VersionId) == "" {
			t.Fatalf("CopyObject from a version = %+v, %v, want source version %q and a new version", cp, err, v1)
		}
		if body, _ := getBody(t, c, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("copy")}); body != "one" {
			t.Errorf("copied body = %q, want one", body)
		}

		// Removing the marker restores the object; removing a version promotes the next.
		rm, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key, VersionId: del.VersionId})
		if err != nil || !aws.ToBool(rm.DeleteMarker) || aws.ToString(rm.VersionId) != aws.ToString(del.VersionId) {
			t.Errorf("DeleteObject of the marker = %+v, %v", rm, err)
		}
		if body, _ := getBody(t, c, &s3.GetObjectInput{Bucket: bucket, Key: key}); body != "two" {
			t.Errorf("restored body = %q, want two", body)
		}
		rm, err = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key, VersionId: aws.String(v2)})
		if err != nil || aws.ToBool(rm.DeleteMarker) || aws.ToString(rm.VersionId) != v2 {
			t.Errorf("DeleteObject of a version = %+v, %v", rm, err)
		}
		if body, _ := getBody(t, c, &s3.GetObjectInput{Bucket: bucket, Key: key}); body != "one" {
			t.Errorf("promoted body = %q, want one", body)
		}
		if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket}); errorCode(err) != "BucketNotEmpty" {
			t.Errorf("DeleteBucket with versions error = %v, want BucketNotEmpty", err)
		}
	})
}

func TestSuspendedVersioning(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "vsusp")
		setVersioning(t, c, "vsusp", types.BucketVersioningStatusEnabled)
		v1 := putVersion(t, c, "vsusp", "k", "one")
		setVersioning(t, c, "vsusp", types.BucketVersioningStatusSuspended)
		if id := putVersion(t, c, "vsusp", "k", "null1"); id != "" {
			t.Errorf("suspended PutObject VersionId = %q, want none", id)
		}
		putVersion(t, c, "vsusp", "k", "null2")
		out, err := c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String("vsusp")})
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, v := range out.Versions {
			ids = append(ids, aws.ToString(v.VersionId))
		}
		if !slices.Equal(ids, []string{"null", v1}) || !aws.ToBool(out.Versions[0].IsLatest) {
			t.Errorf("versions = %v, want [null %s] with null latest", ids, v1)
		}
		del, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("vsusp"), Key: aws.String("k")})
		if err != nil || !aws.ToBool(del.DeleteMarker) || aws.ToString(del.VersionId) != "null" {
			t.Errorf("suspended DeleteObject = %+v, %v, want a null marker", del, err)
		}
		if body, _ := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("vsusp"), Key: aws.String("k"), VersionId: aws.String(v1)}); body != "one" {
			t.Errorf("enabled-era version = %q, want one", body)
		}
	})
}

func TestListObjectVersions(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "vlist")
		putVersion(t, c, "vlist", "plain", "x")
		setVersioning(t, c, "vlist", types.BucketVersioningStatusEnabled)
		for _, key := range []string{"a", "a", "dir/b", "dir/c"} {
			putVersion(t, c, "vlist", key, key)
		}
		if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("vlist"), Key: aws.String("a")}); err != nil {
			t.Fatal(err)
		}

		// Page through everything one entry at a time.
		type entry struct {
			key, id string
			marker  bool
			latest  bool
		}
		var got []entry
		in := &s3.ListObjectVersionsInput{Bucket: aws.String("vlist"), MaxKeys: aws.Int32(1)}
		for range 20 {
			out, err := c.ListObjectVersions(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range out.Versions {
				got = append(got, entry{aws.ToString(v.Key), aws.ToString(v.VersionId), false, aws.ToBool(v.IsLatest)})
			}
			for _, m := range out.DeleteMarkers {
				got = append(got, entry{aws.ToString(m.Key), aws.ToString(m.VersionId), true, aws.ToBool(m.IsLatest)})
			}
			if !aws.ToBool(out.IsTruncated) {
				break
			}
			in.KeyMarker, in.VersionIdMarker = out.NextKeyMarker, out.NextVersionIdMarker
		}
		var keys []string
		for _, e := range got {
			keys = append(keys, e.key)
		}
		if want := []string{"a", "a", "a", "dir/b", "dir/c", "plain"}; !slices.Equal(keys, want) {
			t.Fatalf("paged keys = %v, want %v", keys, want)
		}
		if !got[0].marker || !got[0].latest || got[1].marker || got[1].latest || got[2].latest {
			t.Errorf("entries of a = %+v, want a latest marker over two older versions", got[:3])
		}
		if got[5].id != "null" || !got[5].latest {
			t.Errorf("plain = %+v, want the latest null version", got[5])
		}

		out, err := c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String("vlist"), Delimiter: aws.String("/"), Prefix: aws.String("")})
		if err != nil || len(out.CommonPrefixes) != 1 || aws.ToString(out.CommonPrefixes[0].Prefix) != "dir/" {
			t.Errorf("delimiter listing = %v, %v, want the prefix dir/", out.CommonPrefixes, err)
		}
		out, err = c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String("vlist"), Prefix: aws.String("dir/")})
		if err != nil || len(out.Versions) != 2 || len(out.DeleteMarkers) != 0 {
			t.Errorf("prefix listing = %d versions, %d markers, %v, want 2, 0", len(out.Versions), len(out.DeleteMarkers), err)
		}
	})
}

func TestDeleteObjectsWithVersions(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "vbatch")
		putVersion(t, c, "vbatch", "pre", "pre")
		setVersioning(t, c, "vbatch", types.BucketVersioningStatusEnabled)
		v1 := putVersion(t, c, "vbatch", "k", "one")
		putVersion(t, c, "vbatch", "k", "two")

		out, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("vbatch"), Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: aws.String("k"), VersionId: aws.String(v1)}, {Key: aws.String("pre"), VersionId: aws.String("null")}, {Key: aws.String("k")}, {Key: aws.String("gone")},
		}}})
		if err != nil || len(out.Errors) != 0 || len(out.Deleted) != 4 {
			t.Fatalf("DeleteObjects = %+v, %v, want 4 deleted", out, err)
		}
		d := out.Deleted
		if aws.ToString(d[0].VersionId) != v1 || aws.ToBool(d[0].DeleteMarker) {
			t.Errorf("deleting a version = %+v, want its version ID and no marker", d[0])
		}
		if aws.ToString(d[1].VersionId) != "null" {
			t.Errorf("deleting the null version = %+v, want version null", d[1])
		}
		for _, m := range d[2:] {
			if !aws.ToBool(m.DeleteMarker) || aws.ToString(m.DeleteMarkerVersionId) == "" || m.VersionId != nil {
				t.Errorf("deleting without a version = %+v, want a delete marker", m)
			}
		}
		if _, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("vbatch"), Key: aws.String("pre")}); errorCode(err) != "NoSuchKey" {
			t.Errorf("GetObject of the deleted null version error = %v, want NoSuchKey", err)
		}

		out, err = c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("vbatch"), Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("k"), VersionId: aws.String("bogus")}}}})
		if err != nil || len(out.Errors) != 1 || aws.ToString(out.Errors[0].Code) != "InvalidArgument" {
			t.Errorf("DeleteObjects with a malformed version = %+v, %v, want InvalidArgument", out, err)
		}
	})
}

func TestVersionedMetadataPerVersion(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "vmeta")
		setVersioning(t, c, "vmeta", types.BucketVersioningStatusEnabled)
		v1 := putVersion(t, c, "vmeta", "k", "one")
		putVersion(t, c, "vmeta", "k", "two")
		_, err := c.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: aws.String("vmeta"), Key: aws.String("k"), VersionId: aws.String(v1), Tagging: &types.Tagging{TagSet: tagSet("a", "1")}})
		if err != nil {
			t.Fatal(err)
		}
		old, err := c.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String("vmeta"), Key: aws.String("k"), VersionId: aws.String(v1)})
		if err != nil || len(old.TagSet) != 1 || aws.ToString(old.VersionId) != v1 {
			t.Errorf("tags of the old version = %+v, %v, want one tag", old, err)
		}
		if cur := objectTags(t, c, "vmeta", "k"); len(cur) != 0 {
			t.Errorf("tags of the current version = %v, want none", cur)
		}
		attrs, err := c.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{Bucket: aws.String("vmeta"), Key: aws.String("k"), VersionId: aws.String(v1), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
		if err != nil || aws.ToInt64(attrs.ObjectSize) != 3 || aws.ToString(attrs.VersionId) != v1 {
			t.Errorf("GetObjectAttributes of the old version = %+v, %v, want 3 bytes", attrs, err)
		}
	})
}

// versionPtr is nil for the current version.
func versionPtr(id string) *string {
	if id == "" {
		return nil
	}
	return aws.String(id)
}

func TestVersionedMultipartComplete(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "vmpu")
		setVersioning(t, c, "vmpu", types.BucketVersioningStatusEnabled)
		first := putVersion(t, c, "vmpu", "k", "first")
		bucket, key := aws.String("vmpu"), aws.String("k")
		up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		part, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: bucket, Key: key, UploadId: up.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("second")})
		if err != nil {
			t.Fatal(err)
		}
		done, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: bucket, Key: key, UploadId: up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1), ChecksumCRC32: part.ChecksumCRC32}}}})
		if err != nil || aws.ToString(done.VersionId) == "" || aws.ToString(done.VersionId) == first {
			t.Fatalf("CompleteMultipartUpload = %+v, %v, want a new version ID", done, err)
		}
		if body, _ := getBody(t, c, &s3.GetObjectInput{Bucket: bucket, Key: key, VersionId: aws.String(first)}); body != "first" {
			t.Errorf("earlier version = %q, want first", body)
		}
	})
}
