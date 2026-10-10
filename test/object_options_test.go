package test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// withHeader adds a header before the SDK signs the request.
func withHeader(name, value string) func(*s3.Options) {
	return func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, smithyhttp.AddHeaderValue(name, value))
	}
}

func TestServerSideEncryption(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		mustBucket(t, c, "sse")
		tests := []struct {
			name string
			sse  types.ServerSideEncryption
			want types.ServerSideEncryption
		}{
			{"default", "", types.ServerSideEncryptionAes256},
			{"aes256", types.ServerSideEncryptionAes256, types.ServerSideEncryptionAes256},
			{"kms", types.ServerSideEncryptionAwsKms, types.ServerSideEncryptionAwsKms},
			{"kms dsse", types.ServerSideEncryptionAwsKmsDsse, types.ServerSideEncryptionAwsKmsDsse},
		}
		for _, tt := range tests {
			key := aws.String(strings.ReplaceAll(tt.name, " ", "-"))
			put, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("sse"), Key: key, Body: strings.NewReader("x"), ServerSideEncryption: tt.sse})
			if err != nil || put.ServerSideEncryption != tt.want {
				t.Errorf("%s: PutObject = %q, %v, want %q", tt.name, put.ServerSideEncryption, err, tt.want)
				continue
			}
			head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("sse"), Key: key})
			if err != nil || head.ServerSideEncryption != tt.want {
				t.Errorf("%s: HeadObject = %q, %v, want %q", tt.name, head.ServerSideEncryption, err, tt.want)
			}
			if _, out := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("sse"), Key: key}); out.ServerSideEncryption != tt.want {
				t.Errorf("%s: GetObject = %q, want %q", tt.name, out.ServerSideEncryption, tt.want)
			}
		}

		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("sse"), Key: aws.String("bad"), Body: strings.NewReader("x"), ServerSideEncryption: "bogus"}); errorCode(err) != "InvalidArgument" {
			t.Errorf("PutObject with an unknown method error = %v, want InvalidArgument", err)
		}
		_, err := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("sse"), Key: aws.String("customer"), Body: strings.NewReader("x"),
			SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(strings.Repeat("k", 32)),
		})
		if errorCode(err) != "AccessDenied" {
			t.Errorf("PutObject with SSE-C error = %v, want AccessDenied", err)
		}

		// A multipart upload keeps the method for the object it creates.
		up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String("sse"), Key: aws.String("multi"), ServerSideEncryption: types.ServerSideEncryptionAwsKms})
		if err != nil || up.ServerSideEncryption != types.ServerSideEncryptionAwsKms {
			t.Fatalf("CreateMultipartUpload = %q, %v, want aws:kms", up.ServerSideEncryption, err)
		}
		part, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String("sse"), Key: aws.String("multi"), UploadId: up.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("x")})
		if err != nil || part.ServerSideEncryption != types.ServerSideEncryptionAwsKms {
			t.Fatalf("UploadPart = %q, %v, want aws:kms", part.ServerSideEncryption, err)
		}
		done, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String("sse"), Key: aws.String("multi"), UploadId: up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag, ChecksumCRC32: part.ChecksumCRC32}}},
		})
		if err != nil || done.ServerSideEncryption != types.ServerSideEncryptionAwsKms {
			t.Errorf("CompleteMultipartUpload = %q, %v, want aws:kms", done.ServerSideEncryption, err)
		}

		// A copy takes its method from the request, not from the source.
		cp, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("sse"), Key: aws.String("copy"), CopySource: aws.String("sse/kms")})
		if err != nil || cp.ServerSideEncryption != types.ServerSideEncryptionAes256 {
			t.Errorf("CopyObject = %q, %v, want AES256", cp.ServerSideEncryption, err)
		}
	})
}

func TestStorageClasses(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		mustBucket(t, c, "classes")
		for _, class := range []types.StorageClass{types.StorageClassStandardIa, types.StorageClassGlacier, types.StorageClassGlacierIr} {
			key := aws.String(strings.ToLower(string(class)))
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("classes"), Key: key, Body: strings.NewReader("cold"), StorageClass: class}); err != nil {
				t.Fatalf("PutObject %s error = %v", class, err)
			}
			head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("classes"), Key: key})
			if err != nil || head.StorageClass != class {
				t.Errorf("HeadObject %s = %q, %v, want %s", class, head.StorageClass, err, class)
			}
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("classes"), Key: aws.String("plain"), Body: strings.NewReader("x")}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("classes"), Key: aws.String("bad"), Body: strings.NewReader("x"), StorageClass: "BOGUS"}); errorCode(err) != "InvalidStorageClass" {
			t.Errorf("PutObject with an unknown class error = %v, want InvalidStorageClass", err)
		}

		list, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("classes")})
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]types.ObjectStorageClass{}
		for _, o := range list.Contents {
			got[aws.ToString(o.Key)] = o.StorageClass
		}
		want := map[string]types.ObjectStorageClass{"standard_ia": "STANDARD_IA", "glacier": "GLACIER", "glacier_ir": "GLACIER_IR", "plain": "STANDARD"}
		for key, class := range want {
			if got[key] != class {
				t.Errorf("ListObjectsV2 class of %s = %q, want %s", key, got[key], class)
			}
		}

		// A GLACIER object cannot be read or copied; GLACIER_IR can.
		if _, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("classes"), Key: aws.String("glacier")}); errorCode(err) != "InvalidObjectState" {
			t.Errorf("GetObject of a GLACIER object error = %v, want InvalidObjectState", err)
		}
		if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("classes"), Key: aws.String("copy"), CopySource: aws.String("classes/glacier")}); errorCode(err) != "InvalidObjectState" {
			t.Errorf("CopyObject of a GLACIER object error = %v, want InvalidObjectState", err)
		}
		if body, _ := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("classes"), Key: aws.String("glacier_ir")}); body != "cold" {
			t.Errorf("GetObject of a GLACIER_IR object = %q, want cold", body)
		}

		// A copy has the class of the request, and a multipart upload keeps its own.
		if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("classes"), Key: aws.String("copy"), CopySource: aws.String("classes/standard_ia"), StorageClass: types.StorageClassOnezoneIa}); err != nil {
			t.Fatal(err)
		}
		if head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("classes"), Key: aws.String("copy")}); err != nil || head.StorageClass != types.StorageClassOnezoneIa {
			t.Errorf("HeadObject after a copy with a class = %q, %v, want ONEZONE_IA", head.StorageClass, err)
		}
		if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("classes"), Key: aws.String("copy-default"), CopySource: aws.String("classes/standard_ia")}); err != nil {
			t.Fatal(err)
		}
		if head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("classes"), Key: aws.String("copy-default")}); err != nil || head.StorageClass != "" {
			t.Errorf("HeadObject after a copy without a class = %q, %v, want STANDARD", head.StorageClass, err)
		}

		up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String("classes"), Key: aws.String("multi"), StorageClass: types.StorageClassStandardIa})
		if err != nil {
			t.Fatal(err)
		}
		part, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String("classes"), Key: aws.String("multi"), UploadId: up.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("x")})
		if err != nil {
			t.Fatal(err)
		}
		uploads, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String("classes")})
		if err != nil || len(uploads.Uploads) != 1 || uploads.Uploads[0].StorageClass != types.StorageClassStandardIa {
			t.Errorf("ListMultipartUploads = %+v, %v, want one STANDARD_IA upload", uploads.Uploads, err)
		}
		if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String("classes"), Key: aws.String("multi"), UploadId: up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag, ChecksumCRC32: part.ChecksumCRC32}}},
		}); err != nil {
			t.Fatal(err)
		}
		if head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("classes"), Key: aws.String("multi")}); err != nil || head.StorageClass != types.StorageClassStandardIa {
			t.Errorf("HeadObject after a multipart upload = %q, %v, want STANDARD_IA", head.StorageClass, err)
		}
	})
}

func TestWebsiteRedirect(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		mustBucket(t, c, "redirects")
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("redirects"), Key: aws.String("page"), Body: strings.NewReader("x"), WebsiteRedirectLocation: aws.String("https://example.com/new")}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("redirects"), Key: aws.String("bad"), Body: strings.NewReader("x"), WebsiteRedirectLocation: aws.String("new")}); errorCode(err) != "InvalidRedirectLocation" {
			t.Errorf("PutObject with a relative redirect error = %v, want InvalidRedirectLocation", err)
		}
		head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("redirects"), Key: aws.String("page")})
		if err != nil || aws.ToString(head.WebsiteRedirectLocation) != "https://example.com/new" {
			t.Errorf("HeadObject redirect = %q, %v, want https://example.com/new", aws.ToString(head.WebsiteRedirectLocation), err)
		}
		if _, out := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("redirects"), Key: aws.String("page")}); aws.ToString(out.WebsiteRedirectLocation) != "https://example.com/new" {
			t.Errorf("GetObject redirect = %q, want https://example.com/new", aws.ToString(out.WebsiteRedirectLocation))
		}

		// A default copy drops the redirect; REPLACE takes the new one.
		if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("redirects"), Key: aws.String("copy"), CopySource: aws.String("redirects/page")}); err != nil {
			t.Fatal(err)
		}
		if head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("redirects"), Key: aws.String("copy")}); err != nil || head.WebsiteRedirectLocation != nil {
			t.Errorf("HeadObject after a copy = %q, %v, want no redirect", aws.ToString(head.WebsiteRedirectLocation), err)
		}
		if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket: aws.String("redirects"), Key: aws.String("replaced"), CopySource: aws.String("redirects/page"),
			MetadataDirective: types.MetadataDirectiveReplace, WebsiteRedirectLocation: aws.String("/elsewhere"),
		}); err != nil {
			t.Fatal(err)
		}
		if head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("redirects"), Key: aws.String("replaced")}); err != nil || aws.ToString(head.WebsiteRedirectLocation) != "/elsewhere" {
			t.Errorf("HeadObject after a REPLACE copy = %q, %v, want /elsewhere", aws.ToString(head.WebsiteRedirectLocation), err)
		}
	})
}

func TestNewChecksumAlgorithms(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		mustBucket(t, c, "newsums")
		const body = "hello world"
		// The SDK computes SHA512 itself; for the others the test sends the value of body.
		tests := []struct {
			algorithm types.ChecksumAlgorithm
			sent      string
			value     func(*s3.GetObjectOutput) *string
		}{
			{types.ChecksumAlgorithmSha512, "", func(o *s3.GetObjectOutput) *string { return o.ChecksumSHA512 }},
			{types.ChecksumAlgorithmMd5, "XrY7u+Ae7tCTyyK7j1rNww==", func(o *s3.GetObjectOutput) *string { return o.ChecksumMD5 }},
			{types.ChecksumAlgorithmXxhash64, "RatnNLIeaWg=", func(o *s3.GetObjectOutput) *string { return o.ChecksumXXHASH64 }},
		}
		for _, tt := range tests {
			key := aws.String(strings.ToLower(string(tt.algorithm)))
			in := &s3.PutObjectInput{Bucket: aws.String("newsums"), Key: key, Body: strings.NewReader(body)}
			var opts []func(*s3.Options)
			if tt.sent == "" {
				in.ChecksumAlgorithm = tt.algorithm
			} else {
				opts = append(opts, withHeader("x-amz-checksum-"+strings.ToLower(string(tt.algorithm)), tt.sent))
			}
			if _, err := c.PutObject(ctx, in, opts...); err != nil {
				t.Errorf("PutObject with %s error = %v", tt.algorithm, err)
				continue
			}
			got, out := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("newsums"), Key: key, ChecksumMode: types.ChecksumModeEnabled})
			if got != body || aws.ToString(tt.value(out)) == "" || out.ChecksumType != types.ChecksumTypeFullObject {
				t.Errorf("GetObject %s = %q, checksum %q, type %q, want the body, a checksum, and FULL_OBJECT", tt.algorithm, got, aws.ToString(tt.value(out)), out.ChecksumType)
			}
			if tt.sent != "" && aws.ToString(tt.value(out)) != tt.sent {
				t.Errorf("GetObject %s checksum = %q, want %q", tt.algorithm, aws.ToString(tt.value(out)), tt.sent)
			}
		}
		list, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("newsums")})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range list.Contents {
			if len(o.ChecksumAlgorithm) != 1 || strings.ToLower(string(o.ChecksumAlgorithm[0])) != aws.ToString(o.Key) {
				t.Errorf("listed %s checksum = %v, want its own algorithm", aws.ToString(o.Key), o.ChecksumAlgorithm)
			}
		}

		// A wrong value fails, and XXHASH3 and XXHASH128 are not supported.
		_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("newsums"), Key: aws.String("bad"), Body: strings.NewReader(body), ChecksumSHA512: aws.String(strings.Repeat("A", 86) + "==")})
		if errorCode(err) != "BadDigest" {
			t.Errorf("PutObject with a wrong SHA512 error = %v, want BadDigest", err)
		}
		for _, name := range []string{"x-amz-checksum-xxhash3", "x-amz-checksum-xxhash128"} {
			_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("newsums"), Key: aws.String("bad"), Body: strings.NewReader(body)}, withHeader(name, "AAAAAAAAAAA="))
			if errorCode(err) != "NotImplemented" {
				t.Errorf("PutObject with %s error = %v, want NotImplemented", name, err)
			}
		}
	})
}

func TestRejectUnknownOptions(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		mustBucket(t, c, "reject")
		bucket, key := aws.String("reject"), aws.String("k")
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("original")}); err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name, header, value string
			want                string
		}{
			{"unknown checksum", "x-amz-checksum-bogus", "AAAAAA==", "InvalidRequest"},
			{"object lock mode", "x-amz-object-lock-mode", "GOVERNANCE", "InvalidRequest"},
			{"object lock legal hold", "x-amz-object-lock-legal-hold", "ON", "InvalidRequest"},
		}
		for _, tt := range tests {
			_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("replacement")}, withHeader(tt.header, tt.value))
			if errorCode(err) != tt.want {
				t.Errorf("PutObject with %s error = %v, want %s", tt.name, err, tt.want)
			}
		}
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: strings.NewReader("replacement"), ObjectLockMode: types.ObjectLockModeGovernance, ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn}); errorCode(err) != "InvalidRequest" {
			t.Errorf("PutObject with Object Lock fields error = %v, want InvalidRequest", err)
		}
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("locked"), ObjectLockEnabledForBucket: aws.Bool(true)}); errorCode(err) != "NotImplemented" {
			t.Errorf("CreateBucket with Object Lock error = %v, want NotImplemented", err)
		}
		if body, _ := getBody(t, c, &s3.GetObjectInput{Bucket: bucket, Key: key}); body != "original" {
			t.Errorf("body after rejected writes = %q, want original", body)
		}
	})
}
