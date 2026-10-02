package test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// copySource is the x-amz-copy-source value for key: the SDK sends it as given,
// so each path segment must be escaped here.
func copySource(bucket, key string) string {
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return bucket + "/" + strings.Join(segments, "/")
}

func TestCopyObject(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "origin")
		mustBucket(t, c, "dest")
		const body = "hello world"
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("origin"), Key: aws.String("src"), Body: strings.NewReader(body),
			ContentType: aws.String("text/plain"), Metadata: map[string]string{"color": "blue"},
		}); err != nil {
			t.Fatal(err)
		}

		tests := []struct {
			name        string
			bucket, key string
			directive   types.MetadataDirective
			set         func(*s3.CopyObjectInput)
			wantType    string
			wantColor   string
		}{
			{name: "same bucket", bucket: "origin", key: "dst", wantType: "text/plain", wantColor: "blue"},
			{name: "other bucket", bucket: "dest", key: "dst", wantType: "text/plain", wantColor: "blue"},
			{name: "COPY ignores new metadata", bucket: "dest", key: "copy", directive: types.MetadataDirectiveCopy,
				set: func(in *s3.CopyObjectInput) {
					in.ContentType = aws.String("x/y")
					in.Metadata = map[string]string{"color": "red"}
				},
				wantType: "text/plain", wantColor: "blue"},
			{name: "REPLACE sets new metadata", bucket: "dest", key: "replace", directive: types.MetadataDirectiveReplace,
				set: func(in *s3.CopyObjectInput) {
					in.ContentType = aws.String("application/json")
					in.Metadata = map[string]string{"color": "red"}
				},
				wantType: "application/json", wantColor: "red"},
			{name: "key with a space", bucket: "dest", key: "a b/c d", wantType: "text/plain", wantColor: "blue"},
		}
		for _, tt := range tests {
			in := &s3.CopyObjectInput{
				Bucket: aws.String(tt.bucket), Key: aws.String(tt.key),
				CopySource: aws.String(copySource("origin", "src")), MetadataDirective: tt.directive,
			}
			if tt.set != nil {
				tt.set(in)
			}
			out, err := c.CopyObject(ctx, in)
			if err != nil {
				t.Errorf("%s: CopyObject error = %v", tt.name, err)
				continue
			}
			if out.CopyObjectResult == nil || aws.ToString(out.CopyObjectResult.ETag) == "" || aws.ToTime(out.CopyObjectResult.LastModified).IsZero() {
				t.Errorf("%s: CopyObjectResult = %+v, want an ETag and a LastModified", tt.name, out.CopyObjectResult)
			}
			got, obj := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String(tt.bucket), Key: aws.String(tt.key)})
			if got != body || aws.ToString(obj.ContentType) != tt.wantType || obj.Metadata["color"] != tt.wantColor || aws.ToString(obj.ETag) != aws.ToString(out.CopyObjectResult.ETag) {
				t.Errorf("%s: copy = %q, type %q, color %q, ETag %s; want %q, %q, %q, %s", tt.name, got, aws.ToString(obj.ContentType), obj.Metadata["color"], aws.ToString(obj.ETag), body, tt.wantType, tt.wantColor, aws.ToString(out.CopyObjectResult.ETag))
			}
		}
		if got, _ := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("origin"), Key: aws.String("src")}); got != body {
			t.Errorf("source after copies = %q, want %q", got, body)
		}
	})
}

func TestCopyObjectOntoItself(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "docs")
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("docs"), Key: aws.String("k"), Body: strings.NewReader("v"), ContentType: aws.String("text/plain")}); err != nil {
			t.Fatal(err)
		}
		in := &s3.CopyObjectInput{Bucket: aws.String("docs"), Key: aws.String("k"), CopySource: aws.String("docs/k")}
		if _, err := c.CopyObject(ctx, in); errorCode(err) != "InvalidRequest" {
			t.Errorf("CopyObject onto itself error = %v, want InvalidRequest", err)
		}
		in.MetadataDirective, in.ContentType = types.MetadataDirectiveReplace, aws.String("application/json")
		if _, err := c.CopyObject(ctx, in); err != nil {
			t.Errorf("CopyObject onto itself with REPLACE error = %v", err)
		}
		if got, out := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String("k")}); got != "v" || aws.ToString(out.ContentType) != "application/json" {
			t.Errorf("object after REPLACE = %q, type %q, want v, application/json", got, aws.ToString(out.ContentType))
		}
	})
}

func TestCopyObjectConditions(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "docs")
		put, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("docs"), Key: aws.String("src"), Body: strings.NewReader("v")})
		if err != nil {
			t.Fatal(err)
		}
		etag := aws.ToString(put.ETag)
		past, future := aws.Time(time.Now().Add(-time.Hour)), aws.Time(time.Now().Add(time.Hour))
		tests := []struct {
			name string
			set  func(*s3.CopyObjectInput)
			want string // the error code, or "" for success
		}{
			{"if-match hit", func(in *s3.CopyObjectInput) { in.CopySourceIfMatch = aws.String(etag) }, ""},
			{"if-match miss", func(in *s3.CopyObjectInput) { in.CopySourceIfMatch = aws.String(`"nope"`) }, "PreconditionFailed"},
			{"if-none-match hit", func(in *s3.CopyObjectInput) { in.CopySourceIfNoneMatch = aws.String(etag) }, "PreconditionFailed"},
			{"if-none-match miss", func(in *s3.CopyObjectInput) { in.CopySourceIfNoneMatch = aws.String(`"nope"`) }, ""},
			{"if-modified-since past", func(in *s3.CopyObjectInput) { in.CopySourceIfModifiedSince = past }, ""},
			{"if-modified-since future", func(in *s3.CopyObjectInput) { in.CopySourceIfModifiedSince = future }, "PreconditionFailed"},
			{"if-unmodified-since future", func(in *s3.CopyObjectInput) { in.CopySourceIfUnmodifiedSince = future }, ""},
			{"if-unmodified-since past", func(in *s3.CopyObjectInput) { in.CopySourceIfUnmodifiedSince = past }, "PreconditionFailed"},
		}
		for _, tt := range tests {
			in := &s3.CopyObjectInput{Bucket: aws.String("docs"), Key: aws.String("dst"), CopySource: aws.String("docs/src")}
			tt.set(in)
			_, err := c.CopyObject(ctx, in)
			if got := errorCode(err); got != tt.want {
				t.Errorf("%s: CopyObject error = %v, want code %q", tt.name, err, tt.want)
			}
			_, headErr := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("docs"), Key: aws.String("dst")})
			if (headErr == nil) != (tt.want == "") {
				t.Errorf("%s: HeadObject(dst) error = %v, want the copy to exist only on success", tt.name, headErr)
			}
			if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("docs"), Key: aws.String("dst")}); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestCopyObjectErrors(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "docs")
		tests := []struct{ name, source, want string }{
			{"missing key", "docs/nope", "NoSuchKey"},
			{"missing bucket", "nope/k", "NoSuchBucket"},
			{"no key", "docs", "InvalidArgument"},
		}
		for _, tt := range tests {
			_, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("docs"), Key: aws.String("dst"), CopySource: aws.String(tt.source)})
			if got := errorCode(err); got != tt.want {
				t.Errorf("%s: CopyObject(%q) error = %v, want code %s", tt.name, tt.source, err, tt.want)
			}
		}
	})
}

// listAll returns every key in bucket.
func listAll(t *testing.T, c *s3.Client, bucket string) []string {
	t.Helper()
	var keys []string
	pages := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys
}

func objectIDs(keys []string) []types.ObjectIdentifier {
	ids := make([]types.ObjectIdentifier, len(keys))
	for i, k := range keys {
		ids[i] = types.ObjectIdentifier{Key: aws.String(k)}
	}
	return ids
}

func TestDeleteObjects(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "bulk")
		keys := make([]string, 1001)
		for i := range keys {
			keys[i] = fmt.Sprintf("dir/k%04d", i)
		}
		putKeys(t, c, "bulk", keys)

		over, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("bulk"), Delete: &types.Delete{Objects: objectIDs(keys)}})
		if errorCode(err) != "MalformedXML" {
			t.Errorf("DeleteObjects with %d keys = %v, want MalformedXML", len(keys), over)
		}
		if n := len(listAll(t, c, "bulk")); n != len(keys) {
			t.Fatalf("a rejected DeleteObjects left %d objects, want %d", n, len(keys))
		}

		out, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("bulk"), Delete: &types.Delete{Objects: objectIDs(keys[:1000])}})
		if err != nil {
			t.Fatalf("DeleteObjects with 1000 keys error = %v", err)
		}
		if len(out.Deleted) != 1000 || len(out.Errors) != 0 {
			t.Errorf("DeleteObjects with 1000 keys = %d Deleted, %d Errors, want 1000, 0", len(out.Deleted), len(out.Errors))
		}
		if left := listAll(t, c, "bulk"); len(left) != 1 || left[0] != keys[1000] {
			t.Errorf("objects after DeleteObjects = %v, want [%s]", left, keys[1000])
		}
	})
}

func TestDeleteObjectsEntries(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "docs")
		putKeys(t, c, "docs", []string{"a", "b c", "d"})

		out, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("docs"), Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: aws.String("a")}, {Key: aws.String("b c")}, {Key: aws.String("missing")}, {Key: aws.String("d"), VersionId: aws.String("v1")},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		var deleted []string
		for _, d := range out.Deleted {
			deleted = append(deleted, aws.ToString(d.Key))
		}
		if strings.Join(deleted, ",") != "a,b c,missing" || len(out.Errors) != 1 || aws.ToString(out.Errors[0].Key) != "d" || aws.ToString(out.Errors[0].Code) != "NoSuchVersion" {
			t.Errorf("DeleteObjects = Deleted %v, Errors %+v; want [a b c missing] and a NoSuchVersion error for d", deleted, out.Errors)
		}

		out, err = c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("docs"), Delete: &types.Delete{Quiet: aws.Bool(true), Objects: objectIDs([]string{"d"})}})
		if err != nil || len(out.Deleted) != 0 || len(out.Errors) != 0 {
			t.Errorf("quiet DeleteObjects = Deleted %v, Errors %v, error %v; want none", out.Deleted, out.Errors, err)
		}
		if left := listAll(t, c, "docs"); len(left) != 0 {
			t.Errorf("objects after DeleteObjects = %v, want none", left)
		}

		_, err = c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("nope"), Delete: &types.Delete{Objects: objectIDs([]string{"a"})}})
		if errorCode(err) != "NoSuchBucket" {
			t.Errorf("DeleteObjects in a missing bucket error = %v, want NoSuchBucket", err)
		}
		_, err = c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("docs"), Delete: &types.Delete{Objects: []types.ObjectIdentifier{}}})
		if errorCode(err) != "MalformedXML" {
			t.Errorf("DeleteObjects with no keys error = %v, want MalformedXML", err)
		}
	})
}

// TestDeleteObjectsChecksumSent checks what the SDK sends to satisfy the
// checksum that DeleteObjects requires, over HTTP and over HTTPS.
func TestDeleteObjectsChecksumSent(t *testing.T) {
	servers := []struct {
		name  string
		start func(*testing.T) *pail
	}{{"http", startPail}, {"https", startPailTLS}}
	for _, srv := range servers {
		for _, st := range styles {
			t.Run(srv.name+"/"+st.name, func(t *testing.T) {
				rec := &headerRecorder{}
				c := srv.start(t).clientWith(st, func(next http.RoundTripper) http.RoundTripper {
					rec.next = next
					return rec
				})
				mustBucket(t, c, "docs")
				putKeys(t, c, "docs", []string{"a"})
				rec.headers = nil
				if _, err := c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String("docs"), Delete: &types.Delete{Objects: objectIDs([]string{"a"})}}); err != nil {
					t.Fatalf("DeleteObjects error = %v", err)
				}
				if left := listAll(t, c, "docs"); len(left) != 0 {
					t.Errorf("objects after DeleteObjects = %v, want none", left)
				}
				if len(rec.headers) == 0 {
					t.Fatal("no request recorded")
				}
				// The SDK sends a header checksum, not a trailer or Content-MD5, even over HTTPS.
				sent := rec.headers[0]
				if sent.Get("X-Amz-Checksum-Crc32") == "" || sent.Get("X-Amz-Trailer") != "" || sent.Get("Content-Md5") != "" {
					t.Errorf("SDK sent x-amz-checksum-crc32 %q, x-amz-trailer %q, Content-MD5 %q; want only the checksum header",
						sent.Get("X-Amz-Checksum-Crc32"), sent.Get("X-Amz-Trailer"), sent.Get("Content-Md5"))
				}
			})
		}
	}
}
