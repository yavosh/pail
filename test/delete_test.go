package test

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestDeleteObjectConditions(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		const bucket = "conditional-delete"
		mustBucket(t, c, bucket)
		for _, tc := range []struct {
			name    string
			exists  bool
			ifMatch *string
			want    string
		}{
			{"unconditional existing", true, nil, ""},
			{"matching quoted", true, new(`"{etag}"`), ""},
			{"matching unquoted", true, new("{etag}"), ""},
			{"wildcard existing", true, new("*"), ""},
			{"mismatch", true, new(`"wrong"`), "PreconditionFailed"},
			{"weak ETag", true, new(`W/"{etag}"`), "PreconditionFailed"},
			{"empty condition", true, new(""), "PreconditionFailed"},
			{"unconditional missing", false, nil, ""},
			{"ETag missing", false, new(`"wrong"`), "NoSuchKey"},
			{"wildcard missing", false, new("*"), "NoSuchKey"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				key := tc.name
				var before *s3.PutObjectOutput
				if tc.exists {
					var err error
					before, err = c.PutObject(t.Context(), &s3.PutObjectInput{Bucket: new(bucket), Key: new(key), Body: strings.NewReader("original")})
					if err != nil {
						t.Fatal(err)
					}
				}
				condition := tc.ifMatch
				if condition != nil && before != nil {
					condition = new(strings.ReplaceAll(*condition, "{etag}", strings.Trim(aws.ToString(before.ETag), `"`)))
				}
				_, err := c.DeleteObject(t.Context(), &s3.DeleteObjectInput{Bucket: new(bucket), Key: new(key), IfMatch: condition})
				if got := errorCode(err); got != tc.want {
					t.Fatalf("DeleteObject(IfMatch %v, exists %v) error = %v, want code %q", condition, tc.exists, err, tc.want)
				}
				if tc.exists && tc.want != "" {
					body, after := getBody(t, c, &s3.GetObjectInput{Bucket: new(bucket), Key: new(key)})
					if body != "original" || aws.ToString(after.ETag) != aws.ToString(before.ETag) {
						t.Errorf("failed delete changed object: body %q, ETag %v, want original, %v", body, after.ETag, before.ETag)
					}
					return
				}
				out, err := c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: new(bucket), Key: new(key)})
				if out != nil && out.Body != nil {
					out.Body.Close()
				}
				if got := errorCode(err); got != "NoSuchKey" {
					t.Errorf("GET after delete error = %v, want NoSuchKey", err)
				}
			})
		}
		_, err := c.DeleteObject(t.Context(), &s3.DeleteObjectInput{Bucket: new("missing-bucket"), Key: new("key"), IfMatch: new("*")})
		if got := errorCode(err); got != "NoSuchBucket" {
			t.Errorf("conditional delete in missing bucket = %v, want NoSuchBucket", err)
		}
	})
}

func TestDeleteObjectsConditions(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		const bucket = "conditional-batch-delete"
		mustBucket(t, c, bucket)
		for _, tc := range []struct {
			name  string
			quiet bool
		}{{"verbose", false}, {"quiet", true}} {
			t.Run(tc.name, func(t *testing.T) {
				keys := []string{"quoted", "unquoted", "wildcard", "unconditional", "mismatch", "weak", "empty", "stale"}
				etags := map[string]string{}
				for _, key := range keys {
					put, err := c.PutObject(t.Context(), &s3.PutObjectInput{Bucket: new(bucket), Key: new(key), Body: strings.NewReader("original")})
					if err != nil {
						t.Fatal(err)
					}
					etags[key] = aws.ToString(put.ETag)
				}
				if _, err := c.PutObject(t.Context(), &s3.PutObjectInput{Bucket: new(bucket), Key: new("stale"), Body: strings.NewReader("replacement")}); err != nil {
					t.Fatal(err)
				}
				objects := []types.ObjectIdentifier{
					{Key: new("quoted"), ETag: new(etags["quoted"])},
					{Key: new("unquoted"), ETag: new(strings.Trim(etags["unquoted"], `"`))},
					{Key: new("wildcard"), ETag: new("*")},
					{Key: new("unconditional")},
					{Key: new("mismatch"), ETag: new(`"wrong"`)},
					{Key: new("weak"), ETag: new("W/" + etags["weak"])},
					{Key: new("empty"), ETag: new("")},
					{Key: new("stale"), ETag: new(etags["stale"])},
					{Key: new("missing-unconditional")},
					{Key: new("missing-etag"), ETag: new(`"wrong"`)},
					{Key: new("missing-wildcard"), ETag: new("*")},
				}
				out, err := c.DeleteObjects(t.Context(), &s3.DeleteObjectsInput{Bucket: new(bucket), Delete: &types.Delete{Objects: objects, Quiet: new(tc.quiet)}})
				if err != nil {
					t.Fatal(err)
				}
				wantDeleted := []string{"quoted", "unquoted", "wildcard", "unconditional", "missing-unconditional"}
				if tc.quiet {
					wantDeleted = nil
				}
				var deleted []string
				for _, d := range out.Deleted {
					deleted = append(deleted, aws.ToString(d.Key))
				}
				if !slices.Equal(deleted, wantDeleted) {
					t.Errorf("Deleted = %v, want %v", deleted, wantDeleted)
				}
				wantErrors := map[string]string{
					"mismatch": "PreconditionFailed", "weak": "PreconditionFailed", "empty": "PreconditionFailed", "stale": "PreconditionFailed",
					"missing-etag": "NoSuchKey", "missing-wildcard": "NoSuchKey",
				}
				gotErrors := map[string]string{}
				for _, e := range out.Errors {
					gotErrors[aws.ToString(e.Key)] = aws.ToString(e.Code)
					if aws.ToString(e.Message) == "" {
						t.Errorf("Error entry for %v has no message", e.Key)
					}
				}
				if !maps.Equal(gotErrors, wantErrors) || len(out.Errors) != len(wantErrors) {
					t.Errorf("Errors = %+v, want %v", out.Errors, wantErrors)
				}
				wantKept := []string{"empty", "mismatch", "stale", "weak"}
				if kept := listAll(t, c, bucket); !slices.Equal(kept, wantKept) {
					t.Fatalf("objects after batch delete = %v, want %v", kept, wantKept)
				}
				for _, key := range wantKept {
					wantBody := "original"
					if key == "stale" {
						wantBody = "replacement"
					}
					body, _ := getBody(t, c, &s3.GetObjectInput{Bucket: new(bucket), Key: new(key)})
					if body != wantBody {
						t.Errorf("retained %q body = %q, want %q", key, body, wantBody)
					}
				}
			})
		}
	})
}
