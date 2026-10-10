package test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"io"
	"maps"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func sendForm(t *testing.T, p *pail, endpoint string, fields map[string]string, body string, afterFile bool) (int, http.Header, []byte) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for field, value := range fields {
		if err := mw.WriteField(field, value); err != nil {
			t.Fatal(err)
		}
	}
	file, err := mw.CreateFormFile("file", "image.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(file, body); err != nil {
		t.Fatal(err)
	}
	if afterFile {
		if err := mw.WriteField("key", "escaped"); err != nil {
			t.Fatal(err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Origin", "https://app.example.com")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, data
}

func TestPresignedPost(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, st style, c *s3.Client) {
		ctx := t.Context()
		bucket := aws.String("form-uploads")
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: bucket, CORSConfiguration: &types.CORSConfiguration{CORSRules: []types.CORSRule{{AllowedOrigins: []string{"https://app.example.com"}, AllowedMethods: []string{"POST"}}}}}); err != nil {
			t.Fatal(err)
		}
		for _, status := range []string{"200", "201", "204"} {
			t.Run(status, func(t *testing.T) {
				post, err := s3.NewPresignClient(c).PresignPostObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("incoming/${filename}")}, func(o *s3.PresignPostOptions) {
					o.Conditions = []any{[]any{"starts-with", "$key", "incoming/"}, map[string]string{"success_action_status": status}, map[string]string{"Content-Type": "text/plain"}, map[string]string{"x-amz-meta-user": "alice"}, map[string]string{"acl": "public-read"}, []any{"content-length-range", int64(1), int64(40)}}
				})
				if err != nil {
					t.Fatal(err)
				}
				fields := maps.Clone(post.Values)
				fields["success_action_status"] = status
				fields["Content-Type"] = "text/plain"
				fields["x-amz-meta-user"] = "alice"
				fields["acl"] = "public-read"
				got, headers, body := sendForm(t, p, post.URL, fields, "form bytes", false)
				want := map[string]int{"200": 200, "201": 201, "204": 204}[status]
				if got != want {
					t.Fatalf("POST status = %d, want %d: %s", got, want, body)
				}
				if headers.Get("Access-Control-Allow-Origin") != "https://app.example.com" {
					t.Errorf("POST CORS = %v", headers)
				}
				if status == "201" {
					var result struct{ Bucket, Key, ETag, Location string }
					if err := xml.Unmarshal(body, &result); err != nil || result.Bucket != *bucket || result.Key != "incoming/image.txt" || result.ETag == "" || result.Location == "" {
						t.Fatalf("POST response = %+v, %v", result, err)
					}
				}
				obj, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("incoming/image.txt")})
				if err != nil {
					t.Fatal(err)
				}
				bytes, err := io.ReadAll(obj.Body)
				obj.Body.Close()
				if err != nil || string(bytes) != "form bytes" || obj.Metadata["user"] != "alice" || aws.ToString(obj.ContentType) != "text/plain" {
					t.Fatalf("POST object = %q, %+v, %v", bytes, obj, err)
				}
				for _, tc := range []struct {
					name   string
					mutate func(map[string]string)
					body   string
					after  bool
					want   int
				}{
					{"wrong-key", func(f map[string]string) { f["key"] = "outside" }, "form bytes", false, 403},
					{"wrong-metadata", func(f map[string]string) { f["x-amz-meta-user"] = "bob" }, "form bytes", false, 403},
					{"extra-field", func(f map[string]string) { f["Cache-Control"] = "public" }, "form bytes", false, 403},
					{"bad-signature", func(f map[string]string) { f["X-Amz-Signature"] = strings.Repeat("0", 64) }, "form bytes", false, 403},
					{"too-large", func(map[string]string) {}, strings.Repeat("x", 41), false, 400},
					{"too-small", func(map[string]string) {}, "", false, 400},
					{"file-not-last", func(map[string]string) {}, "replacement", true, 403},
				} {
					t.Run(tc.name, func(t *testing.T) {
						bad := maps.Clone(fields)
						tc.mutate(bad)
						got, _, body := sendForm(t, p, post.URL, bad, tc.body, tc.after)
						if got != tc.want {
							t.Fatalf("rejected POST status = %d, want %d: %s", got, tc.want, body)
						}
					})
				}
				obj, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("incoming/image.txt")})
				if err != nil {
					t.Fatal(err)
				}
				bytes, err = io.ReadAll(obj.Body)
				obj.Body.Close()
				if err != nil || string(bytes) != "form bytes" {
					t.Fatalf("failed POST replaced object: %q, %v", bytes, err)
				}
				// The signature cannot authorize another bucket.
				if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("other-forms")}); err != nil {
					t.Fatal(err)
				}
				got, _, body = sendForm(t, p, p.bucketURL(st, "other-forms"), fields, "form bytes", false)
				if got != 403 {
					t.Fatalf("wrong bucket POST = %d: %s", got, body)
				}
			})
		}
		expired, err := s3.NewPresignClient(c).PresignPostObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("expired")}, func(o *s3.PresignPostOptions) { o.Expires = -time.Hour })
		if err != nil {
			t.Fatal(err)
		}
		got, _, body := sendForm(t, p, expired.URL, expired.Values, "expired", false)
		if got != 403 {
			t.Fatalf("expired POST = %d: %s", got, body)
		}
	})
}

func TestPostIntegrity(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, _ style, c *s3.Client) {
		bucket := aws.String("post-integrity")
		if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		const body = "checksum body"
		md5Sum := md5.Sum([]byte(body))
		shaSum := sha256.Sum256([]byte(body))
		for _, tc := range []struct {
			name   string
			fields map[string]string
			want   int
		}{
			{"md5", map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(md5Sum[:])}, 204},
			{"sha256", map[string]string{"x-amz-checksum-algorithm": "SHA256", "x-amz-checksum-sha256": base64.StdEncoding.EncodeToString(shaSum[:])}, 204},
			{"bad-md5", map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(make([]byte, 16))}, 400},
			{"bad-sha256", map[string]string{"x-amz-checksum-algorithm": "SHA256", "x-amz-checksum-sha256": base64.StdEncoding.EncodeToString(make([]byte, 32))}, 400},
			{"missing-checksum", map[string]string{"x-amz-checksum-algorithm": "SHA256"}, 400},
			{"malformed-md5", map[string]string{"Content-MD5": "broken"}, 400},
			{"tagging", map[string]string{"tagging": "<Tagging><TagSet/></Tagging>"}, 501},
		} {
			t.Run(tc.name, func(t *testing.T) {
				conditions := []any{}
				for name, value := range tc.fields {
					conditions = append(conditions, map[string]string{name: value})
				}
				post, err := s3.NewPresignClient(c).PresignPostObject(t.Context(), &s3.PutObjectInput{Bucket: bucket, Key: aws.String(tc.name)}, func(o *s3.PresignPostOptions) { o.Conditions = conditions })
				if err != nil {
					t.Fatal(err)
				}
				fields := maps.Clone(post.Values)
				maps.Copy(fields, tc.fields)
				fields["bucket"] = *bucket
				status, _, data := sendForm(t, p, post.URL, fields, body, false)
				if status != tc.want {
					t.Fatalf("integrity POST = %d, want %d: %s", status, tc.want, data)
				}
				obj, err := c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: bucket, Key: aws.String(tc.name), ChecksumMode: types.ChecksumModeEnabled})
				if tc.want != 204 {
					if errorCode(err) != "NoSuchKey" {
						if err == nil {
							obj.Body.Close()
						}
						t.Fatalf("failed POST object = %v, want NoSuchKey", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				defer obj.Body.Close()
				data, err = io.ReadAll(obj.Body)
				if err != nil || string(data) != body {
					t.Fatalf("checksummed object = %q, %v", data, err)
				}
			})
		}
	})
}

func TestPostRedirect(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, _ style, c *s3.Client) {
		bucket := aws.String("post-redirect")
		if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		const destination = "https://app.example.com/done?existing=1"
		post, err := s3.NewPresignClient(c).PresignPostObject(t.Context(), &s3.PutObjectInput{Bucket: bucket, Key: aws.String("redirect")}, func(o *s3.PresignPostOptions) {
			o.Conditions = []any{map[string]string{"success_action_redirect": destination}}
		})
		if err != nil {
			t.Fatal(err)
		}
		fields := maps.Clone(post.Values)
		fields["success_action_redirect"] = destination
		httpClient := *p.httpClient
		httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		redirected := *p
		redirected.httpClient = &httpClient
		status, headers, data := sendForm(t, &redirected, post.URL, fields, "redirect", false)
		location, err := url.Parse(headers.Get("Location"))
		if status != 303 || err != nil || location.Host != "app.example.com" || location.Query().Get("bucket") != *bucket || location.Query().Get("key") != "redirect" || location.Query().Get("etag") == "" || location.Query().Get("existing") != "1" {
			t.Fatalf("POST redirect = %d, %v, %s, %v", status, headers, data, err)
		}
	})
}

func TestPostObjectOptions(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, _ style, c *s3.Client) {
		bucket := aws.String("post-options")
		if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name, field, value string
			wantStatus         int
		}{
			{"storage class", "x-amz-storage-class", "STANDARD_IA", 204},
			{"unknown storage class", "x-amz-storage-class", "BOGUS", 400},
			{"encryption", "x-amz-server-side-encryption", "aws:kms", 204},
			{"unknown encryption", "x-amz-server-side-encryption", "bogus", 400},
			{"SSE-C", "x-amz-server-side-encryption-customer-algorithm", "AES256", 403},
			{"website redirect", "x-amz-website-redirect-location", "/target", 204},
			{"relative website redirect", "x-amz-website-redirect-location", "target", 400},
			{"Object Lock", "x-amz-object-lock-mode", "GOVERNANCE", 400},
			{"unknown checksum", "x-amz-checksum-bogus", "AAAAAA==", 400},
		}
		for _, tt := range tests {
			key := strings.ReplaceAll(tt.name, " ", "-")
			post, err := s3.NewPresignClient(c).PresignPostObject(t.Context(), &s3.PutObjectInput{Bucket: bucket, Key: aws.String(key)}, func(o *s3.PresignPostOptions) {
				o.Conditions = []any{map[string]string{tt.field: tt.value}}
			})
			if err != nil {
				t.Fatal(err)
			}
			fields := maps.Clone(post.Values)
			fields[tt.field] = tt.value
			if got, _, body := sendForm(t, p, post.URL, fields, "form bytes", false); got != tt.wantStatus {
				t.Errorf("%s: POST status = %d, want %d: %s", tt.name, got, tt.wantStatus, body)
				continue
			}
			if tt.wantStatus != 204 {
				if _, err := c.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: bucket, Key: aws.String(key)}); errorCode(err) != "NotFound" {
					t.Errorf("%s: HeadObject after a rejected POST error = %v, want NotFound", tt.name, err)
				}
				continue
			}
			head, err := c.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: bucket, Key: aws.String(key)})
			if err != nil {
				t.Errorf("%s: HeadObject error = %v", tt.name, err)
				continue
			}
			switch tt.field {
			case "x-amz-storage-class":
				if string(head.StorageClass) != tt.value {
					t.Errorf("%s: StorageClass = %q, want %q", tt.name, head.StorageClass, tt.value)
				}
			case "x-amz-server-side-encryption":
				if string(head.ServerSideEncryption) != tt.value {
					t.Errorf("%s: ServerSideEncryption = %q, want %q", tt.name, head.ServerSideEncryption, tt.value)
				}
			case "x-amz-website-redirect-location":
				if aws.ToString(head.WebsiteRedirectLocation) != tt.value {
					t.Errorf("%s: WebsiteRedirectLocation = %q, want %q", tt.name, aws.ToString(head.WebsiteRedirectLocation), tt.value)
				}
			}
		}
	})
}
