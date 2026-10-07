package test

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// presignedV2 signs a client request using an explicit canonical resource.
// The Go SDK supports only SigV4; it still creates buckets and verifies objects.
func presignedV2(t *testing.T, method, endpoint, resource string, headers http.Header, expires time.Time) *v4.PresignedHTTPRequest {
	t.Helper()
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	expiration := strconv.FormatInt(expires.Unix(), 10)
	stringToSign := method + "\n" + headers.Get("Content-MD5") + "\n" + headers.Get("Content-Type") + "\n" + expiration + "\n"
	for _, name := range slices.Sorted(maps.Keys(headers)) {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-") {
			stringToSign += strings.ToLower(name) + ":" + strings.Join(headers[name], ",") + "\n"
		}
	}
	stringToSign += resource
	mac := hmac.New(sha1.New, []byte(testSecret))
	_, _ = mac.Write([]byte(stringToSign))
	q := u.Query()
	q.Set("AWSAccessKeyId", testKey)
	q.Set("Expires", expiration)
	q.Set("Signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	u.RawQuery = q.Encode()
	return &v4.PresignedHTTPRequest{URL: u.String(), Method: method, SignedHeader: headers.Clone()}
}

func TestSigV2PresignedURLs(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, st style, c *s3.Client) {
		ctx := t.Context()
		const bucket = "sigv2-urls"
		mustBucket(t, c, bucket)
		owner, err := c.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatal(err)
		}
		const body = "presigned v2 body"
		sum := md5.Sum([]byte(body))
		headers := http.Header{"Content-Type": {"text/plain"}, "Content-Md5": {base64.StdEncoding.EncodeToString(sum[:])}, "X-Amz-Meta-Color": {"blue"}}
		for _, tc := range []struct{ key, escaped string }{
			{"dir/a b+c.txt", "dir/a%20b%2Bc.txt"},
			{"a//b", "a//b"},
			{"100%", "100%25"},
			{"a%2Fb", "a%252Fb"},
			{"✓", "%e2%9c%93"},
		} {
			t.Run(tc.key, func(t *testing.T) {
				endpoint := p.bucketURL(st, bucket) + "/" + tc.escaped
				resource := "/" + bucket + "/" + tc.escaped
				put := presignedV2(t, http.MethodPut, endpoint, resource, headers, time.Now().Add(time.Hour))
				if status, _, data := sendPresigned(t, p, put, body); status != http.StatusOK {
					t.Fatalf("SigV2 PUT %q = %d, %s, want 200", tc.key, status, data)
				}
				obj, err := c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(tc.key)})
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(obj.Body)
				obj.Body.Close()
				if err != nil || string(data) != body || aws.ToString(obj.ContentType) != "text/plain" || obj.Metadata["color"] != "blue" {
					t.Fatalf("SDK GET after SigV2 PUT = %q, metadata %v, type %v, error %v", data, obj.Metadata, obj.ContentType, err)
				}
				objectACL, err := c.GetObjectAcl(t.Context(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String(tc.key)})
				if err != nil || aws.ToString(objectACL.Owner.ID) != aws.ToString(owner.Owner.ID) {
					t.Fatalf("SigV2 owner = %+v, error %v, want %+v", objectACL, err, owner.Owner)
				}
				get := presignedV2(t, http.MethodGet, endpoint, resource, nil, time.Now().Add(time.Hour))
				if status, _, data := sendPresigned(t, p, get, ""); status != http.StatusOK || data != body {
					t.Fatalf("SigV2 GET %q = %d, %q, want 200, %q", tc.key, status, data, body)
				}
				head := presignedV2(t, http.MethodHead, endpoint, resource, nil, time.Now().Add(time.Hour))
				if status, h, _ := sendPresigned(t, p, head, ""); status != http.StatusOK || h.Get("Content-Length") != strconv.Itoa(len(body)) {
					t.Fatalf("SigV2 HEAD = %d, headers %v", status, h)
				}
				get = presignedV2(t, http.MethodGet, endpoint+"?response-content-type=text%2Fhtml", resource+"?response-content-type=text/html", nil, time.Now().Add(time.Hour))
				if status, h, data := sendPresigned(t, p, get, ""); status != http.StatusOK || data != body || h.Get("Content-Type") != "text/html" {
					t.Fatalf("SigV2 response override = %d, %q, headers %v", status, data, h)
				}
				get = presignedV2(t, http.MethodGet, endpoint+"?acl", resource+"?acl", nil, time.Now().Add(time.Hour))
				if status, _, data := sendPresigned(t, p, get, ""); status != http.StatusOK || !strings.Contains(data, "<AccessControlPolicy") {
					t.Fatalf("SigV2 ACL = %d, %s", status, data)
				}
			})
		}
	})
}

func TestSigV2PresignedURLsRejectTampering(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, st style, c *s3.Client) {
		const bucket = "sigv2-tampering"
		mustBucket(t, c, bucket)
		if _, err := c.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader("original"), ACL: types.ObjectCannedACLPublicRead}); err != nil {
			t.Fatal(err)
		}
		endpoint := p.bucketURL(st, bucket) + "/k"
		for _, tc := range []struct {
			name   string
			mutate func(*v4.PresignedHTTPRequest)
			status int
			code   string
		}{
			{"method", func(r *v4.PresignedHTTPRequest) { r.Method = http.MethodDelete }, 403, "SignatureDoesNotMatch"},
			{"path", func(r *v4.PresignedHTTPRequest) { r.URL = strings.Replace(r.URL, "/k?", "/other?", 1) }, 403, "SignatureDoesNotMatch"},
			{"override", func(r *v4.PresignedHTTPRequest) { r.URL += "&response-content-type=text%2Fhtml" }, 403, "SignatureDoesNotMatch"},
			{"header", func(r *v4.PresignedHTTPRequest) { r.SignedHeader = http.Header{"X-Amz-Meta-Color": {"red"}} }, 403, "SignatureDoesNotMatch"},
			{"signature", func(r *v4.PresignedHTTPRequest) { r.URL = strings.Replace(r.URL, "Signature=", "Signature=0", 1) }, 403, "SignatureDoesNotMatch"},
			{"expired", func(r *v4.PresignedHTTPRequest) {
				*r = *presignedV2(t, http.MethodGet, endpoint, "/"+bucket+"/k", nil, time.Now().Add(-time.Hour))
			}, 403, "AccessDenied"},
			{"missing key", func(r *v4.PresignedHTTPRequest) { r.URL = strings.Replace(r.URL, "AWSAccessKeyId=", "missing=", 1) }, 400, "InvalidArgument"},
			{"missing expiry", func(r *v4.PresignedHTTPRequest) { r.URL = strings.Replace(r.URL, "Expires=", "missing=", 1) }, 403, "AccessDenied"},
			{"duplicate expiry", func(r *v4.PresignedHTTPRequest) { r.URL += "&Expires=1" }, 400, "InvalidArgument"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				request := presignedV2(t, http.MethodGet, endpoint, "/"+bucket+"/k", nil, time.Now().Add(time.Hour))
				tc.mutate(request)
				if status, _, data := sendPresigned(t, p, request, ""); status != tc.status || !strings.Contains(data, "<Code>"+tc.code+"</Code>") {
					t.Fatalf("changed %s = %d, %s, want %d %s", tc.name, status, data, tc.status, tc.code)
				}
			})
		}
		streamHeaders := http.Header{"X-Amz-Content-Sha256": {"STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}, "X-Amz-Decoded-Content-Length": {"3"}}
		stream := presignedV2(t, http.MethodPut, endpoint, "/"+bucket+"/k", streamHeaders, time.Now().Add(time.Hour))
		if status, _, data := sendPresigned(t, p, stream, "raw"); status != 501 || !strings.Contains(data, "<Code>NotImplemented</Code>") {
			t.Fatalf("SigV2 streaming body = %d, %s, want 501 NotImplemented", status, data)
		}
		badMD5 := base64.StdEncoding.EncodeToString(make([]byte, 16))
		put := presignedV2(t, http.MethodPut, endpoint, "/"+bucket+"/k", http.Header{"Content-Md5": {badMD5}}, time.Now().Add(time.Hour))
		if status, _, data := sendPresigned(t, p, put, "replacement"); status != 400 || !strings.Contains(data, "<Code>BadDigest</Code>") {
			t.Fatalf("SigV2 bad body = %d, %s, want 400 BadDigest", status, data)
		}
		obj, err := c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")})
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Body.Close()
		data, err := io.ReadAll(obj.Body)
		if err != nil || string(data) != "original" {
			t.Fatalf("failed requests changed object = %q, %v", data, err)
		}
	})
}
