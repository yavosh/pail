package test

import (
	"bytes"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestCORSConfiguration(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, st style, c *s3.Client) {
		ctx := t.Context()
		bucket := aws.String("cors-uploads")
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: bucket}); errorCode(err) != "NoSuchCORSConfiguration" {
			t.Fatalf("missing CORS error = %v", err)
		}
		rules := []types.CORSRule{{AllowedOrigins: []string{"https://*.example.com"}, AllowedMethods: []string{"GET", "PUT", "POST"}, AllowedHeaders: []string{"x-amz-*", "content-type"}, ExposeHeaders: []string{"ETag"}, MaxAgeSeconds: aws.Int32(300)}}
		if _, err := c.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: bucket, CORSConfiguration: &types.CORSConfiguration{CORSRules: rules}}); err != nil {
			t.Fatal(err)
		}
		got, err := c.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: bucket})
		if err != nil || !reflect.DeepEqual(got.CORSRules, rules) {
			t.Fatalf("CORS rules = %+v, %v, want %+v", got, err, rules)
		}
		for _, tc := range []struct {
			name, origin, method, headers string
			status                        int
		}{
			{"allowed", "https://app.example.com", "POST", "Content-Type, X-Amz-Meta-User", 200},
			{"wrong-origin", "https://example.org", "POST", "content-type", 403},
			{"wrong-method", "https://app.example.com", "DELETE", "", 403},
			{"wrong-header", "https://app.example.com", "POST", "authorization", 403},
			{"missing-origin", "", "POST", "", 400},
			{"missing-method", "https://app.example.com", "", "", 400},
		} {
			t.Run(tc.name, func(t *testing.T) {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodOptions, p.bucketURL(st, *bucket)+"/new", nil)
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Origin", tc.origin)
				req.Header.Set("Access-Control-Request-Method", tc.method)
				req.Header.Set("Access-Control-Request-Headers", tc.headers)
				resp, err := p.httpClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != tc.status {
					body, _ := io.ReadAll(resp.Body)
					t.Fatalf("preflight status = %d, want %d: %s", resp.StatusCode, tc.status, body)
				}
				if tc.status == 200 && (resp.Header.Get("Access-Control-Allow-Origin") != tc.origin || resp.Header.Get("Access-Control-Max-Age") != "300" || resp.Header.Get("Access-Control-Allow-Credentials") != "true") {
					t.Errorf("preflight headers = %v", resp.Header)
				}
			})
		}
		presigned, err := s3.NewPresignClient(c).PresignPutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("browser")})
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, presigned.URL, bytes.NewReader([]byte("browser upload")))
		if err != nil {
			t.Fatal(err)
		}
		req.Header = presigned.SignedHeader.Clone()
		req.Header.Set("Origin", "https://app.example.com")
		resp, err := p.httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 || resp.Header.Get("Access-Control-Allow-Origin") != "https://app.example.com" {
			t.Fatalf("browser PUT = %d, headers %v", resp.StatusCode, resp.Header)
		}
		if _, err := c.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: bucket}); errorCode(err) != "NoSuchCORSConfiguration" {
			t.Fatalf("deleted CORS error = %v", err)
		}
	})
}

func TestLifecycleConfiguration(t *testing.T) {
	forEachStyle(t, func(t *testing.T, _ *pail, _ style, c *s3.Client) {
		ctx := t.Context()
		bucket := aws.String("lifecycle-rules")
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: bucket}); errorCode(err) != "NoSuchLifecycleConfiguration" {
			t.Fatalf("missing lifecycle error = %v", err)
		}
		rules := []types.LifecycleRule{
			{ID: aws.String("expire"), Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{Prefix: aws.String("tmp/"), ObjectSizeGreaterThan: aws.Int64(0), ObjectSizeLessThan: aws.Int64(1000)}}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(3)}},
			{ID: aws.String("abort"), Status: types.ExpirationStatusDisabled, Filter: &types.LifecycleRuleFilter{}, AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(2)}},
		}
		if _, err := c.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: bucket, LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: rules}}); err != nil {
			t.Fatal(err)
		}
		got, err := c.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: bucket})
		if err != nil || !reflect.DeepEqual(got.Rules, rules) {
			t.Fatalf("lifecycle rules = %+v, %v, want %+v", got, err, rules)
		}
		if got.TransitionDefaultMinimumObjectSize != types.TransitionDefaultMinimumObjectSizeAllStorageClasses128k {
			t.Errorf("transition minimum = %q", got.TransitionDefaultMinimumObjectSize)
		}
		invalid := []types.LifecycleRule{{Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(0)}}}
		if _, err := c.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: bucket, LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: invalid}}); errorCode(err) != "InvalidRequest" {
			t.Fatalf("invalid days error = %v", err)
		}
		unsupported := []types.LifecycleRule{{Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{}, Transitions: []types.Transition{{Days: aws.Int32(30), StorageClass: types.TransitionStorageClassGlacier}}}}
		if _, err := c.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: bucket, LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: unsupported}}); errorCode(err) != "NotImplemented" {
			t.Fatalf("transition error = %v", err)
		}
		if _, err := c.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{Bucket: bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: bucket}); errorCode(err) != "NoSuchLifecycleConfiguration" {
			t.Fatalf("deleted lifecycle error = %v", err)
		}
	})
}
