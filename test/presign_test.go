package test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// sendPresigned sends a presigned request through p's client, with the
// headers the presigner says are signed. It returns the status, headers, and body.
func sendPresigned(t *testing.T, p *pail, ps *v4.PresignedHTTPRequest, body string) (int, http.Header, string) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), ps.Method, ps.URL, r)
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range ps.SignedHeader {
		if !strings.EqualFold(name, "Host") { // the URL carries the host
			req.Header[name] = values
		}
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, string(b)
}

func TestPresignedURLs(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		ps := s3.NewPresignClient(c)
		mustBucket(t, c, "docs")
		const body = "hello presigned"
		bucket, key := aws.String("docs"), aws.String("dir/a b+c.txt")

		// A signed Content-Type shows that the test sends the signed headers.
		put, err := ps.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, ContentType: aws.String("text/plain")})
		if err != nil {
			t.Fatal(err)
		}
		if status, _, msg := sendPresigned(t, p, put, body); status != http.StatusOK {
			t.Fatalf("presigned PutObject = %d %s, want 200", status, msg)
		}

		get, err := ps.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if status, h, got := sendPresigned(t, p, get, ""); status != http.StatusOK || got != body || h.Get("Content-Type") != "text/plain" {
			t.Errorf("presigned GetObject = %d %q, Content-Type %q, want 200 %q, text/plain", status, got, h.Get("Content-Type"), body)
		}

		disposition := `attachment; filename="a.txt"`
		getCD, err := ps.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: key, ResponseContentDisposition: aws.String(disposition)})
		if err != nil {
			t.Fatal(err)
		}
		if status, h, got := sendPresigned(t, p, getCD, ""); status != http.StatusOK || got != body || h.Get("Content-Disposition") != disposition {
			t.Errorf("presigned GetObject with response-content-disposition = %d %q, Content-Disposition %q, want 200 %q, %q", status, got, h.Get("Content-Disposition"), body, disposition)
		}

		head, err := ps.PresignHeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if status, h, _ := sendPresigned(t, p, head, ""); status != http.StatusOK || h.Get("Content-Length") != "15" {
			t.Errorf("presigned HeadObject = %d, Content-Length %q, want 200, 15", status, h.Get("Content-Length"))
		}

		del, err := ps.PresignDeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if status, _, msg := sendPresigned(t, p, del, ""); status != http.StatusNoContent {
			t.Errorf("presigned DeleteObject = %d %s, want 204", status, msg)
		}
		if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: key}); err == nil {
			t.Errorf("HeadObject after a presigned delete succeeded, want NotFound")
		}
	})
}

func TestPresignedURLIsBoundToItsRequest(t *testing.T) {
	forEachStyle(t, func(t *testing.T, p *pail, _ style, c *s3.Client) {
		ctx := context.Background()
		mustBucket(t, c, "docs")
		get, err := s3.NewPresignClient(c).PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String("k")})
		if err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name, from, to string
		}{
			{"key", "/k?", "/other?"},
			{"query parameter added", "?X-Amz-Algorithm", "?response-content-type=text%2Fhtml&X-Amz-Algorithm"},
			{"signature", "X-Amz-Signature=", "X-Amz-Signature=0"},
		}
		for _, tt := range tests {
			if !strings.Contains(get.URL, tt.from) {
				t.Fatalf("presigned URL %s has no %q to change", get.URL, tt.from)
			}
			modified := *get
			modified.URL = strings.Replace(get.URL, tt.from, tt.to, 1)
			if status, _, msg := sendPresigned(t, p, &modified, ""); status != http.StatusForbidden || !strings.Contains(msg, "<Code>SignatureDoesNotMatch</Code>") {
				t.Errorf("presigned GetObject with a changed %s = %d %s, want 403 SignatureDoesNotMatch", tt.name, status, msg)
			}
		}
	})
}
