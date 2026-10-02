package test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Over HTTPS the SDK sends uploads aws-chunked, with the CRC32 in a trailer.
func TestStreamingUploadOverHTTPS(t *testing.T) {
	body := strings.Repeat("hello world ", 10000)
	for _, st := range styles {
		t.Run(st.name, func(t *testing.T) {
			ctx := context.Background()
			p := startPailTLS(t)
			rec := &headerRecorder{}
			c := p.clientWith(st, func(next http.RoundTripper) http.RoundTripper {
				rec.next = next
				return rec
			})
			mustBucket(t, c, "streams")

			// An unseekable body works only with aws-chunked.
			unseekable := struct{ io.Reader }{strings.NewReader(body)}
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("streams"), Key: aws.String("k"), Body: unseekable, ContentLength: aws.Int64(int64(len(body)))}); err != nil {
				t.Fatal(err)
			}
			sent := rec.headers[len(rec.headers)-1]
			if sent.Get("X-Amz-Content-Sha256") != "STREAMING-UNSIGNED-PAYLOAD-TRAILER" || !strings.Contains(strings.Join(sent.Values("Content-Encoding"), ","), "aws-chunked") {
				t.Fatalf("PutObject sent x-amz-content-sha256 %q, Content-Encoding %q; want an aws-chunked upload",
					sent.Get("X-Amz-Content-Sha256"), sent.Values("Content-Encoding"))
			}
			got, out := getBody(t, c, &s3.GetObjectInput{Bucket: aws.String("streams"), Key: aws.String("k"), ChecksumMode: types.ChecksumModeEnabled})
			if got != body || aws.ToString(out.ChecksumCRC32) == "" || aws.ToString(out.ContentEncoding) != "" {
				t.Errorf("GetObject = %d bytes, CRC32 %q, Content-Encoding %q; want the %d-byte body, the SDK's CRC32, and no encoding",
					len(got), aws.ToString(out.ChecksumCRC32), aws.ToString(out.ContentEncoding), len(body))
			}

			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("streams"), Key: aws.String("gz"), Body: strings.NewReader(body), ContentEncoding: aws.String("gzip")}); err != nil {
				t.Fatal(err)
			}
			head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("streams"), Key: aws.String("gz")})
			if err != nil || aws.ToString(head.ContentEncoding) != "gzip" || aws.ToInt64(head.ContentLength) != int64(len(body)) {
				t.Errorf("HeadObject = Content-Encoding %q, length %d, %v; want gzip and %d", aws.ToString(head.ContentEncoding), aws.ToInt64(head.ContentLength), err, len(body))
			}
		})
	}
}
