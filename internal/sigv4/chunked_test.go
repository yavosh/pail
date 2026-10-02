package sigv4

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// chunkedRequest signs a PUT of body in mode, then aws-chunk encodes it in
// chunks of size. mutate edits the headers before signing; edit, the
// encoded body.
func chunkedRequest(t *testing.T, mode, body string, size int, trailers [][2]string, mutate func(*http.Request), edit func([]byte) []byte) *http.Request {
	t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "http://bkt.localhost/k", http.NoBody)
	r.Header.Set("X-Amz-Content-Sha256", mode)
	r.Header.Set("Content-Encoding", "aws-chunked")
	r.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(body)))
	var names []string
	for _, tr := range trailers {
		names = append(names, tr[0])
	}
	if len(names) > 0 {
		r.Header.Set("X-Amz-Trailer", strings.Join(names, ","))
	}
	if mutate != nil {
		mutate(r)
	}
	creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	if err := signer.SignHTTP(r.Context(), creds, r, mode, "s3", "eu-west-1", time.Now()); err != nil {
		t.Fatal(err)
	}

	amzDate := r.Header.Get("X-Amz-Date")
	scope := amzDate[:8] + "/eu-west-1/s3/aws4_request"
	key := signingKey(testSecret, amzDate[:8], "eu-west-1")
	_, prev, _ := strings.Cut(r.Header.Get("Authorization"), "Signature=")
	signed := mode != streamingUnsignedTrailer
	sign := func(data string) string {
		prev = fmt.Sprintf("%x", hmacSHA256(key, "AWS4-HMAC-SHA256-PAYLOAD\n"+amzDate+"\n"+scope+"\n"+prev+"\n"+emptySHA256+"\n"+sha256Hex(data)))
		return prev
	}

	var b bytes.Buffer
	for rest := body; ; {
		data := rest[:min(size, len(rest))]
		rest = rest[len(data):]
		if signed {
			fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(data), sign(data))
		} else {
			fmt.Fprintf(&b, "%x\r\n", len(data))
		}
		if data == "" {
			break
		}
		b.WriteString(data + "\r\n")
	}
	var canonical strings.Builder
	for _, tr := range trailers {
		b.WriteString(tr[0] + ":" + tr[1] + "\r\n")
		canonical.WriteString(tr[0] + ":" + tr[1] + "\n")
	}
	if mode == streamingSignedTrailer {
		sig := hmacSHA256(key, "AWS4-HMAC-SHA256-TRAILER\n"+amzDate+"\n"+scope+"\n"+prev+"\n"+sha256Hex(canonical.String()))
		fmt.Fprintf(&b, "x-amz-trailer-signature:%x\r\n", sig)
	}
	b.WriteString("\r\n")

	enc := b.Bytes()
	if edit != nil {
		enc = edit(enc)
	}
	r.Body = io.NopCloser(bytes.NewReader(enc))
	r.ContentLength = int64(len(enc))
	return r
}

func TestVerifyChunked(t *testing.T) {
	v := New(testKey, testSecret)
	body := strings.Repeat("0123456789", 2000) // three 8 KiB chunks
	crc := [][2]string{{"x-amz-checksum-crc32", "AAAAAA=="}}
	// replace edits the n-th occurrence of old in the encoded body.
	replace := func(old, repl string, n int) func([]byte) []byte {
		return func(b []byte) []byte {
			i := -1
			for range n {
				i += 1 + bytes.Index(b[i+1:], []byte(old))
			}
			return append(append(bytes.Clone(b[:i]), repl...), b[i+len(old):]...)
		}
	}
	tests := []struct {
		name       string
		mode, body string
		size       int // chunk size; 0 means 8 KiB
		trailers   [][2]string
		mutate     func(*http.Request)
		edit       func([]byte) []byte
		wantVerify error
		wantRead   error
	}{
		{name: "signed", mode: streamingSigned, body: body},
		{name: "signed, empty", mode: streamingSigned},
		{name: "unsigned trailer", mode: streamingUnsignedTrailer, body: body, trailers: crc},
		{name: "unsigned trailer, empty", mode: streamingUnsignedTrailer, trailers: crc},
		{name: "unsigned, no trailer", mode: streamingUnsignedTrailer, body: body},
		{name: "signed trailer", mode: streamingSignedTrailer, body: body, trailers: crc},
		{name: "signed trailer, empty", mode: streamingSignedTrailer, trailers: crc},

		{name: "unsigned, small chunks", mode: streamingUnsignedTrailer, body: body, size: 1 << 10, trailers: crc},
		{name: "signed, small chunks", mode: streamingSigned, body: body, size: 1 << 10, wantRead: ErrChunkTooSmall},
		{name: "signed trailer, small chunks", mode: streamingSignedTrailer, body: body, size: minChunkSize - 1, trailers: crc, wantRead: ErrChunkTooSmall},

		{name: "corrupted chunk", mode: streamingSigned, body: body, edit: replace("01234", "x1234", 1000), wantRead: ErrSignatureMismatch},
		{name: "bad chunk signature", mode: streamingSigned, body: body, edit: func(b []byte) []byte {
			b = bytes.Clone(b)
			i := bytes.LastIndex(b, []byte("chunk-signature=")) + len("chunk-signature=")
			b[i] = "10"[min(b[i]-'0', 1)] // any other hex digit
			return b
		}, wantRead: ErrSignatureMismatch},
		{name: "chunk signature not hex", mode: streamingSigned, body: body, edit: replace("chunk-signature=", "chunk-signature=0", 2), wantRead: ErrMalformedChunk},
		{name: "bad trailer signature", mode: streamingSignedTrailer, body: body, trailers: crc, edit: replace("AAAAAA==", "AAAAAB==", 1), wantRead: ErrSignatureMismatch},
		{name: "truncated", mode: streamingSigned, body: body, edit: func(b []byte) []byte { return b[:len(b)-50] }, wantRead: io.ErrUnexpectedEOF},
		{name: "truncated in data", mode: streamingUnsignedTrailer, body: body, edit: func(b []byte) []byte { return b[:100] }, wantRead: io.ErrUnexpectedEOF},
		{name: "oversized chunk header", mode: streamingUnsignedTrailer, body: body, edit: func([]byte) []byte { return []byte(strings.Repeat("0", maxChunkLine+1) + "\r\n") }, wantRead: ErrMalformedChunk},
		{name: "oversized trailer", mode: streamingUnsignedTrailer, trailers: [][2]string{{"x-amz-checksum-crc32", strings.Repeat("A", maxChunkLine)}}, wantRead: ErrMalformedChunk},
		{name: "chunk size not hex", mode: streamingUnsignedTrailer, body: "abc", edit: replace("3\r\n", "+3\r\n", 1), wantRead: ErrMalformedChunk},
		{name: "extension in unsigned mode", mode: streamingUnsignedTrailer, body: "abc", edit: replace("3\r\n", "3;x=y\r\n", 1), wantRead: ErrMalformedChunk},
		{name: "data without CRLF", mode: streamingUnsignedTrailer, body: "abc", edit: replace("abc\r\n", "abcd\n", 1), wantRead: ErrMalformedChunk},
		{name: "decoded length too long", mode: streamingSigned, body: body, mutate: func(r *http.Request) { r.Header.Set("X-Amz-Decoded-Content-Length", "20001") }, wantRead: ErrMalformedChunk},
		{name: "decoded length too short", mode: streamingSigned, body: body, mutate: func(r *http.Request) { r.Header.Set("X-Amz-Decoded-Content-Length", "19999") }, wantRead: ErrMalformedChunk},
		{name: "undeclared trailer", mode: streamingUnsignedTrailer, body: body, trailers: crc, mutate: func(r *http.Request) { r.Header.Del("X-Amz-Trailer") }, wantRead: ErrMalformedChunk},
		{name: "missing declared trailer", mode: streamingUnsignedTrailer, body: body, mutate: func(r *http.Request) { r.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32") }, wantRead: ErrMalformedChunk},
		{name: "trailer without trailer mode", mode: streamingSigned, body: body, edit: replace("\r\n\r\n", "\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n", 1), wantRead: ErrMalformedChunk},
		{name: "missing trailer signature", mode: streamingSignedTrailer, body: body, trailers: crc, edit: func(b []byte) []byte {
			before, _, _ := bytes.Cut(b, []byte("x-amz-trailer-signature:"))
			return append(bytes.Clone(before), "\r\n"...)
		}, wantRead: ErrMalformedChunk},
		{name: "data after the final chunk", mode: streamingSigned, body: body, edit: func(b []byte) []byte { return append(b, 'x') }, wantRead: ErrMalformedChunk},
		{name: "missing decoded length", mode: streamingSigned, body: body, mutate: func(r *http.Request) { r.Header.Del("X-Amz-Decoded-Content-Length") }, wantVerify: ErrMissingDecodedLength},
		{name: "two decoded lengths", mode: streamingSigned, body: body, mutate: func(r *http.Request) { r.Header.Add("X-Amz-Decoded-Content-Length", "5") }, wantVerify: ErrMissingDecodedLength},
		{name: "invalid decoded length", mode: streamingSigned, body: body, mutate: func(r *http.Request) { r.Header.Set("X-Amz-Decoded-Content-Length", "+20000") }, wantVerify: ErrMissingDecodedLength},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := chunkedRequest(t, tt.mode, tt.body, cmp.Or(tt.size, minChunkSize), tt.trailers, tt.mutate, tt.edit)
			err := v.Verify(r)
			if !errors.Is(err, tt.wantVerify) {
				t.Fatalf("Verify() error = %v, want %v", err, tt.wantVerify)
			}
			if err != nil {
				return
			}
			got, err := io.ReadAll(r.Body)
			if !errors.Is(err, tt.wantRead) {
				t.Fatalf("read body: error = %v, want %v", err, tt.wantRead)
			}
			if err != nil {
				if _, again := r.Body.Read(make([]byte, 1)); !errors.Is(again, tt.wantRead) {
					t.Errorf("read after the error = %v, want the same error", again)
				}
				return
			}
			if string(got) != tt.body {
				t.Errorf("decoded body = %d bytes, want %d", len(got), len(tt.body))
			}
			for _, tr := range tt.trailers {
				if v := r.Trailer.Get(tr[0]); v != tr[1] {
					t.Errorf("r.Trailer[%s] = %q, want %q", tr[0], v, tr[1])
				}
			}
		})
	}
}

// TestVerifyChunkedAWSExample checks the example in the AWS docs, "Signature
// Calculations for the Authorization Header: Transferring Payload in Multiple
// Chunks", which no SDK code produced.
func TestVerifyChunkedAWSExample(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(time.Until(time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)))
		var b bytes.Buffer
		chunks := []struct {
			size int
			sig  string
		}{
			{65536, "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"},
			{1024, "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"},
			{0, "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"},
		}
		for _, c := range chunks {
			fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n%s\r\n", c.size, c.sig, strings.Repeat("a", c.size))
		}
		r := httptest.NewRequestWithContext(context.Background(), http.MethodPut, "http://s3.amazonaws.com/examplebucket/chunkObject.txt", bytes.NewReader(b.Bytes()))
		r.Header.Set("X-Amz-Date", "20130524T000000Z")
		r.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")
		r.Header.Set("X-Amz-Content-Sha256", streamingSigned)
		r.Header.Set("Content-Encoding", "aws-chunked")
		r.Header.Set("X-Amz-Decoded-Content-Length", "66560")
		r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"+
			"SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,"+
			"Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
		if r.ContentLength != 66824 {
			t.Fatalf("encoded length = %d, want 66824 as in the example", r.ContentLength)
		}

		v := New("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
		if err := v.Verify(r); err != nil {
			t.Fatalf("Verify() error = %v", err)
		}
		got, err := io.ReadAll(r.Body)
		if err != nil || len(got) != 66560 {
			t.Errorf("read body = %d bytes, %v, want 66560 bytes", len(got), err)
		}
	})
}
