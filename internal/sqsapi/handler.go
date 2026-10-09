// Package sqsapi serves the Amazon SQS API over the AWS JSON 1.0 protocol.
package sqsapi

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/sigv4"
)

// SQS messages and batches reach 1 MiB, and JSON escaping can triple them.
const maxRequestBytes = 4 << 20

// Options configures the handler.
type Options struct {
	AccessKeyID     string
	SecretAccessKey string
}

type handler struct {
	verifier *sigv4.Verifier
}

// New returns the SQS handler. Every action answers UnsupportedOperation.
func New(opts Options) http.Handler {
	return &handler{verifier: sigv4.New(opts.AccessKeyID, opts.SecretAccessKey)}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("x-amzn-RequestId", newRequestID())
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	op := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.")

	apiErr, msg := errUnsupportedOperation, "the action is not supported"
	if op != "" {
		msg = fmt.Sprintf("%s is not supported", op)
	}
	if err := h.verifier.VerifyService(r, "sqs"); err != nil {
		apiErr, msg = authError(err), err.Error()
	}
	writeError(w, apiErr, msg)
	clogSqsapi().Info("request", "method", r.Method, "op", op, "status", apiErr.status, "duration", time.Since(start))
}

// writeError answers with the JSON 1.0 error body and the legacy query code.
func writeError(w http.ResponseWriter, e apiError, message string) {
	body, _ := json.Marshal(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{e.typ, message})
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("x-amzn-query-error", e.queryCode+";Sender")
	w.WriteHeader(e.status)
	_, _ = w.Write(body)
}

// newRequestID returns a random UUID v4.
func newRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
