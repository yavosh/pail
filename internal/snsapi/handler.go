// Package snsapi serves the Amazon SNS API over the AWS query protocol.
package snsapi

import (
	"crypto/rand"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	"github.com/yavosh/pail/internal/sigv4"
)

// SNS messages reach 1 MiB with a raised topic limit, and form encoding can triple them.
const maxRequestBytes = 4 << 20

// Options configures the handler.
type Options struct {
	AccessKeyID     string
	SecretAccessKey string
}

type handler struct {
	verifier *sigv4.Verifier
}

// New returns the SNS handler. Every action answers InvalidAction.
func New(opts Options) http.Handler {
	return &handler{verifier: sigv4.New(opts.AccessKeyID, opts.SecretAccessKey)}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := newRequestID()
	w.Header().Set("x-amzn-RequestId", requestID)
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)

	var op string
	apiErr, msg := errInvalidAction, "the action is not supported"
	if err := h.verifier.VerifyService(r, "sns"); err != nil {
		apiErr, msg = authError(err), err.Error()
	} else {
		_ = r.ParseForm() // a parse error leaves Action empty
		if op = r.PostForm.Get("Action"); op != "" {
			msg = fmt.Sprintf("%s is not supported", op)
		}
	}
	writeError(w, apiErr, msg, requestID)
	clogSnsapi().Info("request", "method", r.Method, "op", op, "status", apiErr.status, "duration", time.Since(start))
}

// writeError answers with the query protocol's XML error document.
func writeError(w http.ResponseWriter, e apiError, message, requestID string) {
	type errorBody struct {
		Type    string
		Code    string
		Message string
	}
	body, _ := xml.Marshal(struct {
		XMLName   xml.Name `xml:"ErrorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Error     errorBody
		RequestID string `xml:"RequestId"`
	}{Xmlns: "http://sns.amazonaws.com/doc/2010-03-31/", Error: errorBody{e.typ, e.code, message}, RequestID: requestID})
	w.Header().Set("Content-Type", "text/xml")
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
