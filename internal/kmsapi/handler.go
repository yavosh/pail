// Package kmsapi serves AWS KMS over the AWS JSON 1.1 protocol.
package kmsapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yavosh/pail/internal/kms"
	"github.com/yavosh/pail/internal/sigv4"
)

const (
	contentType     = "application/x-amz-json-1.1"
	maxRequestBytes = 128 << 10
)

// Keys is the key engine consumed by the HTTP handler.
type Keys interface {
	Create(context.Context, kms.CreateInput) (kms.Metadata, error)
	Describe(context.Context, string) (kms.Metadata, error)
	Change(context.Context, string, string, string, int) (kms.Metadata, error)
	ListKeys(context.Context, int, string) ([]kms.KeyEntry, string, error)
	ChangeAlias(context.Context, string, string, string) error
	ListAliases(context.Context, string, int, string) ([]kms.Alias, string, error)
	ChangeTags(context.Context, string, []kms.Tag, []string) error
	ListTags(context.Context, string, int, string) ([]kms.Tag, string, error)
	Policy(context.Context, string, string, *string) (string, error)
	Encrypt(context.Context, kms.CryptoInput) (kms.CryptoResult, error)
	Decrypt(context.Context, kms.CryptoInput) (kms.CryptoResult, error)
	ReEncrypt(context.Context, kms.CryptoInput, kms.CryptoInput) (kms.CryptoResult, string, error)
	GenerateDataKey(context.Context, kms.CryptoInput, int, bool) (kms.CryptoResult, error)
}

// Options configures KMS authentication and storage.
type Options struct {
	AccessKeyID, SecretAccessKey string
	Keys                         Keys
}

type opFunc func(*http.Request, []byte) (any, error)

type handler struct {
	verifier *sigv4.Verifier
	keys     Keys
	ops      map[string]opFunc
}

// New returns a KMS handler using one operation table.
func New(opts Options) http.Handler {
	h := &handler{verifier: sigv4.New(opts.AccessKeyID, opts.SecretAccessKey), keys: opts.Keys}
	h.ops = map[string]opFunc{
		"CreateKey":                       operation(h.createKey),
		"DescribeKey":                     operation(h.describeKey),
		"ListKeys":                        operation(h.listKeys),
		"UpdateKeyDescription":            operation(h.changeKey),
		"EnableKey":                       operation(h.changeKey),
		"DisableKey":                      operation(h.changeKey),
		"ScheduleKeyDeletion":             operation(h.changeKey),
		"CancelKeyDeletion":               operation(h.changeKey),
		"CreateAlias":                     operation(h.changeAlias),
		"UpdateAlias":                     operation(h.changeAlias),
		"DeleteAlias":                     operation(h.changeAlias),
		"ListAliases":                     operation(h.listAliases),
		"TagResource":                     operation(h.changeTags),
		"UntagResource":                   operation(h.changeTags),
		"ListResourceTags":                operation(h.listTags),
		"GetKeyPolicy":                    operation(h.keyPolicy),
		"PutKeyPolicy":                    operation(h.keyPolicy),
		"ListKeyPolicies":                 operation(h.listKeyPolicies),
		"Encrypt":                         operation(h.crypto),
		"Decrypt":                         operation(h.crypto),
		"ReEncrypt":                       operation(h.reEncrypt),
		"GenerateDataKey":                 operation(h.dataKey),
		"GenerateDataKeyWithoutPlaintext": operation(h.dataKey),
		"GenerateRandom":                  operation(h.generateRandom),
	}
	return h
}

func operation[In any](fn func(*http.Request, In) (any, error)) opFunc {
	return func(r *http.Request, body []byte) (any, error) {
		body = bytes.TrimSpace(body)
		var in In
		if len(body) == 0 || body[0] != '{' || json.Unmarshal(body, &in) != nil {
			return nil, &kms.Error{Code: "SerializationException", Message: "invalid json request"}
		}
		return fn(r, in)
	}
}

func operationName(r *http.Request) string {
	return strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "TrentService.")
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	op := operationName(r)
	w.Header().Set("X-Amzn-RequestId", requestID())
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate, private")
	w.Header().Set("Expires", "0")
	status := h.serve(w, r, op)
	clogKmsapi().Info("request", "method", r.Method, "op", op, "status", status, "duration", time.Since(start))
}

func (h *handler) serve(w http.ResponseWriter, r *http.Request, op string) int {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	if err := h.verifier.VerifyService(r, "kms"); err != nil {
		return writeAuthError(w, err)
	}
	if r.Method != http.MethodPost || r.URL.Path != "/" || !strings.HasPrefix(r.Header.Get("X-Amz-Target"), "TrentService.") {
		return writeError(w, "UnknownOperationException", "unknown operation", http.StatusBadRequest)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return writeAuthError(w, err)
	}
	fn := h.ops[op]
	if fn == nil {
		if modelOperations[op] {
			return writeError(w, "UnsupportedOperationException", "operation is not supported", http.StatusBadRequest)
		}
		return writeError(w, "UnknownOperationException", "unknown operation", http.StatusBadRequest)
	}
	result, err := fn(r, body)
	if err != nil {
		if api, ok := errors.AsType[*kms.Error](err); ok {
			return writeError(w, api.Code, api.Message, http.StatusBadRequest)
		}
		clogKmsapi().Error("operation failed", "op", op, "error", err)
		return writeError(w, "KMSInternalException", "internal failure", http.StatusInternalServerError)
	}
	var b []byte
	if result != nil {
		b, err = json.Marshal(result)
		if err != nil {
			return writeError(w, "KMSInternalException", "encode response", http.StatusInternalServerError)
		}
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
	return http.StatusOK
}
