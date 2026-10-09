package sqsapi

import (
	"errors"
	"net/http"

	"github.com/yavosh/pail/internal/sigv4"
)

// apiError is one SQS error: the HTTP status, the "__type" in the body, and
// the legacy query code that x-amzn-query-error carries.
type apiError struct {
	status    int
	typ       string
	queryCode string
}

// The auth and size entries are from memory and unverified against AWS.
// PR 2 records them; see docs/plans/2026-10-09-sqs-sns.md.
var (
	errUnsupportedOperation = apiError{http.StatusBadRequest, "com.amazonaws.sqs#UnsupportedOperation", "AWS.SimpleQueueService.UnsupportedOperation"}
	errMissingAuth          = apiError{http.StatusForbidden, "com.amazon.coral.service#MissingAuthenticationTokenException", "MissingAuthenticationToken"}
	errInvalidClientToken   = apiError{http.StatusForbidden, "com.amazon.coral.service#UnrecognizedClientException", "InvalidClientTokenId"}
	errSignatureMismatch    = apiError{http.StatusForbidden, "com.amazon.coral.service#InvalidSignatureException", "SignatureDoesNotMatch"}
	errRequestExpired       = apiError{http.StatusForbidden, "com.amazon.coral.service#InvalidSignatureException", "RequestExpired"}
	errIncompleteSignature  = apiError{http.StatusBadRequest, "com.amazon.coral.service#IncompleteSignatureException", "IncompleteSignature"}
	errTooLarge             = apiError{http.StatusRequestEntityTooLarge, "com.amazon.coral.service#RequestEntityTooLargeException", "RequestEntityTooLarge"}
	errInternalFailure      = apiError{http.StatusInternalServerError, "com.amazon.coral.service#InternalFailure", "InternalFailure"}
)

// authError maps an error from the verifier to an SQS error.
func authError(err error) apiError {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return errTooLarge
	}
	switch {
	case errors.Is(err, sigv4.ErrMissingAuth):
		return errMissingAuth
	case errors.Is(err, sigv4.ErrInvalidAccessKeyID):
		return errInvalidClientToken
	case errors.Is(err, sigv4.ErrSignatureMismatch), errors.Is(err, sigv4.ErrUnsignedHeader):
		return errSignatureMismatch
	case errors.Is(err, sigv4.ErrRequestTimeTooSkewed):
		return errRequestExpired
	case errors.Is(err, sigv4.ErrMalformedAuth), errors.Is(err, sigv4.ErrUnsupportedAuth):
		return errIncompleteSignature
	case errors.Is(err, sigv4.ErrNotImplemented):
		return errUnsupportedOperation
	}
	return errInternalFailure
}
