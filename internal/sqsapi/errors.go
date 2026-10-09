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

// The missing-auth, unknown-key, and bad-signature entries match the AWS
// recordings in test/diff (sqs-auth-errors). The others are from memory.
var (
	errUnsupportedOperation = apiError{http.StatusBadRequest, "com.amazonaws.sqs#UnsupportedOperation", "AWS.SimpleQueueService.UnsupportedOperation"}
	errAccessDenied         = apiError{http.StatusForbidden, "com.amazon.coral.service#AccessDeniedException", "AccessDenied"}
	errInvalidClientToken   = apiError{http.StatusForbidden, "com.amazon.coral.service#UnrecognizedClientException", "InvalidClientTokenId"}
	errSignatureMismatch    = apiError{http.StatusForbidden, "com.amazon.coral.service#InvalidSignatureException", "SignatureDoesNotMatch"}
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
		return errAccessDenied
	case errors.Is(err, sigv4.ErrInvalidAccessKeyID):
		return errInvalidClientToken
	// AWS reports a skewed clock as a signature mismatch.
	case errors.Is(err, sigv4.ErrSignatureMismatch), errors.Is(err, sigv4.ErrUnsignedHeader), errors.Is(err, sigv4.ErrRequestTimeTooSkewed):
		return errSignatureMismatch
	case errors.Is(err, sigv4.ErrMalformedAuth), errors.Is(err, sigv4.ErrUnsupportedAuth):
		return errIncompleteSignature
	case errors.Is(err, sigv4.ErrNotImplemented):
		return errUnsupportedOperation
	}
	return errInternalFailure
}
