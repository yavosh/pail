package snsapi

import (
	"errors"
	"net/http"

	"github.com/yavosh/pail/internal/sigv4"
)

// apiError is one SNS error: the HTTP status, the fault type, and the code.
type apiError struct {
	status int
	typ    string
	code   string
}

// The missing-auth, unknown-key, and bad-signature entries match the AWS
// recordings in test/diff (sns-auth-errors). The others are from memory.
var (
	errInvalidAction       = apiError{http.StatusBadRequest, "Sender", "InvalidAction"}
	errMissingAuth         = apiError{http.StatusForbidden, "Sender", "MissingAuthenticationToken"}
	errInvalidClientToken  = apiError{http.StatusForbidden, "Sender", "InvalidClientTokenId"}
	errSignatureMismatch   = apiError{http.StatusForbidden, "Sender", "SignatureDoesNotMatch"}
	errIncompleteSignature = apiError{http.StatusBadRequest, "Sender", "IncompleteSignature"}
	errTooLarge            = apiError{http.StatusRequestEntityTooLarge, "Sender", "RequestEntityTooLarge"}
	errInternalFailure     = apiError{http.StatusInternalServerError, "Receiver", "InternalFailure"}
)

// authError maps an error from the verifier to an SNS error.
func authError(err error) apiError {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return errTooLarge
	}
	switch {
	case errors.Is(err, sigv4.ErrMissingAuth):
		return errMissingAuth
	case errors.Is(err, sigv4.ErrInvalidAccessKeyID):
		return errInvalidClientToken
	// AWS reports a skewed clock as a signature mismatch.
	case errors.Is(err, sigv4.ErrSignatureMismatch), errors.Is(err, sigv4.ErrUnsignedHeader), errors.Is(err, sigv4.ErrRequestTimeTooSkewed):
		return errSignatureMismatch
	case errors.Is(err, sigv4.ErrMalformedAuth), errors.Is(err, sigv4.ErrUnsupportedAuth):
		return errIncompleteSignature
	case errors.Is(err, sigv4.ErrNotImplemented):
		return errInvalidAction
	}
	return errInternalFailure
}
