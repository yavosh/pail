package snsapi

import (
	"errors"
	"net/http"

	"github.com/yavosh/pail/internal/sigv4"
	"github.com/yavosh/pail/internal/topic"
)

// apiError is one SNS error: the HTTP status, the fault type, and the code.
type apiError struct {
	status int
	typ    string
	code   string
}

// The auth entries, NotFound, InvalidParameter, InvalidAction, ValidationError,
// TooManyEntriesInBatchRequest, BatchEntryIdsNotDistinct, and InvalidBatchEntryId
// match the AWS recordings in test/diff. The others come from the service
// model and are unverified: ResourceNotFound, TagLimitExceeded, BatchRequestTooLong,
// InternalError, and AuthorizationError.
var (
	errInvalidAction       = apiError{http.StatusBadRequest, "Sender", "InvalidAction"}
	errMissingAuth         = apiError{http.StatusForbidden, "Sender", "MissingAuthenticationToken"}
	errInvalidClientToken  = apiError{http.StatusForbidden, "Sender", "InvalidClientTokenId"}
	errSignatureMismatch   = apiError{http.StatusForbidden, "Sender", "SignatureDoesNotMatch"}
	errIncompleteSignature = apiError{http.StatusBadRequest, "Sender", "IncompleteSignature"}
	errTooLarge            = apiError{http.StatusRequestEntityTooLarge, "Sender", "RequestEntityTooLarge"}

	errNotFound            = apiError{http.StatusNotFound, "Sender", "NotFound"}
	errInvalidParameter    = apiError{http.StatusBadRequest, "Sender", "InvalidParameter"}
	errResourceNotFound    = apiError{http.StatusNotFound, "Sender", "ResourceNotFound"}
	errTagLimitExceeded    = apiError{http.StatusBadRequest, "Sender", "TagLimitExceeded"}
	errValidationError     = apiError{http.StatusBadRequest, "Sender", "ValidationError"}
	errTooManyEntries      = apiError{http.StatusBadRequest, "Sender", "TooManyEntriesInBatchRequest"}
	errEntryIDsNotDistinct = apiError{http.StatusBadRequest, "Sender", "BatchEntryIdsNotDistinct"}
	errInvalidBatchEntryID = apiError{http.StatusBadRequest, "Sender", "InvalidBatchEntryId"}
	errBatchRequestTooLong = apiError{http.StatusBadRequest, "Sender", "BatchRequestTooLong"}
	errInternalError       = apiError{http.StatusInternalServerError, "Receiver", "InternalError"}
	errAuthorization       = apiError{http.StatusForbidden, "Sender", "AuthorizationError"}
)

// mapping ties an error to its SNS error.
var mappings = []struct {
	target error
	api    apiError
}{
	{topic.ErrNotFound, errNotFound},
	{topic.ErrInvalidParameter, errInvalidParameter},
	{topic.ErrResourceNotFound, errResourceNotFound},
	{topic.ErrTagLimitExceeded, errTagLimitExceeded},
	{topic.ErrAuthorization, errAuthorization},
	{errValidation, errValidationError},
	{errTooManyInBatch, errTooManyEntries},
	{errBatchIDsNotUniq, errEntryIDsNotDistinct},
	{errBadBatchEntryID, errInvalidBatchEntryID},
	{errBatchTooLong, errBatchRequestTooLong},
}

// mapError maps an engine or API error to an SNS error.
func mapError(err error) apiError {
	for _, m := range mappings {
		if errors.Is(err, m.target) {
			return m.api
		}
	}
	return errInternalError
}

// entryCode returns the Code of a failed batch entry.
func entryCode(err error) string { return mapError(err).code }

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
	return errInternalError
}
