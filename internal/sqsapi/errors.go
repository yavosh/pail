package sqsapi

import (
	"errors"
	"net/http"

	"github.com/yavosh/pail/internal/queue"
	"github.com/yavosh/pail/internal/sigv4"
)

// apiError is one SQS error: the HTTP status, the "__type" in the body, and
// the legacy query code that x-amzn-query-error carries.
type apiError struct {
	status    int
	typ       string
	queryCode string
}

// Entries marked verified match AWS recordings in test/diff. Still unverified:
// MissingParameter for QueueUrl and ReceiptHandle, MessageDeduplicationId on a
// standard queue, the per-entry InternalError code, list attribute values,
// SerializationException for non-object bodies, and UnsupportedOperation for
// model operations that pail does not implement.
var (
	// Verified.
	errQueueDoesNotExist = apiError{http.StatusBadRequest, "com.amazonaws.sqs#QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue"}
	// The auth entries are verified. UnsupportedOperation is not, for unimplemented model operations.
	errUnsupportedOperation = apiError{http.StatusBadRequest, "com.amazonaws.sqs#UnsupportedOperation", "AWS.SimpleQueueService.UnsupportedOperation"}
	errAccessDenied         = apiError{http.StatusForbidden, "com.amazon.coral.service#AccessDeniedException", "AccessDenied"}
	errInvalidClientToken   = apiError{http.StatusForbidden, "com.amazon.coral.service#UnrecognizedClientException", "InvalidClientTokenId"}
	errSignatureMismatch    = apiError{http.StatusForbidden, "com.amazon.coral.service#InvalidSignatureException", "SignatureDoesNotMatch"}
	errIncompleteSignature  = apiError{http.StatusBadRequest, "com.amazon.coral.service#IncompleteSignatureException", "IncompleteSignature"}
	errTooLarge             = apiError{http.StatusRequestEntityTooLarge, "com.amazon.coral.service#RequestEntityTooLargeException", "RequestEntityTooLarge"}
	errInternalFailure      = apiError{http.StatusInternalServerError, "com.amazon.coral.service#InternalFailure", "InternalFailure"}

	errQueueNameExists          = apiError{http.StatusBadRequest, "com.amazonaws.sqs#QueueNameExists", "QueueAlreadyExists"}
	errInvalidAttributeName     = apiError{http.StatusBadRequest, "com.amazonaws.sqs#InvalidAttributeName", "InvalidAttributeName"}
	errInvalidAttributeValue    = apiError{http.StatusBadRequest, "com.amazonaws.sqs#InvalidAttributeValue", "InvalidAttributeValue"}
	errInvalidParameterValue    = apiError{http.StatusBadRequest, "com.amazon.coral.service#InvalidParameterValueException", "InvalidParameterValue"}
	errMissingParameter         = apiError{http.StatusBadRequest, "com.amazon.coral.service#MissingRequiredParameterException", "MissingParameter"}
	errInvalidMessageContents   = apiError{http.StatusBadRequest, "com.amazonaws.sqs#InvalidMessageContents", "InvalidMessageContents"}
	errReceiptHandleIsInvalid   = apiError{http.StatusNotFound, "com.amazonaws.sqs#ReceiptHandleIsInvalid", "ReceiptHandleIsInvalid"}
	errPurgeQueueInProgress     = apiError{http.StatusForbidden, "com.amazonaws.sqs#PurgeQueueInProgress", "AWS.SimpleQueueService.PurgeQueueInProgress"}
	errEmptyBatchRequest        = apiError{http.StatusBadRequest, "com.amazonaws.sqs#EmptyBatchRequest", "AWS.SimpleQueueService.EmptyBatchRequest"}
	errTooManyEntries           = apiError{http.StatusBadRequest, "com.amazonaws.sqs#TooManyEntriesInBatchRequest", "AWS.SimpleQueueService.TooManyEntriesInBatchRequest"}
	errBatchEntryIDsNotDistinct = apiError{http.StatusBadRequest, "com.amazonaws.sqs#BatchEntryIdsNotDistinct", "AWS.SimpleQueueService.BatchEntryIdsNotDistinct"}
	errInvalidBatchEntryID      = apiError{http.StatusBadRequest, "com.amazonaws.sqs#InvalidBatchEntryId", "AWS.SimpleQueueService.InvalidBatchEntryId"}
	errBatchRequestTooLong      = apiError{http.StatusBadRequest, "com.amazonaws.sqs#BatchRequestTooLong", "AWS.SimpleQueueService.BatchRequestTooLong"}
	errSerialization            = apiError{http.StatusBadRequest, "com.amazon.coral.service#SerializationException", "MalformedInput"}
	errUnknownOperation         = apiError{http.StatusBadRequest, "com.amazon.coral.service#UnknownOperationException", "InvalidAction"}
)

// The API's own errors. Handlers wrap them with %w and a message.
var (
	errMissingParam    = errors.New("missing parameter")
	errBadJSON         = errors.New("bad JSON")
	errEmptyBatch      = errors.New("empty batch")
	errTooManyInBatch  = errors.New("too many entries in batch")
	errBatchIDsNotUniq = errors.New("batch entry ids not distinct")
	errBadBatchEntryID = errors.New("invalid batch entry id")
	errBatchTooLong    = errors.New("batch too long")
)

// mapping ties an error to its SQS error. entryCode is the Code of a failed
// batch entry; it is empty for errors that fail a whole request.
type mapping struct {
	target    error
	api       apiError
	entryCode string
}

var mappings = []mapping{
	{queue.ErrQueueDoesNotExist, errQueueDoesNotExist, ""},
	{queue.ErrQueueNameExists, errQueueNameExists, ""},
	{queue.ErrInvalidAttributeName, errInvalidAttributeName, ""},
	{queue.ErrInvalidAttributeValue, errInvalidAttributeValue, ""},
	{queue.ErrInvalidParameterValue, errInvalidParameterValue, "InvalidParameterValue"},
	{queue.ErrUnsupportedOperation, errUnsupportedOperation, "UnsupportedOperation"},
	{queue.ErrInvalidName, errInvalidParameterValue, "InvalidParameterValue"},
	{queue.ErrMessageTooLong, errInvalidParameterValue, "InvalidParameterValue"},
	{queue.ErrMessageNotAvailable, errInvalidParameterValue, "InvalidParameterValue"},
	{queue.ErrTooManyTags, errInvalidParameterValue, "InvalidParameterValue"},
	{errMissingParam, errMissingParameter, "MissingParameter"},
	{queue.ErrMissingParameter, errMissingParameter, "MissingParameter"},
	{queue.ErrInvalidMessageContents, errInvalidMessageContents, "InvalidMessageContents"},
	{queue.ErrReceiptHandleInvalid, errReceiptHandleIsInvalid, "ReceiptHandleIsInvalid"},
	{queue.ErrPurgeInProgress, errPurgeQueueInProgress, ""},
	{errEmptyBatch, errEmptyBatchRequest, ""},
	{errTooManyInBatch, errTooManyEntries, ""},
	{errBatchIDsNotUniq, errBatchEntryIDsNotDistinct, ""},
	{errBadBatchEntryID, errInvalidBatchEntryID, ""},
	{errBatchTooLong, errBatchRequestTooLong, ""},
	{errBadJSON, errSerialization, ""},
}

// lookup finds the mapping for err.
func lookup(err error) (mapping, bool) {
	for _, m := range mappings {
		if errors.Is(err, m.target) {
			return m, true
		}
	}
	return mapping{}, false
}

// mapError maps an engine or API error to an SQS error.
func mapError(err error) apiError {
	if m, ok := lookup(err); ok {
		return m.api
	}
	return errInternalFailure
}

// entryCode returns the Code of a failed batch entry.
func entryCode(err error) string {
	if m, ok := lookup(err); ok && m.entryCode != "" {
		return m.entryCode
	}
	return "InternalError"
}

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
