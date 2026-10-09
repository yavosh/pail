package queue

import "errors"

// The API layer maps these sentinels to SQS error codes.
var (
	ErrQueueDoesNotExist      = errors.New("queue does not exist")
	ErrQueueNameExists        = errors.New("queue name exists with different attributes")
	ErrInvalidName            = errors.New("invalid queue name")
	ErrInvalidAttributeName   = errors.New("invalid attribute name")
	ErrInvalidAttributeValue  = errors.New("invalid attribute value")
	ErrInvalidParameterValue  = errors.New("invalid parameter value")
	ErrMessageTooLong         = errors.New("message is too long")
	ErrInvalidMessageContents = errors.New("invalid message contents")
	ErrReceiptHandleInvalid   = errors.New("receipt handle is invalid")
	ErrMessageNotInflight     = errors.New("message is not in flight")
	ErrMessageNotAvailable    = errors.New("message is not available")
	ErrPurgeInProgress        = errors.New("purge queue in progress")
	ErrTooManyTags            = errors.New("too many tags")
	ErrUnsupported            = errors.New("unsupported")
)
