package topic

import "errors"

// The API layer maps these sentinels to SNS error codes.
var (
	ErrNotFound         = errors.New("topic or subscription does not exist")
	ErrInvalidParameter = errors.New("invalid parameter")
	ErrResourceNotFound = errors.New("resource does not exist")
	ErrTagLimitExceeded = errors.New("too many tags")
)
