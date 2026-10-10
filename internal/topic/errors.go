package topic

import "errors"

// The API layer maps these sentinels to SNS error codes.
var (
	ErrNotFound         = errors.New("topic or subscription does not exist")
	ErrInvalidParameter = errors.New("invalid parameter")
	// ErrParameterValueInvalid is the invalid data type of a message attribute.
	ErrParameterValueInvalid = errors.New("parameter value invalid")
	ErrResourceNotFound      = errors.New("resource does not exist")
	ErrTagLimitExceeded      = errors.New("too many tags")
	ErrAuthorization         = errors.New("not authorized")
)
