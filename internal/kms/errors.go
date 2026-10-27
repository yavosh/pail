package kms

// Error describes a KMS failure that the API can return to a client.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

func fail(code, message string) error { return &Error{Code: code, Message: message} }

func invalid(message string) error { return fail("ValidationException", message) }

func unsupported() error {
	return fail("UnsupportedOperationException", "the requested feature is not supported")
}
