package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"

	"github.com/yavosh/pail/internal/sigv4"
	"github.com/yavosh/pail/internal/store"
)

// apiError is an S3 error: the code clients match on, and its HTTP status.
type apiError struct {
	Code    string
	Status  int
	Message string
}

var (
	errNotImplemented            = apiError{"NotImplemented", http.StatusNotImplemented, "A header or query you provided implies functionality that is not implemented."}
	errInternal                  = apiError{"InternalError", http.StatusInternalServerError, "We encountered an internal error. Please try again."}
	errAccessDenied              = apiError{"AccessDenied", http.StatusForbidden, "Access Denied"}
	errUnsignedHeader            = apiError{"AccessDenied", http.StatusForbidden, "There were headers present in the request which were not signed"}
	errUnsupportedAuth           = apiError{"InvalidRequest", http.StatusBadRequest, "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256."}
	errMissingContentSHA         = apiError{"InvalidRequest", http.StatusBadRequest, "Missing required header for this request: x-amz-content-sha256"}
	errMalformedAuth             = apiError{"AuthorizationHeaderMalformed", http.StatusBadRequest, "The authorization header is malformed."}
	errInvalidAccessKeyID        = apiError{"InvalidAccessKeyId", http.StatusForbidden, "The AWS Access Key Id you provided does not exist in our records."}
	errSignatureMismatch         = apiError{"SignatureDoesNotMatch", http.StatusForbidden, "The request signature we calculated does not match the signature you provided. Check your key and signing method."}
	errTimeTooSkewed             = apiError{"RequestTimeTooSkewed", http.StatusForbidden, "The difference between the request time and the current time is too large."}
	errContentSHAMismatch        = apiError{"XAmzContentSHA256Mismatch", http.StatusBadRequest, "The provided 'x-amz-content-sha256' header does not match what was computed."}
	errNoSuchBucket              = apiError{"NoSuchBucket", http.StatusNotFound, "The specified bucket does not exist"}
	errBucketAlreadyOwnedByYou   = apiError{"BucketAlreadyOwnedByYou", http.StatusConflict, "Your previous request to create the named bucket succeeded and you already own it."}
	errBucketNotEmpty            = apiError{"BucketNotEmpty", http.StatusConflict, "The bucket you tried to delete is not empty"}
	errInvalidBucketName         = apiError{"InvalidBucketName", http.StatusBadRequest, "The specified bucket is not valid."}
	errMalformedXML              = apiError{"MalformedXML", http.StatusBadRequest, "The XML you provided was not well-formed or did not validate against our published schema."}
	errInvalidArgument           = apiError{"InvalidArgument", http.StatusBadRequest, "Invalid Argument"}
	errIllegalLocationConstraint = apiError{"IllegalLocationConstraintException", http.StatusBadRequest, "The specified location-constraint is not valid for this endpoint."}
	errMaxMessageLength          = apiError{"MaxMessageLengthExceeded", http.StatusBadRequest, "Your request was too big."}
	errNoSuchKey                 = apiError{"NoSuchKey", http.StatusNotFound, "The specified key does not exist."}
	errPreconditionFailed        = apiError{"PreconditionFailed", http.StatusPreconditionFailed, "At least one of the pre-conditions you specified did not hold"}
	errBadDigest                 = apiError{"BadDigest", http.StatusBadRequest, "The Content-MD5 you specified did not match what we received."}
	errInvalidDigest             = apiError{"InvalidDigest", http.StatusBadRequest, "The Content-MD5 you specified was invalid."}
	errInvalidRange              = apiError{"InvalidRange", http.StatusRequestedRangeNotSatisfiable, "The requested range is not satisfiable"}
	errKeyTooLong                = apiError{"KeyTooLongError", http.StatusBadRequest, "Your key is too long"}
	errEntityTooLarge            = apiError{"EntityTooLarge", http.StatusBadRequest, "Your proposed upload exceeds the maximum allowed size"}
	errMetadataTooLarge          = apiError{"MetadataTooLarge", http.StatusBadRequest, "Your metadata headers exceed the maximum allowed metadata size."}
	errMissingContentLength      = apiError{"MissingContentLength", http.StatusLengthRequired, "You must provide the Content-Length HTTP header."}
)

// apiErrors maps the errors handlers check with errors.Is to S3 errors.
var apiErrors = []struct {
	err error
	api apiError
}{
	{sigv4.ErrMissingAuth, errAccessDenied},
	{sigv4.ErrUnsignedHeader, errUnsignedHeader},
	{sigv4.ErrUnsupportedAuth, errUnsupportedAuth},
	{sigv4.ErrMissingContentSHA256, errMissingContentSHA},
	{sigv4.ErrMalformedAuth, errMalformedAuth},
	{sigv4.ErrInvalidAccessKeyID, errInvalidAccessKeyID},
	{sigv4.ErrSignatureMismatch, errSignatureMismatch},
	{sigv4.ErrRequestTimeTooSkewed, errTimeTooSkewed},
	{sigv4.ErrContentSHA256Mismatch, errContentSHAMismatch},
	{sigv4.ErrNotImplemented, errNotImplemented},
	{store.ErrNoSuchBucket, errNoSuchBucket},
	{store.ErrBucketExists, errBucketAlreadyOwnedByYou},
	{store.ErrBucketNotEmpty, errBucketNotEmpty},
	{store.ErrNoSuchKey, errNoSuchKey},
	{store.ErrPreconditionFailed, errPreconditionFailed},
	{store.ErrBadDigest, errBadDigest},
}

// toAPIError maps err to an S3 error. An unknown error is InternalError: its
// text can hold host paths, so it is logged, never sent.
func toAPIError(err error) apiError {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return errMaxMessageLength
	}
	for _, m := range apiErrors {
		if errors.Is(err, m.err) {
			return m.api
		}
	}
	// A client that disconnected cancels the request; that is not a server fault.
	if !errors.Is(err, context.Canceled) {
		clogS3api().Error("internal error", "error", err)
	}
	return errInternal
}

type errorBody struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

// writeError sends e as an S3 XML error. A HEAD response gets the status only.
func writeError(w http.ResponseWriter, r *http.Request, e apiError) {
	body, err := xml.Marshal(errorBody{
		Code:      e.Code,
		Message:   e.Message,
		Resource:  r.URL.Path,
		RequestID: w.Header().Get("x-amz-request-id"),
	})
	if err != nil {
		clogS3api().Error("marshal error body", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	body = append([]byte(xml.Header), body...)
	w.Header().Set("Content-Type", "application/xml")
	if r.Method == http.MethodHead {
		w.WriteHeader(e.Status)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(e.Status)
	_, _ = w.Write(body)
}
