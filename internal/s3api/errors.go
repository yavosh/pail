package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/yavosh/pail/internal/sigv4"
	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/tag"
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
	errInvalidBucketOwner        = apiError{"InvalidBucketOwnerAWSAccountID", http.StatusBadRequest, "The value of the expected bucket owner parameter must be an AWS Account ID"}
	errCORSForbidden             = apiError{"AccessForbidden", http.StatusForbidden, "CORSResponse: This CORS request is not allowed."}
	errInvalidLifecycleDays      = apiError{"InvalidRequest", http.StatusBadRequest, "Days must be a positive integer."}
	errUnsignedHeader            = apiError{"AccessDenied", http.StatusForbidden, "There were headers present in the request which were not signed"}
	errUnsupportedAuth           = apiError{"InvalidRequest", http.StatusBadRequest, "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256."}
	errMissingContentSHA         = apiError{"InvalidRequest", http.StatusBadRequest, "Missing required header for this request: x-amz-content-sha256"}
	errMalformedAuth             = apiError{"AuthorizationHeaderMalformed", http.StatusBadRequest, "The authorization header is malformed."}
	errInvalidAccessKeyID        = apiError{"InvalidAccessKeyId", http.StatusForbidden, "The AWS Access Key Id you provided does not exist in our records."}
	errSignatureMismatch         = apiError{"SignatureDoesNotMatch", http.StatusForbidden, "The request signature we calculated does not match the signature you provided. Check your key and signing method."}
	errRequestExpired            = apiError{"AccessDenied", http.StatusForbidden, "Request has expired"}
	errRequestNotYetValid        = apiError{"AccessDenied", http.StatusForbidden, "Request is not valid yet"}
	errMalformedPresign          = apiError{"AuthorizationQueryParametersError", http.StatusBadRequest, "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters."}
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
	errIncompleteBody            = apiError{"IncompleteBody", http.StatusBadRequest, "You did not provide the number of bytes specified by the Content-Length HTTP header."}
	errChecksumMismatch          = apiError{"BadDigest", http.StatusBadRequest, "The checksum you specified did not match the calculated checksum."}
	errInvalidChunkSize          = apiError{"InvalidChunkSizeError", http.StatusForbidden, "Only the last chunk is allowed to have a size less than 8192 bytes"}
	errInvalidChecksum           = apiError{"InvalidRequest", http.StatusBadRequest, "The x-amz-checksum header or algorithm you specified is invalid."}
	errInvalidCopySource         = apiError{"InvalidArgument", http.StatusBadRequest, "Copy Source must mention the source bucket and key: sourcebucket/sourcekey"}
	errUnknownDirective          = apiError{"InvalidArgument", http.StatusBadRequest, "Unknown metadata directive."}
	errCopyToSelf                = apiError{"InvalidRequest", http.StatusBadRequest, "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."}
	errCopySourceTooLarge        = apiError{"InvalidRequest", http.StatusBadRequest, "The specified copy source is larger than the maximum allowable size for a copy source: 5368709120"}
	errMissingContentMD5         = apiError{"InvalidRequest", http.StatusBadRequest, "Missing required header for this request: Content-MD5"}
	errNoSuchUpload              = apiError{"NoSuchUpload", http.StatusNotFound, "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."}
	errInvalidPart               = apiError{"InvalidPart", http.StatusBadRequest, "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag."}
	errInvalidPartOrder          = apiError{"InvalidPartOrder", http.StatusBadRequest, "The list of parts was not in ascending order. Parts must be ordered by part number."}
	errEntityTooSmall            = apiError{"EntityTooSmall", http.StatusBadRequest, "Your proposed upload is smaller than the minimum allowed object size."}
	errInvalidPartNumber         = apiError{"InvalidArgument", http.StatusBadRequest, "Part number must be an integer between 1 and 10000, inclusive"}
	errPartNumberRange           = apiError{"InvalidPartNumber", http.StatusRequestedRangeNotSatisfiable, "The requested partnumber is not satisfiable"}
	errPartWithRange             = apiError{"InvalidRequest", http.StatusBadRequest, "Cannot specify both Range header and partNumber query parameter"}
	errMissingAttributes         = apiError{"InvalidRequest", http.StatusBadRequest, "The x-amz-object-attributes header specifying the attributes to be retrieved is either missing or empty"}
	errChecksumAlgorithmMismatch = apiError{"InvalidRequest", http.StatusBadRequest, "The checksum algorithm you specified does not match the one the multipart upload was created with."}
	errInvalidObjectSize         = apiError{"InvalidRequest", http.StatusBadRequest, "The provided 'x-amz-mp-object-size' header value does not match what was computed."}
	errNoSuchVersion             = apiError{"NoSuchVersion", http.StatusNotFound, "The specified version does not exist."}
)

// apiErrors maps the errors handlers check with errors.Is to S3 errors.
var apiErrors = []struct {
	err error
	api apiError
}{
	{sigv4.ErrMalformedPresignV2, apiError{"InvalidArgument", http.StatusBadRequest, "Query-string authentication requires AWSAccessKeyId, Signature, and Expires."}},
	{store.ErrAccessDenied, errAccessDenied},
	{sigv4.ErrPolicyCondition, apiError{"AccessDenied", http.StatusForbidden, "Invalid according to Policy: Policy Condition failed."}},
	{sigv4.ErrMissingAuth, errAccessDenied},
	{sigv4.ErrUnsignedHeader, errUnsignedHeader},
	{sigv4.ErrUnsupportedAuth, errUnsupportedAuth},
	{sigv4.ErrMissingContentSHA256, errMissingContentSHA},
	{sigv4.ErrMalformedAuth, errMalformedAuth},
	{sigv4.ErrInvalidAccessKeyID, errInvalidAccessKeyID},
	{sigv4.ErrSignatureMismatch, errSignatureMismatch},
	{sigv4.ErrRequestTimeTooSkewed, errTimeTooSkewed},
	{sigv4.ErrRequestExpired, errRequestExpired},
	{sigv4.ErrRequestNotYetValid, errRequestNotYetValid},
	{sigv4.ErrMalformedPresign, errMalformedPresign},
	{sigv4.ErrContentSHA256Mismatch, errContentSHAMismatch},
	{sigv4.ErrMissingDecodedLength, errMissingContentLength},
	{sigv4.ErrMalformedChunk, errIncompleteBody},
	{sigv4.ErrChunkTooSmall, errInvalidChunkSize},
	{errBadTrailerChecksum, errInvalidChecksum},
	{sigv4.ErrNotImplemented, errNotImplemented},
	{tag.ErrInvalid, errInvalidTag},
	{tag.ErrTooMany, errTooManyTags},
	{store.ErrNoSuchBucket, errNoSuchBucket},
	{store.ErrBucketExists, errBucketAlreadyOwnedByYou},
	{store.ErrBucketNotEmpty, errBucketNotEmpty},
	{store.ErrNoSuchKey, errNoSuchKey},
	{store.ErrNoSuchVersion, errNoSuchVersion},
	{store.ErrPreconditionFailed, errPreconditionFailed},
	{store.ErrBadDigest, errBadDigest},
	{store.ErrChecksumMismatch, errChecksumMismatch},
	{store.ErrNoSuchUpload, errNoSuchUpload},
	{store.ErrInvalidPart, errInvalidPart},
	{store.ErrInvalidPartOrder, errInvalidPartOrder},
	{store.ErrEntityTooSmall, errEntityTooSmall},
	{store.ErrEntityTooLarge, errEntityTooLarge},
	{store.ErrSizeMismatch, errInvalidObjectSize},
	{store.ErrChecksumAlgorithmMismatch, errChecksumAlgorithmMismatch},
	{store.ErrChecksumTypeMismatch, errInvalidChecksum},
	{store.ErrMissingPartChecksum, errInvalidChecksum},
	// A body shorter than its Content-Length: the client's fault, not ours.
	{io.ErrUnexpectedEOF, errIncompleteBody},
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
