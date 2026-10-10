package s3api

import (
	"cmp"
	"net/http"
	"slices"
	"strings"

	"github.com/yavosh/pail/internal/store"
	"github.com/yavosh/pail/internal/tag"
)

var (
	errInvalidEncryption   = apiError{"InvalidArgument", http.StatusBadRequest, "The encryption method specified is not supported"}
	errObjectLockMissing   = apiError{"InvalidRequest", http.StatusBadRequest, "Bucket is missing Object Lock Configuration"}
	errInvalidStorageClass = apiError{"InvalidStorageClass", http.StatusBadRequest, "The storage class you specified is not valid"}
	errInvalidRedirect     = apiError{"InvalidRedirectLocation", http.StatusBadRequest, "The website redirect location must have a prefix of 'http://' or 'https://' or '/'."}
	errInvalidObjectState  = apiError{"InvalidObjectState", http.StatusForbidden, "The operation is not valid for the object's storage class"}
	errSSECUnsupported     = apiError{"AccessDenied", http.StatusForbidden, "Server Side Encryption with Customer provided key is incompatible with the encryption method specified"}
	storageClasses         = []string{"STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"}
	encryptionAlgorithms   = []string{"AES256", "aws:kms", "aws:kms:dsse"}
	archiveStorageClasses  = []string{"GLACIER", "DEEP_ARCHIVE"}
	objectLockHeaders      = []string{"x-amz-object-lock-mode", "x-amz-object-lock-retain-until-date", "x-amz-object-lock-legal-hold"}
)

// defaultEncryption is what AWS reports for an object with no requested method.
const defaultEncryption = "AES256"

// parseObjectOptions reads the encryption, storage class, and website redirect
// and tags of a write request. SSE-C and Object Lock headers are rejected, as AWS does
// on a new bucket. pail stores the values and acts only on the tags.
func parseObjectOptions(h http.Header) (store.ObjectOptions, apiError, bool) {
	var opts store.ObjectOptions
	if apiErr, ok := rejectSSEC(h); !ok {
		return opts, apiErr, false
	}
	lock := false
	for name := range h {
		lock = lock || slices.Contains(objectLockHeaders, strings.ToLower(name))
	}
	if lock {
		return opts, errObjectLockMissing, false
	}
	if v := h.Get("x-amz-server-side-encryption"); v != "" {
		if !slices.Contains(encryptionAlgorithms, v) {
			return opts, errInvalidEncryption, false
		}
		opts.ServerSideEncryption = v
	}
	if v := h.Get("x-amz-storage-class"); v != "" {
		if !slices.Contains(storageClasses, v) {
			return opts, errInvalidStorageClass, false
		}
		if v != "STANDARD" {
			opts.StorageClass = v
		}
	}
	if v := h.Get("x-amz-website-redirect-location"); v != "" {
		if !strings.HasPrefix(v, "/") && !strings.HasPrefix(v, "http://") && !strings.HasPrefix(v, "https://") {
			return opts, errInvalidRedirect, false
		}
		opts.WebsiteRedirect = v
	}
	tags, err := tag.ParseHeader(h.Get("x-amz-tagging"), tag.MaxObject)
	if err != nil {
		return opts, toAPIError(err), false
	}
	opts.Tags = tags
	return opts, apiError{}, true
}

// setEncryptionHeader reports the encryption method of an object or upload.
func setEncryptionHeader(h http.Header, method string) {
	h.Set("x-amz-server-side-encryption", cmp.Or(method, defaultEncryption))
}

// setOptionHeaders sets the headers a read or a PutObject returns for the
// stored options. The caller adds the website redirect on reads.
func setOptionHeaders(h http.Header, o store.ObjectOptions) {
	setEncryptionHeader(h, o.ServerSideEncryption)
	if o.StorageClass != "" {
		h.Set("x-amz-storage-class", o.StorageClass)
	}
}

// storageClassName is the class a listing shows.
func storageClassName(o store.ObjectOptions) string {
	return cmp.Or(o.StorageClass, "STANDARD")
}

// isArchived reports whether the object needs a restore before it can be read.
func isArchived(o store.ObjectOptions) bool {
	return slices.Contains(archiveStorageClasses, o.StorageClass)
}

// rejectSSEC refuses customer-provided encryption keys, for the request object
// or a copy source, as AWS does on a new bucket.
func rejectSSEC(h http.Header) (apiError, bool) {
	for name := range h {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-server-side-encryption-customer-") || strings.HasPrefix(lower, "x-amz-copy-source-server-side-encryption-customer-") {
			return errSSECUnsupported, false
		}
	}
	return apiError{}, true
}
