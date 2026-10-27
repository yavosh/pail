package kmsapi

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/yavosh/pail/internal/sigv4"
)

var modelOperations = map[string]bool{
	"ConnectCustomKeyStore": true, "CreateCustomKeyStore": true, "CreateGrant": true,
	"Decapsulate": true, "DeleteCustomKeyStore": true, "DeleteImportedKeyMaterial": true, "DeriveSharedSecret": true,
	"DescribeCustomKeyStores": true, "DisableKeyRotation": true, "DisconnectCustomKeyStore": true,
	"EnableKeyRotation": true, "Encapsulate": true, "GenerateDataKeyPair": true, "GenerateDataKeyPairWithoutPlaintext": true,
	"GenerateMac": true, "GetKeyLastUsage": true, "GetKeyRotationStatus": true, "GetParametersForImport": true,
	"GetPublicKey": true, "ImportKeyMaterial": true, "ListGrants": true, "ListKeyRotations": true,
	"ListRetirableGrants": true, "ReplicateKey": true, "RetireGrant": true, "RevokeGrant": true,
	"RotateKeyOnDemand": true, "Sign": true, "UpdateCustomKeyStore": true,
	"UpdatePrimaryRegion": true, "Verify": true, "VerifyMac": true,
}

func requestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func writeError(w http.ResponseWriter, code, message string, status int) int {
	body, _ := json.Marshal(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{code, message})
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Amzn-Errortype", code)
	w.WriteHeader(status)
	_, _ = w.Write(body)
	return status
}

func writeAuthError(w http.ResponseWriter, err error) int {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return writeError(w, "RequestEntityTooLargeException", "request body exceeds the limit", http.StatusRequestEntityTooLarge)
	}
	code := "InvalidSignatureException"
	switch {
	case errors.Is(err, sigv4.ErrMissingAuth):
		code = "MissingAuthenticationTokenException"
	case errors.Is(err, sigv4.ErrInvalidAccessKeyID):
		code = "UnrecognizedClientException"
	case errors.Is(err, sigv4.ErrMalformedAuth), errors.Is(err, sigv4.ErrUnsupportedAuth):
		code = "IncompleteSignatureException"
	case errors.Is(err, sigv4.ErrNotImplemented):
		code = "UnsupportedOperationException"
	}
	return writeError(w, code, "request authentication failed", http.StatusBadRequest)
}
