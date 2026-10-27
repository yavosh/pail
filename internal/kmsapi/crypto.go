package kmsapi

import (
	"encoding/json"
	"net/http"

	"github.com/yavosh/pail/internal/kms"
)

type cryptoInput struct {
	KeyID               string            `json:"KeyId"`
	Plaintext           []byte            `json:"Plaintext"`
	CiphertextBlob      []byte            `json:"CiphertextBlob"`
	EncryptionAlgorithm string            `json:"EncryptionAlgorithm"`
	EncryptionContext   map[string]string `json:"EncryptionContext"`
	GrantTokens         []string          `json:"GrantTokens"`
	Recipient           json.RawMessage   `json:"Recipient"`
	DryRun              bool              `json:"DryRun"`
}

func unsupportedOptions(recipient json.RawMessage, tokens []string) error {
	if len(recipient) > 0 || len(tokens) > 0 {
		return &kms.Error{Code: "UnsupportedOperationException", Message: "attestation and grants are not supported"}
	}
	return nil
}

func dryRun(result any, err error, dry bool) (any, error) {
	if err != nil {
		return nil, err
	}
	if dry {
		return nil, &kms.Error{Code: "DryRunOperationException", Message: "the request would succeed"}
	}
	return result, nil
}

func (h *handler) crypto(r *http.Request, in cryptoInput) (any, error) {
	if err := unsupportedOptions(in.Recipient, in.GrantTokens); err != nil {
		return nil, err
	}
	input := kms.CryptoInput{KeyID: in.KeyID, Algorithm: in.EncryptionAlgorithm, Context: in.EncryptionContext, Data: in.Plaintext}
	if operationName(r) == "Encrypt" {
		result, err := h.keys.Encrypt(r.Context(), input)
		return dryRun(result, err, in.DryRun)
	}
	if len(in.CiphertextBlob) < 1 || len(in.CiphertextBlob) > 6144 {
		return nil, validation("ciphertext must contain 1 to 6144 bytes")
	}
	input.Data = in.CiphertextBlob
	result, err := h.keys.Decrypt(r.Context(), input)
	return dryRun(result, err, in.DryRun)
}

type reEncryptInput struct {
	CiphertextBlob                 []byte            `json:"CiphertextBlob"`
	SourceKeyID                    string            `json:"SourceKeyId"`
	DestinationKeyID               string            `json:"DestinationKeyId"`
	SourceEncryptionAlgorithm      string            `json:"SourceEncryptionAlgorithm"`
	DestinationEncryptionAlgorithm string            `json:"DestinationEncryptionAlgorithm"`
	SourceEncryptionContext        map[string]string `json:"SourceEncryptionContext"`
	DestinationEncryptionContext   map[string]string `json:"DestinationEncryptionContext"`
	GrantTokens                    []string          `json:"GrantTokens"`
	DryRun                         bool              `json:"DryRun"`
}

func (h *handler) reEncrypt(r *http.Request, in reEncryptInput) (any, error) {
	if err := unsupportedOptions(nil, in.GrantTokens); err != nil {
		return nil, err
	}
	if len(in.CiphertextBlob) < 1 || len(in.CiphertextBlob) > 6144 {
		return nil, validation("ciphertext must contain 1 to 6144 bytes")
	}
	result, source, err := h.keys.ReEncrypt(r.Context(),
		kms.CryptoInput{KeyID: in.SourceKeyID, Data: in.CiphertextBlob, Context: in.SourceEncryptionContext, Algorithm: in.SourceEncryptionAlgorithm},
		kms.CryptoInput{KeyID: in.DestinationKeyID, Context: in.DestinationEncryptionContext, Algorithm: in.DestinationEncryptionAlgorithm})
	return dryRun(struct {
		CiphertextBlob                 []byte `json:"CiphertextBlob"`
		KeyID                          string `json:"KeyId"`
		SourceKeyID                    string `json:"SourceKeyId"`
		SourceEncryptionAlgorithm      string `json:"SourceEncryptionAlgorithm"`
		DestinationEncryptionAlgorithm string `json:"DestinationEncryptionAlgorithm"`
		SourceKeyMaterialID            string `json:"SourceKeyMaterialId"`
		DestinationKeyMaterialID       string `json:"DestinationKeyMaterialId"`
		SourceKeyOrigin                string `json:"SourceKeyOrigin"`
		DestinationKeyOrigin           string `json:"DestinationKeyOrigin"`
	}{result.CiphertextBlob, result.KeyID, source, "SYMMETRIC_DEFAULT", "SYMMETRIC_DEFAULT", result.SourceKeyMaterialID, result.KeyMaterialID, "AWS_KMS", "AWS_KMS"}, err, in.DryRun)
}

type dataKeyInput struct {
	KeyID             string            `json:"KeyId"`
	KeySpec           string            `json:"KeySpec"`
	NumberOfBytes     *int              `json:"NumberOfBytes"`
	EncryptionContext map[string]string `json:"EncryptionContext"`
	Recipient         json.RawMessage   `json:"Recipient"`
	GrantTokens       []string          `json:"GrantTokens"`
	DryRun            bool              `json:"DryRun"`
}

func (h *handler) dataKey(r *http.Request, in dataKeyInput) (any, error) {
	if err := unsupportedOptions(in.Recipient, in.GrantTokens); err != nil {
		return nil, err
	}
	if (in.KeySpec != "") == (in.NumberOfBytes != nil) {
		return nil, validation("specify either key spec or number of bytes")
	}
	size := 0
	if in.NumberOfBytes != nil {
		size = *in.NumberOfBytes
	}
	if in.KeySpec != "" {
		switch in.KeySpec {
		case "AES_128":
			size = 16
		case "AES_256":
			size = 32
		default:
			return nil, validation("key spec must be AES_128 or AES_256")
		}
	}
	result, err := h.keys.GenerateDataKey(r.Context(), kms.CryptoInput{KeyID: in.KeyID, Context: in.EncryptionContext}, size, operationName(r) == "GenerateDataKey")
	return dryRun(result, err, in.DryRun)
}

type randomInput struct {
	NumberOfBytes    int             `json:"NumberOfBytes"`
	CustomKeyStoreID string          `json:"CustomKeyStoreId"`
	Recipient        json.RawMessage `json:"Recipient"`
}

func (*handler) generateRandom(r *http.Request, in randomInput) (any, error) {
	if err := unsupportedOptions(in.Recipient, nil); err != nil {
		return nil, err
	}
	if in.CustomKeyStoreID != "" {
		return nil, &kms.Error{Code: "UnsupportedOperationException", Message: "custom key stores are not supported"}
	}
	data, err := kms.GenerateRandom(r.Context(), in.NumberOfBytes)
	return struct{ Plaintext []byte }{data}, err
}
