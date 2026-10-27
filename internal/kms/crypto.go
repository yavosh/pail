package kms

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"unicode/utf8"
)

const blobPrefix = "pail-kms\x01"

// CryptoInput describes encryption, decryption, or data key generation.
type CryptoInput struct {
	KeyID, Algorithm string
	Data             []byte
	Context          map[string]string
}

// CryptoResult holds a key ARN and the bytes produced by an operation.
type CryptoResult struct {
	KeyID               string `json:"KeyId"`
	CiphertextBlob      []byte `json:"CiphertextBlob,omitempty"`
	Plaintext           []byte `json:"Plaintext,omitempty"`
	Algorithm           string `json:"EncryptionAlgorithm,omitempty"`
	KeyMaterialID       string `json:"KeyMaterialId,omitempty"`
	KeyOrigin           string `json:"KeyOrigin,omitempty"`
	SourceKeyMaterialID string `json:"-"`
}

func validateCrypto(in CryptoInput) error {
	if in.Algorithm != "" && in.Algorithm != "SYMMETRIC_DEFAULT" {
		return fail("InvalidKeyUsageException", "algorithm is incompatible with the key")
	}
	size := 0
	for k, v := range in.Context {
		if k == "" || !utf8.ValidString(k) || !utf8.ValidString(v) {
			return invalid("encryption context requires nonempty keys and valid unicode strings")
		}
		size += len(k) + len(v)
	}
	if size > 8192 {
		return invalid("encryption context exceeds 8192 bytes")
	}
	return nil
}

// Encrypt encrypts at most 4096 bytes and authenticates the encryption context.
func (e *Engine) Encrypt(ctx context.Context, in CryptoInput) (CryptoResult, error) {
	if err := validateCrypto(in); err != nil {
		return CryptoResult{}, err
	}
	if len(in.Data) < 1 || len(in.Data) > 4096 {
		return CryptoResult{}, invalid("plaintext must contain 1 to 4096 bytes")
	}
	if err := e.lock(ctx); err != nil {
		return CryptoResult{}, err
	}
	defer e.mu.Unlock()
	k, err := e.resolve(in.KeyID, true)
	if err != nil {
		return CryptoResult{}, err
	}
	if err := active(k); err != nil {
		return CryptoResult{}, err
	}
	return e.encrypt(k, in.Data, in.Context)
}

func (e *Engine) encrypt(k key, plaintext []byte, context map[string]string) (CryptoResult, error) {
	block, err := aes.NewCipher(k.Material)
	if err != nil {
		return CryptoResult{}, err
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return CryptoResult{}, err
	}
	header := []byte(blobPrefix + k.ID)
	return CryptoResult{
		KeyID: e.arn("key/" + k.ID), Algorithm: "SYMMETRIC_DEFAULT",
		CiphertextBlob: aead.Seal(header, nil, plaintext, e.aad(header, context)),
	}, nil
}

// aad binds the format, key ID, region, account, and canonical context to the ciphertext.
func (e *Engine) aad(header []byte, context map[string]string) []byte {
	if len(context) == 0 {
		context = map[string]string{}
	}
	encoded, _ := json.Marshal(context)
	aad := bytes.Clone(header)
	aad = append(aad, []byte(e.arn(""))...)
	return append(aad, encoded...)
}

// Decrypt resolves the embedded key and requires an exact encryption context match.
func (e *Engine) Decrypt(ctx context.Context, in CryptoInput) (CryptoResult, error) {
	if err := validateCrypto(in); err != nil {
		return CryptoResult{}, err
	}
	if err := e.lock(ctx); err != nil {
		return CryptoResult{}, err
	}
	defer e.mu.Unlock()
	return e.decrypt(in)
}

func (e *Engine) decrypt(in CryptoInput) (CryptoResult, error) {
	const headerSize = len(blobPrefix) + 36
	if len(in.Data) < headerSize+12+16 || len(in.Data) > 6144 || !bytes.HasPrefix(in.Data, []byte(blobPrefix)) {
		return CryptoResult{}, fail("InvalidCiphertextException", "invalid ciphertext")
	}
	id := string(in.Data[len(blobPrefix):headerSize])
	if !keyIDPattern.MatchString(id) {
		return CryptoResult{}, fail("InvalidCiphertextException", "invalid ciphertext")
	}
	k, err := e.resolve(id, false)
	if err != nil {
		return CryptoResult{}, err
	}
	if in.KeyID != "" {
		expected, err := e.resolve(in.KeyID, true)
		if err != nil {
			return CryptoResult{}, err
		}
		if expected.ID != k.ID {
			return CryptoResult{}, fail("IncorrectKeyException", "ciphertext belongs to a different key")
		}
	}
	if err := active(k); err != nil {
		return CryptoResult{}, err
	}
	block, err := aes.NewCipher(k.Material)
	if err != nil {
		return CryptoResult{}, err
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return CryptoResult{}, err
	}
	plaintext, err := aead.Open(nil, nil, in.Data[headerSize:], e.aad(in.Data[:headerSize], in.Context))
	if err != nil {
		return CryptoResult{}, fail("InvalidCiphertextException", "invalid ciphertext")
	}
	return CryptoResult{KeyID: e.arn("key/" + k.ID), Plaintext: plaintext, Algorithm: "SYMMETRIC_DEFAULT", KeyMaterialID: materialID(k), KeyOrigin: "AWS_KMS"}, nil
}

// ReEncrypt decrypts and encrypts under one lock, including both key state checks.
func (e *Engine) ReEncrypt(ctx context.Context, source, destination CryptoInput) (CryptoResult, string, error) {
	if err := validateCrypto(source); err != nil {
		return CryptoResult{}, "", err
	}
	if err := validateCrypto(destination); err != nil {
		return CryptoResult{}, "", err
	}
	if err := e.lock(ctx); err != nil {
		return CryptoResult{}, "", err
	}
	defer e.mu.Unlock()
	decrypted, err := e.decrypt(source)
	if err != nil {
		return CryptoResult{}, "", err
	}
	k, err := e.resolve(destination.KeyID, true)
	if err != nil {
		return CryptoResult{}, "", err
	}
	if err := active(k); err != nil {
		return CryptoResult{}, "", err
	}
	result, err := e.encrypt(k, decrypted.Plaintext, destination.Context)
	result.KeyMaterialID = materialID(k)
	result.SourceKeyMaterialID = decrypted.KeyMaterialID
	return result, decrypted.KeyID, err
}

// GenerateDataKey returns random bytes and their encrypted copy.
func (e *Engine) GenerateDataKey(ctx context.Context, in CryptoInput, size int, plaintext bool) (CryptoResult, error) {
	if size < 1 || size > 1024 {
		return CryptoResult{}, invalid("data key length must be between 1 and 1024 bytes")
	}
	if err := validateCrypto(in); err != nil {
		return CryptoResult{}, err
	}
	if err := e.lock(ctx); err != nil {
		return CryptoResult{}, err
	}
	defer e.mu.Unlock()
	k, err := e.resolve(in.KeyID, true)
	if err != nil {
		return CryptoResult{}, err
	}
	if err := active(k); err != nil {
		return CryptoResult{}, err
	}
	data := make([]byte, size)
	_, _ = rand.Read(data)
	result, err := e.encrypt(k, data, in.Context)
	if err != nil {
		return CryptoResult{}, err
	}
	result.Algorithm = ""
	result.KeyMaterialID = materialID(k)
	result.KeyOrigin = "AWS_KMS"
	if plaintext {
		result.Plaintext = data
	}
	return result, nil
}

// GenerateRandom returns a cryptographically secure byte string.
func GenerateRandom(ctx context.Context, size int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if size < 1 || size > 1024 {
		return nil, invalid("random length must be between 1 and 1024 bytes")
	}
	data := make([]byte, size)
	_, _ = rand.Read(data)
	return data, nil
}
