package topic

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"time"

	"github.com/yavosh/pail/internal/vfs"
)

const (
	signingKeyFile  = "sns/signing-key.pem"
	signingCertFile = "sns/signing-cert.pem"
)

// signer is the key and certificate that sign notifications. pail signs with
// its own certificate, so verifiers that require an amazonaws.com host reject it.
type signer struct {
	key     *rsa.PrivateKey
	certPEM []byte
}

// loadSigner reads the persisted key and certificate. It returns nil when
// either file is missing; the first signature then creates both.
func loadSigner(fsys vfs.FS) (*signer, error) {
	keyPEM, keyErr := vfs.ReadFile(fsys, signingKeyFile)
	certPEM, certErr := vfs.ReadFile(fsys, signingCertFile)
	if errors.Is(keyErr, fs.ErrNotExist) || errors.Is(certErr, fs.ErrNotExist) {
		return nil, nil
	}
	if err := errors.Join(keyErr, certErr); err != nil {
		return nil, fmt.Errorf("read signing material: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	certBlock, _ := pem.Decode(certPEM)
	if keyBlock == nil || certBlock == nil {
		return nil, errors.New("signing material is not PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse signing key: %w", err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("signing key is not an RSA key")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse signing certificate: %w", err)
	}
	if !key.PublicKey.Equal(cert.PublicKey) {
		return nil, errors.New("signing certificate does not match the signing key")
	}
	return &signer{key: key, certPEM: certPEM}, nil
}

// newSigner creates an RSA-2048 key and a self-signed certificate valid for 10 years.
func newSigner() (*signer, []byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("generate signing key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, nil, fmt.Errorf("generate serial number: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "pail SNS"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create signing certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("encode signing key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &signer{key: key, certPEM: certPEM}, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}

// signing returns the signer and creates it on first use. Callers must not hold e.mu.
func (e *Engine) signing() (*signer, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sign != nil {
		return e.sign, nil
	}
	sg, keyPEM, err := newSigner()
	if err != nil {
		return nil, err
	}
	if err := vfs.WriteFile(e.fs, signingKeyFile, keyPEM); err != nil {
		return nil, err
	}
	if err := vfs.WriteFile(e.fs, signingCertFile, sg.certPEM); err != nil {
		return nil, err
	}
	e.sign = sg
	return sg, nil
}

// CertPEM returns the certificate that verifies notification signatures.
func (e *Engine) CertPEM(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sg, err := e.signing()
	if err != nil {
		return nil, err
	}
	return sg.certPEM, nil
}

// signString returns the base64 signature of s. SignatureVersion "2" uses SHA-256
// and any other value uses SHA-1, as SNS does.
func (e *Engine) signString(version, s string) (string, error) {
	sg, err := e.signing()
	if err != nil {
		return "", err
	}
	var sum []byte
	hash := crypto.SHA1
	if version == "2" {
		hash = crypto.SHA256
		h := sha256.Sum256([]byte(s))
		sum = h[:]
	} else {
		h := sha1.Sum([]byte(s))
		sum = h[:]
	}
	sig, err := rsa.SignPKCS1v15(rand.Reader, sg.key, hash, sum)
	if err != nil {
		return "", fmt.Errorf("sign notification: %w", err)
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}
