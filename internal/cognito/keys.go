package cognito

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/big"

	"github.com/yavosh/pail/internal/vfs"
)

type keyPairFile struct {
	ID     []byte `json:"id"`
	Access []byte `json:"access"`
}

type signingKeys struct {
	id, access signingKey
}

type signingKey struct {
	private *rsa.PrivateKey
	public  publicJWK
}

type publicJWK struct {
	KeyType   string `json:"kty"`
	KeyID     string `json:"kid"`
	Algorithm string `json:"alg"`
	Use       string `json:"use"`
	Modulus   string `json:"n"`
	Exponent  string `json:"e"`
}

func loadKeys(fsys privateFS, issuer string) (signingKeys, error) {
	sum := sha256.Sum256([]byte(issuer))
	name := "cognito/" + hex.EncodeToString(sum[:]) + ".json"
	var file keyPairFile
	err := vfs.ReadJSON(fsys, name, &file)
	if errors.Is(err, fs.ErrNotExist) {
		return createKeys(fsys, name)
	}
	if err != nil {
		return signingKeys{}, fmt.Errorf("read signing keys: %w", err)
	}
	id, err := parseKey(file.ID)
	if err != nil {
		return signingKeys{}, err
	}
	access, err := parseKey(file.Access)
	if err != nil {
		return signingKeys{}, err
	}
	if id.public.KeyID == access.public.KeyID {
		return signingKeys{}, errors.New("signing keys must be distinct")
	}
	return signingKeys{id: id, access: access}, nil
}

func parseKey(der []byte) (signingKey, error) {
	key, err := x509.ParsePKCS1PrivateKey(der)
	if err != nil || key.N.BitLen() < 2048 {
		return signingKey{}, errors.New("invalid persisted rsa signing key")
	}
	return describeKey(key), nil
}

func createKeys(fsys privateFS, name string) (signingKeys, error) {
	id, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return signingKeys{}, fmt.Errorf("generate id token key: %w", err)
	}
	access, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return signingKeys{}, fmt.Errorf("generate access token key: %w", err)
	}
	f, err := fsys.CreatePrivateTemp()
	if err != nil {
		return signingKeys{}, fmt.Errorf("create signing key file: %w", err)
	}
	defer func() { _ = f.Abort() }()
	pair := keyPairFile{ID: x509.MarshalPKCS1PrivateKey(id), Access: x509.MarshalPKCS1PrivateKey(access)}
	if err := json.NewEncoder(f).Encode(pair); err != nil {
		return signingKeys{}, fmt.Errorf("write signing key file: %w", err)
	}
	if err := f.Commit(name); err != nil {
		return signingKeys{}, fmt.Errorf("commit signing key file: %w", err)
	}
	return signingKeys{id: describeKey(id), access: describeKey(access)}, nil
}

func describeKey(private *rsa.PrivateKey) signingKey {
	pub := &private.PublicKey
	sum := sha256.Sum256(x509.MarshalPKCS1PublicKey(pub))
	return signingKey{
		private: private,
		public: publicJWK{
			KeyType:   "RSA",
			KeyID:     base64.RawURLEncoding.EncodeToString(sum[:]),
			Algorithm: "RS256",
			Use:       "sig",
			Modulus:   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			Exponent:  base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
		},
	}
}

func (ks signingKeys) jwksJSON() ([]byte, error) {
	return json.Marshal(struct {
		Keys []publicJWK `json:"keys"`
	}{Keys: []publicJWK{ks.id.public, ks.access.public}})
}
