package cognito

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const tokenLifetime = time.Hour

// User is the authenticated subject and its mapped profile.
type User struct {
	Subject, Username string
	Claims            map[string]any
}

// Tokens contains the signed tokens returned for a successful authentication.
type Tokens struct {
	IDToken, AccessToken string
	ExpiresIn            int
	TokenType            string
}

func isProfileClaim(name string) bool {
	switch name {
	case "", "iss", "sub", "aud", "exp", "iat", "nbf", "jti", "azp", "nonce", "auth_time",
		"at_hash", "c_hash", "client_id", "token_use", "username", "cognito:username",
		"scope", "origin_jti", "event_id", "version", "cognito:roles", "cognito:preferred_role":
		return false
	default:
		return true
	}
}

func (p *pool) issue(clientID string, user User, scopes, nonce string) (Tokens, error) {
	now := time.Now()
	claims := func(kind string) map[string]any {
		return map[string]any{
			"iss": p.issuer, "sub": user.Subject, "token_use": kind,
			"iat": now.Unix(), "auth_time": now.Unix(), "exp": now.Add(tokenLifetime).Unix(),
			"jti": rand.Text(),
		}
	}
	idClaims := claims("id")
	for name, value := range user.Claims {
		if isProfileClaim(name) {
			idClaims[name] = value
		}
	}
	idClaims["aud"] = clientID
	idClaims["cognito:username"] = user.Username
	if nonce != "" {
		idClaims["nonce"] = nonce
	}
	id, err := p.keys.id.sign(idClaims)
	if err != nil {
		return Tokens{}, fmt.Errorf("sign id token: %w", err)
	}
	accessClaims := claims("access")
	accessClaims["client_id"] = clientID
	accessClaims["username"] = user.Username
	accessClaims["scope"] = scopes
	if groups, ok := user.Claims["cognito:groups"]; ok {
		accessClaims["cognito:groups"] = groups
	}
	access, err := p.keys.access.sign(accessClaims)
	if err != nil {
		return Tokens{}, fmt.Errorf("sign access token: %w", err)
	}
	return Tokens{IDToken: id, AccessToken: access, ExpiresIn: int(tokenLifetime.Seconds()), TokenType: "Bearer"}, nil
}

func (k signingKey) sign(claims map[string]any) (string, error) {
	h, err := json.Marshal(struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
		Type      string `json:"typ"`
	}{"RS256", k.public.KeyID, "JWT"})
	if err != nil {
		return "", err
	}
	p, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	sum := sha256.Sum256([]byte(payload))
	sig, err := rsa.SignPKCS1v15(rand.Reader, k.private, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return payload + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// subjectID gives local and federated identities separate UUID namespaces.
func subjectID(namespace, issuer, identity string) string {
	id := sha256.Sum256([]byte(namespace + "\x00" + issuer + "\x00" + identity))
	id[6] = id[6]&0x0f | 0x80
	id[8] = id[8]&0x3f | 0x80
	s := hex.EncodeToString(id[:16])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
