package cognito

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const (
	oidcTimeout       = 10 * time.Second
	oidcResponseLimit = 256 << 10
)

var defaultOIDCClient = &http.Client{
	Timeout:       oidcTimeout,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

type oidcMetadata struct {
	authorizeURL url.URL
	tokenURL     string
	jwksURL      string
	authMethod   string
}

func (b *browserState) discover(ctx context.Context, provider *provider) (oidcMetadata, error) {
	var discovery struct {
		Issuer       string   `json:"issuer"`
		AuthorizeURL string   `json:"authorization_endpoint"`
		TokenURL     string   `json:"token_endpoint"`
		JWKSURL      string   `json:"jwks_uri"`
		AuthMethods  []string `json:"token_endpoint_auth_methods_supported"`
	}
	endpoint := strings.TrimSuffix(provider.issuer, "/") + "/.well-known/openid-configuration"
	if err := b.fetchOIDC(ctx, http.MethodGet, endpoint, nil, "", "", &discovery); err != nil {
		return oidcMetadata{}, fmt.Errorf("discover provider: %w", err)
	}
	if discovery.Issuer != provider.issuer {
		return oidcMetadata{}, errors.New("provider returned a different issuer")
	}
	authorizeURL, err := oidcEndpoint(discovery.AuthorizeURL)
	if err != nil {
		return oidcMetadata{}, err
	}
	if _, err := oidcEndpoint(discovery.TokenURL); err != nil {
		return oidcMetadata{}, err
	}
	if _, err := oidcEndpoint(discovery.JWKSURL); err != nil {
		return oidcMetadata{}, err
	}
	method, err := oidcAuthMethod(discovery.AuthMethods, provider.clientSecret != "")
	if err != nil {
		return oidcMetadata{}, err
	}
	return oidcMetadata{
		authorizeURL: *authorizeURL, tokenURL: discovery.TokenURL, jwksURL: discovery.JWKSURL, authMethod: method,
	}, nil
}

func oidcEndpoint(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || endpoint.Scheme != "https" || endpoint.Hostname() == "" ||
		endpoint.User != nil || strings.Contains(raw, "#") {
		return nil, errors.New("provider endpoint must be an https url")
	}
	return endpoint, nil
}

func oidcAuthMethod(methods []string, confidential bool) (string, error) {
	if !confidential && (len(methods) == 0 || slices.Contains(methods, "none")) {
		return "none", nil
	}
	if confidential && (len(methods) == 0 || slices.Contains(methods, "client_secret_basic")) {
		return "client_secret_basic", nil
	}
	if confidential && slices.Contains(methods, "client_secret_post") {
		return "client_secret_post", nil
	}
	return "", errors.New("no supported provider client authentication method")
}

func (tx browserTransaction) providerURL(p *pool, state string) string {
	endpoint := tx.metadata.authorizeURL
	params := endpoint.Query()
	params.Set("response_type", "code")
	params.Set("response_mode", "query")
	params.Set("client_id", p.provider.clientID)
	params.Set("redirect_uri", p.origin+idpResponsePath)
	params.Set("scope", strings.Join(p.provider.scopes, " "))
	params.Set("state", state)
	params.Set("nonce", tx.upstreamNonce)
	params.Set("code_challenge", challengeFor(tx.verifier))
	params.Set("code_challenge_method", "S256")
	endpoint.RawQuery = params.Encode()
	return endpoint.String()
}

func (b *browserState) exchange(ctx context.Context, p *pool, tx browserTransaction, code string) (User, error) {
	ctx, cancel := context.WithTimeout(ctx, oidcTimeout)
	defer cancel()
	params := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {p.origin + idpResponsePath},
		"code_verifier": {tx.verifier},
	}
	var username, password string
	switch tx.metadata.authMethod {
	case "client_secret_basic":
		username = url.QueryEscape(p.provider.clientID)
		password = url.QueryEscape(p.provider.clientSecret)
	case "client_secret_post":
		params.Set("client_id", p.provider.clientID)
		params.Set("client_secret", p.provider.clientSecret)
	default:
		params.Set("client_id", p.provider.clientID)
	}
	var tokens struct {
		IDToken string `json:"id_token"`
	}
	if err := b.fetchOIDC(ctx, http.MethodPost, tx.metadata.tokenURL, params, username, password, &tokens); err != nil {
		return User{}, fmt.Errorf("exchange provider code: %w", err)
	}
	var keys struct {
		Keys []oidcKey `json:"keys"`
	}
	if err := b.fetchOIDC(ctx, http.MethodGet, tx.metadata.jwksURL, nil, "", "", &keys); err != nil {
		return User{}, fmt.Errorf("fetch provider jwks: %w", err)
	}
	claims, subject, err := verifyOIDCToken(tokens.IDToken, keys.Keys, p.provider, tx.upstreamNonce, time.Now())
	if err != nil {
		return User{}, err
	}
	mapped := make(map[string]any, len(p.provider.claimMap))
	for target, source := range p.provider.claimMap {
		if value, ok := claims[source]; ok {
			mapped[target] = value
		}
	}
	return User{
		Subject:  subjectID("federated", p.issuer, p.provider.issuer+"\x00"+subject),
		Username: p.provider.name + "_" + subject,
		Claims:   mapped,
	}, nil
}

func (b *browserState) fetchOIDC(ctx context.Context, method, endpoint string, params url.Values, username, password string, dst any) error {
	var body io.Reader
	if params != nil {
		body = strings.NewReader(params.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return fmt.Errorf("create provider request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if params != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if username != "" {
		req.SetBasicAuth(username, password)
	}
	client := b.client
	if client == nil {
		client = defaultOIDCClient
	}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send provider request: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("provider returned status %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, oidcResponseLimit+1))
	if err != nil {
		return fmt.Errorf("read provider response: %w", err)
	}
	if len(data) > oidcResponseLimit {
		return errors.New("provider response is too large")
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("decode provider response: %w", err)
	}
	return nil
}

type oidcKey struct {
	ID         string   `json:"kid"`
	Type       string   `json:"kty"`
	Use        string   `json:"use"`
	Algorithm  string   `json:"alg"`
	Operations []string `json:"key_ops"`
	Modulus    string   `json:"n"`
	Exponent   string   `json:"e"`
}

func (k oidcKey) publicRSAKey() (*rsa.PublicKey, error) {
	modulus, modulusErr := base64.RawURLEncoding.Strict().DecodeString(k.Modulus)
	exponent, exponentErr := base64.RawURLEncoding.Strict().DecodeString(k.Exponent)
	if modulusErr != nil || exponentErr != nil || len(modulus) > 1024 || len(exponent) > 4 {
		return nil, errors.New("invalid provider rsa key")
	}
	number := new(big.Int).SetBytes(modulus)
	power := new(big.Int).SetBytes(exponent).Int64()
	if number.BitLen() < 2048 || number.Bit(0) == 0 || power < 3 || power > math.MaxInt32 || power%2 != 1 {
		return nil, errors.New("invalid provider rsa parameters")
	}
	return &rsa.PublicKey{N: number, E: int(power)}, nil
}

func verifyOIDCToken(token string, keys []oidcKey, provider *provider, nonce string, now time.Time) (map[string]json.RawMessage, string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, "", errors.New("invalid provider id token")
	}
	header, err := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	if err != nil {
		return nil, "", fmt.Errorf("decode provider id token header: %w", err)
	}
	var protected struct {
		Algorithm string          `json:"alg"`
		KeyID     string          `json:"kid"`
		Critical  json.RawMessage `json:"crit"`
		Base64    json.RawMessage `json:"b64"`
	}
	if err := json.Unmarshal(header, &protected); err != nil || protected.Algorithm != "RS256" ||
		protected.KeyID == "" || protected.Critical != nil || protected.Base64 != nil {
		return nil, "", errors.New("unsupported provider id token header")
	}
	key, err := findOIDCKey(keys, protected.KeyID)
	if err != nil {
		return nil, "", err
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(parts[2])
	if err != nil {
		return nil, "", fmt.Errorf("decode provider id token signature: %w", err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature); err != nil {
		return nil, "", fmt.Errorf("verify provider id token: %w", err)
	}
	payload, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if err != nil {
		return nil, "", fmt.Errorf("decode provider id token claims: %w", err)
	}
	return providerClaims(payload, provider, nonce, now)
}

func findOIDCKey(keys []oidcKey, id string) (*rsa.PublicKey, error) {
	var found *oidcKey
	for _, key := range keys {
		if key.ID != id || key.Type != "RSA" || (key.Use != "" && key.Use != "sig") ||
			(key.Algorithm != "" && key.Algorithm != "RS256") ||
			(len(key.Operations) != 0 && !slices.Contains(key.Operations, "verify")) {
			continue
		}
		if found != nil {
			return nil, errors.New("ambiguous provider key id")
		}
		found = &key
	}
	if found == nil {
		return nil, errors.New("unknown provider key id")
	}
	return found.publicRSAKey()
}

func providerClaims(payload []byte, provider *provider, nonce string, now time.Time) (map[string]json.RawMessage, string, error) {
	var claims struct {
		Issuer     string          `json:"iss"`
		Subject    string          `json:"sub"`
		Audience   json.RawMessage `json:"aud"`
		Party      string          `json:"azp"`
		Nonce      string          `json:"nonce"`
		Expires    int64           `json:"exp"`
		Issued     int64           `json:"iat"`
		ValidAfter int64           `json:"nbf"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, "", fmt.Errorf("decode provider claims: %w", err)
	}
	audience := oidcAudience(claims.Audience)
	if claims.Issuer != provider.issuer || claims.Subject == "" || len(claims.Subject) > 255 || claims.Nonce != nonce ||
		claims.Expires <= now.Unix() || claims.Issued <= 0 || claims.Issued > now.Add(time.Minute).Unix() ||
		claims.ValidAfter > now.Unix() || len(audience) != 1 || audience[0] != provider.clientID ||
		(claims.Party != "" && claims.Party != provider.clientID) {
		return nil, "", errors.New("provider id token claims do not match the sign-in")
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(payload, &values); err != nil {
		return nil, "", fmt.Errorf("decode provider profile claims: %w", err)
	}
	return values, claims.Subject, nil
}

func oidcAudience(value json.RawMessage) []string {
	var one string
	if err := json.Unmarshal(value, &one); err == nil {
		return []string{one}
	}
	var many []string
	if err := json.Unmarshal(value, &many); err != nil {
		return nil
	}
	return many
}
