package cognito

import (
	"bytes"
	"crypto"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yavosh/pail/internal/vfs/localdisk"
)

const (
	coreIssuer   = "https://auth.example.test/us-east-1_local"
	corePassword = "synthetic example password"
	coreSecret   = "synthetic example app secret"
)

func coreFixture(t *testing.T) (*Engine, *localdisk.FS, string) {
	t.Helper()
	dir := t.TempDir()
	for file, content := range map[string]string{"user.txt": corePassword + "\n", "client.txt": coreSecret + "\n"} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := fileConfig{Pools: []poolConfig{{
		Issuer:  coreIssuer,
		Clients: []clientConfig{{ID: "public"}, {ID: "confidential", SecretFile: "client.txt"}},
		Users:   []userConfig{{Username: "alice", PasswordFile: "user.txt", Claims: map[string]any{"email": "alice@example.test"}}},
	}}}
	configPath := filepath.Join(dir, "pools.json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	fsys, err := localdisk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	e, err := Open(t.Context(), fsys, configPath)
	if err != nil {
		t.Fatal(err)
	}
	return e, fsys, configPath
}

func TestPasswordAuthAndKeyReuse(t *testing.T) {
	e, fsys, configPath := coreFixture(t)
	out := passwordRequest(t, e, "public", "alice", corePassword, "")
	if out.Code != http.StatusOK {
		t.Fatalf("public password auth status = %d, want 200", out.Code)
	}
	if got := out.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("password auth cache control = %q, want no-store", got)
	}
	var result struct {
		AuthenticationResult initiateAuthResult
	}
	if err := json.Unmarshal(out.Body.Bytes(), &result); err != nil {
		t.Fatal("could not decode authentication result")
	}
	tokens := result.AuthenticationResult
	if tokens.ExpiresIn != 3600 || tokens.TokenType != "Bearer" {
		t.Errorf("token metadata = %d seconds and %q, want 3600 seconds and Bearer", tokens.ExpiresIn, tokens.TokenType)
	}

	jwks := discoveryRequest(t, e, coreIssuer+jwksSuffix)
	var keySet struct{ Keys []publicJWK }
	if err := json.Unmarshal(jwks, &keySet); err != nil || len(keySet.Keys) != 2 {
		t.Fatal("jwks must contain two signing keys")
	}
	idClaims, idKid := verifyCoreToken(t, tokens.IDToken, keySet.Keys)
	accessClaims, accessKid := verifyCoreToken(t, tokens.AccessToken, keySet.Keys)
	if idKid == accessKid {
		t.Error("id and access tokens share a key")
	}
	for claim, want := range map[string]string{
		"iss": coreIssuer, "token_use": "id", "aud": "public", "cognito:username": "alice", "email": "alice@example.test",
	} {
		if got := idClaims[claim]; got != want {
			t.Errorf("id claim %q = %v, want %q", claim, got, want)
		}
	}
	for claim, want := range map[string]string{
		"iss": coreIssuer, "token_use": "access", "client_id": "public", "username": "alice", "scope": passwordScope,
	} {
		if got := accessClaims[claim]; got != want {
			t.Errorf("access claim %q = %v, want %q", claim, got, want)
		}
	}
	sub, ok := idClaims["sub"].(string)
	if !ok || sub == "" || sub != accessClaims["sub"] {
		t.Error("id and access tokens require the same nonempty subject")
	}
	if accessClaims["email"] != nil || accessClaims["aud"] != nil {
		t.Error("access token contains id-only claims")
	}
	exp, expOK := accessClaims["exp"].(float64)
	iat, iatOK := accessClaims["iat"].(float64)
	if got := exp - iat; !expOK || !iatOK || got != 3600 {
		t.Errorf("access token lifetime = %v, want 3600", got)
	}

	entries, err := fsys.ReadDir("cognito")
	if err != nil || len(entries) != 1 {
		t.Fatalf("cognito entries = %d, error = %v, want 1 entry", len(entries), err)
	}
	info, err := fsys.Stat("cognito/" + entries[0].Name())
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("key file mode = %o, want 600", got)
	}
	reopened, err := Open(t.Context(), fsys, configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(discoveryRequest(t, reopened, coreIssuer+jwksSuffix), jwks) {
		t.Error("jwks changed after reopening pool")
	}
}

func TestPasswordAuthErrors(t *testing.T) {
	e, _, _ := coreFixture(t)
	mac := hmac.New(sha256.New, []byte(coreSecret))
	_, _ = io.WriteString(mac, "aliceconfidential")
	secretHash := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	for _, tt := range []struct {
		name, client, user, password, hash, wantError string
	}{
		{name: "unknown client", client: "missing", user: "alice", password: corePassword, wantError: "ResourceNotFoundException"},
		{name: "wrong password", client: "public", user: "alice", password: "wrong", wantError: "NotAuthorizedException"},
		{name: "unknown username", client: "public", user: "missing", password: corePassword, wantError: "NotAuthorizedException"},
		{name: "missing secret hash", client: "confidential", user: "alice", password: corePassword, wantError: "NotAuthorizedException"},
		{name: "valid secret hash", client: "confidential", user: "alice", password: corePassword, hash: secretHash},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out := passwordRequest(t, e, tt.client, tt.user, tt.password, tt.hash)
			wantStatus := http.StatusBadRequest
			if tt.wantError == "" {
				wantStatus = http.StatusOK
			}
			if got := out.Header().Get("X-Amzn-Errortype"); got != tt.wantError || out.Code != wantStatus {
				t.Errorf("auth = %d and %q, want %d and %q", out.Code, got, wantStatus, tt.wantError)
			}
		})
	}
}

func TestDiscoveryMatchesIssuerAndHost(t *testing.T) {
	e, _, _ := coreFixture(t)
	body := discoveryRequest(t, e, coreIssuer+discoverySuffix)
	var meta discoveryMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Issuer != coreIssuer || meta.JWKSURI != coreIssuer+jwksSuffix || meta.TokenEndpoint != "" {
		t.Errorf("password-only metadata = %+v, want issuer and jwks without browser routes", meta)
	}
	for _, endpoint := range []string{
		"https://wrong.example.test/us-east-1_local" + discoverySuffix,
		"https://auth.example.test/us-east-1_wrong" + discoverySuffix,
		"https://auth.example.test/us-east-1_%6cocal" + discoverySuffix,
	} {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, endpoint, nil)
		if e.ServeDiscovery(httptest.NewRecorder(), r) {
			t.Errorf("unexpected discovery route for %s", endpoint)
		}
	}
}

func TestAppClientPasswordAuthSetting(t *testing.T) {
	e, _, _ := coreFixture(t)
	p := e.poolForHost("auth.example.test")
	for _, tt := range []struct {
		name         string
		redirectURIs []string
		passwordAuth *bool
		wantStatus   int
	}{
		{name: "browser default", redirectURIs: []string{"http://localhost/callback"}, wantStatus: http.StatusBadRequest},
		{name: "browser opt-in", redirectURIs: []string{"http://localhost/callback"}, passwordAuth: new(true), wantStatus: http.StatusOK},
		{name: "sdk default", wantStatus: http.StatusOK},
		{name: "sdk opt-out", passwordAuth: new(false), wantStatus: http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := clientConfig{ID: "test", RedirectURIs: tt.redirectURIs, PasswordAuth: tt.passwordAuth}
			client, err := loadClient(cfg, "", true)
			if err != nil {
				t.Fatal(err)
			}
			p.clients[cfg.ID] = client
			if got := passwordRequest(t, e, cfg.ID, "alice", corePassword, "").Code; got != tt.wantStatus {
				t.Errorf("password auth status = %d, want %d", got, tt.wantStatus)
			}
		})
	}
}

func TestBrowserClientRequiresOpenIDScope(t *testing.T) {
	cfg := clientConfig{ID: "browser", RedirectURIs: []string{"http://localhost/callback"}, Scopes: []string{"email"}}
	if _, err := loadClient(cfg, "", true); err == nil {
		t.Fatal("browser client without openid scope was accepted")
	}
}

func passwordRequest(t *testing.T, e *Engine, client, user, password, hash string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(initiateAuthRequest{
		AuthFlow: "USER_PASSWORD_AUTH", ClientID: client,
		AuthParameters: map[string]string{"USERNAME": user, "PASSWORD": password, "SECRET_HASH": hash},
	})
	if err != nil {
		t.Fatal("could not encode password auth request")
	}
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "https://auth.example.test/", bytes.NewReader(body))
	r.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService.InitiateAuth")
	out := httptest.NewRecorder()
	e.ServeAPI(out, r)
	return out
}

func discoveryRequest(t *testing.T, e *Engine, url string) []byte {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	out := httptest.NewRecorder()
	if !e.ServeDiscovery(out, r) || out.Code != http.StatusOK {
		t.Fatalf("metadata request for %s returned %d", url, out.Code)
	}
	return out.Body.Bytes()
}

func verifyCoreToken(t *testing.T, token string, keys []publicJWK) (map[string]any, string) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatal("expected a compact signed token")
	}
	decode := func(s string) []byte {
		t.Helper()
		data, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil {
			t.Fatal("token segment is not url-safe base64")
		}
		return data
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if err := json.Unmarshal(decode(parts[0]), &header); err != nil || header.Algorithm != "RS256" {
		t.Fatal("expected an rs256 token")
	}
	var public *rsa.PublicKey
	for _, key := range keys {
		if key.KeyID == header.KeyID {
			public = &rsa.PublicKey{
				N: new(big.Int).SetBytes(decode(key.Modulus)),
				E: int(new(big.Int).SetBytes(decode(key.Exponent)).Int64()),
			}
		}
	}
	if public == nil {
		t.Fatal("token uses an unpublished key")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(public, crypto.SHA256, digest[:], decode(parts[2])); err != nil {
		t.Fatal("token signature is invalid")
	}
	var claims map[string]any
	if err := json.Unmarshal(decode(parts[1]), &claims); err != nil {
		t.Fatal("could not decode token claims")
	}
	return claims, header.KeyID
}
