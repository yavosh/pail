package diff

import (
	"crypto"
	"crypto/rand"
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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/yavosh/pail/internal/config"
)

const (
	cognitoAWSHost  = "cognito-idp." + region + ".amazonaws.com"
	initiateAuth    = "AWSCognitoIdentityProviderService.InitiateAuth"
	cognitoAPIMedia = "application/x-amz-json-1.1"
)

type cognitoFixture struct {
	issuer, clientID, username, password string
}

func TestCognitoInitiateAuth(t *testing.T) {
	const name = "cognito-initiate-auth"
	var tg *target
	var fixture cognitoFixture
	if *record {
		tg, fixture = cognitoRecordingTarget(t)
	} else {
		tg, fixture = cognitoPailTarget(t)
	}
	keys := cognitoJWKS(t, tg, fixture.issuer)
	g := golden{Scenario: name}
	for _, tc := range []struct {
		name, password string
		status         int
	}{
		{"password-sign-in", fixture.password, http.StatusOK},
		{"wrong-password", rand.Text(), http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := step{
				name:    tc.name,
				method:  http.MethodPost,
				service: "cognito-idp",
				target:  initiateAuth,
				auth:    authNone,
				header:  map[string]string{"Content-Type": cognitoAPIMedia, "X-Amz-Target": initiateAuth},
				body:    cognitoAuthBody("{clientId}", "{username}", "{password}"),
			}
			req := st
			req.body = cognitoAuthBody(fixture.clientID, fixture.username, tc.password)
			resp, err := tg.do(t.Context(), req, "")
			if err != nil {
				t.Fatalf("InitiateAuth %s request failed", tc.name)
			}
			if resp.status != tc.status {
				t.Fatalf("InitiateAuth %s status = %d, want %d", tc.name, resp.status, tc.status)
			}
			body := normalizeCognitoAuth(t, resp, fixture, keys)
			g.Exchanges = append(g.Exchanges, exchange{Step: st.name, Request: describe(st), Fingerprint: fingerprint(st), Status: resp.status, Body: body})
		})
	}
	if t.Failed() {
		return
	}
	path := filepath.Join(goldenDir, name+".json")
	if *record {
		if err := writeGolden(path, g); err != nil {
			t.Fatal(err)
		}
		t.Logf("recorded %s", path)
		return
	}
	t.Run("aws-replay", func(t *testing.T) {
		want, err := readGolden(path)
		if errors.Is(err, os.ErrNotExist) {
			t.Skip("no AWS recording for Cognito InitiateAuth")
		}
		if err != nil {
			t.Fatal(err)
		}
		if want.Scenario != name || len(want.Exchanges) != len(g.Exchanges) {
			t.Fatal("recording does not match the Cognito scenario")
		}
		for i, got := range g.Exchanges {
			w := want.Exchanges[i]
			if got.Step != w.Step || got.Request != w.Request || got.Fingerprint != w.Fingerprint {
				t.Fatalf("recording step %s is stale", got.Step)
			}
			for field, diff := range compare(w, got) {
				t.Errorf("%s: %s", field, diff)
			}
		}
	})
}

func cognitoAuthBody(clientID, username, password string) string {
	body, _ := json.Marshal(map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientID,
		"AuthParameters": map[string]string{
			"USERNAME": username,
			"PASSWORD": password,
		},
	})
	return string(body)
}

func cognitoPailTarget(t *testing.T) (*target, cognitoFixture) {
	t.Helper()
	const host = "auth.example.test"
	f := cognitoFixture{issuer: "https://" + host + "/" + region + "_local", clientID: "test-client", username: "alice", password: rand.Text()}
	dir := t.TempDir()
	passwordPath := filepath.Join(dir, "password")
	if err := os.WriteFile(passwordPath, []byte(f.password), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "cognito.json")
	pool := fmt.Sprintf(`{"pools":[{"issuer":%q,"clients":[{"id":%q}],"users":[{"username":%q,"password_file":%q}]}]}`, f.issuer, f.clientID, f.username, passwordPath)
	if err := os.WriteFile(path, []byte(pool), 0o600); err != nil {
		t.Fatal(err)
	}
	tg := configuredPailTarget(t, config.Config{Addr: "127.0.0.1:0", DataDir: t.TempDir(), Region: region, CognitoConfig: path})
	tg.serviceHost = func(string) string { return host }
	return tg, f
}

func cognitoRecordingTarget(t *testing.T) (*target, cognitoFixture) {
	t.Helper()
	poolID := os.Getenv("COGNITO_DIFF_POOL_ID")
	f := cognitoFixture{clientID: os.Getenv("COGNITO_DIFF_CLIENT_ID"), username: os.Getenv("COGNITO_DIFF_USERNAME"), password: os.Getenv("COGNITO_DIFF_PASSWORD")}
	if poolID == "" && f.clientID == "" && f.username == "" && f.password == "" {
		t.Skip("Cognito recording needs a dedicated AWS fixture")
	}
	if !regexp.MustCompile(`^`+region+`_[A-Za-z0-9]+$`).MatchString(poolID) || f.clientID == "" || f.username == "" || f.password == "" {
		t.Fatal("set all four Cognito recording inputs for a dedicated us-east-1 pool")
	}
	f.issuer = "https://" + cognitoAWSHost + "/" + poolID
	return &target{
		scheme:      "https",
		serviceHost: func(string) string { return cognitoAWSHost },
		client:      &http.Client{Timeout: 30 * time.Second, CheckRedirect: noRedirect},
	}, f
}

func cognitoJWKS(t *testing.T, tg *target, issuer string) map[string]*rsa.PublicKey {
	t.Helper()
	// Replay uses loopback HTTP behind the fixture's canonical HTTPS host.
	endpoint := tg.scheme + "://" + strings.TrimPrefix(issuer, "https://") + "/.well-known/jwks.json"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal("could not construct the Cognito JWKS request")
	}
	resp, err := tg.client.Do(req)
	if err != nil {
		t.Fatal("Cognito JWKS request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Cognito JWKS status = %d, want 200", resp.StatusCode)
	}
	var jwks struct {
		Keys []struct{ Kid, Kty, Alg, N, E string }
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&jwks) != nil {
		t.Fatal("invalid Cognito JWKS JSON")
	}
	keys := map[string]*rsa.PublicKey{}
	for _, jwk := range jwks.Keys {
		if jwk.Kty != "RSA" || jwk.Alg != "RS256" {
			continue
		}
		n, nErr := base64.RawURLEncoding.DecodeString(jwk.N)
		e, eErr := base64.RawURLEncoding.DecodeString(jwk.E)
		modulus, exponent := new(big.Int).SetBytes(n), new(big.Int).SetBytes(e)
		if jwk.Kid == "" || keys[jwk.Kid] != nil || nErr != nil || eErr != nil || modulus.BitLen() < 2048 ||
			!exponent.IsInt64() || exponent.Int64() < 3 || exponent.Int64() > math.MaxInt32 {
			t.Fatal("invalid Cognito RSA key")
		}
		keys[jwk.Kid] = &rsa.PublicKey{N: modulus, E: int(exponent.Int64())}
	}
	return keys
}

type cognitoClaims struct {
	Issuer   string `json:"iss"`
	Subject  string `json:"sub"`
	Use      string `json:"token_use"`
	Audience string `json:"aud"`
	ClientID string `json:"client_id"`
	Scope    string `json:"scope"`
	Nonce    string `json:"nonce"`
	Email    string `json:"email"`
	Username string `json:"cognito:username"`
	IssuedAt int64  `json:"iat"`
	Expiry   int64  `json:"exp"`
}

func verifyCognitoJWT(t *testing.T, token string, keys map[string]*rsa.PublicKey) (cognitoClaims, *rsa.PublicKey) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatal("Cognito token is not a signed JWT")
	}
	header, hErr := base64.RawURLEncoding.DecodeString(parts[0])
	payload, pErr := base64.RawURLEncoding.DecodeString(parts[1])
	sig, sErr := base64.RawURLEncoding.DecodeString(parts[2])
	var h struct{ Alg, Kid string }
	if hErr != nil || pErr != nil || sErr != nil || json.Unmarshal(header, &h) != nil || h.Alg != "RS256" {
		t.Fatal("invalid RS256 JWT encoding")
	}
	key := keys[h.Kid]
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if key == nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig) != nil {
		t.Fatal("Cognito token signature failed JWKS verification")
	}
	var claims cognitoClaims
	if json.Unmarshal(payload, &claims) != nil {
		t.Fatal("invalid Cognito JWT claims")
	}
	return claims, key
}

func normalizeCognitoAuth(t *testing.T, resp response, f cognitoFixture, keys map[string]*rsa.PublicKey) string {
	t.Helper()
	if resp.status == http.StatusBadRequest {
		var out struct {
			Type string `json:"__type"`
		}
		const kind = "NotAuthorizedException"
		if json.Unmarshal(resp.body, &out) != nil || out.Type[strings.LastIndexByte(out.Type, '#')+1:] != kind {
			t.Fatalf("wrong password returned an unexpected error type, want %s", kind)
		}
		return kind
	}
	var out struct {
		AuthenticationResult struct {
			IDToken, AccessToken, TokenType string
			ExpiresIn                       int64
		}
	}
	if json.Unmarshal(resp.body, &out) != nil {
		t.Fatal("invalid InitiateAuth JSON")
	}
	r := out.AuthenticationResult
	if r.ExpiresIn <= 0 || r.TokenType != "Bearer" {
		t.Fatal("InitiateAuth did not return a bearer token with a positive lifetime")
	}
	id, idKey := verifyCognitoJWT(t, r.IDToken, keys)
	access, accessKey := verifyCognitoJWT(t, r.AccessToken, keys)
	if idKey.N.Cmp(accessKey.N) == 0 || id.Use != "id" || access.Use != "access" || id.Audience != f.clientID || access.ClientID != f.clientID ||
		id.Issuer != f.issuer || access.Issuer != f.issuer || id.Subject == "" || id.Subject != access.Subject {
		t.Fatal("invalid Cognito issuer, subject, signing keys, token use, or client claims")
	}
	now := time.Now().Unix()
	for _, claims := range []cognitoClaims{id, access} {
		if claims.IssuedAt <= 0 || claims.IssuedAt > now+30 || claims.Expiry <= now || claims.Expiry-claims.IssuedAt != r.ExpiresIn {
			t.Fatal("invalid Cognito token timestamps")
		}
	}
	if access.Scope != "aws.cognito.signin.user.admin" {
		t.Fatal("Cognito access token lacks the expected SDK sign-in scope")
	}
	return fmt.Sprintf(`{"AuthenticationResult":{"TokenType":"Bearer","ExpiresIn":%d,`+
		`"IdToken":{"iss":"{issuer}","sub":"{subject}","aud":"{clientId}","token_use":"id"},`+
		`"AccessToken":{"iss":"{issuer}","sub":"{subject}","client_id":"{clientId}","token_use":"access","scope":"aws.cognito.signin.user.admin"}}}`, r.ExpiresIn)
}
