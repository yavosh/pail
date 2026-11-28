package diff

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/yavosh/pail/internal/config"
)

func TestCognitoMockSignIn(t *testing.T) {
	const host = "auth.mock.example.test"
	const issuer = "https://" + host + "/local-pool"
	const redirect = "http://localhost:3000/callback?app=demo"
	const cfg = `{"pools":[{"issuer":"` + issuer + `",
		"clients":[{"id":"demo","redirect_uris":["` + redirect + `"]}],
		"mock_provider":{"name":"Demo","users":[
			{"username":"alice","claims":{"email":"alice@example.test"}},
			{"username":"<bob>"}]}}]}`
	path := filepath.Join(t.TempDir(), "pools.json")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	tg := configuredPailTarget(t, config.Config{Addr: "127.0.0.1:0", DataDir: t.TempDir(), Region: region, CognitoConfig: path})
	endpoint := tg.scheme + "://" + host
	const verifier = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFG"
	digest := sha256.Sum256([]byte(verifier))
	params := url.Values{
		"response_type": {"code"}, "client_id": {"demo"}, "redirect_uri": {redirect},
		"code_challenge": {base64.RawURLEncoding.EncodeToString(digest[:])}, "code_challenge_method": {"S256"},
		"state": {"demo-state"}, "nonce": {"demo-nonce"}, "identity_provider": {"Demo"},
	}
	start := func(t *testing.T) (string, *http.Cookie) {
		t.Helper()
		res := mockRequest(t, tg.client, http.MethodGet, endpoint+"/oauth2/authorize?"+params.Encode(), nil, nil)
		body := string(res.body)
		match := regexp.MustCompile(`name="state" value="([^"]+)"`).FindStringSubmatch(body)
		if res.status != http.StatusOK || len(match) != 2 || len(res.cookies) != 1 ||
			!strings.Contains(body, "Choose a test account") || !strings.Contains(body, "&lt;bob&gt;") || strings.Contains(body, "<bob>") ||
			res.header.Get("Cache-Control") != "no-store" || !res.cookies[0].Secure || !res.cookies[0].HttpOnly {
			t.Fatalf("authorize status = %d, want 200 with an escaped account picker and secure transaction cookie", res.status)
		}
		return match[1], res.cookies[0]
	}
	login := endpoint + "/oauth2/mock-login"
	t.Run("sign in and redeem once", func(t *testing.T) {
		state, cookie := start(t)
		form := url.Values{"state": {state}, "username": {"alice"}}
		for _, tc := range []struct {
			name, username string
			cookie         *http.Cookie
		}{
			{"unknown user", "unknown", cookie},
			{"missing cookie", "alice", nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				attempt := url.Values{"state": {state}, "username": {tc.username}}
				res := mockRequest(t, tg.client, http.MethodPost, login, attempt, tc.cookie)
				if res.status != http.StatusBadRequest {
					t.Fatalf("sign-in status = %d, want 400", res.status)
				}
			})
		}
		res := mockRequest(t, tg.client, http.MethodPost, login, form, cookie)
		location, err := url.Parse(res.header.Get("Location"))
		if err != nil || res.status != http.StatusSeeOther || location.Scheme != "http" || location.Host != "localhost:3000" ||
			location.Path != "/callback" || location.Query().Get("app") != "demo" || location.Query().Get("state") != "demo-state" ||
			location.Query().Get("code") == "" {
			t.Fatalf("sign-in status = %d, want 303 with a code and state at the registered callback", res.status)
		}
		res = mockRequest(t, tg.client, http.MethodPost, login, form, cookie)
		if res.status != http.StatusBadRequest {
			t.Fatalf("repeated sign-in status = %d, want 400", res.status)
		}
		token := endpoint + "/oauth2/token"
		form = url.Values{
			"grant_type": {"authorization_code"}, "client_id": {"demo"}, "redirect_uri": {redirect},
			"code": {location.Query().Get("code")}, "code_verifier": {strings.Repeat("x", 43)},
		}
		res = mockRequest(t, tg.client, http.MethodPost, token, form, nil)
		if res.status != http.StatusBadRequest {
			t.Fatalf("wrong PKCE verifier status = %d, want 400", res.status)
		}
		form.Set("code_verifier", verifier)
		res = mockRequest(t, tg.client, http.MethodPost, token, form, nil)
		var tokens struct {
			IDToken     string `json:"id_token"`
			AccessToken string `json:"access_token"`
		}
		if res.status != http.StatusOK || json.Unmarshal(res.body, &tokens) != nil {
			t.Fatalf("token status = %d, want 200 with JSON tokens", res.status)
		}
		keys := cognitoJWKS(t, tg, issuer)
		id, _ := verifyCognitoJWT(t, tokens.IDToken, keys)
		access, _ := verifyCognitoJWT(t, tokens.AccessToken, keys)
		if id.Issuer != issuer || id.Audience != "demo" || id.Nonce != "demo-nonce" || id.Username != "Demo_alice" ||
			id.Email != "alice@example.test" || id.Subject == "" || id.Subject != access.Subject || access.ClientID != "demo" {
			t.Fatal("signed tokens lack the expected issuer, client, subject, or test user claims")
		}
		res = mockRequest(t, tg.client, http.MethodPost, token, form, nil)
		if res.status != http.StatusBadRequest {
			t.Fatalf("repeated exchange status = %d, want 400", res.status)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		state, cookie := start(t)
		res := mockRequest(t, tg.client, http.MethodPost, login, url.Values{"state": {state}, "cancel": {"1"}}, cookie)
		location, err := url.Parse(res.header.Get("Location"))
		if err != nil || res.status != http.StatusSeeOther || location.Query().Get("error") != "access_denied" ||
			location.Query().Get("state") != "demo-state" || location.Query().Has("code") {
			t.Fatalf("cancel status = %d, want 303 with access_denied and state", res.status)
		}
	})
}

type mockLoginResponse struct {
	status  int
	header  http.Header
	cookies []*http.Cookie
	body    []byte
}

func mockRequest(t *testing.T, client *http.Client, method, endpoint string, form url.Values, cookie *http.Cookie) mockLoginResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal("could not construct mock sign-in request")
	}
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal("mock sign-in request failed")
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal("could not read mock sign-in response")
	}
	return mockLoginResponse{res.StatusCode, res.Header, res.Cookies(), body}
}
