package cognito

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	browserTestHost     = "auth.local.test"
	browserTestOrigin   = "https://hub.local.test"
	browserTestCallback = browserTestOrigin + "/callback?app=hub"
	browserTestVerifier = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func TestBrowserSignIn(t *testing.T) {
	id := newBrowserSigningKey(t)
	access := newBrowserSigningKey(t)
	upstream := testOIDCProvider(t, newBrowserSigningKey(t))

	issuer := "https://" + browserTestHost + "/local-pool"
	p := &pool{
		issuer: issuer,
		origin: "https://" + browserTestHost,
		clients: map[string]ClientConfig{
			"hub": {
				ID: "hub", RedirectURIs: []string{browserTestCallback},
				Scopes: []string{"openid", "email"}, Origins: []string{browserTestOrigin},
			},
		},
		provider: &provider{
			issuer: upstream.URL, name: "LocalOIDC", clientID: "registered-upstream-client",
			scopes: []string{"openid", "profile"}, claimMap: map[string]string{"email": "preferred_email"},
		},
		keys: signingKeys{id: id, access: access},
	}
	e := &Engine{
		pools:   map[string]*pool{browserTestHost: p},
		browser: browserState{client: localBrowserClient(upstream)},
	}
	local := httptest.NewTLSServer(http.HandlerFunc(e.ServeBrowser))
	t.Cleanup(local.Close)
	client := localBrowserClient(local)

	params := url.Values{
		"client_id": {"hub"}, "redirect_uri": {browserTestCallback}, "response_type": {"code"},
		"state": {"hub-state"}, "nonce": {"hub-nonce"}, "scope": {"openid email"}, "identity_provider": {"LocalOIDC"},
		"code_challenge": {challengeFor(browserTestVerifier)}, "code_challenge_method": {"S256"},
	}
	params.Set("redirect_uri", browserTestOrigin+"/unregistered")
	rejected := sendBrowserRequest(t, client, browserTestHost, http.MethodGet, local.URL+browserAuthorizePath+"?"+params.Encode(), nil, "", nil)
	if rejected.status != http.StatusBadRequest || rejected.header.Get("Location") != "" {
		t.Fatalf("authorize unregistered callback = %d with location %t, want 400 without location", rejected.status, rejected.header.Get("Location") != "")
	}
	params.Set("redirect_uri", browserTestCallback)
	first := sendBrowserRequest(t, client, browserTestHost, http.MethodGet, local.URL+browserAuthorizePath+"?"+params.Encode(), nil, "", nil)
	if first.status != http.StatusFound || len(first.cookies) != 1 {
		t.Fatalf("authorize = %d with %d cookies, want 302 with 1 cookie", first.status, len(first.cookies))
	}
	cookie := first.cookies[0]
	if !strings.HasPrefix(cookie.Name, browserCookiePrefix) || cookie.Path != "/" || !cookie.Secure || !cookie.HttpOnly ||
		cookie.SameSite != http.SameSiteLaxMode {
		t.Fatal("authorize cookie lacks secure transaction attributes")
	}
	authorize := browserLocation(t, first)
	q := authorize.Query()
	if authorize.Host != strings.TrimPrefix(upstream.URL, "https://") || q.Get("state") == "" || q.Get("state") == "hub-state" ||
		q.Get("nonce") == "" || q.Get("nonce") == "hub-nonce" || q.Get("code_challenge") == params.Get("code_challenge") {
		t.Fatal("upstream authorization request did not create a separate transaction")
	}

	login := sendBrowserRequest(t, e.browser.client, authorize.Host, http.MethodGet, authorize.String(), nil, "", nil)
	returnURL := browserLocation(t, login)
	callback := local.URL + returnURL.RequestURI()
	wrongCookie := *cookie
	wrongCookie.Value = "wrong-cookie"
	if got := sendBrowserRequest(t, client, browserTestHost, http.MethodGet, callback, nil, "", &wrongCookie).status; got != http.StatusBadRequest {
		t.Fatalf("wrong callback cookie status = %d, want %d", got, http.StatusBadRequest)
	}
	signedIn := sendBrowserRequest(t, client, browserTestHost, http.MethodGet, callback, nil, "", cookie)
	if signedIn.status != http.StatusFound || len(signedIn.cookies) != 1 || signedIn.cookies[0].MaxAge != -1 {
		t.Fatalf("callback = %d with %d cookies, want 302 with 1 cleared cookie", signedIn.status, len(signedIn.cookies))
	}
	returnToHub := browserLocation(t, signedIn)
	result := returnToHub.Query()
	if returnToHub.Host != "hub.local.test" || returnToHub.Path != "/callback" || result.Get("state") != "hub-state" ||
		result.Get("app") != "hub" || result.Get("code") == "" {
		t.Fatal("callback did not return the code and state to the registered Hub callback")
	}
	if got := sendBrowserRequest(t, client, browserTestHost, http.MethodGet, callback, nil, "", cookie).status; got != http.StatusBadRequest {
		t.Fatalf("repeated callback status = %d, want %d", got, http.StatusBadRequest)
	}

	form := url.Values{
		"grant_type": {"authorization_code"}, "client_id": {"hub"}, "code": {result.Get("code")},
		"redirect_uri": {browserTestCallback}, "code_verifier": {strings.Repeat("x", 43)},
	}
	wrongVerifier := sendBrowserRequest(t, client, browserTestHost, http.MethodPost, local.URL+browserTokenPath, form, browserTestOrigin, nil)
	if wrongVerifier.status != http.StatusBadRequest || !strings.Contains(string(wrongVerifier.body), "invalid_grant") {
		t.Fatalf("wrong PKCE verifier status = %d, want 400 invalid_grant", wrongVerifier.status)
	}
	form.Set("code_verifier", browserTestVerifier)
	unknownOrigin := sendBrowserRequest(t, client, browserTestHost, http.MethodPost, local.URL+browserTokenPath, form, "https://unknown.local.test", nil)
	if unknownOrigin.status != http.StatusForbidden || unknownOrigin.header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("unknown origin status = %d, want 403 without CORS access", unknownOrigin.status)
	}
	exchanged := sendBrowserRequest(t, client, browserTestHost, http.MethodPost, local.URL+browserTokenPath, form, browserTestOrigin, nil)
	if exchanged.status != http.StatusOK || exchanged.header.Get("Access-Control-Allow-Origin") != browserTestOrigin ||
		exchanged.header.Get("Cache-Control") != "no-store" {
		t.Fatalf("token exchange status = %d, want 200 with allowed origin and no-store", exchanged.status)
	}
	var tokens struct {
		IDToken     string `json:"id_token"`
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(exchanged.body, &tokens); err != nil || tokens.TokenType != "Bearer" || tokens.ExpiresIn <= 0 {
		t.Fatal("token exchange did not return a bearer token with an expiration")
	}
	idClaims := browserTokenClaims(t, tokens.IDToken, &id.private.PublicKey)
	accessClaims := browserTokenClaims(t, tokens.AccessToken, &access.private.PublicKey)
	if idClaims["iss"] != issuer || idClaims["nonce"] != "hub-nonce" || idClaims["aud"] != "hub" ||
		idClaims["email"] != "member@example.test" || idClaims["unmapped"] != nil || idClaims["token_use"] != "id" {
		t.Fatal("id token claims did not match the local issuer, client, and mapped provider profile")
	}
	subject, ok := idClaims["sub"].(string)
	if !ok || subject == "" || accessClaims["iss"] != issuer || accessClaims["client_id"] != "hub" ||
		accessClaims["scope"] != "openid email" || accessClaims["token_use"] != "access" || accessClaims["sub"] != subject {
		t.Fatal("access token claims did not match the local issuer, client, and subject")
	}
	second := sendBrowserRequest(t, client, browserTestHost, http.MethodPost, local.URL+browserTokenPath, form, browserTestOrigin, nil)
	if second.status != http.StatusBadRequest || !strings.Contains(string(second.body), "invalid_grant") {
		t.Fatalf("repeated exchange status = %d, want 400 invalid_grant", second.status)
	}
}

func TestBrowserStateExpirationAndRedemption(t *testing.T) {
	var b browserState
	now := time.Unix(1735689600, 0)
	if !b.addTransaction("state", browserTransaction{issuer: "issuer", cookie: "cookie"}, now) {
		t.Fatal("transaction was not stored")
	}
	if _, ok := b.takeTransaction("state", "cookie", "other-issuer", now); ok {
		t.Fatal("transaction accepted wrong issuer")
	}
	if _, ok := b.takeTransaction("state", "cookie", "issuer", now.Add(browserTTL)); ok {
		t.Fatal("transaction accepted at expiration")
	}
	grant := browserGrant{issuer: "issuer", clientID: "client", redirectURI: "callback", challenge: "challenge"}
	if !b.addGrant("expired", grant, now) {
		t.Fatal("expiring code was not stored")
	}
	if _, ok := b.redeem("expired", "issuer", "client", "callback", "challenge", now.Add(browserTTL)); ok {
		t.Fatal("code accepted at expiration")
	}
	if !b.addGrant("code", grant, now) {
		t.Fatal("code was not stored")
	}
	if _, ok := b.redeem("code", "issuer", "other-client", "callback", "challenge", now); ok {
		t.Fatal("code accepted a different client")
	}
	if _, ok := b.redeem("code", "issuer", "client", "other-callback", "challenge", now); ok {
		t.Fatal("code accepted a different redirect URI")
	}
	var redeemed atomic.Int32
	var wait sync.WaitGroup
	for range 8 {
		wait.Go(func() {
			if _, ok := b.redeem("code", "issuer", "client", "callback", "challenge", now); ok {
				redeemed.Add(1)
			}
		})
	}
	wait.Wait()
	if got := redeemed.Load(); got != 1 {
		t.Fatalf("concurrent redemptions = %d, want 1", got)
	}
}

func newBrowserSigningKey(t *testing.T) signingKey {
	t.Helper()
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return describeKey(private)
}

func testOIDCProvider(t *testing.T, key signingKey) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	var expected url.Values
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "https://" + r.Host
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer": base, "authorization_endpoint": base + "/signin", "token_endpoint": base + "/token",
			"jwks_uri": base + "/jwks", "token_endpoint_auth_methods_supported": []string{"none"},
		})
	})
	mux.HandleFunc("GET /signin", func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		if params.Get("response_type") != "code" || params.Get("code_challenge_method") != "S256" ||
			params.Get("client_id") != "registered-upstream-client" ||
			params.Get("redirect_uri") != "https://"+browserTestHost+idpResponsePath ||
			params.Get("scope") != "openid profile" {
			http.Error(w, "invalid test provider authorization request", http.StatusBadRequest)
			return
		}
		mu.Lock()
		expected = params
		mu.Unlock()
		response := url.Values{"code": {"upstream-code"}, "state": {params.Get("state")}, "iss": {"https://" + r.Host}}
		http.Redirect(w, r, params.Get("redirect_uri")+"?"+response.Encode(), http.StatusFound)
	})
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		params := expected
		mu.Unlock()
		if err := r.ParseForm(); err != nil || r.PostForm.Get("code") != "upstream-code" ||
			r.PostForm.Get("client_id") != params.Get("client_id") || r.PostForm.Get("redirect_uri") != params.Get("redirect_uri") ||
			r.PostForm.Get("grant_type") != "authorization_code" ||
			challengeFor(r.PostForm.Get("code_verifier")) != params.Get("code_challenge") {
			http.Error(w, "invalid test provider token request", http.StatusBadRequest)
			return
		}
		now := time.Now()
		token, err := key.sign(map[string]any{
			"iss": "https://" + r.Host, "aud": params.Get("client_id"), "sub": "provider-user",
			"nonce": params.Get("nonce"), "iat": now.Unix(), "exp": now.Add(time.Minute).Unix(),
			"preferred_email": "member@example.test", "unmapped": "unmapped-claim-value",
		})
		if err != nil {
			http.Error(w, "sign test id token", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"id_token": token})
	})
	mux.HandleFunc("GET /jwks", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string][]publicJWK{"keys": {key.public}})
	})
	server := httptest.NewTLSServer(mux)
	t.Cleanup(server.Close)
	return server
}

func localBrowserClient(server *httptest.Server) *http.Client {
	client := *server.Client()
	client.Timeout = oidcTimeout
	client.CheckRedirect = defaultOIDCClient.CheckRedirect
	return &client
}

type browserHTTPResponse struct {
	status  int
	header  http.Header
	cookies []*http.Cookie
	body    []byte
}

func sendBrowserRequest(t *testing.T, client *http.Client, host, method, endpoint string, form url.Values, origin string, cookie *http.Cookie) browserHTTPResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return browserHTTPResponse{res.StatusCode, res.Header, res.Cookies(), data}
}

func browserLocation(t *testing.T, res browserHTTPResponse) *url.URL {
	t.Helper()
	u, err := url.Parse(res.header.Get("Location"))
	if err != nil || !u.IsAbs() || res.status != http.StatusFound {
		t.Fatalf("response status = %d with absolute redirect %t, want 302 with absolute redirect", res.status, err == nil && u.IsAbs())
	}
	return u
}

func browserTokenClaims(t *testing.T, token string, key *rsa.PublicKey) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatal("token is not a signed JWT")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal("token signature is not base64url")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], sig); err != nil {
		t.Fatal("token signature validation failed")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal("token payload is not base64url")
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal("token claims are not a JSON object")
	}
	return claims
}
