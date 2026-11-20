package cognito

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const (
	browserAuthorizePath = "/oauth2/authorize"
	browserTokenPath     = "/oauth2/token"
	idpResponsePath      = "/oauth2/idpresponse"
	browserCookiePrefix  = "__Host-pail-cognito-"
	browserRequestLimit  = 16 << 10
)

// ServeBrowser serves the OAuth endpoints on the configured auth host.
func (e *Engine) ServeBrowser(w http.ResponseWriter, r *http.Request) {
	p := e.poolForHost(r.Host)
	if p == nil || p.provider == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Referrer-Policy", "no-referrer")
	switch r.URL.EscapedPath() {
	case browserAuthorizePath:
		if r.Method == http.MethodGet {
			e.browser.authorize(w, r, p)
			return
		}
		w.Header().Set("Allow", http.MethodGet)
	case idpResponsePath:
		if r.Method == http.MethodGet {
			e.browser.callback(w, r, p)
			return
		}
		w.Header().Set("Allow", http.MethodGet)
	case browserTokenPath:
		w.Header().Add("Vary", "Origin")
		switch r.Method {
		case http.MethodPost:
			e.browser.token(w, r, p)
			return
		case http.MethodOptions:
			tokenPreflight(w, r, p)
			return
		}
		w.Header().Set("Allow", "POST, OPTIONS")
	default:
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (b *browserState) authorize(w http.ResponseWriter, r *http.Request, p *pool) {
	params, ok := browserQuery(r)
	if !ok {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	clientID := params.Get("client_id")
	client := p.clients[clientID]
	redirectURI := params.Get("redirect_uri")
	if !slices.Contains(client.RedirectURIs, redirectURI) {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	appState := params.Get("state")
	if params.Get("response_type") != "code" {
		oauthRedirect(w, r, redirectURI, appState, "error", "unsupported_response_type")
		return
	}
	challenge := params.Get("code_challenge")
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(challenge)
	if err != nil || len(decoded) != 32 || len(challenge) != 43 || params.Get("code_challenge_method") != "S256" {
		oauthRedirect(w, r, redirectURI, appState, "error", "invalid_request")
		return
	}
	if name := params.Get("identity_provider"); name != "" && name != p.provider.name {
		oauthRedirect(w, r, redirectURI, appState, "error", "invalid_request")
		return
	}
	scopes := client.Scopes
	if requested := params.Get("scope"); requested != "" {
		scopes = strings.Fields(requested)
	}
	if !slices.Contains(scopes, "openid") || !scopesAllowed(scopes, client.Scopes) {
		oauthRedirect(w, r, redirectURI, appState, "error", "invalid_scope")
		return
	}
	metadata, err := b.discover(r.Context(), p.provider)
	if err != nil {
		oauthRedirect(w, r, redirectURI, appState, "error", "server_error")
		return
	}

	var verifierBytes [32]byte
	rand.Read(verifierBytes[:])
	state := rand.Text()
	tx := browserTransaction{
		issuer: p.issuer, clientID: clientID, redirectURI: redirectURI, challenge: challenge,
		scopes: strings.Join(scopes, " "), nonce: params.Get("nonce"), appState: appState,
		metadata: metadata, verifier: base64.RawURLEncoding.EncodeToString(verifierBytes[:]),
		upstreamNonce: rand.Text(), cookie: rand.Text(),
	}
	if !b.addTransaction(state, tx, time.Now()) {
		oauthRedirect(w, r, redirectURI, appState, "error", "temporarily_unavailable")
		return
	}
	setBrowserCookie(w, state, tx.cookie, int(browserTTL/time.Second))
	http.Redirect(w, r, tx.providerURL(p, state), http.StatusFound)
}

func (b *browserState) callback(w http.ResponseWriter, r *http.Request, p *pool) {
	params, ok := browserQuery(r)
	if !ok {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	state := params.Get("state")
	cookie, err := r.Cookie(browserCookiePrefix + state)
	if err != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	tx, ok := b.takeTransaction(state, cookie.Value, p.issuer, time.Now())
	if !ok {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	setBrowserCookie(w, state, "", -1)
	if params.Get("error") != "" || params.Get("code") == "" {
		oauthRedirect(w, r, tx.redirectURI, tx.appState, "error", "access_denied")
		return
	}
	if issuer := params.Get("iss"); issuer != "" && issuer != p.provider.issuer {
		oauthRedirect(w, r, tx.redirectURI, tx.appState, "error", "access_denied")
		return
	}
	user, err := b.exchange(r.Context(), p, tx, params.Get("code"))
	if err != nil {
		oauthRedirect(w, r, tx.redirectURI, tx.appState, "error", "access_denied")
		return
	}
	code := rand.Text()
	grant := browserGrant{
		issuer: tx.issuer, clientID: tx.clientID, redirectURI: tx.redirectURI,
		challenge: tx.challenge, scopes: tx.scopes, nonce: tx.nonce, user: user,
	}
	if !b.addGrant(code, grant, time.Now()) {
		oauthRedirect(w, r, tx.redirectURI, tx.appState, "error", "temporarily_unavailable")
		return
	}
	oauthRedirect(w, r, tx.redirectURI, tx.appState, "code", code)
}

func (b *browserState) token(w http.ResponseWriter, r *http.Request, p *pool) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/x-www-form-urlencoded" || r.URL.RawQuery != "" {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, browserRequestLimit)
	if err := r.ParseForm(); err != nil || !singleValues(r.PostForm) {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	clientID := r.PostForm.Get("client_id")
	client := p.clients[clientID]
	if len(client.RedirectURIs) == 0 || r.Header.Get("Authorization") != "" || r.PostForm.Has("client_secret") {
		oauthError(w, http.StatusBadRequest, "invalid_client")
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		if origin != p.origin && !slices.Contains(client.Origins, origin) {
			oauthError(w, http.StatusForbidden, "invalid_request")
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
	if r.PostForm.Get("grant_type") != "authorization_code" {
		oauthError(w, http.StatusBadRequest, "unsupported_grant_type")
		return
	}
	verifier := r.PostForm.Get("code_verifier")
	code := r.PostForm.Get("code")
	redirectURI := r.PostForm.Get("redirect_uri")
	if !validVerifier(verifier) || code == "" || redirectURI == "" {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	grant, ok := b.redeem(code, p.issuer, clientID, redirectURI, challengeFor(verifier), time.Now())
	if !ok {
		oauthError(w, http.StatusBadRequest, "invalid_grant")
		return
	}
	tokens, err := p.issue(clientID, grant.user, grant.scopes, grant.nonce)
	if err != nil {
		oauthError(w, http.StatusInternalServerError, "server_error")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		IDToken     string `json:"id_token"`
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}{tokens.IDToken, tokens.AccessToken, tokens.TokenType, tokens.ExpiresIn})
}

func tokenPreflight(w http.ResponseWriter, r *http.Request, p *pool) {
	w.Header().Add("Vary", "Access-Control-Request-Method")
	origin := r.Header.Get("Origin")
	if r.Header.Get("Access-Control-Request-Method") != http.MethodPost || origin == "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if origin != p.origin && !allowsOrigin(p.clients, origin) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", http.MethodPost)
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.WriteHeader(http.StatusNoContent)
}

func allowsOrigin(clients map[string]ClientConfig, origin string) bool {
	for _, client := range clients {
		if len(client.RedirectURIs) > 0 && slices.Contains(client.Origins, origin) {
			return true
		}
	}
	return false
}

func scopesAllowed(scopes, allowed []string) bool {
	for _, scope := range scopes {
		if !slices.Contains(allowed, scope) {
			return false
		}
	}
	return true
}

func browserQuery(r *http.Request) (url.Values, bool) {
	if len(r.URL.RawQuery) > browserRequestLimit {
		return nil, false
	}
	params, err := url.ParseQuery(r.URL.RawQuery)
	return params, err == nil && singleValues(params)
}

func singleValues(params url.Values) bool {
	for _, values := range params {
		if len(values) != 1 {
			return false
		}
	}
	return true
}

func setBrowserCookie(w http.ResponseWriter, state, value string, seconds int) {
	http.SetCookie(w, &http.Cookie{
		Name: browserCookiePrefix + state, Value: value, Path: "/", MaxAge: seconds,
		Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode,
	})
}

func oauthError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}

func oauthRedirect(w http.ResponseWriter, r *http.Request, uri, state, name, value string) {
	target, err := url.Parse(uri)
	if err != nil {
		oauthError(w, http.StatusInternalServerError, "server_error")
		return
	}
	params := target.Query()
	for _, parameter := range []string{"code", "state", "error", "error_description", "error_uri"} {
		params.Del(parameter)
	}
	if state != "" {
		params.Set("state", state)
	}
	params.Set(name, value)
	target.RawQuery = params.Encode()
	http.Redirect(w, r, target.String(), http.StatusFound)
}
