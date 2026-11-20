package cognito

import (
	"bytes"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	passwordRounds = 600_000
	passwordScope  = "aws.cognito.signin.user.admin"
	cognitoAPIType = "application/x-amz-json-1.1"
)

type passwordUser struct {
	User
	salt [16]byte
	hash [32]byte
}

func loadUser(cfg userConfig, issuer, dir string) (passwordUser, error) {
	if cfg.Username == "" || cfg.PasswordFile == "" {
		return passwordUser{}, errors.New("user requires a username and password file")
	}
	for claim := range cfg.Claims {
		if !isProfileClaim(claim) {
			return passwordUser{}, errors.New("user profile contains a reserved claim")
		}
	}
	secret, err := readSecret(dir, cfg.PasswordFile)
	if err != nil {
		return passwordUser{}, fmt.Errorf("load user password: %w", err)
	}
	sub := cfg.Subject
	if sub == "" {
		sub = subjectID("local", issuer, cfg.Username)
	}
	if len(sub) > 255 || strings.ContainsFunc(sub, func(r rune) bool { return r < '!' || r > '~' }) {
		return passwordUser{}, errors.New("subject must contain at most 255 printable ascii characters")
	}
	u := passwordUser{Subject: sub, Username: cfg.Username, Claims: cfg.Claims}
	rand.Read(u.salt[:])
	hash, err := pbkdf2.Key(sha256.New, secret, u.salt[:], passwordRounds, len(u.hash))
	if err != nil {
		return passwordUser{}, fmt.Errorf("hash user password: %w", err)
	}
	copy(u.hash[:], hash)
	return u, nil
}

func (u passwordUser) matches(password string) bool {
	hash, err := pbkdf2.Key(sha256.New, password, u.salt[:], passwordRounds, len(u.hash))
	return err == nil && subtle.ConstantTimeCompare(hash, u.hash[:]) == 1
}

func (c ClientConfig) matchesSecretHash(username, hash string) bool {
	if c.secret == "" {
		return true
	}
	mac := hmac.New(sha256.New, []byte(c.secret))
	_, _ = io.WriteString(mac, username)
	_, _ = io.WriteString(mac, c.ID)
	want := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return subtle.ConstantTimeCompare([]byte(want), []byte(hash)) == 1
}

type initiateAuthRequest struct {
	AuthFlow       string
	ClientID       string `json:"ClientId"`
	AuthParameters map[string]string
}

type initiateAuthResult struct {
	IDToken     string `json:"IdToken"`
	AccessToken string
	ExpiresIn   int
	TokenType   string
}

// ServeAPI implements the public Cognito USER_PASSWORD_AUTH operation.
func (e *Engine) ServeAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Amzn-RequestId", rand.Text())
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", cognitoAPIType)
	p := e.poolForHost(r.Host)
	if p == nil || r.Method != http.MethodPost || r.URL.EscapedPath() != "/" ||
		r.Header.Get("X-Amz-Target") != "AWSCognitoIdentityProviderService.InitiateAuth" {
		writeAPIError(w, http.StatusBadRequest, "UnknownOperationException", "unknown operation")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 128<<10))
	body = bytes.TrimSpace(body)
	var req initiateAuthRequest
	if err != nil || len(body) == 0 || body[0] != '{' || json.Unmarshal(body, &req) != nil {
		writeAPIError(w, http.StatusBadRequest, "SerializationException", "invalid json request")
		return
	}
	if req.AuthFlow != "USER_PASSWORD_AUTH" || req.ClientID == "" ||
		req.AuthParameters["USERNAME"] == "" || req.AuthParameters["PASSWORD"] == "" {
		writeAPIError(w, http.StatusBadRequest, "InvalidParameterException", "expected user_password_auth, client id, username, and password")
		return
	}
	client, exists := p.clients[req.ClientID]
	if !exists {
		writeAPIError(w, http.StatusBadRequest, "ResourceNotFoundException", "user pool client does not exist")
		return
	}
	if !client.passwordAuth {
		writeAPIError(w, http.StatusBadRequest, "InvalidParameterException", "user_password_auth flow is not enabled for this client")
		return
	}
	username := req.AuthParameters["USERNAME"]
	if !client.matchesSecretHash(username, req.AuthParameters["SECRET_HASH"]) {
		writeAPIError(w, http.StatusBadRequest, "NotAuthorizedException", "invalid client secret hash")
		return
	}
	select {
	case e.passwords <- struct{}{}:
		defer func() { <-e.passwords }()
	default:
		writeAPIError(w, http.StatusBadRequest, "TooManyRequestsException", "too many authentication requests")
		return
	}
	user, exists := p.users[username]
	if !user.matches(req.AuthParameters["PASSWORD"]) || !exists {
		writeAPIError(w, http.StatusBadRequest, "NotAuthorizedException", "incorrect username or password")
		return
	}
	tokens, err := p.issue(client.ID, user.User, passwordScope, "")
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "InternalErrorException", "could not issue tokens")
		return
	}
	out := struct {
		AuthenticationResult initiateAuthResult
	}{AuthenticationResult: initiateAuthResult(tokens)}
	_ = json.NewEncoder(w).Encode(out)
}

func writeAPIError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("X-Amzn-Errortype", code)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{Type: code, Message: message})
}
