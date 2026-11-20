package cognito

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	browserTTL      = 5 * time.Minute
	maxBrowserItems = 1024
)

type browserState struct {
	client *http.Client

	mu           sync.Mutex
	transactions map[string]browserTransaction
	grants       map[string]browserGrant
}

type browserTransaction struct {
	issuer, clientID, redirectURI, challenge, scopes, nonce, appState string
	metadata                                                          oidcMetadata
	verifier, upstreamNonce, cookie                                   string
	expires                                                           time.Time
}

type browserGrant struct {
	issuer, clientID, redirectURI, challenge, scopes, nonce string
	user                                                    User
	expires                                                 time.Time
}

func (b *browserState) addTransaction(state string, tx browserTransaction, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	maps.DeleteFunc(b.transactions, func(_ string, tx browserTransaction) bool { return !tx.expires.After(now) })
	if len(b.transactions) >= maxBrowserItems {
		return false
	}
	if b.transactions == nil {
		b.transactions = make(map[string]browserTransaction)
	}
	tx.expires = now.Add(browserTTL)
	b.transactions[state] = tx
	return true
}

func (b *browserState) takeTransaction(state, cookie, issuer string, now time.Time) (browserTransaction, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	tx, ok := b.transactions[state]
	if !ok || !tx.expires.After(now) || tx.issuer != issuer ||
		subtle.ConstantTimeCompare([]byte(tx.cookie), []byte(cookie)) != 1 {
		return browserTransaction{}, false
	}
	delete(b.transactions, state)
	return tx, true
}

func (b *browserState) addGrant(code string, grant browserGrant, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	maps.DeleteFunc(b.grants, func(_ string, grant browserGrant) bool { return !grant.expires.After(now) })
	if len(b.grants) >= maxBrowserItems {
		return false
	}
	if b.grants == nil {
		b.grants = make(map[string]browserGrant)
	}
	grant.expires = now.Add(browserTTL)
	b.grants[code] = grant
	return true
}

func (b *browserState) redeem(code, issuer, clientID, redirectURI, challenge string, now time.Time) (browserGrant, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	grant, ok := b.grants[code]
	if !ok || !grant.expires.After(now) || grant.issuer != issuer || grant.clientID != clientID ||
		grant.redirectURI != redirectURI || subtle.ConstantTimeCompare([]byte(grant.challenge), []byte(challenge)) != 1 {
		return browserGrant{}, false
	}
	delete(b.grants, code)
	return grant, true
}

func validVerifier(verifier string) bool {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
	if len(verifier) < 43 || len(verifier) > 128 {
		return false
	}
	for _, char := range verifier {
		if !strings.ContainsRune(unreserved, char) {
			return false
		}
	}
	return true
}

func challengeFor(verifier string) string {
	digest := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(digest[:])
}
