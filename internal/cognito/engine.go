// Package cognito implements local Cognito user pools. See docs/storage.md for key persistence.
package cognito

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yavosh/pail/internal/vfs"
)

const (
	discoverySuffix = "/.well-known/openid-configuration"
	jwksSuffix      = "/.well-known/jwks.json"
)

type privateFS interface {
	vfs.FS
	CreatePrivateTemp() (vfs.TempFile, error)
}

// Engine serves configured pools and owns their transient authentication state.
type Engine struct {
	pools     map[string]*pool
	passwords chan struct{}
	browser   browserState
}

type pool struct {
	issuer, issuerPath, origin string
	clients                    map[string]ClientConfig
	users                      map[string]passwordUser
	provider                   *provider
	keys                       signingKeys
	discovery, jwks            []byte
}

// Open loads configured pools and initializes their persisted signing keys.
func Open(ctx context.Context, fsys privateFS, configPath string) (*Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, err := readConfig(configPath)
	if err != nil {
		return nil, err
	}
	e := &Engine{pools: make(map[string]*pool, len(cfg.Pools)), passwords: make(chan struct{}, 8)}
	if err := fsys.MkdirAll("cognito"); err != nil {
		return nil, fmt.Errorf("create cognito directory: %w", err)
	}
	for i, cfg := range cfg.Pools {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p, err := loadPool(cfg, filepath.Dir(configPath))
		if err != nil {
			return nil, fmt.Errorf("load pool %d: %w", i+1, err)
		}
		host := normalizedHost(strings.TrimPrefix(p.origin, "https://"))
		if _, exists := e.pools[host]; exists {
			return nil, errors.New("each cognito pool requires a different auth host")
		}
		p.keys, err = loadKeys(fsys, p.issuer)
		if err != nil {
			return nil, fmt.Errorf("load signing keys for pool %d: %w", i+1, err)
		}
		p.discovery, err = p.discoveryJSON()
		if err != nil {
			return nil, fmt.Errorf("encode discovery metadata: %w", err)
		}
		p.jwks, err = p.keys.jwksJSON()
		if err != nil {
			return nil, fmt.Errorf("encode jwks: %w", err)
		}
		e.pools[host] = p
	}
	return e, nil
}

func normalizedHost(host string) string {
	return strings.TrimSuffix(strings.ToLower(host), ":443")
}

func (e *Engine) poolForHost(host string) *pool {
	return e.pools[normalizedHost(host)]
}

// IsAuthHost reports whether host is the configured hostname for a pool.
func (e *Engine) IsAuthHost(host string) bool {
	return e.poolForHost(host) != nil
}

// ServeDiscovery serves the configured pool's public metadata routes.
func (e *Engine) ServeDiscovery(w http.ResponseWriter, r *http.Request) bool {
	p := e.poolForHost(r.Host)
	if p == nil {
		return false
	}
	var body []byte
	switch r.URL.EscapedPath() {
	case p.issuerPath + discoverySuffix:
		body = p.discovery
	case p.issuerPath + jwksSuffix:
		body = p.jwks
	default:
		return false
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_, _ = w.Write(body)
	return true
}

type discoveryMetadata struct {
	Issuer                   string   `json:"issuer"`
	JWKSURI                  string   `json:"jwks_uri"`
	AuthorizationEndpoint    string   `json:"authorization_endpoint,omitempty"`
	TokenEndpoint            string   `json:"token_endpoint,omitempty"`
	GrantTypesSupported      []string `json:"grant_types_supported,omitempty"`
	ResponseTypesSupported   []string `json:"response_types_supported,omitempty"`
	SubjectTypesSupported    []string `json:"subject_types_supported"`
	IDTokenSigningAlgorithms []string `json:"id_token_signing_alg_values_supported"`
	CodeChallengeMethods     []string `json:"code_challenge_methods_supported,omitempty"`
	TokenEndpointAuthMethods []string `json:"token_endpoint_auth_methods_supported,omitempty"`
	ScopesSupported          []string `json:"scopes_supported,omitempty"`
}

func (p *pool) discoveryJSON() ([]byte, error) {
	meta := discoveryMetadata{
		Issuer: p.issuer, JWKSURI: p.issuer + jwksSuffix,
		SubjectTypesSupported: []string{"public"}, IDTokenSigningAlgorithms: []string{"RS256"},
	}
	scopes := make(map[string]struct{})
	for _, client := range p.clients {
		if len(client.RedirectURIs) == 0 {
			continue
		}
		for _, scope := range client.Scopes {
			scopes[scope] = struct{}{}
		}
	}
	if len(scopes) != 0 {
		meta.AuthorizationEndpoint = p.origin + browserAuthorizePath
		meta.TokenEndpoint = p.origin + browserTokenPath
		meta.GrantTypesSupported = []string{"authorization_code"}
		meta.ResponseTypesSupported = []string{"code"}
		meta.CodeChallengeMethods = []string{"S256"}
		meta.TokenEndpointAuthMethods = []string{"none"}
		meta.ScopesSupported = slices.Sorted(maps.Keys(scopes))
	}
	return json.Marshal(meta)
}
