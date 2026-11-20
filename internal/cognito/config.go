package cognito

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

type fileConfig struct {
	Pools []poolConfig `json:"pools"`
}

type poolConfig struct {
	Issuer   string          `json:"issuer"`
	Clients  []clientConfig  `json:"clients"`
	Users    []userConfig    `json:"users"`
	Provider *providerConfig `json:"provider"`
}

type clientConfig struct {
	ID           string   `json:"id"`
	RedirectURIs []string `json:"redirect_uris"`
	Scopes       []string `json:"scopes"`
	Origins      []string `json:"origins"`
	SecretFile   string   `json:"secret_file"`
	PasswordAuth *bool    `json:"password_auth"`
}

type userConfig struct {
	Username     string         `json:"username"`
	Subject      string         `json:"subject"`
	PasswordFile string         `json:"password_file"`
	Claims       map[string]any `json:"claims"`
}

type providerConfig struct {
	Name             string            `json:"name"`
	Issuer           string            `json:"issuer"`
	ClientID         string            `json:"client_id"`
	ClientSecretFile string            `json:"client_secret_file"`
	Scopes           []string          `json:"scopes"`
	ClaimMap         map[string]string `json:"claim_map"`
}

// ClientConfig describes a configured app client's browser routes.
type ClientConfig struct {
	ID           string
	RedirectURIs []string
	Scopes       []string
	Origins      []string
	secret       string
	passwordAuth bool
}

type provider struct {
	name, issuer, clientID, clientSecret string
	scopes                               []string
	claimMap                             map[string]string
}

func readConfig(name string) (fileConfig, error) {
	f, err := os.Open(name)
	if err != nil {
		return fileConfig{}, fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = f.Close() }()
	const limit = 1 << 20
	r := &io.LimitedReader{R: f, N: limit + 1}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var cfg fileConfig
	if err := dec.Decode(&cfg); err != nil || !errors.Is(dec.Decode(new(any)), io.EOF) || r.N == 0 {
		return fileConfig{}, errors.New("invalid cognito config")
	}
	if len(cfg.Pools) == 0 {
		return fileConfig{}, errors.New("cognito config requires a pool")
	}
	return cfg, nil
}

func loadPool(cfg poolConfig, dir string) (*pool, error) {
	u, err := issuerURL(cfg.Issuer)
	if err != nil {
		return nil, err
	}
	p := &pool{
		issuer:     cfg.Issuer,
		issuerPath: u.EscapedPath(),
		origin:     u.Scheme + "://" + u.Host,
		clients:    make(map[string]ClientConfig, len(cfg.Clients)),
		users:      make(map[string]passwordUser, len(cfg.Users)),
	}
	if cfg.Provider != nil {
		p.provider, err = loadProvider(*cfg.Provider, dir)
		if err != nil {
			return nil, fmt.Errorf("configure provider: %w", err)
		}
	}
	if len(cfg.Clients) == 0 {
		return nil, errors.New("pool requires an app client")
	}
	for i, cfg := range cfg.Clients {
		if _, exists := p.clients[cfg.ID]; exists {
			return nil, errors.New("duplicate app client id")
		}
		client, err := loadClient(cfg, dir, p.provider != nil)
		if err != nil {
			return nil, fmt.Errorf("configure app client %d: %w", i+1, err)
		}
		p.clients[client.ID] = client
	}
	subjects := make(map[string]struct{}, len(cfg.Users))
	for i, cfg := range cfg.Users {
		if _, exists := p.users[cfg.Username]; exists {
			return nil, errors.New("duplicate username")
		}
		user, err := loadUser(cfg, p.issuer, dir)
		if err != nil {
			return nil, fmt.Errorf("configure user %d: %w", i+1, err)
		}
		if _, exists := subjects[user.Subject]; exists {
			return nil, errors.New("duplicate user subject")
		}
		subjects[user.Subject] = struct{}{}
		p.users[user.Username] = user
	}
	return p, nil
}

func loadClient(cfg clientConfig, dir string, federation bool) (ClientConfig, error) {
	if cfg.ID == "" || len(cfg.ID) > 128 {
		return ClientConfig{}, errors.New("invalid app client id")
	}
	if len(cfg.RedirectURIs) != 0 && (cfg.SecretFile != "" || !federation) {
		return ClientConfig{}, errors.New("browser clients must be public and require a provider")
	}
	for _, raw := range cfg.RedirectURIs {
		if _, err := browserURL(raw); err != nil {
			return ClientConfig{}, errors.New("invalid redirect uri")
		}
	}
	for _, raw := range cfg.Origins {
		u, err := browserURL(raw)
		if err != nil || raw != u.Scheme+"://"+u.Host {
			return ClientConfig{}, errors.New("invalid origin")
		}
	}
	scopes := cfg.Scopes
	if len(cfg.RedirectURIs) != 0 {
		scopes = withDefaultScopes(scopes)
		if !slices.Contains(scopes, "openid") {
			return ClientConfig{}, errors.New("browser client scopes must include openid")
		}
	}
	if err := checkScopes(scopes); err != nil {
		return ClientConfig{}, err
	}
	secret, err := readSecret(dir, cfg.SecretFile)
	if err != nil {
		return ClientConfig{}, fmt.Errorf("load client secret: %w", err)
	}
	passwordAuth := len(cfg.RedirectURIs) == 0
	if cfg.PasswordAuth != nil {
		passwordAuth = *cfg.PasswordAuth
	}
	return ClientConfig{
		ID: cfg.ID, RedirectURIs: cfg.RedirectURIs, Scopes: scopes, Origins: cfg.Origins,
		secret: secret, passwordAuth: passwordAuth,
	}, nil
}

func loadProvider(cfg providerConfig, dir string) (*provider, error) {
	if cfg.Name == "" || cfg.ClientID == "" {
		return nil, errors.New("provider requires a name and client id")
	}
	if _, err := issuerURL(cfg.Issuer); err != nil {
		return nil, err
	}
	scopes := withDefaultScopes(cfg.Scopes)
	if err := checkScopes(scopes); err != nil {
		return nil, err
	}
	if !slices.Contains(scopes, "openid") {
		return nil, errors.New("provider scopes must include openid")
	}
	for claim, source := range cfg.ClaimMap {
		if !isProfileClaim(claim) || source == "" {
			return nil, errors.New("invalid provider claim mapping")
		}
	}
	secret, err := readSecret(dir, cfg.ClientSecretFile)
	if err != nil {
		return nil, fmt.Errorf("load provider client secret: %w", err)
	}
	return &provider{
		name: cfg.Name, issuer: cfg.Issuer, clientID: cfg.ClientID, clientSecret: secret, scopes: scopes, claimMap: cfg.ClaimMap,
	}, nil
}

func withDefaultScopes(scopes []string) []string {
	if len(scopes) == 0 {
		return []string{"openid"}
	}
	return scopes
}

func checkScopes(scopes []string) error {
	for _, scope := range scopes {
		if scope == "" || strings.ContainsFunc(scope, func(r rune) bool {
			return r < '!' || r > '~' || r == '"' || r == '\\'
		}) {
			return errors.New("invalid scope")
		}
	}
	return nil
}

func issuerURL(raw string) (*url.URL, error) {
	u, err := browserURL(raw)
	if err != nil || u.Scheme != "https" || u.ForceQuery || u.RawQuery != "" ||
		u.EscapedPath() != u.Path || (u.Path != "" && (u.Path == "/" || path.Clean(u.Path) != u.Path)) {
		return nil, errors.New("invalid https issuer url")
	}
	return u, nil
}

func browserURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || strings.Contains(raw, "#") || u.String() != raw {
		return nil, errors.New("invalid url")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || u.Hostname() != "localhost") {
		return nil, errors.New("browser urls require https or http on localhost")
	}
	return u, nil
}

func readSecret(dir, name string) (string, error) {
	if name == "" {
		return "", nil
	}
	if !filepath.IsAbs(name) {
		name = filepath.Join(dir, name)
	}
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	const limit = 4096
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return "", err
	}
	value := strings.TrimSuffix(string(b), "\r\n")
	if len(value) == len(b) {
		value = strings.TrimSuffix(value, "\n")
	}
	if len(value) == 0 || len(b) > limit {
		return "", errors.New("secret file must contain 1 to 4096 bytes")
	}
	return value, nil
}
