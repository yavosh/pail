package cognito

import (
	"encoding/base64"
	"maps"
	"strings"
	"testing"
	"time"
)

func TestOIDCProviderTokenValidation(t *testing.T) {
	key := newBrowserSigningKey(t)
	p := &provider{issuer: "https://idp.local.test/tenant", clientID: "upstream"}
	now := time.Unix(1735689600, 0)
	valid := map[string]any{
		"iss": p.issuer, "aud": []string{p.clientID}, "sub": "member", "nonce": "upstream-nonce",
		"iat": now.Unix(), "exp": now.Add(time.Minute).Unix(),
	}
	jwk := oidcKey{
		ID: key.public.KeyID, Type: "RSA", Use: "sig", Algorithm: "RS256", Operations: []string{"verify"},
		Modulus: key.public.Modulus, Exponent: key.public.Exponent,
	}
	for _, tc := range []struct {
		name, claim string
		value       any
		tamper      bool
		wantOK      bool
	}{
		{name: "valid", wantOK: true},
		{name: "single audience string", claim: "aud", value: p.clientID, wantOK: true},
		{name: "wrong issuer", claim: "iss", value: "https://other.local.test"},
		{name: "wrong audience", claim: "aud", value: []string{"other"}},
		{name: "untrusted additional audience", claim: "aud", value: []string{p.clientID, "other"}},
		{name: "wrong authorized party", claim: "azp", value: "other"},
		{name: "wrong nonce", claim: "nonce", value: "other-nonce"},
		{name: "empty subject", claim: "sub", value: ""},
		{name: "expired", claim: "exp", value: now.Unix()},
		{name: "not yet valid", claim: "nbf", value: now.Add(time.Minute).Unix()},
		{name: "different signature", tamper: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claims := maps.Clone(valid)
			if tc.claim != "" {
				claims[tc.claim] = tc.value
			}
			token, err := key.sign(claims)
			if err != nil {
				t.Fatal("sign test token")
			}
			if tc.tamper {
				head, sig, _ := strings.Cut(token, ".")
				payload, sig, _ := strings.Cut(sig, ".")
				decoded, err := base64.RawURLEncoding.DecodeString(sig)
				if err != nil {
					t.Fatal("decode test signature")
				}
				decoded[0] ^= 1
				token = head + "." + payload + "." + base64.RawURLEncoding.EncodeToString(decoded)
			}
			_, sub, err := verifyOIDCToken(token, []oidcKey{jwk}, p, "upstream-nonce", now)
			if got := err == nil; got != tc.wantOK {
				t.Fatalf("provider token accepted = %v, want %v", got, tc.wantOK)
			}
			if tc.wantOK && sub != "member" {
				t.Errorf("provider subject = %q, want member", sub)
			}
		})
	}
}
