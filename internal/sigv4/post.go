package sigv4

import (
	"crypto/hmac"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// ErrPolicyCondition means the signed policy does not permit the form.
var ErrPolicyCondition = errors.New("post policy condition failed")

// VerifyPost verifies a SigV4 policy and returns its allowed object size range.
// Fields contain the expanded key and the bucket from the request target.
func (v *Verifier) VerifyPost(fields map[string]string) (int64, int64, error) {
	if fields["x-amz-algorithm"] != algorithm {
		return 0, 0, ErrUnsupportedAuth
	}
	a, err := newAuthorization(fields["x-amz-credential"], "host", fields["x-amz-signature"])
	if err != nil {
		return 0, 0, err
	}
	secret, ok := v.secrets[a.accessKeyID]
	if !ok {
		return 0, 0, ErrInvalidAccessKeyID
	}
	signedAt, err := parseSigningTime(fields["x-amz-date"], a.date)
	if err != nil {
		return 0, 0, err
	}
	if time.Until(signedAt) > maxSkew {
		return 0, 0, ErrRequestNotYetValid
	}
	signature, err := hex.DecodeString(a.signature)
	if err != nil || !hmac.Equal(signature, hmacSHA256(signingKey(secret, a.date, a.region), fields["policy"])) {
		return 0, 0, ErrSignatureMismatch
	}
	raw, err := base64.StdEncoding.DecodeString(fields["policy"])
	if err != nil || len(raw) > 20<<10 {
		return 0, 0, ErrPolicyCondition
	}
	var policy struct {
		Expiration time.Time         `json:"expiration"`
		Conditions []json.RawMessage `json:"conditions"`
	}
	if err := json.Unmarshal(raw, &policy); err != nil || len(policy.Conditions) == 0 || policy.Expiration.IsZero() {
		return 0, 0, ErrPolicyCondition
	}
	if !time.Now().Before(policy.Expiration) {
		return 0, 0, ErrRequestExpired
	}
	minSize, maxSize := int64(0), int64(5<<30)
	covered := map[string]bool{}
	for _, raw := range policy.Conditions {
		if len(raw) == 0 {
			return 0, 0, ErrPolicyCondition
		}
		if raw[0] == '{' {
			var matches map[string]string
			if err := json.Unmarshal(raw, &matches); err != nil || len(matches) == 0 {
				return 0, 0, ErrPolicyCondition
			}
			for field, want := range matches {
				field = strings.ToLower(field)
				if got, exists := fields[field]; !exists || got != want {
					return 0, 0, ErrPolicyCondition
				}
				covered[field] = true
			}
			continue
		}
		var condition []json.RawMessage
		if err := json.Unmarshal(raw, &condition); err != nil || len(condition) != 3 {
			return 0, 0, ErrPolicyCondition
		}
		var op string
		if err := json.Unmarshal(condition[0], &op); err != nil {
			return 0, 0, ErrPolicyCondition
		}
		if op == "content-length-range" {
			var lo, hi int64
			if json.Unmarshal(condition[1], &lo) != nil || json.Unmarshal(condition[2], &hi) != nil || lo < 0 || hi < lo {
				return 0, 0, ErrPolicyCondition
			}
			minSize = max(minSize, lo)
			maxSize = min(maxSize, hi)
			continue
		}
		var field, want string
		if json.Unmarshal(condition[1], &field) != nil || json.Unmarshal(condition[2], &want) != nil || !strings.HasPrefix(field, "$") {
			return 0, 0, ErrPolicyCondition
		}
		field = strings.ToLower(strings.TrimPrefix(field, "$"))
		got, exists := fields[field]
		if !exists {
			return 0, 0, ErrPolicyCondition
		}
		switch op {
		case "eq":
			if got != want {
				return 0, 0, ErrPolicyCondition
			}
		case "starts-with":
			values := []string{got}
			if field == "content-type" {
				values = strings.Split(got, ",")
			}
			for _, value := range values {
				if !strings.HasPrefix(value, want) {
					return 0, 0, ErrPolicyCondition
				}
			}
		default:
			return 0, 0, ErrPolicyCondition
		}
		covered[field] = true
	}
	for field := range fields {
		if field == "policy" || field == "x-amz-signature" || strings.HasPrefix(field, "x-ignore-") {
			continue
		}
		if !covered[field] {
			return 0, 0, ErrPolicyCondition
		}
	}
	if minSize > maxSize {
		return 0, 0, ErrPolicyCondition
	}
	return minSize, maxSize, nil
}
