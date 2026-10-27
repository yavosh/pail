package kms

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"maps"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

var aliasPattern = regexp.MustCompile(`^alias/[a-zA-Z0-9/_-]+$`)

// Alias is a customer managed name for a key.
type Alias struct {
	AliasName       string  `json:"AliasName"`
	AliasARN        string  `json:"AliasArn"`
	TargetKeyID     string  `json:"TargetKeyId"`
	CreationDate    float64 `json:"CreationDate"`
	LastUpdatedDate float64 `json:"LastUpdatedDate"`
}

// Tag is one KMS resource tag.
type Tag struct {
	TagKey   string `json:"TagKey"`
	TagValue string `json:"TagValue"`
}

// KeyEntry identifies a key in a ListKeys response.
type KeyEntry struct {
	KeyID  string `json:"KeyId"`
	KeyARN string `json:"KeyArn"`
}

func validateAlias(name string) error {
	if len(name) > 256 || !aliasPattern.MatchString(name) || strings.HasPrefix(name, "alias/aws/") {
		return fail("InvalidAliasNameException", "alias name must use the alias/ prefix and valid characters")
	}
	return nil
}

// ChangeAlias creates, updates, or deletes an alias without changing its key.
func (e *Engine) ChangeAlias(ctx context.Context, name, target, operation string) error {
	if err := validateAlias(name); err != nil {
		return err
	}
	if err := e.lock(ctx); err != nil {
		return err
	}
	defer e.mu.Unlock()
	a, exists := e.state.Aliases[name]
	if operation == "CreateAlias" && exists {
		return fail("AlreadyExistsException", "alias already exists")
	}
	if operation != "CreateAlias" && !exists {
		return fail("NotFoundException", "alias not found")
	}
	next := e.snapshot()
	if operation == "DeleteAlias" {
		delete(next.Aliases, name)
		return e.commit(next)
	}
	if operation != "CreateAlias" && operation != "UpdateAlias" {
		return unsupported()
	}
	k, err := e.resolve(target, false)
	if err != nil {
		return err
	}
	if err := mutable(k); err != nil {
		return err
	}
	if !exists {
		a = Alias{AliasName: name, AliasARN: e.arn(name), CreationDate: timestamp()}
	}
	a.TargetKeyID, a.LastUpdatedDate = k.ID, timestamp()
	next.Aliases[name] = a
	return e.commit(next)
}

// page validates an opaque, operation-specific marker and chooses a page.
func page(names []string, limit int, marker, scope string) ([]string, string, error) {
	if limit < 1 || limit > 1000 {
		return nil, "", invalid("limit must be between 1 and 1000")
	}
	if marker != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(marker)
		prefix := scope + "\x00"
		if err != nil || !strings.HasPrefix(string(decoded), prefix) {
			return nil, "", fail("InvalidMarkerException", "invalid pagination marker")
		}
		after := strings.TrimPrefix(string(decoded), prefix)
		if after == "" {
			return nil, "", fail("InvalidMarkerException", "invalid pagination marker")
		}
		start := slices.IndexFunc(names, func(name string) bool { return name > after })
		if start < 0 {
			start = len(names)
		}
		names = names[start:]
	}
	if len(names) <= limit {
		return names, "", nil
	}
	next := base64.RawURLEncoding.EncodeToString([]byte(scope + "\x00" + names[limit-1]))
	return names[:limit], next, nil
}

// ListKeys returns keys in stable ID order with an opaque continuation marker.
func (e *Engine) ListKeys(ctx context.Context, limit int, marker string) ([]KeyEntry, string, error) {
	if err := e.lock(ctx); err != nil {
		return nil, "", err
	}
	defer e.mu.Unlock()
	names, next, err := page(slices.Sorted(maps.Keys(e.state.Keys)), limit, marker, "keys")
	if err != nil {
		return nil, "", err
	}
	entries := make([]KeyEntry, 0, len(names))
	for _, id := range names {
		entries = append(entries, KeyEntry{KeyID: id, KeyARN: e.arn("key/" + id)})
	}
	return entries, next, nil
}

// ListAliases returns all aliases or those associated with a key ID or ARN.
func (e *Engine) ListAliases(ctx context.Context, id string, limit int, marker string) ([]Alias, string, error) {
	if limit < 1 || limit > 100 {
		return nil, "", invalid("alias limit must be between 1 and 100")
	}
	if err := e.lock(ctx); err != nil {
		return nil, "", err
	}
	defer e.mu.Unlock()
	if id != "" {
		k, err := e.resolve(id, false)
		if err != nil {
			return nil, "", err
		}
		id = k.ID
	}
	names := []string{}
	for name, a := range e.state.Aliases {
		if id == "" || a.TargetKeyID == id {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	names, next, err := page(names, limit, marker, "aliases:"+id)
	if err != nil {
		return nil, "", err
	}
	aliases := make([]Alias, 0, len(names))
	for _, name := range names {
		aliases = append(aliases, e.state.Aliases[name])
	}
	return aliases, next, nil
}

func validTag(s string, max int, empty bool) bool {
	if !utf8.ValidString(s) || utf8.RuneCountInString(s) > max || (!empty && s == "") {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && !unicode.Is(unicode.Z, r) && !strings.ContainsRune("_.:/=+-@", r) {
			return false
		}
	}
	return true
}

func mergeTags(existing map[string]string, tags []Tag) (map[string]string, error) {
	if len(tags) > 50 {
		return nil, fail("TagException", "at most 50 tags are allowed")
	}
	result := map[string]string{}
	maps.Copy(result, existing)
	for _, tag := range tags {
		if !validTag(tag.TagKey, 128, false) || !validTag(tag.TagValue, 256, true) || strings.HasPrefix(tag.TagKey, "aws:") {
			return nil, fail("TagException", "invalid resource tag")
		}
		result[tag.TagKey] = tag.TagValue
	}
	if len(result) > 50 {
		return nil, fail("TagException", "at most 50 tags are allowed")
	}
	return result, nil
}

// ChangeTags adds or removes tags without modifying key material.
func (e *Engine) ChangeTags(ctx context.Context, id string, tags []Tag, remove []string) error {
	if err := e.lock(ctx); err != nil {
		return err
	}
	defer e.mu.Unlock()
	k, err := e.resolve(id, false)
	if err != nil {
		return err
	}
	if err := mutable(k); err != nil {
		return err
	}
	merged, err := mergeTags(k.Tags, tags)
	if err != nil {
		return err
	}
	for _, name := range remove {
		if !validTag(name, 128, false) || strings.HasPrefix(name, "aws:") {
			return fail("TagException", "invalid tag key")
		}
		delete(merged, name)
	}
	k.Tags = merged
	next := e.snapshot()
	next.Keys[k.ID] = k
	return e.commit(next)
}

// ListTags returns a stable page of key tags.
func (e *Engine) ListTags(ctx context.Context, id string, limit int, marker string) ([]Tag, string, error) {
	if limit < 1 || limit > 50 {
		return nil, "", invalid("tag limit must be between 1 and 50")
	}
	if err := e.lock(ctx); err != nil {
		return nil, "", err
	}
	defer e.mu.Unlock()
	k, err := e.resolve(id, false)
	if err != nil {
		return nil, "", err
	}
	names, next, err := page(slices.Sorted(maps.Keys(k.Tags)), limit, marker, "tags:"+k.ID)
	if err != nil {
		return nil, "", err
	}
	tags := make([]Tag, 0, len(names))
	for _, name := range names {
		tags = append(tags, Tag{TagKey: name, TagValue: k.Tags[name]})
	}
	return tags, next, nil
}

func validatePolicy(policy string) error {
	var document map[string]json.RawMessage
	if len(policy) < 1 || len(policy) > 32768 || json.Unmarshal([]byte(policy), &document) != nil || document == nil || document["Statement"] == nil {
		return fail("MalformedPolicyDocumentException", "policy must be a json object with a statement")
	}
	return nil
}

// Policy reads or replaces the default policy. Policies are stored, not enforced.
func (e *Engine) Policy(ctx context.Context, id, name string, policy *string) (string, error) {
	if name == "" {
		name = "default"
	}
	if name != "default" {
		return "", invalid("policy name must be default")
	}
	if policy != nil {
		if err := validatePolicy(*policy); err != nil {
			return "", err
		}
	}
	if err := e.lock(ctx); err != nil {
		return "", err
	}
	defer e.mu.Unlock()
	k, err := e.resolve(id, false)
	if err != nil {
		return "", err
	}
	if policy == nil {
		return k.Policy, nil
	}
	k.Policy = *policy
	next := e.snapshot()
	next.Keys[k.ID] = k
	return *policy, e.commit(next)
}
