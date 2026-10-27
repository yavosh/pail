package kmsapi

import (
	"net/http"

	"github.com/yavosh/pail/internal/kms"
)

func validation(message string) error {
	return &kms.Error{Code: "ValidationException", Message: message}
}

func (h *handler) createKey(r *http.Request, in kms.CreateInput) (any, error) {
	m, err := h.keys.Create(r.Context(), in)
	return struct{ KeyMetadata kms.Metadata }{m}, err
}

type keyInput struct {
	KeyID               string   `json:"KeyId"`
	Description         *string  `json:"Description"`
	PendingWindowInDays *int     `json:"PendingWindowInDays"`
	GrantTokens         []string `json:"GrantTokens"`
}

func (h *handler) describeKey(r *http.Request, in keyInput) (any, error) {
	if err := unsupportedOptions(nil, in.GrantTokens); err != nil {
		return nil, err
	}
	m, err := h.keys.Describe(r.Context(), in.KeyID)
	return struct{ KeyMetadata kms.Metadata }{m}, err
}

func (h *handler) changeKey(r *http.Request, in keyInput) (any, error) {
	op := operationName(r)
	description := ""
	if op == "UpdateKeyDescription" && in.Description == nil {
		return nil, validation("description is required")
	}
	if in.Description != nil {
		description = *in.Description
	}
	days := 30
	if in.PendingWindowInDays != nil {
		days = *in.PendingWindowInDays
	}
	m, err := h.keys.Change(r.Context(), in.KeyID, op, description, days)
	if err != nil {
		return nil, err
	}
	switch op {
	case "ScheduleKeyDeletion":
		return struct {
			KeyID               string  `json:"KeyId"`
			DeletionDate        float64 `json:"DeletionDate"`
			KeyState            string  `json:"KeyState"`
			PendingWindowInDays int     `json:"PendingWindowInDays"`
		}{m.ARN, m.DeletionDate, m.KeyState, days}, nil
	case "CancelKeyDeletion":
		return struct {
			KeyID string `json:"KeyId"`
		}{m.ARN}, nil
	}
	return nil, nil
}

type pageInput struct {
	KeyID  string `json:"KeyId"`
	Limit  *int   `json:"Limit"`
	Marker string `json:"Marker"`
}

func (in pageInput) limit(defaultLimit int) int {
	if in.Limit != nil {
		return *in.Limit
	}
	return defaultLimit
}

type pagination struct {
	NextMarker string `json:"NextMarker,omitempty"`
	Truncated  bool   `json:"Truncated"`
}

func (h *handler) listKeys(r *http.Request, in pageInput) (any, error) {
	keys, next, err := h.keys.ListKeys(r.Context(), in.limit(100), in.Marker)
	return struct {
		Keys []kms.KeyEntry `json:"Keys"`
		pagination
	}{keys, pagination{next, next != ""}}, err
}

type aliasInput struct {
	AliasName   string `json:"AliasName"`
	TargetKeyID string `json:"TargetKeyId"`
}

func (h *handler) changeAlias(r *http.Request, in aliasInput) (any, error) {
	return nil, h.keys.ChangeAlias(r.Context(), in.AliasName, in.TargetKeyID, operationName(r))
}

func (h *handler) listAliases(r *http.Request, in pageInput) (any, error) {
	aliases, next, err := h.keys.ListAliases(r.Context(), in.KeyID, in.limit(50), in.Marker)
	return struct {
		Aliases    []kms.Alias `json:"Aliases"`
		AliasCount int         `json:"AliasCount"`
		pagination
	}{aliases, len(aliases), pagination{next, next != ""}}, err
}

type tagsInput struct {
	KeyID   string    `json:"KeyId"`
	Tags    []kms.Tag `json:"Tags"`
	TagKeys []string  `json:"TagKeys"`
}

func (h *handler) changeTags(r *http.Request, in tagsInput) (any, error) {
	if operationName(r) == "TagResource" {
		if len(in.Tags) == 0 {
			return nil, validation("tags are required")
		}
		return nil, h.keys.ChangeTags(r.Context(), in.KeyID, in.Tags, nil)
	}
	if len(in.TagKeys) == 0 || len(in.TagKeys) > 50 {
		return nil, validation("tag keys must contain 1 to 50 entries")
	}
	return nil, h.keys.ChangeTags(r.Context(), in.KeyID, nil, in.TagKeys)
}

func (h *handler) listTags(r *http.Request, in pageInput) (any, error) {
	tags, next, err := h.keys.ListTags(r.Context(), in.KeyID, in.limit(50), in.Marker)
	return struct {
		Tags []kms.Tag `json:"Tags"`
		pagination
	}{tags, pagination{next, next != ""}}, err
}

type policyInput struct {
	KeyID                          string  `json:"KeyId"`
	PolicyName                     string  `json:"PolicyName"`
	Policy                         *string `json:"Policy"`
	BypassPolicyLockoutSafetyCheck bool    `json:"BypassPolicyLockoutSafetyCheck"`
}

func (h *handler) keyPolicy(r *http.Request, in policyInput) (any, error) {
	if in.PolicyName == "" {
		in.PolicyName = "default"
	}
	policy := in.Policy
	if operationName(r) == "GetKeyPolicy" {
		policy = nil
	} else if policy == nil {
		return nil, validation("policy is required")
	}
	p, err := h.keys.Policy(r.Context(), in.KeyID, in.PolicyName, policy)
	if err != nil {
		return nil, err
	}
	if operationName(r) == "GetKeyPolicy" {
		return struct {
			Policy     string `json:"Policy"`
			PolicyName string `json:"PolicyName"`
		}{p, in.PolicyName}, nil
	}
	return nil, nil
}

func (h *handler) listKeyPolicies(r *http.Request, in pageInput) (any, error) {
	if _, err := h.keys.Policy(r.Context(), in.KeyID, "default", nil); err != nil {
		return nil, err
	}
	if in.limit(100) < 1 || in.limit(100) > 1000 {
		return nil, validation("limit must be between 1 and 1000")
	}
	if in.Marker != "" {
		return nil, &kms.Error{Code: "InvalidMarkerException", Message: "invalid pagination marker"}
	}
	return struct {
		PolicyNames []string `json:"PolicyNames"`
		pagination
	}{[]string{"default"}, pagination{}}, nil
}
