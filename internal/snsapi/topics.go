package snsapi

import (
	"encoding/base64"
	"fmt"
	"maps"
	"net/http"
	"slices"

	"github.com/yavosh/pail/internal/topic"
)

// encodeToken wraps an engine page cursor in an opaque NextToken.
func encodeToken(next string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(next))
}

// decodeToken returns the engine cursor in a NextToken.
func decodeToken(token string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("NextToken is not valid: %w", topic.ErrInvalidParameter)
	}
	return string(b), nil
}

func (h *handler) createTopic(r *http.Request, p params) (string, error) {
	name, err := p.required("Name")
	if err != nil {
		return "", err
	}
	if p.has("DataProtectionPolicy") {
		return "", fmt.Errorf("DataProtectionPolicy is not supported: %w", topic.ErrInvalidParameter)
	}
	arn, err := h.topics.CreateTopic(r.Context(), name, p.attributes("Attributes"), p.tags("Tags"))
	if err != nil {
		return "", err
	}
	var x xmlBuf
	x.elem("TopicArn", arn)
	return x.String(), nil
}

func (h *handler) deleteTopic(r *http.Request, p params) (string, error) {
	arn, err := p.required("TopicArn")
	if err != nil {
		return "", err
	}
	return "", h.topics.DeleteTopic(r.Context(), arn)
}

func (h *handler) listTopics(r *http.Request, p params) (string, error) {
	next, err := decodeToken(p.get("NextToken"))
	if err != nil {
		return "", err
	}
	arns, after, err := h.topics.ListTopics(r.Context(), next)
	if err != nil {
		return "", err
	}
	var x xmlBuf
	if len(arns) == 0 {
		x.WriteString("<Topics/>")
	} else {
		x.open("Topics")
		for _, arn := range arns {
			x.open("member")
			x.elem("TopicArn", arn)
			x.close("member")
		}
		x.close("Topics")
	}
	if after != "" {
		x.elem("NextToken", encodeToken(after))
	}
	return x.String(), nil
}

func (h *handler) getTopicAttributes(r *http.Request, p params) (string, error) {
	arn, err := p.required("TopicArn")
	if err != nil {
		return "", err
	}
	attrs, err := h.topics.TopicAttributes(r.Context(), arn)
	if err != nil {
		return "", err
	}
	var x xmlBuf
	x.attributeMap(attrs)
	return x.String(), nil
}

func (h *handler) setTopicAttributes(r *http.Request, p params) (string, error) {
	arn, err := p.required("TopicArn")
	if err != nil {
		return "", err
	}
	name, err := p.required("AttributeName")
	if err != nil {
		return "", err
	}
	return "", h.topics.SetTopicAttribute(r.Context(), arn, name, p.get("AttributeValue"))
}

func (h *handler) tagResource(r *http.Request, p params) (string, error) {
	arn, err := p.required("ResourceArn")
	if err != nil {
		return "", err
	}
	tags := p.tags("Tags")
	if len(tags) == 0 {
		return "", fmt.Errorf("at least one tag is required: %w", topic.ErrInvalidParameter)
	}
	return "", h.topics.TagResource(r.Context(), arn, tags)
}

func (h *handler) untagResource(r *http.Request, p params) (string, error) {
	arn, err := p.required("ResourceArn")
	if err != nil {
		return "", err
	}
	keys := p.list("TagKeys")
	if len(keys) == 0 {
		return "", fmt.Errorf("TagKeys is required: %w", topic.ErrInvalidParameter)
	}
	return "", h.topics.UntagResource(r.Context(), arn, keys)
}

func (h *handler) listTagsForResource(r *http.Request, p params) (string, error) {
	arn, err := p.required("ResourceArn")
	if err != nil {
		return "", err
	}
	tags, err := h.topics.ListTagsForResource(r.Context(), arn)
	if err != nil {
		return "", err
	}
	var x xmlBuf
	if len(tags) == 0 {
		x.WriteString("<Tags/>")
		return x.String(), nil
	}
	x.open("Tags")
	for _, k := range slices.Sorted(maps.Keys(tags)) {
		x.open("member")
		x.elem("Value", tags[k])
		x.elem("Key", k)
		x.close("member")
	}
	x.close("Tags")
	return x.String(), nil
}
