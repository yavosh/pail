package queue

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
)

// redrivePolicy is the stored form of the RedrivePolicy attribute.
type redrivePolicy struct {
	TargetARN       string `json:"deadLetterTargetArn"`
	MaxReceiveCount int    `json:"maxReceiveCount"`
}

// parseRedrivePolicy reads the shape of a RedrivePolicy. maxReceiveCount may
// be a JSON number or a decimal string.
func parseRedrivePolicy(v string) (redrivePolicy, bool) {
	var in struct {
		TargetARN       string      `json:"deadLetterTargetArn"`
		MaxReceiveCount json.Number `json:"maxReceiveCount"`
	}
	if !isJSONObject(v) || json.Unmarshal([]byte(v), &in) != nil || in.TargetARN == "" || !intRange(0, 999999999)(in.MaxReceiveCount.String()) {
		return redrivePolicy{}, false
	}
	n, _ := strconv.Atoi(in.MaxReceiveCount.String())
	return redrivePolicy{in.TargetARN, n}, true
}

func validRedrivePolicy(v string) bool {
	_, ok := parseRedrivePolicy(v)
	return ok
}

func canonRedrivePolicy(v string) string {
	p, _ := parseRedrivePolicy(v)
	b, _ := json.Marshal(p)
	return string(b)
}

// redriveAllowPolicy is the stored form of the RedriveAllowPolicy attribute.
type redriveAllowPolicy struct {
	Permission      string   `json:"redrivePermission"`
	SourceQueueARNs []string `json:"sourceQueueArns,omitempty"`
}

func parseRedriveAllowPolicy(v string) (redriveAllowPolicy, bool) {
	var p redriveAllowPolicy
	if !isJSONObject(v) || json.Unmarshal([]byte(v), &p) != nil {
		return redriveAllowPolicy{}, false
	}
	switch p.Permission {
	case "allowAll", "denyAll":
		return p, len(p.SourceQueueARNs) == 0
	case "byQueue":
		return p, len(p.SourceQueueARNs) >= 1 && len(p.SourceQueueARNs) <= 10
	}
	return redriveAllowPolicy{}, false
}

func validRedriveAllowPolicy(v string) bool {
	_, ok := parseRedriveAllowPolicy(v)
	return ok
}

func canonRedriveAllowPolicy(v string) string {
	p, _ := parseRedriveAllowPolicy(v)
	b, _ := json.Marshal(p)
	return string(b)
}

// permits reports whether the policy lets the queue with sourceARN use this queue as a dead-letter queue.
func (p redriveAllowPolicy) permits(sourceARN string) bool {
	switch p.Permission {
	case "denyAll":
		return false
	case "byQueue":
		return slices.Contains(p.SourceQueueARNs, sourceARN)
	}
	return true
}

func isFIFOName(name string) bool { return strings.HasSuffix(name, ".fifo") }
