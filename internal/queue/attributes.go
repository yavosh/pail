package queue

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// attrSpec describes one settable attribute. An empty def means no default.
// A canon function rewrites a validated value into its stored form.
type attrSpec struct {
	def      string
	ok       func(string) bool
	canon    func(string) string
	fifoOnly bool
}

func isBool(v string) bool { return v == "true" || v == "false" }

// attrSpecs is the one table of settable attributes.
var attrSpecs = map[string]attrSpec{
	"DelaySeconds":                  {"0", intRange(0, 900), canonInt, false},
	"MaximumMessageSize":            {"1048576", intRange(1024, 1048576), canonInt, false},
	"MessageRetentionPeriod":        {"345600", intRange(60, 1209600), canonInt, false},
	"ReceiveMessageWaitTimeSeconds": {"0", intRange(0, 20), canonInt, false},
	"VisibilityTimeout":             {"30", intRange(0, 43200), canonInt, false},
	"SqsManagedSseEnabled":          {"true", isBool, nil, false},
	"KmsMasterKeyId":                {"", func(v string) bool { return v != "" }, nil, false},
	"KmsDataKeyReusePeriodSeconds":  {"", intRange(60, 86400), canonInt, false},
	"Policy":                        {"", isJSONObject, nil, false},
	"RedrivePolicy":                 {"", validRedrivePolicy, canonRedrivePolicy, false},
	"RedriveAllowPolicy":            {"", validRedriveAllowPolicy, canonRedriveAllowPolicy, false},
	"FifoQueue":                     {"", func(v string) bool { return v == "true" }, nil, true},
	"ContentBasedDeduplication":     {"false", isBool, nil, true},
	"DeduplicationScope":            {"queue", func(v string) bool { return v == "queue" || v == "messageGroup" }, nil, true},
	"FifoThroughputLimit":           {"perQueue", func(v string) bool { return v == "perQueue" || v == "perMessageGroupId" }, nil, true},
}

// clearable attributes are removed when set to the empty string.
var clearable = []string{"RedrivePolicy", "RedriveAllowPolicy"}

// computedAttrs are read-only attributes the engine derives.
var computedAttrs = []string{
	"ApproximateNumberOfMessages",
	"ApproximateNumberOfMessagesDelayed",
	"ApproximateNumberOfMessagesNotVisible",
	"CreatedTimestamp",
	"LastModifiedTimestamp",
	"QueueArn",
}

// isJSONObject accepts a JSON document whose top level is an object.
func isJSONObject(v string) bool {
	return strings.HasPrefix(strings.TrimLeft(v, " \t\r\n"), "{") && json.Valid([]byte(v))
}

// intRange accepts plain decimal digits within lo and hi. Leading zeros are
// dropped before the length check, so a value that could overflow int fails.
func intRange(lo, hi int) func(string) bool {
	return func(v string) bool {
		if v == "" {
			return false
		}
		for _, c := range v {
			if c < '0' || c > '9' {
				return false
			}
		}
		v = strings.TrimLeft(v, "0")
		if len(v) > 9 {
			return false
		}
		n, _ := strconv.Atoi(v)
		return n >= lo && n <= hi
	}
}

// canonInt writes a validated integer in canonical decimal form.
func canonInt(v string) string {
	n, _ := strconv.Atoi(v)
	return strconv.Itoa(n)
}

// canonicalAttrs returns a copy of validated attrs in stored form, so "007"
// is stored and compared as "7".
func canonicalAttrs(attrs map[string]string) map[string]string {
	out := maps.Clone(attrs)
	for name, v := range out {
		if canon := attrSpecs[name].canon; canon != nil && v != "" {
			out[name] = canon(v)
		}
	}
	return out
}

// validateAttrs checks names and values in sorted name order. FIFO-only
// attributes are valid only when fifo is true.
func validateAttrs(attrs map[string]string, fifo bool) error {
	for _, name := range slices.Sorted(maps.Keys(attrs)) {
		spec, ok := attrSpecs[name]
		if !ok || (spec.fifoOnly && !fifo) {
			return fmt.Errorf("attribute %s: %w", name, ErrInvalidAttributeName)
		}
		if attrs[name] == "" && slices.Contains(clearable, name) {
			continue
		}
		if !spec.ok(attrs[name]) {
			err := ErrInvalidAttributeValue
			if name == "RedriveAllowPolicy" && allowPolicyARNMismatch(attrs[name]) {
				err = ErrInvalidParameterValue
			}
			return fmt.Errorf("attribute %s value %q: %w", name, attrs[name], err)
		}
	}
	return nil
}

// defaultAttrs returns the defaults for a queue type overlaid with attrs.
func defaultAttrs(attrs map[string]string, fifo bool) map[string]string {
	out := map[string]string{}
	for name, spec := range attrSpecs {
		if spec.def != "" && (fifo || !spec.fifoOnly) {
			out[name] = spec.def
		}
	}
	maps.Copy(out, attrs)
	return out
}

// knownAttr reports whether name can be requested from Attributes.
func knownAttr(name string) bool {
	_, ok := attrSpecs[name]
	return ok || slices.Contains(computedAttrs, name)
}
