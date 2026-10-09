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
type attrSpec struct {
	def     string
	ok      func(string) bool
	integer bool // stored in canonical decimal form
}

// attrSpecs is the one table of settable attributes. Later PRs add redrive
// and FIFO entries here.
var attrSpecs = map[string]attrSpec{
	"DelaySeconds":                  {"0", intRange(0, 900), true},
	"MaximumMessageSize":            {"1048576", intRange(1024, 1048576), true},
	"MessageRetentionPeriod":        {"345600", intRange(60, 1209600), true},
	"ReceiveMessageWaitTimeSeconds": {"0", intRange(0, 20), true},
	"VisibilityTimeout":             {"30", intRange(0, 43200), true},
	"SqsManagedSseEnabled":          {"true", func(v string) bool { return v == "true" || v == "false" }, false},
	"KmsMasterKeyId":                {"", func(v string) bool { return v != "" }, false},
	"KmsDataKeyReusePeriodSeconds":  {"", intRange(60, 86400), true},
	"Policy":                        {"", isJSONObject, false},
}

// computedAttrs are read-only attributes the engine derives.
var computedAttrs = []string{
	"ApproximateNumberOfMessages",
	"ApproximateNumberOfMessagesDelayed",
	"ApproximateNumberOfMessagesNotVisible",
	"CreatedTimestamp",
	"LastModifiedTimestamp",
	"QueueArn",
}

// unsupportedAttrs are valid SQS attributes that a later PR implements. They
// cannot be set yet, and a standard queue does not report them.
var unsupportedAttrs = []string{
	"FifoQueue", "ContentBasedDeduplication", "DeduplicationScope",
	"FifoThroughputLimit", "RedrivePolicy", "RedriveAllowPolicy",
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

// canonicalAttrs returns a copy of validated attrs with integers in canonical
// decimal form, so "007" is stored and compared as "7".
func canonicalAttrs(attrs map[string]string) map[string]string {
	out := maps.Clone(attrs)
	for name, v := range out {
		if attrSpecs[name].integer {
			n, _ := strconv.Atoi(v)
			out[name] = strconv.Itoa(n)
		}
	}
	return out
}

// validateAttrs checks names and values in sorted name order.
func validateAttrs(attrs map[string]string) error {
	for _, name := range slices.Sorted(maps.Keys(attrs)) {
		if slices.Contains(unsupportedAttrs, name) {
			return fmt.Errorf("attribute %s: %w", name, ErrUnsupported)
		}
		spec, ok := attrSpecs[name]
		if !ok {
			return fmt.Errorf("attribute %s: %w", name, ErrInvalidAttributeName)
		}
		if !spec.ok(attrs[name]) {
			return fmt.Errorf("attribute %s value %q: %w", name, attrs[name], ErrInvalidAttributeValue)
		}
	}
	return nil
}

// defaultAttrs returns the defaults overlaid with attrs.
func defaultAttrs(attrs map[string]string) map[string]string {
	out := map[string]string{}
	for name, spec := range attrSpecs {
		if spec.def != "" {
			out[name] = spec.def
		}
	}
	maps.Copy(out, attrs)
	return out
}

// knownAttr reports whether name can be requested from Attributes.
func knownAttr(name string) bool {
	_, ok := attrSpecs[name]
	return ok || slices.Contains(computedAttrs, name) || slices.Contains(unsupportedAttrs, name)
}
