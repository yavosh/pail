package queue

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
)

// attrSpec describes one settable attribute. An empty def means no default.
type attrSpec struct {
	def string
	ok  func(string) bool
}

// attrSpecs is the one table of settable attributes. Later PRs add redrive
// and FIFO entries here.
var attrSpecs = map[string]attrSpec{
	"DelaySeconds":                  {"0", intRange(0, 900)},
	"MaximumMessageSize":            {"1048576", intRange(1024, 1048576)},
	"MessageRetentionPeriod":        {"345600", intRange(60, 1209600)},
	"ReceiveMessageWaitTimeSeconds": {"0", intRange(0, 20)},
	"VisibilityTimeout":             {"30", intRange(0, 43200)},
	"SqsManagedSseEnabled":          {"true", func(v string) bool { return v == "true" || v == "false" }},
	"KmsMasterKeyId":                {"", func(v string) bool { return v != "" }},
	"KmsDataKeyReusePeriodSeconds":  {"", intRange(60, 86400)},
	"Policy":                        {"", func(v string) bool { return json.Valid([]byte(v)) }},
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

// unsupportedAttrs are valid SQS attributes that a later PR implements.
var unsupportedAttrs = []string{
	"FifoQueue", "ContentBasedDeduplication", "DeduplicationScope",
	"FifoThroughputLimit", "RedrivePolicy", "RedriveAllowPolicy",
}

// intRange accepts plain decimal digits within lo and hi.
func intRange(lo, hi int) func(string) bool {
	return func(v string) bool {
		if v == "" || len(v) > 9 {
			return false
		}
		for _, c := range v {
			if c < '0' || c > '9' {
				return false
			}
		}
		n, err := strconv.Atoi(v)
		return err == nil && n >= lo && n <= hi
	}
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
	return ok || slices.Contains(computedAttrs, name)
}
