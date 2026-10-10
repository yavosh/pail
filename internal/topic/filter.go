package topic

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
)

const (
	scopeAttributes = "MessageAttributes"
	scopeBody       = "MessageBody"

	// AWS limits per its docs (unverified here).
	maxFilterKeys         = 5
	maxFilterCombinations = 150
)

// filterPolicy is a compiled subscription filter policy. Every leaf must match.
type filterPolicy struct {
	body   bool
	leaves []filterLeaf
}

// filterLeaf is one key path with its conditions; any one condition may match.
type filterLeaf struct {
	path  []string
	conds []condition
}

// condition tests one value. An exists condition tests only presence.
type condition struct {
	exists *bool
	test   func(v any) bool
}

// compileFilter validates the filter policy in attrs and compiles it. It
// returns nil when the subscription has no policy.
func compileFilter(attrs map[string]string) (*filterPolicy, error) {
	text := attrs[attrFilter]
	scope := attrs[attrScope]
	if scope != "" && scope != scopeAttributes && scope != scopeBody {
		return nil, invalid("FilterPolicyScope %q must be %s or %s", scope, scopeAttributes, scopeBody)
	}
	if text == "" {
		return nil, nil
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(text), &doc); err != nil || doc == nil {
		return nil, invalid("FilterPolicy must be a JSON object")
	}
	p := &filterPolicy{body: scope == scopeBody}
	if err := p.walk(nil, doc); err != nil {
		return nil, err
	}
	if len(p.leaves) > maxFilterKeys {
		return nil, invalid("FilterPolicy has %d keys, at most %d", len(p.leaves), maxFilterKeys)
	}
	combos := 1
	for _, l := range p.leaves {
		combos *= len(l.conds)
		if combos > maxFilterCombinations {
			return nil, invalid("FilterPolicy has more than %d value combinations", maxFilterCombinations)
		}
	}
	return p, nil
}

func (p *filterPolicy) walk(prefix []string, m map[string]any) error {
	for _, k := range slices.Sorted(maps.Keys(m)) {
		path := append(slices.Clone(prefix), k)
		if k == "$or" {
			return invalid("FilterPolicy operator $or is not supported")
		}
		switch v := m[k].(type) {
		case []any:
			conds, err := compileConds(k, v)
			if err != nil {
				return err
			}
			p.leaves = append(p.leaves, filterLeaf{path, conds})
		case map[string]any:
			if !p.body {
				return invalid("FilterPolicy key %q: a nested policy needs FilterPolicyScope %s", k, scopeBody)
			}
			if err := p.walk(path, v); err != nil {
				return err
			}
		default:
			return invalid("FilterPolicy key %q must be an array or, in body scope, an object", k)
		}
	}
	return nil
}

func compileConds(key string, list []any) ([]condition, error) {
	if len(list) == 0 {
		return nil, invalid("FilterPolicy key %q: the array is empty", key)
	}
	conds := make([]condition, 0, len(list))
	for _, el := range list {
		c, err := compileCond(el)
		if err != nil {
			return nil, invalid("FilterPolicy key %q: %v", key, err)
		}
		conds = append(conds, c)
	}
	return conds, nil
}

// compileCond turns one array element into a condition. Its errors are
// plain; compileConds wraps them.
func compileCond(el any) (condition, error) {
	switch v := el.(type) {
	case string:
		return condition{test: func(x any) bool { return x == v }}, nil
	case float64:
		return condition{test: func(x any) bool { return x == v }}, nil
	case map[string]any:
		if len(v) != 1 {
			return condition{}, errors.New("an operator object needs exactly one key")
		}
		for op, arg := range v {
			return compileOp(op, arg)
		}
	}
	return condition{}, errors.New("an element must be a string, a number, or an operator object")
}

func compileOp(op string, arg any) (condition, error) {
	switch op {
	case "prefix":
		s, ok := arg.(string)
		if !ok {
			return condition{}, errors.New("prefix needs a string")
		}
		return condition{test: func(x any) bool { return hasPrefix(x, s) }}, nil
	case "exists":
		b, ok := arg.(bool)
		if !ok {
			return condition{}, errors.New("exists needs true or false")
		}
		return condition{exists: &b}, nil
	case "anything-but":
		return compileAnythingBut(arg)
	case "numeric":
		return compileNumeric(arg)
	}
	return condition{}, errors.New("operator " + strconv.Quote(op) + " is not supported")
}

func hasPrefix(x any, prefix string) bool {
	s, ok := x.(string)
	return ok && strings.HasPrefix(s, prefix)
}

func compileAnythingBut(arg any) (condition, error) {
	bad := errors.New("anything-but needs a string, a number, a non-empty array of them, or a prefix object")
	var listed []any
	switch v := arg.(type) {
	case string, float64:
		listed = []any{v}
	case []any:
		if len(v) == 0 {
			return condition{}, bad
		}
		for _, el := range v {
			switch el.(type) {
			case string, float64:
			default:
				return condition{}, bad
			}
		}
		listed = v
	case map[string]any:
		s, ok := v["prefix"].(string)
		if len(v) != 1 || !ok {
			return condition{}, bad
		}
		return condition{test: func(x any) bool { return !hasPrefix(x, s) }}, nil
	default:
		return condition{}, bad
	}
	return condition{test: func(x any) bool { return !slices.Contains(listed, x) }}, nil
}

func compileNumeric(arg any) (condition, error) {
	bad := errors.New(`numeric needs ["=", n] or one or two pairs such as [">", 1, "<=", 5]`)
	list, ok := arg.([]any)
	if !ok || len(list) == 0 || len(list)%2 != 0 || len(list) > 4 {
		return condition{}, bad
	}
	var checks []func(float64) bool
	lower, upper := 0, 0
	for i := 0; i < len(list); i += 2 {
		op, ok := list[i].(string)
		n, isNum := list[i+1].(float64)
		if !ok || !isNum {
			return condition{}, bad
		}
		switch op {
		case "=":
			if len(list) != 2 {
				return condition{}, bad
			}
			checks = append(checks, func(x float64) bool { return x == n })
		case ">":
			lower++
			checks = append(checks, func(x float64) bool { return x > n })
		case ">=":
			lower++
			checks = append(checks, func(x float64) bool { return x >= n })
		case "<":
			upper++
			checks = append(checks, func(x float64) bool { return x < n })
		case "<=":
			upper++
			checks = append(checks, func(x float64) bool { return x <= n })
		default:
			return condition{}, bad
		}
	}
	if len(list) == 4 && (lower != 1 || upper != 1) {
		return condition{}, bad
	}
	return condition{test: func(x any) bool {
		f, ok := x.(float64)
		return ok && !slices.ContainsFunc(checks, func(c func(float64) bool) bool { return !c(f) })
	}}, nil
}

// match reports whether a message passes the policy. A nil policy matches all.
func (p *filterPolicy) match(attrs map[string]MessageAttribute, message string) bool {
	if p == nil || len(p.leaves) == 0 {
		return true
	}
	var doc any
	if p.body {
		if err := json.Unmarshal([]byte(message), &doc); err != nil {
			return false
		}
		if _, ok := doc.(map[string]any); !ok {
			return false
		}
	}
	for _, l := range p.leaves {
		var values []any
		var present bool
		if p.body {
			collect(doc, l.path, &values)
			present = len(values) > 0
		} else {
			values, present = attributeValues(attrs, l.path[0])
		}
		if !slices.ContainsFunc(l.conds, func(c condition) bool { return c.matches(values, present) }) {
			return false
		}
	}
	return true
}

func (c condition) matches(values []any, present bool) bool {
	if c.exists != nil {
		return *c.exists == present
	}
	return present && slices.ContainsFunc(values, c.test)
}

// attributeValues returns the values of a message attribute and whether it exists.
func attributeValues(attrs map[string]MessageAttribute, name string) ([]any, bool) {
	a, ok := attrs[name]
	if !ok {
		return nil, false
	}
	base, _, _ := strings.Cut(a.DataType, ".")
	switch {
	case a.DataType == "String.Array":
		var list []any
		_ = json.Unmarshal([]byte(a.StringValue), &list) // checked in validateAttribute
		return list, true
	case base == "String":
		return []any{a.StringValue}, true
	case base == "Number":
		if f, err := strconv.ParseFloat(a.StringValue, 64); err == nil {
			return []any{f}, true
		}
	}
	return nil, true
}

// collect gathers the values at path in v. An array fans out into its
// elements; an object at the end of the path is not a value.
func collect(v any, path []string, out *[]any) {
	if list, ok := v.([]any); ok {
		for _, el := range list {
			collect(el, path, out)
		}
		return
	}
	if len(path) == 0 {
		if _, isObject := v.(map[string]any); !isObject {
			*out = append(*out, v)
		}
		return
	}
	if m, ok := v.(map[string]any); ok {
		if next, ok := m[path[0]]; ok {
			collect(next, path[1:], out)
		}
	}
}
