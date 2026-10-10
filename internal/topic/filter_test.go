package topic

import (
	"errors"
	"strings"
	"testing"
)

func TestCompileFilterErrors(t *testing.T) {
	keys := func(n int) string {
		var parts []string
		for _, k := range []string{"a", "b", "c", "d", "e", "f", "g"}[:n] {
			parts = append(parts, `"`+k+`":["x"]`)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	list := func(n int) string {
		var parts []string
		for i := range n {
			parts = append(parts, `"v`+strings.Repeat("x", i)+`"`)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	tests := []struct {
		name, policy, scope string
	}{
		{"not JSON", `{`, ""},
		{"array", `["a"]`, ""},
		{"string", `"a"`, ""},
		{"null", `null`, ""},
		{"trailing data", `{} {}`, ""},
		{"bad scope", `{"a":["x"]}`, "Everything"},
		{"empty array", `{"a":[]}`, ""},
		{"string value", `{"a":"x"}`, ""},
		{"number value", `{"a":1}`, ""},
		{"$or", `{"$or":{"a":["x"]}}`, "MessageBody"},
		{"$or array", `{"$or":[{"a":["x"]},{"b":["y"]}]}`, ""},
		{"$or scalar array", `{"$or":["x"]}`, ""},
		{"nested in attribute scope", `{"a":{"b":["x"]}}`, ""},
		{"bool element", `{"a":[true]}`, ""},
		{"null element", `{"a":[null]}`, ""},
		{"nested array element", `{"a":[["x"]]}`, ""},
		{"empty operator object", `{"a":[{}]}`, ""},
		{"two operator keys", `{"a":[{"prefix":"x","exists":true}]}`, ""},
		{"suffix", `{"a":[{"suffix":"x"}]}`, ""},
		{"equals-ignore-case", `{"a":[{"equals-ignore-case":"x"}]}`, ""},
		{"cidr", `{"a":[{"cidr":"10.0.0.0/8"}]}`, ""},
		{"wildcard", `{"a":[{"wildcard":"x*"}]}`, ""},
		{"prefix number", `{"a":[{"prefix":1}]}`, ""},
		{"exists string", `{"a":[{"exists":"true"}]}`, ""},
		{"numeric not array", `{"a":[{"numeric":1}]}`, ""},
		{"numeric empty", `{"a":[{"numeric":[]}]}`, ""},
		{"numeric odd", `{"a":[{"numeric":[">"]}]}`, ""},
		{"numeric bad op", `{"a":[{"numeric":["!=",1]}]}`, ""},
		{"numeric string number", `{"a":[{"numeric":[">","1"]}]}`, ""},
		{"numeric equals with bound", `{"a":[{"numeric":["=",1,"<",5]}]}`, ""},
		{"numeric two lower bounds", `{"a":[{"numeric":[">",1,">=",5]}]}`, ""},
		{"numeric two upper bounds", `{"a":[{"numeric":["<",1,"<=",5]}]}`, ""},
		{"numeric three pairs", `{"a":[{"numeric":[">",1,"<",5,"<",9]}]}`, ""},
		{"anything-but empty list", `{"a":[{"anything-but":[]}]}`, ""},
		{"anything-but bool", `{"a":[{"anything-but":true}]}`, ""},
		{"anything-but bool in list", `{"a":[{"anything-but":["x",true]}]}`, ""},
		{"anything-but suffix", `{"a":[{"anything-but":{"suffix":"x"}}]}`, ""},
		{"anything-but prefix number", `{"a":[{"anything-but":{"prefix":1}}]}`, ""},
		{"six keys", keys(6), ""},
		{"151 combinations", `{"a":` + list(11) + `,"b":` + list(14) + `}`, ""},
		{"nested leaf error", `{"a":{"b":[true]}}`, "MessageBody"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := map[string]string{attrFilter: tt.policy, attrScope: tt.scope}
			if p, err := compileFilter(attrs); !errors.Is(err, ErrInvalidParameter) || p != nil {
				t.Errorf("compileFilter(%q, scope %q) = %v, %v; want error %v", tt.policy, tt.scope, p, err, ErrInvalidParameter)
			}
		})
	}
}

func TestCompileFilterValid(t *testing.T) {
	list := func(n int) string {
		var parts []string
		for i := range n {
			parts = append(parts, `"v`+strings.Repeat("x", i)+`"`)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	tests := []struct {
		name, policy, scope string
		wantNil             bool
	}{
		{name: "absent", wantNil: true},
		{name: "empty text", policy: "", scope: "MessageBody", wantNil: true},
		{name: "empty object", policy: `{}`},
		{name: "strings and numbers", policy: `{"a":["x",1,2.5]}`},
		{name: "all operators", policy: `{"a":[{"prefix":"p"},{"anything-but":["x",1]},{"anything-but":{"prefix":"p"}},{"numeric":["=",1]},{"numeric":[">=",1,"<",5]},{"numeric":["<",5,">",1]},{"exists":false}]}`},
		{name: "body scope nested", policy: `{"a":{"b":{"c":["x"]}}}`, scope: "MessageBody"},
		{name: "explicit attribute scope", policy: `{"a":["x"]}`, scope: "MessageAttributes"},
		{name: "five keys", policy: `{"a":["x"],"b":["x"],"c":["x"],"d":["x"],"e":["x"]}`},
		{name: "150 combinations", policy: `{"a":` + list(10) + `,"b":` + list(15) + `}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := map[string]string{attrFilter: tt.policy, attrScope: tt.scope}
			p, err := compileFilter(attrs)
			if err != nil || (p == nil) != tt.wantNil {
				t.Errorf("compileFilter(%q, scope %q) = %v, %v; want nil policy %v and no error", tt.policy, tt.scope, p, err, tt.wantNil)
			}
		})
	}
}

func TestFilterMatch(t *testing.T) {
	str := func(v string) MessageAttribute { return MessageAttribute{DataType: "String", StringValue: v} }
	num := func(v string) MessageAttribute { return MessageAttribute{DataType: "Number", StringValue: v} }
	arr := func(v string) MessageAttribute { return MessageAttribute{DataType: "String.Array", StringValue: v} }
	type attrs = map[string]MessageAttribute
	tests := []struct {
		name, policy, scope string
		attrs               attrs
		message             string
		want                bool
	}{
		{name: "exact string", policy: `{"c":["blue","green"]}`, attrs: attrs{"c": str("green")}, want: true},
		{name: "exact string miss", policy: `{"c":["blue"]}`, attrs: attrs{"c": str("red")}},
		{name: "attribute absent", policy: `{"c":["blue"]}`, attrs: attrs{}},
		{name: "exact number vs Number 15.0", policy: `{"n":[15]}`, attrs: attrs{"n": num("15.0")}, want: true},
		{name: "exact number vs String", policy: `{"n":[15]}`, attrs: attrs{"n": str("15")}},
		{name: "exact string vs Number", policy: `{"n":["15"]}`, attrs: attrs{"n": num("15")}},
		{name: "numeric vs String", policy: `{"n":[{"numeric":[">",10]}]}`, attrs: attrs{"n": str("15")}},
		{name: "prefix", policy: `{"r":[{"prefix":"eu-"}]}`, attrs: attrs{"r": str("eu-west-1")}, want: true},
		{name: "prefix miss", policy: `{"r":[{"prefix":"eu-"}]}`, attrs: attrs{"r": str("us-east-1")}},
		{name: "prefix vs Number", policy: `{"r":[{"prefix":"1"}]}`, attrs: attrs{"r": num("15")}},
		{name: "anything-but value", policy: `{"e":[{"anything-but":"prod"}]}`, attrs: attrs{"e": str("dev")}, want: true},
		{name: "anything-but value miss", policy: `{"e":[{"anything-but":"prod"}]}`, attrs: attrs{"e": str("prod")}},
		{name: "anything-but list", policy: `{"e":[{"anything-but":["prod","stage"]}]}`, attrs: attrs{"e": str("stage")}},
		{name: "anything-but number", policy: `{"e":[{"anything-but":[1,2]}]}`, attrs: attrs{"e": num("3")}, want: true},
		{name: "anything-but other type", policy: `{"e":[{"anything-but":"15"}]}`, attrs: attrs{"e": num("15")}, want: true},
		{name: "anything-but prefix", policy: `{"e":[{"anything-but":{"prefix":"pr"}}]}`, attrs: attrs{"e": str("dev")}, want: true},
		{name: "anything-but prefix miss", policy: `{"e":[{"anything-but":{"prefix":"pr"}}]}`, attrs: attrs{"e": str("prod")}},
		{name: "anything-but absent", policy: `{"e":[{"anything-but":"prod"}]}`, attrs: attrs{}},
		{name: "numeric range in", policy: `{"n":[{"numeric":[">",10,"<=",20]}]}`, attrs: attrs{"n": num("20")}, want: true},
		{name: "numeric range above", policy: `{"n":[{"numeric":[">",10,"<=",20]}]}`, attrs: attrs{"n": num("21")}},
		{name: "numeric range edge", policy: `{"n":[{"numeric":[">",10,"<=",20]}]}`, attrs: attrs{"n": num("10")}},
		{name: "numeric reversed range", policy: `{"n":[{"numeric":["<",20,">=",10]}]}`, attrs: attrs{"n": num("10")}, want: true},
		{name: "numeric equals", policy: `{"n":[{"numeric":["=",3.5]}]}`, attrs: attrs{"n": num("3.50")}, want: true},
		{name: "numeric equals miss", policy: `{"n":[{"numeric":["=",3.5]}]}`, attrs: attrs{"n": num("3")}},
		{name: "exists true", policy: `{"t":[{"exists":true}]}`, attrs: attrs{"t": str("x")}, want: true},
		{name: "exists true miss", policy: `{"t":[{"exists":true}]}`, attrs: attrs{}},
		{name: "exists false", policy: `{"t":[{"exists":false}]}`, attrs: attrs{}, want: true},
		{name: "exists false miss", policy: `{"t":[{"exists":false}]}`, attrs: attrs{"t": str("x")}},
		{name: "binary exists", policy: `{"b":[{"exists":true}]}`, attrs: attrs{"b": {DataType: "Binary", BinaryValue: []byte{1}}}, want: true},
		{name: "binary exact", policy: `{"b":["x"]}`, attrs: attrs{"b": {DataType: "Binary", BinaryValue: []byte("x")}}},
		{name: "binary anything-but", policy: `{"b":[{"anything-but":"x"}]}`, attrs: attrs{"b": {DataType: "Binary", BinaryValue: []byte("x")}}},
		{name: "array any string", policy: `{"t":["b"]}`, attrs: attrs{"t": arr(`["a","b"]`)}, want: true},
		{name: "array any number", policy: `{"t":[2]}`, attrs: attrs{"t": arr(`[1,2]`)}, want: true},
		{name: "array numeric", policy: `{"t":[{"numeric":[">",1]}]}`, attrs: attrs{"t": arr(`[1,2]`)}, want: true},
		{name: "array miss", policy: `{"t":["c"]}`, attrs: attrs{"t": arr(`["a","b"]`)}},
		{name: "array mixed kinds", policy: `{"t":["true"]}`, attrs: attrs{"t": arr(`["a",true]`)}},
		{name: "array prefix", policy: `{"t":[{"prefix":"b"}]}`, attrs: attrs{"t": arr(`["a","bc"]`)}, want: true},
		{name: "array anything-but any value", policy: `{"t":[{"anything-but":"a"}]}`, attrs: attrs{"t": arr(`["a","b"]`)}, want: true},
		{name: "empty array exists", policy: `{"t":[{"exists":true}]}`, attrs: attrs{"t": arr(`[]`)}, want: true},
		{name: "String label", policy: `{"c":["x"]}`, attrs: attrs{"c": {DataType: "String.x", StringValue: "x"}}, want: true},
		{name: "Number label", policy: `{"n":[5]}`, attrs: attrs{"n": {DataType: "Number.x", StringValue: "5"}}, want: true},
		{name: "AND hit", policy: `{"c":["blue"],"n":[15]}`, attrs: attrs{"c": str("blue"), "n": num("15")}, want: true},
		{name: "AND miss", policy: `{"c":["blue"],"n":[15]}`, attrs: attrs{"c": str("red"), "n": num("15")}},
		{name: "empty policy", policy: `{}`, attrs: attrs{}, want: true},
		{name: "empty policy body scope", policy: `{}`, scope: "MessageBody", message: "plain", want: true},
		{name: "body nested", policy: `{"o":{"t":[{"numeric":[">=",100]},150],"k":["book"]}}`, scope: "MessageBody", message: `{"o":{"t":150,"k":"book"}}`, want: true},
		{name: "body nested miss", policy: `{"o":{"k":["book"]}}`, scope: "MessageBody", message: `{"o":{"k":"pen"}}`},
		{name: "body missing path", policy: `{"o":{"k":["book"]}}`, scope: "MessageBody", message: `{"x":1}`},
		{name: "body path through scalar", policy: `{"o":{"k":["book"]}}`, scope: "MessageBody", message: `{"o":"book"}`},
		{name: "body ignores attributes", policy: `{"c":["blue"]}`, scope: "MessageBody", attrs: attrs{"c": str("blue")}, message: `{}`},
		{name: "attribute scope ignores body", policy: `{"c":["blue"]}`, attrs: attrs{}, message: `{"c":"blue"}`},
		{name: "body array of scalars", policy: `{"k":["b"]}`, scope: "MessageBody", message: `{"k":["a","b"]}`, want: true},
		{name: "body array of objects", policy: `{"o":{"k":["b"]}}`, scope: "MessageBody", message: `{"o":[{"k":"a"},{"k":"b"}]}`, want: true},
		{name: "body nested arrays", policy: `{"k":["b"]}`, scope: "MessageBody", message: `{"k":[["a"],["b"]]}`, want: true},
		{name: "body object at path end", policy: `{"o":[{"exists":true}]}`, scope: "MessageBody", message: `{"o":{"k":1}}`},
		{name: "body exists false", policy: `{"o":[{"exists":false}]}`, scope: "MessageBody", message: `{"x":1}`, want: true},
		{name: "body string vs number", policy: `{"k":["1"]}`, scope: "MessageBody", message: `{"k":1}`},
		{name: "body null anything-but", policy: `{"k":[{"anything-but":"a"}]}`, scope: "MessageBody", message: `{"k":null}`, want: true},
		{name: "body not JSON", policy: `{"k":["b"]}`, scope: "MessageBody", message: `plain`},
		{name: "body not an object", policy: `{"k":["b"]}`, scope: "MessageBody", message: `["b"]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := compileFilter(map[string]string{attrFilter: tt.policy, attrScope: tt.scope})
			if err != nil {
				t.Fatalf("compileFilter(%q, scope %q) error = %v", tt.policy, tt.scope, err)
			}
			if got := p.match(tt.attrs, tt.message); got != tt.want {
				t.Errorf("match(%q, scope %q, attrs %v, message %q) = %v, want %v", tt.policy, tt.scope, tt.attrs, tt.message, got, tt.want)
			}
		})
	}
	var nilPolicy *filterPolicy
	if !nilPolicy.match(nil, "x") {
		t.Error("nil policy match = false, want true")
	}
}
