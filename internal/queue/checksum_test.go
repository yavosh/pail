package queue

import "testing"

func TestMD5OfBody(t *testing.T) {
	if got, want := MD5OfBody("hello"), "5d41402abc4b2a76b9719d911017c592"; got != want {
		t.Errorf("MD5OfBody(hello) = %s, want %s", got, want)
	}
}

func TestMD5OfAttributes(t *testing.T) {
	str := func(v string) MessageAttribute { return MessageAttribute{DataType: "String", StringValue: v} }
	base := map[string]MessageAttribute{"a": str("1"), "b": str("2"), "c": str("3"), "d": str("4")}
	if got := MD5OfAttributes(nil); got != "" {
		t.Errorf("MD5OfAttributes(nil) = %q, want empty", got)
	}
	if got := MD5OfAttributes(map[string]MessageAttribute{}); got != "" {
		t.Errorf("MD5OfAttributes(empty) = %q, want empty", got)
	}
	want := MD5OfAttributes(base)
	for range 20 {
		if got := MD5OfAttributes(base); got != want {
			t.Fatalf("MD5OfAttributes varies: %s vs %s", got, want)
		}
	}
	one := func(name string, a MessageAttribute) string {
		return MD5OfAttributes(map[string]MessageAttribute{name: a})
	}
	tests := []struct {
		name string
		got  string
	}{
		{"base", one("n", str("v"))},
		{"other name", one("m", str("v"))},
		{"other type", one("n", MessageAttribute{DataType: "Number", StringValue: "v"})},
		{"custom label", one("n", MessageAttribute{DataType: "String.x", StringValue: "v"})},
		{"other value", one("n", str("w"))},
		{"binary same bytes", one("n", MessageAttribute{DataType: "Binary", BinaryValue: []byte("v")})},
	}
	seen := map[string]string{}
	for _, tt := range tests {
		if prev, dup := seen[tt.got]; dup {
			t.Errorf("%s has the same MD5 as %s: %s", tt.name, prev, tt.got)
		}
		seen[tt.got] = tt.name
	}
}
