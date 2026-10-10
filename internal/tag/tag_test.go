package tag

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestParseHeader(t *testing.T) {
	tests := []struct {
		name  string
		input string
		limit int
		want  []Tag
		err   error
	}{
		{"empty", "", MaxObject, nil, nil},
		{"order kept", "b=2&a=1", MaxObject, []Tag{{"b", "2"}, {"a", "1"}}, nil},
		{"key only", "a", MaxObject, []Tag{{"a", ""}}, nil},
		{"empty value", "a=", MaxObject, []Tag{{"a", ""}}, nil},
		{"escapes", "a%20b=c%2Bd+e", MaxObject, []Tag{{"a b", "c+d e"}}, nil},
		{"bad escape", "a=%zz", MaxObject, nil, ErrInvalid},
		{"duplicate", "a=1&a=2", MaxObject, nil, ErrInvalid},
		{"aws prefix", "aws:a=1", MaxObject, nil, ErrInvalid},
		{"empty key", "=1", MaxObject, nil, ErrInvalid},
		{"too many", "a&b&c", 2, nil, ErrTooMany},
	}
	for _, tt := range tests {
		got, err := ParseHeader(tt.input, tt.limit)
		if !errors.Is(err, tt.err) || !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: ParseHeader(%q, %d) = %v, %v, want %v, %v", tt.name, tt.input, tt.limit, got, err, tt.want, tt.err)
		}
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		tags []Tag
		want error
	}{
		{"key 128", []Tag{{strings.Repeat("k", 128), "v"}}, nil},
		{"key 129", []Tag{{strings.Repeat("k", 129), "v"}}, ErrInvalid},
		{"key 128 multibyte", []Tag{{strings.Repeat("é", 128), "v"}}, nil},
		{"value 256", []Tag{{"k", strings.Repeat("v", 256)}}, nil},
		{"value 257", []Tag{{"k", strings.Repeat("v", 257)}}, ErrInvalid},
		{"aws prefix", []Tag{{"aws:x", "v"}}, ErrInvalid},
		{"AWS prefix in other case", []Tag{{"AWS:x", "v"}}, nil},
		{"duplicate", []Tag{{"k", "1"}, {"k", "2"}}, ErrInvalid},
	}
	for _, tt := range tests {
		if got := Validate(tt.tags, MaxObject); !errors.Is(got, tt.want) {
			t.Errorf("%s: Validate = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestHas(t *testing.T) {
	have := []Tag{{"a", "1"}, {"b", "2"}}
	tests := []struct {
		name string
		want []Tag
		got  bool
	}{
		{"none wanted", nil, true},
		{"one", []Tag{{"b", "2"}}, true},
		{"all", have, true},
		{"wrong value", []Tag{{"a", "2"}}, false},
		{"missing key", []Tag{{"a", "1"}, {"c", "3"}}, false},
	}
	for _, tt := range tests {
		if got := Has(have, tt.want); got != tt.got {
			t.Errorf("%s: Has(%v, %v) = %v, want %v", tt.name, have, tt.want, got, tt.got)
		}
	}
}
