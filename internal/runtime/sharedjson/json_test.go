package sharedjson

import (
	"encoding/json"
	"testing"
)

func TestJSONNumberPredicates(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     json.Number
		numeric   bool
		integer   bool
		asFloat64 bool
	}{
		{name: "safe integer boundary", value: "9007199254740991", numeric: true, integer: true, asFloat64: true},
		{name: "unsafe integer", value: "9007199254740992"},
		{name: "integer exponent", value: "1e3", numeric: true, integer: true, asFloat64: true},
		{name: "binary64-normalized fraction", value: "1.0000000000000001", numeric: true, integer: true, asFloat64: true},
		{name: "ordinary fraction", value: "1.25", numeric: true, integer: false, asFloat64: true},
		{name: "negative zero", value: "-0"},
		{name: "invalid", value: "not-a-number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsNumeric(tc.value); got != tc.numeric {
				t.Fatalf("IsNumeric(%q) = %v, want %v", tc.value, got, tc.numeric)
			}
			if got := IsInteger(tc.value); got != tc.integer {
				t.Fatalf("IsInteger(%q) = %v, want %v", tc.value, got, tc.integer)
			}
			if _, got := AsFloat64(tc.value); got != tc.asFloat64 {
				t.Fatalf("AsFloat64(%q) ok = %v, want %v", tc.value, got, tc.asFloat64)
			}
		})
	}
}
