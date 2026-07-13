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
		{name: "precision boundary integer", value: "9007199254740993", numeric: true, integer: true, asFloat64: true},
		{name: "integer exponent", value: "1e3", numeric: true, integer: true, asFloat64: true},
		{name: "fraction", value: "1.0000000000000001", numeric: true, integer: false, asFloat64: true},
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
