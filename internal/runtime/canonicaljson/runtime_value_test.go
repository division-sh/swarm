package canonicaljson

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestRuntimeJSONCarrierPreservesKindsAndSemanticIdentity(t *testing.T) {
	for _, value := range []any{nil, true, "text", int64(8), float64(8), float64(8.25),
		[]any{int64(8), float64(8), nil}, map[string]any{"nested": []any{nil, int64(8), float64(8)}},
	} {
		got, err := CloneRuntimeValue(value)
		if err != nil || !reflect.DeepEqual(got, value) {
			t.Fatalf("clone %T: %#v %v", value, got, err)
		}
		before, err := Hash(value)
		if err != nil {
			t.Fatal(err)
		}
		after, err := Hash(got)
		if err != nil || before != after {
			t.Fatalf("semantic identity changed: %s %s %v", before, after, err)
		}
	}
	for _, tc := range []struct {
		input any
		want  any
	}{
		{int(8), int64(8)}, {uint(8), int64(8)}, {float32(0.1), float64(0.1)},
		{json.Number("8"), int64(8)}, {json.Number("8.0"), float64(8)}, {json.Number("8e0"), float64(8)},
	} {
		got, err := NormalizeRuntimeNumber(tc.input)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("number %T: %#v %v", tc.input, got, err)
		}
	}
}

func TestRuntimeJSONCarrierRejectsNonValues(t *testing.T) {
	semantic, err := Decode([]byte(`8`))
	if err != nil {
		t.Fatal(err)
	}
	cycle := []any{nil}
	cycle[0] = cycle
	for _, value := range []any{semantic, struct{ N int }{1}, make(chan int), new(int), math.Copysign(0, -1),
		math.NaN(), math.Inf(1), int64(9007199254740992), json.Number("1e9999"), "\xff", map[string]any{"\xff": 1}, cycle,
	} {
		if _, err := CloneRuntimeValue(value); err == nil {
			t.Fatalf("admitted %T", value)
		}
	}
}
