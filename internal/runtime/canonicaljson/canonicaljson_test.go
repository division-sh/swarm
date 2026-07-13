package canonicaljson

import (
	"math"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func TestDecodeNormalizesIJSONSafeNumbers(t *testing.T) {
	raw := []byte(`{"safe_integer":9007199254740991,"fraction":1.25,"equivalent_integer":1e0}`)
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := decoded.Interface().(map[string]any)
	if !ok {
		t.Fatalf("decoded value = %#v, want object", decoded.Interface())
	}
	for field, want := range map[string]float64{"safe_integer": MaxSafeInteger, "fraction": 1.25, "equivalent_integer": 1} {
		if got, ok := object[field].(float64); !ok || got != want {
			t.Fatalf("decoded %s = %#v, want float64(%v)", field, object[field], want)
		}
	}
	wantHash, err := Hash(map[string]any{"value": 1})
	if err != nil {
		t.Fatal(err)
	}
	gotHash, err := HashRaw([]byte(`{"value":1.0}`))
	if err != nil {
		t.Fatal(err)
	}
	if gotHash != wantHash {
		t.Fatalf("equivalent numeric hashes = %q and %q", gotHash, wantHash)
	}
}

func TestFromGoRejectsInvalidUTF8BeforeJSONReplacement(t *testing.T) {
	invalid := string([]byte{0xff})
	for name, value := range map[string]any{
		"value": map[string]any{"field": invalid},
		"key":   map[string]any{invalid: "value"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := FromGo(value); err == nil || !strings.Contains(err.Error(), "UTF-8") {
				t.Fatalf("FromGo(%s) error = %v, want invalid UTF-8", name, err)
			}
		})
	}
}

func TestProgrammaticFloat32UsesItsJSONSemanticValue(t *testing.T) {
	fromFloat32, err := Hash(map[string]any{"value": float32(0.1)})
	if err != nil {
		t.Fatalf("hash float32: %v", err)
	}
	fromJSON, err := HashRaw([]byte(`{"value":0.1}`))
	if err != nil {
		t.Fatalf("hash JSON: %v", err)
	}
	if fromFloat32 != fromJSON {
		t.Fatalf("float32 hash = %s, JSON hash = %s", fromFloat32, fromJSON)
	}
}

func TestSemanticJSONAdmissionFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "unsafe positive integer", raw: `9007199254740992`, want: "safe range"},
		{name: "unsafe negative integer", raw: `-9007199254740992`, want: "safe range"},
		{name: "negative zero", raw: `-0`, want: "negative zero"},
		{name: "negative zero exponent", raw: `-0e10`, want: "negative zero"},
		{name: "non finite overflow", raw: `1e400`, want: "overflows binary64"},
		{name: "positive underflow", raw: `1e-4000`, want: "underflows binary64"},
		{name: "negative underflow", raw: `-1e-4000`, want: "underflows binary64"},
		{name: "duplicate object key", raw: `{"value":1,"value":2}`, want: "duplicate JSON object key"},
		{name: "equivalent escaped key", raw: `{"a":1,"\u0061":2}`, want: "duplicate JSON object key"},
		{name: "unpaired high surrogate", raw: `"\ud800"`, want: "unpaired high surrogate"},
		{name: "unpaired low surrogate", raw: `"\udc00"`, want: "unpaired low surrogate"},
		{name: "trailing JSON", raw: `{} {}`, want: "trailing JSON content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode([]byte(tc.raw))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Decode(%s) error = %v, want %q", tc.raw, err, tc.want)
			}
		})
	}
	if _, err := Hash(map[string]any{"unsafe_integer": int64(9007199254740992)}); err == nil || !strings.Contains(err.Error(), "safe range") {
		t.Fatalf("Hash unsafe programmatic integer error = %v", err)
	}
}

func TestSemanticNumberBoundaryAndCanonicalSpelling(t *testing.T) {
	for _, raw := range []string{
		`0e10`,
		`5e-324`,
		`-5e-324`,
		`9007199254740991`,
		`-9007199254740991`,
	} {
		value, err := Decode([]byte(raw))
		if err != nil {
			t.Fatalf("Decode(%s): %v", raw, err)
		}
		if value.Kind() != semanticvalue.KindNumber {
			t.Fatalf("Decode(%s) kind = %d", raw, value.Kind())
		}
	}

	positive, err := Decode([]byte(`5e-324`))
	if err != nil {
		t.Fatal(err)
	}
	number, _ := positive.Number()
	if number != math.SmallestNonzeroFloat64 {
		t.Fatalf("smallest subnormal = %g, want %g", number, math.SmallestNonzeroFloat64)
	}
	maxSafe, err := Decode([]byte(`9007199254740991`))
	if err != nil {
		t.Fatal(err)
	}
	maxSafeRaw, err := Encode(maxSafe)
	if err != nil {
		t.Fatal(err)
	}
	if string(maxSafeRaw) != `9007199254740991` {
		t.Fatalf("max-safe canonical spelling = %s", maxSafeRaw)
	}

	for _, pair := range [][2]string{
		{`1`, `1.0`},
		{`0`, `0e10`},
		{`5e-324`, `4.9406564584124654e-324`},
	} {
		left, err := HashRaw([]byte(pair[0]))
		if err != nil {
			t.Fatal(err)
		}
		right, err := HashRaw([]byte(pair[1]))
		if err != nil {
			t.Fatal(err)
		}
		if left != right {
			t.Fatalf("equivalent spellings %s and %s hash differently", pair[0], pair[1])
		}
	}
}

func TestDecodeRejectsInvalidUTF8(t *testing.T) {
	_, err := Decode([]byte{'"', 0xff, '"'})
	if err == nil || !strings.Contains(err.Error(), "valid UTF-8") {
		t.Fatalf("invalid UTF-8 error = %v", err)
	}
}
