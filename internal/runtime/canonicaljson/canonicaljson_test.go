package canonicaljson

import (
	"strings"
	"testing"
)

func TestDecodeNormalizesIJSONSafeNumbers(t *testing.T) {
	raw := []byte(`{"safe_integer":9007199254740991,"fraction":1.25,"equivalent_integer":1e0}`)
	var decoded map[string]any
	if err := Decode(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]float64{"safe_integer": MaxSafeInteger, "fraction": 1.25, "equivalent_integer": 1} {
		if got, ok := decoded[field].(float64); !ok || got != want {
			t.Fatalf("decoded %s = %#v, want float64(%v)", field, decoded[field], want)
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
		{name: "unsafe positive integer", raw: `9007199254740992`, want: "I-JSON safe range"},
		{name: "unsafe negative integer", raw: `-9007199254740992`, want: "I-JSON safe range"},
		{name: "negative zero", raw: `-0`, want: "negative zero"},
		{name: "non finite overflow", raw: `1e400`, want: "unsupported JSON number"},
		{name: "duplicate object key", raw: `{"value":1,"value":2}`, want: "duplicate JSON object key"},
		{name: "trailing JSON", raw: `{} {}`, want: "trailing JSON content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var decoded any
			err := Decode([]byte(tc.raw), &decoded)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Decode(%s) error = %v, want %q", tc.raw, err, tc.want)
			}
		})
	}
	if _, err := Hash(map[string]any{"unsafe_integer": int64(9007199254740992)}); err == nil || !strings.Contains(err.Error(), "I-JSON safe range") {
		t.Fatalf("Hash unsafe programmatic integer error = %v", err)
	}
}
