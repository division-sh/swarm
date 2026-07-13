package canonicaljson

import (
	"encoding/json"
	"testing"
)

func TestDecodePreservesNumericTokensAndCanonicalIdentity(t *testing.T) {
	const preciseInteger = int64(9007199254740993)
	raw := []byte(`{"large_integer":9007199254740993}`)
	var decoded map[string]any
	if err := Decode(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	number, ok := decoded["large_integer"].(json.Number)
	if !ok || number.String() != "9007199254740993" {
		t.Fatalf("decoded large_integer = %#v, want exact json.Number", decoded["large_integer"])
	}
	wantHash, err := Hash(map[string]any{"large_integer": preciseInteger})
	if err != nil {
		t.Fatal(err)
	}
	gotHash, err := Hash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if gotHash != wantHash {
		t.Fatalf("decoded numeric hash = %q, want %q", gotHash, wantHash)
	}
}

func TestDecodeRejectsTrailingJSON(t *testing.T) {
	var decoded any
	if err := Decode([]byte(`{} {}`), &decoded); err == nil {
		t.Fatal("Decode accepted trailing JSON")
	}
}
