package semanticvalue

import (
	"encoding/json"
	"math"
	"testing"
)

func TestValueIsClosedAndDefensivelyCopied(t *testing.T) {
	name := MustString("ready")
	input := []Value{name}
	array := Array(input)
	input[0] = MustString("changed")
	got, ok := array.At(0)
	if !ok || !got.Equal(name) {
		t.Fatalf("array retained mutable input alias: %#v", array.Interface())
	}

	object, err := Object([]ObjectEntry{{Name: "status", Value: name}})
	if err != nil {
		t.Fatalf("Object: %v", err)
	}
	members := object.Members()
	members[0].Value = MustString("changed")
	got, ok = object.Lookup("status")
	if !ok || !got.Equal(name) {
		t.Fatalf("object exposed mutable member alias: %#v", object.Interface())
	}
	if _, err := Object([]ObjectEntry{{Name: "x"}, {Name: "x"}}); err == nil {
		t.Fatal("duplicate object key accepted")
	}
}

func TestNumberContract(t *testing.T) {
	for _, value := range []float64{0, 0.1, math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, MaxSafeInteger, -MaxSafeInteger} {
		if _, err := Number(value); err != nil {
			t.Fatalf("Number(%v): %v", value, err)
		}
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), math.Copysign(0, -1), MaxSafeInteger + 1, -(MaxSafeInteger + 1)} {
		if _, err := Number(value); err == nil {
			t.Fatalf("Number(%v) succeeded", value)
		}
	}
}

func TestValueRejectsDirectJSONCodecBypass(t *testing.T) {
	value := MustString("ready")
	if _, err := json.Marshal(value); err == nil {
		t.Fatal("encoding/json marshaled semantic value directly")
	}
	if err := json.Unmarshal([]byte(`"ready"`), &value); err == nil {
		t.Fatal("encoding/json unmarshaled semantic value directly")
	}
}
