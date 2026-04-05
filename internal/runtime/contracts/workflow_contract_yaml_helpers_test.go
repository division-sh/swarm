package contracts

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDecodeStringListNode_NormalizesScalarAndSequenceForms(t *testing.T) {
	var scalar yaml.Node
	if err := yaml.Unmarshal([]byte("check.requested\n"), &scalar); err != nil {
		t.Fatalf("yaml.Unmarshal scalar: %v", err)
	}
	gotScalar, err := decodeStringListNode(scalar.Content[0])
	if err != nil {
		t.Fatalf("decodeStringListNode scalar: %v", err)
	}
	if !reflect.DeepEqual(gotScalar, []string{"check.requested"}) {
		t.Fatalf("scalar = %#v", gotScalar)
	}

	var seq yaml.Node
	if err := yaml.Unmarshal([]byte("- a\n-  b  \n"), &seq); err != nil {
		t.Fatalf("yaml.Unmarshal sequence: %v", err)
	}
	gotSeq, err := decodeStringListNode(seq.Content[0])
	if err != nil {
		t.Fatalf("decodeStringListNode sequence: %v", err)
	}
	if !reflect.DeepEqual(gotSeq, []string{"a", "b"}) {
		t.Fatalf("sequence = %#v", gotSeq)
	}
}

func TestDecodeBoolNode_PreservesConditionalCompatibility(t *testing.T) {
	var node yaml.Node
	if err := yaml.Unmarshal([]byte("conditional\n"), &node); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	got, err := decodeBoolNode(node.Content[0])
	if err != nil {
		t.Fatalf("decodeBoolNode: %v", err)
	}
	if !got {
		t.Fatal("expected conditional to decode as true")
	}
}

func TestParseTypedFieldString_PreservesFlagsAndDefault(t *testing.T) {
	got := parseTypedFieldString("text indexed nullable default pending")
	if got.Type != "text indexed nullable" {
		t.Fatalf("Type = %q", got.Type)
	}
	if !got.Indexed {
		t.Fatal("expected indexed")
	}
	if !got.Nullable {
		t.Fatal("expected nullable")
	}
	if got.Default != "pending" {
		t.Fatalf("Default = %#v", got.Default)
	}
}
