package contracts

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func TestDecodeStringListNode_NormalizesScalarAndSequenceForms(t *testing.T) {
	var scalar yaml.Node
	if err := decodeNodeTestYAML([]byte("check.requested\n"), &scalar); err != nil {
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
	if err := decodeNodeTestYAML([]byte("- a\n-  b  \n"), &seq); err != nil {
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

func TestNodeValueBoolRejectsLegacyConditional(t *testing.T) {
	for _, body := range []string{"true\n", "false\n", "conditional\n"} {
		snapshot, err := yamlsource.Load([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		got, err := nodeValueBool(snapshot.Document("nodes.yaml").Root(), "test boolean")
		if body == "conditional\n" {
			if err == nil {
				t.Fatal("legacy conditional accepted as a boolean")
			}
			continue
		}
		if err != nil || got != (body == "true\n") {
			t.Fatalf("%q: got %v, %v", body, got, err)
		}
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
