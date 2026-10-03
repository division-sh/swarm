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

func TestDecodeStringListValuePreservesTypedProjection(t *testing.T) {
	for _, row := range []struct {
		source string
		want   []string
		bad    bool
	}{
		{"null", nil, false},
		{"''", nil, false},
		{"'  a  '", []string{"a"}, false},
		{"7", []string{"7"}, false},
		{"[]", nil, false},
		{"[a, ' b ', a, '', null, 7, true]", []string{"a", "b", "7", "true"}, false},
		{"{}", nil, true},
		{"[[a]]", nil, true},
	} {
		t.Run(row.source, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(row.source))
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeStringListValue(snapshot.Document("types.yaml").Root())
			if (err != nil) != row.bad || !reflect.DeepEqual(got, row.want) {
				t.Fatalf("projection = %#v, %v; want %#v, bad=%v", got, err, row.want, row.bad)
			}
		})
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
