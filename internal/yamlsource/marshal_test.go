package yamlsource

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMarshalMatchesLegacyFragmentBytes(t *testing.T) {
	nested := map[string]any{
		"inputs":   []string{"inbound.telegram.message"},
		"connect":  []map[string]any{{"event": "inbound.telegram.message", "from": ".", "to": "receiver", "resolution": "select-or-create"}},
		"optional": nil, "enabled": false, "count": 0,
		"text": "quoted \"value\" with colon: and\nnew line",
	}
	for _, test := range []struct {
		name     string
		previous any
		current  any
	}{
		{name: "empty", previous: nil, current: nil},
		{name: "nested", previous: nested, current: nested},
		{name: "quoted_account", previous: map[string]any{"operator_account": &yaml.Node{
			Kind: yaml.ScalarNode, Tag: "!!str", Value: "REPLACE_OPERATOR_ACCOUNT", Style: yaml.DoubleQuotedStyle,
		}}, current: map[string]any{"operator_account": QuotedString("REPLACE_OPERATOR_ACCOUNT")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			previous, err := legacyFragmentBytes(test.previous)
			if err != nil {
				t.Fatal(err)
			}
			current, err := Marshal(test.current)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(previous, current) {
				t.Fatalf("fragment bytes changed:\nold:\n%s\nnew:\n%s", previous, current)
			}
		})
	}
}

func TestMarshalQuotedStringRetainsNumericLookingAccount(t *testing.T) {
	raw, err := Marshal(map[string]any{"operator_account": QuotedString("REPLACE_OPERATOR_ACCOUNT")})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "operator_account: \"REPLACE_OPERATOR_ACCOUNT\"\n" {
		t.Fatalf("quoted account output = %q", raw)
	}
	raw = bytes.ReplaceAll(raw, []byte("REPLACE_OPERATOR_ACCOUNT"), []byte("420079"))
	snapshot, err := NewStore(Limits{}).Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := snapshot.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if account, ok := decoded["operator_account"].(string); !ok || account != "420079" {
		t.Fatalf("numeric-looking account lost string type: %#v", decoded["operator_account"])
	}
}

type marshalErrorValue struct{ err error }

func (value marshalErrorValue) MarshalYAML() (any, error) { return nil, value.err }

func TestMarshalPreservesEncodeError(t *testing.T) {
	want := errors.New("synthetic marshal refusal")
	value := map[string]any{"value": marshalErrorValue{err: want}}
	previous, oldErr := legacyFragmentBytes(value)
	current, err := Marshal(value)
	if previous != nil || current != nil || !errors.Is(oldErr, want) || !errors.Is(err, want) || oldErr.Error() != err.Error() {
		t.Fatalf("encode error changed: old=%q/%v new=%q/%v", previous, oldErr, current, err)
	}
	if !strings.Contains(err.Error(), "synthetic marshal refusal") {
		t.Fatalf("encode cause lost: %v", err)
	}
}

// Keep the original CLI emission sequence as a test-only equivalence oracle.
func legacyFragmentBytes(value any) ([]byte, error) {
	var document yaml.Node
	if err := document.Encode(value); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
