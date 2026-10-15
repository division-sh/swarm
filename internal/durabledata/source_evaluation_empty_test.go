package durabledata

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSourceEvaluationBaseJSONPreservesPayloadPresence(t *testing.T) {
	ref := DeclarationRef{FlowPath: ".", EventName: "items"}
	schema := map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}}
	for _, input := range []string{"", "{\"value\":\"one\"}\n"} {
		t.Run(input, func(t *testing.T) {
			compiled, defects := CompileJSONL(ref, schema, "", []byte(input))
			if len(defects) != 0 {
				t.Fatal(defects)
			}
			head := VersionHead(compiled.VersionID)
			base := SourceEvaluationBase{
				State: "version", Head: HeadResult{Before: head, After: head, Revision: 1},
				Manifest: &compiled.Manifest, CanonicalSchema: compiled.CanonicalSchema, CanonicalJSONL: compiled.CanonicalJSONL,
			}
			encoded, err := json.Marshal(base)
			if err != nil {
				t.Fatal(err)
			}
			if len(input) == 0 && !strings.Contains(string(encoded), `"canonical_jsonl":""`) {
				t.Fatalf("present empty payload was not encoded explicitly: %s", encoded)
			}
			var decoded SourceEvaluationBase
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, base) {
				t.Fatalf("receipt roundtrip lost payload presence: %s; got %#v", encoded, decoded)
			}
			if _, err := decoded.Validate(ref); err != nil {
				t.Fatalf("valid stored base: %v", err)
			}
			decoded.CanonicalJSONL = nil
			if _, err := decoded.Validate(ref); err == nil {
				t.Fatal("missing payload accepted as present empty content")
			}
		})
	}
	base := SourceEvaluationBase{State: "absent", Head: HeadResult{Before: AbsentHead(), After: AbsentHead()}}
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"canonical_jsonl"`) {
		t.Fatalf("absent base acquired payload field: %s", encoded)
	}
	var decoded SourceEvaluationBase
	if err := json.Unmarshal(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, base) {
		t.Fatalf("absent base roundtrip: %#v, %v", decoded, err)
	}
	if _, err := decoded.Validate(ref); err != nil {
		t.Fatal(err)
	}
}
