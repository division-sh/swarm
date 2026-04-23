package entityruntime

import (
	"reflect"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
)

func TestNormalizeFieldValue_AcceptsBuiltInNumberAndJSONTypes(t *testing.T) {
	contract := Contract{
		Entity: runtimecontracts.EntityContract{
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"score":   {Type: "number"},
				"details": {Type: "json"},
			},
		},
	}

	score, err := NormalizeFieldValue(contract, "score", 91.5)
	if err != nil {
		t.Fatalf("NormalizeFieldValue(score): %v", err)
	}
	if score != 91.5 {
		t.Fatalf("score = %#v, want 91.5", score)
	}

	details, err := NormalizeFieldValue(contract, "details", map[string]any{"summary": "ok"})
	if err != nil {
		t.Fatalf("NormalizeFieldValue(details): %v", err)
	}
	if !reflect.DeepEqual(details, map[string]any{"summary": "ok"}) {
		t.Fatalf("details = %#v", details)
	}
}
