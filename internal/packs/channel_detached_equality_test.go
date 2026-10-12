package packs

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestSchemaRelation2630LearnedDetachedEquality(t *testing.T) {
	pair, err := contracts.AdmitToolInputSchemaMap(map[string]any{
		"type": "object", "additionalProperties": false, "required": []any{"mode", "peer"},
		"properties": map[string]any{
			"mode": map[string]any{"type": "string", "minLength": 1, "maxLength": 10, "x-swarm-equalTo": "peer"},
			"peer": map[string]any{"type": "string", "minLength": 1, "maxLength": 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pair.Validate(map[string]any{"mode": "approved", "peer": "approved"}); err != nil {
		t.Fatal(err)
	}
	opaque := map[string]contracts.ToolInputSchema{"conversation_reference": pair, "destination": pair}
	mappings := map[string]ChannelMapping{
		"destination.mode": {From: "conversation_reference.mode"},
		"destination.peer": {From: "conversation_reference.peer"},
	}
	compiled, err := compileLearnedDestination(mappings, opaque)
	if err == nil || !strings.Contains(err.Error(), "learned_destination destination.mode") || !strings.Contains(err.Error(), "requires its enclosing object relation") || len(compiled) != 0 {
		t.Fatalf("scalar-leaf gate cannot prove detached equality: compiled=%v error=%v", compiled, err)
	}
}
