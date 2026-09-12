package pipeline

import (
	"errors"
	"testing"

	engine "github.com/division-sh/swarm/internal/runtime/engine"
)

func TestEntityEmitPersistencePresenceTruthTable(t *testing.T) {
	for _, tc := range []struct {
		name         string
		fields       map[string]any
		prerequisite engine.EmitPersistenceFieldPrerequisite
		valid        bool
	}{
		{"absent", nil, engine.EmitPersistenceFieldPrerequisite{Field: "note", Presence: engine.EntityFieldAbsent}, true},
		{"present_empty", map[string]any{"note": ""}, engine.EmitPersistenceFieldPrerequisite{Field: "note", Presence: engine.EntityFieldPresent, Expected: ""}, true},
		{"resurrected", map[string]any{"note": "old"}, engine.EmitPersistenceFieldPrerequisite{Field: "note", Presence: engine.EntityFieldAbsent}, false},
		{"missing_write", nil, engine.EmitPersistenceFieldPrerequisite{Field: "note", Presence: engine.EntityFieldPresent, Expected: ""}, false},
		{"unknown_presence", nil, engine.EmitPersistenceFieldPrerequisite{Field: "note"}, false},
		{"missing_coordinate", nil, engine.EmitPersistenceFieldPrerequisite{Presence: engine.EntityFieldAbsent}, false},
		{"null_is_not_absence", map[string]any{"note": nil}, engine.EmitPersistenceFieldPrerequisite{Field: "note", Presence: engine.EntityFieldAbsent}, false},
		{"wrong_value", map[string]any{"note": "other"}, engine.EmitPersistenceFieldPrerequisite{Field: "note", Presence: engine.EntityFieldPresent, Expected: ""}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prerequisites := engine.EmitPersistencePrerequisites{Fields: []engine.EmitPersistenceFieldPrerequisite{tc.prerequisite}}
			for _, phase := range []string{"prepared", "persisted"} {
				err := verifyWorkflowEmitFieldPersistence(tc.fields, prerequisites, phase)
				if tc.valid && err != nil || !tc.valid && !errors.Is(err, engine.ErrEmitPersistencePrerequisite) {
					t.Fatalf("%s error=%v, valid=%v", phase, err, tc.valid)
				}
			}
		})
	}
}
