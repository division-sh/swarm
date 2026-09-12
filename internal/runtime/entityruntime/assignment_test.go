package entityruntime

import (
	"reflect"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEntityMutationPrerequisitesDistinguishMapKeysFromRecordPaths(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write c.WorkflowDataWrite
		want  []string
	}{
		{"root map insertion constructs", c.WorkflowDataWrite{Operation: "set", TargetRef: "entity.by_id", Key: c.RefExpression("payload.id")}, nil},
		{"map member set requires map", c.WorkflowDataWrite{Operation: "set", TargetRef: "entity.by_id.note", Key: c.RefExpression("payload.id")}, []string{"by_id"}},
		{"map-held list requires map", c.WorkflowDataWrite{Operation: "append", TargetRef: "entity.by_id.items", Key: c.RefExpression("payload.id")}, []string{"by_id"}},
		{"map merge requires map", c.WorkflowDataWrite{Operation: "merge", TargetRef: "entity.by_id", Key: c.RefExpression("payload.id")}, []string{"by_id"}},
		{"root list append constructs", c.WorkflowDataWrite{Operation: "append", TargetRef: "entity.items"}, nil},
		{"nested list append requires record", c.WorkflowDataWrite{Operation: "append", TargetRef: "entity.record.items"}, []string{"record"}},
		{"list update requires list", c.WorkflowDataWrite{Operation: "update", TargetRef: "entity.record.items"}, []string{"record.items"}},
		{"record leaf set requires record", c.WorkflowDataWrite{TargetPathRef: "entity.record.name"}, []string{"record"}},
		{"clear may be absent", c.WorkflowDataWrite{Operation: "clear", TargetRef: "entity.record.note"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := MutationRequiredPaths(tc.write); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("prerequisites=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestEntityAssignmentStructuralTransfer(t *testing.T) {
	text := c.ResolvedCatalogType{Kind: c.CatalogTypeText}
	record := c.ResolvedCatalogType{Kind: c.CatalogTypeObject, Fields: []c.ResolvedCatalogField{
		{Name: "required", Type: text},
		{Name: "optional", Type: text, IsOptional: true},
	}}
	facts := AssignmentFacts{}
	facts.AssignValue("record", record, map[string]any{"required": "yes", "optional": "present"})
	if !facts.Has("record.required") || !facts.Has("record.optional") {
		t.Fatalf("literal facts = %v", facts)
	}
	facts.Assign("record", record)
	if facts.Has("record.optional") || !facts.Has("record.required") {
		t.Fatalf("replacement preserved old optional member: %v", facts)
	}
	facts.Assign("record.optional", text)
	if facts.Has("record.sibling") {
		t.Fatal("leaf assignment certified its sibling")
	}
	facts.Forget("record")
	if len(facts) != 0 {
		t.Fatalf("clear left descendants: %v", facts)
	}
}

func TestEntityAssignmentJoinDoesNotUnionBranches(t *testing.T) {
	left := AssignmentFacts{"shared": {}, "left": {}}
	right := AssignmentFacts{"shared": {}, "right": {}}
	joined := left.Intersect(right)
	if len(joined) != 1 || !joined.Has("shared") {
		t.Fatalf("join facts = %v", joined)
	}
	if len(joined.Intersect(AssignmentFacts{})) != 0 {
		t.Fatal("backedge certified first entry")
	}
	joined.Forget("shared")
	if !left.Has("shared") || !right.Has("shared") {
		t.Fatal("join mutated predecessor facts")
	}
}

func TestEntityAssignmentObservationNeverSynthesizesRequiredMembers(t *testing.T) {
	record := c.ResolvedCatalogType{Kind: c.CatalogTypeObject, Fields: []c.ResolvedCatalogField{{Name: "required", Type: c.ResolvedCatalogType{Kind: c.CatalogTypeText}}}}
	entity := c.ResolvedCatalogType{Kind: c.CatalogTypeObject, Fields: []c.ResolvedCatalogField{{Name: "record", Type: record}}}
	facts := ObservedAssignmentFacts(entity, map[string]any{"record": map[string]any{}})
	if !facts.Has("record") || facts.Has("record.required") {
		t.Fatalf("fabricated observed member: %v", facts)
	}
	if got := ObservedAssignmentFacts(entity, map[string]any{}); len(got) != 0 {
		t.Fatalf("cleared root resurrected: %v", got)
	}
}
