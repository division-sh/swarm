package bootverify

import (
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type loopEffectiveSchemaSource struct {
	semanticview.Source
	entries map[string]c.EventCatalogEntry
	schemas map[string]c.CompiledEventSchema
}

func (s loopEffectiveSchemaSource) ResolveEffectiveCompiledFlowEventSchema(flow, event string) (c.CompiledEventSchema, bool, error) {
	if schema, ok := s.schemas[event]; ok && flow == "." {
		return schema, true, nil
	}
	return s.Source.ResolveEffectiveCompiledFlowEventSchema(flow, event)
}

func (s loopEffectiveSchemaSource) ResolveFlowEventCatalogEntry(flow, event string) (c.EventCatalogEntry, string, bool) {
	if entry, ok := s.entries[event]; ok && flow == "." {
		return entry, event, true
	}
	return s.Source.ResolveFlowEventCatalogEntry(flow, event)
}

func TestLoopValidationEffectiveRevisionSchemas(t *testing.T) {
	for _, imported := range []bool{false, true} {
		origin := "authored"
		if imported {
			origin = "imported"
		}
		for _, tc := range []struct {
			name     string
			kind     c.ToolSchemaKind
			options  []c.ToolInputSchemaOption
			optional bool
			want     bool
		}{
			{name: "exact_text", kind: c.ToolSchemaString, want: true},
			{name: "refined_text", kind: c.ToolSchemaString, options: []c.ToolInputSchemaOption{c.ToolSchemaPattern("^revision-")}, want: true},
			{name: "enum", kind: c.ToolSchemaString, options: []c.ToolInputSchemaOption{c.ToolSchemaEnum("ready")}},
			{name: "one_of_enum", kind: c.ToolSchemaString, options: []c.ToolInputSchemaOption{c.ToolSchemaOneOf(c.MustToolInputSchema(c.ToolSchemaString, c.ToolSchemaEnum("ready")))}},
			{name: "uuid", kind: c.ToolSchemaString, options: []c.ToolInputSchemaOption{c.ToolSchemaFormat("uuid")}},
			{name: "timestamp", kind: c.ToolSchemaString, options: []c.ToolInputSchemaOption{c.ToolSchemaFormat("date-time")}},
			{name: "integer", kind: c.ToolSchemaInteger},
			{name: "optional", kind: c.ToolSchemaString, optional: true},
		} {
			t.Run(origin+"/"+tc.name, func(t *testing.T) {
				bundle := loopValidationBundle()
				plan := &bundle.Semantics.Loops[0]
				plan.Escape.Emit = c.EmitSpec{Event: "review.escalated", Fields: map[string]c.ExpressionValue{"revision_id": c.RefExpression("loop.revision_id")}}
				bundle.Events["review.escalated"] = loopRevisionEvent("revision_id")
				exact := c.MustToolInputSchema(tc.kind, tc.options...)
				entries := map[string]c.EventCatalogEntry{}
				for event, entry := range bundle.Events {
					if _, present := entry.Payload.Properties["revision_id"]; !present {
						continue
					}
					entry.Payload.Properties["revision_id"] = c.EventFieldSpec{ExactSchema: &exact}
					if tc.optional {
						entry.Payload.Required = nil
					}
					entries[event] = entry
					if !imported {
						bundle.Events[event] = entry
					}
				}
				source := compileBootverifySchemasPreservingPlans(bundle)
				if imported {
					overlay := loopEffectiveSchemaSource{Source: source, entries: entries, schemas: map[string]c.CompiledEventSchema{}}
					for event, entry := range entries {
						schema, err := c.CompileImportedEventSchema(".", event, entry, c.CompiledEventSchemaSource{Layer: "provider_trigger_pack"})
						if err != nil {
							t.Fatal(err)
						}
						overlay.schemas[event] = schema
						delete(bundle.Events, event)
						if _, found := c.ResolveEventFieldType(bundle, ".", event, "revision_id"); found {
							t.Fatal("imported schema leaked into the underlying catalog")
						}
					}
					source = overlay
				}
				plan = &bundle.Semantics.Loops[0]
				for _, index := range []int{1, 2, 3} {
					operation := plan.Operations[index]
					if got := validateLoopInputRevision(source, *plan, operation); (len(got) == 0) != tc.want {
						t.Errorf("%s input findings = %#v, valid=%v", operation.Kind, got, tc.want)
					}
				}
				for _, index := range []int{0, 1, 2} {
					operation := plan.Operations[index]
					handler := bundle.Nodes["controller"].EventHandlers[operation.HandlerEvent]
					if got := validateLoopEmitCarriage(source, *plan, operation, handler); (len(got) == 0) != tc.want {
						t.Errorf("%s emit findings = %#v, valid=%v", operation.Kind, got, tc.want)
					}
				}
				if got := validateLoopEscapeEmit(source, *plan); (len(got) == 0) != tc.want {
					t.Errorf("escape findings = %#v, valid=%v", got, tc.want)
				}
				handler := bundle.Nodes["controller"].EventHandlers["review.issues"]
				handler.Loop = nil
				bundle.Nodes["controller"].EventHandlers["review.issues"] = handler
				topology, _ := semanticview.WorkflowStageTopology(source, ".")
				got := validateLoopRegionHandlers(source, *plan, topology, stringSet(plan.RegionStages))
				if loopFindingContains(got, "omits both loop operation and required text revision field") == tc.want || !loopFindingContains(got, "omits") {
					t.Errorf("region findings = %#v, typed=%v", got, tc.want)
				}
			})
		}
	}
}
