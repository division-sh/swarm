package semanticview

import (
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

type loopRevisionImportedSource struct {
	Source
	entries map[string]c.EventCatalogEntry
	schemas map[string]c.CompiledEventSchema
}

func (s loopRevisionImportedSource) ResolveEffectiveCompiledFlowEventSchema(flow, event string) (c.CompiledEventSchema, bool, error) {
	if schema, ok := s.schemas[event]; ok && flow == "." {
		return schema, true, nil
	}
	return s.Source.ResolveEffectiveCompiledFlowEventSchema(flow, event)
}

func (s loopRevisionImportedSource) ResolveFlowEventCatalogEntry(flow, event string) (c.EventCatalogEntry, string, bool) {
	if entry, ok := s.entries[event]; ok && flow == "." {
		return entry, event, true
	}
	return s.Source.ResolveFlowEventCatalogEntry(flow, event)
}

func TestOriginalLoopCarriageEffectiveRevisionSchemas(t *testing.T) {
	for _, imported := range []bool{false, true} {
		origin := "authored"
		if imported {
			origin = "imported"
		}
		for _, position := range []string{"input", "output", "escape"} {
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
				t.Run(origin+"/"+position+"/"+tc.name, func(t *testing.T) {
					bundle, node := loopCarriageBundle(t)
					event := "review.passed"
					scope := LoopEventScope{FlowID: ".", EventType: event}
					if position == "output" {
						event = "review.requested"
						scope = LoopEventScope{FlowID: ".", EventType: event, Producer: node, HandlerEvent: "start"}
					}
					if position == "escape" {
						event = "review.escalated"
						bundle.Events[event] = bundle.Events["review.passed"]
						emit := c.EmitSpec{Event: event, Fields: map[string]c.ExpressionValue{"revision_id": c.RefExpression("loop.revision_id")}}
						bundle.Semantics.Loops[0].Escape.Emit = emit
						bundle.Semantics.Loops[0].Operations = append(bundle.Semantics.Loops[0].Operations, c.WorkflowLoopOperationPlan{Node: node, HandlerEvent: "review.issues", Kind: c.LoopOperationRepeat, LoopID: "revision"})
						bundle.Events["review.issues"] = bundle.Events["review.passed"]
						bundle.Nodes["controller"].EventHandlers["review.issues"] = c.SystemNodeEventHandler{Loop: &c.LoopOperationSpec{Repeat: "revision", From: "review"}}
						scope = LoopEventScope{FlowID: ".", EventType: event, Producer: node, HandlerEvent: "review.issues"}
					}
					entry := bundle.Events[event]
					exact := c.MustToolInputSchema(tc.kind, tc.options...)
					entry.Payload.Properties = map[string]c.EventFieldSpec{"revision_id": {ExactSchema: &exact}}
					if tc.optional {
						entry.Payload.Required = nil
					}
					if !imported {
						bundle.Events[event] = entry
					}
					compileLoopCarriageFixtureSchemas(t, bundle)
					var source Source = Wrap(bundle)
					if imported {
						schema, err := c.CompileImportedEventSchema(".", event, entry, c.CompiledEventSchemaSource{Layer: "provider_trigger_pack"})
						if err != nil {
							t.Fatal(err)
						}
						delete(bundle.Events, event)
						bundle.FlowTree.Root.Events = bundle.Events
						compileLoopCarriageFixtureSchemas(t, bundle)
						source = loopRevisionImportedSource{Source: source, entries: map[string]c.EventCatalogEntry{event: entry}, schemas: map[string]c.CompiledEventSchema{event: schema}}
						if _, found := c.ResolveEventFieldType(bundle, ".", event, "revision_id"); found {
							t.Fatal("imported schema leaked into the underlying catalog")
						}
					}
					owner, err := compileLoopCarriage(source)
					if (err == nil) != tc.want {
						t.Fatalf("carriage error = %v, valid=%v", err, tc.want)
					}
					if tc.want {
						role, found, err := owner.resolve(scope)
						if err != nil || !found || role.RevisionField() != "revision_id" {
							t.Fatalf("carriage role = %+v/%v/%v", role, found, err)
						}
					}
				})
			}
		}
	}
}
