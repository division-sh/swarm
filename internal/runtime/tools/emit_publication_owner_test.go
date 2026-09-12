package tools

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestEmitPublicationSchemaRefusalRemainsVisibleToValidators(t *testing.T) {
	for _, mode := range []string{runtimecontracts.FlowModeStatic, "template"} {
		t.Run(mode, func(t *testing.T) {
			bundle := emitRoutePlanTestBundle([]emitRoutePlanTestFlow{{id: "left", mode: mode}, {id: "right", mode: mode}}, nil)
			for flow, fieldType := range map[string]string{"left": "MissingLeftType", "right": "text"} {
				bundle.FlowTree.ByID[flow].Events = map[string]runtimecontracts.EventCatalogEntry{
					"result.done": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"value": {Type: fieldType}}}},
				}
				toolTestDeclareAgent(t, bundle, flow+"-agent", flow, "result.done")
			}
			source := toolTestSourceWithDeclaredAgent(t, bundle, "right-agent", "right", "result.done")
			registry := NewEmitRegistry(source, nil)
			for _, flow := range []string{"left", "right"} {
				path := flow
				if mode == "template" {
					path += "/one"
				}
				actor := models.AgentConfig{ID: flow + "-agent", Identity: toolTestAgentIdentity(t, flow+"-agent", flow, path),
					FlowID: flow, FlowPath: path, Role: "shared-role", EmitEvents: []string{"result.done"}}
				definitions := registry.GenerateEmitToolsForActor(actor, nil)
				want := 1
				if flow == "left" {
					want = 0
				}
				if len(definitions) != want {
					t.Fatalf("%s generated tools=%d, want %d", flow, len(definitions), want)
				}
			}
			for name, errs := range map[string][]error{
				"provider": ValidateGeneratedEmitToolSchemasForSource(source),
				"closure":  ValidateGeneratedToolSchemaClosureForSource(source),
			} {
				if len(errs) != 1 || !strings.Contains(errs[0].Error(), "left-agent") || !strings.Contains(errs[0].Error(), "MissingLeftType") {
					t.Fatalf("%s hid failed schema generation or borrowed sibling: %v", name, errs)
				}
			}
		})
	}
}

func TestEmitPublicationSchemaAndPermissionRequireExactDeclaration(t *testing.T) {
	bundle := emitRoutePlanTestBundle([]emitRoutePlanTestFlow{
		{id: "left", mode: runtimecontracts.FlowModeStatic},
		{id: "right", mode: runtimecontracts.FlowModeStatic},
	}, nil)
	for flow, field := range map[string]string{"left": "left_value", "right": "right_value"} {
		bundle.FlowTree.ByID[flow].Events = map[string]runtimecontracts.EventCatalogEntry{
			"result.done": {Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{field: {Type: "text"}},
				Required:   []string{field},
			}},
			"undeclared.done": {},
		}
	}
	toolTestDeclareAgent(t, bundle, "left-agent", "left", "result.done")
	source := toolTestSourceWithDeclaredAgent(t, bundle, "right-agent", "right", "result.done")
	registry := NewEmitRegistry(source, nil)
	for _, flow := range []string{"left", "right"} {
		t.Run(flow, func(t *testing.T) {
			actor := models.AgentConfig{
				ID: flow + "-agent", Identity: toolTestAgentIdentity(t, flow+"-agent", flow, flow),
				FlowID: flow, FlowPath: flow, Role: "shared-role", EmitEvents: []string{"result.done"},
			}
			_, schema, ok := registry.EventSchemaForActorTool(actor, "emit_result_done")
			if !ok || ValidatePayloadAgainstSchema(schema.Schema, map[string]any{flow + "_value": "owned"}) != nil {
				t.Fatal("exact declaration did not select its own schema")
			}
			other := "left"
			if flow == "left" {
				other = "right"
			}
			if ValidatePayloadAgainstSchema(schema.Schema, map[string]any{other + "_value": "foreign"}) == nil {
				t.Fatal("same leaf selected sibling schema")
			}
			for _, forged := range []string{other + "/result.done", "undeclared.done", flow + "/ti-other/result.done"} {
				actor.EmitEvents = []string{forged}
				if _, _, ok := registry.EventSchemaForActorTool(actor, EmitToolName(forged)); ok {
					t.Fatalf("actor-only emit %q acquired declaration permission", forged)
				}
			}
			actor.EmitEvents = nil
			if tools := registry.GenerateEmitToolsForActor(actor, nil); len(tools) != 0 {
				t.Fatal("empty actor emit set acquired role-global tools")
			}
		})
	}
}

func TestEmitPublicationGlobalSchemaLookupDoesNotRescueForeignLeaf(t *testing.T) {
	schemas := map[string]EmitSchema{
		"left/result.done": {Description: "left only"},
	}
	for _, name := range []string{"result.done", "right/result.done"} {
		if _, ok := emitSchemaForEventType(schemas, name); ok {
			t.Fatalf("missing exact schema %q borrowed foreign leaf", name)
		}
	}
	if emitEventTypesEquivalent("left/result.done", "right/result.done") || emitEventTypesEquivalent("left/result.done", "result.done") {
		t.Fatal("role event comparison discarded scope")
	}
}
