package contracts

import (
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestReceiverVariableProjectionPreservesPresenceAndRefinements(t *testing.T) {
	for _, raw := range []string{"integer", "{type: json, default: null}", "text?", "{type: text, pattern: '^ok', length: {min: 2}}", "{type: integer, range: {max: 4}, default: 3}", "{type: integer, equal_to: sibling}"} {
		var before, after FlowVariable
		if err := yaml.Unmarshal([]byte(raw), &before); err != nil {
			t.Fatal(err)
		}
		encoded, err := yaml.Marshal(before)
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal(encoded, &after); err != nil {
			t.Fatalf("%s: %v", encoded, err)
		}
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("%s changed: %#v => %#v", raw, before, after)
		}
	}
}

func TestReceiverConfigurationConsumesSharedSiblingRefinements(t *testing.T) {
	var variables FlowInstanceVariables
	if err := yaml.Unmarshal([]byte("variables:\n  left: integer\n  right: {type: integer, equal_to: left}\n"), &variables); err != nil {
		t.Fatal(err)
	}
	config, err := CompileReceiverConfiguration(variables, TypeCatalogDocument{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := config.Admit(map[string]any{"left": 3, "right": 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Admit(map[string]any{"left": 3, "right": 4}); err == nil {
		t.Fatal("sibling refinement bypassed")
	}
	variables.Variables["right"] = FlowVariable{Type: "integer", Refinements: SchemaRefinements{EqualTo: "missing"}}
	if _, err := CompileReceiverConfiguration(variables, TypeCatalogDocument{}); err == nil {
		t.Fatal("undeclared equal_to accepted")
	}
}

func TestReceiverVariableTypeAndDefaultAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		wantErr      bool
	}{
		{"scalar", "integer", false},
		{"default", "{type: integer, default: 3}", false},
		{"absent type", "{default: 3}", true},
		{"wrong default", "{type: integer, default: bad}", true},
		{"null default", "{type: integer, default: null}", true},
		{"unknown key", "{type: integer, value: 3}", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var variable FlowVariable
			err := yaml.Unmarshal([]byte(tc.source), &variable)
			if err == nil {
				_, err = CompileReceiverConfiguration(FlowInstanceVariables{Variables: map[string]FlowVariable{"count": variable}}, TypeCatalogDocument{})
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
	var absent, null FlowVariable
	if err := yaml.Unmarshal([]byte("{type: integer}"), &absent); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte("{type: integer, default: null}"), &null); err != nil {
		t.Fatal(err)
	}
	if absent.HasDefault || !null.HasDefault {
		t.Fatal("lost default presence")
	}
}

func TestReceiverConfigurationValidatesTypedMapKeys(t *testing.T) {
	catalog := TypeCatalogDocument{Enums: map[string]EnumTypeDecl{"Region": {Values: []string{"east", "west"}, Default: "east"}}}
	for _, tc := range []struct {
		typeRef, valid, invalid string
	}{
		{"map[Region]integer", "east", "north"},
		{"map[uuid]integer", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "not-a-uuid"},
	} {
		t.Run(tc.typeRef, func(t *testing.T) {
			config, err := CompileReceiverConfiguration(FlowInstanceVariables{Variables: map[string]FlowVariable{"values": {Type: tc.typeRef}}}, catalog)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := config.Admit(map[string]any{"values": map[string]any{tc.valid: 7}}); err != nil {
				t.Fatal(err)
			}
			if _, err := config.Admit(map[string]any{"values": map[string]any{tc.invalid: 7}}); err == nil {
				t.Fatal("invalid map key accepted")
			}
		})
	}
}

func TestReceiverInitializationMissingNullAndInvalidNeverDefault(t *testing.T) {
	config, err := CompileReceiverConfiguration(FlowInstanceVariables{Variables: map[string]FlowVariable{
		"count": {Type: "integer", HasDefault: true, Default: 3},
		"flag":  {Type: "boolean", HasDefault: true, Default: true},
	}}, TypeCatalogDocument{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		values  map[string]any
		wantErr bool
	}{
		{"absent", nil, false},
		{"zero false", map[string]any{"count": 0, "flag": false}, false},
		{"null", map[string]any{"count": nil}, true},
		{"wrong", map[string]any{"count": "3"}, true},
		{"undeclared", map[string]any{"other": true}, true},
		{"unsafe", map[string]any{"count": uint64(1) << 63}, true},
		{"nonfinite", map[string]any{"count": math.Inf(1)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := config.Admit(tc.values)
			if (err != nil) != tc.wantErr {
				t.Fatalf("got=%#v error=%v", got, err)
			}
			if tc.name == "absent" && got["count"] != int64(3) {
				t.Fatalf("default=%#v", got)
			}
			if tc.name == "zero false" && (got["count"] != int64(0) || got["flag"] != false) {
				t.Fatalf("false/zero replaced: %#v", got)
			}
		})
	}
}

func TestReceiverInitializationValuesAreIsolated(t *testing.T) {
	defaultValue := map[string]any{"nested": []any{float64(2), int64(2)}}
	config, err := CompileReceiverConfiguration(FlowInstanceVariables{Variables: map[string]FlowVariable{
		"value": {Type: "json", HasDefault: true, Default: defaultValue},
	}}, TypeCatalogDocument{})
	if err != nil {
		t.Fatal(err)
	}
	defaultValue["nested"].([]any)[0] = "changed"
	first, err := config.Admit(nil)
	if err != nil {
		t.Fatal(err)
	}
	first["value"].(map[string]any)["nested"].([]any)[1] = "changed"
	second, err := config.Admit(nil)
	if err != nil {
		t.Fatal(err)
	}
	values := second["value"].(map[string]any)["nested"].([]any)
	if values[0] != float64(2) || values[1] != int64(2) {
		t.Fatalf("aliased or erased kinds: %#v", values)
	}
	if _, err := config.Admit(map[string]any{"value": string([]byte{0xff})}); err == nil {
		t.Fatal("invalid UTF-8 admitted")
	}
}

func TestReceiverInitializeGrammar(t *testing.T) {
	for _, raw := range []string{"null", "{}", "{count: payload}", "{count: entity.count}", "{count: {from: payload.count}}", "{count: payload..count}"} {
		var pins FlowInputPins
		err := yaml.Unmarshal([]byte("events:\n  - event: work.requested\n    resolution: {mode: create}\n    initialize: "+raw+"\n"), &pins)
		if err == nil {
			t.Errorf("admitted %s", raw)
		}
	}
	var pins FlowInputPins
	if err := yaml.Unmarshal([]byte("events:\n  - event: work.requested\n    resolution: {mode: create}\n    initialize: {count: payload.settings.count}\n"), &pins); err != nil {
		t.Fatal(err)
	}
	if got := pins.EventPins[0].Initialize["count"]; got != "payload.settings.count" {
		t.Fatal(got)
	}
}

func TestReceiverInitializationBindsExactPayloadAndRejectsContradictions(t *testing.T) {
	config, err := CompileReceiverConfiguration(FlowInstanceVariables{Variables: map[string]FlowVariable{"count": {Type: "integer", HasDefault: true, Default: 3}}}, TypeCatalogDocument{})
	if err != nil {
		t.Fatal(err)
	}
	catalog := TypeCatalogDocument{Types: map[string]NamedTypeDecl{"Settings": {Fields: map[string]TypeFieldSpec{"count": {Type: "integer", IsOptional: true}}}}}
	schema, err := newCompiledEventSchema("", "work.requested", EventCatalogEntry{Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{"settings": {Type: "Settings"}}}}, catalog, "", CompiledEventSchemaSource{})
	if err != nil {
		t.Fatal(err)
	}
	bindings := map[string]string{"count": "payload.settings.count"}
	plan, err := CompileReceiverInitialization(config, bindings, schema)
	if err != nil {
		t.Fatal(err)
	}
	bindings["count"] = "payload.other"
	for _, tc := range []struct {
		name    string
		payload map[string]any
		want    any
		wantErr string
	}{
		{"exact", map[string]any{"settings": map[string]any{"count": int64(7)}, "count": 99}, int64(7), ""},
		{"absent", map[string]any{"settings": map[string]any{}}, int64(3), ""},
		{"null", map[string]any{"settings": map[string]any{"count": nil}}, nil, "receiver variable count"},
		{"broken parent", map[string]any{"settings": nil}, nil, "non-object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := plan.Evaluate(tc.payload)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got=%#v err=%v", got, err)
				}
				return
			}
			if err != nil || got["count"] != tc.want {
				t.Fatalf("got=%#v err=%v", got, err)
			}
		})
	}
	for _, bindings := range []map[string]string{{"other": "payload.settings.count"}, {"count": "payload.missing"}, {"count": "payload.settings"}} {
		if _, err := CompileReceiverInitialization(config, bindings, schema); err == nil {
			t.Fatalf("admitted %#v", bindings)
		}
	}
}

func TestCompiledInputInitializeIsImmutableAndDigestSensitive(t *testing.T) {
	schema, err := newCompiledEventSchema("", "work.requested", EventCatalogEntry{Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
		"count": {Type: "integer"}, "other": {Type: "integer"},
	}}}, TypeCatalogDocument{}, "", CompiledEventSchemaSource{})
	if err != nil {
		t.Fatal(err)
	}
	makeConfig := func(defaultValue int) ReceiverConfiguration {
		c, err := CompileReceiverConfiguration(FlowInstanceVariables{Variables: map[string]FlowVariable{"count": {Type: "integer", HasDefault: true, Default: defaultValue}}}, TypeCatalogDocument{})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	context := FlowPinCompilationContext{FlowID: "worker", FlowPath: "worker", EventSchema: schema, Configuration: makeConfig(3)}
	authored := FlowInputEventPin{Event: "work.requested", Resolution: FlowInputPinResolution{Mode: FlowInputResolutionModeCreate}, Initialize: map[string]string{"count": "payload.count"}}
	first, err := CompileFlowInputPin(context, authored)
	if err != nil {
		t.Fatal(err)
	}
	authored.Initialize["count"] = "payload.other"
	second, err := CompileFlowInputPin(context, authored)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() == second.Digest() {
		t.Fatal("binding path erased from digest")
	}
	context.Configuration = makeConfig(4)
	third, err := CompileFlowInputPin(context, authored)
	if err != nil {
		t.Fatal(err)
	}
	if third.Digest() == second.Digest() {
		t.Fatal("default erased from digest")
	}
	first.Initialization().Bindings()["count"] = "payload.other"
	got, err := first.Initialization().Evaluate(map[string]any{"count": 7, "other": 99})
	if err != nil || got["count"] != int64(7) {
		t.Fatalf("mutated binding: %#v %v", got, err)
	}
	for _, mode := range []FlowInputResolutionMode{FlowInputResolutionModeNone, FlowInputResolutionModeSelect, FlowInputResolutionModeFanIn, FlowInputResolutionModeReply} {
		authored.Resolution.Mode = mode
		if _, err := CompileFlowInputPin(context, authored); err == nil {
			t.Fatalf("initialize accepted for mode %v", mode)
		}
	}
}

func TestReceiverInitializationUsesNamedAndMapTypes(t *testing.T) {
	catalog := TypeCatalogDocument{Types: map[string]NamedTypeDecl{"Settings": {Fields: map[string]TypeFieldSpec{"count": {Type: "integer"}}}}}
	config, err := CompileReceiverConfiguration(FlowInstanceVariables{Variables: map[string]FlowVariable{
		"settings": {Type: "Settings"}, "labels": {Type: "map[text]integer"},
	}}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{"settings": map[string]any{"count": 3}, "labels": map[string]any{"a": 4}}
	if _, err := config.Admit(values); err != nil {
		t.Fatal(err)
	}
	values["labels"] = map[string]any{"a": "bad"}
	if _, err := config.Admit(values); err == nil {
		t.Fatal("bad map element admitted")
	}
	values["labels"] = map[string]any{}
	values["settings"] = map[string]any{}
	if _, err := config.Admit(values); err == nil {
		t.Fatal("missing named field admitted")
	}
}

func TestReceiverInitializationCreationCorpusLoads(t *testing.T) {
	root := repoRootForContractsTest(t)
	for _, fixture := range []string{
		"tier5-flow-lifecycle/test-create-flow-instance",
		"tier5-flow-lifecycle/test-create-flow-instance-config",
		"tier5-flow-lifecycle/test-create-flow-instance-duplicate",
		"tier5-flow-lifecycle/test-auto-emit-on-create",
		"tier11-flow-composition/test-dynamic-flow-instance",
		"tier9-composition-patterns/test-compose-create-instance-config",
	} {
		t.Run(fixture, func(t *testing.T) {
			bundle, err := LoadWorkflowContractBundleWithOverrides(root, filepath.Join(root, "tests", fixture), DefaultPlatformSpecFile(root))
			if err != nil {
				t.Fatal(err)
			}
			creates := 0
			for flowID := range bundle.FlowSchemas {
				for _, pin := range bundle.FlowInputEventPins(flowID) {
					if pin.Resolution().Mode == FlowInputResolutionModeCreate {
						creates++
					}
				}
			}
			if creates != 1 {
				t.Fatalf("creating receiver pins=%d, want1", creates)
			}
		})
	}
}

func TestReceiverVariableAliasesKeepPresenceAndRejectDuplicateDefaults(t *testing.T) {
	for _, text := range []string{
		"variables:\n  a: &integer {type: integer, default: 3}\n  b: *integer\n",
		"variables:\n  a: &integer {type: integer, default: 3}\n  b: {<<: *integer}\n",
	} {
		var variables FlowInstanceVariables
		if err := yaml.Unmarshal([]byte(text), &variables); err != nil {
			t.Fatal(err)
		}
		c, err := CompileReceiverConfiguration(variables, TypeCatalogDocument{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.Admit(nil)
		if err != nil || got["a"] != int64(3) || got["b"] != int64(3) {
			t.Fatalf("alias default %#v %v", got, err)
		}
	}
	var variables FlowInstanceVariables
	if err := yaml.Unmarshal([]byte("variables:\n  a: &integer {type: integer, default: 3}\n  b: {<<: *integer, default: 4}\n"), &variables); err == nil {
		t.Fatal("duplicate merged default accepted")
	}
}
