package packs_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestW5ToolSchemaSharedConsumerParity(t *testing.T) {
	for _, text := range []string{
		"{}",
		"{type: string, pattern: '^a+$', minLength: 0, maxLength: 3, enum: [a, aa]}",
		"{type: number, minimum: -0.5, maximum: 1.5, enum: [0, 1.5]}",
		"{type: object, properties: {left: {type: integer}, right: {type: integer, x-swarm-equalTo: left}}, required: [left, right], additionalProperties: false}",
		"{type: array, items: {}, minItems: 0, maxItems: 3}",
		"{type: null, enum: [null]}",
		"{type: string, minLength: 1.5}", "{type: string, minLength: -0.5}", "{type: string, minLength: 1.0}",
		"{type: string, minLength: null}", "{type: string, minLength: '1'}", "{type: string, minLength: .inf}",
		"{type: number, minimum: .nan}", "{type: number, maximum: .inf}",
		"{type: string, properties: {}}", "{type: string, required: []}",
		"{type: object, properties: {x: {type: string, unknown: null}}}",
	} {
		t.Run(text, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(text))
			if err != nil {
				t.Fatal(err)
			}
			value := snapshot.Document("schema.yaml").Root()
			expected, expectedError := contracts.AdmitToolInputSchemaValue(value)
			consumers := map[string]func() (contracts.ToolInputSchema, error){
				"tool": func() (contracts.ToolInputSchema, error) {
					s, err := yamlsource.Load([]byte("probe: {input_schema: " + text + "}\n"))
					if err != nil {
						return contracts.ToolInputSchema{}, err
					}
					tools, err := contracts.AdmitToolDeclarationsValue(s.Document("tools.yaml").Root())
					return tools["probe"].InputSchema(), err
				},
				"connector": func() (contracts.ToolInputSchema, error) {
					manifest, err := providerconnectors.ParseConnectorManifest([]byte("provider: probe\ntools: {probe: {input_schema: " + text + "}}\n"))
					return manifest.Tools["probe"].InputSchema(), err
				},
				"pack_interface": func() (contracts.ToolInputSchema, error) {
					spec, err := contracts.ParsePlatformSpecDocument([]byte("interfaces:\n  probe:\n    v1:\n      kind: channel\n      schemas: {probe: "+text+"}\n      operations: {check: {effect_class: non_idempotent_write}}\n      events: {checked: {required_fields: {value: {schema: probe}}}}\n"), "platform-spec.yaml")
					return spec.Interfaces["probe"]["v1"].Schemas["probe"], err
				},
				"channel_opaque": func() (contracts.ToolInputSchema, error) {
					manifest, err := packs.ParseChannelManifest([]byte("provider: probe\nopaque_types: {probe: " + text + "}\noperations: {check: {tool: probe.check}}\nevents: {checked: {event: probe.checked, fields: {value: payload.value}}}\n"))
					return manifest.OpaqueTypes["probe"], err
				},
				"trigger": func() (contracts.ToolInputSchema, error) {
					manifest, err := providertriggers.ParseManifest([]byte("provider: probe\nnormalized_events:\n- event: probe.observed\n  fields: {value: {from: payload.value, schema: " + text + "}}\n"))
					if err != nil {
						return contracts.ToolInputSchema{}, err
					}
					return manifest.NormalizedEvents[0].Fields["value"].Schema, nil
				},
				"programmatic": func() (contracts.ToolInputSchema, error) {
					var raw map[string]any
					if err := value.Project(&raw); err != nil {
						return contracts.ToolInputSchema{}, err
					}
					return contracts.AdmitToolInputSchemaMap(raw)
				},
			}
			for name, consumer := range consumers {
				t.Run(name, func(t *testing.T) {
					got, err := consumer()
					if (err == nil) != (expectedError == nil) {
						t.Fatalf("admission differs: %v / %v", err, expectedError)
					}
					if err != nil {
						return
					}
					if !got.Equal(expected) || !reflect.DeepEqual(got.Projection(), expected.Projection()) {
						t.Fatalf("typed schema drift: %#v / %#v", got.Projection(), expected.Projection())
					}
					for _, candidate := range []any{nil, 0, 0.25, "a", "aaaa", []any{false}, map[string]any{"left": 1, "right": 2}, map[string]any{"left": 1, "right": 1}} {
						if (got.Validate(candidate) == nil) != (expected.Validate(candidate) == nil) {
							t.Fatalf("destination semantics differ for %#v", candidate)
						}
					}
				})
			}
		})
	}
	for _, source := range []string{"{<<: {minLength: 1.5}, type: string}", "{type: object, properties: {value: &p {type: string}, other: *p}}"} {
		_, err := packs.ParseChannelManifest([]byte("provider: probe\nopaque_types: {probe: " + source + "}\noperations: {check: {tool: probe.check}}\nevents: {checked: {event: probe.checked, fields: {value: payload.value}}}\n"))
		if strings.Contains(source, "1.5") && (err == nil || !strings.Contains(err.Error(), "channel.yaml")) {
			t.Fatalf("merged bound diagnostic: %v", err)
		}
		if !strings.Contains(source, "1.5") && err != nil {
			t.Fatal(err)
		}
	}
}
