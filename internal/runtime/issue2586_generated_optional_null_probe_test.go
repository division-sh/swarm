package runtime

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestIssue2586GeneratedActivityOptionalNullProbe(t *testing.T) {
	root := canonicalrouting.CopyGeneratedActivity(t, false, true)
	path := filepath.Join(root, "tools.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	oldOutput := "  output_schema:\n    type: object\n    properties:\n      delivered: {type: boolean}\n"
	newOutput := `  output_schema:
    type: object
    oneOf:
      - type: object
        properties: {label: {type: string}}
        required: []
        additionalProperties: false
      - type: object
        properties: {other: {type: string}}
        required: [other]
        additionalProperties: false
`
	if strings.Count(string(raw), oldOutput) != 1 {
		t.Fatal("generated activity fixture no longer has the exact output schema")
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(raw), oldOutput, newOutput, 1)), 0600); err != nil {
		t.Fatal(err)
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load real generated activity declaration: %v", err)
	}
	const eventName = "send.succeeded"
	t.Run("generated_schema_retained_and_not_importable", func(t *testing.T) {
		schema, ok, err := bundle.ResolveEffectiveCompiledFlowEventSchema(".", eventName)
		if err != nil || !ok {
			t.Fatalf("real loader did not compile the generated activity result: %v", err)
		}
		if schema.Classification() != runtimecontracts.CompiledEventSchemaGenerated || schema.Importable() {
			t.Fatalf("generated schema classification=%s importable=%t", schema.Classification(), schema.Importable())
		}
		properties := schema.AcceptanceSchema()["properties"].(map[string]any)
		result := properties["result"].(map[string]any)
		if branches, ok := result["oneOf"].([]any); !ok || len(branches) != 2 {
			t.Fatalf("generated result lost admitted alternatives: %#v", result)
		}
		if _, ok := bundle.DurableDataDeclarationByName(".", eventName); ok {
			t.Fatal("generated activity unexpectedly became a resource import declaration")
		}
	})
	admitter := testRuntimePayloadAdmitter(t, bundle)
	for _, tc := range []struct {
		name, result string
		wantSuccess  bool
	}{
		{"omitted_equivalent", `{}`, true},
		{"present_label", `{"label":"present"}`, true},
		{"present_other", `{"other":"present"}`, true},
		{"optional_null", `{"label":null}`, true},
		{"required_null_refuses", `{"other":null}`, false},
		{"wrong_branch_refuses", `{"label":"present","other":"present"}`, false},
		{"unknown_null_refuses", `{"unknown":null}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(`{"activity_id":"send","tool":"send","effect_class":"read_only","attempt":1,"result":` + tc.result + `}`)
			before := bytes.Clone(payload)
			event, err := admitRuntimePayload(admitter, eventName, payload)
			if !bytes.Equal(before, payload) {
				t.Fatal("admission changed the original caller bytes")
			}
			if !tc.wantSuccess {
				if err == nil {
					t.Fatal("invalid result acquired payload admission")
				}
				return
			}
			if err != nil {
				t.Fatalf("valid generated activity result refused: %v", err)
			}
			admission, ok := event.PayloadAdmission()
			if !ok || admission.Binding().SchemaClass() != events.PayloadSchemaGenerated {
				t.Fatal("successful admission lost the generated source binding")
			}
			if tc.name == "optional_null" {
				var got map[string]any
				if err := json.Unmarshal(admission.Payload(), &got); err != nil {
					t.Fatal(err)
				}
				if len(got["result"].(map[string]any)) != 0 {
					t.Fatal("optional null survived canonical admission")
				}
			}
		})
	}
}
