package contracts

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestDerivedFlowShapeRejectsAuthoredMode(t *testing.T) {
	for _, value := range []string{"static", "template", "singleton", "null", "''", "{}", "[]", "&shape static\nname: *shape"} {
		t.Run(value, func(t *testing.T) {
			source := "stages: []\nmode: " + value + "\n"
			for _, admission := range []struct {
				name string
				run  func() error
			}{
				{"typed", func() error { _, err := admitSchemaFragment(source); return err }},
				{"direct", func() error { var schema FlowSchemaDocument; return decodeNodeTestYAML([]byte(source), &schema) }},
				{"file", func() error { _, err := loadSchemaFragment(t, source); return err }},
			} {
				t.Run(admission.name, func(t *testing.T) {
					if err := admission.run(); err == nil || !strings.Contains(err.Error(), `schema field "mode" is not supported`) || !strings.Contains(err.Error(), "Valid fields: auto_emit_on_create") {
						t.Fatalf("mode admission = %v; want presence rejection with current schema vocabulary", err)
					}
				})
			}
		})
	}
	if _, err := admitSchemaFragment("stages: []\nmode: static\nmode: template\n"); err == nil {
		t.Fatal("duplicate mode admitted")
	}
	for _, tc := range []struct{ source, mode string }{
		{"stages: []\n", FlowModeStatic},
		{"instance: case_id\nstages: []\n", FlowModeTemplate},
	} {
		schema, err := admitSchemaFragment(tc.source)
		if err != nil || schema.EffectiveMode() != tc.mode {
			t.Fatalf("derived shape = %s, %v; want %s", schema.EffectiveMode(), err, tc.mode)
		}
		provenance := schema.admissionProvenance["mode"]
		if provenance.Origin != EffectiveValueOriginDerived || provenance.RuleID != "flow.shape_from_instance" {
			t.Fatalf("shape provenance = %#v", provenance)
		}
	}
}

func TestConnectionPoliciesRemainEdgeLocal(t *testing.T) {
	repo := repoRootForContractsTest(t)
	for _, reverse := range []bool{false, true} {
		bundle, err := LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyConnectionPolicies(t, reverse), DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatal(err)
		}
		pin, ok := bundle.FlowInputEventPin("worker", "work.ready")
		if !ok || pin.EventType() != "work.ready" {
			t.Fatalf("edge policy leaked into shared pin: %#v", pin)
		}
		seen := map[FlowInputResolutionMode]string{}
		for _, connect := range bundle.CompositionConnects() {
			input, ok, err := bundle.ConnectionInput(connect)
			if err != nil || !ok {
				t.Fatalf("connection evidence: %v, %t", err, ok)
			}
			evidence := input.SourceEvidence()
			seen[input.Mode()] = evidence.Source.Path
			evidence.SourceType.Catalog.Types = nil
			if got := input.SourceEvidence(); got.Source.Path != connect.KeyFrom || got.Field.Path() != "worker_id" {
				t.Fatalf("immutable edge source drifted: %#v", got)
			}
		}
		if seen[FlowInputResolutionModeCreate] != "payload.creation_id" || seen[FlowInputResolutionModeSelectOrCreate] != "payload.reuse_id" {
			t.Fatalf("independent policies drifted with reverse=%t: %#v", reverse, seen)
		}
	}
}

func TestConnectionResolutionClosedAdmission(t *testing.T) {
	for _, tc := range canonicalrouting.ConnectionAdmissionCases() {
		t.Run(tc.Name, func(t *testing.T) {
			schema, err := admitSchemaFragment(tc.Source)
			if tc.WantError != "" {
				if err == nil || tc.WantError != "*" && !strings.Contains(err.Error(), tc.WantError) {
					t.Fatalf("invalid policy admitted or teaching lost: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.Mode != "" && FlowInputResolutionModeCode(schema.Connect[0].Resolution) != tc.Mode {
				t.Fatalf("edge resolution = %#v", schema.Connect)
			}
		})
	}
}

func TestCompiledConnectionInputEvidenceIsDetached(t *testing.T) {
	catalog := TypeCatalogDocument{Types: map[string]NamedTypeDecl{
		"Key": {Fields: map[string]TypeFieldSpec{"name": {Type: "text"}}},
	}}
	input := CompiledConnectionInput{value: &compiledConnectionInputValue{
		evidence: FlowInputInstanceSourceTypeEvidence{
			SourceType:   CatalogTypeReference{Catalog: catalog},
			ReceiverType: CatalogTypeReference{Catalog: catalog},
		},
	}}
	returned := input.SourceEvidence()
	returned.SourceType.Catalog.Types["Key"].Fields["name"] = TypeFieldSpec{Type: "integer"}
	delete(returned.ReceiverType.Catalog.Types, "Key")
	for _, got := range []CatalogTypeReference{input.SourceEvidence().SourceType, input.SourceEvidence().ReceiverType} {
		if got.Catalog.Types["Key"].Fields["name"].Type != "text" {
			t.Fatal("returned type catalog mutated compiled connection evidence")
		}
	}
}

func TestEdgeResolutionHasNoPinInitializationAuthority(t *testing.T) {
	repo := repoRootForContractsTest(t)
	for _, mode := range []FlowInputResolutionMode{FlowInputResolutionModeCreate, FlowInputResolutionModeSelectOrCreate, FlowInputResolutionModeSelect} {
		t.Run(FlowInputResolutionModeCode(mode), func(t *testing.T) {
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyReceiverInitialization(t), DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			schema := *bundle.RootSchema
			for i := range schema.Connect {
				if schema.Connect[i].To == "account" && schema.Connect[i].Event == "account.ready" {
					schema.Connect[i].Resolution = mode
				}
			}
			bundle.RootSchema = &schema
			bundle.FlowTree.Root.Schema = schema
			err = CompileWorkflowSemantics(bundle)
			if err != nil {
				t.Fatal(err)
			}
			if err := bundle.ConnectionInputs().ValidateBindings(); err != nil {
				t.Fatal(err)
			}
			pin, ok := bundle.FlowInputEventPin("account", "account.ready")
			if !ok || pin.EventType() != "account.ready" {
				t.Fatal("initialization manufactured shared-pin selection policy")
			}
			if _, present := reflect.TypeOf(pin).MethodByName("Initialization"); present {
				t.Fatal("pin regained construction authority")
			}
		})
	}
}
