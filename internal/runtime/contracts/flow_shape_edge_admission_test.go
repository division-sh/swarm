package contracts

import (
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"strings"
	"testing"
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
					if err := admission.run(); err == nil || !strings.Contains(err.Error(), "mode") || !strings.Contains(err.Error(), "omit mode") {
						t.Fatalf("retired mode admission = %v; want presence rejection and teaching", err)
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
		if !ok || !pin.Resolution().Empty() {
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
