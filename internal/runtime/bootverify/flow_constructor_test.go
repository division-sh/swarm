package bootverify

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func constructorFixture(t *testing.T, fields, payload string) semanticview.Source {
	return constructorReaderFixture(t, fields, payload, "")
}

func constructorReaderFixture(t *testing.T, fields, payload, handler string) semanticview.Source {
	return constructorCatalogFixture(t, fields, payload, handler, "")
}

func constructorCatalogFixture(t *testing.T, fields, payload, handler, catalog string) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyFlowConstructorSupply(t, fields, payload, handler, catalog)
	repo := repoRootForBootverifyTest(t)
	return semanticview.Wrap(loadFixtureBundleAt(t, repo, root, c.DefaultPlatformSpecFile(repo)))
}

type composedConstructorSchemaSource struct{ semanticview.Source }

func (composedConstructorSchemaSource) ResolveFlowEventStructuralType(string, string) (c.ResolvedCatalogType, bool) {
	return c.ResolvedCatalogType{}, false
}

func TestFlowConstructorConsumesEffectiveSchema(t *testing.T) {
	source := composedConstructorSchemaSource{constructorFixture(t, "  brief: text\n", "  brief: text\n")}
	constructor, err := pipeline.CompileFlowConstructor(source, "consumer", "deploy.done")
	if err != nil || !constructor.Eligible() {
		t.Fatalf("effective-schema constructor: %+v %v", constructor.Refusals(), err)
	}
	fields, err := constructor.InitialFields(map[string]any{"vertical_id": "v1", "brief": "scope"}, "v1")
	if err != nil || fields["brief"] != "scope" || fields["vertical_id"] != "v1" {
		t.Fatalf("effective-schema projection: %v %v", fields, err)
	}
}

func TestFlowConstructorCatalogSupplyAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, catalog, target, supplied string
		valid                           bool
	}{
		{"map", "", "map[text]text", "map[text]text", true},
		{"nested map", "", "list<map[text]text>", "list<map[text]text>", true},
		{"map value mismatch", "", "map[text]integer", "map[text]text", false},
		{"enum", "enums:\n  Mode: {values: [low, high], default: low}\n", "Mode", "Mode", true},
		{"unbounded enum supply", "enums:\n  Mode: {values: [low, high], default: low}\n", "Mode", "text", false},
		{"enum map key", "enums:\n  Mode: {values: [low, high], default: low}\n", "map[Mode]text", "map[Mode]text", true},
		{"unbounded map key", "enums:\n  Mode: {values: [low, high], default: low}\n", "map[Mode]text", "map[text]text", false},
		{"record", "types:\n  Brief:\n    detail: text\n", "Brief", "Brief", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := constructorCatalogFixture(t, "  brief: "+tc.target+"\n", "  brief: "+tc.supplied+"\n", "", tc.catalog)
			_, err := engine.BuildConstructorAssignmentAnalysis(source, "consumer", "deploy.done")
			if (err == nil) != tc.valid {
				t.Fatalf("supply %s -> %s: valid=%v err=%v", tc.supplied, tc.target, tc.valid, err)
			}
		})
	}
}

func TestFlowConstructorEligibilityUsesAssignmentChecker(t *testing.T) {
	for _, tc := range []struct {
		name, payload, handler string
		eligible               bool
	}{
		{"supplied initial read", "  brief: text\n", "      guard: {check: entity.brief != ''}\n", true},
		{"optional is not guaranteed", "  brief: text?\n", "      guard: {check: entity.brief != ''}\n", false},
		{"missing initial read", "", "      guard: {check: entity.brief != ''}\n", false},
		{"guard cannot borrow later write", "", "      guard: {check: entity.brief != ''}\n      data_accumulation:\n        writes: [{target_field: brief, value: 'supplied'}]\n", false},
		{"read after definite write", "", "      data_accumulation:\n        writes: [{target_field: brief, value: 'supplied'}, {target_field: copy, value: entity.brief}]\n", true},
		{"explicit optional presence", "  brief: text?\n", "      guard:\n        check: >-\n          !has(entity.brief) || entity.brief != \"\"\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := constructorReaderFixture(t, "  brief: text\n  copy: text\n", tc.payload, tc.handler)
			constructor, err := pipeline.CompileFlowConstructor(source, "consumer", "deploy.done")
			if err != nil {
				t.Fatal(err)
			}
			if constructor.Eligible() != tc.eligible {
				t.Fatalf("constructor eligible=%v, want %v; refusals=%v", constructor.Eligible(), tc.eligible, constructor.Refusals())
			}
		})
	}
}

func TestFlowConstructorSuppliedInitialFacts(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		want          bool
	}{
		{"required", "  brief: text\n", true},
		{"optional", "  brief: text?\n", false},
		{"absent", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := constructorFixture(t, "  brief: text\n  count: {type: integer, initial: 0}\n", tc.payload)
			analysis, err := engine.BuildConstructorAssignmentAnalysis(source, "consumer", "deploy.done")
			if err != nil {
				t.Fatal(err)
			}
			facts := analysis.StageFacts("initial")
			if facts.Has("brief") != tc.want {
				t.Fatalf("supplied brief assigned=%v, want %v", facts.Has("brief"), tc.want)
			}
			if !facts.Has("vertical_id") || !facts.Has("count") {
				t.Fatalf("resolved key and internal literal must be initial facts: %#v", facts)
			}
		})
	}
}

func TestFlowConstructorProjectionAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, fields, payload, refusal string
	}{
		{"internal collision", "  count: {type: integer, initial: 0}\n", "  count: integer\n", "collides with internal"},
		{"incompatible", "  brief: text\n", "  brief: integer\n", "incompatible"},
		{"unconstrained to pattern", "  brief: {type: text, pattern: '^[A-Z]+$'}\n", "  brief: text\n", "incompatible"},
		{"matching pattern", "  brief: {type: text, pattern: '^[A-Z]+$'}\n", "  brief: {type: text, pattern: '^[A-Z]+$'}\n", ""},
		{"weaker length", "  brief: {type: text, length: {min: 4}}\n", "  brief: {type: text, length: {min: 2}}\n", "incompatible"},
		{"stronger length", "  brief: {type: text, length: {min: 2}}\n", "  brief: {type: text, length: {min: 4}}\n", ""},
		{"message only", "", "  message: text\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := constructorFixture(t, tc.fields, tc.payload)
			_, err := engine.BuildConstructorAssignmentAnalysis(source, "consumer", "deploy.done")
			if tc.refusal == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.refusal) {
				t.Fatalf("want %q refusal, got %v", tc.refusal, err)
			}
		})
	}
}

func TestFlowConstructorRuntimeProjection(t *testing.T) {
	source := constructorFixture(t, "  brief: text\n  note: text?\n  count: {type: integer, initial: 0}\n", "  brief: text\n  message: text?\n")
	constructor, err := pipeline.CompileFlowConstructor(source, "consumer", "deploy.done")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		payload map[string]any
		key     any
		valid   bool
	}{
		{"supplied projection", map[string]any{"vertical_id": "v1", "brief": "scope", "message": "message-only"}, "v1", true},
		{"contradictory key", map[string]any{"vertical_id": "v2", "brief": "scope"}, "v1", false},
		{"wrong key type", map[string]any{"vertical_id": "v1", "brief": "scope"}, 7, false},
		{"missing resolved key", map[string]any{"vertical_id": "v1", "brief": "scope"}, nil, false},
		{"missing required payload", map[string]any{"vertical_id": "v1"}, "v1", false},
		{"malformed payload", map[string]any{"vertical_id": "v1", "brief": 7}, "v1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields, err := constructor.InitialFields(tc.payload, tc.key)
			if !tc.valid {
				if err == nil || fields != nil {
					t.Fatalf("invalid constructor produced fields=%v err=%v", fields, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if fields["vertical_id"] != "v1" || fields["brief"] != "scope" {
				t.Fatalf("projection lost supplied fields: %v", fields)
			}
			if _, ok := fields["count"]; !ok {
				t.Fatalf("projection lost internal literal: %v", fields)
			}
			for _, name := range []string{"message", "note"} {
				if _, ok := fields[name]; ok {
					t.Fatalf("projection fabricated %s: %v", name, fields)
				}
			}
		})
	}
}

func TestFlowConstructorVerificationConsumesContract(t *testing.T) {
	for _, tc := range []struct {
		name, fields, payload, handler string
		valid                          bool
	}{
		{"supplied initial read", "  brief: text\n", "  brief: text\n", "      guard: {check: entity.brief != ''}\n", true},
		{"no eligible input", "  brief: text\n", "", "      guard: {check: entity.brief != ''}\n", false},
		{"internal collision", "  count: {type: integer, initial: 0}\n", "  count: integer\n", "", false},
		{"incompatible supply", "  brief: text\n", "  brief: integer\n", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := constructorReaderFixture(t, tc.fields, tc.payload, tc.handler)
			report := Run(context.Background(), source, Options{})
			var constructorErrors []string
			for _, finding := range report.Findings {
				if finding.CheckID == "flow_constructor_validation" {
					constructorErrors = append(constructorErrors, finding.Message)
				}
			}
			if (len(constructorErrors) == 0) != tc.valid {
				t.Fatalf("constructor errors=%v, valid=%v", constructorErrors, tc.valid)
			}
		})
	}
}

func TestFlowConstructorCandidatesDoNotShareFacts(t *testing.T) {
	root := canonicalrouting.CopyFlowConstructorCandidates(t, canonicalrouting.TemplateInstanceRouteSelect, true)
	repo := repoRootForBootverifyTest(t)
	source := semanticview.Wrap(loadFixtureBundleAt(t, repo, root, c.DefaultPlatformSpecFile(repo)))
	for i := 0; i < 3; i++ {
		constructors, err := pipeline.CompileFlowConstructors(source, "consumer")
		if err != nil {
			t.Fatal(err)
		}
		if len(constructors) != 2 || !constructors[0].Eligible() || constructors[1].Eligible() {
			t.Fatalf("candidate eligibility: %+v", constructors)
		}
		if !reflect.DeepEqual(constructors[0].SuppliedFields(), []string{"brief"}) {
			t.Fatalf("candidate projection: %v", constructors[0].SuppliedFields())
		}
		if _, err := constructors[1].InitialFields(map[string]any{"vertical_id": "v1"}, "v1"); err == nil {
			t.Fatal("nonconstructor borrowed another input's facts")
		}
	}
}

func TestFlowConstructorCreationEdgesConsumeConnectionPolicy(t *testing.T) {
	for _, tc := range []struct {
		mode   string
		policy canonicalrouting.TemplateInstanceRouteMode
	}{{"select", canonicalrouting.TemplateInstanceRouteSelect}, {"create", canonicalrouting.TemplateInstanceRouteCreate}, {"select-or-create", canonicalrouting.TemplateInstanceRouteSelectOrCreate}} {
		t.Run(tc.mode, func(t *testing.T) {
			root := canonicalrouting.CopyFlowConstructorCandidates(t, tc.policy, false)
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, c.DefaultPlatformSpecFile(repo))
			rebuilt, err := c.LoadWorkflowContractBundleFromArtifact(repo, bundle.SourceArtifact, c.DefaultPlatformSpecFile(repo), c.WorkflowContractLoadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, source := range []semanticview.Source{semanticview.Wrap(bundle), semanticview.Wrap(rebuilt)} {
				rejected := false
				for _, finding := range Run(context.Background(), source, Options{}).Findings {
					if finding.CheckID == "flow_constructor_validation" && strings.Contains(finding.Message, "deploy.empty cannot create") {
						rejected = true
					}
				}
				if rejected != (tc.mode != "select") {
					t.Fatalf("connection %s rejected=%v; expected create/select-or-create rejection only", tc.mode, rejected)
				}
			}
		})
	}
}

func TestFlowConstructorReconstructedSourceParity(t *testing.T) {
	for _, tc := range []struct{ name, fields, payload string }{
		{"required", "  brief: text\n", "  brief: text\n"},
		{"optional", "  brief: text\n", "  brief: text?\n"},
		{"collision", "  count: {type: integer, initial: 0}\n", "  count: integer\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := constructorFixture(t, tc.fields, tc.payload)
			bundle, _ := semanticview.Bundle(source)
			artifact, err := sourceartifact.DecodeLogical(bundle.SourceArtifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			repo := repoRootForBootverifyTest(t)
			rebuilt, err := c.LoadWorkflowContractBundleFromArtifact(repo, artifact, c.DefaultPlatformSpecFile(repo), c.WorkflowContractLoadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			a, err := pipeline.CompileFlowConstructors(source, "consumer")
			if err != nil {
				t.Fatal(err)
			}
			b, err := pipeline.CompileFlowConstructors(semanticview.Wrap(rebuilt), "consumer")
			if err != nil {
				t.Fatal(err)
			}
			if len(a) != len(b) {
				t.Fatalf("constructor count: %d != %d", len(a), len(b))
			}
			for i := range a {
				if a[i].Input() != b[i].Input() || a[i].Eligible() != b[i].Eligible() || a[i].KeyField() != b[i].KeyField() || !reflect.DeepEqual(a[i].SuppliedFields(), b[i].SuppliedFields()) || !reflect.DeepEqual(a[i].Refusals(), b[i].Refusals()) {
					t.Fatalf("constructor %d differs after source reconstruction", i)
				}
			}
			if artifact.BundleHash() != bundle.SourceArtifact.BundleHash() {
				t.Fatal("source reconstruction changed hash")
			}
		})
	}
}

func TestFlowConstructorNestedPresence(t *testing.T) {
	for _, tc := range []struct {
		name, member, read string
		eligible           bool
	}{
		{"required member", "text", "entity.brief.detail != ''", true},
		{"optional member", "text?", "entity.brief.detail != ''", false},
		{"presence guarded", "text?", "!has(entity.brief.detail) || entity.brief.detail != ''", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalrouting.CopyFlowConstructorNestedPresence(t, tc.member, tc.read)
			repo := repoRootForBootverifyTest(t)
			source := semanticview.Wrap(loadFixtureBundleAt(t, repo, root, c.DefaultPlatformSpecFile(repo)))
			constructor, err := pipeline.CompileFlowConstructor(source, "consumer", "deploy.done")
			if err != nil {
				t.Fatal(err)
			}
			if constructor.Eligible() != tc.eligible {
				t.Fatalf("eligible=%v refusals=%v", constructor.Eligible(), constructor.Refusals())
			}
		})
	}
}

func TestKeylessConstructorHasNoSuppliedClass(t *testing.T) {
	root := canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{})
	writeBootverifyFixtureFile(t, filepath.Join(root, "producer/entities.yaml"), "producer_state:\n  count: {type: integer, initial: 0}\n  note: text?\n")
	repo := repoRootForBootverifyTest(t)
	source := semanticview.Wrap(loadFixtureBundleAt(t, repo, root, c.DefaultPlatformSpecFile(repo)))
	constructor, err := pipeline.CompileFlowConstructor(source, "producer", "")
	if err != nil {
		t.Fatal(err)
	}
	if !constructor.Eligible() || len(constructor.SuppliedFields()) != 0 || constructor.KeyField() != "" {
		t.Fatalf("keyless contract: %+v", constructor)
	}
	fields, err := constructor.InitialFields(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["note"]; ok {
		t.Fatal("keyless constructor supplied payload fields")
	}
	if !reflect.DeepEqual(fields["count"], int64(0)) {
		t.Fatalf("internal literal replaced: %v (%T)", fields["count"], fields["count"])
	}
	if _, err := constructor.InitialFields(nil, "run-is-not-a-key"); err == nil {
		t.Fatal("keyless constructor accepted a key")
	}
	if _, err := constructor.InitialFields(map[string]any{"count": 7, "note": "not supplied"}, nil); err == nil {
		t.Fatal("keyless constructor accepted supplied arguments")
	}
	if _, err := pipeline.CompileFlowConstructor(source, "producer", "deploy.done"); err == nil {
		t.Fatal("keyless input selected a constructor")
	}
}
