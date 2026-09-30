package contracts

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func admitSchemaFragment(source string) (FlowSchemaDocument, error) {
	snapshot, err := yamlsource.Load([]byte(source))
	if err != nil {
		return FlowSchemaDocument{}, err
	}
	return projectFlowSchemaValue(snapshot.Document("schema.yaml").Root())
}

func loadSchemaFragment(t *testing.T, source string) (*WorkflowContractBundle, error) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "schema.yaml"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	repo := repoRootForContractsTest(t)
	return LoadWorkflowContractBundleWithOverrides(repo, dir, DefaultPlatformSpecFile(repo))
}

func TestSchemaAdmissionOwnsCompleteRoot(t *testing.T) {
	schema, err := admitSchemaFragment(canonicalrouting.SchemaAdmissionCompleteRoot)
	if err != nil {
		t.Fatal(err)
	}
	if schema.Instance.Empty() || schema.Ingress == nil || len(schema.Imports.ConnectorPacks) != 1 || len(schema.Imports.ProviderTriggerEvents) != 1 || len(schema.Connect) != 1 || len(schema.Pins.Inputs.EventPins) != 1 || !schema.RequiredAgentsDeclared || len(schema.RequiredAgents) != 1 || len(schema.StageDeclarations.Entries) != 2 || len(schema.LoopDeclarations.Entries) != 1 {
		t.Fatalf("nested family lost: %#v", schema)
	}
	if variable := schema.InstanceVariables.Variables["note"]; !variable.HasDefault || variable.Default != "" || variable.Refinements.Length.Min == nil || *variable.Refinements.Length.Min != 0 {
		t.Fatalf("receiver presence lost: %#v", variable)
	}
	gate := schema.StageDeclarations.Entries[0].Gate
	if !gate.Context["null_value"].HasLiteralValue() || gate.Context["null_value"].Literal != nil || gate.Context["zero"].Literal != 0 || !gate.Context["dynamic"].HasCELValue() {
		t.Fatalf("shared R2 meaning lost: %#v", gate.Context)
	}
	if gate.Outcomes["approve"].Input["comment"].Required {
		t.Fatal("false boolean was lost")
	}
}

func TestSchemaAdmissionRetiredPresence(t *testing.T) {
	for _, key := range []string{"initial_state", "states", "terminal_states", "tool_surface", "entity", "namespace_prefix", "namespace_rule"} {
		for _, value := range []string{"null", "''", "false", "x", "{}", "{a: b}", "[]", "[x]"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				_, err := loadSchemaFragment(t, "name: retired\n"+key+": "+value+"\n")
				if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "schema.yaml:") {
					t.Fatalf("expected source-located retirement: %v", err)
				}
			})
		}
	}
}

func TestSchemaAdmissionRootAndMarkerlessPosture(t *testing.T) {
	for _, source := range []string{"name: markerless\n", "stages: []\n", "stages: []\nrequired_agents: []\n"} {
		bundle, err := loadSchemaFragment(t, source)
		if err != nil {
			t.Fatal(err)
		}
		if len(bundle.RootSchema.LoweredStates()) != 0 || bundle.RootSchema.LoweredInitialState() != "" {
			t.Fatal("stateless source minted a stage")
		}
	}
	for _, source := range []string{"", "# empty\n", "null\n", "{}\n", "[]\n", "false\n", "stages: {}\n", "stages: {waiting: null}\n"} {
		if _, err := loadSchemaFragment(t, source); err == nil {
			t.Fatalf("invalid root admitted: %q", source)
		}
	}
}

func TestSchemaAdmissionDiagnosticPaths(t *testing.T) {
	_, err := loadSchemaFragment(t, "stages:\n  waiting:\n    initial: true\n    gate:\n      decision: review\n      outcomes:\n        approve:\n          advances_to: done\n          input:\n            note: {type: text, unexpected: true}\n  done: {terminal: true}\n")
	if err == nil || !strings.Contains(err.Error(), `["stages"]["waiting"]["gate"]["outcomes"]["approve"]["input"]["note"]`) || !strings.Contains(err.Error(), "schema.yaml:") {
		t.Fatalf("nested path/coordinates lost: %v", err)
	}
}

func TestSchemaAdmissionDiskCatalogRetainedParity(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := filepath.Join(repo, "examples/routing/template-create-minted-key")
	disk, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := sourceartifact.PersistedFromArtifact(disk.SourceArtifact, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := persisted.Decode()
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	for name, artifact := range map[string]*sourceartifact.AdmittedSourceArtifact{"catalog": catalog, "retained": retained} {
		loaded, err := LoadWorkflowContractBundleFromArtifact(repo, artifact, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(disk.SourceArtifact.LogicalBlob(), artifact.LogicalBlob()) || disk.SourceArtifact.BundleHash() != artifact.BundleHash() {
			t.Fatalf("%s changed authoritative bytes/hash", name)
		}
		for _, pair := range []struct {
			name string
			a, b any
		}{
			{"schema", disk.RootSchema, loaded.RootSchema}, {"flows", disk.FlowSchemas, loaded.FlowSchemas},
			{"plans", disk.CompositionConnects(), loaded.CompositionConnects()}, {"timers", disk.WorkflowTimers(), loaded.WorkflowTimers()},
			{"provenance", disk.EffectiveProvenance().Entries(), loaded.EffectiveProvenance().Entries()},
		} {
			if !reflect.DeepEqual(pair.a, pair.b) {
				t.Fatalf("%s changed %s", name, pair.name)
			}
		}
	}
}

func TestSchemaAdmissionInvalidSourceParity(t *testing.T) {
	for _, source := range []string{
		"mode: static\n", "mode: template\n", "mode: singleton\n", "mode: null\n", "mode: ''\n", "mode: {}\n", "mode: []\n",
		"mode: static\nmode: template\n", "name: &shape static\nmode: *shape\n",
		"tool_surface: null\n",
		"stages: {waiting: {initial: 'true'}}\n",
		"instance_variables: {variables: {note: {type: text, length: {min: -0.5}}}}\n",
		"ingress: {alias: hooks, providers: [{provider: partner, admission: {kind: pack, event: ''}}]}\n",
	} {
		t.Run(source, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "schema.yaml"), []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			repo := repoRootForContractsTest(t)
			_, diskErr := LoadWorkflowContractBundleWithOverrides(repo, dir, DefaultPlatformSpecFile(repo))
			if diskErr == nil {
				t.Fatal("invalid source admitted from disk")
			}
			artifact, err := sourceartifact.AdmitDirectory(dir)
			if err != nil {
				t.Fatal(err)
			}
			fact, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := fact.Decode()
			if err != nil {
				t.Fatal(err)
			}
			retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			for _, candidate := range []*sourceartifact.AdmittedSourceArtifact{catalog, retained} {
				_, err := LoadWorkflowContractBundleFromArtifact(repo, candidate, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
				if err == nil || err.Error() != diskErr.Error() || !bytes.Equal(candidate.LogicalBlob(), artifact.LogicalBlob()) || candidate.BundleHash() != artifact.BundleHash() {
					t.Fatalf("source or refusal changed: disk=%v retained=%v", diskErr, err)
				}
			}
		})
	}
}

func TestSchemaAdmissionProvenanceCompositionAndIsolation(t *testing.T) {
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "examples/routing/template-create-minted-key"), DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	before := bundle.EffectiveProvenance().Entries()
	seen := map[string]bool{}
	for _, entry := range before {
		for _, family := range []string{"schemas[", "nodes[", "events["} {
			if strings.HasPrefix(entry.Path, family) {
				seen[family] = true
			}
		}
	}
	if len(seen) != 3 {
		t.Fatalf("family provenance overwritten/missing: %v", seen)
	}
	populateEffectiveProvenance(bundle)
	if !reflect.DeepEqual(before, bundle.EffectiveProvenance().Entries()) {
		t.Fatal("finalization is not compositional/idempotent")
	}
	before[0].Provenance.SourceFile = "changed"
	if reflect.DeepEqual(before, bundle.EffectiveProvenance().Entries()) {
		t.Fatal("ledger exposes mutable storage")
	}
}

func TestSchemaAdmissionDocumentExpansionBudget(t *testing.T) {
	for _, variant := range []string{"alias", "merge"} {
		t.Run(variant, func(t *testing.T) {
			var source strings.Builder
			source.WriteString("instance_variables:\n  variables:\n    x: &variable\n      type: text\n      default:\n")
			for i := 0; i < 15; i++ {
				if i == 0 {
					fmt.Fprintf(&source, "        a%d: &a%d [x, x]\n", i, i)
				} else {
					fmt.Fprintf(&source, "        a%d: &a%d [*a%d, *a%d]\n", i, i, i-1, i-1)
				}
			}
			if variant == "merge" {
				source.WriteString("    y: {<<: *variable}\n    z: {<<: *variable}\n")
			} else {
				source.WriteString("    y: {type: text, default: *a14}\n    z: {type: text, default: *a14}\n")
			}
			_, err := loadSchemaFragment(t, source.String())
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "expansion") {
				t.Fatalf("document-wide expansion did not fail closed: %v", err)
			}
		})
	}
}

func TestSchemaAdmissionAliasesMergesAndDerivedProvenance(t *testing.T) {
	schema, err := admitSchemaFragment(canonicalrouting.SchemaAdmissionAliasProvenance)
	if err != nil {
		t.Fatal(err)
	}
	alias := schema.admissionProvenance["stages.waiting.description"]
	if alias.Origin != EffectiveValueOriginAuthored || alias.SourceLine != 4 || alias.SourcePresence != "scalar" {
		t.Fatalf("alias source occurrence lost: %#v", alias)
	}
	for _, path := range []string{"stages.waiting.timers[0].id", "pins.inputs.events[0].event", "pins.outputs.events[0].event"} {
		fact := schema.admissionProvenance[path]
		if fact.Origin != EffectiveValueOriginDerived || fact.RuleID == "" || len(fact.InputPaths) == 0 {
			t.Fatalf("missing derived explanation %s: %#v", path, fact)
		}
		for _, input := range fact.InputPaths {
			if _, ok := schema.admissionProvenance[input]; !ok {
				t.Fatalf("%s has dangling input %s", path, input)
			}
		}
	}
	for _, source := range []string{
		"instance_variables: {variables: {a: &v {type: text}, b: {<<: *v}}}\n",
		"stages: {waiting: &s {}, Waiting: {<<: *s}}\n",
	} {
		if _, err := admitSchemaFragment(source); err != nil {
			t.Fatal(err)
		}
	}
	for _, source := range []string{
		"name: &a [*a]\n",
		"instance_variables: {variables: {a: &v {type: text}, b: {<<: *v, type: number}}}\n",
		"stages: {waiting: {}, waiting: {initial: true}}\n",
		"stages: {waiting: {}, ' waiting ': {initial: true}}\n",
		"name: &legacy {tool_surface: false}\n<<: *legacy\n",
	} {
		if _, err := admitSchemaFragment(source); err == nil {
			t.Fatalf("invalid alias/merge admitted: %s", source)
		}
	}
}
