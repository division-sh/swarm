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
	return AdmitFlowSchemaValue(snapshot.Document("schema.yaml").Root())
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
	gate := schema.StageDeclarations.Entries[0].Gate
	if !gate.Context["null_value"].HasLiteralValue() || gate.Context["null_value"].Literal != nil || gate.Context["zero"].Literal != 0 || !gate.Context["dynamic"].HasCELValue() {
		t.Fatalf("shared R2 meaning lost: %#v", gate.Context)
	}
	if gate.Outcomes["approve"].Input["comment"].Required {
		t.Fatal("false boolean was lost")
	}
}

func TestSchemaAdmissionRetiredPresence(t *testing.T) {
	for _, key := range []string{"activation", "initial_state", "states", "terminal_states", "tool_surface", "entity", "namespace_prefix", "namespace_rule", "instance_variables"} {
		for _, value := range []string{"null", "''", "false", "x", "{}", "{a: b}", "[]", "[x]"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				_, err := loadSchemaFragment(t, "name: retired\n"+key+": "+value+"\n")
				if err == nil || !strings.Contains(err.Error(), "is not supported") || !strings.Contains(err.Error(), "schema.yaml:") {
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
	_, err := loadSchemaFragment(t, "stages:\n  waiting:\n    gate:\n      decision: review\n      outcomes:\n        approve:\n          advances_to: done\n          input:\n            note: {type: text, unexpected: true}\n  done: {final: true}\n")
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
		"stages: {waiting: {final: 'true'}}\n",
		"instance_variables: {variables: {note: {type: text, length: {min: -0.5}}}}\n",
		"ingress: {alias: hooks, providers: [{provider: partner, admission: {kind: pack, event: ''}}]}\n",
	} {
		t.Run(source, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "schema.yaml"), []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			assertSchemaAdmissionInvalidSourceParity(t, dir)
		})
	}
}

func TestA9RetiredActivationPresenceAdmission(t *testing.T) {
	for _, scope := range []string{".", "child"} {
		for _, fragment := range []string{
			"activation: standing\n", "activation: null\n", "activation: ''\n", "activation: false\n",
			"activation: {}\n", "activation: []\n", "activation: {kind: standing}\n",
			"name: &value standing\nactivation: *value\n", "<<: {activation: standing}\n",
			"&key activation: standing\n", "name: &key activation\n*key: standing\n",
			"activation: standing\nactivation: standing\n",
		} {
			t.Run(scope+"/"+fragment, func(t *testing.T) {
				dir := t.TempDir()
				if scope != "." {
					if err := os.WriteFile(filepath.Join(dir, "schema.yaml"), []byte("name: root\n"), 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(filepath.Join(dir, scope), 0700); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(dir, scope, "schema.yaml"), []byte(fragment), 0600); err != nil {
					t.Fatal(err)
				}
				err := assertSchemaAdmissionInvalidSourceParity(t, dir)
				if !strings.Contains(err.Error(), "activation") || !strings.Contains(err.Error(), "schema.yaml:") {
					t.Fatalf("retired key lost standard source-located admission error: %v", err)
				}
			})
		}
	}
}

func assertSchemaAdmissionInvalidSourceParity(t *testing.T, dir string) error {
	t.Helper()
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
	return diskErr
}

func TestSchemaAdmissionDocumentExpansionBudget(t *testing.T) {
	for _, variant := range []string{"alias", "merge"} {
		t.Run(variant, func(t *testing.T) {
			var source strings.Builder
			source.WriteString("stages:\n  waiting:\n    gate:\n      decision: test\n      context:\n")
			for i := 0; i < 15; i++ {
				if i == 0 {
					fmt.Fprintf(&source, "        a%d: &a%d [x, x]\n", i, i)
				} else {
					fmt.Fprintf(&source, "        a%d: &a%d [*a%d, *a%d]\n", i, i, i-1, i-1)
				}
			}
			if variant == "merge" {
				source.WriteString("        merged: {<<: *a14}\n")
			} else {
				source.WriteString("        y: *a14\n        z: *a14\n")
			}
			_, err := loadSchemaFragment(t, source.String())
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "expansion") {
				t.Fatalf("document-wide expansion did not fail closed: %v", err)
			}
		})
	}
}

func TestSchemaAdmissionAliasesMergesAndDerivedProvenance(t *testing.T) {
	_, err := admitSchemaFragment(canonicalrouting.SchemaAdmissionAliasProvenance)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		"required_agents: [{role: &r worker, description: *r}, {role: other, description: *r}]\n",
		"stages: {waiting: &s {}, Waiting: {<<: *s}}\n",
	} {
		if _, err := admitSchemaFragment(source); err != nil {
			t.Fatal(err)
		}
	}
	for _, source := range []string{
		"name: &a [*a]\n",
		"instance_variables: {variables: {a: &v {type: text}, b: {<<: *v, type: number}}}\n",
		"stages: {waiting: {}, waiting: {}}\n",
		"stages: {waiting: {}, ' waiting ': {}}\n",
		"name: &legacy {tool_surface: false}\n<<: *legacy\n",
	} {
		if _, err := admitSchemaFragment(source); err == nil {
			t.Fatalf("invalid alias/merge admitted: %s", source)
		}
	}
}
