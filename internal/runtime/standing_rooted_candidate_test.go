package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1StandingCandidatesRetainCompiledRoot(t *testing.T) {
	source, _ := standingTelegramDeclarationSource(t, "inbound.telegram")
	bundle, _ := semanticview.Bundle(source)
	first, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	second, _ := sourceartifactfixture.Rooted(t, "other", first.Artifact())
	for _, rooted := range []struct {
		name string
		load func(*testing.T) *runtimecontracts.WorkflowContractBundle
	}{
		{"first", func(*testing.T) *runtimecontracts.WorkflowContractBundle { return bundle }},
		{"second", func(t *testing.T) *runtimecontracts.WorkflowContractBundle {
			compiled, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), second, "", runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			return compiled
		}},
	} {
		t.Run(rooted.name, func(t *testing.T) {
			compiled := rooted.load(t)
			root, err := compiled.RootedSource()
			if err != nil || root.Artifact().BundleHash() != first.Artifact().BundleHash() {
				t.Fatalf("same-content rooted fixture: %v", err)
			}
			projection, err := packadmission.FromBundle(compiled)
			if err != nil {
				t.Fatal(err)
			}
			credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			for _, enabled := range []bool{false, true} {
				if enabled {
					if err := credentials.Set(context.Background(), "webhook_signing.telegram", "fixture-signing-value"); err != nil {
						t.Fatal(err)
					}
				}
				rt := &Runtime{Options: RuntimeOptions{
					WorkflowModule: semanticOnlyWorkflowRuntime{source: semanticview.Wrap(compiled)}, ProviderTriggerCatalog: projection.ProviderTriggers,
					ProviderCredentials: credentials, SourceArtifactFact: sourceartifactfixture.FactFor(root.Artifact()),
				}}
				candidates, err := rt.PlanStandingServiceCandidates()
				if err != nil || len(candidates) != 1 {
					t.Fatalf("standing candidate planning: %+v, %v", candidates, err)
				}
				candidate := candidates[0]
				if !candidate.RootAdmission.Equal(root.Admission()) || candidate.BindingEnabled != enabled || candidate.Validate() != nil {
					t.Fatalf("standing candidate lost its compiled root: enabled=%t root=%+v wanted=%+v", enabled, candidate.RootAdmission.Reference(), root.Admission().Reference())
				}
			}
		})
	}
}

func TestS1StandingCandidatesRefuseContradictoryContent(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{Options: RuntimeOptions{
		WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog, ProviderCredentials: credentials,
		SourceArtifactFact: sourceartifactfixture.FactFor(sourceartifactfixture.New("schema.yaml", []byte("stages: []\n"))),
	}}
	if candidates, err := rt.PlanStandingServiceCandidates(); err == nil || len(candidates) != 0 {
		t.Fatalf("contradictory source gained a standing candidate: %+v, %v", candidates, err)
	}
}
