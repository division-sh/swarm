package runforkexecution

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedContractLoadersRejectRetiredDynamicAgentTools(t *testing.T) {
	for _, name := range []string{"agent_hire", "agent_fire", "agent_reconfigure"} {
		t.Run(name+"/disk", func(t *testing.T) {
			repoRoot := runForkExecutionRepoRoot(t)
			contractsRoot := writeRetiredSelectedContractFixture(t, name)
			loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repoRoot, SourceRoot: contractsRoot, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot)}
			_, err := loader.LoadRunForkSelectedContractSource(context.Background(), runfork.RunForkContractSelection{
				Mode: runfork.RunForkContractSelectionModeSelectedContracts,
			})
			assertSelectedRetiredToolAdmissionError(t, err, name)
		})

		t.Run(name+"/selected_store", func(t *testing.T) {
			repoRoot := runForkExecutionRepoRoot(t)
			contractsRoot := writeRetiredSelectedContractFixture(t, name)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			record := persistedSourceArtifactForTest(t, bundle)
			sourceRunID := uuid.NewString()
			loader := SourceArtifactSelectedContractSourceLoader{
				RepoRoot: repoRoot,
				Store: &fakeSourceArtifactSelectedContractSourceStore{
					availability: runbundle.Availability{
						RunID: sourceRunID, Status: "running", BundleHash: record.BundleHash,
						SourceArtifactPresent: true,
					},
					record: record,
				},
			}
			_, err = loader.LoadRunForkSelectedContractSourceForRequest(context.Background(), SelectedContractSourceLoadRequest{
				SourceRunID: sourceRunID,
				BundleHash:  record.BundleHash,
				Selection: runfork.RunForkContractSelection{
					Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: record.BundleHash,
				},
			})
			assertSelectedRetiredToolAdmissionError(t, err, name)
		})
	}
}

func writeRetiredSelectedContractFixture(t *testing.T, name string) string {
	t.Helper()
	root := t.TempDir()
	writeSelectedContractFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: selected-retired-tool\n")
	writeSelectedContractFixtureFile(t, filepath.Join(root, "agents.yaml"), `worker:
  id: worker
  role: worker
  memory: false
  intent:
    inline: Reject this selected source before execution.
  tools: [`+name+`]
`)
	writeSelectedContractFixtureFile(t, filepath.Join(root, "tools.yaml"), name+`:
  description: Hostile selected-source fixture.
  handler_type: http
  input_schema:
    type: object
    additionalProperties: false
  http:
    method: POST
    url: https://example.invalid
`)
	return root
}

func assertSelectedRetiredToolAdmissionError(t *testing.T, err error, name string) {
	t.Helper()
	if err == nil {
		t.Fatalf("selected source containing %s was admitted", name)
	}
	for _, want := range []string{"selected-contract source admission failed", name, "RETIRED"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("selected source error = %v, want %q", err, want)
		}
	}
}
