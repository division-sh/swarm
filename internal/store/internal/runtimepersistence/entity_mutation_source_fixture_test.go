package runtimepersistence

import (
	"fmt"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func loadEntityMutationSourceFixture(t *testing.T, flowID, entityYAML string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: entity-mutation-proof\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n")
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, flowID, "schema.yaml"), fmt.Sprintf("name: %s\nmode: template\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n", flowID))
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, flowID, "entities.yaml"), entityYAML)
	repo := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}
