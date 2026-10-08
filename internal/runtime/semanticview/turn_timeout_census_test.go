package semanticview

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestTurnTimeoutHasExactProducerCensusWithoutAgentEmitTool(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Clean(filepath.Join(cwd, "..", "..", ".."))
	root := t.TempDir()
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: timeout-census\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "work.aborted:\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  intent: {inline: investigate}\n  turn_timeout:\n    after: 10m\n    emit: work.aborted\n")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := Wrap(bundle)
	var owned []AuthoredEventEndpoint
	for _, endpoint := range BuildAuthoredEventEndpointCensus(source).Producers() {
		if endpoint.Kind == EventEndpointAgent {
			owned = append(owned, endpoint)
		}
	}
	if len(owned) != 1 || owned[0].FlowID != "." || owned[0].AgentLocalID != "worker" || owned[0].Site != "turn_timeout.emit" || owned[0].SourceLine != 5 || owned[0].ProducerDescription() != "agent worker turn_timeout.emit" {
		t.Fatalf("timeout producer attribution lost: %+v", owned)
	}
	if len(AgentDeclarations(source)[0].Entry.EmitEvents) != 0 {
		t.Fatal("timeout census granted a provider emit tool")
	}
}
