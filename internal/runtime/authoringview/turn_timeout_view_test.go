package authoringview

import (
	"path/filepath"
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestBuildShowsAuthoredTurnTimeoutWithNoDefault(t *testing.T) {
	repo := authoringViewRepoRoot(t)
	root := t.TempDir()
	writeAuthoringViewTestFile(t, filepath.Join(root, "schema.yaml"), "name: timeout-view\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "events.yaml"), "work.aborted:\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "agents.yaml"), "bounded:\n  intent: {inline: investigate}\n  turn_timeout: {after: 10m, emit: work.aborted}\nunbounded:\n  intent: {inline: investigate}\n")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	view := mustBuild(t, semanticview.Wrap(bundle), nil)
	bounded := agentByID(t, view.Root.Agents, "bounded").Fields["turn_timeout"]
	if bounded.Source != runtimecontracts.AgentFieldSourceAuthored || !reflect.DeepEqual(bounded.Value, map[string]any{"after": "10m0s", "emit": "work.aborted"}) {
		t.Fatalf("authored bound presentation changed: %+v", bounded)
	}
	if _, present := agentByID(t, view.Root.Agents, "unbounded").Fields["turn_timeout"]; present {
		t.Fatal("describe invented a default bound")
	}
}
