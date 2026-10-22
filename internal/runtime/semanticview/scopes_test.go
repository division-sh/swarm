package semanticview

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestRootExecutionCoordinateBindsAuthoredRootAndExactRun(t *testing.T) {
	root := runtimecontracts.FlowContractView{
		Path:  ".",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name: "authored-root",
		},
	}
	source := Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "display-workflow"},
		FlowTree: runtimecontracts.FlowTree{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{".": &root},
		},
	})
	const runID = "11111111-1111-1111-1111-111111111111"

	coordinate, err := AdmitRootExecutionCoordinate(source, runID)
	if err != nil {
		t.Fatalf("AdmitRootExecutionCoordinate: %v", err)
	}
	if coordinate.FlowID() != "." || coordinate.RunID() != runID {
		t.Fatalf("coordinate = (%q, %q), want exact filesystem root and run", coordinate.FlowID(), coordinate.RunID())
	}
	if source.WorkflowName() != "display-workflow" || coordinate.FlowID() == source.WorkflowName() {
		t.Fatalf("test requires distinct display and authored root identities: display=%q authored=%q", source.WorkflowName(), coordinate.FlowID())
	}
	if !coordinate.Matches(".", runID) {
		t.Fatal("exact root coordinate did not match")
	}
	for _, hostile := range []struct {
		flowID string
		runID  string
	}{
		{flowID: "display-workflow", runID: runID},
		{flowID: ".", runID: "22222222-2222-2222-2222-222222222222"},
		{flowID: "display-workflow", runID: "22222222-2222-2222-2222-222222222222"},
	} {
		if coordinate.Matches(hostile.flowID, hostile.runID) {
			t.Fatalf("hostile coordinate (%q, %q) matched exact root owner", hostile.flowID, hostile.runID)
		}
	}
	if _, err := AdmitRootExecutionCoordinate(source, ""); err == nil {
		t.Fatal("missing run identity was admitted")
	}
	if _, err := AdmitRootExecutionCoordinate(nil, runID); err == nil {
		t.Fatal("missing semantic source was admitted")
	}
}

func writeSemanticviewFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if filepath.Base(path) == "nodes.yaml" && strings.TrimSpace(contents) == "{}" {
		t.Fatalf("optional declaration fixture %s must be omitted instead of written as {}", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
