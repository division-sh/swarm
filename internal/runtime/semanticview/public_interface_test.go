package semanticview

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestSelectedRootPinBoundaryArtifactParity(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := filepath.Join(t.TempDir(), "source")
	if err := os.CopyFS(root, os.DirFS(filepath.Join(repo, "tests/tier8-boot-verification/test-boot-missing-pin"))); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(filepath.Join(root, "deep/grandchild"), os.DirFS(filepath.Join(root, "child"))); err != nil {
		t.Fatal(err)
	}
	type selection struct {
		artifact        *sourceartifact.AdmittedSourceArtifact
		inputs, outputs []string
	}
	var selected []selection
	for _, relative := range []string{".", "child", "deep/grandchild"} {
		artifact, err := sourceartifact.AdmitDirectory(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		bundle, err := runtimecontracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		source := Wrap(bundle)
		inputs, outputs := publicInterfaceProof(source)
		want := []string{"task.assigned", "task.feedback"}
		if relative == "." {
			want = []string{"task.requested", "task.result"}
		}
		if !reflect.DeepEqual(inputs, want) {
			t.Fatalf("selected %s: inputs=%v want=%v", relative, inputs, want)
		}
		for _, event := range []string{"child/task.assigned", "deep/grandchild/task.assigned", "catalog.only"} {
			if _, public := SelectedRootInputPin(source, event); public {
				t.Fatalf("%s exposed %s", relative, event)
			}
		}
		if relative == "." {
			if _, exists := source.FlowInputEventPin("child", "task.assigned"); !exists {
				t.Fatal("public filtering deleted private receiver evidence")
			}
			if _, exists := source.FlowInputEventPin("deep/grandchild", "task.assigned"); !exists {
				t.Fatal("public filtering deleted grandchild receiver evidence")
			}
		}
		selected = append(selected, selection{artifact, inputs, outputs})
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	for _, original := range selected {
		decoded, err := sourceartifact.DecodeLogical(original.artifact.LogicalBlob())
		if err != nil {
			t.Fatal(err)
		}
		bundle, err := runtimecontracts.LoadWorkflowContractBundleFromArtifact(repo, decoded, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		inputs, outputs := publicInterfaceProof(Wrap(bundle))
		if decoded.BundleHash() != original.artifact.BundleHash() || !reflect.DeepEqual(inputs, original.inputs) || !reflect.DeepEqual(outputs, original.outputs) {
			t.Fatal("artifact reconstruction changed selected-root authority")
		}
	}
}

func publicInterfaceProof(source Source) ([]string, []string) {
	inputs, outputs := []string{}, []string{}
	for _, endpoint := range SelectedRootInputEndpoints(source) {
		inputs = append(inputs, endpoint.PinName)
	}
	for _, pin := range source.FlowOutputEventPins(".") {
		outputs = append(outputs, pin.EventType()+":"+pin.Digest())
	}
	sort.Strings(inputs)
	sort.Strings(outputs)
	return inputs, outputs
}
