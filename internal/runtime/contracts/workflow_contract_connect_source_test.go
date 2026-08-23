package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func TestFlowPackageConnectCapturesMappingLineAndPreservesStrictFields(t *testing.T) {

	var document ProjectPackageDocument
	snippet := canonicalrouting.PackageConnectSourceSnippet(t)
	if err := snippet.Decode(&document); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if len(document.Connect) != 1 || document.Connect[0].SourceLine != 4 {
		t.Fatalf("connect = %#v, want mapping source line 4", document.Connect)
	}

	var connect FlowPackageConnect
	err := yaml.Unmarshal([]byte("event: work.done\nfrom: producer\nto: consumer\nfuture_route: forbidden\n"), &connect)
	if err == nil || !strings.Contains(err.Error(), "future_route") {
		t.Fatalf("yaml.Unmarshal error = %v, want strict unknown-field rejection", err)
	}
}

func TestPopulateWorkflowSemanticsAttachesRootAndNestedConnectSource(t *testing.T) {
	bundle := &WorkflowContractBundle{PackageTree: []LoadedProjectPackage{
		{
			Key:      ".",
			Paths:    ProjectPackagePaths{PackageFile: "/contracts/package.yaml"},
			Manifest: ProjectPackageDocument{Connect: []FlowPackageConnect{{SourceLine: 8, Event: "work.done", From: "producer", To: "consumer"}}},
		},
		{
			Key:      "packages/child",
			Paths:    ProjectPackagePaths{PackageFile: "/contracts/packages/child/package.yaml"},
			Manifest: ProjectPackageDocument{Connect: []FlowPackageConnect{{SourceLine: 12, Event: "work.done", From: "worker", To: "sink"}}},
		},
	}}
	populateWorkflowSemantics(bundle)
	connects := bundle.CompositionConnects()
	if len(connects) != 2 {
		t.Fatalf("connects = %#v, want root and nested", connects)
	}
	if got := connects[0].AuthoredLocation(); got != "/contracts/package.yaml:8" {
		t.Fatalf("root authored location = %q", got)
	}
	if got := connects[1].AuthoredLocation(); got != "/contracts/packages/child/package.yaml:12" {
		t.Fatalf("nested authored location = %q", got)
	}
	cloned := cloneFlowPackageConnects(connects)
	if cloned[0].AuthoredLocation() != connects[0].AuthoredLocation() || cloned[1].AuthoredLocation() != connects[1].AuthoredLocation() {
		t.Fatalf("clone lost source metadata: %#v", cloned)
	}
}
