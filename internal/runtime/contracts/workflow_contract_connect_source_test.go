package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestFlowPackageConnectCapturesMappingLineAndPreservesStrictFields(t *testing.T) {
	// routing-example-census: parser-only issue=none owner=contracts.project_package_decoder proof=TestFlowPackageConnectCapturesMappingLineAndPreservesStrictFields
	var document ProjectPackageDocument
	if err := yaml.Unmarshal([]byte("name: test\nversion: 1.0.0\nconnect:\n  - from: producer.done\n    to: consumer.done\n"), &document); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if len(document.Connect) != 1 || document.Connect[0].SourceLine != 4 {
		t.Fatalf("connect = %#v, want mapping source line 4", document.Connect)
	}

	var connect FlowPackageConnect
	err := yaml.Unmarshal([]byte("from: producer.done\nto: consumer.done\nfuture_route: forbidden\n"), &connect)
	if err == nil || !strings.Contains(err.Error(), "future_route") {
		t.Fatalf("yaml.Unmarshal error = %v, want strict unknown-field rejection", err)
	}
}

func TestPopulateWorkflowSemanticsAttachesRootAndNestedConnectSource(t *testing.T) {
	bundle := &WorkflowContractBundle{PackageTree: []LoadedProjectPackage{
		{
			Key:      ".",
			Paths:    ProjectPackagePaths{PackageFile: "/contracts/package.yaml"},
			Manifest: ProjectPackageDocument{Connect: []FlowPackageConnect{{SourceLine: 8, From: "producer.done", To: "consumer.done"}}},
		},
		{
			Key:      "packages/child",
			Paths:    ProjectPackagePaths{PackageFile: "/contracts/packages/child/package.yaml"},
			Manifest: ProjectPackageDocument{Connect: []FlowPackageConnect{{SourceLine: 12, From: "worker.done", To: "sink.done"}}},
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
