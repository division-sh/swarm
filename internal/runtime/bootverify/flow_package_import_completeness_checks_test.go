package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_FlowPackageImportCompletenessAcceptsFullyBoundFlowPackage(t *testing.T) {
	root := writeFlowPackageImportCompletenessFixture(t, flowPackageImportFixtureOptions{
		importKind: "flow",
		bind:       allFlowPackageBindingsYAML(),
		requires:   allFlowPackageRequiresYAML(),
	})
	source := loadFlowPackageImportCompletenessSource(t, root)

	report := Run(context.Background(), source, Options{})

	if reportContains(report.HardInvalidities(), "flow_package_import_completeness", "imported package") {
		t.Fatalf("unexpected flow_package_import_completeness invalidity: %#v", report.HardInvalidities())
	}
}

func TestRun_FlowPackageImportCompletenessFailsClosedOnMissingBindings(t *testing.T) {
	tests := []struct {
		name        string
		bind        string
		wantMessage string
	}{
		{
			name:        "input",
			bind:        strings.ReplaceAll(allFlowPackageBindingsYAML(), "        work.requested: parent.work_requested\n", ""),
			wantMessage: "required input bindings work.requested",
		},
		{
			name:        "output",
			bind:        strings.ReplaceAll(allFlowPackageBindingsYAML(), "        work.completed: parent.work_completed\n", ""),
			wantMessage: "required output bindings work.completed",
		},
		{
			name:        "policy",
			bind:        strings.ReplaceAll(allFlowPackageBindingsYAML(), "        provider.threshold: parent.policy.threshold\n", ""),
			wantMessage: "required policy bindings provider.threshold",
		},
		{
			name:        "credential",
			bind:        strings.ReplaceAll(allFlowPackageBindingsYAML(), "        provider_token: parent_provider_token\n", ""),
			wantMessage: "required credential bindings provider_token",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeFlowPackageImportCompletenessFixture(t, flowPackageImportFixtureOptions{
				importKind: "flow",
				bind:       tc.bind,
				requires:   allFlowPackageRequiresYAML(),
			})
			source := loadFlowPackageImportCompletenessSource(t, root)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.HardInvalidities(), "flow_package_import_completeness", tc.wantMessage) {
				t.Fatalf("expected flow_package_import_completeness %q, got %#v", tc.wantMessage, report.HardInvalidities())
			}
		})
	}
}

func TestRun_FlowPackageImportCompletenessAcceptsFullyBoundProjectPackageRef(t *testing.T) {
	root := writeFlowPackageImportCompletenessFixture(t, flowPackageImportFixtureOptions{
		importKind: "package",
		bind:       allFlowPackageBindingsYAML(),
		requires:   allFlowPackageRequiresYAML(),
	})
	source := loadFlowPackageImportCompletenessSource(t, root)

	report := Run(context.Background(), source, Options{})

	if reportContains(report.HardInvalidities(), "flow_package_import_completeness", "imported package") {
		t.Fatalf("unexpected package ref import completeness invalidity: %#v", report.HardInvalidities())
	}
}

func TestRun_FlowPackageImportCompletenessIgnoresImportsWithoutRequires(t *testing.T) {
	root := writeFlowPackageImportCompletenessFixture(t, flowPackageImportFixtureOptions{
		importKind: "flow",
		bind:       "",
		requires:   "",
	})
	source := loadFlowPackageImportCompletenessSource(t, root)

	report := Run(context.Background(), source, Options{})

	if reportContains(report.HardInvalidities(), "flow_package_import_completeness", "imported package") {
		t.Fatalf("unexpected import completeness invalidity for package without requires: %#v", report.HardInvalidities())
	}
}

type flowPackageImportFixtureOptions struct {
	importKind string
	bind       string
	requires   string
}

func writeFlowPackageImportCompletenessFixture(t *testing.T, opts flowPackageImportFixtureOptions) string {
	t.Helper()
	root := t.TempDir()

	switch opts.importKind {
	case "package":
		writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: package-import-completeness
version: "1.0.0"
packages:
  - path: packages/worker
`+opts.bind)
		writeBootverifyFixtureFile(t, filepath.Join(root, "packages", "worker", "package.yaml"), `
name: worker-package
version: "1.0.0"
`+opts.requires)
	case "flow":
		writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: flow-import-completeness
version: "1.0.0"
flows:
  - id: worker
    flow: worker
    mode: static
`+opts.bind)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), `
name: worker-package
version: "1.0.0"
`+opts.requires)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), "name: worker\nmode: static\n")
	default:
		t.Fatalf("unknown import kind %q", opts.importKind)
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: flow-import-completeness\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "provider:\n  threshold: 0.8\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	return root
}

func loadFlowPackageImportCompletenessSource(t *testing.T, root string) semanticview.Source {
	t.Helper()
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	return semanticview.Wrap(bundle)
}

func allFlowPackageRequiresYAML() string {
	return `requires:
  inputs: [work.requested]
  outputs: [work.completed]
  policy: [provider.threshold]
  credentials: [provider_token]
`
}

func allFlowPackageBindingsYAML() string {
	return `    bind:
      inputs:
        work.requested: parent.work_requested
      outputs:
        work.completed: parent.work_completed
      policy:
        provider.threshold: parent.policy.threshold
      credentials:
        provider_token: parent_provider_token
`
}
