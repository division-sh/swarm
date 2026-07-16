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

func TestRun_FlowPackageDependencyBindingAcceptsDeclaredPolicyDefault(t *testing.T) {
	root := writeFlowPackageImportCompletenessFixture(t, flowPackageImportFixtureOptions{
		importKind: "flow",
		bind: strings.ReplaceAll(
			allFlowPackageBindingsYAML(),
			"      policy:\n        provider.threshold: parent.policy.provider.threshold\n",
			"",
		),
		requires: `requires:
  inputs: [work.requested]
  outputs: [work.completed]
  policy:
    provider.threshold:
      default: 0.8
  credentials: [provider_token]
`,
	})
	source := loadFlowPackageImportCompletenessSource(t, root)

	report := Run(context.Background(), source, Options{})

	if reportContains(report.HardInvalidities(), "flow_package_import_completeness", "required policy bindings provider.threshold") {
		t.Fatalf("policy default should satisfy import completeness, got %#v", report.HardInvalidities())
	}
	if reportContains(report.HardInvalidities(), "flow_package_dependency_binding", "provider.threshold") {
		t.Fatalf("policy default should satisfy dependency binding, got %#v", report.HardInvalidities())
	}
}

func TestRun_FlowPackageDependencyBindingFailsClosedOnInvalidPolicyCredentialBindings(t *testing.T) {
	tests := []struct {
		name        string
		bind        string
		requires    string
		wantMessage string
	}{
		{
			name: "missing policy binding without default ignores same-name parent policy",
			bind: strings.ReplaceAll(
				allFlowPackageBindingsYAML(),
				"      policy:\n        provider.threshold: parent.policy.provider.threshold\n",
				"",
			),
			requires:    allFlowPackageRequiresYAML(),
			wantMessage: "declared package policy dependency has no import binding or package default",
		},
		{
			name: "unsupported policy reference",
			bind: strings.ReplaceAll(
				allFlowPackageBindingsYAML(),
				"parent.policy.provider.threshold",
				"global.policy.provider.threshold",
			),
			requires:    allFlowPackageRequiresYAML(),
			wantMessage: "policy binding must reference parent.policy.<path> or policy.<path>",
		},
		{
			name: "unknown credential binding",
			bind: strings.ReplaceAll(
				allFlowPackageBindingsYAML(),
				"        provider_token: parent_provider_token\n",
				"        provider_token: parent_provider_token\n        undeclared_token: stray_key\n",
			),
			requires:    allFlowPackageRequiresYAML(),
			wantMessage: "credential bind key is not declared",
		},
		{
			name: "missing credential binding",
			bind: strings.ReplaceAll(
				allFlowPackageBindingsYAML(),
				"        provider_token: parent_provider_token\n",
				"",
			),
			requires:    allFlowPackageRequiresYAML(),
			wantMessage: "declared package credential dependency has no import binding",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeFlowPackageImportCompletenessFixture(t, flowPackageImportFixtureOptions{
				importKind: "flow",
				bind:       tc.bind,
				requires:   tc.requires,
			})
			source := loadFlowPackageImportCompletenessSource(t, root)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.HardInvalidities(), "flow_package_dependency_binding", tc.wantMessage) {
				t.Fatalf("expected flow_package_dependency_binding %q, got %#v", tc.wantMessage, report.HardInvalidities())
			}
		})
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
			bind:        strings.ReplaceAll(allFlowPackageBindingsYAML(), "        provider.threshold: parent.policy.provider.threshold\n", ""),
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

func TestRun_FlowPackagePinBindAliasValidationAcceptsValidAliases(t *testing.T) {
	source := loadFlowPackageAliasValidationSource(t, flowPackageAliasValidationOptions{})

	report := Run(context.Background(), source, Options{})

	if got := report.HardInvalidities(); reportContains(got, "flow_package_pin_bind_alias_validation", "invalid") {
		t.Fatalf("unexpected flow_package_pin_bind_alias_validation invalidity: %#v", got)
	}
	if !reportContains(report.Errors(), "input_pin_wiring", "work.requested") {
		t.Fatalf("bind-only input must not satisfy input pin wiring, got errors %#v", report.Errors())
	}
}

func TestRun_FlowPackagePinBindAliasValidationFailsClosed(t *testing.T) {
	tests := []struct {
		name        string
		opts        flowPackageAliasValidationOptions
		wantMessage string
	}{
		{
			name: "unknown package input pin",
			opts: flowPackageAliasValidationOptions{
				extraInputBind: "unknown.pin: parent.lead_captured",
			},
			wantMessage: "bind key is not declared",
		},
		{
			name: "unknown parent event",
			opts: flowPackageAliasValidationOptions{
				inputBind: "parent.missing_event",
			},
			wantMessage: "does not resolve to a parent-facing event",
		},
		{
			name: "ambiguous parent event",
			opts: flowPackageAliasValidationOptions{
				inputBind:        "shared.ready",
				producerOutput:   "shared.ready",
				secondProducerID: "producer_b",
			},
			wantMessage: "multiple parent-facing event producers",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := loadFlowPackageAliasValidationSource(t, tc.opts)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.HardInvalidities(), "flow_package_pin_bind_alias_validation", tc.wantMessage) {
				t.Fatalf("expected flow_package_pin_bind_alias_validation %q, got %#v", tc.wantMessage, report.HardInvalidities())
			}
		})
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
        provider.threshold: parent.policy.provider.threshold
      credentials:
        provider_token: parent_provider_token
`
}

type flowPackageAliasValidationOptions struct {
	inputBind        string
	outputBind       string
	extraInputBind   string
	producerOutput   string
	secondProducerID string
}

func loadFlowPackageAliasValidationSource(t *testing.T, opts flowPackageAliasValidationOptions) semanticview.Source {
	t.Helper()
	root := writeFlowPackageAliasValidationFixture(t, opts)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	return semanticview.Wrap(bundle)
}

func writeFlowPackageAliasValidationFixture(t *testing.T, opts flowPackageAliasValidationOptions) string {
	t.Helper()
	if opts.inputBind == "" {
		opts.inputBind = "parent.lead_captured"
	}
	if opts.outputBind == "" {
		opts.outputBind = "parent.lead_enriched"
	}
	root := t.TempDir()
	flows := ""
	if opts.producerOutput != "" {
		flows += `  - id: producer
    flow: producer
    mode: static
`
	}
	if opts.secondProducerID != "" {
		flows += `  - id: ` + opts.secondProducerID + `
    flow: ` + opts.secondProducerID + `
    mode: static
`
	}
	flows += `  - id: worker
    flow: worker
    mode: static
    bind:
      inputs:
        work.requested: ` + opts.inputBind + `
`
	if opts.extraInputBind != "" {
		flows += `        ` + opts.extraInputBind + `
`
	}
	flows += `      outputs:
        work.completed: ` + opts.outputBind + `
`
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: flow-package-alias-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
`+flows)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: flow-package-alias-validation\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
parent.lead_captured: {}
parent.lead_enriched: {}
`)
	writeFlowPackageAliasValidationFlow(t, root, "worker", `
pins:
  inputs:
    events: [work.requested]
  outputs:
    events: [work.completed]
`, "")
	if opts.producerOutput != "" {
		writeFlowPackageAliasValidationFlow(t, root, "producer", `
pins:
  outputs:
    events: [`+opts.producerOutput+`]
`, opts.producerOutput)
	}
	if opts.secondProducerID != "" {
		writeFlowPackageAliasValidationFlow(t, root, opts.secondProducerID, `
pins:
  outputs:
    events: [`+opts.producerOutput+`]
`, opts.producerOutput)
	}
	return root
}

func writeFlowPackageAliasValidationFlow(t *testing.T, root, flowID, schemaTail, outputEvent string) {
	t.Helper()
	requires := "requires:\n  inputs: []\n  outputs: []\n"
	if flowID == "worker" {
		requires = "requires:\n  inputs: [work.requested]\n  outputs: [work.completed]\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "package.yaml"), `
name: `+flowID+`
version: "1.0.0"
`+requires)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
mode: static
`+schemaTail)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
	if outputEvent == "" {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), "{}\n")
		return
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), outputEvent+": {}\n")
}
