package semanticview

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestImportBoundaryPolicyResolutionUsesBindingThenPackageDefault(t *testing.T) {
	source := loadImportBoundaryDependencyFixture(t, importBoundaryDependencyFixtureOptions{
		policyBind: "        provider.threshold: parent.policy.provider.threshold\n",
		policyRequires: `  policy:
    provider.threshold: {}
    provider.mode:
      default: strict
`,
		credentialRequires: "  credentials: [provider_key]\n",
		credentialBind:     "        provider_key: tenant_provider_key\n",
	})

	if got, ok := PolicyValueForFlow(source, "worker", "provider.threshold"); !ok || got.Value != 0.91 {
		t.Fatalf("provider.threshold = (%#v, %v), want bound parent value 0.91", got.Value, ok)
	}
	if got, ok := PolicyValueForFlow(source, "worker", "provider.mode"); !ok || got.Value != "strict" {
		t.Fatalf("provider.mode = (%#v, %v), want package default strict", got.Value, ok)
	}
	if _, ok := PolicyValueForFlow(source, "worker", "provider.parent_only"); ok {
		t.Fatal("imported flow inherited parent-only policy key without explicit binding")
	}
	if got, ok := PolicyValueForFlow(source, "worker", "provider.package_only"); !ok || got.Value != "visible" {
		t.Fatalf("provider.package_only = (%#v, %v), want child local package policy", got.Value, ok)
	}
}

func TestImportBoundaryPolicyResolutionReportsOwnerForBindingAndPackageDefault(t *testing.T) {
	source := loadImportBoundaryDependencyFixture(t, importBoundaryDependencyFixtureOptions{
		policyBind: "        provider.threshold: parent.policy.provider.threshold\n",
		policyRequires: `  policy:
    provider.threshold: {}
    provider.mode:
      default: strict
`,
		credentialRequires: "  credentials: [provider_key]\n",
		credentialBind:     "        provider_key: tenant_provider_key\n",
	})

	if got, ok := PolicyValueForFlowWithOwner(source, "worker", "provider.threshold"); !ok || got.Value.Value != 0.91 || got.OwnerKey != "root" {
		t.Fatalf("provider.threshold resolution = (%#v, %v), want root-owned bound value 0.91", got, ok)
	}
	if got, ok := PolicyValueForFlowWithOwner(source, "worker", "provider.mode"); !ok || got.Value.Value != "strict" || got.OwnerKey != "package:flows/worker" {
		t.Fatalf("provider.mode resolution = (%#v, %v), want package-owned default strict", got, ok)
	}
}

func TestImportBoundaryPolicyResolutionUsesResolvedParentPolicyForNestedBinding(t *testing.T) {
	source := loadNestedImportBoundaryPolicyFixture(t)

	if got, ok := PolicyValueForFlow(source, "child", "threshold"); !ok || got.Value != 70 {
		t.Fatalf("child threshold = (%#v, %v), want bound root value 70", got.Value, ok)
	}
	if got, ok := PolicyValueForFlow(source, "grandchild", "threshold"); !ok || got.Value != 70 {
		t.Fatalf("grandchild threshold = (%#v, %v), want child resolved parent value 70", got.Value, ok)
	}
	if issues := ImportBoundaryDependencyIssues(source); len(issues) != 0 {
		t.Fatalf("ImportBoundaryDependencyIssues = %#v, want none", issues)
	}
}

func TestImportBoundaryPolicyResolutionFailsClosedWithoutBindingOrDefaultEvenWithParentSameName(t *testing.T) {
	source := loadImportBoundaryDependencyFixture(t, importBoundaryDependencyFixtureOptions{
		policyRequires:     "  policy: [provider.threshold]\n",
		credentialRequires: "  credentials: [provider_key]\n",
		credentialBind:     "        provider_key: tenant_provider_key\n",
	})

	issues := ImportBoundaryDependencyIssues(source)
	if !importBoundaryDependencyIssueContains(issues, "missing_policy_binding", "provider.threshold") {
		t.Fatalf("expected missing_policy_binding for provider.threshold, got %#v", issues)
	}
	if _, ok := PolicyValueForFlow(source, "worker", "provider.threshold"); ok {
		t.Fatal("unbound imported policy key resolved through ambient parent same-name policy")
	}
}

func TestImportBoundaryPolicyResolutionDoesNotInheritParentPolicyForUndeclaredKeys(t *testing.T) {
	source := loadImportBoundaryDependencyFixture(t, importBoundaryDependencyFixtureOptions{
		credentialRequires: "  credentials: [provider_key]\n",
		credentialBind:     "        provider_key: tenant_provider_key\n",
	})

	if _, ok := PolicyValueForFlow(source, "worker", "provider.parent_only"); ok {
		t.Fatal("imported flow inherited undeclared parent policy through dependency context")
	}
	if got, ok := PolicyValueForFlow(source, "worker", "provider.package_only"); !ok || got.Value != "visible" {
		t.Fatalf("provider.package_only = (%#v, %v), want child local package policy", got.Value, ok)
	}
}

func TestImportBoundaryCredentialStoreKeyRequiresDeclaredBinding(t *testing.T) {
	source := loadImportBoundaryDependencyFixture(t, importBoundaryDependencyFixtureOptions{
		policyRequires:     "  policy: [provider.threshold]\n",
		policyBind:         "        provider.threshold: parent.policy.provider.threshold\n",
		credentialRequires: "  credentials: [provider_key]\n",
		credentialBind:     "        provider_key: tenant_provider_key\n",
	})

	if got, mapped := CredentialStoreKeyForFlow(source, "worker", "provider_key"); !mapped || got != "tenant_provider_key" {
		t.Fatalf("CredentialStoreKeyForFlow(provider_key) = (%q, %v), want tenant_provider_key, mapped", got, mapped)
	}
	if got, mapped := CredentialStoreKeyForFlow(source, "worker", "undeclared_key"); !mapped || got != "" {
		t.Fatalf("CredentialStoreKeyForFlow(undeclared_key) = (%q, %v), want empty mapped fail-closed", got, mapped)
	}
	if got, mapped := CredentialStoreKeyForFlow(source, "", "provider_key"); mapped || got != "provider_key" {
		t.Fatalf("root CredentialStoreKeyForFlow(provider_key) = (%q, %v), want raw unmapped key", got, mapped)
	}
}

func TestImportBoundaryDependencyContextWithoutRequiresStillFailsClosed(t *testing.T) {
	source := loadImportBoundaryDependencyFixture(t, importBoundaryDependencyFixtureOptions{})

	if _, ok := PolicyValueForFlow(source, "worker", "provider.parent_only"); ok {
		t.Fatal("imported flow with empty requires inherited parent-only policy key")
	}
	if got, ok := PolicyValueForFlow(source, "worker", "provider.package_only"); !ok || got.Value != "visible" {
		t.Fatalf("provider.package_only = (%#v, %v), want child local package policy", got.Value, ok)
	}
	if got, mapped := CredentialStoreKeyForFlow(source, "worker", "provider_key"); !mapped || got != "" {
		t.Fatalf("CredentialStoreKeyForFlow(provider_key) with empty requires = (%q, %v), want empty mapped fail-closed", got, mapped)
	}
}

func TestImportBoundaryCredentialStoreKeyUsesExplicitActorFlowContext(t *testing.T) {
	source := loadImportBoundaryDependencyFixture(t, importBoundaryDependencyFixtureOptions{
		policyRequires:     "  policy: [provider.threshold]\n",
		policyBind:         "        provider.threshold: parent.policy.provider.threshold\n",
		credentialRequires: "  credentials: [provider_key]\n",
		credentialBind:     "        provider_key: tenant_provider_key\n",
	})

	if got, mapped := CredentialStoreKeyForActorFlow(source, "worker-agent-instance", "worker", "provider_key"); !mapped || got != "tenant_provider_key" {
		t.Fatalf("CredentialStoreKeyForActorFlow(provider_key) = (%q, %v), want tenant_provider_key, mapped", got, mapped)
	}
	if got, mapped := CredentialStoreKeyForActor(source, "worker-agent-instance", "provider_key"); mapped || got != "provider_key" {
		t.Fatalf("CredentialStoreKeyForActor without flow context = (%q, %v), want raw unmapped key", got, mapped)
	}
}

func TestImportBoundaryDependencyContextMatchesDescendantPackageFlow(t *testing.T) {
	source := loadImportBoundaryDescendantPackageDependencyFixture(t)
	flow, ok := source.FlowScopeByID("worker")
	if !ok {
		t.Fatal("worker flow scope missing")
	}
	if got := filepath.ToSlash(flow.PackageKey); got != "packages/vendor/subpkg" {
		t.Fatalf("worker PackageKey = %q, want descendant packages/vendor/subpkg", flow.PackageKey)
	}

	if got, ok := PolicyValueForFlow(source, "worker", "provider.threshold"); !ok || got.Value != 0.91 {
		t.Fatalf("descendant provider.threshold = (%#v, %v), want bound parent value 0.91", got.Value, ok)
	}
	if got, mapped := CredentialStoreKeyForFlow(source, "worker", "provider_key"); !mapped || got != "tenant_provider_key" {
		t.Fatalf("descendant CredentialStoreKeyForFlow(provider_key) = (%q, %v), want tenant_provider_key, mapped", got, mapped)
	}
	if got, mapped := CredentialStoreKeyForFlow(source, "worker", "undeclared_key"); !mapped || got != "" {
		t.Fatalf("descendant CredentialStoreKeyForFlow(undeclared_key) = (%q, %v), want empty mapped fail-closed", got, mapped)
	}
}

func importBoundaryDependencyIssueContains(issues []ImportBoundaryDependencyIssue, kind, dependency string) bool {
	for _, issue := range issues {
		if issue.Kind == kind && issue.Dependency == dependency {
			return true
		}
	}
	return false
}

type importBoundaryDependencyFixtureOptions struct {
	policyRequires     string
	policyBind         string
	credentialRequires string
	credentialBind     string
}

func loadImportBoundaryDependencyFixture(t *testing.T, opts importBoundaryDependencyFixtureOptions) Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()

	bindPolicy := ""
	if opts.policyBind != "" {
		bindPolicy = "      policy:\n" + opts.policyBind
	}
	bindCredentials := ""
	if opts.credentialBind != "" {
		bindCredentials = "      credentials:\n" + opts.credentialBind
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: import-boundary-dependencies
version: "1.0.0"
flows:
  - id: worker
    flow: worker
    mode: static
    bind:
`+bindPolicy+bindCredentials)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: import-boundary-dependencies\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), `
provider:
  threshold: 0.91
  parent_only: leaked
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), `
name: worker-package
version: "1.0.0"
requires:
`+opts.policyRequires+opts.credentialRequires)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), "name: worker\nmode: static\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), `
provider:
  threshold: child-local
  mode: child-local
  package_only: visible
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "worker", "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "{}\n")

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return Wrap(bundle)
}

func loadNestedImportBoundaryPolicyFixture(t *testing.T) Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()

	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: nested-import-policy
version: "1.0.0"
flows:
  - id: child
    flow: child
    mode: static
    bind:
      policy:
        threshold: parent.policy.root_threshold
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: nested-import-policy\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "root_threshold: 70\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "package.yaml"), `
name: child-package
version: "1.0.0"
requires:
  policy: [threshold]
flows:
  - id: grandchild
    flow: grandchild
    mode: static
    bind:
      policy:
        threshold: parent.policy.threshold
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), "name: child\nmode: static\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), "{}\n")

	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "flows", "grandchild", "package.yaml"), `
name: grandchild-package
version: "1.0.0"
requires:
  policy: [threshold]
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "flows", "grandchild", "schema.yaml"), "name: grandchild\nmode: static\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "flows", "grandchild", "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "flows", "grandchild", "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "flows", "grandchild", "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "child", "flows", "grandchild", "events.yaml"), "{}\n")

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return Wrap(bundle)
}

func loadImportBoundaryDescendantPackageDependencyFixture(t *testing.T) Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()

	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: import-boundary-descendant-dependencies
version: "1.0.0"
packages:
  - path: packages/vendor
    bind:
      policy:
        provider.threshold: parent.policy.provider.threshold
      credentials:
        provider_key: tenant_provider_key
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: import-boundary-descendant-dependencies\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), `
provider:
  threshold: 0.91
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	writeSemanticviewFixtureFile(t, filepath.Join(root, "packages", "vendor", "package.yaml"), `
name: vendor-package
version: "1.0.0"
requires:
  policy: [provider.threshold]
  credentials: [provider_key]
packages:
  - path: subpkg
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "packages", "vendor", "subpkg", "package.yaml"), `
name: vendor-subpackage
version: "1.0.0"
flows:
  - id: worker
    flow: worker
    mode: static
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "packages", "vendor", "subpkg", "flows", "worker", "schema.yaml"), "name: worker\nmode: static\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "packages", "vendor", "subpkg", "flows", "worker", "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "packages", "vendor", "subpkg", "flows", "worker", "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "packages", "vendor", "subpkg", "flows", "worker", "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "packages", "vendor", "subpkg", "flows", "worker", "events.yaml"), "{}\n")

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return Wrap(bundle)
}
