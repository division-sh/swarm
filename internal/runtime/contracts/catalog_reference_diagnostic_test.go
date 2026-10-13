package contracts

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogReferenceDiagnosticNamesAuthoredEvent(t *testing.T) {
	for _, flow := range []string{".", "support", "support/worker"} {
		t.Run(flow, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: diagnostic\nstages: {done: {final: true}}\n")
			if flow != "." {
				writeFixtureFile(t, filepath.Join(root, "support/schema.yaml"), "name: support\nstages: {done: {final: true}}\n")
				writeFixtureFile(t, filepath.Join(root, flow, "schema.yaml"), "name: child\nstages: {done: {final: true}}\n")
			}
			writeFixtureFile(t, filepath.Join(root, flow, "events.yaml"), "local.done:\n  unsupported: NotDeclared\n")
			repo := repoRootForContractsTest(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err == nil || bundle != nil {
				t.Fatalf("invalid event admitted: %v", bundle)
			}
			diagnostic, ok := AsLoaderDiagnostic(err)
			if !ok || diagnostic.Code != "contract_loader.invalid_type_reference" {
				t.Fatalf("typed refusal changed: %T %v", err, err)
			}
			name := "local.done"
			if flow != "." {
				name = flow + "/" + name
			}
			for _, want := range []string{name, "unsupported", `unsupported structural schema type "NotDeclared"`, `unknown catalog type "NotDeclared"`} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("diagnostic=%v, missing %q", err, want)
				}
			}
			if !strings.Contains(diagnostic.Location.YAMLPath, name+".unsupported") || !strings.HasSuffix(filepath.ToSlash(diagnostic.Location.File), filepath.ToSlash(filepath.Join(flow, "events.yaml"))) {
				t.Fatalf("authored event source coordinates lost: %+v", diagnostic.Location)
			}
			encoded := contractScopeKey(ContractItemSource{FlowPath: flow, Family: "events"}, "local.done")
			if strings.Contains(err.Error(), encoded) || strings.Contains(diagnostic.Location.YAMLPath, encoded) {
				t.Fatalf("internal declaration encoding leaked: %v", err)
			}
		})
	}
}
