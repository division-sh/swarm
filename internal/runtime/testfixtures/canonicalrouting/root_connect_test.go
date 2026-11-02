package canonicalrouting

import (
	"path"
	"slices"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestS1RootConnectFixturesUseDirectoryIdentity(t *testing.T) {
	for _, test := range []struct {
		name  string
		build func(testing.TB) string
		flows []string
	}{
		{"public-template-input", WritePublicTemplateInputRoute, []string{".", "operating"}},
		{"finite-public-template-input", WriteFinitePublicTemplateInputRoute, []string{".", "operating"}},
		{"root-output", func(t testing.TB) string { return CopyRootOutputConnect(t, RootConnectNoEmitter) }, []string{".", "consumer"}},
		{"root-output-emitter", func(t testing.TB) string { return CopyRootOutputConnect(t, RootConnectCanonicalEmit) }, []string{".", "consumer"}},
		{"missing-receiver", CopyRootOutputConnectMissingReceiver, []string{".", "consumer"}},
		{"child-publisher", CopyRootOutputConnectWithChildPublisher, []string{".", "child", "consumer"}},
		{"singleton", CopyRootOutputSingletonConnect, []string{".", "consumer"}},
		{"singleton-arc", CopyRootOutputSingletonArc, []string{".", "receiver"}},
		{"singleton-return", CopySingletonOutputRootConnect, []string{".", "scout"}},
		{"nested-singleton-return", CopyNestedSingletonOutputRootConnect, []string{".", "left", "left/child", "left/child/scout"}},
		{"boomerang", CopyRootSingletonBoomerang, []string{".", "boomerang"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := test.build(t)
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range artifact.Entries() {
				if base := path.Base(entry.Label()); base != "manifest.yaml" && base != "schema.yaml" {
					continue
				}
				document, present := artifact.YAML(entry.Label())
				if !present {
					t.Fatalf("missing captured declaration %s", entry.Label())
				}
				name, err := document.Root().Lookup("name")
				if err != nil || name.Presence != yamlsource.PresenceMissing {
					t.Fatalf("%s retains an authored name instead of directory identity: %v", entry.Label(), err)
				}
			}
			rooted, err := artifact.RootedSource()
			if err != nil {
				t.Fatal(err)
			}
			bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(RepoRoot(t), rooted, "", runtimecontracts.WorkflowContractLoadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			identity, err := runtimecontracts.BootBundleIdentity(bundle)
			if err != nil || identity.WorkflowName != "." || identity.BundleHash != artifact.BundleHash() || identity.WorkflowVersion != artifact.BundleHash() {
				t.Fatalf("directory fixture changed exact machine identity: %+v, %v", identity, err)
			}
			var flows []string
			for _, scope := range semanticview.Wrap(bundle).FlowScopes() {
				flows = append(flows, scope.ID)
				if _, present := bundle.FlowViewByID(scope.ID); !present {
					t.Fatalf("flow scope %q is not backed by an admitted directory", scope.ID)
				}
			}
			slices.Sort(flows)
			if !slices.Equal(flows, test.flows) {
				t.Fatalf("directory-derived flow identities = %q, want %q", flows, test.flows)
			}
		})
	}
}
