package bootverify

import (
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestEntityContractResolutionCannotBorrowParentState(t *testing.T) {
	repo := repoRootForBootverifyTest(t)
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "stages: []\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "state.yaml"), "parent_only: text\nshared: text\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "child", "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "child", "schema.yaml"), "stages: []\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "child", "state.yaml"), "local: text\nshared: integer\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "local-state", "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "local-state", "schema.yaml"), "stages: []\n")
	bundle := loadFixtureBundleAt(t, repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	source := semanticview.Wrap(bundle)
	if _, present := entityruntime.ResolveForFlow(source, "local-state"); present {
		t.Fatal("flow without state.yaml borrowed the same-named root's state/tool contract")
	}
	for _, tc := range []struct {
		flow, field, typ string
	}{
		{".", "shared", "text"},
		{"child", "shared", "integer"},
		{"child", "local", "text"},
	} {
		t.Run(tc.flow+"/"+tc.field, func(t *testing.T) {
			read, owner, err := wave1ResolveEntityPathWithOwner(source, tc.flow, "state."+tc.field)
			if err != nil || owner != tc.flow || read.Type != tc.typ {
				t.Fatalf("local read = %#v, owner %q, error %v", read, owner, err)
			}
			node, err := runtimeidentity.AdmitExecutableNodeDeclaration(tc.flow, "worker")
			if err != nil {
				t.Fatal(err)
			}
			target := wave1WriteTarget{Node: node, Entity: true, Target: "state." + tc.field, Field: tc.field}
			write, ok := wave1WriteTargetContract(source, target)
			if !ok || write.FlowID != tc.flow || write.Contract.Fields[tc.field].Type != tc.typ {
				t.Fatalf("local write = %#v, found %v", write, ok)
			}
			_, owner, field, err := wave1ResolveWriteTargetPath(source, target)
			if err != nil || owner != tc.flow || field != tc.field {
				t.Fatalf("local write path = owner %q, field %q, error %v", owner, field, err)
			}
		})
	}
	if _, owner, err := wave1ResolveEntityPathWithOwner(source, "child", "state.parent_only"); err == nil || owner == "." {
		t.Fatalf("child borrowed parent read: owner %q, error %v", owner, err)
	}
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("child", "worker")
	if err != nil {
		t.Fatal(err)
	}
	target := wave1WriteTarget{Node: node, Entity: true, Target: "state.parent_only", Field: "parent_only"}
	write, _ := wave1WriteTargetContract(source, target)
	if write.FlowID == "." {
		t.Fatalf("child borrowed parent write: %#v", write)
	}
	_, owner, _, err := wave1ResolveWriteTargetPath(source, target)
	if err != nil || owner != "child" {
		t.Fatalf("undeclared write must stay receiver-local for compliance validation: owner %q, error %v", owner, err)
	}
}
