package bootverify

import (
	"context"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestEntityContractResolutionCannotBorrowParentState(t *testing.T) {
	repo := repoRootForBootverifyTest(t)
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: local-state\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "Root:\n  parent_only: text\n  shared: text\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "child", "schema.yaml"), "name: child\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "child", "entities.yaml"), "Child:\n  local: text\n  shared: integer\n")
	bundle := loadFixtureBundleAt(t, repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	source := semanticview.Wrap(bundle)
	for _, tc := range []struct {
		flow, field, typ string
	}{
		{".", "shared", "text"},
		{"child", "shared", "integer"},
		{"child", "local", "text"},
	} {
		t.Run(tc.flow+"/"+tc.field, func(t *testing.T) {
			read, owner, err := wave1ResolveEntityPathWithOwner(source, tc.flow, "entity."+tc.field)
			if err != nil || owner != tc.flow || read.Type != tc.typ {
				t.Fatalf("local read = %#v, owner %q, error %v", read, owner, err)
			}
			node, err := runtimeidentity.AdmitExecutableNodeDeclaration(tc.flow, "worker")
			if err != nil {
				t.Fatal(err)
			}
			target := wave1WriteTarget{Node: node, Entity: true, Target: "entity." + tc.field, Field: tc.field}
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
	if _, owner, err := wave1ResolveEntityPathWithOwner(source, "child", "entity.parent_only"); err == nil || owner == "." {
		t.Fatalf("child borrowed parent read: owner %q, error %v", owner, err)
	}
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("child", "worker")
	if err != nil {
		t.Fatal(err)
	}
	target := wave1WriteTarget{Node: node, Entity: true, Target: "entity.parent_only", Field: "parent_only"}
	write, _ := wave1WriteTargetContract(source, target)
	if write.FlowID == "." {
		t.Fatalf("child borrowed parent write: %#v", write)
	}
	_, owner, _, err := wave1ResolveWriteTargetPath(source, target)
	if err != nil || owner != "child" {
		t.Fatalf("undeclared write must stay receiver-local for compliance validation: owner %q, error %v", owner, err)
	}
}

func TestRun_PromptEntityWriteCannotBorrowParentField(t *testing.T) {
	root := writePromptWriterCoverageFixture(t, `
writer:
  role: writer
  intent: prompts/writer.md
  workspace_class: factory
  manager_fallback: ops
  entity_writes:
    case:
      save: all
`, "case:\n  local_status: text\n", "Use `save_entity_field` for `priority`.\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "root_case:\n  priority: integer\n")
	repo := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "entity_writer_coverage", "undeclared field path priority") {
		t.Fatalf("parent state rescued undeclared child write: %#v", report.Errors())
	}
}
