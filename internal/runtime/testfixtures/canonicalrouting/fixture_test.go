package canonicalrouting

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func canonicalExampleNames() []ArtifactID {
	return []ArtifactID{
		RootIngress,
		ParentConnect,
		TemplateSelectExisting,
		TemplateSelectOrCreate,
		TemplateReply,
		TemplateCreateMintedKey,
	}
}

func TestCanonicalPositiveFixtureOwnerSetIsClosed(t *testing.T) {
	for _, id := range canonicalExampleNames() {
		if _, ok := canonicalExamplePath(id); !ok {
			t.Fatalf("canonical artifact %q was rejected", id)
		}
	}
	for _, id := range []ArtifactID{
		"notify-all-children",
		"examples/routing/notify-all-children",
		"tests/tier7-composition/test-full-lifecycle",
	} {
		if root, ok := canonicalExamplePath(id); ok {
			t.Fatalf("non-canonical artifact %q resolved to positive owner %q", id, root)
		}
	}
}

func TestCanonicalRoutingExamplesLoadAndVerify(t *testing.T) {
	Prove(t, RootIngress, ParentConnect, TemplateSelectExisting, TemplateSelectOrCreate, TemplateReply, TemplateCreateMintedKey)
	for _, name := range canonicalExampleNames() {
		t.Run(string(name), func(t *testing.T) {
			root := ExampleRoot(t, name)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
				RepoRoot(t),
				root,
				runtimecontracts.DefaultPlatformSpecFile(RepoRoot(t)),
			)
			if err != nil {
				t.Fatalf("load canonical example: %v", err)
			}
			report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})
			if findings := report.HardInvalidities(); len(findings) != 0 {
				t.Fatalf("hard invalidities: %#v", findings)
			}
		})
	}
}

func TestCanonicalRoutingExampleInventoryAndTeachingContract(t *testing.T) {
	// routing-example-census: parser-only issue=none owner=examples.routing.teaching_contract proof=internal/runtime/testfixtures/canonicalrouting/fixture_test.go:TestCanonicalRoutingExampleInventoryAndTeachingContract
	artifactIDs := canonicalExampleNames()
	want := make([]string, 0, len(artifactIDs))
	for _, id := range artifactIDs {
		want = append(want, filepath.Base(string(id)))
	}
	sort.Strings(want)
	entries, err := os.ReadDir(filepath.Join(RepoRoot(t), "examples", "routing"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "notify-all-children" {
			continue
		}
		got = append(got, entry.Name())
	}
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("canonical routing inventory = %v, want %v", got, want)
	}

	for _, name := range want {
		t.Run(name, func(t *testing.T) {
			root := ExampleRoot(t, ArtifactID("examples/routing/"+name))
			readme, err := os.ReadFile(filepath.Join(root, "README.md"))
			if err != nil {
				t.Fatal(err)
			}
			text := string(readme)
			for _, required := range []string{
				"swarm verify --contracts examples/routing/" + name,
				"swarm serve --contracts examples/routing/" + name,
				"swarm event publish",
				"Expected:",
			} {
				if !strings.Contains(text, required) {
					t.Fatalf("README missing %q", required)
				}
			}
			if !strings.Contains(text, "If ") && !strings.Contains(text, "On ") && !strings.Contains(text, "For ") {
				t.Fatal("README must state recovery or fail-closed guidance")
			}

			err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
					return nil
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				for _, forbidden := range []string{"delivery:", "on_missing:", "on_conflict:", "broadcast:"} {
					if strings.Contains(string(raw), forbidden) {
						t.Fatalf("%s teaches retired/transitional field %s", path, forbidden)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
