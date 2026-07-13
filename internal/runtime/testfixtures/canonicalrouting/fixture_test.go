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

func canonicalExampleNames() []string {
	return []string{
		RootIngress,
		ParentConnect,
		TemplateSelectExisting,
		TemplateSelectOrCreate,
		TemplateReply,
		TemplateCreateMintedKey,
	}
}

func TestCanonicalRoutingExamplesLoadAndVerify(t *testing.T) {
	for _, name := range canonicalExampleNames() {
		t.Run(name, func(t *testing.T) {
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
	want := canonicalExampleNames()
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
			root := ExampleRoot(t, name)
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
