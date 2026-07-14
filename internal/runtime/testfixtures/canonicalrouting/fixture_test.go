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
		FanInStream,
		FanInBarrier,
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
	Prove(t, RootIngress, ParentConnect, TemplateSelectExisting, TemplateSelectOrCreate, TemplateReply, TemplateCreateMintedKey, FanInStream, FanInBarrier)
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
	ProveSource(t, canonicalRoutingTeachingContractSource(t))
}

func canonicalRoutingTeachingContractSource(t *testing.T) SourceToken {
	t.Helper()
	return ExecuteSource(t,
		SourceID("internal/runtime/testfixtures/canonicalrouting/fixture_test.go:canonicalRoutingTeachingContractSource"), func() {
			artifactIDs := canonicalExampleNames()
			wantSet := map[string]struct{}{}
			for _, id := range artifactIDs {
				wantSet[strings.Split(string(id), "/")[0]] = struct{}{}
			}
			want := make([]string, 0, len(wantSet))
			for name := range wantSet {
				want = append(want, name)
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

			for _, id := range artifactIDs {
				t.Run(string(id), func(t *testing.T) {
					root := ExampleRoot(t, id)
					readme, err := os.ReadFile(filepath.Join(root, "README.md"))
					if err != nil {
						t.Fatal(err)
					}
					text := string(readme)
					for _, required := range []string{
						"swarm verify --contracts examples/routing/" + string(id),
						"swarm serve --contracts examples/routing/" + string(id),
						"Expected:",
					} {
						if !strings.Contains(text, required) {
							t.Fatalf("README missing %q", required)
						}
					}
					if !strings.Contains(text, "If ") && !strings.Contains(text, "On ") && !strings.Contains(text, "For ") {
						t.Fatal("README must state recovery or fail-closed guidance")
					}
					validateCanonicalPublishCommands(t, text)
					if id == FanInStream || id == FanInBarrier {
						for _, required := range []string{"Proof boundary:", "producer", "project"} {
							if !strings.Contains(text, required) {
								t.Fatalf("fan-in README missing producer-driven supported-path accounting %q", required)
							}
						}
						if strings.Contains(text, "full producer-driven execution is not claimed here") {
							t.Fatal("fan-in README retains the retired producer-boundary limitation")
						}
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
		})
}

func validateCanonicalPublishCommands(t *testing.T, readme string) {
	t.Helper()
	found := false
	for _, line := range strings.Split(readme, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 3 || fields[0] != "swarm" || fields[1] != "event" || fields[2] != "publish" {
			continue
		}
		found = true
		payloadFlag := 0
		for index, field := range fields[3:] {
			switch {
			case field == "--payload-json":
				if index+4 >= len(fields) || strings.HasPrefix(fields[index+4], "--") {
					t.Fatalf("canonical publish command has no --payload-json value: %s", line)
				}
				payloadFlag++
			case strings.HasPrefix(field, "--payload-json="):
				if strings.TrimPrefix(field, "--payload-json=") == "" {
					t.Fatalf("canonical publish command has an empty --payload-json value: %s", line)
				}
				payloadFlag++
			case field == "--payload" || strings.HasPrefix(field, "--payload="):
				t.Fatalf("canonical publish command uses unsupported --payload flag: %s", line)
			}
		}
		if payloadFlag != 1 {
			t.Fatalf("canonical publish command must use exactly one --payload-json flag: %s", line)
		}
	}
	if !found {
		t.Fatal("README must contain a swarm event publish command")
	}
}
