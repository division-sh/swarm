package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	flowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
	"gopkg.in/yaml.v3"
)

func TestResolvedAgentIntent_LocalScopedDeclarationOwner(t *testing.T) {
	repo := repoRoot(t)
	root := writePromptTestBundle(t, repo)
	packagePath := filepath.Join(root, "package.yaml")
	packageYAML, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatal(err)
	}
	packageYAML = append(packageYAML, []byte("packages:\n  - path: extras\n")...)
	if err := os.WriteFile(packagePath, packageYAML, 0o644); err != nil {
		t.Fatal(err)
	}
	writePromptFixtureFile(t, filepath.Join(root, "extras", "package.yaml"), "name: extras\nversion: \"1.0.0\"\nflows: []\n")
	writePromptFixtureFile(t, filepath.Join(root, "extras", "agents.yaml"), "ops-lead:\n  id: ops-lead\n  role: package-ops-lead\n  intent: intent/ops-lead.md\n")
	writePromptFixtureFile(t, filepath.Join(root, "extras", "intent", "ops-lead.md"), "Package-local intent.\n")
	writePromptFixtureFile(t, filepath.Join(root, "flows", "intake", "agents.yaml"), "ops-lead:\n  id: ops-lead\n  role: flow-ops-lead\n  intent: intent/ops-lead.md\n")
	writePromptFixtureFile(t, filepath.Join(root, "flows", "intake", "intent", "ops-lead.md"), "Flow-local intent.\n")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	var entry AgentRegistryEntry
	for _, record := range bundleAgentRecords(bundle) {
		if record.Entry.ID == "ops-lead" && record.Entry.ResolvedIntent.Provenance == "agents.yaml#agents.ops-lead.intent" {
			entry = record.Entry
			break
		}
	}
	if entry.ResolvedIntent.Empty() {
		t.Fatal("ops-lead missing")
	}
	if got, want := entry.ResolvedIntent.Kind, runtimeagentintent.SourceLocal; got != want {
		t.Fatalf("kind = %q, want %q", got, want)
	}
	if got, want := entry.ResolvedIntent.Coordinate, "intent/ops-lead.md"; got != want {
		t.Fatalf("coordinate = %q, want %q", got, want)
	}
	if !strings.Contains(entry.ResolvedIntent.Provenance, "agents.yaml#agents.ops-lead.intent") {
		t.Fatalf("provenance = %q", entry.ResolvedIntent.Provenance)
	}
	if err := entry.ResolvedIntent.Validate(); err != nil {
		t.Fatalf("resolved intent: %v", err)
	}
	wantScoped := map[string]string{
		"agents.yaml#agents.ops-lead.intent":              entry.ResolvedIntent.Content,
		"extras/agents.yaml#agents.ops-lead.intent":       "Package-local intent.\n",
		"flows/intake/agents.yaml#agents.ops-lead.intent": "Flow-local intent.\n",
	}
	gotScoped := map[string]string{}
	for _, record := range bundleAgentRecords(bundle) {
		if record.Entry.ID == "ops-lead" {
			gotScoped[record.Entry.ResolvedIntent.Provenance] = record.Entry.ResolvedIntent.Content
		}
	}
	if len(gotScoped) != len(wantScoped) {
		t.Fatalf("scoped ops-lead intents = %#v, want %#v", gotScoped, wantScoped)
	}
	for provenance, content := range wantScoped {
		if gotScoped[provenance] != content {
			t.Fatalf("scoped intent %s = %q, want %q; all=%#v", provenance, gotScoped[provenance], content, gotScoped)
		}
	}
}

func writePromptFixtureFile(t testing.TB, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestResolvedAgentIntent_InlineExactBytes(t *testing.T) {
	var source runtimeagentintent.Source
	if err := source.UnmarshalYAML(yamlScalarNodeForTest{value: ""}.node()); err == nil {
		t.Fatal("empty local scalar accepted")
	}
	content := "  retain leading and trailing bytes  \n"
	resolved, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "agents.yaml#agents.worker.intent", content)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.Content != content {
		t.Fatalf("content = %q, want exact %q", resolved.Content, content)
	}
}

func resolvedInlineIntentForTest(t testing.TB, provenance, content string) runtimeagentintent.Resolved {
	t.Helper()
	resolved, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", provenance, content)
	if err != nil {
		t.Fatalf("resolve test intent: %v", err)
	}
	return resolved
}

// yamlScalarNodeForTest keeps this test focused on the public YAML source
// decoder without building another contract fixture.
type yamlScalarNodeForTest struct{ value string }

func (n yamlScalarNodeForTest) node() *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: n.value}
}

func TestAgentIntentSourceUnion_FailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
		want string
	}{
		{name: "unknown", yaml: "intent: {guess: worker.md}\n", want: "unsupported"},
		{name: "inline_and_import", yaml: "intent: {inline: worker, import: pack@1}\n", want: "exactly inline"},
		{name: "override_without_import", yaml: "intent: {override: worker.md}\n", want: "exactly inline"},
		{name: "unpinned_import", yaml: "intent: {import: support-drafter}\n", want: "explicitly versioned"},
		{name: "blank_import_version", yaml: "intent: {import: support-drafter@}\n", want: "explicitly versioned"},
		{name: "retired_prompt_ref", yaml: "prompt_ref: worker\n", want: "RETIRED"},
		{name: "retired_prompt_inputs", yaml: "prompt_inputs: [customer]\n", want: "RETIRED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var entry AgentRegistryEntry
			err := yaml.Unmarshal([]byte(tc.yaml), &entry)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestResolvedAgentIntent_DuplicateCanonicalCoordinateFailsClosed(t *testing.T) {
	repo := repoRoot(t)
	root := writePromptTestBundle(t, repo)
	agentsPath := filepath.Join(root, "agents.yaml")
	if err := os.WriteFile(agentsPath, []byte(`first:
  id: first
  role: first
  intent: intent/ops-lead.md
second:
  id: second
  role: second
  intent: intent/ops-lead.md
`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err == nil || !strings.Contains(err.Error(), "duplicate canonical intent coordinate") {
		t.Fatalf("load error = %v, want duplicate canonical coordinate rejection", err)
	}
}

func TestResolvedAgentIntent_CaseCollidingCanonicalCoordinatesFailClosed(t *testing.T) {
	repo := repoRoot(t)
	root := writePromptTestBundle(t, repo)
	upperPath := filepath.Join(root, "intent", "Ops-Lead.md")
	lowerPath := filepath.Join(root, "intent", "ops-lead.md")
	if err := os.WriteFile(upperPath, []byte("Upper-case coordinate.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(lowerPath); err != nil {
		t.Fatal(err)
	} else if same, err := os.Stat(upperPath); err != nil || os.SameFile(info, same) {
		t.Skip("case-insensitive filesystem cannot represent the collision fixture")
	}
	if err := os.WriteFile(filepath.Join(root, "agents.yaml"), []byte(`first:
  id: first
  role: first
  intent: intent/Ops-Lead.md
second:
  id: second
  role: second
  intent: intent/ops-lead.md
`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err == nil || !strings.Contains(err.Error(), "case-colliding agent intent coordinates") {
		t.Fatalf("load error = %v, want case-colliding coordinate rejection", err)
	}
}

func TestAgentIntentImport_TeachingRejectsUntilPackOwner(t *testing.T) {
	repo := repoRoot(t)
	root := writePromptTestBundle(t, repo)
	agentsPath := filepath.Join(root, "agents.yaml")
	if err := os.WriteFile(agentsPath, []byte("worker:\n  intent: {import: support-drafter@1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err == nil || !strings.Contains(err.Error(), "unavailable") || !strings.Contains(err.Error(), "#1685/#1770") {
		t.Fatalf("load error = %v, want teaching rejection", err)
	}
}

func TestResolveAgentIntentLocalPath_FailsClosed(t *testing.T) {
	for _, path := range []string{"", ".", "..", "a/../b", "a//b", "/tmp/intent.md", `C:\\intent.md`, `a\\intent.md`} {
		t.Run(strings.NewReplacer("/", "_", `\\`, "_").Replace(path), func(t *testing.T) {
			if _, err := validateLocalIntentPath(path); err == nil {
				t.Fatalf("validateLocalIntentPath(%q) succeeded", path)
			}
		})
	}
}

func TestResolvedAgentIntentLocalFile_FailsClosedOnHostileFilesystemShapes(t *testing.T) {
	repo := repoRoot(t)
	for _, tc := range []struct {
		name     string
		mutate   func(testing.TB, string)
		contains string
	}{
		{
			name: "leaf_symlink",
			mutate: func(t testing.TB, root string) {
				t.Helper()
				path := filepath.Join(root, "intent", "ops-lead.md")
				target := filepath.Join(root, "intent", "target.md")
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Skipf("symlink unsupported: %v", err)
				}
			},
			contains: "symlink",
		},
		{
			name: "parent_symlink",
			mutate: func(t testing.TB, root string) {
				t.Helper()
				dir := filepath.Join(root, "intent")
				target := filepath.Join(root, "intent-target")
				if err := os.Rename(dir, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, dir); err != nil {
					t.Skipf("symlink unsupported: %v", err)
				}
			},
			contains: "symlink",
		},
		{
			name: "non_regular",
			mutate: func(t testing.TB, root string) {
				t.Helper()
				path := filepath.Join(root, "intent", "ops-lead.md")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o755); err != nil {
					t.Fatal(err)
				}
			},
			contains: "regular file",
		},
		{
			name: "unreadable",
			mutate: func(t testing.TB, root string) {
				t.Helper()
				if err := os.Chmod(filepath.Join(root, "intent", "ops-lead.md"), 0o200); err != nil {
					t.Fatal(err)
				}
			},
			contains: "not readable",
		},
		{
			name: "invalid_utf8",
			mutate: func(t testing.TB, root string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, "intent", "ops-lead.md"), []byte{0xff}, 0o644); err != nil {
					t.Fatal(err)
				}
			},
			contains: "valid UTF-8",
		},
		{
			name: "blank",
			mutate: func(t testing.TB, root string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, "intent", "ops-lead.md"), []byte(" \n\t"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			contains: "must not be blank",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writePromptTestBundle(t, repo)
			tc.mutate(t, root)
			_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("load error = %v, want %q", err, tc.contains)
			}
		})
	}
}

func TestResolvedAgentIntentLocalFile_IsRelativeToExactDeclaringAgentsYAML(t *testing.T) {
	repo := repoRoot(t)
	root := writePromptTestBundle(t, repo)
	flowDir := filepath.Join(root, "flows", "child")
	if err := os.MkdirAll(filepath.Join(flowDir, "intent"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(flowDir, "agents.yaml"), []byte("child:\n  id: child\n  role: child\n  intent: intent/child.md\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(flowDir, "intent", "child.md"), []byte("Flow-local intent.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "intent", "child.md"), []byte("Wrong root intent.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	packagePath := filepath.Join(root, "package.yaml")
	raw, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, []byte("  - id: child\n    flow: child\n    mode: static\n")...)
	if err := os.WriteFile(packagePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(flowDir, "schema.yaml"), []byte("name: child\nmode: static\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	var entry AgentRegistryEntry
	var ok bool
	for _, candidate := range bundle.ScopedAgentEntries() {
		if candidate.ID == "child" {
			entry, ok = candidate, true
			break
		}
	}
	if !ok || entry.ResolvedIntent.Content != "Flow-local intent.\n" || entry.ResolvedIntent.Coordinate != "flows/child/intent/child.md" {
		t.Fatalf("flow-local resolved intent = %#v", entry.ResolvedIntent)
	}
}

func TestResolvedAgentIntent_DoesNotGuessSources(t *testing.T) {
	repo := repoRoot(t)
	root := writePromptTestBundle(t, repo)
	if err := os.WriteFile(filepath.Join(root, "ops-lead.review.md"), []byte("wrong mode"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "ops_lead.md"), []byte("wrong role"), 0o644); err != nil {
		t.Fatal(err)
	}
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := bundle.AgentEntry("ops-lead")
	if strings.Contains(entry.ResolvedIntent.Content, "wrong") {
		t.Fatalf("implicit source selected: %#v", entry.ResolvedIntent)
	}
}

func TestAssembleAgentPrompt_DeliversCriteriaWithoutChangingIntentIdentity(t *testing.T) {
	flow := FlowContractView{
		Paths: FlowContractPaths{ID: "validation", Flow: "validation"},
		Policy: PolicyDocument{Criteria: map[string]PolicyCriteriaSet{
			"feasibility_exclusions": criteriaValidationTestSet(),
		}},
	}
	root := &FlowContractView{Children: []FlowContractView{flow}}
	bundle := &WorkflowContractBundle{FlowTree: flowmodel.Tree[FlowContractView]{Root: root, ByID: map[string]*FlowContractView{"validation": &root.Children[0]}}}
	resolved, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "flows/validation/agents.yaml#agents.cto.intent", "Base intent.")
	if err != nil {
		t.Fatal(err)
	}
	entry := AgentRegistryEntry{ResolvedIntent: resolved, Criteria: []string{"feasibility_exclusions"}}
	prompt, err := AssembleAgentPrompt(bundle, "validation", entry, []string{"feasibility_exclusions"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := prompt.Text(resolved, entry.Criteria)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Base intent.", "## Contract Criteria", "### feasibility_exclusions", "FX-HARD-01"} {
		if !strings.Contains(got, want) {
			t.Fatalf("assembled prompt missing %q:\n%s", want, got)
		}
	}
	if entry.ResolvedIntent != resolved {
		t.Fatal("criteria assembly mutated resolved intent")
	}
}
