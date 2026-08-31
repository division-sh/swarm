package canonicalrouting

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"gopkg.in/yaml.v3"
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
		HarnessInjection,
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
	Prove(t, RootIngress, ParentConnect, TemplateSelectExisting, TemplateSelectOrCreate, TemplateReply, TemplateCreateMintedKey, FanInStream, FanInBarrier, HarnessInjection)
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

func TestReleaseE2EClaudeLifecycleFixtureLoadsAndVerifies(t *testing.T) {
	const fixture = ArtifactID("internal/releasee2e/testdata/claude_cli_managed_lifecycle")
	Prove(t, ArtifactID("internal/releasee2e/testdata/claude_cli_managed_lifecycle"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repo,
		filepath.Join(repo, filepath.FromSlash(string(fixture))),
		runtimecontracts.DefaultPlatformSpecFile(repo),
	)
	if err != nil {
		t.Fatalf("load release E2E Claude lifecycle fixture: %v", err)
	}
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("release E2E Claude lifecycle fixture hard invalidities: %#v", findings)
	}
	registry := runtimetools.NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	actorDefinitions := registry.GenerateEmitToolsForActor(models.AgentConfig{
		ID:         "release-worker",
		Role:       "release-worker",
		FlowID:     "worker",
		FlowPath:   "worker",
		EmitEvents: []string{"worker/agent.completed"},
	}, nil)
	if len(actorDefinitions) != 1 {
		t.Fatalf("release E2E actor definitions = %#v, want one flow-scoped emit definition", actorDefinitions)
	}
	roleDefinitions := registry.GenerateEmitToolsForRole("release-worker", nil)
	if len(roleDefinitions) != 1 {
		t.Fatalf("release E2E role/global definitions = %#v, want one deliberately different definition", roleDefinitions)
	}
	if runtimellm.ToolDefinitionIdentity(roleDefinitions[0]) == runtimellm.ToolDefinitionIdentity(actorDefinitions[0]) {
		t.Fatalf("release E2E role/global definition unexpectedly equals actor/flow definition: %#v", actorDefinitions[0])
	}
}

func TestReleaseE2EChannelOnboardingFixtureIsRegistered(t *testing.T) {
	Prove(t, ArtifactID("internal/releasee2e/testdata/channel_onboarding_release"))
}

func TestReleaseE2EGoldenAgentWorkloadFixtureLoadsAndVerifies(t *testing.T) {
	Prove(t, ArtifactID("internal/releasee2e/testdata/golden_agent_workload"))
}

func TestReleaseE2EFullLifecycleFixtureLoadsAndVerifies(t *testing.T) {
	const fixture = ArtifactID("internal/releasee2e/testdata/full_lifecycle/standing_telegram")
	Prove(t, ArtifactID("internal/releasee2e/testdata/full_lifecycle/standing_telegram"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		repo,
		filepath.Join(repo, filepath.FromSlash(string(fixture))),
		runtimecontracts.DefaultPlatformSpecFile(repo),
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory},
	)
	if err != nil {
		t.Fatalf("load release E2E full lifecycle fixture: %v", err)
	}
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("hash release E2E full lifecycle fixture: %v", err)
	}
	sourceFact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		t.Fatalf("create release E2E full lifecycle source fact: %v", err)
	}
	projection, err := runtimepkg.AdmitEffectiveSourceProjection(runtimepkg.EffectiveSourceProjectionRequest{
		Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceFact,
	})
	if err != nil {
		t.Fatalf("admit release E2E full lifecycle effective source: %v", err)
	}
	report := runtimebootverify.Run(context.Background(), projection.Source(), runtimebootverify.Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("release E2E full lifecycle fixture hard invalidities: %#v", findings)
	}
}

func TestSelectedForkFlowScopedMCPFixtureLoadsAndVerifies(t *testing.T) {
	const fixture = ArtifactID("internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp")
	Prove(t, ArtifactID("internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repo,
		filepath.Join(repo, filepath.FromSlash(string(fixture))),
		runtimecontracts.DefaultPlatformSpecFile(repo),
	)
	if err != nil {
		t.Fatalf("load selected-fork flow-scoped MCP fixture: %v", err)
	}
	report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("selected-fork flow-scoped MCP fixture hard invalidities: %#v", findings)
	}
}

func TestCanonicalRoutingExampleInventoryAndTeachingContract(t *testing.T) {
	ProveSource(t, canonicalRoutingTeachingContractSource(t))
}

func TestCanonicalRoutingDocumentationRejectsRetiredInstanceIdentitySyntax(t *testing.T) {
	root := filepath.Join(RepoRoot(t), "examples", "routing")
	forbidden := []string{
		"resolution.instance_key",
		"instance.key.",
		"mint: uuid",
		"mint: event_id",
	}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || strings.ToLower(filepath.Ext(path)) != ".md" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, retired := range forbidden {
			if strings.Contains(string(raw), retired) {
				t.Fatalf("%s retains retired instance identity syntax %q", path, retired)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTemplateInstanceCheckedSchemasUseScalarPolicyFreeGrammar(t *testing.T) {
	paths := []string{
		"examples/routing/fan-in/barrier/operating/schema.yaml",
		"examples/routing/fan-in/stream/operating/schema.yaml",
		"examples/routing/notify-all-children/account/schema.yaml",
		"examples/routing/template-create-minted-key/validator/schema.yaml",
		"examples/routing/template-reply/requester/schema.yaml",
		"examples/routing/template-select-existing/account/schema.yaml",
		"examples/routing/template-select-or-create/account/schema.yaml",
		"tests/tier5-flow-lifecycle/test-create-flow-instance-config/worker-flow/schema.yaml",
		"tests/tier5-flow-lifecycle/test-create-flow-instance-duplicate/worker-flow/schema.yaml",
		"tests/tier5-flow-lifecycle/test-create-flow-instance/worker-flow/schema.yaml",
		"tests/tier9-composition-patterns/test-compose-create-instance-config/worker/schema.yaml",
		"tests/tier11-flow-composition/test-dynamic-flow-instance/worker/schema.yaml",
	}
	for _, rel := range paths {
		t.Run(rel, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(RepoRoot(t), filepath.FromSlash(rel)))
			if err != nil {
				t.Fatal(err)
			}
			var document yaml.Node
			if err := yaml.Unmarshal(raw, &document); err != nil {
				t.Fatalf("decode checked schema: %v", err)
			}
			instances := 0
			walkYAMLMapping(t, &document, func(key string, value *yaml.Node) {
				switch key {
				case "instance":
					instances++
					if value.Kind != yaml.ScalarNode || strings.TrimSpace(value.Value) == "" {
						t.Fatalf("instance must be one non-empty scalar, got kind=%d value=%q", value.Kind, value.Value)
					}
				case "on_missing", "on_conflict", "address":
					t.Fatalf("checked schema retains retired routing key %q", key)
				}
			})
			if instances != 1 {
				t.Fatalf("instance declarations = %d, want exactly 1", instances)
			}
		})
	}
}

func walkYAMLMapping(t *testing.T, node *yaml.Node, visit func(string, *yaml.Node)) {
	t.Helper()
	if node == nil {
		return
	}
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			visit(node.Content[index].Value, node.Content[index+1])
		}
	}
	for _, child := range node.Content {
		walkYAMLMapping(t, child, visit)
	}
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

			index, err := os.ReadFile(filepath.Join(RepoRoot(t), "examples", "routing", "README.md"))
			if err != nil {
				t.Fatal(err)
			}
			for _, root := range append(artifactIDs, ArtifactID("notify-all-children")) {
				if !strings.Contains(string(index), "`"+string(root)+"`") {
					t.Fatalf("routing example index does not expose %q", root)
				}
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
						"swarm verify examples/routing/" + string(id),
						"swarm serve examples/routing/" + string(id),
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
