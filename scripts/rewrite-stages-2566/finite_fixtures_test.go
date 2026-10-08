package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runstart"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestRewrite2566FiniteFixturePlanMatchesReviewedOutputs(t *testing.T) {
	if err := apply(canonicalrouting.RepoRoot(t), "scripts/rewrite-stages-2566/finite-fixtures.json", false, true); err != nil {
		t.Fatal(err)
	}
}

// These are sources of real finite run.start calls, not RPC doubles, scenario
// imports, event.publish services, or low-level construction tests.
func TestRewrite2566FiniteInitiationSourcesHaveCompleteClosure(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	sources := []struct {
		name  string
		build func(testing.TB) string
	}{
		{"claude-release", func(testing.TB) string {
			return filepath.Join(repo, "internal/releasee2e/testdata/claude_cli_managed_lifecycle")
		}},
		{"numeric-release", func(testing.TB) string {
			return filepath.Join(repo, "tests/tier11-flow-composition/test-numeric-data-scatter-park")
		}},
		{"resource-release", func(testing.TB) string {
			return filepath.Join(repo, "internal/releasee2e/testdata/claude_resource_read")
		}},
		{"served-reporter", canonicalrouting.CopyServedFanOutReporter},
		{"two-pinned-feeds", canonicalrouting.CopyTwoDeploymentFeeds},
		{"deployment-corpus", func(t testing.TB) string {
			return canonicalrouting.CopyNotifyAllChildren(t, canonicalrouting.NotifyAllChildrenOptions{FiniteLifecycle: true})
		}},
		{"served-root-ingress", canonicalrouting.CopyRootIngressServedFollowUp},
		{"novel-scenario-root-input", canonicalrouting.WriteNovelDerivedScenarioBundleWithRootInput},
		{"numeric-scenario-overlay", func(t testing.TB) string {
			root := canonicalrouting.WriteNovelDerivedScenarioBundleWithRootInput(t)
			canonicalrouting.InstallNovelNumericScenarioLifecycle(t, root)
			return root
		}},
		{"authored-response-scenario-overlay", func(t testing.TB) string {
			root := canonicalrouting.WriteNovelDerivedScenarioBundleWithRootInput(t)
			canonicalrouting.InstallNovelAuthoredScenarioLifecycle(t, root)
			return root
		}},
		{"numeric-ingress", canonicalrouting.CopySemanticNumericIngress},
		{"deployment-run-start", canonicalrouting.CopyDeploymentRunStart},
	}
	for _, route := range []string{"root", "singleton", "dynamic"} {
		for _, keyed := range []bool{false, true} {
			name := route + "/unkeyed"
			if keyed {
				name = route + "/keyed"
			}
			sources = append(sources, struct {
				name  string
				build func(testing.TB) string
			}{name, func(t testing.TB) string { return canonicalrouting.CopySelectedDeploymentResource(t, route, keyed) }})
		}
	}
	for _, source := range sources {
		t.Run(source.name, func(t *testing.T) {
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, source.build(t), contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			if err := runstart.ValidateFinite(semanticview.Wrap(bundle), nil); err != nil {
				t.Fatal(err)
			}
			if findings := bootverify.Run(context.Background(), semanticview.Wrap(bundle), bootverify.Options{Purpose: bootverify.StructuralValidation}).HardInvalidities(); len(findings) != 0 {
				t.Fatalf("finite fixture failed ordinary source verification: %+v", findings)
			}
		})
	}
}

func TestRewrite2566FiniteReleaseRootsDoNotCompleteTheirWorkers(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	for _, fixture := range []struct{ source, worker, entry, final string }{
		{"internal/releasee2e/testdata/claude_cli_managed_lifecycle", "worker", "pending", "done"},
		{"tests/tier11-flow-composition/test-numeric-data-scatter-park", "registry", "registered", "archived"},
	} {
		t.Run(fixture.source, func(t *testing.T) {
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, fixture.source), contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			root, ok := bundle.WorkflowStageTopology(".")
			if !ok || root.InitialStage != "done" || !reflect.DeepEqual(root.FinalStageIDs(), []string{"done"}) || len(root.Handlers) != 0 || len(root.Edges) != 0 {
				t.Fatalf("routing-only root acquired work or lost its end: %+v", root)
			}
			worker, ok := bundle.WorkflowStageTopology(fixture.worker)
			entry, err := worker.ResolveStage(fixture.entry)
			if !ok || err != nil || entry.IsFinal() || worker.InitialStage != fixture.entry || !reflect.DeepEqual(worker.FinalStageIDs(), []string{fixture.final}) {
				t.Fatalf("ended container incorrectly ended its worker: %+v / %v", worker, err)
			}
		})
	}
}

func TestRewrite2566ScenarioOverlaysRejectMissingCompletion(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	for _, authored := range []bool{false, true} {
		name := "numeric"
		if authored {
			name = "authored"
		}
		t.Run(name, func(t *testing.T) {
			root := canonicalrouting.CopyNovelScenarioMissingCompletion(t, authored)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			findings := bootverify.Run(context.Background(), semanticview.Wrap(bundle), bootverify.Options{Purpose: bootverify.StructuralValidation}).HardInvalidities()
			for _, finding := range findings {
				if finding.CheckID == "semantic_drift_unreachable_state" && finding.Location == "fulfillment" && strings.Contains(finding.Message, "done") {
					return
				}
			}
			t.Fatalf("post-construction overlay lost completion without source refusal: %+v", findings)
		})
	}
}

func TestRewrite2566DeliberateServicesStillRefuseFiniteInitiation(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	for _, build := range []func(testing.TB) string{
		func(t testing.TB) string { return canonicalrouting.CopyFiniteInitiation(t, false) },
		canonicalrouting.CopyFiniteStatelessContainer,
		canonicalrouting.WriteNovelDerivedScenarioBundle,
		func(t testing.TB) string {
			return canonicalrouting.CopyNotifyAllChildren(t, canonicalrouting.NotifyAllChildrenOptions{})
		},
	} {
		bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, build(t), contracts.DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatal(err)
		}
		var refusal *runstart.FiniteStartError
		if err := runstart.ValidateFinite(semanticview.Wrap(bundle), nil); !errors.As(err, &refusal) || refusal.FlowID != "." {
			t.Fatalf("service acquired a completion exemption: %v", err)
		}
	}
}

func TestRewrite2566NoFinalRootCensusIsExplicitlyClassified(t *testing.T) {
	groups := map[string][]string{
		"service examples; ordinary event publication, not finite run.start": {
			"examples/integrations/telegram-agent", "examples/routing/fan-in/barrier", "examples/routing/fan-in/stream",
			"examples/routing/notify-all-children", "examples/routing/parent-connect", "examples/routing/policy-rules",
			"examples/routing/template-create-minted-key", "examples/routing/template-reply",
			"examples/routing/template-select-existing", "examples/routing/template-select-or-create",
		},
		"standing/startup/provider/component sources, not finite run.start": {
			"internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/h1",
			"internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/h2",
			"internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/h3",
			"internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/m33",
			"internal/runtime/testfixtures/canonicalrouting/testdata/clock-deployment/nested",
			"internal/cliapp/archetypes/zero-agent-automation", "internal/releasee2e/testdata/channel_onboarding_release",
			"internal/releasee2e/testdata/full_lifecycle/standing_telegram", "internal/releasee2e/testdata/node_identity_activity",
			"internal/releasee2e/testdata/standing_root_tree", "internal/runtime/conformance/testdata/stage-lifecycle-identity",
			"internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp", "internal/runtime/scenarioderivation/testdata/hostile",
			"internal/runtime/testfixtures/canonicalrouting/testdata/fan-out-execution/mixed-agent", "internal/store/selected/testdata/retained_actor_admission",
		},
		"scenario-import or component corpus; not the finite run.start admission path": {
			"tests/tier1-primitives/test-clear-gates", "tests/tier1-primitives/test-record-evidence",
			"tests/tier1-primitives/test-rules-data-accumulation", "tests/tier1-primitives/test-rules-else",
			"tests/tier1-primitives/test-rules-match", "tests/tier1-primitives/test-rules-no-match", "tests/tier1-primitives/test-sets-gate",
			"tests/tier10-policy-patterns/test-policy-counter-escalate", "tests/tier12-runtime-fork/test-run-scoped-flow-agent-fork",
			"tests/tier12-runtime-fork/test-selected-contract-fork-execution", "tests/tier12-runtime-tools/test-flow-data-access",
			"tests/tier5-flow-lifecycle/test-create-flow-instance-config", "tests/tier5-flow-lifecycle/test-create-flow-instance",
			"tests/tier5-flow-lifecycle/test-template-no-boot-instance", "tests/tier6-event-loop/test-chain-depth-limit",
			"tests/tier6-event-loop/test-cross-entity-concurrent", "tests/tier9-composition-patterns/test-compose-guard-counter-escalate",
		},
		"deliberately unreachable boot-verification rejection": {"tests/tier8-boot-verification/test-boot-state-machine-unreachable"},
	}
	expected := map[string]bool{}
	for _, paths := range groups {
		for _, path := range paths {
			if expected[path] {
				t.Fatalf("duplicate service classification %s", path)
			}
			expected[path] = true
		}
	}
	repo := canonicalrouting.RepoRoot(t)
	command := exec.Command("git", "ls-files", "-z", "--", "*schema.yaml")
	command.Dir = repo
	raw, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[string]bool{}
	for _, name := range strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00") {
		if filepath.Base(name) == "schema.yaml" {
			schemas[filepath.Dir(name)] = true
		}
	}
	actual := map[string]bool{}
	for source := range schemas {
		nested := false
		for parent := filepath.Dir(source); parent != "."; parent = filepath.Dir(parent) {
			if schemas[parent] {
				nested = true
				break
			}
		}
		if nested {
			continue
		}
		body, err := os.ReadFile(filepath.Join(repo, source, "schema.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := contracts.AdmitFlowSchemaValue(snapshot.Document(source + "/schema.yaml").Root())
		if err != nil {
			t.Fatal(err)
		}
		if len(schema.StageDeclarations.FinalStages()) == 0 {
			actual[source] = true
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("no-final root census needs explicit caller classification: actual=%v expected=%v", actual, expected)
	}
}
