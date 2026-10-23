package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"

	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runstart"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRewrite2566FiniteFixturePlanMatchesReviewedOutputs(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := finiteFixtureRootWithEmbeddedSpecSuccessor(t, repo)
	body, err := os.ReadFile(filepath.Join(repo, "scripts/rewrite-stages-2566/finite-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var p plan
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Baseline != "c6a90a08be52e613fe273a85875c9ff57bcd28b3" || p.Scope == "" || len(p.Changes) == 0 {
		t.Fatal("missing or changed historical finite-fixture baseline")
	}
	seen := map[string]bool{}
	composed := 0
	for _, c := range p.Changes {
		if seen[c.File] {
			t.Fatalf("duplicate ledger file %s", c.File)
		}
		seen[c.File] = true
		t.Run(c.File, func(t *testing.T) {
			before := finiteFixtureGitSource(t, repo, p.Baseline, c.File)
			if digest(before) != c.BeforeHash {
				t.Fatal("historical baseline does not match the recorded Before")
			}
			historical, err := rewrite(c, before)
			if err != nil {
				t.Fatal(err)
			}
			second, err := rewrite(c, historical)
			if err != nil || !bytes.Equal(second, historical) {
				t.Fatalf("historical replay is not byte-idempotent: %v", err)
			}
			var predecessor []byte
			if _, _, _, listed := finiteFixtureRootInputTransform(c.File); listed {
				composed++
				predecessor = finiteFixtureGitSource(t, repo, finiteFixtureFeedPredecessor, c.File)
			}
			if err := checkFiniteFixtureCurrentOutput(root, c, historical, predecessor); err != nil {
				t.Fatal(err)
			}
		})
	}
	if composed != 2 {
		t.Fatalf("root-input compositions=%d, want exactly the two reviewed files", composed)
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

func TestRewrite2566WhatsAppStandingFixturesRefuseFiniteInitiation(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	for _, name := range []string{"whatsapp-session-input", "whatsapp-session-notice"} {
		t.Run(name, func(t *testing.T) {
			bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, filepath.Join(repo, "internal/serveapp/testdata", name),
				contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			flowID := "."
			if name == "whatsapp-session-notice" {
				flowID = "notices"
			}
			var refusal *runstart.FiniteStartError
			if err := runstart.ValidateFinite(semanticview.Wrap(bundle), nil); !errors.As(err, &refusal) || refusal.FlowID != flowID {
				t.Fatalf("standing WhatsApp source gained finite initiation authority: %v", err)
			}
		})
	}
}
