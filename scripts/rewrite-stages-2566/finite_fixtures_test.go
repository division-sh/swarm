package main

import (
	"context"

	"errors"

	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runstart"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// These are sources of real finite run.start calls, not RPC doubles, scenario
// imports, event.publish services, or low-level construction tests.

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
