package apiv1

import (
	"context"
	"fmt"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type stageCompletionJourneyStore interface {
	RunReadStore
	ObservabilityReadStore
	APIIdempotencyStore
	runtimebus.EventStore
	runtimepipeline.WorkflowPersistenceOwner
	runtimerunlifecycle.CandidateOwner
	apiTestDurableWorkflowRoles
	sourceartifactfixture.Writer
	sourceartifactfixture.Reader
	RequestCompletionCandidate(context.Context, runtimerunlifecycle.CandidateRequest) (runtimerunlifecycle.CandidateRequestDisposition, error)
}

func TestExactStageCompletionRealWriterAndRehydratedSourceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected stageCompletionJourneyStore
			var reopen func() stageCompletionJourneyStore
			if backend == "sqlite" {
				s, open := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background())
				selected, reopen = s, func() stageCompletionJourneyStore { return open() }
			} else {
				s, open := storetest.StartPostgresRuntimeStoreWithReopen(t)
				selected, reopen = s, func() stageCompletionJourneyStore { return open() }
			}
			bundle := exactStageCompletionJourneyBundle(t)
			source := semanticview.Wrap(bundle)
			fact := sourceArtifactFactForTestBundle(t, bundle)
			sourceartifactfixture.RequireArtifact(t, testAuthorActivityContextForSource(context.Background(), fact), selected, bundle.SourceArtifact)
			var coordinator *runtimepipeline.PipelineCoordinator
			bus, err := newScopedAPITestEventBus(t, selected, runtimebus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact,
				InterceptorProvider: func() []runtimebus.EventInterceptor {
					if coordinator == nil {
						return nil
					}
					return []runtimebus.EventInterceptor{coordinator}
				},
			})
			if err != nil {
				t.Fatalf("create selected EventBus: %v", err)
			}
			coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, completeAPITestDurableWorkflowOptions(t, selected, bus, runtimepipeline.PipelineCoordinatorOptions{
				Module:             newRunCompletionSystemNodeModule(t, source),
				Persistence:        runtimepipeline.NewWorkflowPersistence(selected),
				SourceArtifactFact: fact,
			}))
			handler := eventPublishTestHandlerWithStores(t, selected, selected, selected, bus, source)
			runID := uuid.NewString()
			started := rpcCall(t, handler, runStartBody(runID, fact.BundleHash(), "flow.started", `{"topic":"case-distinct"}`, "idem-case-start"))
			if started.Error != nil {
				t.Fatalf("run.start: %#v", started.Error)
			}
			stageCompletionWaitForDelivery(t, selected, runID, "flow.started")
			stageCompletionAssertStoredState(t, selected, runID, "ready")

			recovered, recoveredSource := stageCompletionRehydrate(t, selected, reopen, fact.BundleHash())
			catalog := stageCompletionCatalog(t, recoveredSource)
			ctx := testAuthorActivityContextForSource(context.Background(), fact)
			if _, err := recovered.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil {
				t.Fatalf("request nonterminal completion: %v", err)
			}
			outcome, err := storetest.ExecuteRunCompletionCandidate(ctx, recovered, fact.BundleHash(), runID, catalog)
			if err != nil || outcome.Outcome != runtimerunlifecycle.OutcomeAwaitMutation {
				t.Fatalf("nonterminal ready candidate = %s, %v", outcome.Outcome, err)
			}
			stageCompletionAssertPublicStatus(t, handler, runID, "running")

			finished := rpcCall(t, handler, eventPublishBody(runID, fact.BundleHash(), "flow.finish", `{"topic":"case-distinct"}`, "", "idem-case-finish"))
			if finished.Error != nil {
				t.Fatalf("event.publish finish: %#v", finished.Error)
			}
			stageCompletionWaitForDelivery(t, selected, runID, "flow.finish")
			stageCompletionAssertStoredState(t, selected, runID, "Ready")
			recovered, recoveredSource = stageCompletionRehydrate(t, selected, reopen, fact.BundleHash())
			catalog = stageCompletionCatalog(t, recoveredSource)
			if _, err := recovered.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil {
				t.Fatalf("request terminal completion: %v", err)
			}
			outcome, err = storetest.ExecuteRunCompletionCandidate(ctx, recovered, fact.BundleHash(), runID, catalog)
			if err != nil || outcome.Outcome != runtimerunlifecycle.OutcomeTerminallyEligible {
				t.Fatalf("terminal Ready candidate = %s, %v", outcome.Outcome, err)
			}
			stageCompletionAssertPublicStatus(t, handler, runID, "completed")
		})
	}
}

func exactStageCompletionJourneyBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := canonicalrouting.CopyStageCompletionJourney(t)
	repo := runCompletionRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load authored case-distinct bundle: %v", err)
	}
	return bundle
}

func stageCompletionRehydrate(t *testing.T, selected stageCompletionJourneyStore, reopen func() stageCompletionJourneyStore, hash string) (stageCompletionJourneyStore, semanticview.Source) {
	t.Helper()
	record, err := selected.GetSourceArtifact(context.Background(), hash)
	if err != nil {
		t.Fatalf("read retained selected source: %v", err)
	}
	artifact, err := record.Decode()
	if err != nil {
		t.Fatalf("decode retained selected source: %v", err)
	}
	repo := runCompletionRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatalf("recompile retained selected source: %v", err)
	}
	recovered := reopen()
	requireStageCompletionIndependentOwner(t, selected, recovered)
	return recovered, semanticview.Wrap(bundle)
}

func requireStageCompletionIndependentOwner(t testing.TB, original, recovered stageCompletionJourneyStore) {
	t.Helper()
	if recovered == nil || recovered == original {
		t.Fatal("stage completion rehydration requires an independent native same-location owner")
	}
}

func stageCompletionCatalog(t *testing.T, source semanticview.Source) runtimerunlifecycle.FinalCatalog {
	t.Helper()
	root, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok {
		t.Fatal("rehydrated source has no root graph")
	}
	child, ok := semanticview.WorkflowStageTopology(source, "discovery")
	if !ok {
		t.Fatal("rehydrated source has no discovery graph")
	}
	classifier, err := runtimecontracts.NewWorkflowStageClassifier(root, map[string]runtimecontracts.WorkflowStageTopology{"discovery": child})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := runtimerunlifecycle.NewCompiledFinalCatalog(classifier)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func stageCompletionWaitForDelivery(t *testing.T, selected any, runID, eventName string) {
	t.Helper()
	nodeID := identitytest.FlowNode(t, "discovery", "pipeline").Key()
	rootNodeID := ""
	want := 1
	if eventName == "flow.finish" {
		rootNodeID = identitytest.RootNode(t, "root-completion").Key()
		want = 2
	}
	requireAPIV1Convergence(t, "case-distinct handler delivery "+eventName, func() (bool, error) {
		count, err := storetest.CountStageCompletionNodeDeliveries(context.Background(), selected, runID, eventName, nodeID, rootNodeID)
		if err != nil {
			return false, err
		}
		return count == want, fmt.Errorf("delivered %s count=%d, want %d exact root/child deliveries", eventName, count, want)
	})
}

func stageCompletionAssertStoredState(t *testing.T, selected any, runID, want string) {
	t.Helper()
	state, err := storetest.ReadStageCompletionDiscoveryState(context.Background(), selected, runID, runtimepipeline.FlowInstanceEntityID("discovery"))
	if err != nil || state != want {
		t.Fatalf("stored selected stage = %q, %v, want %q", state, err, want)
	}
}

func stageCompletionAssertPublicStatus(t *testing.T, handler *Handler, runID, want string) {
	t.Helper()
	for _, method := range []string{"run.get", "run.diagnose"} {
		resp := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q,"params":{"run_id":%q}}`, method, method, runID))
		if resp.Error != nil {
			t.Fatalf("%s error = %#v", method, resp.Error)
		}
		run := asMap(t, asMap(t, resp.Result)["run"])
		if run["status"] != want {
			t.Fatalf("%s status = %#v, want %s", method, run["status"], want)
		}
	}
}
