package apiv1

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
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
			var db *sql.DB
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, db = s, storetest.DatabaseForTest(s)
			} else {
				_, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected, db = storetest.AdmitPostgresRuntimeStore(t, opened), opened
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
			stageCompletionWaitForDelivery(t, db, backend, runID, "flow.started")
			stageCompletionAssertStoredState(t, db, backend, runID, "ready")

			recovered, recoveredSource := stageCompletionRehydrate(t, selected, db, backend, fact.BundleHash())
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
			stageCompletionWaitForDelivery(t, db, backend, runID, "flow.finish")
			stageCompletionAssertStoredState(t, db, backend, runID, "Ready")
			recovered, recoveredSource = stageCompletionRehydrate(t, selected, db, backend, fact.BundleHash())
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
	root := t.TempDir()
	for path, contents := range map[string]string{
		"entities.yaml":           "run:\n  topic: string\n",
		"schema.yaml":             "initial_state: active\nstates: [active, done]\nterminal_states: [done]\npins:\n  inputs:\n    events: [flow.started, flow.finish]\n",
		"discovery/schema.yaml":   "name: discovery\nstages:\n  ready:\n    initial: true\n  Ready:\n    terminal: true\npins:\n  inputs:\n    events: [flow.started, flow.finish]\n",
		"discovery/entities.yaml": "discovery: {}\n",
		"discovery/events.yaml":   "flow.started:\n  topic:\n    type: string?\nflow.finish:\n  topic:\n    type: string?\n",
		"discovery/nodes.yaml":    "pipeline:\n  execution_type: system_node\n  subscribes_to: [flow.started, flow.finish]\n  event_handlers:\n    flow.started:\n      create_entity: true\n    flow.finish:\n      advances_to: Ready\n",
	} {
		writeRunCompletionFixtureFile(t, filepath.Join(root, path), contents)
	}
	repo := runCompletionRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load authored case-distinct bundle: %v", err)
	}
	return bundle
}

func stageCompletionRehydrate(t *testing.T, selected stageCompletionJourneyStore, db *sql.DB, backend, hash string) (stageCompletionJourneyStore, semanticview.Source) {
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
	if backend == "sqlite" {
		return storetest.AdmitSQLiteRuntimeStore(t, db), semanticview.Wrap(bundle)
	}
	return storetest.AdmitPostgresRuntimeStore(t, db), semanticview.Wrap(bundle)
}

func stageCompletionCatalog(t *testing.T, source semanticview.Source) runtimerunlifecycle.TerminalCatalog {
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
	catalog, err := runtimerunlifecycle.NewCompiledTerminalCatalog(classifier)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func stageCompletionWaitForDelivery(t *testing.T, db *sql.DB, backend, runID, eventName string) {
	t.Helper()
	nodeID := identitytest.FlowNode(t, "discovery", "pipeline").Key()
	query := `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id = d.event_id
		WHERE e.run_id = ? AND e.event_name = ? AND d.subscriber_type = 'node' AND d.subscriber_id = ? AND d.status = 'delivered'`
	if backend == "postgres" {
		query = `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id = d.event_id
			WHERE e.run_id = $1::uuid AND e.event_name = $2 AND d.subscriber_type = 'node' AND d.subscriber_id = $3 AND d.status = 'delivered'`
	}
	requireAPIV1Convergence(t, "case-distinct handler delivery "+eventName, func() (bool, error) {
		var count int
		if err := db.QueryRow(query, runID, eventName, nodeID).Scan(&count); err != nil {
			return false, err
		}
		return count == 1, fmt.Errorf("delivered %s count=%d, want one", eventName, count)
	})
}

func stageCompletionAssertStoredState(t *testing.T, db *sql.DB, backend, runID, want string) {
	t.Helper()
	query := "SELECT current_state FROM entity_state WHERE run_id = ? AND entity_id = ? AND flow_instance = 'discovery'"
	if backend == "postgres" {
		query = "SELECT current_state FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid AND flow_instance = 'discovery'"
	}
	var state string
	if err := db.QueryRow(query, runID, runtimepipeline.FlowInstanceEntityID("discovery")).Scan(&state); err != nil || state != want {
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
