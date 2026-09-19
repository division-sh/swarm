package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestIssue2394PublicationGroupDeferredAgentSIGKILLBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"after_commit_before_ack", "before_continuation_signal"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				location := filepath.Join(t.TempDir(), "agent-group-crash.db")
				if backend == "postgres" {
					location, _, _ = testutil.StartPostgres(t)
				}
				fixture := openFanOutCrashStore(t, backend, location)
				root := canonicalrouting.CopyFanOutGroupAgentCrash(t)
				source := loadPublicationGroupAgentCrashSource(t, root)
				ctx, seeded := seedPublicationGroupAgentIntent(t, backend, fixture, source)
				env := []string{"SWARM_FAN_OUT_CRASH_BACKEND=" + backend, "SWARM_FAN_OUT_CRASH_LOCATION=" + location,
					"SWARM_FAN_OUT_CRASH_ROOT=" + root, "SWARM_FAN_OUT_CRASH_RUN=" + seeded.runID, "SWARM_FAN_OUT_CRASH_CUT=" + cut,
					"SWARM_PUBLICATION_GROUP_CRASH_RECEIVER=agent"}
				cmd, evidence, exited := startPublicationGroupCrashChild(t, "predecessor", env)
				old := awaitFanOutCrashEvidence(t, evidence)
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-exited:
					exit, ok := err.(*exec.ExitError)
					if !ok {
						t.Fatalf("agent predecessor did not die: %v", err)
					}
					status, ok := exit.Sys().(syscall.WaitStatus)
					if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
						t.Fatalf("agent predecessor not SIGKILL: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("agent predecessor failed to exit")
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, fixture.db, seeded, 3, 3)
				ids := fanOutCrashOutcomeIDs(t, ctx, fixture.db, seeded.runID)
				assertPublicationGroupAgentEffects(t, ctx, fixture, seeded.runID, ids, false)
				var active, handed int
				if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN continuation_handoff_at IS NOT NULL THEN 1 ELSE 0 END),0) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status IN ('pending','in_progress')`, seeded.runID).Scan(&active, &handed); err != nil || active != 3 || handed != 3 {
					t.Fatalf("crash lost actual durable owed agent work: active=%d handed=%d err=%v", active, handed, err)
				}
				if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='in_progress' AND current_attempt_open=TRUE`, seeded.runID).Scan(&active); err != nil || active != 1 {
					t.Fatalf("crash lacks exactly one held actual durable agent claim: active=%d err=%v", active, err)
				}
				oldJSON, err := json.Marshal(old.Authority)
				if err != nil {
					t.Fatal(err)
				}
				env = append(env, "SWARM_FAN_OUT_CRASH_PREDECESSOR="+string(oldJSON))
				_, evidence, exited = startPublicationGroupCrashChild(t, "successor", env)
				next := awaitFanOutCrashEvidence(t, evidence)
				if !next.Recovered || next.Authority.PredecessorAuthorityID != old.Authority.AuthorityID {
					t.Fatalf("agent recovery lacks exact process succession: %+v", next)
				}
				select {
				case err := <-exited:
					if err != nil {
						t.Fatalf("agent successor failed: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("agent successor did not join")
				}
				if got := fanOutCrashOutcomeIDs(t, ctx, fixture.db, seeded.runID); !reflect.DeepEqual(ids, got) {
					t.Fatalf("agent recovery changed committed group identities: %v -> %v", ids, got)
				}
				assertPublicationGroupAgentEffects(t, ctx, fixture, seeded.runID, ids, true)
				t.Logf("B15 %s actual unfinished LLMAgent claim survives SIGKILL; successor startup restores exact durable continuations, real mock provider/tool emission yields three exact business results, no signal replay or node-only substitute", cut)
			})
		}
	}
}

func loadPublicationGroupAgentCrashSource(t *testing.T, root string) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func runPublicationGroupAgentCrashChild(t *testing.T, mode string) {
	backend, location := os.Getenv("SWARM_FAN_OUT_CRASH_BACKEND"), os.Getenv("SWARM_FAN_OUT_CRASH_LOCATION")
	fixture := openFanOutCrashStore(t, backend, location)
	source := loadPublicationGroupAgentCrashSource(t, os.Getenv("SWARM_FAN_OUT_CRASH_ROOT"))
	bundle, _ := semanticview.Bundle(source)
	fact := mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	runID := os.Getenv("SWARM_FAN_OUT_CRASH_RUN")
	ctx, cancel := context.WithTimeout(correlation.WithRunID(correlation.WithSourceArtifactFact(testAuthorActivityContextForBundle(fact.BundleHash()), fact), runID), 40*time.Second)
	defer cancel()
	request := testStartupAcquireRequest("publication-group-agent-" + mode)
	process, err := fixture.store.(startupownership.Store).AcquireProcessCapability(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	processEvidence, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	if mode == "successor" {
		var old startupownership.Authority
		if err := json.Unmarshal([]byte(os.Getenv("SWARM_FAN_OUT_CRASH_PREDECESSOR")), &old); err != nil {
			t.Fatal(err)
		}
		if processEvidence.AuthorityID == old.AuthorityID || processEvidence.PredecessorAuthorityID != old.AuthorityID || processEvidence.AuthorityGeneration != old.AuthorityGeneration+1 || processEvidence.AcquisitionKind != startupownership.AcquisitionCrashTakeover {
			t.Fatalf("not exact agent crash takeover: old=%+v next=%+v", old, processEvidence)
		}
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(request.RuntimeInstanceID, fact.BundleHash()))
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := authoractivity.ScopeFromContext(ctx)
	catalog, err := fixture.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Release)
	workProcess := worklifetime.NewProcess()
	work, err := workProcess.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: request.RuntimeInstanceID, BundleHash: fact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	ctx = worklifetime.WithRuntimeOccurrence(ctx, work)
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := work.RetireAndWait(deadline); err != nil {
			t.Error(err)
		}
		workProcess.Retire()
		if _, err := workProcess.Join(deadline); err != nil {
			t.Error(err)
		}
	})
	selected := fixture.store.(storeTestDurableEventBusStore)
	deliveryAuthority, err := deliverylifecycle.NewNormalExecutionAuthority(fact, request.RuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.ActivateDeliveryAuthority(ctx, deliveryAuthority); err != nil {
		t.Fatal(err)
	}
	eventBus, err := newStoreTestEventBus(t, selected, bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, WorkOwner: work, RuntimeInstanceID: request.RuntimeInstanceID, DeliveryAuthority: deliveryAuthority, ExecutionPosture: executionposture.MockOnly})
	if err != nil {
		t.Fatal(err)
	}
	gate := &publicationGroupAgentGate{hold: mode == "predecessor", started: make(chan struct{})}
	factory := publicationGroupRealAgentFactory(t, fixture.store, eventBus, source, gate)
	am := manager.NewAgentManagerWithOptions(eventBus, factory, manager.AgentManagerOptions{
		SourceArtifactFact: fact, SemanticSource: source, DeliveryStore: selected, ExecutionPosture: executionposture.MockOnly, LLMBackend: llmselection.BackendAnthropic,
		PersistenceRoles: manager.PersistenceRoles{AgentRoutes: eventBus, RouteInstaller: eventBus, RouteVerifier: eventBus, RouteRestorer: eventBus, RouteRetirer: eventBus, RouteRemover: eventBus, CreationPublisher: eventBus, DeliveryRuntime: eventBus, LifecycleState: fixture.store.(manager.AgentLifecycleStateReader)},
		WorkOwner:        work, ReceiverExecution: eventreceiver.NormalExecution(),
	}, fixture.store.(manager.ManagerPersistence))
	t.Cleanup(func() {
		if err := am.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	eventBus.SetCommittedAgentReadinessFinalizer(bus.CommittedAgentReadinessFinalizerFunc(am.FinalizeCommittedAgentReadiness))
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	desired, err := am.CompileStaticTopologyDesiredAgents(source, coordinate)
	if err != nil || len(desired) != 1 {
		t.Fatalf("real static agent topology=%+v err=%v", desired, err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatal(err)
	}
	current, exists, err := process.CurrentSourceSet(ctx)
	if err != nil || exists != (mode == "successor") || (exists && current.Revision != plan.Revision) {
		t.Fatalf("agent source-set continuity: exists=%v current=%+v err=%v", exists, current, err)
	}
	if !exists {
		if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
			t.Fatal(err)
		}
	}
	live, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fact.BundleHash(), RuntimeInstanceID: request.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := agenttopology.StaticAdmission(plan.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := am.InstallStartupTopology(live, admission, plan); err != nil {
		t.Fatal(err)
	}
	if err := am.ReconcileStaticTopologyForStartup(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := live.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := live.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	managed, err := managedexecution.New(managedexecution.KindNormalRuntime, request.RuntimeInstanceID, 1, "", "publication-group-agent", fact.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx = managedexecution.WithAdmission(ctx, managed)
	if _, err := am.HydrateForStartup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := am.Run(ctx); err != nil {
		t.Fatal(err)
	}
	workflow := fixture.store.(workflowTestSelectedStore)
	nodes, err := pipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
		Module: forkFanOutConsumerModule{runForkGateWorkflowModule{source: source}, nodes}, Persistence: pipeline.NewWorkflowPersistence(workflow), DeliveryStore: workflow,
		DeadLetters: workflow, PipelineObligations: workflow.PipelineObligations(), DecisionCards: workflow, ProposedEffects: workflow, HumanTasks: workflow,
		DecisionCardDraftExpiry: workflow, HumanTaskExpiry: workflow, DeliveryRuntime: eventBus, RunLifecycle: workflow,
		SourceArtifactFact: fact, ExecutionPosture: executionposture.MockOnly, ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work,
	})
	if coordinator == nil {
		t.Fatal("real agent crash pipeline dependencies incomplete")
	}
	eventBus.SetInterceptors(coordinator)
	continuations, err := deliverycontinuation.New(selected, selected, deliveryAuthority, work, eventBus, func(_ context.Context, err error) { t.Errorf("agent continuation recovery: %v", err) })
	if err != nil {
		t.Fatal(err)
	}
	signal := &publicationGroupSignalCrash{DeliveryContinuationOwner: continuations}
	if err := eventBus.SetDeliveryContinuationOwner(signal); err != nil {
		t.Fatal(err)
	}
	if err := continuations.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := continuations.Retire(deadline); err != nil {
			t.Error(err)
		}
	})
	report := os.NewFile(3, "publication-group-agent-crash-evidence")
	defer report.Close()
	sink := &pipelineGracefulSink{}
	sinkRegistration, err := fixture.store.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: fact.BundleHash()}, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sinkRegistration.Release)
	control := &pipelineCrashConnector{cut: os.Getenv("SWARM_FAN_OUT_CRASH_CUT")}
	var beforeSubmit int32
	barrier := func() {
		if gate.entries.Load() != 1 {
			panic("agent group cut lacks exactly one held actual agent invocation")
		}
		if control.cut == "after_commit_before_ack" && sink.submits.Load() != beforeSubmit {
			panic("agent group candidate submission preceded acknowledgement")
		}
		if err := json.NewEncoder(report).Encode(fanOutCrashEvidence{Authority: processEvidence}); err != nil {
			panic(err)
		}
		select {}
	}
	control.barrier, signal.barrier = barrier, barrier
	serving := &publicationGroupAgentCrashExecutor{publicationGroupCrashExecutor: &publicationGroupCrashExecutor{PipelineCoordinator: coordinator, errors: make(chan error, 8)}}
	if mode == "predecessor" {
		serving.fault = publicationGroupAgentCrashGroup{control: control, gate: gate, signal: signal, cut: control.cut, before: func() { beforeSubmit = sink.submits.Load() }}
	}
	workers := 1
	registration, err := startupownership.StartFanOutServing(ctx, live, work, &workers, serving)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	if mode == "predecessor" {
		select {
		case err := <-serving.errors:
			t.Fatalf("actual agent group serving before crash: %v", err)
		case <-ctx.Done():
			t.Fatal("real agent group did not reach crash window")
		}
		return
	}
	if err := pipeline.NewRecoveryManagerWith(eventBus).RecoverToExhaustion(ctx); err != nil {
		t.Fatal(err)
	}
	ids := fanOutCrashOutcomeIDs(t, ctx, fixture.db, runID)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var delivered int
		if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='delivered'`, runID).Scan(&delivered); err != nil {
			t.Fatal(err)
		}
		if delivered == 3 {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("successor did not drain actual agent continuations: delivered=%d", delivered)
		}
	}
	if err := eventBus.WaitForQuiescence(ctx); err != nil {
		t.Fatal(err)
	}
	assertPublicationGroupAgentEffects(t, ctx, fixture, runID, ids, true)
	if gate.entries.Load() != 3 {
		t.Fatalf("successor real agent invocations=%d want3", gate.entries.Load())
	}
	before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
	if err := continuations.Synchronize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.NewRecoveryManagerWith(eventBus).RecoverToExhaustion(ctx); err != nil {
		t.Fatal(err)
	}
	if err := eventBus.WaitForQuiescence(ctx); err != nil {
		t.Fatal(err)
	}
	if gate.entries.Load() != 3 || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
		t.Fatal("duplicate recovery reexecuted an agent or mutated committed business history")
	}
	sinkRegistration.Release()
	observed := &pipelineCrashCandidateObserver{CandidateStore: fixture.store.(runlifecycle.CandidateStore), results: make(chan pipelineCrashCandidateResult, 8)}
	lifecycle, err := runlifecycle.NewExecutor(observed, runlifecycle.CandidateScope{BundleHash: fact.BundleHash()}, runlifecycle.NewTerminalCatalog(nil, map[string][]string{".": {"done", "exhausted"}}), work, runlifecycle.ExecutorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := lifecycle.Retire(deadline); err != nil {
			t.Error(err)
		}
	})
	if err := lifecycle.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-observed.results:
		if result.err != nil || result.candidate.RunID != runID || result.outcome.Outcome != runlifecycle.OutcomeAwaitMutation {
			t.Fatalf("actual agent-run startup candidate recovery: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not discover durable agent-run candidate")
	}
	if err := lifecycle.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if observed.executions.Load() != 1 {
		t.Fatalf("agent-run candidate executed %d times", observed.executions.Load())
	}
	if err := json.NewEncoder(report).Encode(fanOutCrashEvidence{Authority: processEvidence, Recovered: true}); err != nil {
		t.Fatal(err)
	}
}

func assertPublicationGroupAgentEffects(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, runID string, ids []string, recovered bool) {
	t.Helper()
	if len(ids) != 3 {
		t.Fatalf("agent group expected three exact outcomes, got %v", ids)
	}
	reader := fixture.store.(interface {
		LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
	})
	name, err := agentidentity.DeclaredName("item-worker", "swarm://item-worker")
	if err != nil {
		t.Fatal(err)
	}
	wantIdentity, err := agentidentity.New(runID, name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	for ordinal, id := range ids {
		view, err := reader.LoadOperatorEvent(ctx, id)
		if err != nil || view.RunID != runID || view.ExecutionMode != executionmode.Mock || view.Payload["value"] != fmt.Sprintf("item-%03d", ordinal) || len(view.Deliveries) != 1 || view.NoDelivery != nil || len(view.DeadLetters) != 0 {
			t.Fatalf("actual agent publication: %+v err=%v", view, err)
		}
		delivery := view.Deliveries[0]
		if delivery.SubscriberType != "agent" || delivery.SubscriberID != "item-worker" || !delivery.Route.Recipient.IsAgent() || delivery.Route.AgentIdentity != wantIdentity || delivery.Target.Kind != "" || delivery.Target.EntityID != "" || delivery.Failure != nil {
			t.Fatalf("missing real exact managed agent delivery: %+v", delivery)
		}
		if recovered && (delivery.Status != "delivered" || !delivery.Terminal) {
			t.Fatalf("recovered agent not delivered: %+v", delivery)
		}
		if !recovered && (delivery.Terminal || (delivery.Status != "pending" && delivery.Status != "in_progress")) {
			t.Fatalf("held agent work ceased to be owed at SIGKILL: %+v", delivery)
		}
		var count, success int
		if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN outcome='success' THEN 1 ELSE 0 END),0) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, id).Scan(&count, &success); err != nil || count != 1 || success != 1 {
			t.Fatalf("group acknowledgement missing at agent cut/recovery: %d/%d err=%v", count, success, err)
		}
	}
	rows, err := fixture.db.QueryContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE run_id=$1 AND event_name='items.processed' ORDER BY created_at,event_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	var results []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		results = append(results, id)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := 0
	if recovered {
		want = 3
	}
	if len(results) != want {
		t.Fatalf("real agent tool emissions=%v want%d", results, want)
	}
	seen := [3]bool{}
	for _, id := range results {
		view, err := reader.LoadOperatorEvent(ctx, id)
		if err != nil || view.ProducerType != events.EventProducerAgent || view.ExecutionMode != executionmode.Mock || len(view.Deliveries) != 1 || view.Deliveries[0].SubscriberID != mustPersistenceRootNode("result-consumer").Key() || view.Deliveries[0].Status != "delivered" || !view.Deliveries[0].Terminal || view.NoDelivery != nil || len(view.DeadLetters) != 0 {
			t.Fatalf("real emitted business result: %+v err=%v", view, err)
		}
		matched := false
		for ordinal, request := range ids {
			if view.Payload["request_event_id"] == request {
				if seen[ordinal] || view.Payload["value"] != fmt.Sprintf("item-%03d", ordinal) || view.SourceEventID != request {
					t.Fatalf("duplicate/wrong agent result: %+v", view)
				}
				seen[ordinal], matched = true, true
			}
		}
		if !matched {
			t.Fatalf("agent result lost exact input membership: %+v", view)
		}
		var count int
		if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2 AND domain='authored_field' AND path='processed_value'`, runID, id).Scan(&count); err != nil || count != 1 {
			t.Fatalf("agent result business mutation=%d err=%v", count, err)
		}
		var raw string
		if err := fixture.db.QueryRowContext(ctx, `SELECT CAST(new_value AS TEXT) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2 AND domain='authored_field' AND path='processed_value'`, runID, id).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var value string
		if err := json.Unmarshal([]byte(raw), &value); err != nil || value != view.Payload["value"] {
			t.Fatalf("agent effect payload disagreement: value=%s err=%v", raw, err)
		}
	}
}
