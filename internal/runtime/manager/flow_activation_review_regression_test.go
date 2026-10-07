package manager

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type activationFinalizerEvidenceStore struct {
	*flowActivationTestInstanceStore
	finalizations int
}

func TestFlowReadinessVerifiesAgentEntityOwnership(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	req := testActivationRequest(testFlowBundle(t, ""), "review", "inst-1", "ent-1", "review/inst-1")
	setFlowActivationManagerSemanticSource(am, req.ContractBundle)
	plan, err := am.PrepareFlowInstanceActivation(testAuthorActivityContext(context.Background()), req)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := semanticview.FlowScopeByID(req.ContractBundle, "review")
	schema, _ := req.ContractBundle.FlowSchemaByID("review")
	records, err := am.flowInstanceAgentRecords(plan.Readiness.RunID, req, schema, scope)
	if err != nil || len(records) == 0 {
		t.Fatalf("exact declared agents: %+v %v", records, err)
	}
	if err := verifyDynamicFlowAgentExpectations(records, plan.Readiness.Agents); err != nil {
		t.Fatal(err)
	}
	for _, entity := range []string{"", "foreign-entity"} {
		changed := append([]runtimepipeline.DynamicFlowRuntimeAgentExpectation(nil), plan.Readiness.Agents...)
		changed[0].EntityID = entity
		if err := verifyDynamicFlowAgentExpectations(records, changed); err == nil || !strings.Contains(err.Error(), "entity ownership") {
			t.Fatalf("changed owner with unchanged config revision was admitted: %v", err)
		}
	}
}

func (s *activationFinalizerEvidenceStore) FinalizeInitialEntryLifecycle(context.Context, runtimepipeline.CommittedWorkflowLifecycleMutation) error {
	s.finalizations++
	return nil
}

func TestFlowActivationFinalizerRefusesUnacknowledgedEvidence(t *testing.T) {
	instances := &activationFinalizerEvidenceStore{flowActivationTestInstanceStore: &flowActivationTestInstanceStore{}}
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, instances)
	req := testActivationRequest(testFlowBundle(t, ""), "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())
	setFlowActivationManagerSemanticSource(am, req.ContractBundle)
	plan, err := am.PrepareFlowInstanceActivation(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	evidence := runtimepipeline.CommittedFlowInstanceActivation{Plan: plan, Created: true, ReadinessAttemptOrdinal: 1}
	if err := evidence.Validate(); err != nil {
		t.Fatalf("test did not supply otherwise complete command data: %v", err)
	}
	if err := am.FinalizeCommittedFlowInstanceActivation(ctx, evidence); err == nil || !strings.Contains(err.Error(), "not acknowledged") {
		t.Fatalf("unacknowledged evidence reached finalization: %v", err)
	}
	if instances.finalizations != 0 || instances.readinessLoads != 0 || len(bus.addedPaths) != 0 {
		t.Fatalf("unacknowledged evidence acquired lifecycle/topology: finalizations=%d loads=%d routes=%v", instances.finalizations, instances.readinessLoads, bus.addedPaths)
	}
}

func TestFlowReadinessPassUsesAtMostTwoPublicLoads(t *testing.T) {
	for _, path := range []string{"retry", "ensure", "committed_callback"} {
		t.Run(path, func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{}
			bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
			am := newFlowActivationManager(t, bus, instances)
			bundle := testFlowBundle(t, "")
			setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
			ctx := testAuthorActivityContext(context.Background())
			if err := activateFlowInstanceForTest(am, ctx, req); err != nil {
				t.Fatal(err)
			}
			row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
			if err != nil || !found {
				t.Fatalf("read active attachment: found=%v err=%v", found, err)
			}
			instances.readinessMu.Lock()
			before := instances.readinessLoads
			instances.readinessMu.Unlock()
			switch path {
			case "retry":
				err = am.reconcileDynamicFlowRuntimeReadiness(ctx, row.Plan.RunID, row.InstancePath)
			case "ensure":
				_, err = am.EnsureFlowInstance(ctx, req)
			case "committed_callback":
				err = am.reconcileCommittedDynamicFlowRuntimeReadinessPlan(ctx, row.Plan, row.AttemptOrdinal, am.semanticReadinessSource.source)
			}
			if err != nil {
				t.Fatal(err)
			}
			instances.readinessMu.Lock()
			loads := instances.readinessLoads - before
			instances.readinessMu.Unlock()
			if loads != 2 {
				t.Fatalf("%s used %d public readiness loads, want two bounded observations", path, loads)
			}
			if !bus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) {
				t.Fatal("bounded pass did not retain the exact route")
			}
		})
	}
}

func TestStartupFinalizationConsumesRetainedPreparationAttempt(t *testing.T) {
	for _, predecessor := range []string{"planned", "retired", "aborted"} {
		t.Run(predecessor, func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{}
			agents := &flowActivationTestStore{}
			first := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
			bundle := testFlowBundle(t, "")
			setFlowActivationManagerSemanticSource(first, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
			ctx := testAuthorActivityContext(context.Background())
			switch predecessor {
			case "planned":
				plan, err := first.PrepareFlowInstanceActivation(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				committed, err := first.roles.FlowActivation.CommitFlowInstanceActivation(ctx, plan)
				if err != nil || !committed.Acknowledged || !committed.Created {
					t.Fatalf("commit planned construction: acknowledged=%v created=%v err=%v", committed.Acknowledged, committed.Created, err)
				}
			case "retired":
				if err := activateFlowInstanceForTest(first, ctx, req); err != nil {
					t.Fatal(err)
				}
			case "aborted":
				agents.failAgentID = "reviewer"
				if err := activateFlowInstanceForTest(first, ctx, req); err == nil {
					t.Fatal("injected agent failure was not reported")
				}
				agents.failAgentID = ""
			}
			if err := first.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if predecessor == "retired" {
				// This stopped Manager fixture has no execution loops to join.
				if err := first.retireDynamicFlowAttemptsAfterJoin(ctx); err != nil {
					t.Fatal(err)
				}
			}
			item, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
			if err != nil || !found || item.AttemptState != predecessor {
				t.Fatalf("preparation entry: found=%v state=%s err=%v", found, item.AttemptState, err)
			}
			bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
			restarted := newFlowActivationManager(t, bus, instances, agents)
			setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
			source, err := restarted.dynamicFlowRuntimeReadinessSource(ctx)
			if err != nil {
				t.Fatal(err)
			}
			before := instances.readinessLoads
			if err := restarted.reconcileDynamicFlowRuntimeReadinessItem(ctx, item, source, true, false); err != nil {
				t.Fatalf("prepare topology: %v", err)
			}
			if loads := instances.readinessLoads - before; loads != 1 {
				t.Fatalf("preparation used %d public loads, want one", loads)
			}
			key := dynamicFlowRuntimeReadinessKey{runID: item.Plan.RunID, instancePath: item.InstancePath}
			restarted.dynamicFlowReadinessMu.Lock()
			prepared := restarted.dynamicFlowActiveAttempts[key].receipt
			restarted.dynamicFlowReadinessMu.Unlock()
			wantOrdinal := item.AttemptOrdinal
			if predecessor != "planned" {
				wantOrdinal++
			}
			if prepared.Ordinal() != wantOrdinal {
				t.Fatalf("prepared ordinal=%d, want %d", prepared.Ordinal(), wantOrdinal)
			}
			before = instances.readinessLoads
			if err := restarted.reconcileDynamicFlowRuntimeReadinessItem(ctx, item, source, false, true); err != nil {
				t.Fatalf("finalize against original inventory: %v", err)
			}
			if loads := instances.readinessLoads - before; loads != 2 {
				t.Fatalf("finalization used %d public loads, want two", loads)
			}
			row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, item.Plan.RunID, req.Instance.Route())
			if err != nil || !found || row.Pending() || row.AttemptOrdinal != prepared.Ordinal() || row.AttemptState != "accepted" {
				t.Fatalf("finalized exact preparation: found=%v row=%#v err=%v", found, row, err)
			}
			if !bus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) || len(instances.creates) != 1 {
				t.Fatalf("handoff repeated construction or lost route: constructions=%d", len(instances.creates))
			}
		})
	}
}

func TestFlowActivationPostMarkFailureReturnsToPendingRetry(t *testing.T) {
	for _, outcome := range []string{"error", "cancellation", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{}
			bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
			am := newFlowActivationManager(t, bus, instances)
			bundle := testFlowBundle(t, "")
			setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
			ctx := testAuthorActivityContext(context.Background())
			activationCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			var canceledAttemptDone <-chan struct{}
			instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
				switch outcome {
				case "error":
					instances.readyAcknowledgementErr = errors.New("lost phase acknowledgment after durable completion")
				case "cancellation":
					key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
					am.dynamicFlowReadinessMu.Lock()
					canceledAttemptDone = am.dynamicFlowReadinessAttempts[key].done
					am.dynamicFlowReadinessMu.Unlock()
					cancel()
					instances.readyAcknowledgementErr = activationCtx.Err()
				case "panic":
					panic("injected panic after acknowledged topology completion")
				}
			}
			var activationErr error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				activationErr = activateFlowInstanceForTest(am, activationCtx, req)
			}()
			if outcome == "panic" {
				if recovered != nil || activationErr == nil || !strings.Contains(activationErr.Error(), "panic") {
					t.Fatalf("panic was not reported by the activation owner: err=%v panic=%v", activationErr, recovered)
				}
			} else if activationErr == nil || recovered != nil {
				t.Fatalf("expected injected %s failure: err=%v panic=%v", outcome, activationErr, recovered)
			}
			if outcome == "cancellation" {
				select {
				case <-canceledAttemptDone:
				case <-time.After(5 * time.Second):
					t.Fatal("canceled caller returned before the accepted activation settled")
				}
			}
			instances.readyAcknowledgementErr = nil
			if bus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) {
				t.Fatal("failed attempt retained a route")
			}
			if err := am.reconcilePendingDynamicFlowRuntimeReadiness(ctx); err != nil {
				t.Fatal(err)
			}
			if !bus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) {
				t.Fatal("retry omitted the abandoned topology")
			}
		})
	}
}

func TestFlowActivationShutdownPreservesFailedAttemptDisposition(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	instances.topologyMarkErr = errors.New("ready phase CAS failed before acknowledgement")
	calls := 0
	instances.retireAttempt = func() error {
		calls++
		if calls == 1 {
			return errors.New("failed abandonment retained for retry")
		}
		return nil
	}
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
		t.Fatal("missing injected error")
	}
	instances.topologyMarkErr = nil
	key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
	am.dynamicFlowReadinessMu.Lock()
	active := am.dynamicFlowActiveAttempts[key]
	failed := active != nil && active.retirementKind == flowActivationFailedRetirement && active.locallyRetired
	am.dynamicFlowReadinessMu.Unlock()
	if !failed {
		t.Fatal("control: expected retained failed attempt after local join")
	}
	if err := am.Shutdown(); err != nil {
		t.Fatal(err)
	}
	row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, req.Instance.Route())
	if err != nil || !found {
		t.Fatalf("load: found=%v err=%v", found, err)
	}
	if !row.Pending() {
		t.Fatal("shutdown changed failed abandonment into orderly retirement")
	}
}

func TestFlowActivationShutdownRetriesAcknowledgedFailedAbandonment(t *testing.T) {
	for _, autoEmit := range []string{"", "task.started"} {
		name := "no_creation"
		if autoEmit != "" {
			name = "creation_emitted"
		}
		t.Run(name, func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{settleAttemptPostCommitErr: errors.New("acknowledged abandonment response lost")}
			bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
			am := newFlowActivationManager(t, bus, instances)
			bundle := testFlowBundle(t, autoEmit)
			setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
			instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
				instances.readyAcknowledgementErr = errors.New("phase acknowledgment lost after durable progress")
			}
			ctx := testAuthorActivityContext(context.Background())
			if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
				t.Fatal("missing injected failure")
			}
			instances.readyAcknowledgementErr = nil
			key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
			am.dynamicFlowReadinessMu.Lock()
			active := am.dynamicFlowActiveAttempts[key]
			failed := active != nil && active.retirementKind == flowActivationFailedRetirement && active.locallyRetired
			am.dynamicFlowReadinessMu.Unlock()
			if !failed {
				t.Fatal("acknowledged abandonment lost retained failure disposition")
			}
			if err := am.Shutdown(); err != nil {
				t.Fatal(err)
			}
			row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, req.Instance.Route())
			if err != nil || !found || !row.Pending() {
				t.Fatalf("failed abandonment did not return readiness to pending: found=%v pending=%v err=%v", found, row.Pending(), err)
			}
		})
	}
}

func TestFlowActivationFailedAttemptShutdownRetainsPersistentSettlement(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
		instances.readyAcknowledgementErr = errors.New("phase acknowledgment lost after durable progress")
	}
	calls := 0
	instances.retireAttempt = func() error {
		calls++
		if calls <= 2 {
			return errors.New("durable abandonment unavailable")
		}
		return nil
	}
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
		t.Fatal("missing injected activation failure")
	}
	instances.readyAcknowledgementErr = nil
	key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
	if err := am.Shutdown(); err == nil {
		t.Fatal("shutdown released a persistently failed abandonment")
	}
	am.dynamicFlowReadinessMu.Lock()
	retained := am.dynamicFlowActiveAttempts[key]
	am.dynamicFlowReadinessMu.Unlock()
	if retained == nil || retained.retirementKind != flowActivationFailedRetirement {
		t.Fatal("failed shutdown lost the exact abandonment owner")
	}
	if err := am.Shutdown(); err != nil {
		t.Fatalf("retry joined shutdown settlement: %v", err)
	}
	row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, req.Instance.Route())
	if err != nil || !found || !row.Pending() {
		t.Fatalf("retry did not abandon failed attempt: found=%v pending=%v err=%v", found, row.Pending(), err)
	}
}

func TestFlowActivationFailedAttemptShutdownRetriesSettlementPanic(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
		instances.readyAcknowledgementErr = errors.New("phase acknowledgment lost after durable progress")
	}
	calls := 0
	instances.retireAttempt = func() error {
		calls++
		switch calls {
		case 1:
			return errors.New("durable abandonment unavailable")
		case 2:
			panic("durable abandonment panicked")
		default:
			return nil
		}
	}
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
		t.Fatal("missing injected activation failure")
	}
	instances.readyAcknowledgementErr = nil
	if err := am.Shutdown(); err == nil || !strings.Contains(err.Error(), "durable abandonment panicked") {
		t.Fatalf("shutdown did not report retained settlement panic: %v", err)
	}
	key := dynamicFlowRuntimeReadinessKey{runID: req.TriggerEvent.RunID(), instancePath: req.Instance.Route().InstancePath}
	am.dynamicFlowReadinessMu.Lock()
	retained := am.dynamicFlowActiveAttempts[key]
	am.dynamicFlowReadinessMu.Unlock()
	if retained == nil || retained.retirementKind != flowActivationFailedRetirement {
		t.Fatal("settlement panic lost failed disposition")
	}
	if err := am.Shutdown(); err != nil {
		t.Fatalf("retry after settlement panic: %v", err)
	}
	row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, req.Instance.Route())
	if err != nil || !found || !row.Pending() {
		t.Fatalf("retry did not abandon failed attempt: found=%v pending=%v err=%v", found, row.Pending(), err)
	}
}

func TestCompletedStandingPreRunHandoffRetainsReconstructionAuthority(t *testing.T) {
	for _, replay := range []bool{false, true} {
		name := "replay_off"
		if replay {
			name = "replay_on"
		}
		t.Run(name, func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{}
			agents := &flowActivationTestStore{}
			bundle := testFlowBundleWithTwoAgents(t, "")
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
			ctx := testAuthorActivityContext(context.Background())
			initial := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
			if err := activateFlowInstanceForTest(initial, ctx, req); err != nil {
				t.Fatal(err)
			}
			if err := initial.Shutdown(); err != nil {
				t.Fatal(err)
			}
			restarted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
			setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
			if _, _, err := restarted.PrepareStandingFlowInstance(ctx, req); err != nil {
				t.Fatal(err)
			}
			startup, err := restarted.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, authorActivityTestSourceArtifactFact, replay)
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.PrepareAdmittedDynamicFlowAgentsForStart(ctx); err != nil {
				t.Fatal(err)
			}
			runCtx, cancel := context.WithCancel(ctx)
			if err := restarted.Run(managedExecutionTestContext(t, runCtx)); err != nil {
				cancel()
				t.Fatal(err)
			}
			t.Cleanup(func() { cancel(); _ = restarted.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}) })
			if err := restarted.CompleteDynamicFlowRuntimeStartupTopology(ctx, startup); err != nil {
				t.Fatalf("completed standing reconstruction rejected: %v", err)
			}
		})
	}
}

func TestInterruptedStandingPreRunRequiresExplicitRecovery(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	bundle := testFlowBundleWithTwoAgents(t, "")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())
	initial := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	if err := activateFlowInstanceForTest(initial, ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := initial.Shutdown(); err != nil {
		t.Fatal(err)
	}
	interrupted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	setFlowActivationManagerSemanticSource(interrupted, semanticview.Wrap(bundle))
	if _, _, err := interrupted.PrepareStandingFlowInstance(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := interrupted.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, authorActivityTestSourceArtifactFact, false); err != nil {
		t.Fatal(err)
	}
	if err := interrupted.PrepareAdmittedDynamicFlowAgentsForStart(ctx); err != nil {
		t.Fatal(err)
	}
	if err := interrupted.Shutdown(); err != nil {
		t.Fatal(err)
	}
	restarted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
	if _, _, err := restarted.PrepareStandingFlowInstance(ctx, req); err != nil {
		t.Fatal(err)
	}
	before, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
	if err != nil || !found || !before.Pending() || before.AttemptOrdinal != 2 {
		t.Fatalf("interrupted successor did not retain incomplete evidence: readiness=%+v found=%v err=%v", before, found, err)
	}
	if _, err := restarted.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, authorActivityTestSourceArtifactFact, false); err == nil || !strings.Contains(err.Error(), "requires recovery for incomplete source-owned instance") {
		t.Fatalf("interrupted reconstruction bypassed recovery admission: %v", err)
	}
	after, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
	if err != nil || !found || !reflect.DeepEqual(before, after) {
		t.Fatalf("recovery refusal changed durable readiness: before=%+v after=%+v found=%v err=%v", before, after, found, err)
	}
	if _, err := restarted.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, authorActivityTestSourceArtifactFact, true); err != nil {
		t.Fatalf("explicit recovery did not admit interrupted successor: %v", err)
	}
}

func TestStartupTopologyRefusesUnfinalizedPostSnapshotConstruction(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	ctx := testAuthorActivityContext(context.Background())
	initial := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	if err := activateFlowInstanceForTest(am, ctx, initial); err != nil {
		t.Fatal(err)
	}
	startup, err := am.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, authorActivityTestSourceArtifactFact, true)
	if err != nil {
		t.Fatal(err)
	}
	unfinalized := testActivationRequest(bundle, "review", "inst-2", "ent-2", "review/inst-2")
	plan, err := am.PrepareFlowInstanceActivation(ctx, unfinalized)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := am.roles.FlowActivation.CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged {
		t.Fatalf("commit new construction: %+v err=%v", committed, err)
	}
	before, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, plan.Readiness.RunID, unfinalized.Instance.Route())
	if err != nil || !found || !before.Pending() {
		t.Fatalf("unfinalized construction: %+v found=%v err=%v", before, found, err)
	}
	if err := am.CompleteDynamicFlowRuntimeStartupTopology(ctx, startup); err == nil || !strings.Contains(err.Error(), "lacks pending authorization") {
		t.Fatalf("post-snapshot construction bypassed its exact finalizer: %v", err)
	}
	after, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, plan.Readiness.RunID, unfinalized.Instance.Route())
	if err != nil || !found || !reflect.DeepEqual(before, after) || bus.HasFlowInstanceRoute(testActivationFlowIdentity(unfinalized)) {
		t.Fatalf("refusal changed unfinalized topology: before=%+v after=%+v found=%v err=%v", before, after, found, err)
	}
	if err := am.Shutdown(); err != nil {
		t.Fatal(err)
	}
}
