package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type fanInBarrierConformanceStore interface {
	conformanceDurableEventBusStore
	runtimebus.CommitPublicationOwner
	runtimepipeline.WorkflowPersistenceOwner
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
	LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
}

type fanInBarrierGenericScheduleWakeups struct{}

func (fanInBarrierGenericScheduleWakeups) ReconcileWakeupWithRecovery(context.Context, string) (bool, error) {
	return false, nil
}

type fanInBarrierRuntime struct {
	bus         *runtimebus.EventBus
	diagnostics *fanInBarrierDiagnosticBus
	pipeline    *runtimepipeline.PipelineCoordinator
}

type fanInBarrierDiagnosticBus struct {
	*runtimebus.EventBus
	mu      sync.Mutex
	entries []runtimepipeline.RuntimeLogEntry
}

func (b *fanInBarrierDiagnosticBus) LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.mu.Lock()
	b.entries = append(b.entries, entry)
	b.mu.Unlock()
	return b.EventBus.LogRuntime(ctx, entry)
}

func (b *fanInBarrierDiagnosticBus) snapshot() []runtimepipeline.RuntimeLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]runtimepipeline.RuntimeLogEntry(nil), b.entries...)
}

func TestFanInBarrierCanonicalRuntimeCompletesAfterRestartOnBothBackends(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.FanInBarrier)
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical fan-in barrier: %v", err)
	}
	source := semanticview.Wrap(bundle)

	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (fanInBarrierConformanceStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
				backend := storetest.StartSQLiteRuntimeStore(t)
				return backend, storetest.DatabaseForTest(backend)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, db := tc.setup(t)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			seedFanInBarrierRun(t, ctx, backend, db, runID)
			runtime := newFanInBarrierRuntime(t, backend, db, source)
			seedFanInBarrierPortfolioShell(t, ctx, runtime.pipeline, bundle, runtimeflowidentity.EntityID("portfolio"))
			const periodID = "2026-Q3"
			memberA := uuid.NewString()
			memberB := uuid.NewString()

			publishFanInBarrierEvent(t, ctx, runtime.bus, source, uuid.NewString(), "portfolio", "portfolio.setup", map[string]any{
				"portfolio_id":           "portfolio",
				"expected_operating_ids": []string{memberA, memberB},
				"period_id":              periodID,
			})
			portfolio := loadFanInBarrierPortfolio(t, ctx, runtime.pipeline)
			if portfolio.CurrentState != "awaiting" {
				dumpFanInBarrierEvents(t, ctx, backend, db)
				t.Logf("fan-in runtime diagnostics: %#v", runtime.diagnostics.snapshot())
				t.Fatalf("portfolio state after setup = %q, want awaiting", portfolio.CurrentState)
			}
			activation := loadFanInBarrierActivation(t, portfolio, periodID)
			if activation.Status != joinruntime.StatusOpen || activation.Completed() != 0 || activation.Expected() != 2 {
				t.Fatalf("activation after setup = %#v, want open 0/2", activation)
			}

			publishFanInBarrierEvent(t, ctx, runtime.bus, source, memberB, "ingress", "operating.report.requested", map[string]any{
				"period_id": periodID,
				"revenue":   22,
			})
			portfolio = loadFanInBarrierPortfolio(t, ctx, runtime.pipeline)
			activation = loadFanInBarrierActivation(t, portfolio, periodID)
			if portfolio.CurrentState != "awaiting" || activation.Status != joinruntime.StatusOpen || activation.Completed() != 1 {
				dumpFanInBarrierEvents(t, ctx, backend, db)
				t.Logf("fan-in runtime diagnostics: %#v", runtime.diagnostics.snapshot())
				t.Fatalf("partial barrier = state:%s activation:%#v, want awaiting open 1/2", portfolio.CurrentState, activation)
			}
			// Reconstruct both EventBus and PipelineCoordinator. The second arrival
			// must consume the persisted activation rather than in-memory state.
			runtime = newFanInBarrierRuntime(t, backend, db, source)
			publishFanInBarrierEvent(t, ctx, runtime.bus, source, memberA, "ingress", "operating.report.requested", map[string]any{
				"period_id": periodID,
				"revenue":   11,
			})
			portfolio = loadFanInBarrierPortfolio(t, ctx, runtime.pipeline)
			activation = loadFanInBarrierActivation(t, portfolio, periodID)
			requireFanInBarrierReportTargets(t, ctx, backend, db, 2, events.RouteIdentity{
				FlowID: "portfolio", FlowInstance: "portfolio", EntityID: runtimeflowidentity.EntityID("portfolio"),
			})
			if portfolio.CurrentState != "complete" || activation.Status != joinruntime.StatusClosed || activation.CloseReason != joinruntime.CloseReasonComplete {
				t.Fatalf("completed barrier = state:%s activation:%#v", portfolio.CurrentState, activation)
			}
			results := activation.Results()
			if len(results) != 2 || results[0] != float64(11) || results[1] != float64(22) {
				t.Fatalf("barrier results = %#v, want declared membership order [11 22]", results)
			}
		})
	}
}

func TestRootToSingletonFirstDeliveryMaterializesReceiverEntityOnBothBackends(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.CopyRootOutputSingletonConnect(t),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load root-to-singleton conformance fixture: %v", err)
	}
	source := semanticview.Wrap(bundle)
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (fanInBarrierConformanceStore, *sql.DB)
	}{
		{
			name: "sqlite",
			setup: func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
				backend := storetest.StartSQLiteRuntimeStore(t)
				return backend, storetest.DatabaseForTest(backend)
			},
		},
		{
			name: "postgres",
			setup: func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, db := tc.setup(t)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			seedFanInBarrierRun(t, ctx, backend, db, runID)
			runtime := newFanInBarrierRuntime(t, backend, db, source)
			sourceEntityID := eventtest.UUID("root-to-singleton-source-" + tc.name)
			sourceRoute := events.RouteIdentity{
				FlowID: source.WorkflowName(), FlowInstance: runID, EntityID: sourceEntityID,
			}.Normalized()
			ctx = runtimedelivery.WithRoute(ctx, events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(conformanceNode(t, "", "root-producer")),
				Target:    events.MustExistingEntityTarget(sourceRoute),
			})
			routingSource, err := events.NewRootRoutingSource(sourceEntityID)
			if err != nil {
				t.Fatalf("construct root source: %v", err)
			}
			eventID := uuid.NewString()
			parentEventID := uuid.NewString()
			parent := eventtest.ExistingRunRootIngress(
				parentEventID, events.EventType("custom.root"), "root-producer", "", json.RawMessage(`{"proof":"causal-parent"}`), 0,
				runID, events.EnvelopeForEntityID(events.EventEnvelope{}, sourceEntityID), time.Now().UTC().Add(-time.Second),
			)
			storetest.CommitSemanticEventWithRoutes(t, ctx, backend, parent, nil, runtimepipelineobligation.ScopeDirect)
			event := eventtest.ChildForProducerWithRoutingSource(
				eventID, events.EventType("root.ready"), eventtest.Producer(events.EventProducerNode, "root-producer"), "",
				json.RawMessage(`{"entity_id":"consumer-one"}`), 0,
				events.EventLineage{RunID: runID, ParentEventID: parentEventID, ExecutionMode: executionmode.Live},
				events.EventEnvelope{}, routingSource, time.Now().UTC(),
			)
			wantRoute := events.RouteIdentity{
				FlowID: "consumer", FlowInstance: "consumer", EntityID: runtimeflowidentity.EntityID("consumer"),
			}.Normalized()
			wantOwner := events.MustMaterializingEntityTarget(wantRoute)
			if sourceEntityID == wantRoute.EntityID {
				t.Fatal("root source and singleton receiver identities must remain distinguishable")
			}
			if instance, found, err := runtime.pipeline.Load(ctx, runtimeflowidentity.RouteForInstancePath("consumer")); err != nil || found {
				t.Fatalf("singleton receiver must not exist before first delivery: found:%t instance:%#v err:%v", found, instance, err)
			}
			plan, err := runtime.bus.CheckPublishRecipientPlan(ctx, event)
			if err != nil {
				t.Fatalf("preflight root-to-singleton conformance delivery: %v", err)
			}
			if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target != wantOwner || plan.DeliveryRoutes[0].ConnectClaim.Empty() {
				t.Fatalf("root-to-singleton plan = failure:%q routes:%#v, want exact materializing receiver", plan.TargetFailure, plan.DeliveryRoutes)
			}
			if err := runtime.bus.PublishAcknowledged(ctx, event); err != nil {
				t.Fatalf("publish root-to-singleton conformance delivery: %v", err)
			}
			waitCtx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 10*time.Second)
			defer cancel()
			if err := runtime.bus.WaitForQuiescence(waitCtx); err != nil {
				t.Fatalf("wait for root-to-singleton execution: %v", err)
			}
			routes, err := backend.ListEventDeliveryRoutes(ctx, eventID)
			if err != nil {
				t.Fatalf("load root-to-singleton routes: %v", err)
			}
			if len(routes) != 1 || routes[0].Target != wantOwner || !routes[0].ConnectClaim.Equal(plan.DeliveryRoutes[0].ConnectClaim) {
				t.Fatalf("persisted root-to-singleton routes = %#v, want exact preflight owner and claim", routes)
			}
			instance, found, err := runtime.pipeline.Load(ctx, runtimeflowidentity.RouteForInstancePath("consumer"))
			if err != nil {
				t.Fatalf("load materialized singleton receiver: %v", err)
			}
			if !found || strings.TrimSpace(instance.EntityID) != wantRoute.EntityID {
				t.Fatalf("materialized singleton receiver = found:%t instance:%#v, want entity %s", found, instance, wantRoute.EntityID)
			}
			view, err := backend.LoadOperatorEvent(ctx, eventID)
			if err != nil {
				t.Fatalf("load public root-to-singleton event: %v", err)
			}
			if view.NoDelivery != nil || len(view.DeadLetters) != 0 || len(view.Deliveries) != 1 ||
				view.Deliveries[0].Target.Kind != wantOwner.Code() || view.Deliveries[0].Target.EntityID != wantRoute.EntityID {
				t.Fatalf("public root-to-singleton projection = %#v, want one successful materializing receiver", view)
			}
		})
	}
}

func TestFanInSingletonRoutePersistsExactSelectedOwnerOnBothBackends(t *testing.T) {
	testFanInSingletonRoutePersistsExactSelectedOwnerOnBothBackends(t)
}

func TestEventRouteSettlementRestartPreservesExactTargetOwner(t *testing.T) {
	testFanInSingletonRoutePersistsExactSelectedOwnerOnBothBackends(t)
}

func TestNestedCrossFlowTargetOwnershipRestartAndDuplicateOnBothBackends(t *testing.T) {
	testFanInSingletonRoutePersistsExactSelectedOwnerOnBothBackends(t)
}

func testFanInSingletonRoutePersistsExactSelectedOwnerOnBothBackends(t *testing.T) {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical fan-in barrier: %v", err)
	}
	source := semanticview.Wrap(bundle)

	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (fanInBarrierConformanceStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
				backend := storetest.StartSQLiteRuntimeStore(t)
				return backend, storetest.DatabaseForTest(backend)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, db := tc.setup(t)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			seedFanInBarrierRun(t, ctx, backend, db, runID)
			selectedOwner := eventtest.UUID("fan-in-selected-owner-" + tc.name)
			selectedTarget := events.RouteIdentity{
				FlowID: "portfolio", FlowInstance: "portfolio", EntityID: selectedOwner,
			}.Normalized()
			seedRuntime := newFanInBarrierRuntime(t, backend, db, source)
			seedFanInBarrierPortfolioShell(t, ctx, seedRuntime.pipeline, bundle, selectedOwner)
			requireSelectedRunTargetOwner(t, ctx, backend, "portfolio", selectedOwner)

			proofBus := newFanInBarrierRouteProofBus(t, backend, source)
			eventID := uuid.NewString()
			sourceRoute := events.RouteIdentity{
				FlowID: "operating", FlowInstance: "operating/proof-instance",
				EntityID: eventtest.UUID("fan-in-source-owner-" + tc.name),
			}.Normalized()
			eventType := events.EventType(sourceRoute.FlowInstance + "/operating.reported")
			sink := newFanInBarrierRouteProofSink(t, ctx, proofBus, eventType)
			routingSource, err := events.NewConcreteTemplateInstanceRoutingSource(sourceRoute)
			if err != nil {
				t.Fatalf("create proof routing source: %v", err)
			}
			payload, err := json.Marshal(map[string]any{
				"operating_id": "proof-instance", "period_id": "2026-Q3", "revenue": 42,
			})
			if err != nil {
				t.Fatalf("marshal proof payload: %v", err)
			}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID, eventType, "operating-proof", "", payload, 0, runID,
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute), routingSource, time.Now().UTC(),
			)
			if sourceRoute.EntityID == selectedOwner || runtimeflowidentity.EntityID("portfolio") == selectedOwner {
				t.Fatal("proof identities must make source, path-derived, and selected owners distinguishable")
			}

			if inserted := commitFanInBarrierEnginePublication(t, ctx, backend, proofBus, event); !inserted {
				t.Fatal("initial fan-in proof publication was not inserted")
			}
			initialSettlement := requireFanInBarrierRouteProof(t, ctx, backend, db, eventID, selectedTarget)
			initialPublic := requireFanInBarrierPublicRouteProof(t, ctx, backend, eventID, selectedTarget)
			sink.requireDelivery(t, eventID, selectedTarget)
			if err := sink.close(true); err != nil {
				t.Fatalf("retire first proof sink: %v", err)
			}

			removeFanInBarrierSelectedOwner(t, ctx, backend, db, runID, selectedOwner)
			restartedBus := newFanInBarrierRouteProofBus(t, backend, source)
			restartedSink := newFanInBarrierRouteProofSink(t, ctx, restartedBus, eventType)
			if inserted := commitFanInBarrierEnginePublication(t, ctx, backend, restartedBus, event); inserted {
				t.Fatal("exact duplicate fan-in proof publication was inserted again")
			}
			restartedSink.requireNoDelivery(t)
			if err := restartedSink.close(false); err != nil {
				t.Fatalf("retire restarted proof sink: %v", err)
			}
			if got := requireFanInBarrierRouteProof(t, ctx, backend, db, eventID, selectedTarget); got != initialSettlement {
				t.Fatalf("duplicate route settlement changed:\ninitial=%s\nreplayed=%s", initialSettlement, got)
			}
			if got := requireFanInBarrierPublicRouteProof(t, ctx, backend, eventID, selectedTarget); got != initialPublic {
				t.Fatalf("duplicate public route projection changed:\ninitial=%s\nreplayed=%s", initialPublic, got)
			}
		})
	}
}

type fanInBarrierRouteProofSink struct {
	subscription worklifetime.InternalSubscription
}

func newFanInBarrierRouteProofSink(t *testing.T, ctx context.Context, eventBus *runtimebus.EventBus, eventType events.EventType) *fanInBarrierRouteProofSink {
	t.Helper()
	subscription, err := eventBus.SubscribeInternal(ctx, conformanceNode(t, "portfolio", "portfolio-collector").Key(), eventType)
	if err != nil {
		t.Fatalf("subscribe fan-in proof sink: %v", err)
	}
	subscription.MarkReady()
	return &fanInBarrierRouteProofSink{subscription: subscription}
}

func (s *fanInBarrierRouteProofSink) requireDelivery(t *testing.T, eventID string, wantTarget events.RouteIdentity) {
	t.Helper()
	select {
	case delivery := <-s.subscription.Deliveries():
		if delivery == nil {
			t.Fatal("fan-in proof delivery is nil")
		}
		if got := delivery.Event().ID(); got != eventID {
			t.Fatalf("fan-in proof event = %s, want %s", got, eventID)
		}
		if got := delivery.HandoffRoute().Target.Route().Normalized(); got != wantTarget.Normalized() {
			t.Fatalf("fan-in proof handoff target = %#v, want %#v", got, wantTarget.Normalized())
		}
		if got := delivery.Event().TargetRoute().Normalized(); got != wantTarget.Normalized() {
			t.Fatalf("fan-in proof event target = %#v, want %#v", got, wantTarget.Normalized())
		}
		if err := delivery.Complete(); err != nil {
			t.Fatalf("complete fan-in proof delivery: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for fan-in proof delivery")
	}
}

func (s *fanInBarrierRouteProofSink) requireNoDelivery(t *testing.T) {
	t.Helper()
	select {
	case delivery := <-s.subscription.Deliveries():
		if delivery != nil {
			_ = delivery.Complete()
			t.Fatalf("exact duplicate dispatched fan-in proof event %s", delivery.Event().ID())
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func (s *fanInBarrierRouteProofSink) close(restart bool) error {
	if s == nil || s.subscription == nil {
		return nil
	}
	err := s.subscription.Complete(restart)
	s.subscription = nil
	return err
}

func newFanInBarrierRouteProofBus(t *testing.T, backend fanInBarrierConformanceStore, source semanticview.Source) *runtimebus.EventBus {
	t.Helper()
	eventBus, err := newScopedTestEventBus(t, backend, durableConformanceEventBusOptions(backend, runtimebus.EventBusOptions{
		ContractBundle: source,
		WorkOwner:      conformanceTestRuntimeOccurrence(t, authorActivityTestBundleSourceFact.BundleHash()),
	}))
	if err != nil {
		t.Fatalf("create fan-in route proof EventBus: %v", err)
	}
	for _, route := range mustFanInBarrierRoutes(t, backend) {
		if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route.Identity}); err != nil {
			t.Fatalf("restore fan-in proof route %s: %v", route.Identity.InstancePath, err)
		}
	}
	return eventBus
}

func commitFanInBarrierEnginePublication(
	t *testing.T,
	ctx context.Context,
	backend fanInBarrierConformanceStore,
	eventBus *runtimebus.EventBus,
	event events.Event,
) bool {
	t.Helper()
	plans, err := eventBus.PrepareEnginePublications(ctx, []runtimeengine.EmitIntent{{Event: event}})
	if err != nil {
		t.Fatalf("prepare fan-in proof publication: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("fan-in proof publication plans = %d, want 1", len(plans))
	}
	plan, ok := plans[0].(runtimebus.EnginePublicationPlan)
	if !ok {
		t.Fatalf("fan-in proof publication plan = %T", plans[0])
	}
	committed, err := backend.CommitPublication(ctx, plan.PublicationCommand())
	if err != nil {
		_ = eventBus.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
		t.Fatalf("commit fan-in proof publication: %v", err)
	}
	proof, err := runtimebus.NewCommittedEnginePublication(plan, committed)
	if err != nil {
		_ = eventBus.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
		t.Fatalf("admit committed fan-in proof publication: %v", err)
	}
	if err := eventBus.FinalizeEnginePublications(ctx, []runtimeengine.CommittedDurablePublication{proof}); err != nil {
		t.Fatalf("finalize fan-in proof publication: %v", err)
	}
	if err := eventBus.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{Event: event}}); err != nil {
		t.Fatalf("dispatch committed fan-in proof publication: %v", err)
	}
	return proof.NewlyInserted()
}

func requireFanInBarrierRouteProof(
	t *testing.T,
	ctx context.Context,
	backend fanInBarrierConformanceStore,
	db *sql.DB,
	eventID string,
	wantTarget events.RouteIdentity,
) string {
	t.Helper()
	routes, err := backend.ListEventDeliveryRoutes(ctx, eventID)
	if err != nil {
		t.Fatalf("load fan-in proof routes: %v", err)
	}
	portfolioCollector := conformanceNode(t, "portfolio", "portfolio-collector").Key()
	if len(routes) != 1 || routes[0].Recipient.ID() != portfolioCollector || !routes[0].Target.ExistingEntity() || routes[0].Target.Route().Normalized() != wantTarget.Normalized() {
		t.Fatalf("fan-in proof routes = %#v, want portfolio-collector at %#v", routes, wantTarget.Normalized())
	}
	query := `SELECT route_settlement::text FROM events WHERE event_id = $1::uuid`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT route_settlement FROM events WHERE event_id = ?`
	}
	var raw string
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&raw); err != nil {
		t.Fatalf("load fan-in proof settlement: %v", err)
	}
	var settlement events.RouteSettlement
	if err := json.Unmarshal([]byte(raw), &settlement); err != nil {
		t.Fatalf("decode fan-in proof settlement: %v", err)
	}
	if !settlement.Delivered() || settlement.NoDelivery() || settlement.WriteClass() != events.EventWriteNormalPublication {
		t.Fatalf("fan-in proof settlement = %#v, want normal delivery arm", settlement)
	}
	if err := settlement.Validate(routes); err != nil {
		t.Fatalf("validate fan-in proof settlement: %v", err)
	}
	matched := false
	for _, plan := range settlement.Ledger().Plans() {
		if plan.Resolution() != events.ConnectPlanResolved {
			continue
		}
		targetMatched := false
		for _, target := range plan.Targets() {
			targetMatched = targetMatched || target.Normalized() == wantTarget.Normalized()
		}
		candidateMatched := false
		for _, candidate := range plan.Candidates() {
			candidateMatched = candidateMatched || (candidate.Recipient().ID() == portfolioCollector && candidate.Outcome() == events.ConnectCandidateAccepted)
		}
		matched = matched || (targetMatched && candidateMatched)
	}
	if !matched {
		t.Fatalf("fan-in proof connect evaluation = %#v, want resolved exact target and accepted portfolio-collector", settlement.Ledger().Plans())
	}
	normalized, err := json.Marshal(settlement)
	if err != nil {
		t.Fatalf("normalize fan-in proof settlement: %v", err)
	}
	return string(normalized)
}

func requireFanInBarrierPublicRouteProof(
	t *testing.T,
	ctx context.Context,
	backend fanInBarrierConformanceStore,
	eventID string,
	wantTarget events.RouteIdentity,
) string {
	t.Helper()
	view, err := backend.LoadOperatorEvent(ctx, eventID)
	if err != nil {
		t.Fatalf("load public fan-in proof event: %v", err)
	}
	if view.NoDelivery != nil || len(view.Deliveries) != 1 {
		t.Fatalf("public fan-in settlement = deliveries:%#v no_delivery:%#v", view.Deliveries, view.NoDelivery)
	}
	delivery := view.Deliveries[0]
	if delivery.SubscriberID != conformanceNode(t, "portfolio", "portfolio-collector").Key() || delivery.Target.Kind != "existing_entity" || delivery.Target.FlowID != wantTarget.FlowID ||
		delivery.Target.FlowInstance != wantTarget.FlowInstance || delivery.Target.EntityID != wantTarget.EntityID {
		t.Fatalf("public fan-in delivery = %#v, want exact target %#v", delivery, wantTarget.Normalized())
	}
	snapshot, err := view.EventSnapshot()
	if err != nil {
		t.Fatalf("load public fan-in event snapshot: %v", err)
	}
	if got := snapshot.TargetRoute().Normalized(); got != wantTarget.Normalized() {
		t.Fatalf("public fan-in event target = %#v, want %#v", got, wantTarget.Normalized())
	}
	projection, err := json.Marshal(struct {
		EventID    string               `json:"event_id"`
		Target     events.RouteIdentity `json:"target"`
		NoDelivery bool                 `json:"no_delivery"`
	}{
		EventID: view.EventID,
		Target: events.RouteIdentity{
			FlowID: delivery.Target.FlowID, FlowInstance: delivery.Target.FlowInstance, EntityID: delivery.Target.EntityID,
		}.Normalized(),
		NoDelivery: view.NoDelivery != nil,
	})
	if err != nil {
		t.Fatalf("marshal public fan-in proof: %v", err)
	}
	return string(projection)
}

func removeFanInBarrierSelectedOwner(t *testing.T, ctx context.Context, backend fanInBarrierConformanceStore, db *sql.DB, runID, entityID string) {
	t.Helper()
	query := `DELETE FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid AND flow_instance = 'portfolio'`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `DELETE FROM entity_state WHERE run_id = ? AND entity_id = ? AND flow_instance = 'portfolio'`
	}
	result, err := db.ExecContext(ctx, query, runID, entityID)
	if err != nil {
		t.Fatalf("remove fan-in selected owner: %v", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("inspect removed fan-in selected owner: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed fan-in selected owners = %d, want 1", removed)
	}
}

func requireSelectedRunTargetOwner(t *testing.T, ctx context.Context, backend fanInBarrierConformanceStore, flowInstance, entityID string) {
	t.Helper()
	owners, err := backend.ListSelectedRunTargetOwners(ctx)
	if err != nil {
		t.Fatalf("list selected-run target owners: %v", err)
	}
	matches := make([]runtimebus.ActiveTargetDescriptor, 0, 1)
	for _, owner := range owners {
		owner = owner.Normalized()
		if owner.FlowInstance == flowInstance {
			matches = append(matches, owner)
		}
	}
	if len(matches) != 1 || matches[0].EntityID != entityID {
		t.Fatalf("selected-run owner for %s = %#v, want exact entity %s", flowInstance, matches, entityID)
	}
}

func newFanInBarrierRuntime(t *testing.T, backend fanInBarrierConformanceStore, db *sql.DB, source semanticview.Source) fanInBarrierRuntime {
	t.Helper()
	workflowPersistence := runtimepipeline.NewWorkflowPersistence(backend)
	if sqliteStore, ok := backend.(*store.SQLiteRuntimeStore); ok {
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(sqliteStore)
	}
	var (
		coordinator *runtimepipeline.PipelineCoordinator
		manager     *runtimemanager.AgentManager
	)
	workOwner := conformanceTestRuntimeOccurrence(t, authorActivityTestBundleSourceFact.BundleHash())
	eventBus, err := newScopedTestEventBus(t, backend, durableConformanceEventBusOptions(backend, runtimebus.EventBusOptions{
		ContractBundle: source,
		WorkOwner:      workOwner,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if coordinator == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{coordinator}
		},
		TemplateInstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return fmt.Errorf("fan-in barrier instance manager is not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if manager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("fan-in barrier instance manager is not initialized")
			}
			return manager.PrepareFlowInstanceActivation(ctx, req)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if manager == nil {
				return fmt.Errorf("fan-in barrier instance manager is not initialized")
			}
			return manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
		}),
	}))
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	for _, route := range mustFanInBarrierRoutes(t, backend) {
		if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route.Identity}); err != nil {
			t.Fatalf("restore fan-in route %s: %v", route.Identity.InstancePath, err)
		}
	}
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	module := conformanceLoadedWorkflowModule{
		source: source, workflow: workflow, nodes: nodes,
		guards:  runtimepipeline.NewContractGuardRegistry(source),
		actions: runtimepipeline.NewContractActionRegistry(source),
	}
	diagnosticBus := &fanInBarrierDiagnosticBus{EventBus: eventBus}
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(diagnosticBus, runtimepipeline.PipelineCoordinatorOptions{
		ExecutionPosture: executionposture.Live,
		Module:           module,
		InstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return fmt.Errorf("fan-in barrier instance manager is not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
		Persistence:             workflowPersistence,
		RunLifecycle:            backend,
		PipelineObligations:     backend.PipelineObligations(),
		DeliveryStore:           backend,
		DeadLetters:             backend,
		DecisionCards:           backend,
		ProposedEffects:         backend,
		HumanTasks:              backend,
		DecisionCardDraftExpiry: backend,
		HumanTaskExpiry:         backend,
		DeliveryRuntime:         eventBus,
		FlowRoutes:              eventBus,
		GenericSchedules:        fanInBarrierGenericScheduleWakeups{}, ReceiverExecution: eventreceiver.NormalExecution(),
	})

	manager = ownConformanceTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(eventBus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:  executionposture.Live,
		BaseContext:       testAuthorActivityContext(context.Background()),
		BundleSourceFact:  authorActivityTestBundleSourceFact,
		SemanticSource:    source,
		WorkflowInstances: coordinator,
		WorkOwner:         workOwner,
		DeliveryStore:     backend,
		PersistenceRoles:  conformanceManagerPersistenceRoles(backend, eventBus, coordinator), ReceiverExecution: eventreceiver.NormalExecution(),
	}))
	return fanInBarrierRuntime{bus: eventBus, diagnostics: diagnosticBus, pipeline: coordinator}
}

func seedFanInBarrierRun(t *testing.T, ctx context.Context, backend fanInBarrierConformanceStore, db *sql.DB, runID string) {
	t.Helper()
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
	} else {
		runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
	}
}

func seedFanInBarrierPortfolioShell(t *testing.T, ctx context.Context, pipeline *runtimepipeline.PipelineCoordinator, bundle *runtimecontracts.WorkflowContractBundle, entityID string) {
	t.Helper()
	enteredAt := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pipeline.MaterializeInitialEntry(runtimeeffects.WithExecutionMode(ctx, executionmode.Live), runtimepipeline.WorkflowInstance{
		InstanceID:      "portfolio",
		StorageRef:      "portfolio",
		EntityID:        entityID,
		InstanceKind:    "singleton",
		WorkflowName:    "portfolio",
		WorkflowVersion: bundle.WorkflowVersion(),
		CurrentState:    "collecting",
		EnteredStageAt:  enteredAt,
		CreatedAt:       enteredAt,
		Fields:          map[string]any{"portfolio_id": "portfolio"},
	}, enteredAt); err != nil {
		t.Fatalf("seed portfolio singleton identity shell: %v", err)
	}
}

func requireFanInBarrierReportTargets(
	t *testing.T,
	ctx context.Context,
	backend fanInBarrierConformanceStore,
	db *sql.DB,
	wantCount int,
	wantTarget events.RouteIdentity,
) {
	t.Helper()
	query := `SELECT event_id::text FROM events WHERE event_name LIKE $1 ORDER BY created_at, event_id`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT event_id FROM events WHERE event_name LIKE ? ORDER BY created_at, event_id`
	}
	rows, err := db.QueryContext(ctx, query, "%operating.reported")
	if err != nil {
		t.Fatalf("query fan-in report events: %v", err)
	}
	defer rows.Close()
	var eventIDs []string
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			t.Fatalf("scan fan-in report event: %v", err)
		}
		eventIDs = append(eventIDs, eventID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate fan-in report events: %v", err)
	}
	if len(eventIDs) != wantCount {
		t.Fatalf("fan-in report event count = %d, want %d", len(eventIDs), wantCount)
	}
	sourceEntities := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		routes, err := backend.ListEventDeliveryRoutes(ctx, eventID)
		if err != nil {
			t.Fatalf("load fan-in report routes for %s: %v", eventID, err)
		}
		matched := false
		for _, route := range routes {
			if route.Recipient.ID() == conformanceNode(t, "portfolio", "portfolio-collector").Key() && route.Target.Route().Normalized() == wantTarget.Normalized() {
				matched = true
			}
		}
		if !matched {
			t.Fatalf("fan-in report routes for %s = %#v, want exact selected target %#v", eventID, routes, wantTarget.Normalized())
		}
		view, err := backend.LoadOperatorEvent(ctx, eventID)
		if err != nil {
			t.Fatalf("load public fan-in report %s: %v", eventID, err)
		}
		snapshot, err := view.EventSnapshot()
		if err != nil {
			t.Fatalf("load fan-in report snapshot %s: %v", eventID, err)
		}
		sourceRoute := snapshot.RoutingSource().Route().Normalized()
		if sourceRoute.FlowID != "operating" || sourceRoute.FlowInstance == "" || sourceRoute.EntityID == "" {
			t.Fatalf("fan-in report source for %s = %#v, want exact concrete operating owner", eventID, sourceRoute)
		}
		if sourceRoute.EntityID == wantTarget.EntityID {
			t.Fatalf("fan-in report %s copied source entity %s into receiver ownership", eventID, sourceRoute.EntityID)
		}
		sourceEntities[sourceRoute.EntityID] = struct{}{}
	}
	if len(sourceEntities) != wantCount {
		t.Fatalf("fan-in report source entities = %#v, want %d distinguishable child owners", sourceEntities, wantCount)
	}
}

func mustFanInBarrierRoutes(t *testing.T, backend fanInBarrierConformanceStore) []runtimebus.FlowInstanceRouteRecord {
	t.Helper()
	routes, err := backend.ListFlowInstanceRoutes(testAuthorActivityContext(context.Background()))
	if err != nil {
		t.Fatalf("ListFlowInstanceRoutes: %v", err)
	}
	out := make([]runtimebus.FlowInstanceRouteRecord, 0, len(routes))
	for _, route := range routes {
		out = append(out, runtimebus.FlowInstanceRouteRecord{Identity: route})
	}
	return out
}

func publishFanInBarrierIngress(t *testing.T, ctx context.Context, eventBus *runtimebus.EventBus, source semanticview.Source, eventID, localEvent string, payload map[string]any) {
	t.Helper()
	publishFanInBarrierEvent(t, ctx, eventBus, source, eventID, "ingress", localEvent, payload)
}

func publishFanInBarrierEvent(t *testing.T, ctx context.Context, eventBus *runtimebus.EventBus, source semanticview.Source, eventID, flowID, localEvent string, payload map[string]any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", localEvent, err)
	}
	evt := eventtest.ExistingRunRootIngress(
		eventID,
		events.EventType(source.ResolveFlowEventReference(flowID, localEvent)),
		flowID,
		"",
		raw,
		0,
		runtimecorrelation.RunIDFromContext(ctx),
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID: flowID, FlowInstance: flowID, EntityID: runtimeflowidentity.EntityID(flowID),
		}),
		time.Now().UTC(),
	)
	if err := eventBus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatalf("PublishAcknowledged(%s): %v", localEvent, err)
	}
	waitCtx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 10*time.Second)
	defer cancel()
	if err := eventBus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence(%s): %v", localEvent, err)
	}
}

func loadFanInBarrierPortfolio(t *testing.T, ctx context.Context, pipeline *runtimepipeline.PipelineCoordinator) runtimepipeline.WorkflowInstance {
	t.Helper()
	instance, ok, err := pipeline.Load(ctx, runtimeflowidentity.RouteForInstancePath("portfolio"))
	if err != nil || !ok {
		t.Fatalf("load portfolio = found:%v err:%v", ok, err)
	}
	return instance
}

func loadFanInBarrierActivation(t *testing.T, instance runtimepipeline.WorkflowInstance, periodID string) joinruntime.Activation {
	t.Helper()
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
	if err != nil {
		t.Fatalf("load portfolio state carrier: %v", err)
	}
	key := joinruntime.ActivationKey("awaiting", "awaiting", periodID)
	activation, ok, err := joinruntime.Load(carrier.StateBuckets, conformanceNode(t, "portfolio", "portfolio-collector"), key)
	if err != nil || !ok {
		t.Fatalf("load portfolio barrier activation %q = found:%v err:%v", key, ok, err)
	}
	return activation
}

func dumpFanInBarrierEvents(t *testing.T, ctx context.Context, backend fanInBarrierConformanceStore, db *sql.DB) {
	t.Helper()
	query := `SELECT event_id::text, event_name, payload::text FROM events ORDER BY created_at, event_id`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT event_id, event_name, payload FROM events ORDER BY created_at, event_id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Logf("query fan-in events: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var eventID, eventName, payload string
		if err := rows.Scan(&eventID, &eventName, &payload); err != nil {
			t.Logf("scan fan-in event: %v", err)
			return
		}
		routes, routeErr := backend.ListEventDeliveryRoutes(ctx, eventID)
		t.Logf("fan-in event %s %s payload=%s routes=%#v err=%v", eventID, eventName, payload, routes, routeErr)
	}
	deliveryQuery := `SELECT event_id::text, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(failure::text, '') FROM event_deliveries ORDER BY created_at, delivery_id`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		deliveryQuery = `SELECT event_id, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(failure, '') FROM event_deliveries ORDER BY created_at, delivery_id`
	}
	deliveryRows, err := db.QueryContext(ctx, deliveryQuery)
	if err != nil {
		t.Logf("query fan-in deliveries: %v", err)
		return
	}
	defer deliveryRows.Close()
	for deliveryRows.Next() {
		var eventID, subscriberID, status, reason, failure string
		if err := deliveryRows.Scan(&eventID, &subscriberID, &status, &reason, &failure); err != nil {
			t.Logf("scan fan-in delivery: %v", err)
			return
		}
		t.Logf("fan-in delivery event=%s subscriber=%s status=%s reason=%s failure=%s", eventID, subscriberID, status, reason, failure)
	}
}
