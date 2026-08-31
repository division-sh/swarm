package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type completeEventDispatchStore interface {
	runtimebus.EventStore
	runtimepipeline.WorkflowPersistenceOwner
	runtimebus.TargetFailureDeadLetterRecorder
	runtimedelivery.Store
	runtimemanager.ManagerPersistence
	runtimemanager.AgentLifecycleStateReader
	storetest.AgentFixtureStore
	runtimerunlifecycle.OperationOwner
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.DecisionCardDraftExpiry
	runtimepipeline.HumanTaskExpiry
	PipelineObligations() runtimepipelineobligation.Store
}

type standingDispatchWorkflowModule struct{}

func (standingDispatchWorkflowModule) SemanticSource() semanticview.Source { return nil }
func (standingDispatchWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}
func (standingDispatchWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode  { return nil }
func (standingDispatchWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry   { return nil }
func (standingDispatchWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry { return nil }

type completeEventDispatchFixture struct {
	store    completeEventDispatchStore
	db       *sql.DB
	dialect  string
	ctx      context.Context
	bus      *runtimebus.EventBus
	event    events.Event
	agentID  string
	identity agentidentity.Identity
	source   semanticview.Source
}

type completeEventDispatchGeneration struct {
	manager     *runtimemanager.AgentManager
	coordinator *runtimedeliverycontinuation.Coordinator
	process     *worklifetime.Process
	owner       *worklifetime.RuntimeOccurrence
	grant       runtimestartupownership.GenerationGrant
	capability  runtimestartupownership.ProcessCapability
	once        sync.Once
}

func (g *completeEventDispatchGeneration) close() error {
	if g == nil {
		return nil
	}
	var closeErr error
	g.once.Do(func() {
		if g.coordinator != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			closeErr = errors.Join(closeErr, g.coordinator.Retire(ctx))
		}
		if g.manager != nil {
			closeErr = errors.Join(closeErr, g.manager.Shutdown())
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if g.owner != nil {
			_, err := g.owner.RetireAndWait(ctx)
			closeErr = errors.Join(closeErr, err)
		}
		if g.process != nil {
			g.process.Retire()
			_, err := g.process.Join(ctx)
			closeErr = errors.Join(closeErr, err)
		}
		if closeErr != nil {
			return
		}
		if g.grant != nil {
			closeErr = errors.Join(closeErr, g.grant.Retire(context.Background()))
		}
		if closeErr == nil && g.capability != nil {
			closeErr = errors.Join(closeErr, g.capability.Release(context.Background()))
		}
	})
	return closeErr
}

func TestCompleteEventSnapshotDispatchesThroughRecoveryOwnersOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, surface := range []string{"startup", "global_sweeper", "run_queue", "decision_obligation"} {
			t.Run(backend+"/"+surface, func(t *testing.T) {
				fixture := newCompleteEventDispatchFixture(t, backend, surface == "decision_obligation")
				admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
					AgentID: fixture.agentID, Subscriptions: []string{string(fixture.event.Type())},
				})
				if err != nil {
					t.Fatalf("admit complete-event route: %v", err)
				}
				fixture.bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{Identity: fixture.identity})
				ch := runtimebustest.SubscribeIdentity(t, fixture.bus, fixture.identity, admission)
				defer runtimebustest.UnsubscribeIdentity(fixture.bus, fixture.identity)

				if err := fixture.updateChainDepth(-1); err == nil {
					t.Fatalf("%s schema admitted negative chain_depth", backend)
				}
				if _, err := fixture.invoke(surface); err != nil {
					t.Fatalf("%s dispatch: %v", surface, err)
				}
				assertCompleteLocalDelivery(t, ch, fixture.event)
			})
		}
	}
}

func TestCompleteEventSnapshotDispatchesThroughManagerBacklogOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "flows/global/agents.yaml#agents."+fixture.agentID+".intent", "Prove complete event dispatch.")
			if err != nil {
				t.Fatalf("resolve complete-event agent intent: %v", err)
			}
			fixture.source = completeEventAgentSource(fixture.agentID, string(fixture.event.Type()), intent)
			work, err := fixture.store.PipelineObligations().ClaimEvent(
				fixture.ctx,
				fixture.event.ID(),
				runtimepipelineobligation.PurposeRecovery,
			)
			if err != nil {
				t.Fatalf("claim pipeline obligation: %v", err)
			}
			if _, err := fixture.store.PipelineObligations().Settle(
				fixture.ctx,
				work.Claim,
				runtimepipelineobligation.Acknowledged("pipeline_persisted"),
			); err != nil {
				t.Fatalf("settle pipeline obligation: %v", err)
			}

			if err := fixture.updateChainDepth(-1); err == nil {
				t.Fatalf("%s schema admitted negative chain_depth", backend)
			}
			seen := make(chan events.Event, 1)
			generation := fixture.newRecordingManager(t, seen)
			manager := generation.manager
			managerCtx := fixture.managedContext(t)
			if _, err := manager.HydrateForStartup(managerCtx); err != nil {
				t.Fatalf("hydrate manager: %v", err)
			}
			if err := manager.Run(managerCtx); err != nil {
				t.Fatalf("run manager: %v", err)
			}
			fixture.startDeliveryContinuations(t, managerCtx, generation)
			assertCompleteEventDelivery(t, seen, fixture.event)
		})
	}
}

func TestNormalDeliveryContinuationAcceptCommittedIsAtomicOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(fixture.agentID), AgentIdentity: fixture.identity}
			proof, err := fixture.store.ProveHandoff(fixture.ctx, fixture.event.ID(), route)
			if err != nil {
				t.Fatalf("prove normal committed handoff: %v", err)
			}
			snapshot, err := fixture.store.Snapshot(fixture.ctx, proof.DeliveryID())
			if err != nil {
				t.Fatalf("load normal delivery authority: %v", err)
			}
			process := worklifetime.NewProcess()
			owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
				RuntimeInstanceID: snapshot.Authority.ExecutionID(),
				BundleHash:        "normal-accept-atomicity",
			})
			if err != nil {
				t.Fatalf("create normal delivery work owner: %v", err)
			}
			t.Cleanup(func() {
				if _, err := owner.RetireAndWait(context.Background()); err != nil {
					t.Errorf("retire normal delivery work owner: %v", err)
				}
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Errorf("join normal delivery process owner: %v", err)
				}
			})
			coordinator, err := runtimedeliverycontinuation.New(
				fixture.store,
				fixture.store,
				snapshot.Authority,
				owner,
				fixture.bus,
				nil,
			)
			if err != nil {
				t.Fatalf("construct normal delivery coordinator: %v", err)
			}
			if err := coordinator.AcceptCommitted([]runtimedelivery.DurableHandoffProof{
				proof,
				runtimedelivery.DurableHandoffProof{},
			}); err == nil {
				t.Fatal("partially invalid normal handoff batch succeeded")
			}
			if _, err := coordinator.Acquire(proof.DeliveryID()); err == nil {
				t.Fatal("normal handoff valid prefix transferred before whole-batch validation")
			}
			if err := coordinator.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
				t.Fatalf("accept valid normal handoff: %v", err)
			}
			capability, err := coordinator.Acquire(proof.DeliveryID())
			if err != nil {
				t.Fatalf("acquire accepted normal continuation: %v", err)
			}
			if resolution, err := capability.Resolve(fixture.ctx, worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
				t.Fatalf("return accepted normal continuation: %v", err)
			}
		})
	}
}

func newCompleteEventDispatchFixture(t *testing.T, backend string, decisionObligation bool) completeEventDispatchFixture {
	t.Helper()
	return newCompleteEventDispatchFixtureWithOrigin(
		t,
		backend,
		decisionObligation,
		runlifecyclefixture.ScenarioSetupOrigin(),
	)
}

func newCompleteEventDispatchFixtureWithOrigin(
	t *testing.T,
	backend string,
	decisionObligation bool,
	origin runtimerunlifecycle.RunOrigin,
) completeEventDispatchFixture {
	t.Helper()
	var selected completeEventDispatchStore
	var db *sql.DB
	switch backend {
	case "sqlite":
		sqlite := storetest.StartSQLiteRuntimeStore(t)
		selected, db = sqlite, storetest.DatabaseForTest(sqlite)
	case "postgres":
		_, postgresDB, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		postgres := storetest.AdmitPostgresRuntimeStore(t, postgresDB)
		selected, db = postgres, postgresDB
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	bus, err := newScopedTestEventBus(selected)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := testAuthorActivityContext(context.Background())
	createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	runID, eventID := uuid.NewString(), uuid.NewString()
	if origin.Kind() == runtimerunlifecycle.OriginStandingGeneration {
		runID = runtimeflowidentity.StandingGenerationRunID(origin.ServiceID(), origin.Generation())
		workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)
		if backend == "sqlite" {
			workflowPersistence = runtimepipeline.NewWorkflowPersistence(selected.(*store.SQLiteRuntimeStore))
		}
		workflow := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, runtimepipeline.PipelineCoordinatorOptions{
			ExecutionPosture:        executionposture.Live,
			Module:                  standingDispatchWorkflowModule{},
			Persistence:             workflowPersistence,
			RunLifecycle:            selected,
			DeliveryStore:           selected,
			DeadLetters:             selected,
			PipelineObligations:     selected.PipelineObligations(),
			DecisionCards:           selected,
			ProposedEffects:         selected,
			HumanTasks:              selected,
			DecisionCardDraftExpiry: selected,
			HumanTaskExpiry:         selected,
			DeliveryRuntime:         bus,
			ReceiverExecution:       eventreceiver.NormalExecution(),
		})

		reconciled, err := workflow.ReconcileStandingService(ctx, runtimepipeline.StandingServiceCandidate{
			ServiceID:  origin.ServiceID(),
			PackageKey: "standing-recovery-proof",
			FlowID:     backend,
			InstanceID: uuid.NewString(),
			EntityID:   uuid.NewString(),
			Source:     authorActivityTestBundleSourceFact,
		})
		if err != nil {
			t.Fatalf("reconcile standing recovery fixture: %v", err)
		}
		if reconciled.RunID != runID || reconciled.Generation != origin.Generation() {
			t.Fatalf("standing recovery fixture = %#v, want run %s generation %d", reconciled, runID, origin.Generation())
		}
	} else {
		seedCompleteEventDispatchRunWithOrigin(t, ctx, db, backend, runID, createdAt, origin)
	}
	sourceRoute := events.RouteIdentity{
		FlowID: "source-flow", FlowInstance: "source-flow/one", EntityID: uuid.NewString(),
	}
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute)
	event := eventtest.InExecutionMode(eventtest.PersistedChildForProducer(
		eventID,
		events.EventType("custom.replay.checked"),
		eventtest.Producer(events.EventProducerNode, "declarative-node"),
		"event-owned-task",
		[]byte(`{"task_id":"payload-owned-task","text":"complete snapshot"}`),
		3,
		runID,
		uuid.NewString(),
		envelope,
		createdAt,
	), executionmode.Mock)
	agentID := "complete-event-agent"
	identity := agentidentitytest.RootDeclaredForRun(t, runID, agentID, completeEventAgentOwnerURI(agentID))
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: identity}}, runtimepipelineobligation.ScopeSubscribed)
	fixture := completeEventDispatchFixture{
		store: selected, db: db, dialect: backend, ctx: ctx, bus: bus, event: event, agentID: agentID, identity: identity,
	}
	if decisionObligation {
		fixture.insertDecisionObligation(t)
	}
	return fixture
}

func (f completeEventDispatchFixture) subscribe(t testing.TB, eventTypes ...events.EventType) <-chan *runtimebus.LocalDelivery {
	t.Helper()
	subscriptions := make([]string, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		subscriptions = append(subscriptions, string(eventType))
	}
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID: f.agentID, FlowPath: f.identity.FlowInstance(), Subscriptions: subscriptions,
	})
	if err != nil {
		t.Fatalf("admit complete-event route: %v", err)
	}
	f.bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{Identity: f.identity})
	return runtimebustest.SubscribeIdentity(t, f.bus, f.identity, admission)
}

func seedCompleteEventDispatchRun(t testing.TB, ctx context.Context, db *sql.DB, backend, runID string, startedAt time.Time) {
	t.Helper()
	seedCompleteEventDispatchRunWithOrigin(
		t,
		ctx,
		db,
		backend,
		runID,
		startedAt,
		runlifecyclefixture.ScenarioSetupOrigin(),
	)
}

func seedCompleteEventDispatchRunWithOrigin(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	backend, runID string,
	startedAt time.Time,
	origin runtimerunlifecycle.RunOrigin,
) {
	t.Helper()
	if backend == "postgres" {
		runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: origin, RunID: runID, StartedAt: startedAt})
	} else {
		runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: origin, RunID: runID, StartedAt: startedAt})
	}
}

func (f completeEventDispatchFixture) invoke(surface string) (int, error) {
	switch surface {
	case "startup":
		return 0, runtimepipeline.NewRecoveryManagerWith(f.bus).Recover(f.ctx)
	case "global_sweeper", "decision_obligation":
		result, err := f.bus.SweepPipelineObligations(f.ctx, 10)
		return result.Settled, err
	case "run_queue":
		result, err := f.bus.ReleaseRunQueue(f.ctx, f.event.RunID(), 10)
		return result.Settled, err
	default:
		return 0, errors.New("unknown complete event dispatch surface")
	}
}

func (f completeEventDispatchFixture) updateChainDepth(depth int) error {
	query := `UPDATE events SET chain_depth = ? WHERE event_id = ?`
	args := []any{depth, f.event.ID()}
	if f.dialect == "postgres" {
		query = `UPDATE events SET chain_depth = $1 WHERE event_id = $2::uuid`
	}
	_, err := f.db.ExecContext(f.ctx, query, args...)
	return err
}

func (f completeEventDispatchFixture) assertNoAgentDispatchMutation(t *testing.T) {
	t.Helper()
	var outcomes int
	query := `SELECT COUNT(*) FROM event_delivery_outcomes o JOIN event_deliveries d ON d.delivery_id = o.delivery_id WHERE d.event_id = ? AND d.subscriber_type = 'agent' AND d.subscriber_id = ?`
	args := []any{f.event.ID(), f.agentID}
	if f.dialect == "postgres" {
		query = `SELECT COUNT(*) FROM event_delivery_outcomes o JOIN event_deliveries d ON d.delivery_id = o.delivery_id WHERE d.event_id = $1::uuid AND d.subscriber_type = 'agent' AND d.subscriber_id = $2`
	}
	if err := f.db.QueryRowContext(f.ctx, query, args...).Scan(&outcomes); err != nil {
		t.Fatalf("count agent delivery outcomes: %v", err)
	}
	if outcomes != 0 {
		t.Fatalf("agent delivery outcomes after corrupt readback = %d, want 0", outcomes)
	}
}

func (f completeEventDispatchFixture) decisionObligationStatus(t *testing.T, eventID string) string {
	t.Helper()
	query := `SELECT status FROM decision_card_route_obligations WHERE event_id = ?`
	if f.dialect == "postgres" {
		query = `SELECT status FROM decision_card_route_obligations WHERE event_id = $1::uuid`
	}
	var status string
	if err := f.db.QueryRowContext(f.ctx, query, eventID).Scan(&status); err != nil {
		t.Fatalf("load decision route status for %s: %v", eventID, err)
	}
	return status
}

func (f completeEventDispatchFixture) insertDecisionObligation(t *testing.T) {
	f.insertDecisionObligationFor(t, f.event)
}

func (f completeEventDispatchFixture) insertDecisionObligationFor(t *testing.T, event events.Event) {
	t.Helper()
	cardID := uuid.NewString()
	if f.dialect == "postgres" {
		if _, err := f.db.ExecContext(f.ctx, `
			INSERT INTO decision_cards (
				card_id, run_id, anchor_kind, anchor, status, execution_mode, snapshot,
				card_content_hash, decision_schema_hash, bundle_hash, effective_cadence,
				provenance, verdict, fields, decided_by, decided_at, decision_event_id,
				created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, 'stage_gate', '{}'::jsonb, 'decided', 'mock', '{}'::jsonb,
				'card-hash', 'schema-hash', 'bundle-hash', '{}'::jsonb,
				'{}'::jsonb, 'approve', '{}'::jsonb, 'test', $3, $4::uuid, $3, $3
			)
		`, cardID, event.RunID(), event.CreatedAt(), event.ID()); err != nil {
			t.Fatalf("insert decision card: %v", err)
		}
		if _, err := f.db.ExecContext(f.ctx, `
			INSERT INTO decision_card_route_obligations (
				event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, $3::uuid, 'pending', 0, $4, $4, $4)
		`, event.ID(), cardID, event.RunID(), event.CreatedAt()); err != nil {
			t.Fatalf("insert decision obligation: %v", err)
		}
		return
	}
	if _, err := f.db.ExecContext(f.ctx, `
		INSERT INTO decision_cards (
			card_id, run_id, anchor_kind, anchor, status, execution_mode, snapshot,
			card_content_hash, decision_schema_hash, bundle_hash, effective_cadence,
			provenance, verdict, fields, decided_by, decided_at, decision_event_id,
			created_at, updated_at
		) VALUES (?, ?, 'stage_gate', '{}', 'decided', 'mock', '{}',
			'card-hash', 'schema-hash', 'bundle-hash', '{}', '{}', 'approve', '{}',
			'test', ?, ?, ?, ?)
	`, cardID, event.RunID(), event.CreatedAt(), event.ID(), event.CreatedAt(), event.CreatedAt()); err != nil {
		t.Fatalf("insert decision card: %v", err)
	}
	if _, err := f.db.ExecContext(f.ctx, `
		INSERT INTO decision_card_route_obligations (
			event_id, card_id, run_id, status, attempt_count, next_attempt_at, created_at, updated_at
		) VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)
	`, event.ID(), cardID, event.RunID(), event.CreatedAt(), event.CreatedAt(), event.CreatedAt()); err != nil {
		t.Fatalf("insert decision obligation: %v", err)
	}
}

func (f completeEventDispatchFixture) managedContext(t *testing.T) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"complete-event-dispatch",
		1,
		"",
		"complete-event-proof",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed execution admission: %v", err)
	}
	return managedexecution.WithAdmission(f.ctx, admission)
}

func (f completeEventDispatchFixture) newRecordingManager(
	t *testing.T,
	seen chan<- events.Event,
) *completeEventDispatchGeneration {
	t.Helper()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        authorActivityTestBundleSourceFact.BundleHash(),
	})
	if err != nil {
		t.Fatalf("create complete-event work owner: %v", err)
	}
	manager := runtimemanager.NewAgentManagerWithOptions(f.bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return &completeEventRecordingAgent{id: cfg.ID, subscriptions: []events.EventType{f.event.Type()}, seen: seen}, nil
	}, runtimemanager.AgentManagerOptions{
		BundleSourceFact: authorActivityTestBundleSourceFact,
		SemanticSource:   f.source,
		DeliveryStore:    f.store,
		ExecutionPosture: executionposture.Live,
		PersistenceRoles: runtimemanager.PersistenceRoles{
			AgentRoutes: f.bus, RouteInstaller: f.bus, RouteVerifier: f.bus,
			RouteRestorer: f.bus, RouteRetirer: f.bus, RouteRemover: f.bus,
			CreationPublisher: f.bus, DeliveryRuntime: f.bus,
			LifecycleState: f.store,
		},
		WorkOwner:         owner,
		ReceiverExecution: eventreceiver.NormalExecution(),
	}, f.store)
	f.bus.SetCommittedAgentReadinessFinalizer(runtimebus.CommittedAgentReadinessFinalizerFunc(manager.FinalizeCommittedAgentReadiness))
	generation := &completeEventDispatchGeneration{manager: manager, process: process, owner: owner}
	t.Cleanup(func() {
		if err := generation.close(); err != nil {
			t.Errorf("close complete-event dispatch generation: %v", err)
		}
	})
	bundleHash, bundleSource := authorActivityTestBundleSourceFact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
	desired, err := manager.CompileStaticTopologyDesiredAgents(f.source, coordinate)
	if err != nil {
		t.Fatalf("compile complete-event static topology: %v", err)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatalf("construct complete-event source set: %v", err)
	}
	capability, err := f.store.AcquireProcessCapability(f.ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "complete-event-dispatch-fixture", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	})
	if err != nil {
		t.Fatalf("acquire complete-event process capability: %v", err)
	}
	generation.capability = capability
	if _, err := capability.InstallCompleteSourceSet(f.ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatalf("install complete-event source set: %v", err)
	}
	grant, err := capability.IssueGenerationGrant(f.ctx, runtimestartupownership.GrantRequest{
		BundleHash: bundleHash, BundleSource: bundleSource, RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue complete-event generation grant: %v", err)
	}
	generation.grant = grant
	admission, err := runtimeagenttopology.StaticAdmission(plan.Revision, bundleHash, bundleSource, runtimeagenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatalf("construct complete-event static admission: %v", err)
	}
	if err := manager.InstallStartupTopology(grant, admission, plan); err != nil {
		t.Fatalf("install complete-event startup topology: %v", err)
	}
	if err := manager.ReconcileStaticTopologyForStartup(f.ctx, f.source); err != nil {
		t.Fatalf("reconcile complete-event static topology: %v", err)
	}
	return generation
}

func completeEventAgentSource(agentID, subscription string, intent runtimeagentintent.Resolved) semanticview.Source {
	ownerURI := completeEventAgentOwnerURI(agentID)
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "global", Flow: "global"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			agentID: {
				ID: agentID, Type: "recording", Role: "complete-event-proof", Model: "regular",
				Intent:         runtimeagentintent.Source{Kind: runtimeagentintent.SourceInline, Inline: "Prove complete event dispatch."},
				ResolvedIntent: intent, Subscriptions: []string{subscription},
			},
		},
		AgentURIs: map[string]string{agentID: ownerURI},
	}
	root := &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{"global": &root.Children[0]},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{"global/" + agentID: {Kind: "agent", FlowID: "global", LocalID: agentID, Full: ownerURI}},
			ByURI:  map[string]runtimecontracts.ContractURIRef{ownerURI: {Kind: "agent", FlowID: "global", LocalID: agentID, Full: ownerURI}},
		},
	})
}

func completeEventAgentOwnerURI(agentID string) string {
	return "swarm-test://global/agents/" + agentID
}

func (f completeEventDispatchFixture) startDeliveryContinuations(
	t *testing.T,
	ctx context.Context,
	generation *completeEventDispatchGeneration,
) {
	t.Helper()
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.identity}
	deliveryID, err := runtimedelivery.DeliveryID(f.event.ID(), route)
	if err != nil {
		t.Fatalf("derive complete-event delivery identity: %v", err)
	}
	snapshot, err := f.store.Snapshot(ctx, deliveryID)
	if err != nil {
		t.Fatalf("load complete-event delivery authority: %v", err)
	}
	if err := f.store.ActivateDeliveryAuthority(ctx, snapshot.Authority); err != nil {
		t.Fatalf("activate complete-event delivery authority: %v", err)
	}
	if err := f.bus.SetDeliveryAuthority(snapshot.Authority); err != nil {
		t.Fatalf("configure complete-event delivery authority: %v", err)
	}
	coordinator, err := runtimedeliverycontinuation.New(
		f.store,
		f.store,
		snapshot.Authority,
		generation.owner,
		f.bus,
		func(_ context.Context, reportErr error) {
			t.Errorf("complete-event delivery continuation failed: %v", reportErr)
		},
	)
	if err != nil {
		t.Fatalf("construct complete-event delivery coordinator: %v", err)
	}
	if err := f.bus.SetDeliveryContinuationOwner(coordinator); err != nil {
		t.Fatalf("configure complete-event delivery coordinator: %v", err)
	}
	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("start complete-event delivery coordinator: %v", err)
	}
	generation.coordinator = coordinator
}

type completeEventRecordingAgent struct {
	id            string
	subscriptions []events.EventType
	seen          chan<- events.Event
}

func (a *completeEventRecordingAgent) ID() string { return a.id }

func (*completeEventRecordingAgent) Type() string { return "recording" }

func (a *completeEventRecordingAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}

func (a *completeEventRecordingAgent) OnEvent(_ context.Context, event events.Event) ([]events.Event, error) {
	a.seen <- event
	return nil, nil
}

func assertCompleteEventDelivery(t *testing.T, delivered <-chan events.Event, want events.Event) {
	t.Helper()
	select {
	case got := <-delivered:
		assertCompleteEventSnapshot(t, got, want)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for complete event dispatch")
	}
}

func assertCompleteLocalDelivery(t *testing.T, delivered <-chan *runtimebus.LocalDelivery, want events.Event) {
	t.Helper()
	select {
	case delivery := <-delivered:
		got := delivery.Event()
		_ = delivery.Complete()
		assertCompleteEventSnapshot(t, got, want)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for complete event dispatch")
	}
}

func assertCompleteEventSnapshot(t *testing.T, got, want events.Event) {
	t.Helper()
	var gotPayload, wantPayload any
	if err := json.Unmarshal(got.Payload(), &gotPayload); err != nil {
		t.Fatalf("decode delivered payload: %v", err)
	}
	if err := json.Unmarshal(want.Payload(), &wantPayload); err != nil {
		t.Fatalf("decode expected payload: %v", err)
	}
	if got.ID() != want.ID() || got.Type() != want.Type() || !got.Producer().Equal(want.Producer()) ||
		got.TaskID() != want.TaskID() || got.ChainDepth() != want.ChainDepth() || got.RunID() != want.RunID() ||
		got.ParentEventID() != want.ParentEventID() || got.ExecutionMode() != want.ExecutionMode() ||
		!got.CreatedAt().Equal(want.CreatedAt()) || !reflect.DeepEqual(gotPayload, wantPayload) ||
		!reflect.DeepEqual(got.Envelope(), want.Envelope()) {
		t.Fatalf("complete event snapshot changed\n got: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v\nwant: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v",
			got.ID(), got.Type(), got.ProducerType(), got.SourceAgent(), got.TaskID(), got.ChainDepth(), got.RunID(), got.ParentEventID(), got.ExecutionMode(), got.CreatedAt(), got.Payload(), got.Envelope(),
			want.ID(), want.Type(), want.ProducerType(), want.SourceAgent(), want.TaskID(), want.ChainDepth(), want.RunID(), want.ParentEventID(), want.ExecutionMode(), want.CreatedAt(), want.Payload(), want.Envelope())
	}
}

var _ completeEventDispatchStore = (*store.PostgresStore)(nil)
var _ completeEventDispatchStore = (*store.SQLiteRuntimeStore)(nil)
