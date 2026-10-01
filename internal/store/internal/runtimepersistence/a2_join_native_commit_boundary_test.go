package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

type a2NativeJoinStore interface {
	pipeline.WorkflowEngineMutationOwner
	pipeline.WorkflowInstancePersistenceReader
	LoadGenericScheduleActivation(context.Context, string) (genericschedule.Activation, bool, error)
	Snapshot(context.Context, string) (deliverylifecycle.Snapshot, error)
	Outcomes(context.Context, string) ([]deliverylifecycle.Outcome, error)
}

type a2NativeJoinFixture struct {
	command    pipeline.WorkflowEngineMutationCommand
	arm        joinruntime.Activation
	scheduleID string
}

type a2NativeJoinEvidence struct {
	Instance  pipeline.WorkflowInstance
	Schedule  genericschedule.Activation
	Delivery  deliverylifecycle.Snapshot
	Outcomes  []deliverylifecycle.Outcome
	Mutations int
	Revisions int
	Timers    int
	Attempts  int
	Events    int
}

// M32 exercises the physical driver Commit, not loss of an already-acknowledged
// owner result. All mutation SQL, evidence finalization and claims are real.
// Reopening the pool/selected adapter proves durable reload and exact retry,
// including the pending join outcome. It does not claim process-kill recovery,
// outcome publication, scheduler dispatch or public API proof.
func TestA2JoinNativeCommitBoundaryOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cell := range []string{"acknowledged", "commit_before_lost_ack", "rollback_before_lost_ack", "native_deferred_rejection"} {
			t.Run(backend+"/"+cell, func(t *testing.T) {
				selected, db, connector := newStopCommitStore(t, backend)
				ctx, cancel := context.WithTimeout(storeTestWorkContext(t, testAuthorActivityContext()), time.Minute)
				defer cancel()
				fixture := seedA2NativeJoinFixture(t, ctx, backend, db, selected)
				ctx = correlation.WithRunID(ctx, fixture.command.State.Identity.RunID)
				owner := selected.(a2NativeJoinStore)
				before := readA2NativeJoinEvidence(t, ctx, db, owner, fixture)
				assertA2NativeJoinState(t, before, fixture, false)
				if cell == "native_deferred_rejection" {
					installA2NativeJoinCommitRejection(t, ctx, backend, db)
				}

				lost := errors.New("injected native join COMMIT return unavailable")
				var nativeErr error
				var calls int
				connector.arm(func(tx driver.Tx) error {
					calls++
					if cell == "rollback_before_lost_ack" {
						nativeErr = tx.Rollback()
					} else {
						nativeErr = tx.Commit()
					}
					if cell == "commit_before_lost_ack" || cell == "rollback_before_lost_ack" {
						return errors.Join(lost, nativeErr)
					}
					return nativeErr
				})
				result, err := owner.CommitWorkflowEngineMutation(ctx, fixture.command)
				if calls != 1 {
					t.Fatalf("native boundary calls = %d, want exactly one", calls)
				}
				if cell == "acknowledged" {
					if nativeErr != nil || err != nil {
						t.Fatalf("native acknowledged commit: physical=%v owner=%v", nativeErr, err)
					}
					assertA2NativeJoinAcknowledgement(t, result, fixture)
				} else {
					if err == nil || !reflect.DeepEqual(result, pipeline.CommittedWorkflowEngineMutation{}) {
						t.Fatalf("unacknowledged commit fabricated result: result=%#v err=%v", result, err)
					}
					if cell == "native_deferred_rejection" {
						assertA2NativeJoinConstraintError(t, backend, nativeErr, err)
					} else if nativeErr != nil || !errors.Is(err, lost) {
						t.Fatalf("lost return did not preserve its actual native outcome: physical=%v owner=%v", nativeErr, err)
					}
				}

				committed := cell == "acknowledged" || cell == "commit_before_lost_ack"
				after := readA2NativeJoinEvidence(t, ctx, db, owner, fixture)
				if committed {
					assertA2NativeJoinCommitDelta(t, before, after, fixture)
				} else if !reflect.DeepEqual(before, after) {
					t.Fatalf("native rollback/rejection changed durable evidence:\nbefore=%#v\nafter=%#v", before, after)
				}
				if cell == "native_deferred_rejection" {
					var violations int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM a2_commit_child`).Scan(&violations); err != nil || violations != 0 {
						t.Fatalf("native rejected transaction retained violation: count=%d err=%v", violations, err)
					}
					if _, err := db.ExecContext(ctx, `DROP TRIGGER a2_commit_violation`+a2NativeJoinTriggerRelation(backend)); err != nil {
						t.Fatal(err)
					}
				}

				// No in-memory result supplies recovery authority: start another pool
				// and adapter, read the exact durable delivery and retained join arm.
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				restartedDB := sql.OpenDB(connector)
				t.Cleanup(func() { _ = restartedDB.Close() })
				restarted := newStopCommitStoreOwner(t, backend, restartedDB).(a2NativeJoinStore)
				reloaded := readA2NativeJoinEvidence(t, ctx, restartedDB, restarted, fixture)
				if !reflect.DeepEqual(reloaded, after) {
					t.Fatalf("restart changed native outcome evidence:\nbefore=%#v\nreloaded=%#v", after, reloaded)
				}
				if !committed {
					retried, err := restarted.CommitWorkflowEngineMutation(ctx, fixture.command)
					if err != nil {
						t.Fatalf("retry real rolled-back command: %v", err)
					}
					assertA2NativeJoinAcknowledgement(t, retried, fixture)
					reloaded = readA2NativeJoinEvidence(t, ctx, restartedDB, restarted, fixture)
					assertA2NativeJoinCommitDelta(t, before, reloaded, fixture)
				}
				// Exact old command/claim may not apply a second closure after either
				// durable lost-ack recovery or a successfully retried rollback.
				duplicate, err := restarted.CommitWorkflowEngineMutation(ctx, fixture.command)
				if err == nil || !reflect.DeepEqual(duplicate, pipeline.CommittedWorkflowEngineMutation{}) {
					t.Fatalf("duplicate command acknowledged a second closure: result=%#v err=%v", duplicate, err)
				}
				if again := readA2NativeJoinEvidence(t, ctx, restartedDB, restarted, fixture); !reflect.DeepEqual(again, reloaded) {
					t.Fatalf("duplicate command changed settled evidence:\nbefore=%#v\nafter=%#v", reloaded, again)
				}
			})
		}
	}
}

func seedA2NativeJoinFixture(t *testing.T, ctx context.Context, backend string, db *sql.DB, selected selectedFanOutLifecycleOwner) a2NativeJoinFixture {
	t.Helper()
	runID, entityID := uuid.NewString(), uuid.NewString()
	flowID, path := "native-join", "native-join/one"
	ctx = correlation.WithRunID(ctx, runID)
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, path, "active", 1, at)
	record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, path, entityID, "active", 1, at)
	record.CurrentState = "awaiting"
	effect, err := workflowlifecycle.NewInitialEntry(record.Identity.Route, identity.NormalizeEntityID(entityID), record.CurrentState, executionmode.Live, record.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := effect.StageEntry(record.Identity)
	if err != nil || !found {
		t.Fatalf("canonical stage-entry construction: found=%v err=%v", found, err)
	}
	bookkeeping := map[string]any{}
	if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
		t.Fatal(err)
	}
	record.Bookkeeping, err = json.Marshal(bookkeeping)
	if err != nil {
		t.Fatal(err)
	}
	node := mustPersistenceNode(flowID, "join-node")
	ref, err := timeridentity.NewJoinRef(node, "item.completed", "awaiting", "native-commit")
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	arm, err := joinruntime.NewActivation(ref, []string{"one", "two"}, nil, record.UpdatedAt, record.UpdatedAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if disposition, err := arm.Add("one", map[string]any{"value": int64(1)}); err != nil || disposition != joinruntime.AddAccepted {
		t.Fatalf("initial member admission: disposition=%s err=%v", disposition, err)
	}
	encodeArm := func(arm joinruntime.Activation) json.RawMessage {
		buckets := map[string]map[string]any{}
		if err := joinruntime.Store(buckets, arm); err != nil {
			t.Fatal(err)
		}
		raw, err := canonicaljson.MarshalPreservingNumberKinds(engine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets())
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	record.Accumulator = encodeArm(arm)
	handle, err := timeridentity.JoinTimeoutHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.FromGo(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	source, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: entityID})
	if err != nil {
		t.Fatal(err)
	}
	schedule := genericschedule.AdmissionCommand{
		ScheduleKey: handle.TaskID(), RunID: runID, EntityID: entityID, FlowInstance: path,
		OwnerKind: genericschedule.OwnerSystem, OwnerID: "workflow-runtime", EventType: handle.EventType(), Payload: payload,
		RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.AbsoluteDue(arm.FireAt), TaskID: handle.TaskID(),
	}
	owner := selected.(a2NativeJoinStore)
	seeded, err := owner.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: record,
		Lifecycle: pipeline.WorkflowLifecycleMutationPlan{StageEntry: &entry, Schedules: []pipeline.WorkflowScheduleMutation{{Kind: pipeline.WorkflowScheduleMutationUpsert, Command: schedule}}},
	})
	if err != nil || !seeded.Committed || !seeded.Lifecycle.Committed || len(seeded.Lifecycle.GenericScheduleActivations) != 1 {
		t.Fatalf("persist initial join and exact deadline: result=%#v err=%v", seeded, err)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: entityID})}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "item.completed", "fixture", "", []byte(`{"member":"two","value":2}`), 0, runID,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), path), record.UpdatedAt)
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(ctx, selected.(deliveryFixtureStore), event, route)
	if err != nil {
		t.Fatal(err)
	}
	if disposition, err := arm.Add("two", map[string]any{"value": int64(2)}); err != nil || disposition != joinruntime.AddAccepted {
		t.Fatalf("closing member admission: disposition=%s err=%v", disposition, err)
	}
	if !arm.Close(joinruntime.CloseReasonComplete, true, false) {
		t.Fatal("fresh arm did not close")
	}
	arm.TimerCancelled = true
	record.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	record.ExpectedState, record.ExpectedRevision = "awaiting", 2
	record.UpdatedAt = record.UpdatedAt.Add(time.Second)
	record.Fields = json.RawMessage(`{"account_id":"preserved","handled":true,"closed":true}`)
	record.Accumulator = encodeArm(arm)
	return a2NativeJoinFixture{arm: arm, scheduleID: seeded.Lifecycle.GenericScheduleActivations[0].ID,
		command: pipeline.WorkflowEngineMutationCommand{State: record,
			Lifecycle:       pipeline.WorkflowLifecycleMutationPlan{StageEntry: &entry, Schedules: []pipeline.WorkflowScheduleMutation{{Kind: pipeline.WorkflowScheduleMutationCancel, Command: schedule, CancelCause: "join_complete", CancelledAt: record.UpdatedAt}}},
			DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
		},
	}
}

func readA2NativeJoinEvidence(t *testing.T, ctx context.Context, db *sql.DB, owner a2NativeJoinStore, fixture a2NativeJoinFixture) a2NativeJoinEvidence {
	t.Helper()
	var evidence a2NativeJoinEvidence
	var found bool
	var err error
	evidence.Instance, found, err = owner.LoadWorkflowInstance(ctx, fixture.command.State.Identity)
	if err != nil || !found {
		t.Fatalf("reload canonical workflow instance: found=%v err=%v", found, err)
	}
	evidence.Schedule, found, err = owner.LoadGenericScheduleActivation(ctx, fixture.scheduleID)
	if err != nil || !found {
		t.Fatalf("reload canonical join deadline: found=%v err=%v", found, err)
	}
	evidence.Delivery, err = owner.Snapshot(ctx, fixture.command.DeliverySuccess.Claim.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	evidence.Outcomes, err = owner.Outcomes(ctx, fixture.command.DeliverySuccess.Claim.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	for query, count := range map[string]*int{
		`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1`:             &evidence.Mutations,
		`SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`:           &evidence.Revisions,
		`SELECT COUNT(*) FROM timers WHERE run_id=$1`:                       &evidence.Timers,
		`SELECT COUNT(*) FROM events WHERE run_id=$1`:                       &evidence.Events,
		`SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1`: &evidence.Attempts,
	} {
		id := fixture.command.State.Identity.RunID
		if count == &evidence.Attempts {
			id = fixture.command.DeliverySuccess.Claim.DeliveryID()
		}
		if err := db.QueryRowContext(ctx, query, id).Scan(count); err != nil {
			t.Fatal(err)
		}
	}
	return evidence
}

func assertA2NativeJoinState(t *testing.T, evidence a2NativeJoinEvidence, fixture a2NativeJoinFixture, closed bool) {
	t.Helper()
	entry, found, err := workflowlifecycle.LoadStageEntry(evidence.Instance.Bookkeeping)
	if err != nil || !found || entry != *fixture.command.Lifecycle.StageEntry || evidence.Instance.EntityID != fixture.command.State.EntityID || evidence.Instance.CurrentState != "awaiting" {
		t.Fatalf("wrong exact stage-entry/entity owner: entry=%#v instance=%#v err=%v", entry, evidence.Instance, err)
	}
	carrier, err := engine.StateCarrierFromPersisted(evidence.Instance.Fields, evidence.Instance.Bookkeeping, evidence.Instance.Gates, evidence.Instance.StateBuckets)
	if err != nil {
		t.Fatalf("decode persisted join carrier: %v", err)
	}
	arms, err := joinruntime.List(carrier.StateBuckets)
	if err != nil || len(arms) != 1 || arms[0].JoinRef() != fixture.arm.JoinRef() {
		t.Fatalf("wrong retained join arm: arms=%#v err=%v", arms, err)
	}
	if evidence.Schedule.Command.ScheduleKey != fixture.command.Lifecycle.Schedules[0].Command.ScheduleKey || evidence.Schedule.Command.EntityID != fixture.command.State.EntityID || evidence.Schedule.Command.RunID != fixture.command.State.Identity.RunID || evidence.Schedule.Command.FlowInstance != fixture.command.State.Identity.Route.InstancePath || evidence.Attempts != 1 || evidence.Timers != 1 {
		t.Fatalf("wrong exact deadline/claim evidence: %#v", evidence)
	}
	if !closed {
		if evidence.Instance.Revision != 2 || arms[0].Status != joinruntime.StatusOpen || len(arms[0].Outputs) != 1 || evidence.Schedule.Status != genericschedule.StatusActive || evidence.Delivery.Status != deliverylifecycle.StatusInProgress || len(evidence.Outcomes) != 0 {
			t.Fatalf("initial join is not open with active deadline and exact claim: %#v", evidence)
		}
		return
	}
	gotArm, err := canonicaljson.MarshalPreservingNumberKinds(arms[0])
	if err != nil {
		t.Fatal(err)
	}
	wantArm, err := canonicaljson.MarshalPreservingNumberKinds(fixture.arm)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Instance.Revision != 3 || !bytes.Equal(gotArm, wantArm) || evidence.Instance.Fields["closed"] != true || evidence.Schedule.Status != genericschedule.StatusCancelled || evidence.Schedule.CancelCause != "join_complete" || !evidence.Schedule.CancelledAt.Equal(fixture.command.State.UpdatedAt) || evidence.Delivery.Status != deliverylifecycle.StatusDelivered || len(evidence.Outcomes) != 1 {
		t.Fatalf("atomic closure is incomplete: arms=%#v evidence=%#v", arms, evidence)
	}
	claim, outcome := fixture.command.DeliverySuccess.Claim, evidence.Outcomes[0]
	if outcome.ClaimVersion != claim.Version() || outcome.DeliveryID != claim.DeliveryID() || !reflect.DeepEqual(outcome.SideEffects, []string{"handler_completed"}) || !evidence.Delivery.MatchesSettlementClaim(claim) {
		t.Fatalf("closure has wrong settled claim/outcome: %#v", evidence)
	}
}

func assertA2NativeJoinCommitDelta(t *testing.T, before, after a2NativeJoinEvidence, fixture a2NativeJoinFixture) {
	t.Helper()
	assertA2NativeJoinState(t, after, fixture, true)
	if after.Revisions != before.Revisions+1 || after.Mutations != before.Mutations+2 || after.Events != before.Events || after.Timers != before.Timers || after.Attempts != before.Attempts {
		t.Fatalf("closure duplicated/lost exact history: before=%#v after=%#v", before, after)
	}
}

func assertA2NativeJoinAcknowledgement(t *testing.T, result pipeline.CommittedWorkflowEngineMutation, fixture a2NativeJoinFixture) {
	t.Helper()
	if !result.Committed || !result.Lifecycle.Committed || result.DeliverySuccess == nil || !result.DeliverySuccess.Same(fixture.command.DeliverySuccess.Claim) || len(result.Lifecycle.GenericScheduleCancellations) != 1 || result.Lifecycle.GenericScheduleCancellations[0].ID != fixture.scheduleID || len(result.Publications) != 0 || result.Validate() != nil {
		t.Fatalf("acknowledgement lost exact closure/claim evidence: %#v", result)
	}
}

func installA2NativeJoinCommitRejection(t *testing.T, ctx context.Context, backend string, db *sql.DB) {
	t.Helper()
	statements := []string{
		`CREATE TABLE a2_commit_parent (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE a2_commit_child (id INTEGER REFERENCES a2_commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
	}
	if backend == "postgres" {
		statements = append(statements,
			`CREATE FUNCTION a2_commit_violation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO a2_commit_child(id) VALUES (1); RETURN NEW; END $$`,
			`CREATE TRIGGER a2_commit_violation AFTER UPDATE ON entity_state FOR EACH ROW WHEN (NEW.current_state='awaiting') EXECUTE FUNCTION a2_commit_violation()`)
	} else {
		statements = append(statements, `CREATE TRIGGER a2_commit_violation AFTER UPDATE ON entity_state WHEN NEW.current_state='awaiting' BEGIN INSERT INTO a2_commit_child(id) VALUES (1); END`)
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func a2NativeJoinTriggerRelation(backend string) string {
	if backend == "postgres" {
		return " ON entity_state"
	}
	return ""
}

func assertA2NativeJoinConstraintError(t *testing.T, backend string, nativeErr, err error) {
	t.Helper()
	if nativeErr == nil || !errors.Is(err, nativeErr) {
		t.Fatalf("native commit rejection cause lost: physical=%v owner=%v", nativeErr, err)
	}
	if backend == "postgres" {
		var native *pq.Error
		if !errors.As(err, &native) || native.Code != "23503" {
			t.Fatalf("expected native PostgreSQL deferred foreign-key rejection: %v", err)
		}
	} else {
		var native *sqlite.Error
		if !errors.As(err, &native) || native.Code() != 787 {
			t.Fatalf("expected native SQLite deferred foreign-key rejection: %v", err)
		}
	}
}
