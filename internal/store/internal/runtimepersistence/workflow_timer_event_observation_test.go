package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestWorkflowTimerEventObserversKeepPhysicalTraceFailureAndGlobalLogBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			at := time.Now().UTC().Truncate(time.Microsecond)
			node := mustPersistenceNode("review", "progress")
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, state.Identity.RunID, events.EventEnvelope{}, at)
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: state.Identity.Route.InstancePath, EntityID: state.EntityID})}
			owner := f.store.(deliveryFixtureStore)
			if err := commitSemanticEventFixtureWithRoutes(f.ctx, owner, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			pending, err := ReadWorkflowTimerNodeFailureForTest(f.ctx, f.store, event.ID(), node.Key())
			if err != nil || pending.Status != "pending" || string(pending.Failure) != "{}" {
				t.Fatalf("pending timer node lost NULL coalescence: %+v %v", pending, err)
			}
			claimed, err := claimDeliveryFixture(f.ctx, owner, event, route)
			if err != nil {
				t.Fatal(err)
			}
			failure := testRetryableFailure()
			if _, err := owner.SettleFailure(f.ctx, claimed.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: failure.Detail.Code, Failure: failure, RuleSelection: handlerselection.NotReached()}); err != nil {
				t.Fatal(err)
			}
			unrouted := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/other.requested", "fixture", "", []byte(`{}`), 0, state.Identity.RunID, events.EventEnvelope{}, at.Add(time.Millisecond))
			if err := commitSemanticEventFixture(f.ctx, owner, unrouted); err != nil {
				t.Fatal(err)
			}
			for index := range 2 {
				log := eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", []byte(fmt.Sprintf(`{"log_level":"warn","message":"whole-store timer diagnostic","sequence":%d}`, index)), 0, "", "", events.EventEnvelope{}, at.Add(time.Duration(index+2)*time.Millisecond))
				if err := commitDiagnosticRuntimeLogFixture(f.ctx, f.store.(diagnosticRuntimeLogFixtureStore), log); err != nil {
					t.Fatal(err)
				}
			}
			var want WorkflowTimerNodeFailureStorage
			if err := f.db.QueryRowContext(f.ctx, `SELECT status, COALESCE(failure, '{}') FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2`, event.ID(), node.Key()).Scan(&want.Status, &want.Failure); err != nil || string(want.Failure) == "{}" {
				t.Fatalf("native failed delivery has no exact positive failure witness: %+v %v", want, err)
			}
			var logText string
			if err := f.db.QueryRowContext(f.ctx, `SELECT CAST(payload AS TEXT) FROM events WHERE event_name='platform.runtime_log' ORDER BY created_at DESC LIMIT 1`).Scan(&logText); err != nil {
				t.Fatal(err)
			}
			wantTrace := workflowTimerOriginalTrace(t, f, state.Identity.RunID)
			if len(wantTrace) != 2 || wantTrace[0] != "review/finish.requested subscriber="+node.Key()+" status=dead_letter" || wantTrace[1] != "review/other.requested subscriber= status=" {
				t.Fatalf("native trace lacks ordered routed and LEFT JOIN absence witnesses: %v", wantTrace)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if got, err := ReadWorkflowTimerEventIdentityForTest(f.ctx, f.store, state.Identity.RunID, string(event.Type())); err != nil || got != event.ID() {
				t.Fatalf("timer event identity changed run/name scope: %q %v", got, err)
			}
			if got, err := CountRunEventNameStorageForTest(f.ctx, f.store, state.Identity.RunID, string(event.Type())); err != nil || got != 1 {
				t.Fatalf("timer event count changed run/name scope: %d %v", got, err)
			}
			if got, err := ReadWorkflowTimerNodeFailureForTest(f.ctx, f.store, event.ID(), node.Key()); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("timer node diagnostic changed status or physical failure bytes: %+v %v", got, err)
			}
			if got, err := ReadWorkflowTimerDeliveryTraceForTest(f.ctx, f.store, state.Identity.RunID); err != nil || got.ScanError != "" || !reflect.DeepEqual(got.Lines, wantTrace) {
				t.Fatalf("timer trace changed order, LEFT JOIN, coalescence or physical status: %+v %v", got, err)
			}
			if got, err := ReadLatestWorkflowTimerRuntimeLogTextForTest(f.ctx, f.store); err != nil || got != logText {
				t.Fatalf("timer diagnostic narrowed the independent whole-store newest log: %q %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 5 || counts.Total.ReadCommits != 5 || counts.Total.WriteCommits != 0 {
				t.Fatalf("timer observations merged cuts or escaped selected coordinator: %+v", counts)
			}
			for _, scope := range [][2]string{{uuid.NewString(), string(event.Type())}, {state.Identity.RunID, "foreign.event"}} {
				if got, err := ReadWorkflowTimerEventIdentityForTest(f.ctx, f.store, scope[0], scope[1]); !errors.Is(err, sql.ErrNoRows) || got != "" {
					t.Fatalf("foreign run/name borrowed event identity: %q %v", got, err)
				}
				if got, err := CountRunEventNameStorageForTest(f.ctx, f.store, scope[0], scope[1]); err != nil || got != 0 {
					t.Fatalf("foreign run/name borrowed event count: %d %v", got, err)
				}
			}
			if got, err := ReadWorkflowTimerDeliveryTraceForTest(f.ctx, f.store, uuid.NewString()); err != nil || got.Lines != nil || got.ScanError != "" {
				t.Fatalf("foreign run borrowed trace: %+v %v", got, err)
			}
			if got, err := ReadWorkflowTimerNodeFailureForTest(f.ctx, f.store, event.ID(), "foreign-node"); !errors.Is(err, sql.ErrNoRows) || got.Status != "" || got.Failure != nil {
				t.Fatalf("foreign node borrowed failure: %+v %v", got, err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("timer observations changed complete history, data, lease or clocks")
			}
		})
	}
}

func workflowTimerOriginalTrace(t *testing.T, f receiverConfigActivationFixture, runID string) []string {
	t.Helper()
	rows, err := f.db.QueryContext(f.ctx, `SELECT e.event_name, COALESCE(d.subscriber_id, ''), COALESCE(d.status, '') FROM events e LEFT JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 ORDER BY e.created_at`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var trace []string
	for rows.Next() {
		var name, subscriber, status string
		if err := rows.Scan(&name, &subscriber, &status); err != nil {
			t.Fatal(err)
		}
		trace = append(trace, name+" subscriber="+subscriber+" status="+status)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return trace
}

func assertWorkflowTimerEventObserversRefuse(t *testing.T, ctx context.Context, selected any, cause error) {
	t.Helper()
	failed := func(err error) bool { return err != nil && (cause == nil || errors.Is(err, cause)) }
	id := uuid.NewString()
	if got, err := ReadWorkflowTimerEventIdentityForTest(ctx, selected, id, "timer.event"); !failed(err) || got != "" {
		t.Fatalf("failed owner manufactured timer event: %q %v", got, err)
	}
	if got, err := ReadLatestWorkflowTimerRuntimeLogTextForTest(ctx, selected); !failed(err) || got != "" {
		t.Fatalf("failed owner manufactured global diagnostic: %q %v", got, err)
	}
	if got, err := ReadWorkflowTimerDeliveryTraceForTest(ctx, selected, id); !failed(err) || got.Lines != nil || got.ScanError != "" {
		t.Fatalf("failed owner manufactured trace: %+v %v", got, err)
	}
	if got, err := ReadWorkflowTimerNodeFailureForTest(ctx, selected, id, "timer-node"); !failed(err) || got.Status != "" || got.Failure != nil {
		t.Fatalf("failed owner manufactured node diagnostic: %+v %v", got, err)
	}
}

func TestWorkflowTimerEventObserversRefuseCancelledRawAndClosedOwnersBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		assertWorkflowTimerEventObserversRefuse(t, context.Background(), selected, nil)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newCompetingRevisionPairFixture(t, backend)
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			assertWorkflowTimerEventObserversRefuse(t, ctx, f.store, context.Canceled)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			assertWorkflowTimerEventObserversRefuse(t, f.ctx, f.store, nil)
		})
	}
}
