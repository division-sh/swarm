package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

type runDispositionFixtureInput struct {
	EventID     string
	RunID       string
	ParentID    string
	SourceRunID string
	SourceEvent string
	OccurredAt  time.Time
}

func TestExistingRunEventClassesNeverCreateOrReopenRunsParity(t *testing.T) {
	tests := []struct {
		name               string
		make               func(runDispositionFixtureInput) events.Event
		needsSameRunParent bool
		exactDuplicate     bool
	}{
		{name: "operator", make: func(in runDispositionFixtureInput) events.Event {
			return eventtest.OperatorInjected(in.EventID, "test.operator", "operator", "", json.RawMessage(`{}`), 0, in.RunID, nil, events.EventEnvelope{}, in.OccurredAt)
		}, exactDuplicate: true},
		{name: "child", make: func(in runDispositionFixtureInput) events.Event {
			return eventtest.ChildWithLineage(in.EventID, "test.child", "worker", "", json.RawMessage(`{}`), 1, events.EventLineage{RunID: in.RunID, ParentEventID: in.ParentID, ExecutionMode: executionmode.Live}, events.EventEnvelope{}, in.OccurredAt)
		}, needsSameRunParent: true, exactDuplicate: true},
		{name: "replay", make: func(in runDispositionFixtureInput) events.Event {
			return eventtest.Replay(in.EventID, "test.replay", "worker", "", json.RawMessage(`{}`), 1, events.EventLineage{RunID: in.RunID, ParentEventID: in.ParentID, ExecutionMode: executionmode.Live}, events.EventEnvelope{}, in.OccurredAt)
		}, needsSameRunParent: true, exactDuplicate: true},
		{name: "selected_fork", make: func(in runDispositionFixtureInput) events.Event {
			lineage, err := events.NewSelectedForkLineage(in.RunID, in.SourceRunID, in.SourceEvent, "selection:test", "", executionmode.Live)
			if err != nil {
				panic(err)
			}
			return eventtest.SelectedForkReplay(in.EventID, "test.selected_fork", eventtest.Producer(events.EventProducerAgent, "worker"), "", json.RawMessage(`{}`), 1, lineage, events.EventEnvelope{}, in.OccurredAt)
		}},
		{name: "causal_runtime_control", make: func(in runDispositionFixtureInput) events.Event {
			return eventtest.RuntimeControl(in.EventID, "platform.paused", "runtime", "", json.RawMessage(`{}`), 1, in.RunID, in.ParentID, events.EventEnvelope{}, in.OccurredAt)
		}, needsSameRunParent: true, exactDuplicate: true},
		{name: "run_scoped_runtime_control", make: func(in runDispositionFixtureInput) events.Event {
			return eventtest.RuntimeControl(in.EventID, "platform.paused", "runtime", "", json.RawMessage(`{}`), 0, in.RunID, "", events.EventEnvelope{}, in.OccurredAt)
		}, exactDuplicate: true},
		{name: "causal_runtime_diagnostic", make: func(in runDispositionFixtureInput) events.Event {
			return eventtest.RuntimeDiagnostic(in.EventID, "platform.agent_started", "runtime", "", json.RawMessage(`{}`), 1, in.RunID, in.ParentID, events.EventEnvelope{}, in.OccurredAt)
		}, needsSameRunParent: true, exactDuplicate: true},
		{name: "run_scoped_runtime_diagnostic", make: func(in runDispositionFixtureInput) events.Event {
			return eventtest.RuntimeDiagnostic(in.EventID, "platform.run_stalled", "runtime", "", json.RawMessage(`{}`), 0, in.RunID, "", events.EventEnvelope{}, in.OccurredAt)
		}, exactDuplicate: true},
		{name: "run_scoped_diagnostic_direct", make: func(in runDispositionFixtureInput) events.Event {
			return eventtest.DiagnosticDirect(in.EventID, events.EventTypePlatformAgentDirective, "runtime", "", json.RawMessage(`{}`), 0, in.RunID, "", events.EventEnvelope{}, in.OccurredAt)
		}, exactDuplicate: true},
	}

	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		for _, test := range tests {
			backend, test := backend, test
			t.Run(backend.name+"/"+test.name, func(t *testing.T) {
				t.Run("missing_run", func(t *testing.T) {
					fixture := backend.open(t)
					ctx := testAuthorActivityContext()
					in := newRunDispositionFixtureInput()
					before := eventMutationSurfaceCounts(t, fixture.db, ctx)
					_, _, err := appendAdmittedBoundaryFixture(ctx, fixture.store, test.make(in))
					if !errors.Is(err, storerunlifecycle.ErrRunNotFound) {
						t.Fatalf("missing-run error = %v, want run not found", err)
					}
					assertEventMutationCountsUnchanged(t, fixture.db, ctx, before, "missing-run refusal")
				})

				t.Run("terminal_run", func(t *testing.T) {
					fixture := backend.open(t)
					ctx := testAuthorActivityContext()
					in := newRunDispositionFixtureInput()
					seedAuthorActivityReceiptRun(t, fixture, ctx, in.RunID)
					if test.needsSameRunParent {
						seedRunDispositionParent(t, fixture, ctx, in.RunID, in.ParentID, in.OccurredAt.Add(-time.Second))
					}
					setRunDispositionFixtureStatus(t, fixture, ctx, in.RunID, "completed")
					before := eventMutationSurfaceCounts(t, fixture.db, ctx)
					_, _, err := appendAdmittedBoundaryFixture(ctx, fixture.store, test.make(in))
					if !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
						t.Fatalf("terminal-run error = %v, want run not active", err)
					}
					assertEventMutationCountsUnchanged(t, fixture.db, ctx, before, "terminal-run refusal")
				})

				if test.exactDuplicate {
					t.Run("exact_duplicate_after_terminal", func(t *testing.T) {
						fixture := backend.open(t)
						ctx := testAuthorActivityContext()
						in := newRunDispositionFixtureInput()
						seedAuthorActivityReceiptRun(t, fixture, ctx, in.RunID)
						if test.needsSameRunParent {
							seedRunDispositionParent(t, fixture, ctx, in.RunID, in.ParentID, in.OccurredAt.Add(-time.Second))
						}
						event := test.make(in)
						_, outcome, err := appendAdmittedBoundaryFixture(ctx, fixture.store, event)
						if err != nil || outcome != runtimebus.EventAppendInserted {
							t.Fatalf("initial append outcome=%v err=%v", outcome, err)
						}
						setRunDispositionFixtureStatus(t, fixture, ctx, in.RunID, "completed")
						before := eventMutationSurfaceCounts(t, fixture.db, ctx)
						_, outcome, err = appendAdmittedBoundaryFixture(ctx, fixture.store, event)
						if err != nil || outcome != runtimebus.EventAppendExactDuplicate {
							t.Fatalf("terminal duplicate outcome=%v err=%v", outcome, err)
						}
						assertEventMutationCountsUnchanged(t, fixture.db, ctx, before, "terminal exact duplicate")
					})
				}
			})
		}
	}
}

func TestOnlyRootAndStandaloneAdmissionCreateRunsParity(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			for _, test := range []struct {
				name         string
				make         func(runID string, at time.Time) events.Event
				wantRunDelta int
			}{
				{name: "root", make: func(runID string, at time.Time) events.Event {
					return eventtest.RootIngress(uuid.NewString(), "test.root", "ingress", "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, at)
				}, wantRunDelta: 1},
				{name: "standalone_runtime", make: func(_ string, at time.Time) events.Event {
					return eventtest.RuntimeControl(uuid.NewString(), "platform.reset", "runtime", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, at)
				}, wantRunDelta: 1},
				{name: "run_creating_agent_directive", make: func(runID string, at time.Time) events.Event {
					return eventtest.RunCreatingDiagnosticDirect(uuid.NewString(), events.EventTypePlatformAgentDirective, "runtime", "", json.RawMessage(`{}`), 0, runID, events.EventEnvelope{}, at)
				}, wantRunDelta: 1},
				{name: "runless_runtime_log", make: func(_ string, at time.Time) events.Event {
					return eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"message":"global"}`), 0, "", "", events.EventEnvelope{}, at)
				}},
			} {
				test := test
				t.Run(test.name, func(t *testing.T) {
					fixture := backend.open(t)
					ctx := testAuthorActivityContext()
					before := eventMutationSurfaceCounts(t, fixture.db, ctx)
					admitted, outcome, err := appendAdmittedBoundaryFixture(ctx, fixture.store, test.make(uuid.NewString(), time.Date(2026, 7, 19, 18, 30, 0, 0, time.UTC)))
					if err != nil || outcome != runtimebus.EventAppendInserted {
						t.Fatalf("append outcome=%v err=%v", outcome, err)
					}
					after := eventMutationSurfaceCounts(t, fixture.db, ctx)
					if after["runs"]-before["runs"] != test.wantRunDelta || after["events"]-before["events"] != 1 {
						t.Fatalf("row deltas = runs:%d events:%d, want %d/1", after["runs"]-before["runs"], after["events"]-before["events"], test.wantRunDelta)
					}
					if test.wantRunDelta == 0 && admitted.Event().RunID() != "" {
						t.Fatalf("runless event acquired run %s", admitted.Event().RunID())
					}
				})
			}
		})
	}
}

func TestNewRunDirectiveOperationCarriesCreationAuthorityParity(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			store := fixture.store.(runtimeagentcontrol.DirectiveOperationStore)
			req := newRunDispositionDirectiveRequest(t, runtimeagentcontrol.RunResolutionNewRunAllocated)
			before := eventMutationSurfaceCounts(t, fixture.db, ctx)

			reserved, err := store.ReserveDirectiveOperation(ctx, req)
			if err != nil {
				t.Fatalf("ReserveDirectiveOperation: %v", err)
			}
			if !reserved.Created || reserved.Operation.ResolvedRunID != req.Operation.ResolvedRunID {
				t.Fatalf("reservation = %#v", reserved)
			}
			after := eventMutationSurfaceCounts(t, fixture.db, ctx)
			if after["runs"]-before["runs"] != 1 || after["events"]-before["events"] != 1 {
				t.Fatalf("row deltas = runs:%d events:%d, want 1/1", after["runs"]-before["runs"], after["events"]-before["events"])
			}
			var operationCount int
			if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM agent_directive_operations WHERE directive_event_id = "+directiveEventIDPlaceholder(fixture.dialect), req.Operation.DirectiveEventID).Scan(&operationCount); err != nil {
				t.Fatalf("count directive operation: %v", err)
			}
			if operationCount != 1 {
				t.Fatalf("directive operation count = %d, want 1", operationCount)
			}

			record, found, err := loadEventProducerIdentityRecord(ctx, fixture, req.Operation.DirectiveEventID)
			if err != nil || !found {
				t.Fatalf("load canonical directive event found=%v err=%v", found, err)
			}
			restored, err := decodeEventRecord(record)
			if err != nil {
				t.Fatalf("decode canonical directive event: %v", err)
			}
			if restored.RunDisposition() != events.AdmittedRunRequireActive {
				t.Fatalf("restored run disposition = %q, want require_active", restored.RunDisposition())
			}
		})
	}
}

func TestDirectiveOperationResolutionMustMatchAdmittedRunAuthorityParity(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		for _, test := range []struct {
			name              string
			eventResolution   string
			claimedResolution string
		}{
			{name: "create_event_claimed_existing", eventResolution: runtimeagentcontrol.RunResolutionNewRunAllocated, claimedResolution: runtimeagentcontrol.RunResolutionSpecified},
			{name: "existing_event_claimed_create", eventResolution: runtimeagentcontrol.RunResolutionSpecified, claimedResolution: runtimeagentcontrol.RunResolutionNewRunAllocated},
		} {
			backend, test := backend, test
			t.Run(backend.name+"/"+test.name, func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				req := newRunDispositionDirectiveRequest(t, test.eventResolution)
				req.Operation.RunIDResolution = test.claimedResolution
				before := eventMutationSurfaceCounts(t, fixture.db, ctx)
				if _, err := fixture.store.(runtimeagentcontrol.DirectiveOperationStore).ReserveDirectiveOperation(ctx, req); err == nil {
					t.Fatal("ReserveDirectiveOperation error = nil")
				}
				assertEventMutationCountsUnchanged(t, fixture.db, ctx, before, "directive resolution mismatch")
			})
		}
	}
}

func newRunDispositionDirectiveRequest(t *testing.T, resolution string) runtimeagentcontrol.ReserveDirectiveOperationRequest {
	t.Helper()
	now := time.Date(2026, 7, 19, 19, 30, 0, 0, time.UTC)
	runID := uuid.NewString()
	operationID := uuid.NewString()
	eventID := uuid.NewString()
	req := runtimeagentcontrol.SendDirectiveRequest{
		AgentID: "agent-1", Directive: "continue", Source: runtimeagentcontrol.DirectiveSourceV1RPC,
		OperatorID: "actor-1",
	}
	if resolution != runtimeagentcontrol.RunResolutionNewRunAllocated {
		req.RunID = runID
	}
	event, err := runtimeagentcontrol.NewDirectiveEvent(req, runtimeagentcontrol.RunTargetResolution{RunID: runID, Mode: resolution}, operationID, eventID, now)
	if err != nil {
		t.Fatalf("NewDirectiveEvent: %v", err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("AdmitForPersistence: %v", err)
	}
	return runtimeagentcontrol.ReserveDirectiveOperationRequest{
		Operation: runtimeagentcontrol.DirectiveOperation{
			OperationID: operationID, Method: runtimeagentcontrol.DirectiveOperationMethod,
			ActorTokenID: "actor-1", RequestHash: "hash-1", AgentID: req.AgentID,
			Directive: req.Directive, RequestedRunID: req.RunID, ResolvedRunID: runID,
			RunIDResolution: resolution, Source: req.Source, OperatorID: req.OperatorID,
			DirectiveEventID: eventID, State: runtimeagentcontrol.DirectiveOperationPrepared,
		},
		Event: admitted,
		Now:   now,
	}
}

func directiveEventIDPlaceholder(dialect runtimeauthoractivity.Dialect) string {
	if dialect == runtimeauthoractivity.DialectPostgres {
		return "$1::uuid"
	}
	return "?"
}

func newRunDispositionFixtureInput() runDispositionFixtureInput {
	return runDispositionFixtureInput{
		EventID: uuid.NewString(), RunID: uuid.NewString(), ParentID: uuid.NewString(),
		SourceRunID: uuid.NewString(), SourceEvent: uuid.NewString(),
		OccurredAt: time.Date(2026, 7, 19, 18, 0, 0, 0, time.UTC),
	}
}

func appendAdmittedBoundaryFixture(ctx context.Context, selected any, event events.Event) (events.AdmittedEvent, runtimebus.EventAppendOutcome, error) {
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return events.AdmittedEvent{}, runtimebus.EventAppendOutcomeUnknown, err
	}
	ctx, release, err := semanticEventFixtureContext(ctx, selected, admitted.Event())
	if err != nil {
		return events.AdmittedEvent{}, runtimebus.EventAppendOutcomeUnknown, err
	}
	defer release()
	var outcome runtimebus.EventAppendOutcome
	switch store := selected.(type) {
	case *PostgresStore:
		err = store.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			var appendErr error
			outcome, appendErr = store.appendAdmittedEventTxOutcome(txctx, tx, admitted)
			return appendErr
		})
	case *SQLiteRuntimeStore:
		err = store.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			var appendErr error
			outcome, appendErr = store.appendAdmittedEventTxOutcome(txctx, tx, admitted)
			return appendErr
		})
	default:
		err = fmt.Errorf("unsupported run-disposition fixture store %T", selected)
	}
	return admitted, outcome, err
}

func seedRunDispositionParent(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID, eventID string, at time.Time) {
	t.Helper()
	parent := eventtest.RootIngress(eventID, "test.parent", "ingress", "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, at)
	if err := insertCanonicalEventRecordFixture(ctx, fixture.store, parent); err != nil {
		t.Fatalf("seed same-run parent: %v", err)
	}
}

func setRunDispositionFixtureStatus(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context, runID, status string) {
	t.Helper()
	query := `UPDATE runs SET status = ?, ended_at = ? WHERE run_id = ?`
	args := []any{status, time.Date(2026, 7, 19, 19, 0, 0, 0, time.UTC), runID}
	if fixture.dialect == "postgres" {
		query = `UPDATE runs SET status = $2, ended_at = $3 WHERE run_id = $1::uuid`
		args = []any{runID, status, time.Date(2026, 7, 19, 19, 0, 0, 0, time.UTC)}
	}
	if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("set run %s status %s: %v", runID, status, err)
	}
}

func assertEventMutationCountsUnchanged(t *testing.T, db *sql.DB, ctx context.Context, before map[string]int, operation string) {
	t.Helper()
	after := eventMutationSurfaceCounts(t, db, ctx)
	for table, count := range before {
		if after[table] != count {
			t.Fatalf("%s changed %s rows from %d to %d", operation, table, count, after[table])
		}
	}
}
