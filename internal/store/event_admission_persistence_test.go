package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestEventAdmissionRejectsMalformedDurableIdentityBeforeEveryMutationParity(t *testing.T) {
	type malformedCase struct {
		name string
		want string
		make func(eventID, runID string, createdAt time.Time) events.Event
	}
	cases := []malformedCase{
		{
			name: "event_id",
			want: "event_id",
			make: func(_ string, runID string, createdAt time.Time) events.Event {
				return eventtest.PersistedProjection("not-an-event-uuid", "test.invalid", "agent-1", "task", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, createdAt)
			},
		},
		{
			name: "run_id",
			want: "run_id",
			make: func(eventID, _ string, createdAt time.Time) events.Event {
				return eventtest.PersistedProjection(eventID, "test.invalid", "agent-1", "task", json.RawMessage(`{}`), 0, "not-a-run-uuid", "", events.EventEnvelope{}, createdAt)
			},
		},
		{
			name: "parent_event_id",
			want: "parent_event_id",
			make: func(eventID, runID string, createdAt time.Time) events.Event {
				return eventtest.PersistedProjection(eventID, "test.invalid", "agent-1", "task", json.RawMessage(`{}`), 1, runID, "not-a-parent-uuid", events.EventEnvelope{}, createdAt)
			},
		},
	}

	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		for _, test := range cases {
			backend, test := backend, test
			t.Run(backend.name+"/"+test.name, func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				before := eventMutationSurfaceCounts(t, fixture.db, ctx)
				eventID := uuid.NewString()
				candidate := test.make(eventID, uuid.NewString(), time.Now().UTC().Truncate(time.Microsecond))
				err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, candidate, []string{"agent-1"})
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("malformed event error = %v, want %q", err, test.want)
				}
				after := eventMutationSurfaceCounts(t, fixture.db, ctx)
				for table, count := range before {
					if after[table] != count {
						t.Fatalf("%s rows changed from %d to %d after rejected admission", table, count, after[table])
					}
				}
			})
		}
	}
}

func eventMutationSurfaceCounts(t *testing.T, db *sql.DB, ctx context.Context) map[string]int {
	t.Helper()
	out := make(map[string]int)
	for _, table := range []string{"runs", "events", "event_deliveries", "event_receipts", "author_activity_occurrences", "decision_card_route_obligations"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		out[table] = count
	}
	return out
}

type terminalEventAdmissionState struct {
	Status        string
	RunEventCount int
	EventExists   int
	DeliveryCount int
}

type terminalEventAdmissionHarness struct {
	append           func(context.Context, events.Event) error
	appendTx         func(context.Context, events.Event) error
	appendDiagnostic func(context.Context, events.Event) error
	markTerminal     func(context.Context, string, string, string) error
	loadState        func(context.Context, string, string) (terminalEventAdmissionState, error)
	persistVariants  map[string]func(context.Context, events.Event, string) error
}

type runtimeLogStatusState struct {
	Status         string
	EndedAt        string
	Failure        string
	ControlStatus  string
	ControlReason  string
	EventCounter   int
	EntityCounter  int
	EventRows      int
	EntityRows     int
	RuntimeLogRows int
	ProducedByType string
	DeliveryRows   int
	ReceiptRows    int
}

type runtimeLogStatusHarness struct {
	appendOrdinary func(context.Context, events.Event) error
	transition     func(context.Context, string, string, string) error
	persistLog     func(context.Context, runtimepkg.RuntimeLogPersistenceRecord) error
	loadState      func(context.Context, string) (runtimeLogStatusState, error)
}

type terminalAdmissionCompletionOwner interface {
	UpsertPipelineReceipt(context.Context, string, string, *runtimefailures.Envelope) error
	ConvergeNormalRunCompletion(context.Context, string, []string, map[string][]string) error
}

func seedCanonicalForkedRunForAdmissionTest(ctx context.Context, db *sql.DB, postgres bool, runID string) error {
	continuedAsRunID := uuid.NewString()
	now := time.Now().UTC()
	if postgres {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at, ended_at) VALUES ($1::uuid, 'completed', $2, $2)`, continuedAsRunID, now); err != nil {
			return err
		}
		_, err := db.ExecContext(ctx, `UPDATE runs SET status = 'forked', ended_at = $2, continued_as_run_id = $3::uuid WHERE run_id = $1::uuid`, runID, now, continuedAsRunID)
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at, ended_at) VALUES (?, 'completed', ?, ?)`, continuedAsRunID, now, now); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `UPDATE runs SET status = 'forked', ended_at = ?, continued_as_run_id = ? WHERE run_id = ?`, now, continuedAsRunID, runID)
	return err
}

func convergeTerminalAdmissionRun(
	ctx context.Context,
	db *sql.DB,
	postgres bool,
	owner terminalAdmissionCompletionOwner,
	runID string,
	eventID string,
) error {
	entityID := uuid.NewString()
	now := time.Now().UTC()
	query := `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (?, ?, 'terminal-admission', 'default', 'terminal-admission', 'Terminal Admission', 'done',
			'{}', '{}', '{}', 1, ?, ?, ?)
	`
	args := []any{runID, entityID, now, now, now}
	if postgres {
		query = `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
				gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, 'terminal-admission', 'default', 'terminal-admission', 'Terminal Admission', 'done',
				'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $3, $4, $5)
		`
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("seed terminal completion entity: %w", err)
	}
	if err := owner.UpsertPipelineReceipt(ctx, eventID, "processed", nil); err != nil {
		return fmt.Errorf("settle terminal completion event: %w", err)
	}
	if err := owner.ConvergeNormalRunCompletion(ctx, eventID, nil, map[string][]string{"terminal-admission": {"done"}}); err != nil {
		return fmt.Errorf("converge terminal completion run: %w", err)
	}
	return nil
}

func TestPostgresTerminalEventAdmissionIsImmutableAndIdempotent(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	harness := terminalEventAdmissionHarness{
		append: func(ctx context.Context, evt events.Event) error {
			return commitSemanticEventFixture(ctx, pg, evt)
		},
		appendTx: func(ctx context.Context, evt events.Event) error {
			return pg.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
				return commitSemanticEventFixtureTx(txctx, pg, tx, evt)
			})
		},
		markTerminal: func(ctx context.Context, runID, eventID, status string) error {
			if status == "forked" {
				return seedCanonicalForkedRunForAdmissionTest(ctx, db, true, runID)
			}
			if status == "completed" {
				return convergeTerminalAdmissionRun(ctx, db, true, pg, runID, eventID)
			}
			failure := terminalEventAdmissionFailure(status)
			_, err := pg.MarkRunTerminal(ctx, runID, status, failure, time.Now().UTC())
			return err
		},
		loadState: func(ctx context.Context, runID, eventID string) (terminalEventAdmissionState, error) {
			var state terminalEventAdmissionState
			if err := db.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = $1::uuid`, runID).Scan(&state.Status); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`, runID).Scan(&state.RunEventCount); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&state.EventExists); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&state.DeliveryCount); err != nil {
				return state, err
			}
			return state, nil
		},
		appendDiagnostic: func(ctx context.Context, evt events.Event) error {
			return commitDiagnosticRuntimeLogFixture(ctx, pg, evt)
		},
		persistVariants: terminalEventAdmissionPersistVariants(pg),
	}
	assertTerminalEventAdmission(t, harness)
}

func TestSQLiteTerminalEventAdmissionIsImmutableAndIdempotent(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	harness := terminalEventAdmissionHarness{
		append: func(ctx context.Context, evt events.Event) error {
			return commitSemanticEventFixture(ctx, sqliteStore, evt)
		},
		appendTx: func(ctx context.Context, evt events.Event) error {
			return sqliteStore.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
				return commitSemanticEventFixtureTx(txctx, sqliteStore, tx, evt)
			})
		},
		markTerminal: func(ctx context.Context, runID, eventID, status string) error {
			if status == "forked" {
				return seedCanonicalForkedRunForAdmissionTest(ctx, sqliteStore.DB, false, runID)
			}
			if status == "completed" {
				return convergeTerminalAdmissionRun(ctx, sqliteStore.DB, false, sqliteStore, runID, eventID)
			}
			failure := terminalEventAdmissionFailure(status)
			_, err := sqliteStore.MarkRunTerminal(ctx, runID, status, failure, time.Now().UTC())
			return err
		},
		loadState: func(ctx context.Context, runID, eventID string) (terminalEventAdmissionState, error) {
			var state terminalEventAdmissionState
			if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COALESCE(status, '') FROM runs WHERE run_id = ?`, runID).Scan(&state.Status); err != nil {
				return state, err
			}
			if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = ?`, runID).Scan(&state.RunEventCount); err != nil {
				return state, err
			}
			if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = ?`, eventID).Scan(&state.EventExists); err != nil {
				return state, err
			}
			if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, eventID).Scan(&state.DeliveryCount); err != nil {
				return state, err
			}
			return state, nil
		},
		appendDiagnostic: func(ctx context.Context, evt events.Event) error {
			return commitDiagnosticRuntimeLogFixture(ctx, sqliteStore, evt)
		},
		persistVariants: terminalEventAdmissionPersistVariants(sqliteStore),
	}
	assertTerminalEventAdmission(t, harness)
}

func TestPostgresRuntimeLogAdmissionPreservesEveryRunStatus(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := newTestPostgresStore(t, db)
	assertRuntimeLogAdmissionPreservesEveryRunStatus(t, runtimeLogStatusHarness{
		appendOrdinary: func(ctx context.Context, evt events.Event) error {
			return commitSemanticEventFixture(ctx, store, evt)
		},
		transition: func(ctx context.Context, runID, eventID, status string) error {
			switch status {
			case "running":
				return nil
			case "paused":
				_, err := store.PauseRunControl(ctx, runtimeruncontrol.TransitionRequest{
					RunID: runID, Reason: "runtime_log_status_test", ControlledBy: "test", Now: time.Now().UTC(),
				})
				return err
			case "cancelled":
				_, err := store.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{
					RunID: runID, Reason: "runtime_log_status_test", ControlledBy: "test", Now: time.Now().UTC(),
				})
				return err
			case "completed":
				return convergeTerminalAdmissionRun(ctx, db, true, store, runID, eventID)
			case "forked":
				return seedCanonicalForkedRunForAdmissionTest(ctx, db, true, runID)
			default:
				failure := terminalEventAdmissionFailure(status)
				_, err := store.MarkRunTerminal(ctx, runID, status, failure, time.Now().UTC())
				return err
			}
		},
		persistLog: store.PersistRuntimeLog,
		loadState: func(ctx context.Context, runID string) (runtimeLogStatusState, error) {
			var state runtimeLogStatusState
			if err := db.QueryRowContext(ctx, `
				SELECT
					COALESCE(status, ''),
					COALESCE(ended_at::text, ''),
					COALESCE(failure::text, ''),
					COALESCE(event_count, 0),
					COALESCE(entity_count, 0),
					COALESCE((SELECT control_status FROM run_control_state WHERE run_id = runs.run_id), ''),
					COALESCE((SELECT reason FROM run_control_state WHERE run_id = runs.run_id), '')
				FROM runs
				WHERE run_id = $1::uuid
			`, runID).Scan(
				&state.Status, &state.EndedAt, &state.Failure,
				&state.EventCounter, &state.EntityCounter,
				&state.ControlStatus, &state.ControlReason,
			); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`, runID).Scan(&state.EventRows); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id = $1::uuid`, runID).Scan(&state.EntityRows); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `
				SELECT COUNT(*), COALESCE(MAX(produced_by_type), '')
				FROM events
				WHERE run_id = $1::uuid AND event_name = 'platform.runtime_log'
			`, runID).Scan(&state.RuntimeLogRows, &state.ProducedByType); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM event_deliveries d
				JOIN events e ON e.event_id = d.event_id
				WHERE e.run_id = $1::uuid AND e.event_name = 'platform.runtime_log'
			`, runID).Scan(&state.DeliveryRows); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM event_receipts r
				JOIN events e ON e.event_id = r.event_id
				WHERE e.run_id = $1::uuid AND e.event_name = 'platform.runtime_log'
			`, runID).Scan(&state.ReceiptRows); err != nil {
				return state, err
			}
			return state, nil
		},
	})
}

func TestSQLiteRuntimeLogAdmissionPreservesEveryRunStatus(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	assertRuntimeLogAdmissionPreservesEveryRunStatus(t, runtimeLogStatusHarness{
		appendOrdinary: func(ctx context.Context, evt events.Event) error {
			return commitSemanticEventFixture(ctx, store, evt)
		},
		transition: func(ctx context.Context, runID, eventID, status string) error {
			switch status {
			case "running":
				return nil
			case "paused":
				_, err := store.PauseRunControl(ctx, runtimeruncontrol.TransitionRequest{
					RunID: runID, Reason: "runtime_log_status_test", ControlledBy: "test", Now: time.Now().UTC(),
				})
				return err
			case "cancelled":
				_, err := store.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{
					RunID: runID, Reason: "runtime_log_status_test", ControlledBy: "test", Now: time.Now().UTC(),
				})
				return err
			case "completed":
				return convergeTerminalAdmissionRun(ctx, store.DB, false, store, runID, eventID)
			case "forked":
				return seedCanonicalForkedRunForAdmissionTest(ctx, store.DB, false, runID)
			default:
				failure := terminalEventAdmissionFailure(status)
				_, err := store.MarkRunTerminal(ctx, runID, status, failure, time.Now().UTC())
				return err
			}
		},
		persistLog: store.PersistRuntimeLog,
		loadState: func(ctx context.Context, runID string) (runtimeLogStatusState, error) {
			var state runtimeLogStatusState
			if err := store.DB.QueryRowContext(ctx, `
				SELECT
					COALESCE(status, ''),
					COALESCE(CAST(ended_at AS TEXT), ''),
					COALESCE(CAST(failure AS TEXT), ''),
					COALESCE(event_count, 0),
					COALESCE(entity_count, 0),
					COALESCE((SELECT control_status FROM run_control_state WHERE run_id = runs.run_id), ''),
					COALESCE((SELECT reason FROM run_control_state WHERE run_id = runs.run_id), '')
				FROM runs
				WHERE run_id = ?
			`, runID).Scan(
				&state.Status, &state.EndedAt, &state.Failure,
				&state.EventCounter, &state.EntityCounter,
				&state.ControlStatus, &state.ControlReason,
			); err != nil {
				return state, err
			}
			if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = ?`, runID).Scan(&state.EventRows); err != nil {
				return state, err
			}
			if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id = ?`, runID).Scan(&state.EntityRows); err != nil {
				return state, err
			}
			if err := store.DB.QueryRowContext(ctx, `
				SELECT COUNT(*), COALESCE(MAX(produced_by_type), '')
				FROM events
				WHERE run_id = ? AND event_name = 'platform.runtime_log'
			`, runID).Scan(&state.RuntimeLogRows, &state.ProducedByType); err != nil {
				return state, err
			}
			if err := store.DB.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM event_deliveries d
				JOIN events e ON e.event_id = d.event_id
				WHERE e.run_id = ? AND e.event_name = 'platform.runtime_log'
			`, runID).Scan(&state.DeliveryRows); err != nil {
				return state, err
			}
			if err := store.DB.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM event_receipts r
				JOIN events e ON e.event_id = r.event_id
				WHERE e.run_id = ? AND e.event_name = 'platform.runtime_log'
			`, runID).Scan(&state.ReceiptRows); err != nil {
				return state, err
			}
			return state, nil
		},
	})
}

func assertRuntimeLogAdmissionPreservesEveryRunStatus(t *testing.T, harness runtimeLogStatusHarness) {
	t.Helper()
	ctx := testAuthorActivityContext()
	for _, status := range []string{"running", "paused", "completed", "cancelled", "failed", "forked"} {
		status := status
		t.Run(status, func(t *testing.T) {
			runID := uuid.NewString()
			seed := terminalEventAdmissionEvent(
				uuid.NewString(), runID, `{"value":"runtime-log-status-seed"}`,
				time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond),
			)
			if err := harness.appendOrdinary(ctx, seed); err != nil {
				t.Fatalf("seed ordinary event: %v", err)
			}
			if err := harness.transition(ctx, runID, seed.ID(), status); err != nil {
				t.Fatalf("transition run to %s: %v", status, err)
			}
			before, err := harness.loadState(ctx, runID)
			if err != nil {
				t.Fatalf("load state before runtime log: %v", err)
			}
			payload, err := json.Marshal(map[string]any{
				"log_level": "warn",
				"message":   "runtime log admitted for " + status,
				"details": map[string]any{
					"component": "event_admission_test",
					"action":    "runtime_log_status_" + status,
				},
			})
			if err != nil {
				t.Fatalf("marshal runtime log payload: %v", err)
			}
			if err := harness.persistLog(ctx, runtimepkg.RuntimeLogPersistenceRecord{RunID: runID, Payload: payload, ExecutionMode: executionmode.Live}); err != nil {
				t.Fatalf("persist runtime log for %s: %v", status, err)
			}
			after, err := harness.loadState(ctx, runID)
			if err != nil {
				t.Fatalf("load state after runtime log: %v", err)
			}
			if after.Status != before.Status || after.EndedAt != before.EndedAt || after.Failure != before.Failure ||
				after.ControlStatus != before.ControlStatus || after.ControlReason != before.ControlReason {
				t.Fatalf("runtime log changed lifecycle evidence: before=%+v after=%+v", before, after)
			}
			if after.EventCounter != before.EventCounter+1 || after.EventRows != before.EventRows+1 ||
				after.RuntimeLogRows != before.RuntimeLogRows+1 {
				t.Fatalf("runtime log event accounting mismatch: before=%+v after=%+v", before, after)
			}
			if after.EntityCounter != before.EntityCounter || after.EntityRows != before.EntityRows {
				t.Fatalf("runtime log changed entity state accounting: before=%+v after=%+v", before, after)
			}
			if after.ProducedByType != "platform" || after.DeliveryRows != 0 || after.ReceiptRows != 0 {
				t.Fatalf("runtime log gained executable effects: after=%+v", after)
			}
		})
	}
}

func terminalEventAdmissionPersistVariants(store semanticEventFixtureStore) map[string]func(context.Context, events.Event, string) error {
	return map[string]func(context.Context, events.Event, string) error{
		"commit_event": func(ctx context.Context, evt events.Event, recipient string) error {
			return commitSemanticEventFixtureWithAgents(ctx, store, evt, []string{recipient})
		},
	}
}

func TestJSONSemanticallyEqualPreservesExactNumbers(t *testing.T) {
	t.Parallel()

	if !jsonSemanticallyEqual(
		[]byte(`{"nested":{"value":1},"items":[1e3]}`),
		[]byte(`{"items":[1000.0],"nested":{"value":1.0}}`),
	) {
		t.Fatal("semantically equivalent JSON values were not equal")
	}
	if jsonSemanticallyEqual(
		[]byte(`{"value":9007199254740992}`),
		[]byte(`{"value":9007199254740993}`),
	) {
		t.Fatal("adjacent large JSON integers compared equal")
	}
}

func assertTerminalEventAdmission(t *testing.T, harness terminalEventAdmissionHarness) {
	t.Helper()
	ctx := testAuthorActivityContext()
	for _, status := range []string{"completed", "cancelled", "failed", "forked"} {
		status := status
		t.Run(status, func(t *testing.T) {
			runID := uuid.NewString()
			createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			original := terminalEventAdmissionEvent(uuid.NewString(), runID, `{"value":"original"}`, createdAt)
			if err := harness.append(ctx, original); err != nil {
				t.Fatalf("seed event: %v", err)
			}
			if err := harness.markTerminal(ctx, runID, original.ID(), status); err != nil {
				t.Fatalf("mark terminal: %v", err)
			}

			if err := harness.append(ctx, original); err != nil {
				t.Fatalf("exact duplicate direct append: %v", err)
			}
			if err := harness.appendTx(ctx, original); err != nil {
				t.Fatalf("exact duplicate transactional append: %v", err)
			}

			conflicting := terminalEventAdmissionEvent(original.ID(), runID, `{"value":"changed"}`, createdAt)
			if err := harness.append(ctx, conflicting); !errors.Is(err, ErrEventIdentityConflict) {
				t.Fatalf("conflicting direct duplicate error = %v, want event identity conflict", err)
			}
			if err := harness.appendTx(ctx, conflicting); !errors.Is(err, ErrEventIdentityConflict) {
				t.Fatalf("conflicting transactional duplicate error = %v, want event identity conflict", err)
			}
			taskConflict := terminalEventAdmissionEventWithFacts(original.ID(), runID, "different-task", executionmode.Live, `{"value":"original"}`, createdAt)
			if err := harness.append(ctx, taskConflict); !errors.Is(err, ErrEventIdentityConflict) {
				t.Fatalf("different-task duplicate error = %v, want event identity conflict", err)
			}
			modeConflict := terminalEventAdmissionEventWithFacts(original.ID(), runID, "", executionmode.Mock, `{"value":"original"}`, createdAt)
			if err := harness.appendTx(ctx, modeConflict); !errors.Is(err, ErrEventIdentityConflict) {
				t.Fatalf("different-mode duplicate error = %v, want event identity conflict", err)
			}

			newEvent := terminalEventAdmissionEvent(uuid.NewString(), runID, `{"value":"new"}`, createdAt.Add(time.Second))
			if err := harness.append(ctx, newEvent); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				t.Fatalf("new direct event error = %v, want inactive-run rejection", err)
			}
			if err := harness.appendTx(ctx, newEvent); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				t.Fatalf("new transactional event error = %v, want inactive-run rejection", err)
			}

			state, err := harness.loadState(ctx, runID, newEvent.ID())
			if err != nil {
				t.Fatalf("load state: %v", err)
			}
			if state.Status != status || state.RunEventCount != 1 || state.EventExists != 0 || state.DeliveryCount != 0 {
				t.Fatalf("terminal state after append attempts = %+v, want status=%s one original event and no new rows", state, status)
			}
		})
	}

	for _, status := range []string{"completed", "cancelled", "failed", "forked"} {
		status := status
		for name, persist := range harness.persistVariants {
			name, persist := name, persist
			t.Run("atomic_"+name+"_"+status, func(t *testing.T) {
				runID := uuid.NewString()
				createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				seed := terminalEventAdmissionEvent(uuid.NewString(), runID, `{"value":"seed"}`, createdAt)
				if err := harness.append(ctx, seed); err != nil {
					t.Fatalf("seed event: %v", err)
				}
				if err := harness.markTerminal(ctx, runID, seed.ID(), status); err != nil {
					t.Fatalf("mark %s: %v", status, err)
				}
				if err := persist(ctx, seed, "agent-terminal-duplicate"); err != nil {
					t.Fatalf("exact duplicate atomic persistence: %v", err)
				}
				seedState, err := harness.loadState(ctx, runID, seed.ID())
				if err != nil {
					t.Fatalf("load exact duplicate state: %v", err)
				}
				if seedState.Status != status || seedState.RunEventCount != 1 || seedState.EventExists != 1 || seedState.DeliveryCount != 0 {
					t.Fatalf("state after exact duplicate atomic no-op = %+v", seedState)
				}

				candidate := terminalEventAdmissionEvent(uuid.NewString(), runID, `{"value":"atomic"}`, createdAt.Add(time.Second))
				if err := persist(ctx, candidate, "agent-terminal-new"); !errors.Is(err, storerunlifecycle.ErrRunNotActive) {
					t.Fatalf("atomic persistence error = %v, want inactive-run rejection", err)
				}
				state, err := harness.loadState(ctx, runID, candidate.ID())
				if err != nil {
					t.Fatalf("load state: %v", err)
				}
				if state.Status != status || state.RunEventCount != 1 || state.EventExists != 0 || state.DeliveryCount != 0 {
					t.Fatalf("state after atomic refusal = %+v", state)
				}
			})
		}
	}

	for name, persist := range harness.persistVariants {
		name, persist := name, persist
		t.Run("atomic_"+name+"_active_duplicate_does_not_expand", func(t *testing.T) {
			runID := uuid.NewString()
			createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			evt := terminalEventAdmissionEvent(uuid.NewString(), runID, `{"value":"active"}`, createdAt)
			if err := persist(ctx, evt, "agent-original"); err != nil {
				t.Fatalf("persist original atomic event: %v", err)
			}
			before, err := harness.loadState(ctx, runID, evt.ID())
			if err != nil {
				t.Fatalf("load original side effects: %v", err)
			}
			if err := persist(ctx, evt, "agent-expansion"); err != nil {
				t.Fatalf("persist exact active duplicate: %v", err)
			}
			after, err := harness.loadState(ctx, runID, evt.ID())
			if err != nil {
				t.Fatalf("load duplicate side effects: %v", err)
			}
			if after != before {
				t.Fatalf("exact active duplicate expanded atomic side effects: before=%+v after=%+v", before, after)
			}
		})
	}

	t.Run("typed_diagnostic_direct_preserves_terminal_status", func(t *testing.T) {
		runID := uuid.NewString()
		createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
		seed := terminalEventAdmissionEvent(uuid.NewString(), runID, `{"value":"seed"}`, createdAt)
		if err := harness.append(ctx, seed); err != nil {
			t.Fatalf("seed event: %v", err)
		}
		if err := harness.markTerminal(ctx, runID, seed.ID(), "completed"); err != nil {
			t.Fatalf("mark completed: %v", err)
		}
		diagnostic := eventtest.DiagnosticDirect(
			uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"message":"late evidence"}`),
			0, runID, "", events.EventEnvelope{}, createdAt.Add(time.Second),
		)
		if err := harness.appendDiagnostic(ctx, diagnostic); err != nil {
			t.Fatalf("append typed diagnostic-direct event: %v", err)
		}
		state, err := harness.loadState(ctx, runID, diagnostic.ID())
		if err != nil {
			t.Fatalf("load diagnostic state: %v", err)
		}
		if state.Status != "completed" || state.RunEventCount != 2 || state.EventExists != 1 || state.DeliveryCount != 0 {
			t.Fatalf("state after diagnostic append = %+v", state)
		}
		candidate := eventtest.DiagnosticDirect(
			uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"message":"must remain non-routed"}`),
			0, runID, "", events.EventEnvelope{}, createdAt.Add(2*time.Second),
		)
		if err := harness.append(ctx, candidate); err == nil || !strings.Contains(err.Error(), "requires its named persistence operation") {
			t.Fatalf("diagnostic generic persistence error = %v, want named-operation refusal", err)
		}

		err = events.ValidateEventStructuralContract(
			events.EventAdmissionRootIngress,
			events.EventTypePlatformRuntimeLog,
			eventtest.Producer(events.EventProducerExternal, "runtime"),
			seed.RunID(),
			seed.Scope(),
		)
		if err == nil {
			t.Fatal("root-ingress constructor accepted a closed diagnostic event type")
		}

		for _, subtype := range []struct {
			name      string
			eventType events.EventType
		}{
			{name: "inbound_recorded", eventType: events.EventTypePlatformInboundRecord},
			{name: "agent_directive", eventType: events.EventTypePlatformAgentDirective},
		} {
			subtype := subtype
			t.Run(subtype.name+"_still_requires_active_run", func(t *testing.T) {
				evt := eventtest.DiagnosticDirect(
					uuid.NewString(), subtype.eventType, "runtime", "", json.RawMessage(`{"evidence":"active-only"}`),
					0, runID, "", events.EventEnvelope{}, createdAt.Add(3*time.Second),
				)
				if err := harness.append(ctx, evt); err == nil || !strings.Contains(err.Error(), "requires its named persistence operation") {
					t.Fatalf("diagnostic generic persistence error = %v, want named-operation refusal", err)
				}
			})
		}
	})
}

func terminalEventAdmissionFailure(status string) *runtimefailures.Envelope {
	if status != "failed" {
		return nil
	}
	failure := testFailureEnvelope(runtimefailures.ClassInternalFailure, "terminal_event_admission_test", nil)
	return &failure
}

func terminalEventAdmissionEvent(eventID, runID, payload string, createdAt time.Time) events.Event {
	return terminalEventAdmissionEventWithFacts(eventID, runID, "", executionmode.Live, payload, createdAt)
}

func terminalEventAdmissionEventWithFacts(eventID, runID, taskID string, mode executionmode.Mode, payload string, createdAt time.Time) events.Event {
	if !json.Valid([]byte(payload)) {
		panic(fmt.Sprintf("invalid terminal event admission test payload %q", payload))
	}
	return eventtest.RunCreatingRootIngressWithMode(
		eventID,
		events.EventType("test.terminal_admission"),
		"agent-1",
		taskID,
		json.RawMessage(payload),
		0,
		runID,
		"",
		events.EventEnvelope{},
		createdAt,
		mode,
	)
}

func TestSQLiteDiagnosticDirectEventsRequireClosedTypedOwners(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	assertDiagnosticDirectEventsRequireClosedTypedOwners(t, func(ctx context.Context, evt events.Event) error {
		return commitSemanticEventFixture(ctx, store, evt)
	})
}

func TestPostgresDiagnosticDirectEventsRequireClosedTypedOwners(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	assertDiagnosticDirectEventsRequireClosedTypedOwners(t, func(ctx context.Context, evt events.Event) error {
		return commitSemanticEventFixture(ctx, store, evt)
	})
}

func assertDiagnosticDirectEventsRequireClosedTypedOwners(t *testing.T, appendEvent func(context.Context, events.Event) error) {
	t.Helper()
	runID := uuid.NewString()
	closed := []events.Event{
		eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"ok":true}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
		eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformInboundRecord, "runtime", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC()),
		eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformAgentDirective, "runtime", "", json.RawMessage(`{"ok":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC()),
	}
	lineage, err := events.NewSelectedForkLineage(uuid.NewString(), uuid.NewString(), uuid.NewString(), "selection:generic-refusal", "", executionmode.Live)
	if err != nil {
		t.Fatalf("construct selected-fork lineage: %v", err)
	}
	closed = append(closed, eventtest.SelectedForkReplay(
		uuid.NewString(), "selected.generic_refusal", eventtest.Producer(events.EventProducerNode, "fork-node"), "", json.RawMessage(`{"ok":true}`),
		0, lineage, events.EventEnvelope{}, time.Now().UTC(),
	))
	for _, evt := range closed {
		evt := evt
		t.Run(string(evt.Type()), func(t *testing.T) {
			err := appendEvent(context.Background(), evt)
			if err == nil {
				t.Fatalf("generic append accepted closed event class %s", evt.AdmissionClass())
			}
			if !strings.Contains(err.Error(), "requires its named persistence operation") {
				t.Fatalf("closed event error = %v", err)
			}
		})
	}
}

type globalRuntimeLogIdentityState struct {
	EventCount     int
	RunCount       int
	RunID          string
	ProducedBy     string
	ProducedByType string
	DeliveryCount  int
	ReceiptCount   int
}

type globalRuntimeLogIdentityHarness struct {
	append    func(context.Context, events.Event) error
	appendTx  func(context.Context, events.Event) error
	loadState func(context.Context, string) (globalRuntimeLogIdentityState, error)
}

func TestPostgresGlobalRuntimeLogIdentityIsIdempotentAndNonRouted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	assertGlobalRuntimeLogIdentity(t, globalRuntimeLogIdentityHarness{
		append: func(ctx context.Context, evt events.Event) error {
			return commitDiagnosticRuntimeLogFixture(ctx, store, evt)
		},
		appendTx: func(ctx context.Context, evt events.Event) error {
			return commitDiagnosticRuntimeLogFixture(ctx, store, evt)
		},
		loadState: func(ctx context.Context, eventID string) (globalRuntimeLogIdentityState, error) {
			var state globalRuntimeLogIdentityState
			if err := db.QueryRowContext(ctx, `
				SELECT COUNT(*), COALESCE(MAX(run_id::text), ''), COALESCE(MAX(produced_by), ''), COALESCE(MAX(produced_by_type), '')
				FROM events
				WHERE event_id = $1::uuid
			`, eventID).Scan(&state.EventCount, &state.RunID, &state.ProducedBy, &state.ProducedByType); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&state.RunCount); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&state.DeliveryCount); err != nil {
				return state, err
			}
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, eventID).Scan(&state.ReceiptCount); err != nil {
				return state, err
			}
			return state, nil
		},
	})
}

func TestSQLiteGlobalRuntimeLogIdentityIsIdempotentAndNonRouted(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	assertGlobalRuntimeLogIdentity(t, globalRuntimeLogIdentityHarness{
		append: func(ctx context.Context, evt events.Event) error {
			return commitDiagnosticRuntimeLogFixture(ctx, store, evt)
		},
		appendTx: func(ctx context.Context, evt events.Event) error {
			return commitDiagnosticRuntimeLogFixture(ctx, store, evt)
		},
		loadState: func(ctx context.Context, eventID string) (globalRuntimeLogIdentityState, error) {
			var state globalRuntimeLogIdentityState
			if err := store.DB.QueryRowContext(ctx, `
				SELECT COUNT(*), COALESCE(MAX(run_id), ''), COALESCE(MAX(produced_by), ''), COALESCE(MAX(produced_by_type), '')
				FROM events
				WHERE event_id = ?
			`, eventID).Scan(&state.EventCount, &state.RunID, &state.ProducedBy, &state.ProducedByType); err != nil {
				return state, err
			}
			if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&state.RunCount); err != nil {
				return state, err
			}
			if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, eventID).Scan(&state.DeliveryCount); err != nil {
				return state, err
			}
			if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = ?`, eventID).Scan(&state.ReceiptCount); err != nil {
				return state, err
			}
			return state, nil
		},
	})
}

func assertGlobalRuntimeLogIdentity(t *testing.T, harness globalRuntimeLogIdentityHarness) {
	t.Helper()
	ctx := context.Background()
	eventID := uuid.NewString()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	evt := eventtest.DiagnosticDirect(
		eventID, events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"message":"global identity"}`),
		0, "", "", events.EventEnvelope{}, createdAt,
	)
	baseline, err := harness.loadState(ctx, eventID)
	if err != nil {
		t.Fatalf("load baseline state: %v", err)
	}
	if err := harness.append(ctx, evt); err != nil {
		t.Fatalf("append global runtime log: %v", err)
	}
	if err := harness.appendTx(ctx, evt); err != nil {
		t.Fatalf("append exact global runtime log duplicate: %v", err)
	}
	conflict := eventtest.DiagnosticDirect(
		eventID, events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"message":"conflicting global identity"}`),
		0, "", "", events.EventEnvelope{}, createdAt,
	)
	if err := harness.append(ctx, conflict); !errors.Is(err, ErrEventIdentityConflict) {
		t.Fatalf("conflicting global runtime log error = %v, want event identity conflict", err)
	}
	state, err := harness.loadState(ctx, eventID)
	if err != nil {
		t.Fatalf("load persisted state: %v", err)
	}
	if state.EventCount != 1 || state.RunCount != baseline.RunCount || state.RunID != "" || state.ProducedBy != "runtime" || state.ProducedByType != "platform" || state.DeliveryCount != 0 || state.ReceiptCount != 0 {
		t.Fatalf("global runtime log state = %+v, baseline runs=%d", state, baseline.RunCount)
	}
}

func TestPostgresRunScopedRuntimeLogRequiresExistingRun(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	assertRunScopedRuntimeLogRequiresExistingRun(t, func(ctx context.Context, evt events.Event) error {
		return commitDiagnosticRuntimeLogFixture(ctx, store, evt)
	}, func(ctx context.Context, runID, eventID string) (int, int, error) {
		var runCount, eventCount int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`, runID).Scan(&runCount); err != nil {
			return 0, 0, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&eventCount); err != nil {
			return 0, 0, err
		}
		return runCount, eventCount, nil
	})
}

func TestSQLiteRunScopedRuntimeLogRequiresExistingRun(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	assertRunScopedRuntimeLogRequiresExistingRun(t, func(ctx context.Context, evt events.Event) error {
		return commitDiagnosticRuntimeLogFixture(ctx, store, evt)
	}, func(ctx context.Context, runID, eventID string) (int, int, error) {
		var runCount, eventCount int
		if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, runID).Scan(&runCount); err != nil {
			return 0, 0, err
		}
		if err := store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = ?`, eventID).Scan(&eventCount); err != nil {
			return 0, 0, err
		}
		return runCount, eventCount, nil
	})
}

func assertRunScopedRuntimeLogRequiresExistingRun(
	t *testing.T,
	appendEvent func(context.Context, events.Event) error,
	loadCounts func(context.Context, string, string) (int, int, error),
) {
	t.Helper()
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	evt := eventtest.DiagnosticDirect(
		eventID, events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"message":"missing run"}`),
		0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := appendEvent(ctx, evt); !errors.Is(err, storerunlifecycle.ErrRunNotFound) {
		t.Fatalf("run-scoped runtime log error = %v, want run-not-found refusal", err)
	}
	runCount, eventCount, err := loadCounts(ctx, runID, eventID)
	if err != nil {
		t.Fatalf("load refusal state: %v", err)
	}
	if runCount != 0 || eventCount != 0 {
		t.Fatalf("run-scoped runtime log refusal rows = runs:%d events:%d, want 0/0", runCount, eventCount)
	}
}

func TestPostgresRuntimeLogWriterUsesAdmissionFactsAndRemainsNonRouted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := runtimeeffects.WithExecutionMode(testAuthorActivityContext(), runtimeeffects.ExecutionModeLive)

	logger := runtimepkg.NewRuntimeLogger(pg)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     diaglog.LevelWarn,
		Message:   "admitted global runtime log",
		Component: "admission",
		Action:    "runtime_log_admission",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log: %v", err)
	}

	logEventID, logRunID, logCreatedAt := loadPostgresAdmissionEventFacts(t, ctx, db, `
		SELECT event_id::text, COALESCE(run_id::text, ''), created_at
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND payload->>'message' = $1
	`, "admitted global runtime log")
	if logEventID == "" || logRunID != "" || logCreatedAt.IsZero() {
		t.Fatalf("runtime_log facts id=%q run=%q created_at=%s, want id/no-run/created", logEventID, logRunID, logCreatedAt)
	}

	assertPostgresNoDeliveries(t, ctx, db, logEventID)
}

func TestSQLiteRuntimeLogDiagnosticDirectUsesAdmissionFacts(t *testing.T) {
	sqliteStore := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := runtimeeffects.WithExecutionMode(testAuthorActivityContext(), runtimeeffects.ExecutionModeLive)

	logger := runtimepkg.NewRuntimeLogger(sqliteStore)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     diaglog.LevelWarn,
		Message:   "admitted sqlite global runtime log",
		Component: "admission",
		Action:    "sqlite_runtime_log_admission",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log sqlite: %v", err)
	}

	var eventID, runID, createdAt string
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT event_id, COALESCE(run_id, ''), created_at
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND json_extract(payload, '$.message') = ?
	`, "admitted sqlite global runtime log").Scan(&eventID, &runID, &createdAt); err != nil {
		t.Fatalf("load sqlite runtime_log facts: %v", err)
	}
	if eventID == "" || runID != "" || strings.TrimSpace(createdAt) == "" {
		t.Fatalf("sqlite runtime_log facts id=%q run=%q created_at=%s, want id/no-run/created", eventID, runID, createdAt)
	}

	var deliveries int
	if err := sqliteStore.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ?`, eventID).Scan(&deliveries); err != nil {
		t.Fatalf("count sqlite runtime_log deliveries: %v", err)
	}
	if deliveries != 0 {
		t.Fatalf("sqlite runtime_log deliveries = %d, want 0", deliveries)
	}
}

func loadPostgresAdmissionEventFacts(t *testing.T, ctx context.Context, db rowQueryer, query string, arg any) (string, string, time.Time) {
	t.Helper()
	var (
		eventID   string
		runID     string
		createdAt time.Time
	)
	if err := db.QueryRowContext(ctx, query, arg).Scan(&eventID, &runID, &createdAt); err != nil {
		t.Fatalf("load postgres event facts: %v", err)
	}
	return eventID, runID, createdAt
}

func assertPostgresNoDeliveries(t *testing.T, ctx context.Context, db rowQueryer, eventID string) {
	t.Helper()
	var deliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&deliveries); err != nil {
		t.Fatalf("count event deliveries for %s: %v", eventID, err)
	}
	if deliveries != 0 {
		t.Fatalf("event %s deliveries = %d, want 0", eventID, deliveries)
	}
}
