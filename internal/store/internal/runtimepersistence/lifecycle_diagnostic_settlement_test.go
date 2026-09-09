package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type lifecycleDiagnosticTestStore interface {
	agentfixture.Store
	runtimepkg.RuntimeLogPersistence
	runtimemanager.AgentLifecycleDiagnosticPersistence
	ApplyDestructiveResetQuiescence(context.Context, destructivereset.QuiescenceRequest) (destructivereset.QuiescenceResult, error)
}

type lostDiagnosticAcknowledgment struct {
	runtimepkg.RuntimeLogPersistence
}

func (s lostDiagnosticAcknowledgment) PersistLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic, record runtimepkg.RuntimeLogPersistenceRecord) (bool, error) {
	inserted, err := s.RuntimeLogPersistence.PersistLifecycleDiagnostic(ctx, item, record)
	if err == nil && inserted {
		return false, errors.New("lost committed diagnostic acknowledgment")
	}
	return inserted, err
}

func TestLifecycleDiagnosticSettlementFailureCuts(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		for _, phase := range []string{"log", "acknowledgment", "lost_ack"} {
			t.Run(fmt.Sprintf("sqlite=%t/%s", sqlite, phase), func(t *testing.T) {
				store, db := newLifecycleDiagnosticTestStore(t, sqlite)
				ctx := testAuthorActivityContext()
				item := createLifecycleDiagnostic(t, ctx, store)
				recorder := runtimebus.NewEmittedEventsRecorder()
				ctx = runtimebus.WithEmittedEventsRecorder(ctx, recorder)
				var persistence runtimepkg.RuntimeLogPersistence = store
				removeFault := func() {}
				if phase == "lost_ack" {
					persistence = lostDiagnosticAcknowledgment{store}
				} else {
					removeFault = installLifecycleDiagnosticFailure(t, db, sqlite, phase)
				}
				if err := runtimepkg.NewRuntimeLogger(persistence, executionposture.Live).ProjectLifecycleDiagnostic(ctx, item); err == nil {
					t.Fatal("faulted diagnostic settlement succeeded")
				}
				if entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {
					t.Fatalf("recorder claimed uncertain or rolled-back persistence: %v", entries)
				}
				wantLogs, wantPending := 0, 1
				if phase == "lost_ack" {
					wantLogs, wantPending = 1, 0
				}
				if got := diagnosticLogCount(t, db, item.OutboxID); got != wantLogs {
					t.Fatalf("logs=%d want=%d", got, wantLogs)
				}
				pending, err := store.ListPendingAgentLifecycleDiagnostics(ctx, 100)
				if err != nil || len(pending) != wantPending {
					t.Fatalf("pending=%d want=%d err=%v", len(pending), wantPending, err)
				}
				removeFault()
				logger := runtimepkg.NewRuntimeLogger(store, executionposture.Live)
				for i := 0; i < 2; i++ {
					if err := logger.ProjectLifecycleDiagnostic(ctx, item); err != nil {
						t.Fatal(err)
					}
				}
				if got := diagnosticLogCount(t, db, item.OutboxID); got != 1 {
					t.Fatalf("replayed logs=%d want=1", got)
				}
				if entries := recorder.SnapshotFlightRecorder(); len(entries) != wantPending {
					t.Fatalf("recorder entries=%d want=%d", len(entries), wantPending)
				}
			})
		}
	}
}

func installLifecycleDiagnosticFailure(t *testing.T, db *sql.DB, sqlite bool, phase string) func() {
	t.Helper()
	table, action := "events", "INSERT"
	if phase == "acknowledgment" {
		table, action = "agent_lifecycle_diagnostic_outbox", "UPDATE"
	}
	exec := func(query string) {
		t.Helper()
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if sqlite {
		exec(fmt.Sprintf("CREATE TRIGGER fail_diagnostic BEFORE %s ON %s BEGIN SELECT RAISE(ABORT, 'forced diagnostic failure'); END", action, table))
	} else {
		exec("CREATE FUNCTION fail_diagnostic() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'forced diagnostic failure'; END; $$ LANGUAGE plpgsql")
		exec(fmt.Sprintf("CREATE TRIGGER fail_diagnostic BEFORE %s ON %s FOR EACH ROW EXECUTE FUNCTION fail_diagnostic()", action, table))
	}
	cleanup := func() {
		if sqlite {
			exec("DROP TRIGGER IF EXISTS fail_diagnostic")
		} else {
			exec("DROP TRIGGER IF EXISTS fail_diagnostic ON " + table)
			exec("DROP FUNCTION IF EXISTS fail_diagnostic()")
		}
	}
	t.Cleanup(cleanup)
	return cleanup
}

func newLifecycleDiagnosticTestStore(t *testing.T, sqlite bool) (lifecycleDiagnosticTestStore, *sql.DB) {
	t.Helper()
	if sqlite {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		return store, store.backend.ConstructionHandle()
	}
	_, db, _ := testutil.StartPostgres(t)
	return admitTestPostgresStore(t, db), db
}

func createLifecycleDiagnostic(t *testing.T, ctx context.Context, store lifecycleDiagnosticTestStore) diaglog.LifecycleDiagnostic {
	t.Helper()
	return createNamedLifecycleDiagnostic(t, ctx, store, "diagnostic-worker")
}

func createNamedLifecycleDiagnostic(t *testing.T, ctx context.Context, store lifecycleDiagnosticTestStore, name string) diaglog.LifecycleDiagnostic {
	t.Helper()
	return createLifecycleDiagnosticWithIdentity(t, ctx, store, testAgentIdentity(t, name, ""))
}

func createLifecycleDiagnosticWithIdentity(t *testing.T, ctx context.Context, store lifecycleDiagnosticTestStore, identity agentidentity.Identity) diaglog.LifecycleDiagnostic {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	record := runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ID: identity.AgentID(), Identity: identity, Type: "sonnet", Role: "worker",
			FlowID: "global", Model: "regular", ExecutionMode: runtimeeffects.ExecutionModeLive, Config: []byte("{}"),
		}),
		Status: "active", HiredBy: "test", StartedAt: now,
	}
	op := uuid.NewString()
	if _, err := agentfixture.CommitStatic(t, ctx, store, runtimemanager.AgentLifecycleTransition{
		OperationID: op, OperationKind: "spawn", RequestHash: "diagnostic-spawn",
		Identity: identity, AgentID: identity.AgentID(), Trigger: "spawn", TargetEpoch: 1,
		TargetGeneration: 1, TargetPhase: runtimemanager.AgentLifecycleRegistered,
		ConfigRevision: "diagnostic-revision", RunMode: runtimemanager.AgentRunModeStopped, Agent: &record, Now: now,
	}); err != nil {
		t.Fatalf("real lifecycle diagnostic producer: %v", err)
	}
	rows, err := store.ListPendingAgentLifecycleDiagnostics(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.OperationID == op {
			return row
		}
	}
	t.Fatal("producer did not enqueue exact diagnostic")
	return diaglog.LifecycleDiagnostic{}
}

func diagnosticLogCount(t *testing.T, db *sql.DB, outboxID string) int {
	t.Helper()
	rows, err := db.Query("SELECT payload FROM events WHERE event_name = 'platform.runtime_log'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var raw []byte
		var payload struct {
			Details map[string]any `json:"details"`
		}
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Details["outbox_id"] == outboxID {
			count++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestLifecycleDiagnosticRejectsMissingSinkAndAlteredSnapshot(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		t.Run(fmt.Sprintf("sqlite=%t", sqlite), func(t *testing.T) {
			store, db := newLifecycleDiagnosticTestStore(t, sqlite)
			ctx := testAuthorActivityContext()
			item := createLifecycleDiagnostic(t, ctx, store)
			var nilLogger *runtimepkg.RuntimeLogger
			for _, logger := range []*runtimepkg.RuntimeLogger{nilLogger, runtimepkg.NewRuntimeLogger(nil, executionposture.Live)} {
				if err := logger.ProjectLifecycleDiagnostic(ctx, item); err == nil {
					t.Fatal("missing logger/persistence acknowledged diagnostic")
				}
			}
			var bus runtimebus.EventBus
			if err := bus.ProjectLifecycleDiagnostic(ctx, item); err == nil {
				t.Fatal("missing bus logger acknowledged diagnostic")
			}
			wrongStore, _ := newLifecycleDiagnosticTestStore(t, sqlite)
			if err := runtimepkg.NewRuntimeLogger(wrongStore, executionposture.Live).ProjectLifecycleDiagnostic(ctx, item); err == nil {
				t.Fatal("wrong selected store acknowledged diagnostic")
			}
			mutations := []struct {
				name string
				edit func(*diaglog.LifecycleDiagnostic)
			}{
				{"operation", func(d *diaglog.LifecycleDiagnostic) { d.OperationID = uuid.NewString() }},
				{"run", func(d *diaglog.LifecycleDiagnostic) { d.Identity.RunID = uuid.NewString() }},
				{"agent", func(d *diaglog.LifecycleDiagnostic) { d.AgentID = "other" }},
				{"timestamp", func(d *diaglog.LifecycleDiagnostic) { d.CreatedAt = d.CreatedAt.Add(time.Second) }},
				{"payload", func(d *diaglog.LifecycleDiagnostic) { d.Payload = map[string]any{"phase": "invented"} }},
			}
			for _, mutation := range mutations {
				t.Run(mutation.name, func(t *testing.T) {
					wrong := item
					mutation.edit(&wrong)
					if err := runtimepkg.NewRuntimeLogger(store, executionposture.Live).ProjectLifecycleDiagnostic(ctx, wrong); err == nil {
						t.Fatal("altered immutable snapshot acknowledged")
					}
				})
			}
			if got := diagnosticLogCount(t, db, item.OutboxID); got != 0 {
				t.Fatalf("invalid projection wrote %d logs", got)
			}
			pending, err := store.ListPendingAgentLifecycleDiagnostics(ctx, 100)
			if err != nil || len(pending) != 1 {
				t.Fatalf("pending=%d want=1 err=%v", len(pending), err)
			}
		})
	}
}

func TestLifecycleDiagnosticSettlementOnBothStores(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		name := "postgres"
		if sqlite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store, db, reopen := newIndependentLifecycleDiagnosticStores(t, sqlite)
			ctx := testAuthorActivityContext()
			identity := testAgentIdentity(t, "diagnostic-worker", "")
			ctx = runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{
				Owner: "diagnostic-producer", RunID: identity.RunID,
				RowCategory: runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer,
			})
			item := createLifecycleDiagnostic(t, ctx, store)
			other, closeOther := reopen()
			defer closeOther()
			stores := []lifecycleDiagnosticTestStore{store, other}
			start := make(chan struct{})
			results := make(chan error, 16)
			var ready sync.WaitGroup
			ready.Add(16)
			for i := 0; i < 16; i++ {
				go func(selected lifecycleDiagnosticTestStore) {
					logger := runtimepkg.NewRuntimeLogger(selected, executionposture.Live)
					foreign := runtimecorrelation.WithRunID(ctx, uuid.NewString())
					foreign = runtimecorrelation.WithRuntimeLineage(foreign, runtimecorrelation.RuntimeLineage{
						Owner: "foreign-consumer", RunID: uuid.NewString(),
						RowCategory: runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer,
					})
					ready.Done()
					<-start
					results <- logger.ProjectLifecycleDiagnostic(foreign, item)
				}(stores[i%len(stores)])
			}
			ready.Wait()
			close(start)
			for i := 0; i < 16; i++ {
				if err := <-results; err != nil {
					t.Error(err)
				}
			}
			if t.Failed() {
				return
			}
			if count := diagnosticLogCount(t, db, item.OutboxID); count != 1 {
				t.Fatalf("durable projections = %d, want one", count)
			}
			var raw []byte
			if err := db.QueryRow("SELECT payload FROM events WHERE event_name = 'platform.runtime_log'").Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var logged struct {
				Details map[string]any `json:"details"`
			}
			if err := json.Unmarshal(raw, &logged); err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]any{
				"run_id":                       identity.RunID,
				"runtime_lineage_run_id":       identity.RunID,
				"runtime_lineage_owner":        "diagnostic-producer",
				"runtime_lineage_row_category": "diagnostic",
			} {
				if logged.Details[key] != want {
					t.Errorf("%s=%v want=%v", key, logged.Details[key], want)
				}
			}
			pending, err := store.ListPendingAgentLifecycleDiagnostics(ctx, 100)
			if err != nil || len(pending) != 0 {
				t.Fatalf("pending=%v err=%v", pending, err)
			}
			var transitions int
			if err := db.QueryRow("SELECT count(*) FROM agent_lifecycle_transition_facts").Scan(&transitions); err != nil || transitions != 1 {
				t.Fatalf("transitions=%d err=%v", transitions, err)
			}
			// The committed receipt, not the replay caller's posture, owns projection.
			if err := runtimepkg.NewRuntimeLogger(store, executionposture.MockOnly).ProjectLifecycleDiagnostic(ctx, item); err != nil {
				t.Fatal(err)
			}
			wrong := item
			wrong.OperationID = uuid.NewString()
			if err := runtimepkg.NewRuntimeLogger(store, executionposture.Live).ProjectLifecycleDiagnostic(ctx, wrong); err == nil {
				t.Fatal("conflicting immutable snapshot accepted")
			}
		})
	}
}

func newIndependentLifecycleDiagnosticStores(t *testing.T, sqlite bool) (lifecycleDiagnosticTestStore, *sql.DB, func() (lifecycleDiagnosticTestStore, func())) {
	t.Helper()
	if sqlite {
		path := filepath.Join(t.TempDir(), "diagnostics.db")
		first := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
		return first, first.backend.ConstructionHandle(), func() (lifecycleDiagnosticTestStore, func()) {
			t.Helper()
			next, err := NewSQLiteRuntimeStore(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := next.BootstrapSchema(context.Background(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
				_ = next.Close()
				t.Fatal(err)
			}
			return next, func() {
				if err := next.Close(); err != nil {
					t.Error(err)
				}
			}
		}
	}
	dsn, db, _ := testutil.StartPostgres(t)
	return admitTestPostgresStore(t, db), db, func() (lifecycleDiagnosticTestStore, func()) {
		t.Helper()
		nextDB, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatal(err)
		}
		return admitTestPostgresStore(t, nextDB), func() {
			if err := nextDB.Close(); err != nil {
				t.Error(err)
			}
		}
	}
}

type diagnosticSelectionBarrier struct {
	runtimemanager.AgentLifecycleDiagnosticPersistence
	arrived chan<- struct{}
	release <-chan struct{}
}

type diagnosticRuntimeLoggerHook struct {
	*runtimepkg.RuntimeLogger
}

func (h diagnosticRuntimeLoggerHook) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	return h.RuntimeLogger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level: level, Message: message, Component: component, Action: action,
		EventID: eventID, EventType: eventType, AgentID: agentID, EntityID: entityID,
		SessionID: sessionID, Correlation: correlation, Detail: detail,
		Failure: failure, DurationUS: durationUS,
	})
}

func (s diagnosticSelectionBarrier) ListPendingAgentLifecycleDiagnostics(ctx context.Context, limit int) ([]diaglog.LifecycleDiagnostic, error) {
	rows, err := s.AgentLifecycleDiagnosticPersistence.ListPendingAgentLifecycleDiagnostics(ctx, limit)
	if err != nil || len(rows) == 0 {
		return rows, err
	}
	select {
	case s.arrived <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-s.release:
		return rows, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Diagnostic recovery precedes static admission. Even two real managers stopped
// at that later gate must settle their identical SQL selections exactly once.
// Successful complete startup is covered by the served crash/recovery proof.
func TestLifecycleDiagnosticCompetingStartupManagersBeforeAdmission(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		for _, sameManager := range []bool{true, false} {
			t.Run(fmt.Sprintf("sqlite=%t/same_manager=%t", sqlite, sameManager), func(t *testing.T) {
				store, db, reopen := newIndependentLifecycleDiagnosticStores(t, sqlite)
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
				defer cancel()
				item := createLifecycleDiagnostic(t, ctx, store)
				other, closeOther := reopen()
				defer closeOther()
				arrived, release := make(chan struct{}, 2), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				makeManager := func(selected lifecycleDiagnosticTestStore) *runtimemanager.AgentManager {
					t.Helper()
					bus, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore), runtimebus.EventBusOptions{
						Logger: diagnosticRuntimeLoggerHook{runtimepkg.NewRuntimeLogger(selected, executionposture.Live)},
					})
					if err != nil {
						t.Fatal(err)
					}
					return ownStoreTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
						BaseContext: ctx, ExecutionPosture: executionposture.Live,
						ReceiverExecution: eventreceiver.NormalExecution(),
						WorkOwner:         storeTestWorkOwner(t),
						PersistenceRoles: runtimemanager.PersistenceRoles{
							LifecycleDiagnostics: diagnosticSelectionBarrier{selected, arrived, release},
						},
					}, selected.(runtimemanager.ManagerPersistence)))
				}
				first := makeManager(store)
				second := first
				if !sameManager {
					second = makeManager(other)
				}
				results := make(chan error, 2)
				var workers sync.WaitGroup
				defer workers.Wait()
				defer unblock()
				for _, manager := range []*runtimemanager.AgentManager{first, second} {
					workers.Add(1)
					go func() {
						defer workers.Done()
						_, err := manager.HydrateForStartup(runtimecorrelation.WithRunID(ctx, uuid.NewString()))
						results <- err
					}()
				}
				for i := 0; i < 2; i++ {
					select {
					case <-arrived:
					case <-ctx.Done():
						t.Fatal("both real manager readers did not reach the selected row")
					}
				}
				unblock()
				for i := 0; i < 2; i++ {
					if err := <-results; err == nil || !strings.Contains(err.Error(), "static declaration reconciliation must hydrate agents") {
						t.Fatalf("startup must settle diagnostics then fail closed at unprepared static admission: %v", err)
					}
				}
				pending, err := store.ListPendingAgentLifecycleDiagnostics(ctx, 100)
				if err != nil || len(pending) != 0 || diagnosticLogCount(t, db, item.OutboxID) != 1 {
					t.Fatalf("competing manager settlement: pending=%d err=%v", len(pending), err)
				}
				var transitions int
				if err := db.QueryRow("SELECT count(*) FROM agent_lifecycle_transition_facts").Scan(&transitions); err != nil || transitions != 1 {
					t.Fatalf("diagnostic recovery repeated lifecycle work: transitions=%d err=%v", transitions, err)
				}
			})
		}
	}
}

func TestLifecycleDiagnosticDurablePageBoundaryAndReopen(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		t.Run(fmt.Sprintf("sqlite=%t", sqlite), func(t *testing.T) {
			store, db, reopen := newIndependentLifecycleDiagnosticStores(t, sqlite)
			ctx := testAuthorActivityContext()
			item := createLifecycleDiagnostic(t, ctx, store)
			for i := 0; i < 100; i++ {
				state, found, err := store.LoadAgentLifecycleState(ctx, item.Identity)
				if err != nil || !found {
					t.Fatalf("lifecycle state: found=%t err=%v", found, err)
				}
				trigger := "restart"
				if i == 0 {
					trigger = "start"
				}
				if _, err := agentfixture.CommitStatic(t, ctx, store, runtimemanager.AgentLifecycleTransition{
					OperationID: uuid.NewString(), OperationKind: trigger, RequestHash: fmt.Sprintf("page-%d", i),
					Identity: item.Identity, AgentID: item.AgentID, Trigger: trigger,
					ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
					TargetEpoch: state.RuntimeEpoch, TargetGeneration: state.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRunning,
					ConfigRevision: state.ConfigRevision, RunMode: runtimemanager.AgentRunModeStandard, Now: time.Now().UTC(),
				}); err != nil {
					t.Fatal(err)
				}
			}
			page, err := store.ListPendingAgentLifecycleDiagnostics(ctx, 100)
			if err != nil || len(page) != 100 {
				t.Fatalf("page=%d err=%v", len(page), err)
			}
			projector, closeProjector := reopen()
			logger := runtimepkg.NewRuntimeLogger(projector, executionposture.Live)
			for _, item := range page {
				if err := logger.ProjectLifecycleDiagnostic(ctx, item); err != nil {
					closeProjector()
					t.Fatal(err)
				}
			}
			remaining, err := projector.ListPendingAgentLifecycleDiagnostics(ctx, 100)
			if err != nil || len(remaining) != 1 {
				closeProjector()
				t.Fatalf("second page=%d err=%v", len(remaining), err)
			}
			removeFault := installLifecycleDiagnosticFailure(t, db, sqlite, "acknowledgment")
			if err := logger.ProjectLifecycleDiagnostic(ctx, remaining[0]); err == nil {
				closeProjector()
				t.Fatal("second-page acknowledgment fault was hidden")
			}
			if got := diagnosticLogCount(t, db, remaining[0].OutboxID); got != 0 {
				closeProjector()
				t.Fatalf("failed second-page transaction retained %d logs", got)
			}
			closeProjector()
			removeFault()
			// Reopen, not just reallocate a logger: durable receipt and pending selection
			// must survive loss of the complete projecting handle.
			next, closeNext := reopen()
			defer closeNext()
			logger = runtimepkg.NewRuntimeLogger(next, executionposture.MockOnly)
			pending, err := next.ListPendingAgentLifecycleDiagnostics(ctx, 100)
			if err != nil || len(pending) != 1 {
				t.Fatalf("reopened pending=%d err=%v", len(pending), err)
			}
			for _, item := range append(page, pending...) {
				if err := logger.ProjectLifecycleDiagnostic(ctx, item); err != nil {
					t.Fatal(err)
				}
			}
			pending, err = next.ListPendingAgentLifecycleDiagnostics(ctx, 100)
			if err != nil || len(pending) != 0 {
				t.Fatalf("remaining pending=%d err=%v", len(pending), err)
			}
			for _, query := range []string{
				"SELECT count(*) FROM events WHERE event_name = 'platform.runtime_log'",
				"SELECT count(*) FROM agent_lifecycle_transition_facts",
				"SELECT count(*) FROM agent_lifecycle_diagnostic_outbox WHERE projected_at IS NOT NULL",
			} {
				var count int
				if err := db.QueryRow(query).Scan(&count); err != nil || count != 101 {
					t.Fatalf("%s: count=%d err=%v", query, count, err)
				}
			}
		})
	}
}

func TestLifecycleDiagnosticResetHistoryOnBothStores(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		for _, before := range []bool{false, true} {
			t.Run(fmt.Sprintf("sqlite=%t/projected_before_reset=%t", sqlite, before), func(t *testing.T) {
				store, db := newLifecycleDiagnosticTestStore(t, sqlite)
				ctx := testAuthorActivityContext()
				item := createLifecycleDiagnostic(t, ctx, store)
				logger := runtimepkg.NewRuntimeLogger(store, executionposture.Live)
				if before {
					if err := logger.ProjectLifecycleDiagnostic(ctx, item); err != nil {
						t.Fatal(err)
					}
				}
				capability, err := agentfixture.ProcessCapability(t, ctx, store)
				if err != nil {
					t.Fatal(err)
				}
				request := admitRetainedResetCleanupProof(t, capability, store, item.Identity.RunID, false)
				if _, err := capability.ApplyDestructiveResetCleanup(ctx, request, nil); err != nil {
					t.Fatal(err)
				}
				after := runtimepkg.NewRuntimeLogger(store, executionposture.Live)
				for i := 0; i < 2; i++ {
					if err := after.ProjectLifecycleDiagnostic(runtimecorrelation.WithRunID(ctx, uuid.NewString()), item); err != nil {
						t.Fatal(err)
					}
				}
				want := 1
				if before {
					want = 0
				}
				if count := diagnosticLogCount(t, db, item.OutboxID); count != want {
					t.Fatalf("retained historical log count=%d, want %d", count, want)
				}
				var runs int
				if err := db.QueryRow("SELECT count(*) FROM runs").Scan(&runs); err != nil || runs != 0 {
					t.Fatalf("runs resurrected: %d err=%v", runs, err)
				}
				var transitions int
				if err := db.QueryRow("SELECT count(*) FROM agent_lifecycle_transition_facts").Scan(&transitions); err != nil || transitions != 1 {
					t.Fatalf("historical transition count=%d err=%v", transitions, err)
				}
			})
		}
	}
}
