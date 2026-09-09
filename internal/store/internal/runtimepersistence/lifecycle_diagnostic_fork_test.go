package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestLifecycleDiagnosticForkLifetime(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, scenario := range []string{"delayed_close", "causal_delayed_close", "causal_discard", "discard_pending", "discard_projected", "discard_rollback", "discard_race", "historical_provenance_conflict", "ack_event_conflict", "ack_without_event"} {
				t.Run(scenario, func(t *testing.T) {
					selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
					fixture := newSelectedCompletionFixture(t, selected, db, sqlite)
					// This proof materializes its actor through the real lifecycle owner,
					// rather than the completion-only fixture's synthetic agent row.
					if _, err := db.Exec(`DELETE FROM agents WHERE run_id=$1`, fixture.forkRun); err != nil {
						t.Fatal(err)
					}
					ctx := testAuthorActivityContext()
					issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
					if err != nil {
						t.Fatal(err)
					}
					authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "diagnostic-proof", time.Minute)
					if err != nil {
						t.Fatal(err)
					}
					projector := selected.(diagnosticProjectionTestStore)
					origin := runtimemanager.LifecycleDiagnosticOrigin{
						Owner: runtimemanager.LifecycleDiagnosticSelectedFork, Causality: runtimemanager.LifecycleDiagnosticObservation,
						SelectedFork: authority.SelectedFork, SourceRunID: fixture.sourceRun, ForkEventID: fixture.eventID,
					}
					if strings.HasPrefix(scenario, "causal_") {
						parent := uuid.NewString()
						lineage, err := events.NewSelectedForkLineage(fixture.forkRun, fixture.sourceRun, fixture.eventID, runfork.RunForkSelectedContractExecutionOwner, "", "live")
						if err != nil {
							t.Fatal(err)
						}
						now := time.Now().UTC()
						event := eventtest.SelectedForkReplay(parent, events.EventType("selected.test"), eventtest.Producer(events.EventProducerPlatform, runfork.RunForkSelectedContractExecutionOwner), "", []byte(`{}`), 0, lineage, events.EventEnvelope{}, now)
						if err := commitSelectedForkEventFixture(ctx, selected.(selectedForkEventFixtureStore), event, runfork.RunForkSelectedContractExecutionLineage{
							ForkRunID: fixture.forkRun, SourceRunID: fixture.sourceRun, SourceEventID: fixture.eventID, ForkEventID: parent, EventName: "selected.test", SelectionAuthority: runfork.RunForkSelectedContractExecutionOwner, CreatedAt: now,
						}); err != nil {
							t.Fatal(err)
						}
						origin.Causality, origin.ParentEventID = runtimemanager.LifecycleDiagnosticAcceptedEvent, parent
					}
					item := enqueueDiagnosticTestIdentity(t, projector, mustTestAgentIdentityForRun(fixture.forkRun, "diagnostic-fork-worker", "global"), origin, executionmode.Live)
					if err := selected.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
						t.Fatal(err)
					}
					if err := selected.CloseRunForkSelectedContractRuntimeExecution(ctx, authority.ID); err != nil {
						t.Fatal(err)
					}
					if scenario == "historical_provenance_conflict" {
						if _, err := db.Exec(`UPDATE run_fork_selected_contract_runtime_executions SET actor_census_fingerprint='foreign' WHERE execution_id=$1`, authority.ID); err != nil {
							t.Fatal(err)
						}
						if _, err := projector.ProjectAgentLifecycleDiagnostics(ctx, 100); err == nil {
							t.Fatal("projected foreign historical execution")
						}
						assertDiagnosticCounts(t, db, item.OutboxID, 0, 1)
						return
					}
					if scenario == "delayed_close" || scenario == "causal_delayed_close" || scenario == "discard_projected" || strings.HasPrefix(scenario, "ack_") {
						if n, err := projector.ProjectAgentLifecycleDiagnostics(context.Background(), 100); err != nil || n != 1 {
							t.Fatalf("delayed projection n=%d err=%v", n, err)
						}
						assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
						if scenario == "ack_event_conflict" {
							if _, err := db.Exec(`UPDATE events SET payload='{}',payload_bytes=$2 WHERE event_id=$1`, item.OutboxID, []byte(`{}`)); err != nil {
								t.Fatal(err)
							}
						}
						if scenario == "ack_without_event" {
							if _, err := db.Exec(`DELETE FROM events WHERE event_id=$1`, item.OutboxID); err != nil {
								t.Fatal(err)
							}
						}
						var validationErr error
						if sqlite {
							s := selected.(*SQLiteRuntimeStore)
							validationErr = s.backend.RunTransaction(ctx, "validate diagnostic proof", func(ctx context.Context, tx *sql.Tx) error {
								_, err := s.agentSQLiteOwner.LifecycleObservationIDsTx(ctx, tx, fixture.forkRun)
								return err
							})
						} else {
							s := selected.(*PostgresStore)
							validationErr = s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
								if err := s.eventPostgresOwner.RequirePresentTx(ctx, tx, fixture.forkRun); err != nil {
									return err
								}
								_, err := s.agentPostgresOwner.LifecycleObservationIDsTx(ctx, tx, fixture.forkRun)
								return err
							})
						}
						if strings.HasPrefix(scenario, "ack_") {
							if validationErr == nil {
								t.Fatal("forged acknowledged observation accepted")
							}
							return
						}
						if validationErr != nil {
							t.Fatal(validationErr)
						}
						if scenario == "delayed_close" || scenario == "causal_delayed_close" {
							return
						}
					}
					if scenario == "discard_rollback" {
						if _, err := projector.ProjectAgentLifecycleDiagnostics(ctx, 100); err != nil {
							t.Fatal(err)
						}
						remove := installDiagnosticFailureTrigger(t, db, sqlite, "events", "DELETE", "")
						defer remove()
						if err := selected.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err == nil {
							t.Fatal("injected discard succeeded")
						}
						assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
						return
					}
					if scenario == "discard_race" {
						sink := selected.(interface {
							SetEventPayloadValidator(func(context.Context, string, []byte) error)
						})
						entered, release := make(chan struct{}), make(chan struct{})
						var once sync.Once
						sink.SetEventPayloadValidator(func(ctx context.Context, name string, _ []byte) error {
							if name == "platform.runtime_log" {
								once.Do(func() { close(entered); <-release })
							}
							return nil
						})
						projected, discarded := make(chan error, 1), make(chan error, 1)
						go func() { _, err := projector.ProjectAgentLifecycleDiagnostics(ctx, 100); projected <- err }()
						select {
						case <-entered:
						case <-time.After(5 * time.Second):
							close(release)
							t.Fatal("projector did not acquire run before event admission")
						}
						go func() { discarded <- selected.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun) }()
						close(release)
						for index, done := range []chan error{projected, discarded} {
							select {
							case err := <-done:
								if err != nil {
									var conflict *pq.Error
									if index != 1 || sqlite || !errors.As(err, &conflict) || conflict.Code != "40001" {
										t.Fatal(err)
									}
									assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
									// Serializable discard may lose to the committed projection.
									// The explicit caller retry must remove the intact aggregate.
									if err := selected.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err != nil {
										t.Fatal(err)
									}
								}
							case <-time.After(10 * time.Second):
								t.Fatal("projector/discard did not join")
							}
						}
						sink.SetEventPayloadValidator(nil)
					} else {
						if err := selected.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err != nil {
							t.Fatal(err)
						}
					}
					var rows int
					if err := db.QueryRow(`SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1`, fixture.forkRun).Scan(&rows); err != nil || rows != 0 {
						t.Fatalf("discard retained queue=%d err=%v", rows, err)
					}
					if n, err := projector.ProjectAgentLifecycleDiagnostics(ctx, 100); err != nil || n != 0 {
						t.Fatalf("discard resurrection n=%d err=%v", n, err)
					}
					assertDiagnosticCounts(t, db, item.OutboxID, 0, 0)
				})
			}
		})
	}
}
