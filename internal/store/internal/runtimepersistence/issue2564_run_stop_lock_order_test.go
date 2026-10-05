package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/lib/pq"
)

type issue2564RunStopOutcome struct {
	committed bool
	err       error
}

// Hold the exact run-authority row independently. NOWAIT distinguishes a header
// acquired by the waiting writer without fabricating a normal-stop deadlock;
// Story mutations also consume the existing global historical-order fence.
func TestIssue2564M24RunAuthorityPrecedesHeaderLockPostgres(t *testing.T) {
	backend := eventRecordContractBackend{name: "postgres", open: openPostgresAuthorActivityReceiptFixture}
	f := newConstructedGateFixtureForFields(t, backend, true, false)
	ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancel()
	command, before := issue2564RunStopWriterCommand(t, f)
	probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	holder, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	var holderPID int
	if err := holder.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holderPID); err != nil {
		t.Fatal(err)
	}
	if err := requirePostgresRunActive(ctx, holder, f.runID); err != nil {
		t.Fatal(err)
	}
	done := make(chan issue2564RunStopOutcome, 1)
	go func() {
		result, err := f.store.CommitWorkflowEngineMutation(ctx, command)
		done <- issue2564RunStopOutcome{result.Committed, err}
	}()
	joined := false
	defer func() {
		_ = holder.Rollback()
		if !joined {
			issue2564AwaitRunStopOutcome(t, ctx, done)
		}
	}()
	_, query := issue2564AwaitRunStopSQLWait(t, ctx, f.db, holderPID, done)
	if !issue2564RunLockQuery(query) {
		t.Errorf("ordinary writer waits on %q, want the run-authority lock", query)
	}
	if probe.Snapshot().Active != 1 {
		t.Error("run admission wait did not retain its native mutation transaction")
	}
	var revision int64
	headerErr := holder.QueryRowContext(ctx, `SELECT revision FROM flow_instances WHERE run_id=$1::uuid AND entity_id=$2::uuid FOR UPDATE NOWAIT`, f.runID, f.entityID).Scan(&revision)
	if headerErr != nil {
		var native *pq.Error
		if errors.As(headerErr, &native) && native.Code == "55P03" {
			t.Error("writer acquired the header while blocked on run authority; run-before-instance ordering is not preserved")
		} else {
			t.Errorf("probe the native header lock: %v", headerErr)
		}
	} else if revision != before.Revision {
		t.Errorf("waiting writer changed header revision: got %d want %d", revision, before.Revision)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	result := issue2564AwaitRunStopOutcome(t, ctx, done)
	joined = true
	if !result.committed || result.err != nil {
		t.Fatalf("ordinary writer after run-fence release: %+v", result)
	}
	if snapshot := probe.Snapshot(); snapshot.Active != 0 || snapshot.ByOperation[transactiontest.WorkflowMutation].WriteCommits != 1 {
		t.Fatalf("native writer did not join with one acknowledged commit: %+v", snapshot)
	}
}

func TestIssue2564M24RunStopWriterBothOrdersAndRollbackBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, first := range []string{"writer", "stop"} {
			for _, rollback := range []bool{false, true} {
				outcome := "commit"
				if rollback {
					outcome = "rollback"
				}
				t.Run(backend.name+"/"+first+"_first/"+outcome, func(t *testing.T) {
					f := newConstructedGateFixtureForFields(t, backend, true, false)
					ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
					defer cancel()
					command, before := issue2564RunStopWriterCommand(t, f)
					barrier := newForkContentionBarrier(t, backend.name, rollback)
					// Both operations contribute through the existing revision owner.
					// The SQL trigger holds an actual write transaction at that frontier.
					barrier.install(t, f.db, f.runID, "", "writer")
					defer barrier.release()
					defer barrier.resume()
					secondStore := f.store
					observer := f.db
					if backend.name == "sqlite" {
						var sequence int
						var database, path string
						if err := f.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &database, &path); err != nil {
							t.Fatal(err)
						}
						// A distinct native backend avoids merely queuing on the same
						// process-local SQLite mutation token or a one-connection pool.
						f.store = newBootstrappedSQLiteRuntimeStoreForPath(t, path)
						secondStore = barrier.observeSQLiteStore(t, f.store.(*SQLiteRuntimeStore), path)
						var err error
						observer, err = sql.Open("sqlite", path)
						if err != nil {
							t.Fatal(err)
						}
						defer observer.Close()
					}
					probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
					if err != nil {
						t.Fatal(err)
					}
					defer restore()
					var secondProbe *transactiontest.Collector
					if backend.name == "sqlite" {
						var restoreSecond func()
						secondProbe, restoreSecond, err = InstallTransactionProbeForTest(secondStore, transactiontest.Options{})
						if err != nil {
							t.Fatal(err)
						}
						defer restoreSecond()
					}
					baseline := snapshotForkHistoricalExecutionTables(t, observer, backend.name == "postgres")
					firstCall := func() issue2564RunStopOutcome { return issue2564RunStopWrite(ctx, f.store, command) }
					secondCtx := context.WithValue(ctx, forkContentionContextKey{}, barrier)
					secondCall := func() issue2564RunStopOutcome { return issue2564RunStopTerminal(secondCtx, secondStore, f.runID) }
					if first == "stop" {
						firstCall = func() issue2564RunStopOutcome { return issue2564RunStopTerminal(ctx, f.store, f.runID) }
						secondCall = func() issue2564RunStopOutcome { return issue2564RunStopWrite(secondCtx, secondStore, command) }
					}
					firstDone, secondDone := make(chan issue2564RunStopOutcome, 1), make(chan issue2564RunStopOutcome, 1)
					firstJoined, secondJoined, secondStarted := false, false, false
					go func() { firstDone <- firstCall() }()
					defer func() {
						barrier.release()
						barrier.resume()
						if !firstJoined {
							issue2564AwaitRunStopOutcome(t, ctx, firstDone)
						}
						if secondStarted && !secondJoined {
							issue2564AwaitRunStopOutcome(t, ctx, secondDone)
						}
					}()
					winnerPID := issue2564AwaitRunStopBarrier(t, ctx, observer, barrier, firstDone)
					// Queue an observer ahead of the contender at the winner's exact
					// run row, so rollback can be inspected before the loser proceeds.
					gate := barrier.queueGate(t, ctx, observer, f.runID, "stop", winnerPID)
					defer func() {
						if gate != nil {
							_ = gate.tx.Rollback()
						}
					}()
					secondStarted = true
					go func() { secondDone <- secondCall() }()
					if backend.name == "sqlite" {
						select {
						case <-barrier.busy:
						case result := <-secondDone:
							secondJoined = true
							t.Fatalf("contender escaped native SQLite BUSY/rollback: %+v", result)
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					} else {
						_, query := issue2564AwaitRunStopSQLWait(t, ctx, observer, winnerPID, secondDone)
						if !issue2564RunLockQuery(query) && !strings.Contains(query, "author_activity_order") {
							t.Errorf("%s contender waited below run/history authority: %q", map[string]string{"writer": "stop", "stop": "writer"}[first], query)
						}
					}
					if snapshot := probe.Snapshot(); snapshot.Active == 0 {
						t.Fatal("SQL barrier did not retain a native transaction")
					}
					barrier.release()
					firstResult := issue2564AwaitRunStopOutcome(t, ctx, firstDone)
					firstJoined = true
					if rollback {
						if firstResult.committed || firstResult.err == nil || !strings.Contains(firstResult.err.Error(), "h18_requested_rollback") {
							t.Fatalf("winner did not roll back its actual SQL writes: %+v", firstResult)
						}
					} else if !firstResult.committed || firstResult.err != nil {
						t.Fatalf("winner lacked native COMMIT acknowledgment: %+v", firstResult)
					}
					barrier.awaitGate(t, ctx, gate)
					// SQLite's BUSY observer retains no SQL locks after rollback;
					// PostgreSQL's queued run fence retains the same inspection cut.
					if rollback {
						if after := snapshotForkHistoricalExecutionTables(t, observer, backend.name == "postgres"); !reflect.DeepEqual(baseline, after) {
							t.Error("rolled-back winner leaked state, gate/card, mutation, story, revision or source evidence")
						}
					}
					barrier.openGate(t, gate)
					barrier.resume()
					secondResult := issue2564AwaitRunStopOutcome(t, ctx, secondDone)
					secondJoined = true
					if first == "stop" && !rollback {
						if secondResult.committed || !errors.Is(secondResult.err, runlifecycle.ErrRunNotActive) {
							t.Fatalf("run-stop winner did not refuse stale business/gate state: %+v", secondResult)
						}
					} else if !secondResult.committed || secondResult.err != nil {
						t.Fatalf("contender did not complete after SQL fence release: %+v", secondResult)
					}
					writerCommitted := first == "writer" && !rollback || first == "stop" && rollback
					stopCommitted := first == "writer" || !rollback
					issue2564AssertRunStopPreservation(t, f, before, writerCommitted, stopCommitted)
					for _, collector := range []*transactiontest.Collector{probe, secondProbe} {
						if collector != nil && collector.Snapshot().Active != 0 {
							t.Error("native transaction did not join after commit/refusal/rollback")
						}
					}
					if stopCommitted {
						stopped := snapshotForkHistoricalExecutionTables(t, observer, backend.name == "postgres")
						late := issue2564RunStopWrite(ctx, f.store, command)
						if late.committed || !errors.Is(late.err, runlifecycle.ErrRunNotActive) {
							t.Fatalf("stale replay resurrected stopped run authority: %+v", late)
						}
						if after := snapshotForkHistoricalExecutionTables(t, observer, backend.name == "postgres"); !reflect.DeepEqual(stopped, after) {
							t.Error("terminal refusal changed persisted application/history families")
						}
					}
				})
			}
		}
	}
}

func issue2564RunStopWriterCommand(t *testing.T, f forkContentionFixture) (pipeline.WorkflowEngineMutationCommand, pipeline.WorkflowInstance) {
	t.Helper()
	before, found, err := f.store.(workflowTestSelectedStore).LoadWorkflowInstance(f.ctx, f.state.Identity)
	if err != nil || !found {
		t.Fatalf("read constructed writer R1: found=%t err=%v", found, err)
	}
	state := f.state
	state.ExpectedRevision, state.ExpectedState = before.Revision, before.CurrentState
	state.Name, state.Fields = "writer-preserved", json.RawMessage(`{"name":"writer-preserved"}`)
	state.Accumulator, err = json.Marshal(before.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	state.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
	return pipeline.WorkflowEngineMutationCommand{State: state}, before
}

func issue2564RunStopWrite(ctx context.Context, selected snapshotOwnershipStore, command pipeline.WorkflowEngineMutationCommand) issue2564RunStopOutcome {
	result, err := selected.CommitWorkflowEngineMutation(ctx, command)
	return issue2564RunStopOutcome{result.Committed, err}
}

func issue2564RunStopTerminal(ctx context.Context, selected snapshotOwnershipStore, runID string) issue2564RunStopOutcome {
	owner := selected.(interface {
		MarkTerminalRun(context.Context, runlifecycle.TerminalRequest) (runlifecycle.Snapshot, runlifecycle.MutationDisposition, error)
	})
	_, disposition, err := owner.MarkTerminalRun(ctx, runlifecycle.TerminalRequest{RunID: runID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC().Truncate(time.Microsecond)})
	return issue2564RunStopOutcome{disposition == runlifecycle.MutationApplied, err}
}

func issue2564AwaitRunStopOutcome(t *testing.T, ctx context.Context, done <-chan issue2564RunStopOutcome) issue2564RunStopOutcome {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		t.Fatal("native run-stop/writer operation did not join: ", ctx.Err())
		return issue2564RunStopOutcome{}
	}
}

func issue2564AwaitRunStopSQLWait(t *testing.T, ctx context.Context, db *sql.DB, blocker int, done chan issue2564RunStopOutcome) (int, string) {
	t.Helper()
	var pid int
	var query string
	forkContentionPoll(t, ctx, func() bool {
		err := db.QueryRowContext(ctx, `SELECT pid, query FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) ORDER BY pid LIMIT 1`, blocker).Scan(&pid, &query)
		if err != nil && err != sql.ErrNoRows {
			t.Fatal(err)
		}
		select {
		case result := <-done:
			done <- result
			t.Fatalf("operation returned before native row-lock contention: %+v", result)
		default:
		}
		return err == nil
	})
	return pid, query
}

func issue2564RunLockQuery(query string) bool {
	query = strings.ToUpper(strings.Join(strings.Fields(query), " "))
	return strings.Contains(query, "FROM RUNS") && strings.Contains(query, "FOR UPDATE") && !strings.Contains(query, "FLOW_INSTANCES")
}

func issue2564AwaitRunStopBarrier(t *testing.T, ctx context.Context, db *sql.DB, barrier *forkContentionBarrier, done chan issue2564RunStopOutcome) int {
	t.Helper()
	if barrier.backend == "sqlite" {
		select {
		case <-barrier.entered:
			return 0
		case result := <-done:
			done <- result
			t.Fatalf("winner returned before native SQL barrier: %+v", result)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	var pid int
	forkContentionPoll(t, ctx, func() bool {
		err := db.QueryRowContext(ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory' AND objid=$1 AND NOT granted`, barrier.key).Scan(&pid)
		if err != nil && err != sql.ErrNoRows {
			t.Fatal(err)
		}
		select {
		case result := <-done:
			done <- result
			t.Fatalf("winner returned before native SQL barrier: %+v", result)
		default:
		}
		return err == nil
	})
	return pid
}

func issue2564AssertRunStopPreservation(t *testing.T, f forkContentionFixture, before pipeline.WorkflowInstance, writerCommitted, stopCommitted bool) {
	t.Helper()
	current, found, err := f.store.(workflowTestSelectedStore).LoadWorkflowInstance(f.ctx, f.state.Identity)
	if err != nil || !found {
		t.Fatalf("load current header and fields: found=%t err=%v", found, err)
	}
	wantName, wantRevision := "At R", before.Revision
	if writerCommitted {
		wantName = "writer-preserved"
		wantRevision++
	}
	if stopCommitted {
		wantRevision++
	}
	if current.Revision != wantRevision || current.CurrentState != before.CurrentState || current.Fields["name"] != wantName {
		t.Fatalf("lost business fields or phantom/reverted header revision: before=%+v after=%+v want name=%s revision=%d", before, current, wantName, wantRevision)
	}
	carrier, err := engine.StateCarrierFromPersisted(current.Fields, current.Bookkeeping, current.Gates, current.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	gate, found, err := gateruntime.Load(carrier.StateBuckets, ".", "review")
	if err != nil || !found || gate.CardID != f.cardID {
		t.Fatalf("lost exact canonical gate/card: %+v found=%t err=%v", gate, found, err)
	}
	wantStatus, wantGate := "running", gateruntime.StatusOpen
	if stopCommitted {
		wantStatus, wantGate = "cancelled", gateruntime.StatusSuperseded
	}
	var status string
	if err := f.db.QueryRowContext(f.ctx, `SELECT status FROM runs WHERE run_id=$1`, f.runID).Scan(&status); err != nil || status != wantStatus || gate.Status != wantGate {
		t.Fatalf("run/gate authority: status=%s gate=%s want=%s/%s err=%v", status, gate.Status, wantStatus, wantGate, err)
	}
	var effects int
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND domain='authored_field' AND path='name' AND CAST(new_value AS TEXT) LIKE '%writer-preserved%'`, f.runID, f.entityID).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	wantEffects := 0
	if writerCommitted {
		wantEffects = 1
	}
	if effects != wantEffects {
		t.Errorf("acknowledged/rolled-back business evidence=%d want=%d", effects, wantEffects)
	}
	_, postgres := f.store.(*PostgresStore)
	if decisionGateStatusMutationExists(t, f.ctx, f.db, postgres, f.runID, f.entityID, string(gateruntime.StatusSuperseded)) != stopCommitted {
		t.Error("run-freeze journal contribution disagrees with native COMMIT/rollback")
	}
	card, err := f.store.(workflowTestSelectedStore).GetDecisionCard(f.ctx, f.cardID)
	wantCard := decisioncard.StatusPending
	if stopCommitted {
		wantCard = decisioncard.StatusSuperseded
	}
	if err != nil || card.Status != wantCard {
		t.Errorf("atomic gate/card freeze: card=%s want=%s err=%v", card.Status, wantCard, err)
	}
}
