package conformance

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestFanOutServingM06GenerationRetirementOnBothBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, stage := range []struct {
				name  string
				phase servingMatrixPhase
			}{{"before_claim", servingMatrixClaim}, {"evaluation", servingMatrixEvaluation}, {"before_commit", servingMatrixCommit}} {
				t.Run(stage.name, func(t *testing.T) {
					retiring := newServingMatrixProbe(stage.phase, 1)
					retiring.commitWithoutCancel = true
					sibling := newServingMatrixProbe(servingMatrixUnheld, 0)
					f := newServingMatrixFixture(t, backend, nil, retiring, sibling)
					ctx, runID := f.startRun(t, 0)
					_, siblingRun := f.startRun(t, 1)
					trigger := f.submit(t, 0, runID, "retired-caller")
					turn := waitServingMatrixHeld(t, retiring)
					waitServingMatrixTriggerReceipt(t, f.db, trigger)
					before := readServingLifetimeState(t, f.db, runID)
					assertServingLifetimeUnissued(t, before)
					if stage.phase == servingMatrixCommit && (len(turn.command.Outcomes) != 1 || turn.command.Outcomes[0].Publication == nil) {
						t.Fatal("retirement commit gate lacks an actual evaluated publication")
					}
					grant := f.topology.grants[f.runtimes[0].sourceArtifactFact.BundleHash()]
					if err := grant.Retire(ctx); err != nil {
						t.Fatalf("retire real generation: %v", err)
					}
					// Close joins accepted turns, including this deliberately held caller.
					closed := make(chan struct{})
					go func() {
						f.runtimes[0].fanOutServing.Close()
						close(closed)
					}()
					join := beginServingLifetimeJoin(f.runtimes[0], nil)
					assertServingJoinBlocked(t, join)
					select {
					case <-closed:
						t.Fatal("registration Close returned before the held caller exited")
					default:
					}
					if f.runtimes[0].workOwner.ActiveCount() == 0 {
						t.Fatal("retirement recycled the held turn's exact runtime lease")
					}
					f.submit(t, 1, siblingRun, "unrelated-generation")
					if backend == "postgres" {
						proof := waitServingMatrixReceipt(t, sibling, time.Now().Add(5*time.Second))
						if proof.err != nil || !proof.result.Refill {
							t.Fatalf("retiring one generation stopped its sibling: %v", proof.err)
						}
					} else {
						select {
						case attempt := <-sibling.attempts:
							t.Fatalf("SQLite lent retiring generation's still-held permit: %+v", attempt)
						case <-time.After(200 * time.Millisecond):
						}
					}
					select {
					case <-closed:
						t.Fatal("registration Close abandoned the held caller during sibling progress")
					default:
					}
					turn.release()
					deadline := time.Now().Add(5 * time.Second)
					receipt := waitServingMatrixReceipt(t, retiring, deadline)
					assertServingRetiredTurn(t, receipt, stage.phase)
					if stage.phase != servingMatrixClaim && receipt.turn.cleanupErr != nil {
						t.Fatalf("retired generation could not discard its exact claim under still-live process possession: %v", receipt.turn.cleanupErr)
					}
					select {
					case <-closed:
					case <-time.After(time.Until(deadline)):
						t.Fatal("registration Close did not finish after the held caller exited")
					}
					assertServingJoinComplete(t, join, f.runtimes[0], nil)
					after := readServingLifetimeState(t, f.db, runID)
					assertServingLifetimeUnchanged(t, before, after)
					assertServingLifetimeClaimOwner(t, f.db, runID, "")
					f.assertSettled(t, 1, siblingRun, 1)
					assertServingRetiredRegistration(t, f.runtimes[0], retiring)
					t.Logf("M06/%s: exact grant retired, held lease joined only after caller exit, no semantic mutation, sibling generation remained executable", stage.name)
				})
			}
		})
	}
}

func TestFanOutServingM07ActualProcessLossJoinsHeldTurnOnBothBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			probe := newServingMatrixProbe(servingMatrixCommit, 1)
			probe.commitWithoutCancel = true
			f := newServingMatrixFixture(t, backend, nil, probe)
			ctx, runID := f.startRun(t, 0)
			trigger := f.submit(t, 0, runID, "lost-process-caller")
			turn := waitServingMatrixHeld(t, probe)
			waitServingMatrixTriggerReceipt(t, f.db, trigger)
			if len(turn.command.Outcomes) != 1 || turn.command.Outcomes[0].Publication == nil {
				t.Fatal("process-loss gate lacks a real publication command")
			}
			before := readServingLifetimeState(t, f.db, runID)
			assertServingLifetimeUnissued(t, before)
			assertServingMatrixContenderRefused(t, f, ctx)
			loseServingMatrixProcessPossession(t, backend, f.db)
			if err := f.topology.capability.ProveCurrent(ctx); err == nil {
				t.Fatal("actual lost selected-store possession still proved current")
			}
			select {
			case <-f.topology.capability.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("actual process loss did not retire the capability")
			}
			terminal, done := f.topology.capability.TerminalResult()
			if !done || terminal.Cause != startupownership.TerminalOwnershipUnprovable {
				t.Fatalf("process loss requires actual unprovable possession, not graceful release: %+v done=%t", terminal, done)
			}
			grant := f.topology.grants[f.runtimes[0].sourceArtifactFact.BundleHash()]
			select {
			case <-grant.Done():
			default:
				t.Fatal("process loss did not retire the exact serving grant")
			}
			process := conformanceTestProcessOwner(t)
			join := beginServingLifetimeJoin(f.runtimes[0], process)
			assertServingJoinBlocked(t, join)
			if f.runtimes[0].workOwner.ActiveCount() == 0 {
				t.Fatal("process loss recycled the held finite work lease")
			}
			turn.release()
			receipt := waitServingMatrixReceipt(t, probe, time.Now().Add(5*time.Second))
			assertServingRetiredTurn(t, receipt, servingMatrixCommit)
			if receipt.turn.cleanupErr == nil {
				t.Fatal("lost process possession still authorized a claim cleanup mutation")
			}
			assertServingJoinComplete(t, join, f.runtimes[0], process)
			after := readServingLifetimeState(t, f.db, runID)
			assertServingLifetimeUnchanged(t, before, after)
			assertServingLifetimeClaimOwner(t, f.db, runID, turn.claim.Owner)
			assertServingRetiredRegistration(t, f.runtimes[0], probe)
			assertServingLifetimeUnchanged(t, after, readServingLifetimeState(t, f.db, runID))
			t.Log("M07: real retained possession lost; actual evaluated mutation rejected even without caller cancellation; exact runtime+process joins waited for caller; no later admission or semantic mutation")
		})
	}
}

type servingLifetimeJoin struct {
	runtime *worklifetime.RuntimeRetirementReceipt
	process *worklifetime.ProcessJoinReceipt
	err     error
}

func beginServingLifetimeJoin(rt notifyAllChildrenRuntime, process *worklifetime.Process) <-chan servingLifetimeJoin {
	rt.workOwner.Retire()
	if process != nil {
		process.Retire()
	}
	done := make(chan servingLifetimeJoin, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdownErr := rt.manager.Shutdown()
		runtimeReceipt, err := rt.workOwner.RetireAndWait(ctx)
		result := servingLifetimeJoin{runtime: runtimeReceipt, err: errors.Join(shutdownErr, err)}
		if process != nil {
			result.process, err = process.Join(ctx)
			result.err = errors.Join(result.err, err)
		}
		done <- result
	}()
	return done
}

func assertServingJoinBlocked(t *testing.T, done <-chan servingLifetimeJoin) {
	t.Helper()
	select {
	case result := <-done:
		t.Fatalf("exact lifetime join returned before the held caller exited: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
}

func assertServingJoinComplete(t *testing.T, done <-chan servingLifetimeJoin, rt notifyAllChildrenRuntime, process *worklifetime.Process) {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil || result.runtime == nil || rt.workOwner.ActiveCount() != 0 {
			t.Fatalf("exact runtime join failed: receipt=%+v active=%d err=%v", result.runtime, rt.workOwner.ActiveCount(), result.err)
		}
		if process != nil {
			if err := process.ValidateJoinReceipt(result.process); err != nil {
				t.Fatalf("exact process join receipt: %v", err)
			}
			foreign := worklifetime.NewProcess()
			if err := foreign.ValidateJoinReceipt(result.process); err == nil {
				t.Fatal("process join receipt was accepted by a different owner")
			}
			if _, err := foreign.Join(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("exact lifetime join did not finish after held caller exit")
	}
}

func assertServingRetiredTurn(t *testing.T, receipt servingMatrixReceipt, phase servingMatrixPhase) {
	t.Helper()
	if receipt.err == nil || receipt.result.Refill {
		t.Fatalf("retired caller acquired a legal disposition: result=%+v err=%v", receipt.result, receipt.err)
	}
	if phase == servingMatrixCommit && receipt.turn.commitErr == nil {
		t.Fatal("actual generation-bound commit accepted the retired caller")
	}
	if phase == servingMatrixClaim {
		if receipt.turn.claim.Generation != 0 || len(receipt.turn.cleanup) != 0 {
			t.Fatalf("retired pre-claim caller acquired or cleaned an unowned claim: claim=%+v cleanup=%+v", receipt.turn.claim, receipt.turn.cleanup)
		}
	} else {
		if len(receipt.turn.cleanup) != 1 || receipt.turn.cleanup[0] != receipt.turn.claim {
			t.Fatalf("retired caller cleanup was not exactly its acknowledged claim: claim=%+v cleanup=%+v", receipt.turn.claim, receipt.turn.cleanup)
		}
		if receipt.turn.cleanupErr != nil && !errors.Is(receipt.err, receipt.turn.cleanupErr) {
			t.Fatalf("retired cleanup failure was dropped: cleanup=%v turn=%v", receipt.turn.cleanupErr, receipt.err)
		}
	}
}

func assertServingRetiredRegistration(t *testing.T, rt notifyAllChildrenRuntime, probe *servingMatrixProbe) {
	t.Helper()
	permit, available, err := rt.fanOutServing.BeginTurn(context.Background())
	if permit != nil {
		permit.Done()
	}
	if err == nil || available || permit != nil {
		t.Fatalf("retired registration admitted another turn: permit=%v available=%t err=%v", permit, available, err)
	}
	_, _, _, before := probe.snapshot()
	rt.fanOutServing.Wake()
	time.Sleep(1100 * time.Millisecond)
	_, _, _, after := probe.snapshot()
	if before != after {
		t.Fatalf("retired executor called after exact join: before=%d after=%d", before, after)
	}
}

type servingLifetimeState struct {
	cursor, outcomes, events, history, retry, generation int
	status                                               string
}

func readServingLifetimeState(t *testing.T, db *sql.DB, runID string) servingLifetimeState {
	t.Helper()
	var state servingLifetimeState
	if err := db.QueryRow(`SELECT cursor,status,claim_generation,CASE WHEN retry_ready_at IS NULL THEN 0 ELSE 1 END FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&state.cursor, &state.status, &state.generation, &state.retry); err != nil {
		t.Fatal(err)
	}
	for _, query := range []struct {
		sql string
		out *int
	}{
		{`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, &state.outcomes},
		{`SELECT COUNT(*) FROM events WHERE run_id=$1`, &state.events},
		{`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations'`, &state.history},
	} {
		if err := db.QueryRow(query.sql, runID).Scan(query.out); err != nil {
			t.Fatal(err)
		}
	}
	return state
}

func assertServingLifetimeUnissued(t *testing.T, state servingLifetimeState) {
	t.Helper()
	if state.cursor != 0 || state.outcomes != 0 || state.status != "open" || state.retry != 0 || state.history != 1 {
		t.Fatalf("lifetime gate did not preserve real unissued intent: %+v", state)
	}
}

func assertServingLifetimeUnchanged(t *testing.T, before, after servingLifetimeState) {
	t.Helper()
	if before != after {
		t.Fatalf("retired authority mutated semantic state: before=%+v after=%+v", before, after)
	}
}

func assertServingLifetimeClaimOwner(t *testing.T, db *sql.DB, runID, want string) {
	t.Helper()
	var owner string
	if err := db.QueryRow(`SELECT COALESCE(claim_owner,'') FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&owner); err != nil || owner != want {
		t.Fatalf("exact claim cleanup authority: owner=%q want=%q err=%v", owner, want, err)
	}
}

func assertServingMatrixContenderRefused(t *testing.T, f *servingMatrixFixture, ctx context.Context) {
	t.Helper()
	var before, after int
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM runtime_startup_authority_facts`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	contender, err := f.selected.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "fan-out-m07-contender", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	})
	if contender != nil {
		t.Cleanup(func() { _ = contender.Release(context.Background()) })
		t.Fatal("second ordinary process obtained a separate serving capability while the first held work")
	}
	var acquisition *startupownership.AcquisitionError
	if !errors.As(err, &acquisition) || acquisition.Failure != startupownership.AcquisitionTakeoverRequired {
		t.Fatalf("second ordinary process refusal lacks typed possession evidence: %v", err)
	}
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM runtime_startup_authority_facts`).Scan(&after); err != nil || after != before {
		t.Fatalf("refused competing process mutated authority history: before=%d after=%d err=%v", before, after, err)
	}
}

func loseServingMatrixProcessPossession(t *testing.T, backend string, db *sql.DB) {
	t.Helper()
	if backend == "postgres" {
		// Target the named retained process coordinate, not an unrelated run's
		// transaction/revision advisory lock in this same isolated database.
		rows, err := db.Query(`SELECT DISTINCT pid FROM pg_locks
			WHERE locktype='advisory' AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
			AND classid::bigint=CASE WHEN hashtext($1)<0 THEN 4294967295::bigint ELSE 0::bigint END
			AND objid::bigint=(hashtext($1)::bigint & 4294967295::bigint) AND objsubid=1`, "swarm:runtime:shared-store-owner")
		if err != nil {
			t.Fatal(err)
		}
		var pids []int
		for rows.Next() {
			var pid int
			if err := rows.Scan(&pid); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			pids = append(pids, pid)
		}
		err = rows.Err()
		rows.Close()
		if err != nil || len(pids) != 1 {
			t.Fatalf("isolated database must identify exactly one retained process session: pids=%v err=%v", pids, err)
		}
		var terminated bool
		if err := db.QueryRow(`SELECT pg_terminate_backend($1)`, pids[0]).Scan(&terminated); err != nil || !terminated {
			t.Fatalf("terminate exact retained process session: pid=%d terminated=%t err=%v", pids[0], terminated, err)
		}
		return
	}
	rows, err := db.Query(`PRAGMA database_list`)
	if err != nil {
		t.Fatal(err)
	}
	var path string
	for rows.Next() {
		var index int
		var name, file string
		if err := rows.Scan(&index, &name, &file); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if name == "main" {
			path = file
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil || path == "" || !filepath.IsAbs(path) {
		t.Fatalf("selected SQLite test database path unavailable: path=%q err=%v", path, err)
	}
	coordinate := path + ".possession"
	lost := coordinate + ".m07-lost"
	if err := os.Rename(coordinate, lost); err != nil {
		t.Fatalf("lose real SQLite retained possession coordinate: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Rename(lost, coordinate); err != nil {
			t.Errorf("restore test possession coordinate after loss proof: %v", err)
		}
	})
}

func waitServingMatrixTriggerReceipt(t *testing.T, db *sql.DB, eventID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var outcome string
		err := db.QueryRow(`SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, eventID).Scan(&outcome)
		if err == nil {
			if outcome != "success" {
				t.Fatalf("trigger did not receive a successful real pipeline receipt: %q", outcome)
			}
			return
		}
		if !errors.Is(err, sql.ErrNoRows) || time.Now().After(deadline) {
			t.Fatalf("trigger pipeline acknowledgement did not settle before retirement: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}
