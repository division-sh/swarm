package conformance

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// Schedule one old waiter after each competing admission, not after a fixed
// 30-second sleep. Every refill round includes its retry and a real peer commit.
type pressureReservationGate struct {
	target  chan pressureSoakTurnTiming
	peer    chan pressureSoakTurnTiming
	resume  chan struct{}
	recover chan struct{}
	done    chan struct{}
	once    sync.Once
	cleanup chan pressureReservationCleanup
}

type pressureReservationCleanup struct {
	Claim             fanoutobligation.Claim
	Started, Returned time.Time
	Err               error
}

func newPressureReservationGate() *pressureReservationGate {
	return &pressureReservationGate{
		target: make(chan pressureSoakTurnTiming, 1), peer: make(chan pressureSoakTurnTiming, 1),
		resume: make(chan struct{}), recover: make(chan struct{}), done: make(chan struct{}),
		cleanup: make(chan pressureReservationCleanup, 1),
	}
}

func (g *pressureReservationGate) close() { g.once.Do(func() { close(g.done) }) }

func (g *pressureReservationGate) afterAttempt(ctx context.Context, timing pressureSoakTurnTiming, granted, available bool) {
	if timing.ReserveSequence != 1 {
		if available {
			select {
			case g.peer <- timing:
			default:
			}
		}
		return
	}
	if granted {
		return
	}
	select {
	case g.target <- timing:
	case <-g.done:
		return
	case <-ctx.Done():
		return
	}
	select {
	case <-g.resume:
	case <-g.done:
	case <-ctx.Done():
	}
}

func waitPressureReservationTiming(t *testing.T, ch <-chan pressureSoakTurnTiming) pressureSoakTurnTiming {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(15 * time.Second):
		t.Fatal("finite reservation schedule lost the unchanged15s progress bound")
		return pressureSoakTurnTiming{}
	}
}

func resumePressureReservation(t *testing.T, g *pressureReservationGate) {
	t.Helper()
	select {
	case g.resume <- struct{}{}:
	case <-time.After(time.Second):
		t.Fatal("old reservation was not at its deterministic retry gate")
	}
}

func TestIssue2394PressureReservationStarvationPostgres(t *testing.T) {
	provePressureReservationSchedule(t, true)
}

func TestIssue2394PressureReservationFIFOControlPostgres(t *testing.T) {
	provePressureReservationSchedule(t, false)
}

func provePressureReservationSchedule(t *testing.T, barging bool) {
	t.Helper()
	const floor = 22
	c := newPressureSoakControl(floor, time.Second)
	c.allowReservationBarging = barging
	g := newPressureReservationGate()
	c.reservationGate = g
	defer c.logTiming(t)
	p := newSupplementalServingProbe(0)
	f := newSupplementalServingFixture(t, "postgres", p, func(inner startupownership.FanOutExecutor) startupownership.FanOutExecutor {
		return pressureSoakExecutor{FanOutExecutor: inner, control: c}
	})
	t.Cleanup(c.drain)
	t.Cleanup(g.close)
	_, run := f.startRun(t, 0)
	installSupplementalScanObserver(t, f, p, nil)
	f.runtimes[0].pipeline.InstallFanOutWorkNotifier(p)
	produced, finished := 0, 0
	add := func() {
		if produced >= 3*floor {
			t.Fatal("finite reservation diagnostic exceeded its66-intent ceiling")
		}
		submitPressureSoakDurableIntent(t, f, p, run, produced)
		produced++
		c.added(1)
	}
	for produced < floor {
		add()
	}
	old := waitPressureReservationTiming(t, g.target)
	if old.ReserveSequence != 1 || old.Claim.Generation != 1 || old.Claim.LeaseUntil.IsZero() {
		t.Fatalf("missing actual first claimed waiter: %+v", old)
	}
	c.startWindow()
	lastRetry := old.LastReserveAttempt
	var maxRetryGap time.Duration
	rounds := 0
	for time.Since(old.ReserveEntered) <= 31*time.Second {
		add()
		var peer pressureSoakTurnTiming
		if barging || rounds == 0 {
			peer = waitPressureReservationTiming(t, g.peer)
			if peer.ReserveSequence <= old.ReserveSequence || peer.ReserveGranted.IsZero() == barging {
				t.Fatalf("competing admission disagrees with fixture policy: barging=%t old=%+v peer=%+v", barging, old, peer)
			}
			resumePressureReservation(t, g)
			if barging {
				retry := waitPressureReservationTiming(t, g.target)
				if retry.ReserveSequence != old.ReserveSequence || !retry.ReserveGranted.IsZero() || !retry.LastReserveAttempt.After(peer.ReserveGranted) {
					t.Fatalf("old waiter unexpectedly acquired a reservation: %+v", retry)
				}
				if gap := retry.LastReserveAttempt.Sub(lastRetry); gap > maxRetryGap {
					maxRetryGap = gap
				}
				lastRetry = retry.LastReserveAttempt
				if maxRetryGap > 15*time.Second {
					t.Fatalf("diagnostic parked old waiter instead of making it retry: max_gap=%s", maxRetryGap)
				}
			}
		}
		r := waitSupplementalReceipt(t, p, 1)
		assertPressureSoakReceipt(t, r, run)
		if barging && r.Key == old.Claim.Key {
			t.Fatal("starved waiter committed during competing admissions")
		}
		if !barging && rounds == 0 {
			c.mu.Lock()
			first := c.longestReservation
			c.mu.Unlock()
			if r.Key != old.Claim.Key || first.ReserveSequence != old.ReserveSequence || first.NewestGrantBeforeThis != 0 || first.ReserveGranted.Before(peer.LastReserveAttempt) || !first.CommitEntered.Before(first.Claim.LeaseUntil) || first.reserveWait() > 15*time.Second {
				t.Fatalf("FIFO did not protect the old real claim from a later contender: first=%+v peer=%+v receipt=%+v", first, peer, r)
			}
			t.Logf("FIFO causal control: later contender denied=%+v old committed=%+v", peer, first)
		}
		finished++
		rounds++
		assertPressureSoakPopulation(t, f, floor, 2*floor)
		s, _ := c.snapshot()
		if s.Err != nil || s.Committed != finished || s.Produced-s.Committed != floor || s.Reserved != 0 || s.progressGap(time.Now()) > 15*time.Second || s.MaxCommitGap > 15*time.Second {
			t.Fatalf("finite companion progress/floor failed: %+v", s)
		}
		t.Logf("reservation round=%d barging=%t old_sequence=%d peer_sequence=%d peer_grant=%s peer_claim=%s actual_commit=%s old_retry=%s", rounds, barging, old.ReserveSequence, peer.ReserveSequence, peer.ReserveGranted.Format(time.RFC3339Nano), peer.ClaimReturned.Format(time.RFC3339Nano), r.CommitReturned.Format(time.RFC3339Nano), lastRetry.Format(time.RFC3339Nano))
	}
	if barging {
		assertPressureReservationState(t, f, old.Claim, true)
	}
	// The finite pressure window is now over. The original90s exact drain
	// remains; its first old turn must fail, then recover via a real new claim.
	c.drain()
	if barging {
		resumePressureReservation(t, g)
	}
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	failures, recovered := 0, false
	for finished < produced {
		select {
		case r := <-p.completed:
			if r.Err != nil {
				if !barging || failures != 0 || r.Key != old.Claim.Key || !errors.Is(r.Err, fanoutobligation.ErrStaleClaim) {
					t.Fatalf("unexpected finite diagnostic failure: %+v", r)
				}
				failures++
				c.mu.Lock()
				failed := c.firstFailedTurn
				c.mu.Unlock()
				if !errors.Is(failed.Err, fanoutobligation.ErrStaleClaim) || failed.reserveWait() <= 30*time.Second || !failed.ReserveGranted.After(failed.Claim.LeaseUntil) || !failed.CommitEntered.After(failed.Claim.LeaseUntil) || failed.NewestGrantBeforeThis <= failed.ReserveSequence || failed.CommittedAtGrant-failed.CommittedAtEntry < 2 {
					t.Fatalf("stale failure did not isolate postclaim gate residence: %+v", failed)
				}
				select {
				case cleanup := <-g.cleanup:
					if cleanup.Err != nil || cleanup.Claim != old.Claim || cleanup.Started.Before(failed.CommitReturned) || cleanup.Returned.Before(cleanup.Started) {
						t.Fatalf("expired exact-claim cleanup failed: %+v", cleanup)
					}
					t.Logf("actual expired-claim cleanup: %+v", cleanup)
				case <-time.After(time.Second):
					t.Fatal("expired turn returned without observed exact-claim cleanup")
				}
				assertPressureReservationState(t, f, old.Claim, false)
				t.Logf("expected fixture starvation proven: retries=%d max_retry_gap=%s failed=%+v", rounds, maxRetryGap, failed)
				close(g.recover)
				continue
			}
			if barging && r.Key == old.Claim.Key {
				if failures != 1 || recovered || r.Generation != 2 || !r.Result.Refill || r.Commits != 1 || r.Publications != 1 || r.Discovery.IsZero() || r.ClaimAt.Before(r.Discovery) || r.ClaimAt.Sub(r.Discovery) > time.Second || r.LoadedAt.Before(r.ClaimAt) || r.CommitAt.Before(r.LoadedAt) || r.CommitReturned.Before(r.CommitAt) || r.FinishedAt.Before(r.CommitReturned) {
					t.Fatalf("old intent did not recover through one real successor: %+v", r)
				}
				recovered = true
			} else {
				assertPressureSoakReceipt(t, r, run)
			}
			finished++
		case <-deadline.C:
			t.Fatalf("finite diagnostic failed original90s drain: produced=%d finished=%d stats=%+v", produced, finished, p.snapshot())
		}
	}
	if barging && (failures != 1 || !recovered) {
		t.Fatalf("missing expected stale rejection/recovery: failures=%d recovered=%t", failures, recovered)
	}
	assertSupplementalEffects(t, f, run, 0, produced, 1)
	s, _ := c.snapshot()
	stats := p.snapshot()
	wantTurns := produced
	if barging {
		wantTurns++
	}
	if barging && !errors.Is(s.Err, fanoutobligation.ErrStaleClaim) || !barging && (s.Err != nil || stats.Err != nil) || s.Committed != produced || s.Reserved != 0 || stats.Turns != wantTurns || stats.Finished != wantTurns || stats.Loaded != 0 || stats.PendingCommits != 0 {
		t.Fatalf("finite negative control final accounting: control=%+v serving=%+v", s, stats)
	}
	c.mu.Lock()
	longest, waiting := c.longestReservation, c.waitingCount
	c.mu.Unlock()
	if waiting != 0 || !barging && longest.reserveWait() > 15*time.Second {
		t.Fatalf("FIFO reservation bound/accounting: waiting=%d longest=%+v", waiting, longest)
	}
	t.Logf("finite reservation control exact drain: barging=%t intents=%d pressure_commits=%d max_retry_gap=%s max_actual_commit_gap=%s longest_reservation=%s", barging, produced, rounds, maxRetryGap, s.MaxCommitGap, longest.reserveWait())
}

func assertPressureReservationState(t *testing.T, f *servingMatrixFixture, claim fanoutobligation.Claim, held bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var cursor, outcomes, generation int
	var status string
	var owner sql.NullString
	var until, retry sql.NullTime
	key := claim.Key
	err := f.db.QueryRowContext(ctx, `SELECT i.cursor,i.status,i.claim_owner,i.claim_generation,i.lease_expires_at,i.retry_ready_at,
        (SELECT COUNT(*) FROM fan_out_outcomes o WHERE o.run_id=i.run_id AND o.triggering_delivery_id=i.triggering_delivery_id AND o.flow_path=i.flow_path AND o.declaration_family=i.declaration_family AND o.semantic_path=i.semantic_path)
        FROM fan_out_intents i WHERE i.run_id=$1 AND i.triggering_delivery_id=$2 AND i.flow_path=$3 AND i.declaration_family=$4 AND i.semantic_path=$5`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&cursor, &status, &owner, &generation, &until, &retry, &outcomes)
	validOwnership := !owner.Valid && !until.Valid
	if held {
		validOwnership = owner.Valid && owner.String == claim.Owner && until.Valid && until.Time.Equal(claim.LeaseUntil)
	}
	if err != nil || cursor != 0 || outcomes != 0 || status != "open" || uint64(generation) != claim.Generation || retry.Valid || !validOwnership || !time.Now().After(claim.LeaseUntil) {
		t.Fatalf("expired claim state before successor: held=%t cursor=%d outcomes=%d status=%s owner=%+v generation=%d until=%+v retry=%+v claim=%+v err=%v", held, cursor, outcomes, status, owner, generation, until, retry, claim, err)
	}
	t.Logf("expired claim readback at=%s held=%t cursor=%d outcomes=%d status=%s owner=%+v generation=%d lease_until=%+v retry=%+v key=%+v", time.Now().Format(time.RFC3339Nano), held, cursor, outcomes, status, owner, generation, until, retry, key)
}
