package conformance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func TestIssue2394SupplementalPressureFiniteProbeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel() // Separate selected stores and pressure controls.
			proveSupplementalPressureSoak(t, backend, 3, 3*time.Second, 200*time.Millisecond)
		})
	}
}

func TestIssue2394TwentyTwoIntentPressureStartupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			// A22-commit refill at SQLite capacity1 needs more than4.4s
			// even before actual transaction and handoff time.
			proveSupplementalPressureSoak(t, backend, 22, 10*time.Second, 200*time.Millisecond)
		})
	}
}

// Keep the historical batch-release stall as an explicit negative control.
// Both cases submit the same 22 durable arrivals with the same finite pacing;
// only notification of already durable work differs.
func TestIssue2394PressureDurableArrivalAccountingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			for _, incremental := range []bool{false, true} {
				t.Run(fmt.Sprintf("incremental=%t", incremental), func(t *testing.T) {
					provePressureDurableArrivalAccounting(t, backend, incremental)
				})
			}
		})
	}
}

func provePressureDurableArrivalAccounting(t *testing.T, backend string, incremental bool) {
	t.Helper()
	const floor = 22
	c := newPressureSoakControl(floor, 20*time.Millisecond)
	defer c.logTiming(t)
	p := newSupplementalServingProbe(0)
	f := newSupplementalServingFixture(t, backend, p, func(inner startupownership.FanOutExecutor) startupownership.FanOutExecutor {
		return pressureSoakExecutor{FanOutExecutor: inner, control: c}
	})
	t.Cleanup(c.drain)
	_, run := f.startRun(t, 0)
	installSupplementalScanObserver(t, f, p, nil)
	f.runtimes[0].pipeline.InstallFanOutWorkNotifier(p)
	for batch := 0; batch < 2*floor; batch++ {
		submitPressureSoakDurableIntent(t, f, p, run, batch)
		c.added(1)
	}
	finished := 0
	for finished < floor {
		assertPressureSoakReceipt(t, waitSupplementalReceipt(t, p, 1), run)
		finished++
	}
	before, _ := c.snapshot()
	if before.Committed != floor || before.Reserved != 0 || before.Err != nil {
		t.Fatalf("finite refill did not reach the unchanged pressure floor: %+v", before)
	}
	started := c.startWindow()
	var firstRefill supplementalReceipt
	consume := func(r supplementalReceipt) {
		assertPressureSoakReceipt(t, r, run)
		if firstRefill.CommitReturned.IsZero() {
			firstRefill = r
		}
		finished++
	}
	for batch := 2 * floor; batch < 3*floor; batch++ {
		// A slow external producer isolates the batch gate without needing
		// ten minutes of accumulated history or changing production code.
		time.Sleep(700 * time.Millisecond)
		submitPressureSoakDurableIntent(t, f, p, run, batch)
		if incremental {
			c.added(1)
		}
		assertPressureSoakPopulation(t, f, floor, 2*floor)
	drainReceipts:
		for {
			select {
			case r := <-p.completed:
				consume(r)
			default:
				break drainReceipts
			}
		}
		s, _ := c.snapshot()
		if s.Err != nil || p.snapshot().Err != nil {
			t.Fatalf("finite refill serving failure: control=%+v serving=%+v", s, p.snapshot())
		}
		if incremental && (s.progressGap(time.Now()) > 15*time.Second || s.MaxCommitGap > 15*time.Second) {
			t.Fatalf("incrementally observed durable arrivals lost actual progress: %+v", s)
		}
		if !incremental && (s.Committed != floor || s.Reserved != 0) {
			t.Fatalf("batch-release negative control unexpectedly allowed a commit: %+v", s)
		}
	}
	after, _ := c.snapshot()
	if incremental {
		if after.Committed <= floor || firstRefill.CommitReturned.IsZero() {
			t.Fatalf("incremental refill made no actual progress during arrivals: %+v", after)
		}
	} else {
		if time.Since(started) <= 15*time.Second || !after.LastCommitAt.Equal(before.LastCommitAt) {
			t.Fatalf("batch-release negative control did not reproduce the15s gap: before=%+v after=%+v", before, after)
		}
		t.Logf("expected fixture-only no-progress reproduced: refill=%s control=%+v;22 newly durable intents not yet announced", time.Since(started), after)
		c.added(floor)
		consume(waitSupplementalReceipt(t, p, 1))
		if firstRefill.CommitReturned.Sub(before.LastCommitAt) <= 15*time.Second {
			t.Fatal("batch-release receipt did not corroborate the actual commit gap")
		}
	}
	t.Logf("finite refill backend=%s incremental=%t previous_commit=%s refill_start=%s first_discovery=%s first_claim=%s first_commit_entry=%s first_commit_return=%s actual_commit_gap=%s", backend, incremental, before.LastCommitAt.Format(time.RFC3339Nano), started.Format(time.RFC3339Nano), firstRefill.Discovery.Format(time.RFC3339Nano), firstRefill.ClaimAt.Format(time.RFC3339Nano), firstRefill.CommitAt.Format(time.RFC3339Nano), firstRefill.CommitReturned.Format(time.RFC3339Nano), firstRefill.CommitReturned.Sub(before.LastCommitAt))
	c.drain()
	for finished < 3*floor {
		consume(waitSupplementalReceipt(t, p, 1))
	}
	assertSupplementalEffects(t, f, run, 0, 3*floor, 1)
	s, _ := c.snapshot()
	stats := p.snapshot()
	if s.Err != nil || s.Produced != 3*floor || s.Committed != 3*floor || s.Reserved != 0 || stats.Err != nil || stats.Turns != 3*floor || stats.Finished != 3*floor || stats.DroppedWakes != 3*floor || stats.FoundScans != 3*floor || stats.Loaded != 0 || stats.PendingCommits != 0 {
		t.Fatalf("finite refill exact final accounting: control=%+v serving=%+v", s, stats)
	}
}

type pressureSoakSnapshot struct {
	Produced, Committed, Reserved int
	Draining                      bool
	Err                           error
	WindowStarted, LastCommitAt   time.Time
	MaxCommitGap                  time.Duration
}

// Keep only the longest completed reservation and first failed turn. Each
// active worker owns one local record; evidence never grows with history.
type pressureSoakTurnTiming struct {
	ReserveSequence, NewestGrantBeforeThis uint64
	ReserveAttempts                        int
	LastReserveAttempt                     time.Time
	Claim                                  fanoutobligation.Claim
	ClaimStarted, ClaimReturned            time.Time
	ReserveEntered, ReserveGranted         time.Time
	CommitEntered, CommitReturned          time.Time
	ProducedAtEntry, ProducedAtGrant       int
	CommittedAtEntry, CommittedAtGrant     int
	Err                                    error
}

func (r pressureSoakTurnTiming) reserveWait() time.Duration {
	if r.ReserveGranted.IsZero() {
		return 0
	}
	return r.ReserveGranted.Sub(r.ReserveEntered)
}

// This is a test-only commit-delay fault, not a candidate or claim scheduler.
// Reservations prevent the real writer from draining below the pressure floor
// while the external producer forms its next bounded wave via ordinary ingress.
// All selection, claims, authority checks and writes remain production-owned.
type pressureSoakControl struct {
	mu                      sync.Mutex
	state                   pressureSoakSnapshot
	changed                 chan struct{}
	floor                   int
	delay                   time.Duration
	longestReservation      pressureSoakTurnTiming
	firstFailedTurn         pressureSoakTurnTiming
	reservationEntries      uint64
	newestGrantedEntry      uint64
	reservationGate         *pressureReservationGate
	allowReservationBarging bool
	waiting                 [4]uint64
	waitingCount            int
}

func newPressureSoakControl(floor int, delay time.Duration) *pressureSoakControl {
	return &pressureSoakControl{floor: floor, delay: delay, changed: make(chan struct{})}
}

func (c *pressureSoakControl) notifyLocked() { close(c.changed); c.changed = make(chan struct{}) }

func (c *pressureSoakControl) snapshot() (pressureSoakSnapshot, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state, c.changed
}

func (c *pressureSoakControl) recordTiming(r pressureSoakTurnTiming) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.longestReservation.ReserveEntered.IsZero() || r.reserveWait() > c.longestReservation.reserveWait() {
		c.longestReservation = r
	}
	if r.Err != nil && c.firstFailedTurn.Err == nil {
		c.firstFailedTurn = r
	}
}

func (c *pressureSoakControl) logTiming(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	longest, failed := c.longestReservation, c.firstFailedTurn
	c.mu.Unlock()
	t.Logf("pressure longest completed reservation wait=%s chronology=%+v", longest.reserveWait(), longest)
	if failed.Err != nil {
		t.Logf("pressure first failed commit reserve_wait=%s chronology=%+v", failed.reserveWait(), failed)
	}
}

func (c *pressureSoakControl) added(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Produced += n
	c.notifyLocked()
}

func (c *pressureSoakControl) startWindow() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.WindowStarted = time.Now()
	c.state.MaxCommitGap = 0
	return c.state.WindowStarted
}

func (s pressureSoakSnapshot) progressGap(now time.Time) time.Duration {
	last := s.LastCommitAt
	if last.Before(s.WindowStarted) {
		last = s.WindowStarted
	}
	return now.Sub(last)
}

func (c *pressureSoakControl) drain() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Draining = true
	c.notifyLocked()
}

func (c *pressureSoakControl) reserve(ctx context.Context, timing *pressureSoakTurnTiming) error {
	c.mu.Lock()
	c.reservationEntries++
	timing.ReserveSequence = c.reservationEntries
	timing.ReserveEntered = time.Now()
	timing.ProducedAtEntry, timing.CommittedAtEntry = c.state.Produced, c.state.Committed
	if !c.allowReservationBarging {
		if c.waitingCount == len(c.waiting) {
			c.mu.Unlock()
			return fmt.Errorf("pressure fixture exceeded its four real serving waiters")
		}
		c.waiting[c.waitingCount] = timing.ReserveSequence
		c.waitingCount++
		defer c.removeWaiter(timing.ReserveSequence)
	}
	c.mu.Unlock()
	for {
		c.mu.Lock()
		timing.ReserveAttempts++
		timing.LastReserveAttempt = time.Now()
		available := c.state.Draining || c.state.Produced-c.state.Committed-c.state.Reserved > c.floor
		if available && (c.allowReservationBarging || c.waiting[0] == timing.ReserveSequence) {
			c.state.Reserved++
			timing.ReserveGranted = time.Now()
			timing.ProducedAtGrant, timing.CommittedAtGrant = c.state.Produced, c.state.Committed
			// A larger sequence already granted proves a later entrant passed
			// this waiter; it does not change the existing reservation policy.
			timing.NewestGrantBeforeThis = c.newestGrantedEntry
			if timing.ReserveSequence > c.newestGrantedEntry {
				c.newestGrantedEntry = timing.ReserveSequence
			}
			c.mu.Unlock()
			if c.reservationGate != nil {
				c.reservationGate.afterAttempt(ctx, *timing, true, available)
			}
			return nil
		}
		changed := c.changed
		c.mu.Unlock()
		if c.reservationGate != nil {
			c.reservationGate.afterAttempt(ctx, *timing, false, available)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// FIFO applies only to this artificial postclaim delay. Production candidate
// selection and claims remain untouched. Cancellation removes its exact ticket.
func (c *pressureSoakControl) removeWaiter(sequence uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.waitingCount; i++ {
		if c.waiting[i] == sequence {
			copy(c.waiting[i:], c.waiting[i+1:c.waitingCount])
			c.waitingCount--
			c.waiting[c.waitingCount] = 0
			c.notifyLocked()
			return
		}
	}
}

func (c *pressureSoakControl) finished(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Reserved--
	if err == nil {
		now := time.Now()
		if !c.state.WindowStarted.IsZero() && !c.state.Draining {
			if gap := c.state.progressGap(now); gap > c.state.MaxCommitGap {
				c.state.MaxCommitGap = gap
			}
		}
		c.state.LastCommitAt = now
		c.state.Committed++
	} else if c.state.Err == nil {
		c.state.Err = err
	}
	c.notifyLocked()
}

type pressureSoakExecutor struct {
	startupownership.FanOutExecutor
	control *pressureSoakControl
}

func (e pressureSoakExecutor) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	o := &pressureSoakOwner{FanOutObligationOwner: owner, control: e.control}
	result, err := e.FanOutExecutor.ServeFanOutCandidate(ctx, o, key)
	if e.control.reservationGate != nil && o.timing.ReserveSequence == 1 && err != nil {
		// Retain the actual serving permit until the finite negative control
		// reads the failed claim; no successor can alter that evidence.
		select {
		case <-e.control.reservationGate.recover:
		case <-e.control.reservationGate.done:
		case <-ctx.Done():
		}
	}
	return result, err
}

type pressureSoakOwner struct {
	pipeline.FanOutObligationOwner
	control *pressureSoakControl
	timing  pressureSoakTurnTiming
}

func (o *pressureSoakOwner) ClaimFanOutIntent(ctx context.Context, req pipeline.FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	o.timing.ClaimStarted = time.Now()
	i, claim, found, err := o.FanOutObligationOwner.ClaimFanOutIntent(ctx, req)
	o.timing.ClaimReturned = time.Now()
	if found && err == nil {
		o.timing.Claim = claim
	}
	return i, claim, found, err
}

func (o *pressureSoakOwner) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	return o.FanOutObligationOwner.BeginFanOutPublicationGroup(ctx, claim)
}

func (o *pressureSoakOwner) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutClaimSettlement, error) {
	started := time.Now()
	settlement, err := o.FanOutObligationOwner.ReleaseFanOutClaim(ctx, claim)
	if g := o.control.reservationGate; g != nil && o.timing.ReserveSequence == 1 {
		select {
		case g.cleanup <- pressureReservationCleanup{Claim: claim, Started: started, Returned: time.Now(), Err: err}:
		case <-g.done:
		case <-ctx.Done():
		}
	}
	return settlement, err
}

func (o *pressureSoakOwner) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (out pipeline.CommittedFanOutChunk, err error) {
	if len(command.Outcomes) != 1 || command.Outcomes[0].Publication == nil {
		return pipeline.CommittedFanOutChunk{}, fmt.Errorf("pressure soak requires one real publication per replenishable intent")
	}
	c := o.control
	reserved := false
	defer func() {
		o.timing.Err = err
		// Preserve the failing turn before exposing its error to the producer,
		// whose wake check can fail before consuming the completion mailbox.
		c.recordTiming(o.timing)
		if reserved {
			c.finished(err)
		}
	}()
	if err := c.reserve(ctx, &o.timing); err != nil {
		return pipeline.CommittedFanOutChunk{}, err
	}
	reserved = true
	// The claim keeps its real30s lease. Delay is injected before entering the
	// real selected mutation; no transaction, timestamp or lease is fabricated.
	wait := time.NewTimer(c.delay)
	select {
	case <-wait.C:
	case <-ctx.Done():
		wait.Stop()
		return pipeline.CommittedFanOutChunk{}, ctx.Err()
	}
	o.timing.CommitEntered = time.Now()
	out, err = o.FanOutObligationOwner.CommitFanOutChunk(ctx, command)
	o.timing.CommitReturned = time.Now()
	return out, err
}

func submitPressureSoakDurableIntent(t *testing.T, f *servingMatrixFixture, p *supplementalServingProbe, run string, batch int) {
	t.Helper()
	submitSupplementalIntent(t, f, run, batch, 1)
	waitServingMatrixIntentCount(t, f, batch+1)
	requireSupplementalWake(t, p, batch+1)
}

func proveSupplementalPressureSoak(t *testing.T, backend string, floor int, span, pacing time.Duration) {
	t.Helper()
	workers := 1
	if backend == "postgres" {
		workers = 4
	}
	// Fixed evidence/durable-work ceiling, derived from the injected per-turn
	// minimum delay plus initial/replenishment/drain slack. Never grow a trace.
	maxProduced := workers*(int(span/pacing)+1) + 3*floor
	c := newPressureSoakControl(floor, pacing)
	defer c.logTiming(t)
	p := newSupplementalServingProbe(0)
	f := newSupplementalServingFixture(t, backend, p, func(inner startupownership.FanOutExecutor) startupownership.FanOutExecutor {
		return pressureSoakExecutor{FanOutExecutor: inner, control: c}
	})
	t.Cleanup(c.drain)
	_, run := f.startRun(t, 0)
	// Let the real selector claim during initial ingress. Holding an empty-scan
	// callback while44 intents arrive would itself violate the1s opportunity
	// bound. reserve already holds commits until the population is registered.
	installSupplementalScanObserver(t, f, p, nil)
	f.runtimes[0].pipeline.InstallFanOutWorkNotifier(p)
	produced, waves, finished := 0, 0, 0
	consume := func(r supplementalReceipt) {
		assertPressureSoakReceipt(t, r, run)
		finished++
	}
	addWave := func(n int) {
		if produced+n > maxProduced {
			t.Fatalf("bounded pressure workload exceeded explicit ceiling%d", maxProduced)
		}
		for i := 0; i < n; i++ {
			submitPressureSoakDurableIntent(t, f, p, run, produced)
			produced++
			// Release only newly observed durable work, without holding the
			// writer at the floor until the entire ingress wave completes.
			c.added(1)
		drainReceipts:
			for {
				select {
				case r := <-p.completed:
					consume(r)
				default:
					break drainReceipts
				}
			}
		}
		waves++
	}
	// Two bounded waves establish pressure plus one consumable wave. A single
	// wave alone would merely park all workers at the floor without progress.
	addWave(2 * floor)
	assertPressureSoakPopulation(t, f, floor, 2*floor)
	started := c.startWindow()
	end := started.Add(span)
	observations := time.NewTicker(time.Second)
	defer observations.Stop()
	deadline := time.NewTimer(span)
	defer deadline.Stop()
	lastLog := started
	samples := 0
	for time.Now().Before(end) {
		s, changed := c.snapshot()
		if s.Err != nil || p.snapshot().Err != nil {
			t.Fatalf("pressure serving failed: control=%+v serving=%+v", s, p.snapshot())
		}
		// Fold actual commit-return times even while synchronous ingress is
		// running; observing a later commit must not erase an earlier gap.
		if s.progressGap(time.Now()) > 15*time.Second || s.MaxCommitGap > 15*time.Second {
			t.Fatalf("no actual committed progress for15s under continuous pressure: %+v", s)
		}
		if s.Produced-s.Committed == floor {
			assertPressureSoakPopulation(t, f, floor, 2*floor)
			addWave(floor)
			assertPressureSoakPopulation(t, f, floor, 2*floor)
			continue
		}
		select {
		case r := <-p.completed:
			consume(r)
		case <-changed:
		case <-observations.C:
			assertPressureSoakPopulation(t, f, floor, 2*floor)
			samples++
			if time.Since(lastLog) >= 15*time.Second {
				logSupplementalState(t, f, p, fmt.Sprintf("pressure elapsed=%s resident_floor=%d resident_ceiling=%d waves=%d", time.Since(started), floor, 2*floor, waves))
				lastLog = time.Now()
			}
		case <-deadline.C:
		}
	}
	// Check real unfinished work at the end of the entire interval, then permit
	// the existing writer to drain. No cancellation or semantic tail deletion.
	assertPressureSoakPopulation(t, f, floor, 2*floor)
	s, _ := c.snapshot()
	if s.progressGap(time.Now()) > 15*time.Second || s.MaxCommitGap > 15*time.Second {
		t.Fatalf("no actual committed progress for15s under continuous pressure: %+v", s)
	}
	if time.Since(started) < span || waves < 2 || s.Committed < floor || samples == 0 {
		t.Fatalf("ongoing rolling pressure not established: elapsed=%s waves=%d samples=%d control=%+v", time.Since(started), waves, samples, s)
	}
	logSupplementalState(t, f, p, "pressure_window_complete_before_exact_drain")
	c.drain()
	drainDeadline := time.NewTimer(90 * time.Second)
	defer drainDeadline.Stop()
	for finished < produced {
		select {
		case r := <-p.completed:
			consume(r)
		case <-drainDeadline.C:
			t.Fatalf("bounded pressure tail failed exact drain: produced=%d finished=%d stats=%+v", produced, finished, p.snapshot())
		}
	}
	assertSupplementalEffects(t, f, run, 0, produced, 1)
	s, _ = c.snapshot()
	stats := p.snapshot()
	if s.Err != nil || s.Produced != produced || s.Committed != produced || s.Reserved != 0 || !s.Draining || stats.Err != nil || stats.Turns != produced || stats.Finished != produced || stats.DroppedWakes != produced || stats.FoundScans != produced || stats.Loaded != 0 || stats.PendingCommits != 0 {
		t.Fatalf("pressure exact final accounting: control=%+v serving=%+v expected=%d", s, stats, produced)
	}
	t.Logf("M32 sustained resident pressure backend=%s window=%s elapsed_with_drain=%s unfinished=%d..%d waves=%d intents=%d bounded_ceiling=%d samples=%d explicit_commit_delay=%s max_actual_commit_gap=%s stats=%+v; exact final effects and0->1 histories", backend, span, time.Since(started), floor, 2*floor, waves, produced, maxProduced, samples, pacing, s.MaxCommitGap, stats)
}

func assertPressureSoakReceipt(t *testing.T, r supplementalReceipt, run string) {
	t.Helper()
	if r.Err != nil || !r.Result.Refill || r.Key.RunID != run || r.Generation != 1 || r.Commits != 1 || r.Publications != 1 || r.Discovery.IsZero() || r.ClaimAt.Before(r.Discovery) || r.ClaimAt.Sub(r.Discovery) > time.Second || r.LoadedAt.Before(r.ClaimAt) || r.CommitAt.Before(r.LoadedAt) || r.CommitReturned.Before(r.CommitAt) || r.FinishedAt.Before(r.CommitReturned) {
		t.Fatalf("pressure turn lost actual phase/claim/publication evidence: %+v", r)
	}
}

func assertPressureSoakPopulation(t *testing.T, f *servingMatrixFixture, floor, ceiling int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var unfinished, owed, retry, blocked int
	err := f.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN cursor<cardinality THEN 1 ELSE 0 END),0),COALESCE(SUM(cardinality-cursor),0),COALESCE(SUM(CASE WHEN retry_ready_at IS NOT NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END),0) FROM fan_out_intents`).Scan(&unfinished, &owed, &retry, &blocked)
	if err != nil || unfinished < floor || unfinished > ceiling || owed != unfinished || retry != 0 || blocked != 0 {
		t.Fatalf("durable pressure invariant: unfinished=%d owed=%d want=%d..%d retry=%d blocked=%d err=%v", unfinished, owed, floor, ceiling, retry, blocked, err)
	}
}
