package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type supplementalDuration struct {
	Count      int
	Total, Max time.Duration
}

func (d *supplementalDuration) add(v time.Duration) {
	d.Count++
	d.Total += v
	if v > d.Max {
		d.Max = v
	}
}

type supplementalStats struct {
	Turns, Finished, Loaded, PeakLoaded, PendingCommits, PeakPendingCommits int
	Scans, EmptyScans, FoundScans, DroppedWakes                             int
	LastEmpty, LastFound, LastClaim, LastCommit, LastWake                   time.Time
	Claim, ClaimToLoaded, Planning, Commit, Handoff, TestHold               supplementalDuration
	GroupAdmission                                                          supplementalDuration
	Err                                                                     error
}

type supplementalReceipt struct {
	Key                                                                fanoutobligation.IntentKey
	Discovery, ClaimAt, LoadedAt, CommitAt, CommitReturned, FinishedAt time.Time
	ClaimDuration, Hold                                                time.Duration
	Generation                                                         uint64
	Commits, Publications                                              int
	Result                                                             pipeline.FanOutTurnResult
	Err                                                                error
}

type supplementalPhase struct {
	Key  fanoutobligation.IntentKey
	Name string
}

// No append-only trace: live keys are bounded by process capacity, completion
// evidence has a fixed mailbox, and long-running observations are scalar folds.
type supplementalServingProbe struct {
	*pipeline.PipelineCoordinator
	mu                   sync.Mutex
	stats                supplementalStats
	active               map[fanoutobligation.IntentKey]bool
	discoveries          map[fanoutobligation.IntentKey]time.Time
	completed            chan supplementalReceipt
	phases               chan supplementalPhase
	holdFirst            int
	loadGate, commitGate chan struct{}
	loadOnce, commitOnce sync.Once
}

func newSupplementalServingProbe(holdFirst int) *supplementalServingProbe {
	return &supplementalServingProbe{
		active: make(map[fanoutobligation.IntentKey]bool), discoveries: make(map[fanoutobligation.IntentKey]time.Time),
		completed: make(chan supplementalReceipt, 32), phases: make(chan supplementalPhase, 8),
		holdFirst: holdFirst, loadGate: make(chan struct{}), commitGate: make(chan struct{}),
	}
}

func (p *supplementalServingProbe) snapshot() supplementalStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

func (p *supplementalServingProbe) failLocked(err error) {
	if p.stats.Err == nil {
		p.stats.Err = err
	}
}

func (p *supplementalServingProbe) releaseLoaded()  { p.loadOnce.Do(func() { close(p.loadGate) }) }
func (p *supplementalServingProbe) releaseCommits() { p.commitOnce.Do(func() { close(p.commitGate) }) }
func (p *supplementalServingProbe) releaseAll()     { p.releaseLoaded(); p.releaseCommits() }

func (p *supplementalServingProbe) scan(candidate startupownership.FanOutCandidate, found bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats.Scans++
	if err != nil {
		p.failLocked(err)
		return
	}
	if !found {
		p.stats.EmptyScans++
		p.stats.LastEmpty = time.Now()
		return
	}
	p.stats.FoundScans++
	p.stats.LastFound = time.Now()
	if len(p.discoveries) >= 8 {
		p.failLocked(fmt.Errorf("selector evidence exceeded fixed live-key bound"))
		return
	}
	p.discoveries[candidate.Key] = p.stats.LastFound
}

// Wake is deliberately not forwarded. Only real production recovery/refill can
// serve arrivals while this post-commit notification fault is installed.
func (p *supplementalServingProbe) Wake() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats.DroppedWakes++
	p.stats.LastWake = time.Now()
}

func (p *supplementalServingProbe) ReportFanOutServingError(ctx context.Context, err error) {
	p.mu.Lock()
	p.failLocked(err)
	p.mu.Unlock()
	p.PipelineCoordinator.ReportFanOutServingError(ctx, err)
}

func (p *supplementalServingProbe) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	p.mu.Lock()
	p.stats.Turns++
	if p.active[key] || len(p.active) >= 4 {
		p.failLocked(fmt.Errorf("duplicate/excess actual serving key: %+v", key))
	}
	p.active[key] = true
	turn := &supplementalServingTurn{FanOutObligationOwner: owner, probe: p, held: p.stats.Turns <= p.holdFirst,
		receipt: supplementalReceipt{Key: key, Discovery: p.discoveries[key]}}
	delete(p.discoveries, key)
	p.mu.Unlock()
	result, err := p.PipelineCoordinator.ServeFanOutCandidate(ctx, turn, key)
	r := turn.receipt
	r.Result, r.Err, r.FinishedAt = result, err, time.Now()
	p.mu.Lock()
	delete(p.active, key)
	if !r.LoadedAt.IsZero() {
		p.stats.Loaded--
	}
	p.stats.Finished++
	if err != nil {
		p.failLocked(err)
	}
	if err == nil {
		p.stats.Claim.add(r.ClaimDuration)
		p.stats.ClaimToLoaded.add(r.LoadedAt.Sub(r.ClaimAt))
		p.stats.Planning.add(r.CommitAt.Sub(r.LoadedAt) - r.Hold)
		p.stats.Commit.add(r.CommitReturned.Sub(r.CommitAt))
		p.stats.Handoff.add(r.FinishedAt.Sub(r.CommitReturned))
		p.stats.TestHold.add(r.Hold)
	}
	select {
	case p.completed <- r:
	default:
		p.failLocked(fmt.Errorf("fixed completion mailbox overflow; consumer did not drain"))
	}
	p.mu.Unlock()
	return result, err
}

type supplementalServingTurn struct {
	pipeline.FanOutObligationOwner
	probe   *supplementalServingProbe
	held    bool
	receipt supplementalReceipt
}

func (o *supplementalServingTurn) pause(ctx context.Context, name string, gate <-chan struct{}) {
	if !o.held {
		return
	}
	start := time.Now()
	o.probe.phases <- supplementalPhase{Key: o.receipt.Key, Name: name}
	select {
	case <-gate:
	case <-ctx.Done():
	}
	o.receipt.Hold += time.Since(start)
}

func (o *supplementalServingTurn) ClaimFanOutIntent(ctx context.Context, req pipeline.FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	o.receipt.ClaimAt = time.Now()
	o.probe.mu.Lock()
	o.probe.stats.LastClaim = o.receipt.ClaimAt
	o.probe.mu.Unlock()
	i, c, found, err := o.FanOutObligationOwner.ClaimFanOutIntent(ctx, req)
	o.receipt.ClaimDuration = time.Since(o.receipt.ClaimAt)
	if found && err == nil {
		o.receipt.Generation = c.Generation
	}
	return i, c, found, err
}

func (o *supplementalServingTurn) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutEvaluationInput, error) {
	in, err := o.FanOutObligationOwner.LoadFanOutEvaluation(ctx, claim)
	if err == nil {
		o.receipt.LoadedAt = time.Now()
		p := o.probe
		p.mu.Lock()
		p.stats.Loaded++
		if p.stats.Loaded > p.stats.PeakLoaded {
			p.stats.PeakLoaded = p.stats.Loaded
		}
		p.mu.Unlock()
		o.pause(ctx, "loaded", p.loadGate)
	}
	return in, err
}

func (o *supplementalServingTurn) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	p := o.probe
	o.pause(ctx, "commit_ready", p.commitGate)
	o.receipt.CommitAt = time.Now()
	o.receipt.Commits++
	p.mu.Lock()
	p.stats.PendingCommits++
	if p.stats.PendingCommits > p.stats.PeakPendingCommits {
		p.stats.PeakPendingCommits = p.stats.PendingCommits
	}
	p.mu.Unlock()
	out, err := o.FanOutObligationOwner.CommitFanOutChunk(ctx, command)
	o.receipt.CommitReturned = time.Now()
	o.receipt.Publications += len(out.Publications)
	p.mu.Lock()
	p.stats.PendingCommits--
	p.stats.LastCommit = o.receipt.CommitReturned
	p.mu.Unlock()
	return out, err
}

func (o *supplementalServingTurn) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	started := time.Now()
	group, err := o.FanOutObligationOwner.BeginFanOutPublicationGroup(ctx, claim)
	elapsed := time.Since(started)
	o.probe.mu.Lock()
	o.probe.stats.GroupAdmission.add(elapsed)
	o.probe.mu.Unlock()
	return group, err
}

func newSupplementalServingFixture(t *testing.T, backend string, p *supplementalServingProbe, wrap ...func(startupownership.FanOutExecutor) startupownership.FanOutExecutor) *servingMatrixFixture {
	t.Helper()
	f := &servingMatrixFixture{started: make([]bool, 1)}
	if backend == "postgres" {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		f.selected, f.db = storetest.AdmitPostgresRuntimeStore(t, db), db
	} else {
		s := storetest.StartSQLiteRuntimeStore(t)
		f.selected, f.db = s, storetest.DatabaseForTest(s)
	}
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{NumericRegistrationRows: true, NumericReporterSink: true})
	f.sources = []semanticview.Source{source}
	fact := conformanceSourceArtifactFact(t, source)
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		t.Fatal("supplemental fixture requires actual compiled bundle")
	}
	storetest.RequireBundleDataCatalog(t, testAuthorActivityContextForBundle(context.Background(), fact), f.selected, bundle)
	f.topology = newNotifyAllChildrenProcessTopology(t, testAuthorActivityContextForBundle(context.Background(), fact), f.selected, source)
	rt := newNotifyAllChildrenRuntime(t, f.selected, f.db, source, time.Now, notifyAllChildrenRuntimeOptions{
		processTopology: f.topology,
		fanOutExecutor: func(pc *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
			p.PipelineCoordinator = pc
			if len(wrap) != 0 {
				return wrap[0](p)
			}
			return p
		},
	})
	f.runtimes = []notifyAllChildrenRuntime{rt}
	t.Cleanup(p.releaseAll)
	return f
}

func submitSupplementalIntent(t *testing.T, f *servingMatrixFixture, runID string, batch, count int) {
	t.Helper()
	rt := f.runtimes[0]
	rows := make([]map[string]any, count)
	for i := range rows {
		rows[i] = map[string]any{"account_id": supplementalAccount(batch, i), "eng_roles": 7, "gem_score": 7.25}
	}
	ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact), runID)
	publishNotifyAllChildrenEventAsync(t, ctx, rt, f.sources[0], runID, "portfolio.accounts.register.requested", map[string]any{"portfolio_id": runID, "account_ids": rows})
}

func supplementalAccount(batch, row int) string {
	return fmt.Sprintf("supplement-%02d-%02d", batch, row)
}

func waitSupplementalReceipt(t *testing.T, p *supplementalServingProbe, rows int) supplementalReceipt {
	t.Helper()
	select {
	case r := <-p.completed:
		if r.Err != nil || !r.Result.Refill || r.Generation != 1 || r.Commits != 1 || r.Publications != rows || r.Discovery.IsZero() || r.ClaimAt.Before(r.Discovery) || r.LoadedAt.Before(r.ClaimAt) || r.CommitAt.Before(r.LoadedAt) || r.CommitReturned.Before(r.CommitAt) || r.FinishedAt.Before(r.CommitReturned) {
			t.Fatalf("real supplemental turn lacks exact successful phase evidence: %+v", r)
		}
		return r
	case <-time.After(15 * time.Second):
		t.Fatalf("supplemental production turn stalled: %+v", p.snapshot())
	}
	return supplementalReceipt{}
}

func assertSupplementalEffects(t *testing.T, f *servingMatrixFixture, runID string, firstBatch, intents, cardinality int) {
	t.Helper()
	rt := f.runtimes[0]
	waitNotifyAllChildrenRuntimeWithin(t, rt, runID, 30*time.Second)
	ctx := testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact)
	s, err := f.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
	want := intents * cardinality
	if err != nil || s.Intents != intents || s.Cardinality != want || s.Cursor != want || s.Committed != want || s.SemanticRejected != 0 || s.Owed != 0 || s.Unsettled != 0 {
		t.Fatalf("supplemental summary=%+v want=%d err=%v", s, want, err)
	}
	actual := loadNotifyAllChildrenNumericRegistrations(t, ctx, f.selected, f.db, runID)
	if len(actual) != want {
		t.Fatalf("exact supplemental effects=%d want=%d", len(actual), want)
	}
	for batch := firstBatch; batch < firstBatch+intents; batch++ {
		for row := 0; row < cardinality; row++ {
			id := supplementalAccount(batch, row)
			r, ok := actual[id]
			if !ok || r.ID == "" || r.EngRoles != 7 || r.GemScore != 7.25 {
				t.Fatalf("missing/changed exact supplemental effect %s: %+v", id, r)
			}
		}
	}
	var outcomes, distinctEvents int
	if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT event_id) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed'`, runID).Scan(&outcomes, &distinctEvents); err != nil || outcomes != want || distinctEvents != want {
		t.Fatalf("durable outcomes=%d distinct events=%d want=%d err=%v", outcomes, distinctEvents, want, err)
	}
	assertSupplementalHistory(t, ctx, f.db, runID, intents, cardinality)
}

func assertSupplementalHistory(t *testing.T, ctx context.Context, db *sql.DB, runID string, intents, cardinality int) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY fact_key,revision`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var previous string
	var versions, observed int
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			t.Fatal(err)
		}
		var fact struct {
			Kind        string `json:"fact_kind"`
			Cursor      int    `json:"cursor"`
			Cardinality int    `json:"cardinality"`
		}
		if err := json.Unmarshal(raw, &fact); err != nil {
			t.Fatal(err)
		}
		if fact.Kind != "intent" {
			continue
		}
		if key != previous {
			if previous != "" && versions != 2 {
				t.Fatalf("intent %s history versions=%d want2", previous, versions)
			}
			previous, versions = key, 0
			observed++
		}
		wantCursor := 0
		if versions == 1 {
			wantCursor = cardinality
		}
		if versions >= 2 || fact.Cursor != wantCursor || fact.Cardinality != cardinality {
			t.Fatalf("intent %s history version%d: %+v", key, versions, fact)
		}
		versions++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if observed != intents || versions != 2 {
		t.Fatalf("durable intent histories=%d final versions=%d want=%d", observed, versions, intents)
	}
	t.Logf("durable history: run=%s intents=%d creation+single%d-ordinal commit per intent", runID, observed, cardinality)
}
