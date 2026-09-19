package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
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
	"github.com/google/uuid"
)

type servingMatrixPhase uint8

const (
	servingMatrixUnheld servingMatrixPhase = iota
	servingMatrixClaim
	servingMatrixEvaluation
	servingMatrixCommit
)

type servingMatrixAttempt struct {
	key fanoutobligation.IntentKey
	at  time.Time
}

type servingMatrixReceipt struct {
	turn   *servingMatrixTurn
	result pipeline.FanOutTurnResult
	err    error
	at     time.Time
}

type servingMatrixProbe struct {
	*pipeline.PipelineCoordinator
	phase               servingMatrixPhase
	holdCount           int
	commitWithoutCancel bool
	held                chan *servingMatrixTurn
	attempts            chan servingMatrixAttempt
	completed           chan servingMatrixReceipt

	mu         sync.Mutex
	turns      []*servingMatrixTurn
	active     map[fanoutobligation.IntentKey]int
	peak       int
	duplicates int
	closing    bool
	errors     []error
}

type servingMatrixTurn struct {
	pipeline.FanOutObligationOwner
	probe                    *servingMatrixProbe
	key                      fanoutobligation.IntentKey
	hold                     bool
	resume                   chan struct{}
	once                     sync.Once
	claim                    fanoutobligation.Claim
	claimAt                  time.Time
	loadedAt                 time.Time
	groupAt, groupReturnedAt time.Time
	groupErr                 error
	commitAt                 time.Time
	command                  pipeline.FanOutChunkCommand
	commitErr                error
	cleanup                  []fanoutobligation.Claim
	cleanupErr               error
}

func newServingMatrixProbe(phase servingMatrixPhase, holdCount int) *servingMatrixProbe {
	return &servingMatrixProbe{
		phase: phase, holdCount: holdCount, held: make(chan *servingMatrixTurn, 32),
		attempts: make(chan servingMatrixAttempt, 64), completed: make(chan servingMatrixReceipt, 64),
		active: make(map[fanoutobligation.IntentKey]int),
	}
}

func (p *servingMatrixProbe) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	p.mu.Lock()
	turn := &servingMatrixTurn{FanOutObligationOwner: owner, probe: p, key: key, resume: make(chan struct{})}
	turn.hold = !p.closing && p.phase != servingMatrixUnheld && (p.holdCount < 0 || len(p.turns) < p.holdCount)
	p.turns = append(p.turns, turn)
	p.mu.Unlock()
	result, err := p.PipelineCoordinator.ServeFanOutCandidate(ctx, turn, key)
	p.mu.Lock()
	if !turn.loadedAt.IsZero() {
		p.active[key]--
		if p.active[key] == 0 {
			delete(p.active, key)
		}
	}
	p.mu.Unlock()
	p.completed <- servingMatrixReceipt{turn: turn, result: result, err: err, at: time.Now()}
	return result, err
}

func (p *servingMatrixProbe) ReportFanOutServingError(ctx context.Context, err error) {
	p.mu.Lock()
	p.errors = append(p.errors, err)
	p.mu.Unlock()
	p.PipelineCoordinator.ReportFanOutServingError(ctx, err)
}

func (p *servingMatrixProbe) releaseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closing = true
	for _, turn := range p.turns {
		turn.release()
	}
}

func (p *servingMatrixProbe) snapshot() (active, peak, duplicates, turns int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.active), p.peak, p.duplicates, len(p.turns)
}

func (turn *servingMatrixTurn) release() { turn.once.Do(func() { close(turn.resume) }) }

func (turn *servingMatrixTurn) pause(phase servingMatrixPhase) {
	if !turn.hold || turn.probe.phase != phase {
		return
	}
	turn.probe.held <- turn
	// Deliberately retain the finite caller after cancellation. Lifetime joins
	// must wait for this real turn rather than recycling its permit early.
	<-turn.resume
}

func (turn *servingMatrixTurn) ClaimFanOutIntent(ctx context.Context, req pipeline.FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	turn.pause(servingMatrixClaim)
	if turn.probe.commitWithoutCancel {
		ctx = context.WithoutCancel(ctx)
	}
	turn.claimAt = time.Now()
	turn.probe.attempts <- servingMatrixAttempt{key: turn.key, at: turn.claimAt}
	intent, claim, found, err := turn.FanOutObligationOwner.ClaimFanOutIntent(ctx, req)
	if found && err == nil {
		turn.claim = claim
	}
	return intent, claim, found, err
}

func (turn *servingMatrixTurn) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutEvaluationInput, error) {
	input, err := turn.FanOutObligationOwner.LoadFanOutEvaluation(ctx, claim)
	if err != nil {
		return input, err
	}
	turn.loadedAt = time.Now()
	p := turn.probe
	p.mu.Lock()
	p.active[turn.key]++
	if p.active[turn.key] != 1 {
		p.duplicates++
	}
	if len(p.active) > p.peak {
		p.peak = len(p.active)
	}
	p.mu.Unlock()
	turn.pause(servingMatrixEvaluation)
	return input, nil
}

func (turn *servingMatrixTurn) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	turn.command = command
	turn.pause(servingMatrixCommit)
	if turn.probe.commitWithoutCancel {
		// A canceled worker must also fail its actual generation-bound mutation
		// fence when cancellation alone cannot protect the selected store.
		ctx = context.WithoutCancel(ctx)
	}
	turn.commitAt = time.Now()
	committed, err := turn.FanOutObligationOwner.CommitFanOutChunk(ctx, command)
	turn.commitErr = err
	return committed, err
}

func (turn *servingMatrixTurn) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	if turn.probe.commitWithoutCancel {
		ctx = context.WithoutCancel(ctx)
	}
	turn.groupAt = time.Now()
	group, err := turn.FanOutObligationOwner.BeginFanOutPublicationGroup(ctx, claim)
	turn.groupReturnedAt, turn.groupErr = time.Now(), err
	return group, err
}

func (turn *servingMatrixTurn) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) error {
	turn.cleanup = append(turn.cleanup, claim)
	turn.cleanupErr = turn.FanOutObligationOwner.ReleaseFanOutClaim(ctx, claim)
	return turn.cleanupErr
}

func waitServingMatrixHeld(t *testing.T, p *servingMatrixProbe) *servingMatrixTurn {
	t.Helper()
	select {
	case turn := <-p.held:
		return turn
	case receipt := <-p.completed:
		t.Fatalf("turn completed before required held phase: key=%+v err=%v", receipt.turn.key, receipt.err)
	case <-time.After(5 * time.Second):
		p.mu.Lock()
		defer p.mu.Unlock()
		t.Fatalf("no real turn reached held phase: turns=%d errors=%v", len(p.turns), p.errors)
	}
	return nil
}

func waitServingMatrixReceipt(t *testing.T, p *servingMatrixProbe, deadline time.Time) servingMatrixReceipt {
	t.Helper()
	select {
	case receipt := <-p.completed:
		if receipt.at.After(deadline) {
			t.Fatalf("production shared turn settled after deadline by %s", receipt.at.Sub(deadline))
		}
		return receipt
	case <-time.After(time.Until(deadline)):
		t.Fatal("production shared turn did not settle before deadline")
	}
	return servingMatrixReceipt{}
}

type servingMatrixFixture struct {
	selected notifyAllChildrenStore
	db       *sql.DB
	topology *notifyAllChildrenProcessTopology
	sources  []semanticview.Source
	runtimes []notifyAllChildrenRuntime
	probes   []*servingMatrixProbe
	started  []bool
}

func newServingMatrixFixture(t *testing.T, backend string, workers *int, probes ...*servingMatrixProbe) *servingMatrixFixture {
	t.Helper()
	f := &servingMatrixFixture{probes: probes, started: make([]bool, len(probes))}
	if backend == "postgres" {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		f.selected, f.db = storetest.AdmitPostgresRuntimeStore(t, db), db
	} else {
		sqlite := storetest.StartSQLiteRuntimeStore(t)
		f.selected, f.db = sqlite, storetest.DatabaseForTest(sqlite)
	}
	for i := range probes {
		source := notifyallchildren.LoadSource(t, notifyallchildren.Options{
			NumericRegistrationRows: true, NumericReporterSink: true, RegistrationUUIDField: i > 0,
		})
		bundle, ok := semanticview.Bundle(source)
		if !ok || bundle == nil {
			t.Fatal("serving matrix requires real compiled bundles")
		}
		storetest.RequireBundleDataCatalog(t, testAuthorActivityContextForBundle(context.Background(), conformanceSourceArtifactFact(t, source)), f.selected, bundle)
		f.sources = append(f.sources, source)
	}
	f.topology = newNotifyAllChildrenProcessTopology(t, testAuthorActivityContext(context.Background()), f.selected, f.sources...)
	for i, probe := range probes {
		runtime := newNotifyAllChildrenRuntime(t, f.selected, f.db, f.sources[i], time.Now, notifyAllChildrenRuntimeOptions{
			processTopology: f.topology, fanOutWorkers: workers,
			fanOutExecutor: func(pc *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
				probe.PipelineCoordinator = pc
				return probe
			},
		})
		f.runtimes = append(f.runtimes, runtime)
		t.Cleanup(probe.releaseAll)
	}
	return f
}

func (f *servingMatrixFixture) startRun(t *testing.T, runtimeIndex int) (context.Context, string) {
	t.Helper()
	rt := f.runtimes[runtimeIndex]
	runID := uuid.NewString()
	ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact), runID)
	if !f.started[runtimeIndex] {
		if err := rt.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "fan-out-serving-matrix", rt.sourceArtifactFact)); err != nil {
			t.Fatalf("run serving matrix manager: %v", err)
		}
		f.started[runtimeIndex] = true
	}
	publishNotifyAllChildrenRunCreatingEvent(t, ctx, rt, f.sources[runtimeIndex], runID, "portfolio.opened", map[string]any{"portfolio_id": runID, "threshold": 75})
	return ctx, runID
}

func (f *servingMatrixFixture) submit(t *testing.T, runtimeIndex int, runID, account string) string {
	t.Helper()
	rt := f.runtimes[runtimeIndex]
	ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact), runID)
	row := map[string]any{"account_id": account, "eng_roles": 7, "gem_score": 7.25}
	if runtimeIndex > 0 {
		row["external_id"] = uuid.NewString()
	}
	return publishNotifyAllChildrenEventAsync(t, ctx, rt, f.sources[runtimeIndex], runID, "portfolio.accounts.register.requested", map[string]any{"portfolio_id": runID, "account_ids": []map[string]any{row}})
}

func (f *servingMatrixFixture) assertSettled(t *testing.T, runtimeIndex int, runID string, cardinality int) {
	t.Helper()
	rt := f.runtimes[runtimeIndex]
	waitNotifyAllChildrenRuntimeWithin(t, rt, runID, 10*time.Second)
	ctx := testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact)
	summary, err := f.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
	if err != nil || summary.Intents != cardinality || summary.Cardinality != cardinality || summary.Cursor != cardinality || summary.Committed != cardinality || summary.SemanticRejected != 0 || summary.Owed != 0 || summary.Unsettled != 0 {
		t.Fatalf("exact shared-serving outcomes: %+v err=%v", summary, err)
	}
	registrations := loadNotifyAllChildrenNumericRegistrations(t, ctx, f.selected, f.db, runID)
	if len(registrations) != cardinality {
		t.Fatalf("durable publications=%d want=%d", len(registrations), cardinality)
	}
	for key, registration := range registrations {
		if registration.ID == "" || registration.EngRoles != 7 || registration.GemScore != 7.25 {
			t.Fatalf("publication %s differs from evaluated input: %+v", key, registration)
		}
	}
	assertServingMatrixHistory(t, ctx, f.db, runID, cardinality)
}

func assertServingMatrixHistory(t *testing.T, ctx context.Context, db *sql.DB, runID string, intents int) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY revision,fact_key`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	observed := make(map[string][]int)
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
		if fact.Cardinality != 1 {
			t.Fatalf("short intent %s cardinality=%d", key, fact.Cardinality)
		}
		observed[key] = append(observed[key], fact.Cursor)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(observed) != intents {
		t.Fatalf("history intent identities=%d want=%d", len(observed), intents)
	}
	for key, cursors := range observed {
		if len(cursors) != 2 || cursors[0] != 0 || cursors[1] != 1 {
			t.Fatalf("intent %s must retain exactly creation+publication cursor revisions: %v", key, cursors)
		}
	}
}

type servingMatrixScan struct {
	candidate startupownership.FanOutCandidate
	found     bool
	err       error
	at        time.Time
}

func observeServingMatrixScans(t *testing.T, rt notifyAllChildrenRuntime, holdEmpty ...<-chan struct{}) <-chan servingMatrixScan {
	t.Helper()
	observer, ok := any(rt.fanOutServing).(interface {
		SetTestScanObserver(func(startupownership.FanOutCandidate, bool, error))
	})
	if !ok {
		t.Fatal("production serving registration must expose the coordinated read-only scan observation hook")
	}
	scans := make(chan servingMatrixScan, 64)
	var holdOnce sync.Once
	observer.SetTestScanObserver(func(candidate startupownership.FanOutCandidate, found bool, err error) {
		select {
		case scans <- servingMatrixScan{candidate: candidate, found: found, err: err, at: time.Now()}:
		default:
		}
		if !found && err == nil && len(holdEmpty) != 0 {
			holdOnce.Do(func() { <-holdEmpty[0] })
		}
	})
	t.Cleanup(func() { observer.SetTestScanObserver(nil) })
	rt.fanOutServing.Wake()
	select {
	case scan := <-scans:
		if scan.err != nil || scan.found {
			t.Fatalf("required actual empty scan: %+v", scan)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shared serving did not complete an empty scan")
	}
	return scans
}

type servingMatrixDroppedWake struct{ calls chan time.Time }

func (n servingMatrixDroppedWake) Wake() {
	select {
	case n.calls <- time.Now():
	default:
	}
}
