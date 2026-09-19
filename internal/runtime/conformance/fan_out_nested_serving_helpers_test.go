package conformance

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

// These are commit-boundary samples, not a census of every prepared plan.
// Preparation/discard lifecycle evidence is required separately for M29.
type nestedServingCounts struct {
	Active, PeakActive                   int
	CommitPlans, PeakCommitPlans         int
	Carriers, PeakCarriers               int
	LoadedItems, PeakLoadedItems         int
	Started, Returned, SuccessfulCommits int
	FirstError                           error
}

type nestedServingReceipt struct {
	Key                      fanoutobligation.IntentKey
	ParentEvent              string
	LoadedAt                 time.Time
	GroupAt, GroupReturnedAt time.Time
	CommittedAt              time.Time
	ReturnedAt               time.Time
	Publications             int
	Result                   pipeline.FanOutTurnResult
	Err                      error
}

type nestedServingProbe struct {
	*pipeline.PipelineCoordinator
	mu           sync.Mutex
	counts       nestedServingCounts
	holdNext     bool
	closing      bool
	held         chan nestedServingReceipt
	receipts     chan nestedServingReceipt
	release      chan struct{}
	once         sync.Once
	publications nestedPublicationLifetime
	rejection    *nestedPreparedRejection
}

func newNestedServingProbe(t *testing.T) *nestedServingProbe {
	t.Helper()
	p := &nestedServingProbe{
		held: make(chan nestedServingReceipt, 1), receipts: make(chan nestedServingReceipt, 64),
		release: make(chan struct{}),
	}
	t.Cleanup(p.releaseHeld)
	return p
}

func (p *nestedServingProbe) armPostCommitHold(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing || p.holdNext || p.counts.Active != 0 {
		t.Fatalf("nested post-commit gate requires idle owner: counts=%+v closing=%v", p.counts, p.closing)
	}
	p.holdNext = true
}

func (p *nestedServingProbe) releaseHeld() {
	if p.rejection != nil {
		p.rejection.open()
	}
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	p.once.Do(func() { close(p.release) })
}

func (p *nestedServingProbe) snapshot() nestedServingCounts {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts
}

func (p *nestedServingProbe) noteError(err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts.FirstError == nil {
		p.counts.FirstError = err
	}
}

func (p *nestedServingProbe) ReportFanOutServingError(ctx context.Context, err error) {
	if p.rejection == nil || !p.rejection.reportExpected(err) {
		p.noteError(err)
	}
	p.PipelineCoordinator.ReportFanOutServingError(ctx, err)
}

func (p *nestedServingProbe) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	if p.rejection != nil {
		p.rejection.beforeTurn(ctx)
	}
	p.mu.Lock()
	p.counts.Active++
	p.counts.Started++
	p.counts.PeakActive = max(p.counts.PeakActive, p.counts.Active)
	turn := &nestedServingTurn{FanOutObligationOwner: owner, probe: p, receipt: nestedServingReceipt{Key: key}, hold: p.holdNext && !p.closing}
	p.holdNext = false
	p.mu.Unlock()
	result, err := p.PipelineCoordinator.ServeFanOutCandidate(context.WithValue(ctx, nestedTurnContextKey{}, turn), turn, key)
	p.publications.finish(turn)
	turn.receipt.Result, turn.receipt.Err, turn.receipt.ReturnedAt = result, err, time.Now()
	p.mu.Lock()
	p.counts.Active--
	p.counts.Returned++
	p.counts.LoadedItems -= turn.loadedItems
	p.counts.CommitPlans -= turn.commitPlans
	p.counts.Carriers -= turn.carriers
	p.mu.Unlock()
	select {
	case p.receipts <- turn.receipt:
	default:
		p.noteError(fmt.Errorf("nested proof receipt bound exceeded; drain finite receipts"))
	}
	if p.rejection != nil {
		p.rejection.afterTurn(turn.receipt)
	}
	return result, err
}

type nestedServingTurn struct {
	pipeline.FanOutObligationOwner
	probe       *nestedServingProbe
	receipt     nestedServingReceipt
	hold        bool
	loadedItems int
	commitPlans int
	carriers    int
}

func (turn *nestedServingTurn) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutEvaluationInput, error) {
	input, err := turn.FanOutObligationOwner.LoadFanOutEvaluation(ctx, claim)
	if err != nil {
		return input, err
	}
	turn.receipt.ParentEvent, turn.receipt.LoadedAt = input.Trigger.ID(), time.Now()
	p := turn.probe
	p.mu.Lock()
	p.counts.LoadedItems += len(input.Items) - turn.loadedItems
	turn.loadedItems = len(input.Items)
	p.counts.PeakLoadedItems = max(p.counts.PeakLoadedItems, p.counts.LoadedItems)
	p.mu.Unlock()
	return input, nil
}

func (turn *nestedServingTurn) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	plans := 0
	for _, outcome := range command.Outcomes {
		if outcome.Publication != nil {
			plans++
		}
	}
	p := turn.probe
	p.mu.Lock()
	p.counts.CommitPlans += plans - turn.commitPlans
	turn.commitPlans = plans
	p.counts.PeakCommitPlans = max(p.counts.PeakCommitPlans, p.counts.CommitPlans)
	p.mu.Unlock()
	if p.rejection != nil {
		if err := p.rejection.rejectPrepared(command); err != nil {
			return pipeline.CommittedFanOutChunk{}, err
		}
	}
	committed, err := turn.FanOutObligationOwner.CommitFanOutChunk(ctx, command)
	if err != nil {
		return committed, err
	}
	turn.receipt.CommittedAt, turn.receipt.Publications = time.Now(), len(committed.Publications)
	p.publications.committed(committed.Publications)
	p.mu.Lock()
	p.counts.SuccessfulCommits++
	p.counts.Carriers += len(committed.Publications) - turn.carriers
	turn.carriers = len(committed.Publications)
	p.counts.PeakCarriers = max(p.counts.PeakCarriers, p.counts.Carriers)
	p.mu.Unlock()
	if turn.hold {
		turn.hold = false
		p.held <- turn.receipt
		// Keep the actual committed carrier and permit alive even when canceled.
		// Cleanup must release this gate before runtime lifetime joins can finish.
		<-p.release
	}
	return committed, nil
}

func (turn *nestedServingTurn) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	turn.receipt.GroupAt = time.Now()
	group, err := turn.FanOutObligationOwner.BeginFanOutPublicationGroup(ctx, claim)
	turn.receipt.GroupReturnedAt = time.Now()
	return group, err
}

func newNestedServingRuntime(t *testing.T, backend string, source semanticview.Source, gate *notifyAllChildrenAgentGate, probe *nestedServingProbe, heldChild ...*nestedChildHandlerGate) (notifyAllChildrenRuntime, *sql.DB) {
	t.Helper()
	var selected notifyAllChildrenStore
	var db *sql.DB
	switch backend {
	case "postgres":
		var cleanup func()
		_, db, cleanup = testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		selected = storetest.AdmitPostgresRuntimeStore(t, db)
	case "sqlite":
		sqlite := storetest.StartSQLiteRuntimeStore(t)
		selected, db = sqlite, storetest.DatabaseForTest(sqlite)
	default:
		t.Fatalf("unsupported nested-serving backend %q", backend)
	}
	workers := 1
	rt := newNotifyAllChildrenRuntime(t, selected, db, source, time.Now, notifyAllChildrenRuntimeOptions{
		realMockAgents: gate != nil, agentGate: gate, enableGenericSchedules: true, fanOutWorkers: &workers,
		nestedPublications: &probe.publications,
		fanOutExecutor: func(pc *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
			probe.PipelineCoordinator = pc
			return probe
		},
	})
	// Registered after runtime cleanup so a failed held assertion cannot deadlock
	// the registration's exact turn join during testing cleanup.
	t.Cleanup(probe.releaseHeld)
	if len(heldChild) > 1 {
		t.Fatal("nested fixture accepts only one exact child handler gate")
	}
	if len(heldChild) == 1 {
		rt.pipeline.SetTestLifecycleProbe(heldChild[0])
		t.Cleanup(heldChild[0].open)
	}
	return rt, db
}

func waitNestedServingPostCommit(t *testing.T, p *nestedServingProbe) nestedServingReceipt {
	t.Helper()
	select {
	case receipt := <-p.held:
		counts := p.snapshot()
		if counts.Active != 1 || counts.PeakActive != 1 || counts.Carriers != receipt.Publications || receipt.Publications == 0 {
			t.Fatalf("capacity-one committed handoff ownership: receipt=%+v counts=%+v", receipt, counts)
		}
		return receipt
	case <-time.After(5 * time.Second):
		t.Fatalf("nested actual commit did not reach held handoff: %+v", p.snapshot())
	}
	return nestedServingReceipt{}
}

func assertNestedServingDrained(t *testing.T, p *nestedServingProbe) {
	t.Helper()
	counts := p.snapshot()
	if counts.FirstError != nil || counts.Active != 0 || counts.CommitPlans != 0 || counts.Carriers != 0 || counts.LoadedItems != 0 || counts.Started != counts.Returned {
		t.Fatalf("nested finite callers did not drain: %+v", counts)
	}
	if counts.PeakActive != 1 || counts.PeakCommitPlans > fanoutobligation.InitialChunkSize || counts.PeakCarriers > fanoutobligation.InitialChunkSize || counts.PeakLoadedItems > fanoutobligation.InitialChunkSize {
		t.Fatalf("nested capacity-one boundary bound exceeded: %+v", counts)
	}
	t.Logf("nested actual caller/commit-boundary highwaters (not full preparation lifecycle): %+v", counts)
	p.publications.mu.Lock()
	defer p.publications.mu.Unlock()
	life := &p.publications
	if life.err != nil || len(life.live) != 0 || life.acquired != life.released+life.returned || life.peak < 1 || life.peak > fanoutobligation.InitialChunkSize {
		t.Fatalf("nested publication lifecycle: live=%d peak=%d acquired=%d released=%d returned=%d err=%v", len(life.live), life.peak, life.acquired, life.released, life.returned, life.err)
	}
	t.Logf("nested real planner-to-caller lifetime: peak=%d acquired=%d released=%d returned=%d", life.peak, life.acquired, life.released, life.returned)
	if life.groupPrepared != life.acquired || life.groupFinalized != life.returned || life.groupDispatched != life.returned || life.groupSeals == 0 {
		t.Fatalf("grouped planner bypassed lifetime observation: prepared=%d/%d finalized=%d dispatched=%d/%d seals=%d", life.groupPrepared, life.acquired, life.groupFinalized, life.groupDispatched, life.returned, life.groupSeals)
	}
	if life.batchCalls == 0 || life.batchResults < life.groupPrepared || life.batchEmptyResults != 0 {
		t.Fatalf("batch planner bypassed result observation: calls=%d results=%d prepared=%d empty=%d", life.batchCalls, life.batchResults, life.groupPrepared, life.batchEmptyResults)
	}
	t.Logf("actual batch preparation: calls=%d results=%d acquisition_errors=%d row_errors=%d plans_with_row_errors=%d", life.batchCalls, life.batchResults, life.batchErrors, life.batchRowErrors, life.batchErrorPlans)
	t.Logf("actual grouped planner durations (includes held dispatch): prepare=%s seal=%s finalize=%s dispatch=%s seals=%d", life.prepareDuration, life.sealDuration, life.finalizeDuration, life.dispatchDuration, life.groupSeals)
}
