package bus

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
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

type sourceMutationProbeOwner struct {
	runtimepipelineobligation.Store

	mu               sync.Mutex
	claimIssuer      *runtimepipelineobligation.ClaimIssuer
	scanIssuer       *runtimepipelineobligation.ScanIssuer
	claims           map[string]runtimepipelineobligation.Claim
	opened           chan struct{}
	openOnce         sync.Once
	claimPublication int
	claimEvent       int
	openScan         int
	claimBatch       int
	closeScan        int
	release          int
	settle           int
}

type sourceMutationProbeCounts struct {
	claimPublication int
	claimEvent       int
	openScan         int
	claimBatch       int
	closeScan        int
	release          int
	settle           int
}

func newSourceMutationProbeOwner() *sourceMutationProbeOwner {
	return &sourceMutationProbeOwner{
		claimIssuer: runtimepipelineobligation.NewClaimIssuer(),
		scanIssuer:  runtimepipelineobligation.NewScanIssuer(),
		claims:      map[string]runtimepipelineobligation.Claim{},
		opened:      make(chan struct{}),
	}
}

func (o *sourceMutationProbeOwner) ClaimPublication(_ context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claimPublication++
	claim, err := o.claimIssuer.Issue(eventID, runtimepipelineobligation.PurposePublication)
	if err == nil {
		o.claims[eventID] = claim
	}
	return claim, err
}

func (o *sourceMutationProbeOwner) ClaimEvent(_ context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claimEvent++
	claim, err := o.claimIssuer.Issue(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	o.claims[eventID] = claim
	return runtimepipelineobligation.ClaimedWork{Claim: claim}, nil
}

func (o *sourceMutationProbeOwner) OpenScan(_ context.Context, request runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	if err := request.Validate(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.openScan++
	o.openOnce.Do(func() { close(o.opened) })
	return o.scanIssuer.Issue()
}

func (o *sourceMutationProbeOwner) ClaimBatch(_ context.Context, _ runtimepipelineobligation.Scan, _ int) (runtimepipelineobligation.ScanBatch, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claimBatch++
	return runtimepipelineobligation.ScanBatch{Exhausted: true}, nil
}

func (o *sourceMutationProbeOwner) CloseScan(_ context.Context, scan runtimepipelineobligation.Scan) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closeScan++
	_, err := o.scanIssuer.Token(scan)
	return err
}

func (o *sourceMutationProbeOwner) MarkDecisionProcessed(context.Context, runtimepipelineobligation.Claim) error {
	return nil
}

func (o *sourceMutationProbeOwner) Settle(_ context.Context, claim runtimepipelineobligation.Claim, _ runtimepipelineobligation.Disposition) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.settle++
	delete(o.claims, claim.EventID())
	return nil
}

func (o *sourceMutationProbeOwner) Release(_ context.Context, claim runtimepipelineobligation.Claim) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.release++
	delete(o.claims, claim.EventID())
	return nil
}

func (*sourceMutationProbeOwner) GlobalWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, nil
}

func (*sourceMutationProbeOwner) SummarizeRun(_ context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	return runtimepipelineobligation.RunSummary{RunID: strings.TrimSpace(runID)}, nil
}

func (*sourceMutationProbeOwner) TerminalizeRun(context.Context, string, runtimepipelineobligation.Disposition, time.Time) (int, error) {
	return 0, nil
}

func (o *sourceMutationProbeOwner) counts() sourceMutationProbeCounts {
	o.mu.Lock()
	defer o.mu.Unlock()
	return sourceMutationProbeCounts{
		claimPublication: o.claimPublication,
		claimEvent:       o.claimEvent,
		openScan:         o.openScan,
		claimBatch:       o.claimBatch,
		closeScan:        o.closeScan,
		release:          o.release,
		settle:           o.settle,
	}
}

func (o *sourceMutationProbeOwner) owns(eventID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.claims[eventID]
	return ok
}

type sourceMutationProbeTransaction struct {
	begin    int
	finalize int
}

func (t *sourceMutationProbeTransaction) BeginPreparedPublish(context.Context, PreparedPublishEvent) (EventAppendOutcome, error) {
	t.begin++
	return EventAppendInserted, nil
}

func (t *sourceMutationProbeTransaction) FinalizePreparedPublish(context.Context, PreparedPublishFinalization) error {
	t.finalize++
	return nil
}

func sourceMutationFact(t testing.TB, marker string) runtimecorrelation.BundleSourceFact {
	t.Helper()
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:" + strings.Repeat(marker, 64))
	if err != nil {
		t.Fatalf("construct source fact: %v", err)
	}
	return fact
}

func newSourceMutationProbeBus(
	t testing.TB,
	fact runtimecorrelation.BundleSourceFact,
	owner *sourceMutationProbeOwner,
) *EventBus {
	t.Helper()
	process := worklifetime.NewProcess()
	runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: uuid.NewString(),
		BundleHash:        "source-mutation-probe",
	})
	if err != nil {
		t.Fatalf("create runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = runtimeOwner.RetireAndWait(ctx)
		process.Retire()
		_, _ = process.Join(ctx)
	})
	bus, err := newEventBusWithOptions(InMemoryEventStore{}, EventBusOptions{
		BundleSourceFact:    fact,
		RuntimeInstanceID:   uuid.NewString(),
		PipelineObligations: owner,
		WorkOwner:           runtimeOwner,
	})
	if err != nil {
		t.Fatalf("create event bus: %v", err)
	}
	return bus
}

func sourceMutationEvent() events.Event {
	return eventtest.RuntimeControl(
		uuid.NewString(),
		events.EventType("test.source_mutation"),
		"test",
		"",
		[]byte(`{}`),
		0,
		uuid.NewString(),
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
}

func sourceMutationContext(ctx context.Context, transaction CommitPublishTransaction) (context.Context, *[]runtimepipeline.OwnerAction) {
	rollback := make([]runtimepipeline.OwnerAction, 0, 2)
	postCommit := make([]runtimepipeline.OwnerAction, 0, 2)
	ctx = runtimepipeline.WithPipelineRollbackActions(ctx, &rollback)
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommit)
	return WithCommitPublishTransaction(ctx, transaction), &postCommit
}

func TestEngineOutboxAdmitsExactBundleSourceBeforePersistenceOrClaim(t *testing.T) {
	owned := sourceMutationFact(t, "a")
	foreign := sourceMutationFact(t, "b")
	for _, tc := range []struct {
		name      string
		busFact   runtimecorrelation.BundleSourceFact
		context   func(context.Context) context.Context
		wantError bool
	}{
		{name: "missing", wantError: true},
		{
			name:    "foreign",
			busFact: owned,
			context: func(ctx context.Context) context.Context {
				return runtimecorrelation.WithBundleSourceFact(ctx, foreign)
			},
			wantError: true,
		},
		{name: "bus owned", busFact: owned},
		{
			name:    "context owned selected fork",
			context: func(ctx context.Context) context.Context { return runtimecorrelation.WithBundleSourceFact(ctx, owned) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := newSourceMutationProbeOwner()
			bus := newSourceMutationProbeBus(t, tc.busFact, owner)
			transaction := &sourceMutationProbeTransaction{}
			ctx := context.Background()
			if tc.context != nil {
				ctx = tc.context(ctx)
			}
			ctx, postCommit := sourceMutationContext(ctx, transaction)
			event := sourceMutationEvent()

			err := bus.EngineOutbox().WriteOutbox(ctx, []runtimeengine.EmitIntent{{Event: event}})
			runtimepipeline.FlushPipelinePostCommitActions(*postCommit)
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "bundle source fact") {
					t.Fatalf("WriteOutbox error = %v, want source admission failure", err)
				}
				if transaction.begin != 0 || transaction.finalize != 0 {
					t.Fatalf("transaction mutations = begin:%d finalize:%d, want zero", transaction.begin, transaction.finalize)
				}
				if got := owner.counts(); got != (sourceMutationProbeCounts{}) {
					t.Fatalf("pipeline mutations = %#v, want zero", got)
				}
				bus.mu.RLock()
				pending := len(bus.pendingOutboxByID[event.ID()])
				bus.mu.RUnlock()
				if pending != 0 {
					t.Fatalf("pending operations = %d, want zero", pending)
				}
				return
			}
			if err != nil {
				t.Fatalf("WriteOutbox: %v", err)
			}
			if transaction.begin != 1 || transaction.finalize != 1 {
				t.Fatalf("transaction mutations = begin:%d finalize:%d, want one each", transaction.begin, transaction.finalize)
			}
			if got := owner.counts().claimPublication; got != 1 {
				t.Fatalf("publication claims = %d, want one", got)
			}
			bus.mu.RLock()
			pending := len(bus.pendingOutboxByID[event.ID()])
			bus.mu.RUnlock()
			if pending != 1 {
				t.Fatalf("pending operations = %d, want one", pending)
			}
			bus.clearPendingOutboxOperation(event.ID())
		})
	}
}

func TestPostCommitAndDeferredDispatchRetainPendingWorkOnSourceRejection(t *testing.T) {
	owned := sourceMutationFact(t, "c")
	foreign := sourceMutationFact(t, "d")
	for _, source := range []struct {
		name    string
		busFact runtimecorrelation.BundleSourceFact
		context func(context.Context) context.Context
	}{
		{name: "missing"},
		{
			name:    "foreign",
			busFact: owned,
			context: func(ctx context.Context) context.Context {
				return runtimecorrelation.WithBundleSourceFact(ctx, foreign)
			},
		},
	} {
		for _, surface := range []string{"post_commit", "deferred_private_root"} {
			t.Run(source.name+"/"+surface, func(t *testing.T) {
				owner := newSourceMutationProbeOwner()
				bus := newSourceMutationProbeBus(t, source.busFact, owner)
				event := sourceMutationEvent()
				claim, err := bus.claimPipelinePublication(context.Background(), event.ID())
				if err != nil {
					t.Fatalf("claim staged publication: %v", err)
				}
				bus.stagePendingOutboxOperation(
					context.Background(),
					runtimeengine.EmitIntent{Event: event},
					EventAppendInserted,
					claim,
				)
				before := owner.counts()
				ctx := context.Background()
				if source.context != nil {
					ctx = source.context(ctx)
				}
				switch surface {
				case "post_commit":
					err = bus.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{Event: event}})
				case "deferred_private_root":
					err = bus.publishDeferred(ctx, event)
				}
				if err == nil || !strings.Contains(err.Error(), "bundle source fact") {
					t.Fatalf("%s error = %v, want source admission failure", surface, err)
				}
				if got := owner.counts(); got != before {
					t.Fatalf("pipeline mutations after rejection = %#v, want unchanged %#v", got, before)
				}
				bus.mu.RLock()
				pending := len(bus.pendingOutboxByID[event.ID()])
				bus.mu.RUnlock()
				if pending != 1 || !owner.owns(event.ID()) {
					t.Fatalf("pending operation/claim = %d/%v, want exact staged work retained", pending, owner.owns(event.ID()))
				}
				bus.clearPendingOutboxOperation(event.ID())
			})
		}
	}
}

func TestQueuedPostCommitDispatchPreservesContextOwnedSourceFact(t *testing.T) {
	owned := sourceMutationFact(t, "4")
	owner := newSourceMutationProbeOwner()
	bus := newSourceMutationProbeBus(t, runtimecorrelation.BundleSourceFact{}, owner)
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	rollback := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), owned)
	ctx = runtimepipeline.WithPipelineSQLTxContext(ctx, &sql.Tx{})
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommit)
	ctx = runtimepipeline.WithPipelineRollbackActions(ctx, &rollback)

	if err := bus.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{Event: sourceMutationEvent()}}); err != nil {
		t.Fatalf("DispatchPostCommit: %v", err)
	}
	if len(postCommit) != 1 {
		t.Fatalf("queued post-commit actions = %d, want one", len(postCommit))
	}
	if got := owner.counts(); got.claimEvent != 0 || got.settle != 0 {
		t.Fatalf("pipeline mutations before commit = %#v, want none", got)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if got := owner.counts(); got.claimEvent != 1 || got.settle != 1 {
		t.Fatalf("pipeline mutations after commit = %#v, want one claim and settlement", got)
	}
}

func TestPreparedPublishRejectsCrossBusAndForeignSourceWithoutConsumingClaim(t *testing.T) {
	owned := sourceMutationFact(t, "e")
	foreign := sourceMutationFact(t, "f")
	actions := []struct {
		name string
		run  func(*EventBus, context.Context, PreparedPublish) error
	}{
		{name: "abandon", run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
			return bus.AbandonPreparedPublish(ctx, prepared)
		}},
		{name: "sync", run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
			return bus.DispatchPreparedPublish(ctx, prepared)
		}},
		{name: "wait", run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
			return bus.DispatchPreparedPublishAndWait(ctx, prepared)
		}},
		{name: "async", run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
			return bus.DispatchPreparedPublishAsync(ctx, prepared)
		}},
		{name: "queue", run: func(bus *EventBus, ctx context.Context, prepared PreparedPublish) error {
			postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
			ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommit)
			ctx = WithCommitPublishTransaction(ctx, &sourceMutationProbeTransaction{})
			err := bus.queuePreparedPublishInMutation(ctx, prepared)
			if len(postCommit) != 0 {
				return errors.New("invalid prepared publish queued post-commit work")
			}
			return err
		}},
	}
	for _, action := range actions {
		for _, transfer := range []struct {
			name      string
			targetBus func(testing.TB, *sourceMutationProbeOwner) *EventBus
			context   func(context.Context) context.Context
		}{
			{
				name: "same source different bus",
				targetBus: func(t testing.TB, owner *sourceMutationProbeOwner) *EventBus {
					return newSourceMutationProbeBus(t, owned, owner)
				},
			},
			{
				name: "different source different bus",
				targetBus: func(t testing.TB, owner *sourceMutationProbeOwner) *EventBus {
					return newSourceMutationProbeBus(t, foreign, owner)
				},
			},
			{
				name:      "same bus foreign context",
				targetBus: nil,
				context: func(ctx context.Context) context.Context {
					return runtimecorrelation.WithBundleSourceFact(ctx, foreign)
				},
			},
		} {
			t.Run(action.name+"/"+transfer.name, func(t *testing.T) {
				ownerA := newSourceMutationProbeOwner()
				busA := newSourceMutationProbeBus(t, owned, ownerA)
				event := sourceMutationEvent()
				claim, err := busA.claimPipelinePublication(context.Background(), event.ID())
				if err != nil {
					t.Fatalf("claim prepared publication: %v", err)
				}
				prepared := PreparedPublish{Event: event, publicationClaim: claim}
				target := busA
				if transfer.targetBus != nil {
					target = transfer.targetBus(t, newSourceMutationProbeOwner())
					prepared.dispatchContext = runtimecorrelation.WithBundleSourceFact(context.Background(), owned)
				}
				ctx := context.Background()
				if transfer.context != nil {
					ctx = transfer.context(ctx)
				}

				err = action.run(target, ctx, prepared)
				if err == nil {
					t.Fatal("invalid prepared handoff unexpectedly succeeded")
				}
				if got := ownerA.counts(); got.release != 0 || got.settle != 0 || !ownerA.owns(event.ID()) {
					t.Fatalf("preparing claim after rejection = %#v owned:%v, want intact", got, ownerA.owns(event.ID()))
				}
				if err := busA.AbandonPreparedPublish(context.Background(), prepared); err != nil {
					t.Fatalf("preparing bus abandon after rejection: %v", err)
				}
				if got := ownerA.counts().release; got != 1 {
					t.Fatalf("preparing bus release count = %d, want one", got)
				}
			})
		}
	}
}

func TestPipelineSweepRootsAdmitSourceBeforeWorkOccurrenceOrStoreMutation(t *testing.T) {
	owned := sourceMutationFact(t, "1")
	foreign := sourceMutationFact(t, "2")
	for _, source := range []struct {
		name    string
		busFact runtimecorrelation.BundleSourceFact
		context func(context.Context) context.Context
	}{
		{name: "missing"},
		{
			name:    "foreign",
			busFact: owned,
			context: func(ctx context.Context) context.Context {
				return runtimecorrelation.WithBundleSourceFact(ctx, foreign)
			},
		},
	} {
		for _, surface := range []string{"start", "global", "shared_internal", "run_release"} {
			t.Run(source.name+"/"+surface, func(t *testing.T) {
				owner := newSourceMutationProbeOwner()
				bus := newSourceMutationProbeBus(t, source.busFact, owner)
				ctx := context.Background()
				if source.context != nil {
					ctx = source.context(ctx)
				}
				var err error
				switch surface {
				case "start":
					err = bus.StartOutboxSweeper(ctx, OutboxSweeperConfig{Interval: time.Hour, Limit: 1})
				case "global":
					_, err = bus.SweepPipelineObligations(ctx, 1)
				case "shared_internal":
					_, err = bus.sweepPipelineObligations(ctx, runtimepipelineobligation.GlobalScanRequest(), 1)
				case "run_release":
					_, err = bus.ReleaseRunQueue(ctx, uuid.NewString(), 1)
				}
				if err == nil || !strings.Contains(err.Error(), "bundle source fact") {
					t.Fatalf("%s error = %v, want source admission failure", surface, err)
				}
				if got := owner.counts(); got != (sourceMutationProbeCounts{}) {
					t.Fatalf("pipeline store mutations = %#v, want zero", got)
				}
				if bus.OutboxSweeperActive() || bus.outboxSweeperDone != nil {
					t.Fatal("source rejection created a sweeper occurrence")
				}
			})
		}
	}
}

func TestPipelineSweepRootsExecuteForBusAndContextOwnedSources(t *testing.T) {
	owned := sourceMutationFact(t, "3")
	t.Run("bus owned global", func(t *testing.T) {
		owner := newSourceMutationProbeOwner()
		bus := newSourceMutationProbeBus(t, owned, owner)
		result, err := bus.SweepPipelineObligations(context.Background(), 1)
		if err != nil {
			t.Fatalf("SweepPipelineObligations: %v", err)
		}
		if !result.Exhausted {
			t.Fatal("global sweep did not execute to exhaustion")
		}
		if got := owner.counts(); got.openScan != 1 || got.claimBatch != 1 || got.closeScan != 1 {
			t.Fatalf("global sweep mutations = %#v, want one open/batch/close", got)
		}
	})
	t.Run("context owned run release", func(t *testing.T) {
		owner := newSourceMutationProbeOwner()
		bus := newSourceMutationProbeBus(t, runtimecorrelation.BundleSourceFact{}, owner)
		ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), owned)
		result, err := bus.ReleaseRunQueue(ctx, uuid.NewString(), 1)
		if err != nil {
			t.Fatalf("ReleaseRunQueue: %v", err)
		}
		if !result.Exhausted {
			t.Fatal("run release did not execute to exhaustion")
		}
		if got := owner.counts(); got.openScan != 1 || got.claimBatch != 1 || got.closeScan != 1 {
			t.Fatalf("run release mutations = %#v, want one open/batch/close", got)
		}
	})
	t.Run("context owned periodic", func(t *testing.T) {
		owner := newSourceMutationProbeOwner()
		bus := newSourceMutationProbeBus(t, runtimecorrelation.BundleSourceFact{}, owner)
		ctx, cancel := context.WithCancel(runtimecorrelation.WithBundleSourceFact(context.Background(), owned))
		if err := bus.StartOutboxSweeper(ctx, OutboxSweeperConfig{Interval: time.Hour, Limit: 1}); err != nil {
			cancel()
			t.Fatalf("StartOutboxSweeper: %v", err)
		}
		select {
		case <-owner.opened:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("periodic sweeper did not execute")
		}
		cancel()
		waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
		defer waitCancel()
		if err := bus.WaitForOutboxSweeper(waitCtx); err != nil {
			t.Fatalf("WaitForOutboxSweeper: %v", err)
		}
		if got := owner.counts(); got.openScan != 1 || got.claimBatch != 1 || got.closeScan != 1 {
			t.Fatalf("periodic sweep mutations = %#v, want one open/batch/close", got)
		}
	})
}
