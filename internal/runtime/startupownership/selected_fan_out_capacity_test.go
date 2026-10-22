package startupownership

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

type selectedFanOutCapacitySession struct {
	*fanOutCapacitySession
	binding SelectedForkGrantBinding
	revoked atomic.Bool
}

func TestIssue642SelectedSharedServingRecoversWithoutAnyWake(t *testing.T) {
	release := make(chan struct{})
	close(release)
	f := newServingFixture(t, 1, release)
	ctx := context.Background()
	fingerprint := "sha256:" + strings.Repeat("1", 64)
	binding := SelectedForkGrantBinding{
		BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), ExecutionID: uuid.NewString(),
		ExecutionGeneration: 1, FenceGeneration: 1, ExecutionOwner: "selected-worker",
		AdmissionFingerprint: fingerprint, ContainerPlanFingerprint: fingerprint, ActorCensusFingerprint: fingerprint,
		EffectiveConfigFingerprint: fingerprint, DeclarationPlanFingerprint: fingerprint, PreparationFingerprint: fingerprint,
	}
	session := f.process.(*processCapability).session.(*servingSessionProbe)
	session.selected = &selectedFanOutCapacitySession{fanOutCapacitySession: session.fanOutCapacitySession, binding: binding}
	grant, err := f.process.IssueSelectedForkGenerationGrant(ctx, SelectedForkGrantRequest{BundleHash: startupBundleHashB, RuntimeInstanceID: f.probe.authority.RuntimeInstanceID, Binding: binding})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.AdmitExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	occurrence, err := f.work.NewSelectedFork(ctx, worklifetime.SelectedForkIdentity{ExecutionID: binding.ExecutionID, RunID: binding.ForkRunID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occurrence.RetireAndWait(ctx) })
	f.store.mu.Lock()
	f.store.bound[evidence.GrantID] = &servingBoundProbe{grantID: evidence.GrantID}
	f.store.mu.Unlock()
	executor := &servingExecutorProbe{store: f.store, grantID: evidence.GrantID, started: f.started, release: release, errors: f.errors}
	r, err := StartFanOutServing(ctx, grant, occurrence, nil, executor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	scanned := make(chan struct{}, 1)
	r.SetTestScanObserver(func(_ FanOutCandidate, found bool, err error) {
		if !found && err == nil {
			select {
			case scanned <- struct{}{}:
			default:
			}
		}
	})
	r.Wake()
	select {
	case <-scanned:
	case <-time.After(time.Second):
		t.Fatal("selected registration's initial hint was not consumed")
	}
	key := fanoutobligation.IntentKey{RunID: binding.ForkRunID, TriggeringDeliveryID: uuid.NewString(),
		ElementRef: contracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "nodes.scatter"}}
	f.store.mu.Lock()
	f.store.queue = append(f.store.queue, FanOutCandidate{GrantID: evidence.GrantID, Key: key, CreatedAt: time.Now(), PositionAt: time.Now()})
	f.store.mu.Unlock()
	if got := f.waitStarted(t, 1400*time.Millisecond); got != key {
		t.Fatalf("selected recovery served %v, expected %v", got, key)
	}
	r.Close()
	if _, _, err := r.BeginTurn(ctx); err == nil {
		t.Fatal("selected no-wake registration escaped retirement")
	}
}

func (s *selectedFanOutCapacitySession) ProveSelectedForkGenerationGrant(_ context.Context, grant GrantEvidence) error {
	if s.revoked.Load() || grant.SelectedFork == nil || *grant.SelectedFork != s.binding {
		return errors.New("selected fan-out test executor no longer owns its binding")
	}
	return nil
}

func TestIssue642FanOutCapacityIncludesExactSelectedExecution(t *testing.T) {
	for _, workers := range []int{1, 2} {
		t.Run(string(rune('0'+workers)), func(t *testing.T) { testSelectedFanOutCapacity(t, workers) })
	}
}

func testSelectedFanOutCapacity(t *testing.T, workers int) {
	t.Helper()
	ctx := context.Background()
	probe, _ := testRetainedSession(t)
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: startupBundleHashA}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	probe.plan = plan
	binding := SelectedForkGrantBinding{
		BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), ExecutionID: uuid.NewString(),
		ExecutionGeneration: 2, FenceGeneration: 3, ExecutionOwner: "selected-worker",
		AdmissionFingerprint: "sha256:" + strings.Repeat("1", 64), ContainerPlanFingerprint: "sha256:" + strings.Repeat("2", 64),
		ActorCensusFingerprint: "sha256:" + strings.Repeat("3", 64), EffectiveConfigFingerprint: "sha256:" + strings.Repeat("4", 64),
		DeclarationPlanFingerprint: "sha256:" + strings.Repeat("5", 64), PreparationFingerprint: "sha256:" + strings.Repeat("6", 64),
	}
	capacity := SQLiteFanOutCapacity()
	if workers > 1 {
		capacity, err = PostgreSQLFanOutCapacity(6, 1)
		if err != nil {
			t.Fatal(err)
		}
	}
	session := &selectedFanOutCapacitySession{fanOutCapacitySession: &fanOutCapacitySession{retainedSessionProbe: probe, capacity: capacity}, binding: binding}
	process, err := newProcessCapability(session, time.Hour, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer process.Release(ctx)
	work := worklifetime.NewProcess()
	defer func() {
		work.Retire()
		cleanup, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if _, err := work.Join(cleanup); err != nil {
			t.Error(err)
		}
	}()
	ordinary, err := process.IssueGenerationGrant(ctx, GrantRequest{BundleHash: startupBundleHashA, RuntimeInstanceID: probe.authority.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := process.IssueSelectedForkGenerationGrant(ctx, SelectedForkGrantRequest{BundleHash: startupBundleHashB, RuntimeInstanceID: probe.authority.RuntimeInstanceID, Binding: binding})
	if err != nil {
		t.Fatal(err)
	}
	identity := worklifetime.SelectedForkIdentity{ExecutionID: binding.ExecutionID, RunID: binding.ForkRunID, Generation: binding.ExecutionGeneration}
	occurrence, err := work.NewSelectedFork(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer occurrence.RetireAndWait(ctx)
	for _, grant := range []GenerationGrant{ordinary, selected} {
		if grant == selected {
			if r, err := RegisterFanOutServing(ctx, grant, occurrence, nil); err == nil {
				r.Close()
				t.Fatal("prepared selected grant admitted serving")
			}
		}
		if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if grant == selected {
			if r, err := RegisterFanOutServing(ctx, grant, occurrence, nil); err == nil {
				r.Close()
				t.Fatal("probe-settled selected grant admitted serving")
			}
		}
		if _, err := grant.AdmitExecution(ctx); err != nil {
			t.Fatal(err)
		}
	}
	runtime, err := work.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: probe.authority.RuntimeInstanceID, BundleHash: startupBundleHashA})
	if err != nil {
		t.Fatal(err)
	}
	defer retireFanOutTestRuntime(t, runtime)
	live, err := RegisterFanOutServing(ctx, ordinary, runtime, &workers)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	for _, change := range []func(*worklifetime.SelectedForkIdentity){
		func(i *worklifetime.SelectedForkIdentity) { i.ExecutionID = uuid.NewString() },
		func(i *worklifetime.SelectedForkIdentity) { i.RunID = uuid.NewString() },
		func(i *worklifetime.SelectedForkIdentity) { i.Generation++ },
	} {
		wrong := identity
		change(&wrong)
		occurrence, err := work.NewSelectedFork(ctx, wrong)
		if err != nil {
			t.Fatal(err)
		}
		if registration, err := RegisterFanOutServing(ctx, selected, occurrence, nil); err == nil {
			registration.Close()
			t.Error("selected registration accepted a foreign occurrence")
		}
		if err := occurrence.RetireAndWait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if registration, err := RegisterFanOutServing(ctx, selected, runtime, nil); err == nil {
		registration.Close()
		t.Fatal("selected grant borrowed an ordinary occurrence")
	}
	if registration, err := RegisterFanOutServing(ctx, ordinary, occurrence, nil); err == nil {
		registration.Close()
		t.Fatal("ordinary grant borrowed a selected occurrence")
	}
	r, err := RegisterFanOutServing(ctx, selected, occurrence, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if duplicate, err := RegisterFanOutServing(ctx, selected, occurrence, nil); err == nil {
		duplicate.Close()
		t.Fatal("duplicate selected registration multiplied capacity")
	}
	baseline := work.ActiveCount()
	var permits []*FanOutTurnPermit
	for i := 0; i < workers; i++ {
		permit, found, err := r.BeginTurn(ctx)
		if err != nil || !found {
			t.Fatalf("selected turn: found=%t error=%v", found, err)
		}
		permits = append(permits, permit)
		defer permit.Done()
	}
	if _, found, err := live.BeginTurn(ctx); err != nil || found {
		t.Fatalf("selected work did not consume the shared budget: found=%t error=%v", found, err)
	}
	owner, present := worklifetime.OccurrenceFromContext(permits[0].Context())
	if !present || owner != occurrence {
		t.Fatal("turn lost its exact selected occurrence")
	}
	for _, permit := range permits {
		permit.Done()
	}
	ordinaryTurn, found, err := live.BeginTurn(ctx)
	if err != nil || !found {
		t.Fatalf("shared capacity was not returned: found=%t error=%v", found, err)
	}
	ordinaryTurn.Done()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := r.BeginTurn(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled selected admission: %v", err)
	}
	session.revoked.Store(true)
	if _, _, err := r.BeginTurn(ctx); err == nil {
		t.Fatal("selected turn ignored fresh execution revocation")
	}
	r.Close()
	if _, _, err := r.BeginTurn(ctx); err == nil {
		t.Fatal("joined selected registration admitted new work")
	}
	if got := work.ActiveCount(); got != baseline {
		t.Fatalf("selected retirement leaked finite work: got=%d baseline=%d", got, baseline)
	}
}
