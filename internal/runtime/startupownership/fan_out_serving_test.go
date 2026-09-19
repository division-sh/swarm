package startupownership

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type servingSessionProbe struct {
	*fanOutCapacitySession
	store *servingStoreProbe
}

func (s *servingSessionProbe) FanOutServingStore() (FanOutServingStore, error) {
	return s.store, nil
}

type servingStoreProbe struct {
	mu            sync.Mutex
	queue         []FanOutCandidate
	bound         map[string]*servingBoundProbe
	completed     map[fanoutobligation.IntentKey]bool
	queries       int
	executionRows map[fanoutobligation.IntentKey]FanOutExecutionObservation
}

type servingBoundProbe struct {
	pipeline.FanOutObligationOwner
	grantID string
}

func (o *servingBoundProbe) ClaimFanOutIntent(_ context.Context, request pipeline.FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	if request.Owner != o.grantID || request.Candidate == nil {
		return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, fmt.Errorf("candidate routed to wrong runtime grant")
	}
	return fanoutobligation.Intent{}, fanoutobligation.Claim{Key: *request.Candidate}, true, nil
}

func (o *servingBoundProbe) CommitFanOutChunk(context.Context, pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	return pipeline.CommittedFanOutChunk{}, nil
}

func (s *servingStoreProbe) ObserveFanOutExecutions(_ context.Context, _ []FanOutRegistrationObservation, keys []fanoutobligation.IntentKey) (FanOutExecutionSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := FanOutExecutionSnapshot{ObservedAt: time.Now(), Rows: make([]FanOutExecutionObservation, len(keys))}
	for i, key := range keys {
		row, ok := s.executionRows[key]
		if !ok {
			row = FanOutExecutionObservation{Key: key, Reason: "runtime_not_registered"}
		}
		result.Rows[i] = row
	}
	return result, nil
}

func (s *servingStoreProbe) NextFanOutCandidate(_ context.Context, grants []FanOutRegistrationObservation, excluded []fanoutobligation.IntentKey) (FanOutCandidate, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries++
	for _, candidate := range s.queue {
		if s.completed[candidate.Key] || slices.Contains(excluded, candidate.Key) {
			continue
		}
		if slices.ContainsFunc(grants, func(g FanOutRegistrationObservation) bool { return !g.Closing && g.Grant.GrantID == candidate.GrantID }) {
			return candidate, true, nil
		}
	}
	return FanOutCandidate{}, false, nil
}

func (s *servingStoreProbe) BindFanOutGrant(g GrantEvidence) (pipeline.FanOutObligationOwner, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bound[g.GrantID], nil
}

type servingExecutorProbe struct {
	store               *servingStoreProbe
	grantID             string
	started             chan fanoutobligation.IntentKey
	release             <-chan struct{}
	errors              chan error
	beforeClaim         <-chan struct{}
	beforeCleanup       <-chan struct{}
	commitBeforeRelease bool
	committed           chan fanoutobligation.IntentKey
}

func (e *servingExecutorProbe) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	if correlation.RunIDFromContext(ctx) != key.RunID {
		return pipeline.FanOutTurnResult{}, fmt.Errorf("shared executor lost exact selected run correlation")
	}
	defer func() {
		if e.beforeCleanup != nil {
			<-e.beforeCleanup
		}
	}()
	if e.beforeClaim != nil {
		select {
		case <-ctx.Done():
			return pipeline.FanOutTurnResult{}, ctx.Err()
		case <-e.beforeClaim:
		}
	}
	if _, _, _, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: e.grantID, Candidate: &key}); err != nil {
		return pipeline.FanOutTurnResult{}, err
	}
	e.started <- key
	if e.commitBeforeRelease {
		if _, err := owner.CommitFanOutChunk(ctx, pipeline.FanOutChunkCommand{}); err != nil {
			return pipeline.FanOutTurnResult{}, err
		}
		e.committed <- key
	}
	select {
	case <-ctx.Done():
		return pipeline.FanOutTurnResult{}, ctx.Err()
	case <-e.release:
	}
	e.store.mu.Lock()
	e.store.completed[key] = true
	e.store.mu.Unlock()
	return pipeline.FanOutTurnResult{Refill: true}, nil
}

func (e *servingExecutorProbe) ReportFanOutServingError(ctx context.Context, err error) {
	failure := failures.Normalize(err, "", "")
	if failure.Detail.Code == "fan_out_claim_opportunity_missed" && failure.Detail.Attributes["run_id"] != correlation.RunIDFromContext(ctx) {
		err = fmt.Errorf("D3 log context lost the exact selected run: %v", err)
	}
	select {
	case e.errors <- err:
	default:
	}
}

type servingFixture struct {
	process       ProcessCapability
	work          *worklifetime.Process
	store         *servingStoreProbe
	registrations []*FanOutServingRegistration
	grants        []LiveGenerationGrant
	occurrences   []*worklifetime.RuntimeOccurrence
	started       chan fanoutobligation.IntentKey
	errors        chan error
	probe         *retainedSessionProbe
	committed     chan fanoutobligation.IntentKey
}

type servingProbeControls struct {
	beforeClaim         <-chan struct{}
	beforeCleanup       <-chan struct{}
	commitBeforeRelease bool
}

func newServingFixture(t *testing.T, workers int, release <-chan struct{}, controls ...servingProbeControls) *servingFixture {
	t.Helper()
	ctx := context.Background()
	probe, _ := testRetainedSession(t)
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: startupBundleHashA}, {BundleHash: startupBundleHashB}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	probe.plan = plan
	capacity := SQLiteFanOutCapacity()
	if workers > 1 {
		capacity, err = PostgreSQLFanOutCapacity(workers+2, 1)
		if err != nil {
			t.Fatal(err)
		}
	}
	f := &servingFixture{store: &servingStoreProbe{bound: make(map[string]*servingBoundProbe), completed: make(map[fanoutobligation.IntentKey]bool)}, work: worklifetime.NewProcess(), started: make(chan fanoutobligation.IntentKey, 64), errors: make(chan error, 64)}
	f.probe = probe
	f.committed = make(chan fanoutobligation.IntentKey, 64)
	f.process, err = newProcessCapability(&servingSessionProbe{fanOutCapacitySession: &fanOutCapacitySession{retainedSessionProbe: probe, capacity: capacity}, store: f.store}, time.Hour, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, r := range f.registrations {
			r.Close()
		}
		for _, occurrence := range f.occurrences {
			retireFanOutTestRuntime(t, occurrence)
		}
		if err := f.process.Release(context.Background()); err != nil {
			t.Error(err)
		}
		f.work.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := f.work.Join(ctx); err != nil {
			t.Error(err)
		}
	})
	for _, bundle := range []string{startupBundleHashA, startupBundleHashB} {
		grant, err := f.process.IssueGenerationGrant(ctx, GrantRequest{BundleHash: bundle, RuntimeInstanceID: probe.authority.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := grant.AdmitExecution(ctx); err != nil {
			t.Fatal(err)
		}
		occurrence, err := f.work.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: probe.authority.RuntimeInstanceID, BundleHash: bundle})
		if err != nil {
			t.Fatal(err)
		}
		f.occurrences = append(f.occurrences, occurrence)
		evidence, err := grant.Evidence()
		if err != nil {
			t.Fatal(err)
		}
		f.store.mu.Lock()
		f.store.bound[evidence.GrantID] = &servingBoundProbe{grantID: evidence.GrantID}
		f.store.mu.Unlock()
		executor := &servingExecutorProbe{store: f.store, grantID: evidence.GrantID, started: f.started, release: release, errors: f.errors, committed: f.committed}
		if len(controls) != 0 {
			executor.beforeClaim = controls[0].beforeClaim
			executor.beforeCleanup = controls[0].beforeCleanup
			executor.commitBeforeRelease = controls[0].commitBeforeRelease
		}
		r, err := StartFanOutServing(ctx, grant, occurrence, fanOutWorkers(workers), executor)
		if err != nil {
			t.Fatal(err)
		}
		f.registrations = append(f.registrations, r)
		f.grants = append(f.grants, grant)
	}
	return f
}

func (f *servingFixture) append(t *testing.T, registration int, runID string) fanoutobligation.IntentKey {
	t.Helper()
	evidence, err := f.grants[registration].Evidence()
	if err != nil {
		t.Fatal(err)
	}
	key := fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: uuid.NewString(), ElementRef: contracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "handlers/start/fan_out/0"}}
	f.store.mu.Lock()
	f.store.queue = append(f.store.queue, FanOutCandidate{GrantID: evidence.GrantID, Key: key, PositionAt: time.Now(), CreatedAt: time.Now()})
	f.store.mu.Unlock()
	return key
}

func (f *servingFixture) waitStarted(t *testing.T, timeout time.Duration) fanoutobligation.IntentKey {
	t.Helper()
	select {
	case key := <-f.started:
		return key
	case err := <-f.errors:
		t.Fatalf("serving failed: %v", err)
	case <-time.After(timeout):
		t.Fatal("eligible candidate did not enter execution")
	}
	return fanoutobligation.IntentKey{}
}

func TestFanOutSharedServingDrainsCoalescedCrossSourceBacklog(t *testing.T) {
	release := make(chan struct{})
	close(release)
	f := newServingFixture(t, 1, release)
	const count = 24
	for i := 0; i < count; i++ {
		f.append(t, i%2, uuid.NewString())
	}
	// One hint, many completed short intents. The production one-second ticker
	// cannot make this pass the sub-tick bound; refill must follow completion.
	f.registrations[0].Wake()
	deadline := time.Now().Add(750 * time.Millisecond)
	for i := 0; i < count; i++ {
		f.waitStarted(t, time.Until(deadline))
	}
}

func TestFanOutSharedServingRecoversWithoutAnyWake(t *testing.T) {
	release := make(chan struct{})
	close(release)
	f := newServingFixture(t, 1, release)
	// Wait until the registration hints have been consumed, then add durable
	// work without publishing any hint. Do not shorten the production sweep.
	deadline := time.Now().Add(time.Second)
	for {
		f.store.mu.Lock()
		queries := f.store.queries
		f.store.mu.Unlock()
		if queries > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("initial query not observed")
		}
		time.Sleep(time.Millisecond)
	}
	expected := f.append(t, 1, uuid.NewString())
	if actual := f.waitStarted(t, 1400*time.Millisecond); actual != expected {
		t.Fatalf("recovery selected %v, want %v", actual, expected)
	}
}

func TestFanOutSharedServingCapacityAndWorkConservation(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			release := make(chan struct{})
			f := newServingFixture(t, workers, release)
			defer close(release)
			firstRun := uuid.NewString()
			first := f.append(t, 0, firstRun)
			second := f.append(t, 0, firstRun)
			for i := 0; i < workers+1; i++ {
				f.append(t, 1, uuid.NewString())
			}
			f.registrations[0].Wake()
			seen := make(map[fanoutobligation.IntentKey]bool)
			for i := 0; i < workers; i++ {
				key := f.waitStarted(t, 750*time.Millisecond)
				if seen[key] {
					t.Fatal("one exact intent occupied multiple shared worker slots")
				}
				seen[key] = true
			}
			if !seen[first] {
				t.Fatal("global oldest candidate was not served")
			}
			if workers > 1 && !seen[second] {
				t.Fatal("an unrelated hard per-run cap displaced the next oldest intent")
			}
			select {
			case key := <-f.started:
				t.Fatalf("worker cap multiplied across registrations: %v", key)
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}

func TestFanOutSharedServingRetiresExactGenerationWithoutLendingItsSlot(t *testing.T) {
	release := make(chan struct{})
	f := newServingFixture(t, 1, release)
	defer close(release)
	first := f.append(t, 0, uuid.NewString())
	second := f.append(t, 1, uuid.NewString())
	f.registrations[0].Wake()
	if key := f.waitStarted(t, time.Second); key != first {
		t.Fatal("wrong first runtime")
	}
	if err := f.grants[0].Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The retired executor returns through its normal disposition. Recovery
	// then spends the returned slot on the other source, never on the old one.
	deadline := time.After(1500 * time.Millisecond)
	for {
		select {
		case key := <-f.started:
			if key != second {
				t.Fatalf("retired source served again: %v", key)
			}
			return
		case <-f.errors: // expected cancellation from the retired execution
		case <-deadline:
			t.Fatal("healthy source did not reclaim the settled slot")
		}
	}
}

func TestFanOutSharedServingDetectorMeasuresActualClaimEntry(t *testing.T) {
	release := make(chan struct{})
	close(release)
	beforeClaim := make(chan struct{})
	defer close(beforeClaim)
	f := newServingFixture(t, 1, release, servingProbeControls{beforeClaim: beforeClaim})
	key := f.append(t, 0, uuid.NewString())
	f.registrations[0].Wake()
	select {
	case err := <-f.errors:
		failure := failures.Normalize(err, "", "")
		if failure.Detail.Code != "fan_out_claim_opportunity_missed" || failure.Detail.Attributes["run_id"] != key.RunID || failure.Detail.Attributes["triggering_delivery_id"] != key.TriggeringDeliveryID {
			t.Fatalf("wrong opportunity incident: %+v", failure)
		}
	case <-time.After(2200 * time.Millisecond):
		t.Fatal("worker callback was mistaken for claim entry; D3 was not reported")
	}
	select {
	case err := <-f.errors:
		t.Fatalf("same missed opportunity reported twice: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestFanOutSharedServingJoinsDispositionBeforeStoreRelease(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	cleanup := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(cleanup) })
	f := newServingFixture(t, 1, release, servingProbeControls{beforeCleanup: cleanup})
	f.append(t, 0, uuid.NewString())
	f.registrations[0].Wake()
	f.waitStarted(t, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := f.process.Release(ctx); err == nil {
		t.Fatal("process released while accepted turn disposition was outstanding")
	}
	f.probe.mu.Lock()
	released := f.probe.released
	f.probe.mu.Unlock()
	if released {
		t.Fatal("store possession released before worker join")
	}
	if _, _, err := f.registrations[1].BeginTurn(context.Background()); err == nil {
		t.Fatal("closing service admitted a new turn")
	}
	once.Do(func() { close(cleanup) })
	if err := f.process.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.probe.mu.Lock()
	released = f.probe.released
	f.probe.mu.Unlock()
	if !released {
		t.Fatal("joined process retained store possession")
	}
}

func TestFanOutOpportunityEpisodesAreExactAndExcludeUnknownState(t *testing.T) {
	s := &fanOutServingService{owner: &processCapability{fanOutCapacity: &fanOutCapacityState{limit: 4}}, claimEntered: make(map[fanoutobligation.IntentKey]bool)}
	a := FanOutCandidate{GrantID: "grant-a", Key: fanoutobligation.IntentKey{RunID: uuid.NewString(), TriggeringDeliveryID: uuid.NewString()}}
	b := a
	b.GrantID = "grant-b"
	now := time.Now()
	if s.observeOpportunity(a, true, now) || s.observeOpportunity(a, true, now.Add(time.Second)) {
		t.Fatal("incident emitted before exceeding the exact bound")
	}
	// An unrelated actual claim must not clear the original candidate's age.
	s.claimEntered[fanoutobligation.IntentKey{RunID: uuid.NewString()}] = true
	if !s.observeOpportunity(a, true, now.Add(time.Second+time.Nanosecond)) {
		t.Fatal("unrelated attempt hid this candidate's missed opportunity")
	}
	if s.observeOpportunity(a, true, now.Add(2*time.Second)) {
		t.Fatal("duplicate episode incident")
	}
	if s.observeOpportunity(b, true, now.Add(3*time.Second)) {
		t.Fatal("new generation inherited the old generation's observation age")
	}
	s.observeOpportunity(FanOutCandidate{}, false, now.Add(4*time.Second))
	if s.observeOpportunity(b, true, now.Add(5*time.Second)) {
		t.Fatal("unknown interval counted as continuous eligibility")
	}
	s.claimEntered[b.Key] = true
	if s.observeOpportunity(b, true, now.Add(7*time.Second)) {
		t.Fatal("actual claim entry was classified as silence")
	}
}
