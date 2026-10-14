package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type receiverCompletionResult struct {
	candidate runlifecycle.Candidate
	result    runlifecycle.CompletionResult
	err       error
}

// Observe the real candidate owner and executor; do not replace their decisions
// or stop completion work to manufacture an exclusive diagnostic writer.
type receiverCompletionBoundary struct {
	runlifecycle.CandidateOwner
	sourceOnce sync.Once
	mu         sync.Mutex
	executor   *runlifecycle.Executor
	pending    int
	executing  int
	version    uint64
	changed    chan struct{}
	results    []receiverCompletionResult
	holdRun    string
	entered    chan runlifecycle.Candidate
	release    <-chan struct{}
	held       bool
}

func installReceiverCompletionBoundary(t *testing.T) *receiverCompletionBoundary {
	t.Helper()
	boundary := &receiverCompletionBoundary{changed: make(chan struct{})}
	previous := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		persistence := previous(owner)
		boundary.sourceOnce.Do(func() { boundary.CandidateOwner = persistence.deps.RunLifecycleCandidates })
		persistence.deps.RunLifecycleCandidates = boundary
		return persistence
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
	return boundary
}

func (b *receiverCompletionBoundary) notifyLocked() {
	b.version++
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *receiverCompletionBoundary) RegisterCompletionCandidateSink(ctx context.Context, scope runlifecycle.CandidateScope, sink runlifecycle.CandidateSink) (runlifecycle.CandidateRegistration, error) {
	executor, ok := sink.(*runlifecycle.Executor)
	if !ok {
		return nil, fmt.Errorf("receiver completion boundary requires the live executor, got %T", sink)
	}
	b.mu.Lock()
	b.executor = executor
	b.mu.Unlock()
	return b.CandidateOwner.RegisterCompletionCandidateSink(ctx, scope, receiverCompletionSink{boundary: b, sink: sink})
}

type receiverCompletionSink struct {
	boundary *receiverCompletionBoundary
	sink     runlifecycle.CandidateSink
}

func (s receiverCompletionSink) ReserveCompletionCandidate(ctx context.Context) (runlifecycle.CandidateAdmission, error) {
	s.boundary.mu.Lock()
	s.boundary.pending++
	s.boundary.notifyLocked()
	s.boundary.mu.Unlock()
	admission, err := s.sink.ReserveCompletionCandidate(ctx)
	if err != nil {
		s.boundary.finishReservation()
		return nil, err
	}
	return &receiverCompletionAdmission{CandidateAdmission: admission, boundary: s.boundary}, nil
}

type receiverCompletionAdmission struct {
	runlifecycle.CandidateAdmission
	boundary *receiverCompletionBoundary
	once     sync.Once
}

func (a *receiverCompletionAdmission) Submit(candidate runlifecycle.Candidate) error {
	err := a.CandidateAdmission.Submit(candidate)
	a.once.Do(a.boundary.finishReservation)
	return err
}

func (a *receiverCompletionAdmission) Cancel() error {
	err := a.CandidateAdmission.Cancel()
	a.once.Do(a.boundary.finishReservation)
	return err
}

func (b *receiverCompletionBoundary) finishReservation() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending--
	b.notifyLocked()
}

func (b *receiverCompletionBoundary) ExecuteCompletionCandidate(ctx context.Context, candidate runlifecycle.Candidate, catalog runlifecycle.FinalCatalog) (runlifecycle.CompletionResult, error) {
	b.mu.Lock()
	b.executing++
	hold := (b.holdRun == candidate.RunID || b.holdRun == "next") && !b.held
	if hold {
		b.held = true
	}
	b.notifyLocked()
	b.mu.Unlock()
	if hold {
		b.entered <- candidate
		<-b.release
	}
	result, err := b.CandidateOwner.ExecuteCompletionCandidate(ctx, candidate, catalog)
	b.mu.Lock()
	b.results = append(b.results, receiverCompletionResult{candidate: candidate, result: result, err: err})
	b.executing--
	b.notifyLocked()
	b.mu.Unlock()
	return result, err
}

func (b *receiverCompletionBoundary) wait(ctx context.Context, bundleHash string) ([]receiverCompletionResult, error) {
	// The predicate is exact owner accounting, not an arbitrary quiet interval.
	// The executor removes its chain just after the observed native return.
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		b.mu.Lock()
		version, idle, changed, executor := b.version, b.pending == 0 && b.executing == 0, b.changed, b.executor
		b.mu.Unlock()
		if idle && executor != nil && executor.ActiveCandidates() == 0 {
			page, err := b.ListCompletionCandidates(ctx, runlifecycle.CandidateScope{BundleHash: bundleHash}, runlifecycle.CandidateCursor{}, 128)
			if err != nil {
				return nil, err
			}
			b.mu.Lock()
			settled := version == b.version && b.pending == 0 && b.executing == 0 && page.Exhausted && len(page.Candidates) == 0 && executor.ActiveCandidates() == 0
			results := append([]receiverCompletionResult(nil), b.results...)
			b.mu.Unlock()
			if settled {
				for _, receipt := range results {
					if receipt.err != nil || !receipt.result.Committed || receipt.result.Validate() != nil || receipt.result.Outcome == runlifecycle.OutcomeRetryCurrent || receipt.result.Outcome == runlifecycle.OutcomeRearmAt {
						return nil, fmt.Errorf("completion boundary contains unsettled result: candidate=%+v result=%+v err=%v", receipt.candidate, receipt.result, receipt.err)
					}
				}
				return results, nil
			}
		}
		select {
		case <-changed:
		case <-tick.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func requireReceiverCompletionBoundary(t *testing.T, ctx context.Context, boundary *receiverCompletionBoundary, bundleHash, seedRun string) []receiverCompletionResult {
	t.Helper()
	results, err := boundary.wait(ctx, bundleHash)
	if err != nil {
		t.Fatalf("join receiver completion writers before conservation snapshot: %v", err)
	}
	seedSettled := false
	for _, receipt := range results {
		seed := receipt.candidate.RunID == seedRun
		t.Logf("completion writer settled before held-origin snapshot: seed=%t run=%s revision=%d due=%s outcome=%s committed=%t", seed, receipt.candidate.RunID, receipt.candidate.Revision, receipt.candidate.DueAt.Format(time.RFC3339Nano), receipt.result.Outcome, receipt.result.Committed)
		seedSettled = seedSettled || seed && receipt.result.Outcome == runlifecycle.OutcomeAwaitMutation
	}
	if !seedSettled {
		t.Fatal("receiver conservation lacks its seed run's committed await-mutation completion outcome")
	}
	return results
}

func TestServedReceiverCompletionSnapshotBoundaryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, provider := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/provider=%t", backend, provider), func(t *testing.T) {
				probe := newExactOriginPublicationProbe(t)
				originRelease, completionRelease := make(chan struct{}), make(chan struct{})
				releaseOrigin := sync.OnceFunc(func() { close(originRelease) })
				releaseCompletion := sync.OnceFunc(func() { close(completionRelease) })
				defer releaseOrigin()
				defer releaseCompletion()
				root, event := canonicalrouting.CopyServedReceiverInitialization(t), "work.requested"
				if provider {
					root, event = canonicalrouting.CopyProviderReceiverInitialization(t), "inbound.telegram.text_message"
				}
				completion := installReceiverCompletionBoundary(t)
				process, rt := startServedBoundaryFixture(t, backend, root)
				ctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)
				defer cancel()
				if _, err := completion.wait(ctx, rt.BundleHash); err != nil {
					t.Fatal(err)
				}
				completion.mu.Lock()
				completion.holdRun, completion.entered, completion.release = "next", make(chan runlifecycle.Candidate, 1), completionRelease
				completion.mu.Unlock()
				runtime := servedTestProcessRuntime(t, process)
				hold := &exactOriginDiagnosticHold{logger: runtime.Logger, eventType: event, entered: make(chan string, 1), release: originRelease}
				runtime.Bus.SetLoggerHook(hold)
				params := map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "idempotency_key": "typed-init-completion-boundary",
					"payload": map[string]any{"account_id": "boundary-account", "values": json.RawMessage(`{"active":true,"label":"kept","attributes":{}}`)}}
				if provider {
					params = map[string]any{"bundle_hash": rt.BundleHash, "event_name": event, "idempotency_key": "completion-boundary", "payload": map[string]any{"conversation_reference": "2307", "conversation_scope": "direct", "external_account_reference": "2307", "provider_message_reference": 7, "text": "direct initialization"}}
				}
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				waitServedBoundaryPublicationCompletion(t, rt, seed.RunID)
				var candidate runlifecycle.Candidate
				select {
				case candidate = <-completion.entered:
				case <-ctx.Done():
					t.Fatal("seed completion did not reach its native execution boundary")
				}
				if candidate.RunID != seed.RunID {
					t.Fatalf("controlled completion borrowed another run: candidate=%+v seed=%+v", candidate, seed)
				}
				canceled, cancelWait := context.WithCancel(ctx)
				cancelWait()
				if _, err := completion.wait(canceled, rt.BundleHash); !errors.Is(err, context.Canceled) {
					t.Fatalf("completion boundary accepted an in-flight native candidate: %v", err)
				}
				before := requireReceiverApplicationSnapshot(t, rt.ReceiverStateReader)
				old := receiverCompletionRunRow(t, before, seed.RunID)
				if string(old["completion_due_at"].Value) == "null" {
					t.Fatal("old publication boundary did not retain its unexecuted completion candidate")
				}
				releaseCompletion()
				receipts := requireReceiverCompletionBoundary(t, ctx, completion, rt.BundleHash, seed.RunID)
				after := requireReceiverApplicationSnapshot(t, rt.ReceiverStateReader)
				current := receiverCompletionRunRow(t, after, seed.RunID)
				if string(current["completion_due_at"].Value) != "null" || reflect.DeepEqual(old, current) {
					t.Fatalf("old snapshot boundary did not expose candidate settlement: before=%+v after=%+v", old, current)
				}
				old["completion_due_at"] = current["completion_due_at"]
				if !reflect.DeepEqual(old, current) {
					t.Fatal("controlled candidate changed another run field")
				}
				var revision int64
				if err := json.Unmarshal(current["completion_revision"].Value, &revision); err != nil {
					t.Fatal(err)
				}
				matched := false
				for _, receipt := range receipts {
					matched = matched || receipt.candidate.RunID == seed.RunID && receipt.candidate.Revision == revision && receipt.result.Outcome == runlifecycle.OutcomeAwaitMutation
				}
				if !matched {
					t.Fatalf("cleared completion due lacks its exact committed run/revision acknowledgment: run=%s revision=%d", seed.RunID, revision)
				}
				for table, rows := range before {
					if table != "runs" && !reflect.DeepEqual(rows, after[table]) {
						t.Fatalf("controlled completion changed retained application work: %s", table)
					}
				}
				instances, err := rt.ReceiverStateReader.ListWorkflowInstances(ctx, seed.RunID)
				if err != nil || len(instances) == 0 {
					t.Fatalf("completion lost its fieldless-stage workflow owners: %v %v", instances, err)
				}
				for _, instance := range instances {
					if instance.StageDefined {
						t.Fatal("controlled await-mutation fixture acquired a terminal-stage descriptor")
					}
				}
				t.Logf("old boundary counterexample: exact seed run=%s revision=%d outcome=await_mutation blocking_reason=no_stage_defined_entities; all delivery/handoff/receipt gates passed and instance rows remain", seed.RunID, revision)
				requireReceiverInitializationHeldOrigin(t, rt.ReceiverStateReader, rt.Endpoint, probe, hold, seed, releaseOrigin, completion, rt.BundleHash)
			})
		}
	}
}

func receiverCompletionRunRow(t *testing.T, snapshot map[string][]string, runID string) map[string]storetest.SelectedForkSnapshotCell {
	t.Helper()
	var columns []string
	if err := json.Unmarshal([]byte(snapshot["runs/columns"][0]), &columns); err != nil {
		t.Fatal(err)
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(storetest.SelectedForkStorageTableSnapshot{Columns: columns, Rows: snapshot["runs"]})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		id, err := workspaceProofPhysicalText(row, "run_id")
		if err != nil {
			t.Fatal(err)
		}
		if id == runID {
			return row
		}
	}
	t.Fatalf("completion snapshot lost exact run %s", runID)
	return nil
}
