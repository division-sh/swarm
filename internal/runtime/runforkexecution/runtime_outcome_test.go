package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Errors are injected after real successful store calls, not as simulated ACK loss.
type selectedRuntimeOutcomeProbe struct {
	SelectedContractRuntimeExecutionLifecycle
	SelectedContractForkLifecycle
	SelectedContractReplayPersistence
	mu                                                   sync.Mutex
	t                                                    *testing.T
	prepared                                             *PreparedSelectedFork
	issueErr, claimErr, failErr, closeErr, activationErr error
	materializationErr                                   error
	sourceErr, discardErr                                error
	materializationCommitted, sourceCommitted            bool
	refuseMaterialization                                bool
	refuseIssue, refuseClaim                             bool
	cancelClaim                                          context.CancelFunc
	issued                                               runfork.SelectedContractRuntimeExecution
	authority                                            runtimeeffects.Authority
	issues, claims, fails, closes, activations, discards int
	loads, commits                                       int
	publications                                         []selectedRuntimeOutcomePublication
	committedCloses                                      []string
	diagnostics                                          *selectedForkCommitDiagnostics
	activatedFailures                                    int
}

type selectedRuntimeOutcomePublication struct {
	request runtimebus.CommitSelectedForkEventRequest
	result  runtimebus.CommittedSelectedForkEvent
	err     error
}

type selectedRuntimeOutcomeSnapshot struct {
	calls             []int
	issued            runfork.SelectedContractRuntimeExecution
	authority         runtimeeffects.Authority
	publications      []selectedRuntimeOutcomePublication
	committedCloses   []string
	activatedFailures int
}

func (p *selectedRuntimeOutcomeProbe) snapshot() selectedRuntimeOutcomeSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return selectedRuntimeOutcomeSnapshot{
		calls:  []int{p.issues, p.claims, p.fails, p.closes, p.activations, p.discards, p.loads, p.commits},
		issued: p.issued, authority: p.authority,
		publications:      append([]selectedRuntimeOutcomePublication(nil), p.publications...),
		committedCloses:   append([]string(nil), p.committedCloses...),
		activatedFailures: p.activatedFailures,
	}
}

func requireSelectedRuntimeOutcomeErrors(t *testing.T, phase string, got error, wants ...error) {
	t.Helper()
	wantError := false
	for _, want := range wants {
		if want != nil {
			wantError = true
			if !errors.Is(got, want) {
				t.Errorf("%s error %v lost %v", phase, got, want)
			}
		}
	}
	if !wantError && got != nil {
		t.Errorf("unexpected %s error: %v", phase, got)
	}
}

func requireSelectedRuntimeOutcomeDurableState(t *testing.T, ctx context.Context, selected SelectedContractForkLifecycle, forkID, phase, wantState, wantStatus string) {
	t.Helper()
	storage, err := storetest.ReadSelectedExecutionStorage(ctx, selected, forkID)
	if err != nil || storage.State != wantState || storage.RunStatus != wantStatus || storage.Occurrences != 1 {
		t.Fatalf("%s durable storage=%+v err=%v, want state=%q status=%q occurrences=1", phase, storage, err, wantState, wantStatus)
	}
}

type selectedMaterializationCommitTestError struct {
	value runfork.RunForkMaterialization
	cause error
}

func (e selectedMaterializationCommitTestError) Error() string { return e.cause.Error() }
func (e selectedMaterializationCommitTestError) Unwrap() error { return e.cause }
func (e selectedMaterializationCommitTestError) SelectedForkMaterializationCommit() runfork.RunForkMaterialization {
	return e.value
}

type selectedSourceEventsCommitTestError struct {
	sourceRunID, forkRunID string
	value                  []runfork.RunForkSelectedContractSourceEvent
	cause                  error
}

func (e selectedSourceEventsCommitTestError) Error() string { return e.cause.Error() }
func (e selectedSourceEventsCommitTestError) Unwrap() error { return e.cause }
func (e selectedSourceEventsCommitTestError) SelectedForkSourceEventsCommit() (string, string, []runfork.RunForkSelectedContractSourceEvent) {
	return e.sourceRunID, e.forkRunID, e.value
}

func (p *selectedRuntimeOutcomeProbe) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	if p.refuseMaterialization {
		return runfork.RunForkMaterialization{ForkRunID: uuid.NewString()}, p.materializationErr
	}
	value, err := p.SelectedContractForkLifecycle.MaterializeRunForkForSelectedContractExecution(ctx, req)
	if err != nil {
		p.t.Fatalf("real materialization must succeed before outcome injection: %v", err)
	}
	if p.materializationErr == nil {
		return value, nil
	}
	if p.materializationCommitted {
		return value, selectedMaterializationCommitTestError{value: value, cause: p.materializationErr}
	}
	return value, p.materializationErr
}

func (p *selectedRuntimeOutcomeProbe) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, req runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error) {
	p.mu.Lock()
	p.issues++
	p.mu.Unlock()
	if p.refuseIssue {
		return runfork.SelectedContractRuntimeExecution{}, p.issueErr
	}
	issued, err := p.SelectedContractRuntimeExecutionLifecycle.IssueRunForkSelectedContractRuntimeExecution(ctx, req)
	if err != nil {
		p.t.Fatalf("real issuance must succeed before outcome injection: %v", err)
	}
	p.mu.Lock()
	p.issued = issued
	p.mu.Unlock()
	return issued, p.issueErr
}

func (p *selectedRuntimeOutcomeProbe) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, issued runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (runtimeeffects.Authority, error) {
	p.mu.Lock()
	p.claims++
	p.mu.Unlock()
	if p.refuseClaim {
		return runtimeeffects.Authority{}, p.claimErr
	}
	authority, err := p.SelectedContractRuntimeExecutionLifecycle.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, owner, lease)
	if err != nil {
		p.t.Fatalf("real claim must succeed before outcome injection: %v", err)
	}
	p.mu.Lock()
	p.authority = authority
	p.mu.Unlock()
	if p.cancelClaim != nil {
		p.cancelClaim()
	}
	return authority, p.claimErr
}

func (p *selectedRuntimeOutcomeProbe) FailRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, failure json.RawMessage) error {
	p.mu.Lock()
	p.fails++
	wantAuthority := p.authority
	p.mu.Unlock()
	if ctx.Err() != nil || !reflect.DeepEqual(authority, wantAuthority) {
		p.t.Fatal("failure settlement lost exact authority or inherited caller cancellation")
	}
	if p.failErr != nil {
		return p.failErr
	}
	return p.SelectedContractRuntimeExecutionLifecycle.FailRunForkSelectedContractRuntimeExecution(ctx, authority, failure)
}

func (p *selectedRuntimeOutcomeProbe) FailActivatedRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, failure json.RawMessage) (bool, error) {
	p.mu.Lock()
	p.activatedFailures++
	wantAuthority := p.authority
	p.mu.Unlock()
	if ctx.Err() != nil || !reflect.DeepEqual(authority, wantAuthority) {
		p.t.Error("activated failure settlement lost exact authority or inherited caller cancellation")
		return false, errors.New("activated failure settlement lost exact authority or inherited caller cancellation")
	}
	return p.SelectedContractRuntimeExecutionLifecycle.FailActivatedRunForkSelectedContractRuntimeExecution(ctx, authority, failure)
}

func (p *selectedRuntimeOutcomeProbe) CloseRunForkSelectedContractRuntimeExecution(ctx context.Context, id string) (bool, error) {
	p.mu.Lock()
	p.closes++
	wantID := p.authority.ID
	p.mu.Unlock()
	if ctx.Err() != nil || id != wantID {
		p.t.Error("close lost execution identity or inherited caller cancellation")
		return false, errors.New("close lost execution identity or inherited caller cancellation")
	}
	acknowledged, err := p.SelectedContractRuntimeExecutionLifecycle.CloseRunForkSelectedContractRuntimeExecution(ctx, id)
	if !acknowledged {
		p.t.Errorf("real close must acknowledge before outcome injection: %v", err)
		return acknowledged, err
	}
	p.mu.Lock()
	p.committedCloses = append(p.committedCloses, id)
	p.mu.Unlock()
	return acknowledged, errors.Join(err, p.closeErr)
}

func (p *selectedRuntimeOutcomeProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	p.mu.Lock()
	p.activations++
	p.mu.Unlock()
	activation, err := p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
	if err != nil {
		p.t.Fatalf("real activation must succeed before outcome injection: %v", err)
	}
	if activation.Activated {
		p.prepared.bindMu.Lock()
		if runtime := p.prepared.retainedRuntime; runtime != nil {
			p.diagnostics = runtime.completionDiagnostics
		}
		p.prepared.bindMu.Unlock()
	}
	return activation, p.activationErr
}

func (p *selectedRuntimeOutcomeProbe) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, id string) error {
	p.mu.Lock()
	p.discards++
	discardErr := p.discardErr
	p.mu.Unlock()
	if ctx.Err() != nil {
		p.t.Fatal("discard inherited caller cancellation")
	}
	if discardErr != nil {
		return discardErr
	}
	return p.SelectedContractForkLifecycle.DiscardMaterializedSelectedContractExecutionFork(ctx, id)
}

func (p *selectedRuntimeOutcomeProbe) LoadRunForkSelectedContractSourceEvents(ctx context.Context, source, fork string, ids []string, original semanticview.OriginalLoopCarriage, originalRoot sourceartifact.RootAdmission) ([]runfork.RunForkSelectedContractSourceEvent, error) {
	p.mu.Lock()
	p.loads++
	p.mu.Unlock()
	events, err := p.SelectedContractReplayPersistence.LoadRunForkSelectedContractSourceEvents(ctx, source, fork, ids, original, originalRoot)
	if err != nil {
		p.t.Fatalf("real event preparation must succeed before outcome injection: %v", err)
	}
	if p.sourceErr != nil && p.sourceCommitted {
		return events, selectedSourceEventsCommitTestError{sourceRunID: source, forkRunID: fork, value: events, cause: p.sourceErr}
	}
	return events, p.sourceErr
}

func (p *selectedRuntimeOutcomeProbe) CommitSelectedForkEvent(ctx context.Context, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	p.mu.Lock()
	p.commits++
	p.mu.Unlock()
	committed, err := p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, req)
	p.mu.Lock()
	p.publications = append(p.publications, selectedRuntimeOutcomePublication{request: req, result: committed, err: err})
	p.mu.Unlock()
	return committed, err
}

func materializeSelectedRuntimeOutcomeFork(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, request SelectedContractExecutionRequest) runfork.RunForkMaterialization {
	t.Helper()
	prepared, err := owner.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.completePreparation(prepared); err != nil {
			t.Error(err)
		}
	}()
	materialized, acknowledged, err := owner.materializePrepared(prepared.operation.PreparationContext(), prepared)
	if err != nil || !acknowledged {
		t.Fatal(err)
	}
	return materialized
}

func TestSelectedForkRuntimeConsumersSettleReturnedOutcomes(t *testing.T) {
	operationErr := errors.New("acknowledged operation cleanup")
	settlementErr := errors.New("independent settlement failure")
	for _, backend := range []string{"postgres", "sqlite"} {
		for _, tc := range []struct {
			name                                                    string
			issue, claim, fail, close, activation, source           error
			materialization, discard                                error
			materializationCommitted, sourceCommitted               bool
			refuseMaterialization, refuseIssue, refuseClaim, cancel bool
			state, status                                           string
			activate                                                bool
		}{
			{name: "healthy", activate: true},
			{name: "issue_refused", issue: operationErr, refuseIssue: true},
			{name: "issue_acknowledged_error", issue: operationErr, state: "prepared", status: "cancelled"},
			{name: "issue_discard_error", issue: operationErr, discard: settlementErr, state: "prepared", status: "paused"},
			{name: "claim_refused", claim: operationErr, refuseClaim: true, state: "prepared", status: "cancelled"},
			{name: "claim_acknowledged_error", claim: operationErr, state: "closed", status: "cancelled"},
			{name: "claim_acknowledged_cancel", claim: operationErr, cancel: true, state: "closed", status: "cancelled"},
			{name: "claim_fail_error", claim: operationErr, fail: settlementErr, state: "running", status: "paused"},
			{name: "claim_close_error", claim: operationErr, close: settlementErr, state: "closed", status: "cancelled"},
			{name: "prepared_events_error", source: operationErr, state: "closed", status: "cancelled"},
			{name: "materialization_acknowledged_cleanup", materialization: operationErr, materializationCommitted: true, activate: true},
			{name: "materialization_missing_ack", materialization: operationErr, refuseMaterialization: true},
			{name: "source_events_acknowledged_cleanup", source: operationErr, sourceCommitted: true, activate: true},
			{name: "activation_acknowledged_error", activation: operationErr, activate: true},
		} {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				fixture := newActivityForkFixture(t, backend)
				owner, selected := fixture.owner, fixture.owner.ports.fork
				ctx, request, _ := q1GateComponentSource(t, owner, false, true, true)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				loaded, err := request.SourceLoader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
					Responsibility: OriginalRunSource(request.SourceRunID), Selection: request.ContractSelection,
				})
				if err != nil {
					t.Fatal(err)
				}
				credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				for key, value := range map[string]string{"telegram.telegram_bot_token": "runtime-outcome-bot", "telegram.webhook_signing_secret": "runtime-outcome-signing"} {
					if err := credentials.Set(ctx, key, value); err != nil {
						t.Fatal(err)
					}
				}
				ordinary, contexts := liveForkRuntimeFixture(t, ctx, selected, owner, loaded, request.SourceRunID, executionposture.Live,
					SelectedContractAgentRuntimeOptions{Credentials: credentials})
				sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, request.SourceRunID)
				if err != nil {
					t.Fatal(err)
				}
				probe := &selectedRuntimeOutcomeProbe{
					SelectedContractRuntimeExecutionLifecycle: owner.ports.runtimeExecution,
					SelectedContractForkLifecycle:             owner.ports.fork, SelectedContractReplayPersistence: owner.ports.replay,
					t: t, issueErr: tc.issue, claimErr: tc.claim, failErr: tc.fail, closeErr: tc.close,
					activationErr: tc.activation, sourceErr: tc.source, discardErr: tc.discard,
					materializationErr: tc.materialization, materializationCommitted: tc.materializationCommitted, sourceCommitted: tc.sourceCommitted,
					refuseMaterialization: tc.refuseMaterialization, refuseIssue: tc.refuseIssue, refuseClaim: tc.refuseClaim,
				}
				if tc.cancel {
					probe.cancelClaim = cancel
				}
				owner.ports.runtimeExecution, owner.ports.fork, owner.ports.replay = probe, probe, probe
				request.Owner, request.LiveRuntime, request.LiveContexts, request.LiveRuns = owner, ordinary, contexts, selected.(runlifecycle.LiveRunReader)
				if !tc.activate {
					request.ForkOperation = nil
				}
				prepared, err := owner.prepareWithOperation(ctx, request, nil)
				if err != nil {
					t.Fatal(err)
				}
				probe.prepared = prepared
				if err := admitSelectedDeploymentRevisionFrontier(prepared.plan, prepared.frontier); err != nil {
					t.Fatal(err)
				}
				result, returned := executePreparedLiveFork(ctx, request, prepared)
				read := context.WithoutCancel(ctx)
				snapshot := probe.snapshot()
				for _, executionID := range snapshot.committedCloses {
					if executionID != snapshot.authority.ID {
						t.Fatal("cleanup closed another selected execution")
					}
				}
				if tc.refuseMaterialization {
					requireSelectedRuntimeOutcomeErrors(t, "unacknowledged materialization", returned, tc.materialization)
					if result.Materialization.ForkRunID != "" || result.ForkLocalRuntimeContainer != nil || result.Activation.Activated || len(snapshot.publications) != 0 {
						t.Fatal("unacknowledged materialization fabricated execution or ACK evidence")
					}
				} else {
					child := result.Materialization.ForkRunID
					if child == "" || result.Activation.Activated != tc.activate {
						t.Fatalf("native outcome lost its child or ACK disposition: %+v error=%v", result, returned)
					}
					if tc.refuseIssue {
						if result.ForkLocalRuntimeContainer != nil {
							t.Fatal("refused issuance fabricated executable identity")
						}
					} else if proof := result.ForkLocalRuntimeContainer; proof == nil || proof.RuntimeExecutionID != snapshot.issued.ExecutionID ||
						proof.RuntimeGeneration != snapshot.issued.Generation || proof.AuthorityExecutionOwner != snapshot.authority.ExecutionOwner {
						t.Fatalf("returned identity differs from its native issue/claim: proof=%+v issue=%+v authority=%+v", proof, snapshot.issued, snapshot.authority)
					}
					if tc.activate {
						if snapshot.activatedFailures != 0 {
							t.Fatal("ordinary live ACK was recast as isolated selected failure")
						}
						requireSelectedRuntimeOutcomeErrors(t, "acknowledged handoff", returned, tc.activation)
						requireSelectedRuntimeOutcomeErrors(t, "joined native cleanup diagnostics", probe.diagnostics.err(), tc.materialization, tc.source, tc.close)
						requireSelectedRuntimeOutcomeDurableState(t, read, selected, child, "live handoff", "closed", "live")
						if result.ExecutedEventCount != 1 || len(snapshot.publications) != 1 || snapshot.publications[0].err != nil {
							t.Fatalf("ACK lost exact selected publication: result=%+v publications=%+v", result, snapshot.publications)
						}
						publication := snapshot.publications[0]
						if publication.request.Commit.Event.Event().RunID() != child || publication.result.AppendOutcome != runtimebus.EventAppendInserted ||
							storetest.ObservePipelineReceipt(t, read, selected, publication.request.Commit.Event.Event().ID()).Count != 1 {
							t.Fatal("ACK borrowed an event or preceded canonical business settlement")
						}
						record, found, err := selected.LoadForkOperationByID(read, request.ForkOperation.OperationID)
						if err != nil || !found || record.Status != runfork.ForkOperationActivated || record.Result == nil || record.ForkRunID != child {
							t.Fatalf("native ACK lacks its permanent operation: %+v found=%t err=%v", record, found, err)
						}
						replayed, err := ExecuteSelectedContractRunFork(read, request)
						if err != nil || !replayed.Activation.Activated || replayed.Materialization.ForkRunID != child ||
							len(probe.snapshot().publications) != 1 {
							t.Fatalf("immutable ACK replay repeated execution or lost child identity: %+v error=%v", replayed, err)
						}
					} else {
						requireSelectedRuntimeOutcomeErrors(t, "refused construction", returned, tc.issue, tc.claim, tc.fail, tc.close, tc.source, tc.discard)
						if len(snapshot.publications) != 0 || result.ExecutedEventCount != 0 {
							t.Fatal("refused construction published child business work")
						}
						if tc.state != "" {
							requireSelectedRuntimeOutcomeDurableState(t, read, selected, child, "refused construction", tc.state, tc.status)
						}
					}
				}
				if sourceAfter, err := storetest.ReadSelectedForkSourceDomain(read, selected, request.SourceRunID); err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
					t.Fatalf("child outcome changed source business state: %v", err)
				}
				// End only the injected faults; cleanup still runs through the
				// exact preparation that retained the unacknowledged resources.
				probe.failErr, probe.discardErr = nil, nil
				if err := prepared.Close(); err != nil {
					t.Fatalf("release the exact retained preparation: %v", err)
				}
				if err := ordinary.Shutdown(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
