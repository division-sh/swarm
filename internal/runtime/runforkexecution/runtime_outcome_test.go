package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Errors are injected after real successful store calls, not as simulated ACK loss.
type selectedRuntimeOutcomeProbe struct {
	SelectedContractRuntimeExecutionLifecycle
	SelectedContractForkLifecycle
	SelectedContractReplayPersistence
	mu                                                   sync.Mutex
	t                                                    *testing.T
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
	ackChild                                             string
	ackHeld                                              bool
	ackEntered, ackRelease                               chan struct{}
	releaseOnce                                          sync.Once
	publications                                         []selectedRuntimeOutcomePublication
	committedCloses                                      []string
	diagnostics                                          *selectedForkCommitDiagnostics
	failureDiagnostics                                   error
	activatedFailures                                    int
}

type selectedRuntimeOutcomePublication struct {
	request runtimebus.CommitSelectedForkEventRequest
	result  runtimebus.CommittedSelectedForkEvent
	err     error
}

type selectedRuntimeOutcomeSnapshot struct {
	calls              []int
	issued             runfork.SelectedContractRuntimeExecution
	authority          runtimeeffects.Authority
	publications       []selectedRuntimeOutcomePublication
	committedCloses    []string
	failureDiagnostics error
	activatedFailures  int
}

func (p *selectedRuntimeOutcomeProbe) snapshot() selectedRuntimeOutcomeSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return selectedRuntimeOutcomeSnapshot{
		calls:  []int{p.issues, p.claims, p.fails, p.closes, p.activations, p.discards, p.loads, p.commits},
		issued: p.issued, authority: p.authority,
		publications:       append([]selectedRuntimeOutcomePublication(nil), p.publications...),
		committedCloses:    append([]string(nil), p.committedCloses...),
		failureDiagnostics: p.failureDiagnostics, activatedFailures: p.activatedFailures,
	}
}

func (p *selectedRuntimeOutcomeProbe) releaseACK() {
	p.releaseOnce.Do(func() { close(p.ackRelease) })
}

type selectedRuntimeOutcomeRunLifecycleProbe struct {
	runlifecycle.OperationOwner
	probe *selectedRuntimeOutcomeProbe
}

func (p selectedRuntimeOutcomeRunLifecycleProbe) RequireActiveRunSource(ctx context.Context, id string) (runtimecorrelation.SourceArtifactFact, error) {
	p.probe.mu.Lock()
	hold := p.probe.ackChild != "" && id == p.probe.ackChild && !p.probe.ackHeld
	if hold {
		p.probe.ackHeld = true
		close(p.probe.ackEntered)
	}
	p.probe.mu.Unlock()
	if hold {
		select {
		case <-p.probe.ackRelease:
		case <-ctx.Done():
			return runtimecorrelation.SourceArtifactFact{}, ctx.Err()
		}
	}
	return p.OperationOwner.RequireActiveRunSource(ctx, id)
}

func awaitSelectedRuntimeOutcome(t *testing.T, ctx context.Context, done <-chan struct{}, phase string, probe *selectedRuntimeOutcomeProbe) {
	t.Helper()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("%s did not settle: %v; probe=%+v", phase, ctx.Err(), probe.snapshot())
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

func selectedRuntimeOutcomeRetainedContext(t *testing.T, owner SelectedContractExecutionOwner, forkID string) (*selectedForkContext, *selectedForkCommitDiagnostics) {
	t.Helper()
	contexts := owner.ports.contexts
	contexts.mu.Lock()
	var retained *selectedForkContext
	matches := 0
	for _, entry := range contexts.entries {
		if entry.binding.ForkRunID == forkID {
			retained = entry
			matches++
		}
	}
	contexts.mu.Unlock()
	if matches != 1 || retained.retained == nil {
		t.Fatalf("acknowledged activation lost its exact process-owned retained context: matches=%d", matches)
	}
	prepared := retained.retained
	prepared.bindMu.Lock()
	runtime := prepared.retainedRuntime
	prepared.bindMu.Unlock()
	if runtime == nil || runtime.completionDiagnostics == nil {
		t.Fatal("acknowledged activation lost its runtime diagnostic owner")
	}
	return retained, runtime.completionDiagnostics
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
	// This caller owns the joined runtime's diagnostics. A cleanup-pending done
	// signal alone cannot synchronize the later serving-tail diagnostic append.
	p.failureDiagnostics = p.diagnostics.err()
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
		p.mu.Lock()
		p.ackChild = req.ForkRunID
		p.mu.Unlock()
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

func (p *selectedRuntimeOutcomeProbe) LoadRunForkSelectedContractSourceEvents(ctx context.Context, source, fork string, ids []string, original semanticview.OriginalLoopCarriage) ([]runfork.RunForkSelectedContractSourceEvent, error) {
	p.mu.Lock()
	p.loads++
	p.mu.Unlock()
	events, err := p.SelectedContractReplayPersistence.LoadRunForkSelectedContractSourceEvents(ctx, source, fork, ids, original)
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

func TestSelectedForkRuntimeConsumersSettleReturnedOutcomes(t *testing.T) {
	operationErr := errors.New("acknowledged operation cleanup")
	settlementErr := errors.New("independent settlement failure")
	for _, target := range []string{"postgres/execute", "postgres/activation_gate", "sqlite/execute", "sqlite/activation_gate"} {
		backend, surface, _ := strings.Cut(target, "/")
		for _, tc := range []struct {
			name                                          string
			issue, claim, fail, close, activation, source error
			materialization                               error
			discard                                       error
			materializationCommitted, sourceCommitted     bool
			refuseMaterialization                         bool
			executeOnly                                   bool
			refuseIssue, refuseClaim, cancel              bool
			claims, fails, closes, activations, discards  int
			loads, commits                                int
			state, status, finalStatus                    string
		}{
			{name: "healthy", claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running", finalStatus: "completed"},
			{name: "issue_refused", issue: operationErr, refuseIssue: true, discards: 1},
			{name: "issue_acknowledged_error", issue: operationErr, discards: 1, state: "prepared", status: "cancelled"},
			{name: "issue_discard_error", issue: operationErr, discard: settlementErr, discards: 1, state: "prepared", status: "paused"},
			{name: "claim_refused", claim: operationErr, refuseClaim: true, claims: 1, discards: 1, state: "prepared", status: "cancelled"},
			{name: "claim_acknowledged_error", claim: operationErr, claims: 1, fails: 1, closes: 1, discards: 1, state: "closed", status: "cancelled"},
			{name: "claim_acknowledged_cancel", claim: operationErr, cancel: true, claims: 1, fails: 1, closes: 1, discards: 1, state: "closed", status: "cancelled"},
			{name: "claim_fail_error", claim: operationErr, fail: settlementErr, claims: 1, fails: 1, state: "running", status: "paused"},
			{name: "claim_close_error", claim: operationErr, close: settlementErr, claims: 1, fails: 1, closes: 1, discards: 1, state: "closed", status: "cancelled"},
			{name: "prepared_events_error", source: operationErr, claims: 1, fails: 1, closes: 1, discards: 1, loads: 1, state: "closed", status: "cancelled"},
			{name: "materialization_acknowledged_cleanup", materialization: operationErr, materializationCommitted: true, executeOnly: true, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running", finalStatus: "completed"},
			{name: "reviewer_materialization_native_cleanup_shape", materialization: errors.Join(operationErr), materializationCommitted: true, executeOnly: true, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running", finalStatus: "completed"},
			{name: "materialization_missing_ack", materialization: operationErr, refuseMaterialization: true, executeOnly: true},
			{name: "source_events_acknowledged_cleanup", source: operationErr, sourceCommitted: true, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running", finalStatus: "completed"},
			{name: "reviewer_source_events_native_cleanup_shape", source: errors.Join(operationErr), sourceCommitted: true, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running", finalStatus: "completed"},
			{name: "activation_acknowledged_error", activation: operationErr, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running", finalStatus: "completed"},
			{name: "activation_and_close_errors", activation: operationErr, close: settlementErr, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running", finalStatus: "completed"},
			{name: "close_acknowledged_error", close: settlementErr, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running", finalStatus: "completed"},
		} {
			if tc.executeOnly && surface != "execute" {
				continue
			}
			t.Run(target+"/"+tc.name, func(t *testing.T) {
				var db *sql.DB
				var selected SelectedContractForkLifecycle
				var owner SelectedContractExecutionOwner
				if backend == "postgres" {
					_, db, _ = testutil.StartPostgres(t)
					pg := storetest.AdmitPostgresRuntimeStore(t, db)
					selected, owner = pg, selectedContractExecutionOwnerForTest(t, pg)
				} else {
					sqlite := storetest.StartSQLiteRuntimeStore(t)
					db = storetest.DatabaseForTest(sqlite)
					selected, owner = sqlite, selectedRuntimeOutcomeSQLiteOwner(t, sqlite)
				}
				ctx, cancel := context.WithCancel(runForkTestContext(t))
				defer cancel()
				repo := runForkExecutionRepoRoot(t)
				loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"), PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo)}
				loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				ctx = runtimecorrelation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact)
				scope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, loaded.SourceArtifactFact.BundleHash())
				if err != nil {
					t.Fatal(err)
				}
				ctx = runtimeauthoractivity.WithScope(ctx, scope)
				descriptors, err := rootruntime.AuthorActivityEventDescriptors(loaded.Source)
				if err != nil {
					t.Fatal(err)
				}
				catalog, err := selected.RegisterAuthorActivityEventCatalog(scope, descriptors)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(catalog.Release)
				sourceID, eventID := uuid.NewString(), uuid.NewString()
				entityID := sourceID
				at := time.Unix(1700002200, 0).UTC()
				historicalTarget := events.RouteIdentity{FlowID: semanticview.RootExecutionFlowID(loaded.Source), FlowInstance: sourceID, EntityID: entityID}
				route := selectedExecutionEntitylessNodeRoute("source-only-node")
				if surface == "activation_gate" || backend == "sqlite" {
					route = selectedExecutionTestAgentRoute(t, sourceID, "source-agent-that-must-not-route", "")
					route.Target = events.MustExistingEntityTarget(historicalTarget)
				}
				seedSelectedRuntimeOutcomeSource(t, ctx, backend, selected, loaded, sourceID, eventID, route, at)
				selection := runforkadmission.SelectedContractSelection(loaded.Source)
				probe := &selectedRuntimeOutcomeProbe{
					SelectedContractRuntimeExecutionLifecycle: owner.ports.runtimeExecution,
					SelectedContractForkLifecycle:             owner.ports.fork, SelectedContractReplayPersistence: owner.ports.replay,
					t: t, issueErr: tc.issue, claimErr: tc.claim, failErr: tc.fail, closeErr: tc.close,
					activationErr: tc.activation, sourceErr: tc.source, discardErr: tc.discard,
					materializationErr: tc.materialization, materializationCommitted: tc.materializationCommitted, sourceCommitted: tc.sourceCommitted,
					refuseMaterialization: tc.refuseMaterialization,
					refuseIssue:           tc.refuseIssue, refuseClaim: tc.refuseClaim,
					ackEntered: make(chan struct{}), ackRelease: make(chan struct{}),
				}
				t.Cleanup(probe.releaseACK)
				if tc.cancel {
					probe.cancelClaim = cancel
				}
				owner.ports.runtimeExecution, owner.ports.fork, owner.ports.replay = probe, probe, probe
				owner.ports.busDurable.RunLifecycle = selectedRuntimeOutcomeRunLifecycleProbe{OperationOwner: owner.ports.busDurable.RunLifecycle, probe: probe}
				var proof *SelectedContractForkLocalRuntimeContainer
				var activation runfork.RunForkActivation
				var forkID string
				var executed int
				if surface == "execute" {
					result, executeErr := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
						SourceRunID: sourceID, At: eventID, AllowSourceFreeze: true, Owner: owner,
						SourceLoader: loader, ContractSelection: selection,
						AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
					})
					proof, activation, forkID, executed, err = result.ForkLocalRuntimeContainer, result.Activation, result.Materialization.ForkRunID, result.ExecutedEventCount, executeErr
				} else {
					materialized := materializeSelectedRuntimeOutcomeFork(t, ctx, owner, loader, selection, sourceID, eventID)
					forkID = materialized.ForkRunID
					result, activateErr := ActivateSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{
						ForkRunID: forkID, AllowSourceFreeze: true, Store: selected.(SelectedContractActivationStore), ExecutionOwner: owner, SourceLoader: loader,
						AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
					})
					proof, activation, executed, err = result.ForkLocalRuntimeContainer, result.RunForkActivation, result.ExecutedEventCount, activateErr
				}
				wantErrors := []error{tc.issue, tc.claim, tc.fail, tc.close, tc.activation, tc.source, tc.materialization, tc.discard}
				if activation.Activated != (tc.activations == 1) || executed != 0 {
					t.Fatalf("lost activation ACK/event evidence: %#v executed:%d", activation, executed)
				}
				if activation.Activated {
					wait, stopWaiting := context.WithTimeout(context.Background(), 30*time.Second)
					defer stopWaiting()
					awaitSelectedRuntimeOutcome(t, wait, probe.ackEntered, "retained activation ACK barrier", probe)
					requireSelectedRuntimeOutcomeErrors(t, "activation ACK return", err)
					ack := probe.snapshot()
					wantACKCalls := []int{1, tc.claims, 0, 0, tc.activations, 0, tc.loads, 0}
					if !reflect.DeepEqual(ack.calls, wantACKCalls) {
						t.Fatalf("activation ACK calls=%v, want %v", ack.calls, wantACKCalls)
					}
					requireSelectedRuntimeOutcomeDurableState(t, wait, selected, forkID, "activation ACK", "running", tc.status)
					retained, diagnostics := selectedRuntimeOutcomeRetainedContext(t, owner, forkID)
					probe.mu.Lock()
					probe.diagnostics = diagnostics
					probe.mu.Unlock()
					requireSelectedRuntimeOutcomeErrors(t, "activation ACK diagnostics", diagnostics.err(), tc.materialization, tc.source, tc.activation)
					probe.releaseACK()
					awaitSelectedRuntimeOutcome(t, wait, retained.done, "exact retained preparation retirement", probe)
					contexts := owner.ports.contexts
					contexts.mu.Lock()
					retirementErr := retained.err
					_, stillRetained := contexts.entries[retained.operation]
					contexts.mu.Unlock()
					settled := probe.snapshot()
					var finalDiagnostics error
					if retirementErr != nil || stillRetained {
						// Failed cleanup can append another diagnostic after done closes.
						// Use the snapshot taken inside the joined failure owner instead.
						finalDiagnostics = settled.failureDiagnostics
						t.Errorf("retained preparation did not retire: err=%v retained=%v; committed closes=%v activated failures=%d diagnostics=%v", retirementErr, stillRetained, settled.committedCloses, settled.activatedFailures, finalDiagnostics)
					} else {
						finalDiagnostics = diagnostics.err()
					}
					requireSelectedRuntimeOutcomeErrors(t, "retained final diagnostics", finalDiagnostics, wantErrors...)
					if settled.activatedFailures != 0 {
						t.Errorf("acknowledged cleanup was recast as activated execution failure: calls=%d diagnostics=%v", settled.activatedFailures, finalDiagnostics)
					}
				} else {
					requireSelectedRuntimeOutcomeErrors(t, "returned refusal", err, wantErrors...)
				}
				settled := probe.snapshot()
				gotCalls := settled.calls
				wantIssues := 1
				if tc.refuseMaterialization {
					wantIssues = 0
				}
				wantCalls := []int{wantIssues, tc.claims, tc.fails, tc.closes, tc.activations, tc.discards, tc.loads, tc.commits}
				if !reflect.DeepEqual(gotCalls, wantCalls) {
					t.Fatalf("issue/claim/fail/close/activate/discard/load/commit calls = %v, want %v; err=%v", gotCalls, wantCalls, err)
				}
				if tc.refuseMaterialization {
					if forkID != "" || proof != nil || activation.Activated || executed != 0 {
						t.Fatalf("unacknowledged materialization leaked output: fork=%q proof=%+v activation=%+v events=%d", forkID, proof, activation, executed)
					}
					var count int
					if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM run_fork_selected_contract_bindings`).Scan(&count); err != nil || count != 0 {
						t.Fatalf("unacknowledged materialization wrote binding: count=%d err=%v", count, err)
					}
					return
				}
				if tc.refuseIssue {
					if proof != nil {
						t.Fatalf("refused issuance fabricated proof: %#v", proof)
					}
				} else if proof == nil || proof.RuntimeExecutionID != settled.issued.ExecutionID || proof.RuntimeGeneration != settled.issued.Generation || proof.AuthorityExecutionOwner != settled.authority.ExecutionOwner {
					t.Fatalf("lost returned runtime identity: %#v, issued:%#v authority:%#v", proof, settled.issued, settled.authority)
				}
				if len(settled.committedCloses) != tc.closes {
					t.Fatalf("committed close evidence=%v, want %d calls", settled.committedCloses, tc.closes)
				}
				for _, id := range settled.committedCloses {
					if id != settled.authority.ID {
						t.Fatalf("committed close lost execution identity: %q, want %q", id, settled.authority.ID)
					}
				}
				if len(settled.publications) != tc.commits {
					t.Fatalf("lost final event evidence: commits=%d, want %d", len(settled.publications), tc.commits)
				}
				for _, publication := range settled.publications {
					lineage := publication.request.Lineage
					if lineage.SourceRunID != sourceID || lineage.ForkRunID != forkID || lineage.SourceEventID != eventID || lineage.ForkEventID != activityidentity.ForkLineageEventID(forkID, eventID) || lineage.EventName != "item.received" || lineage.SelectionAuthority != proof.ExecutionOwner {
						t.Fatalf("final event lost exact fork lineage: %+v", lineage)
					}
					if publication.err != nil || publication.result.AppendOutcome != runtimebus.EventAppendInserted || publication.result.Validate() != nil {
						t.Fatalf("final event lacks its exact insertion ACK: %+v err=%v", publication.result, publication.err)
					}
				}
				if activation.Activated {
					if tc.materializationCommitted || tc.sourceCommitted {
						var eventCount, executionCount int
						if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, forkID, activityidentity.ForkLineageEventID(forkID, eventID)).Scan(&eventCount); err != nil {
							t.Fatal(err)
						}
						if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, forkID).Scan(&executionCount); err != nil {
							t.Fatal(err)
						}
						if eventCount != 1 || executionCount != 1 {
							t.Fatalf("acknowledged cleanup duplicated fork execution: events=%d executions=%d", eventCount, executionCount)
						}
					}
				}
				observer := context.Background()
				wantStatus := tc.status
				if activation.Activated {
					wantStatus = tc.finalStatus
				}
				if tc.refuseIssue {
					var state, status string
					stateErr := db.QueryRowContext(observer, `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, forkID).Scan(&state)
					statusErr := db.QueryRowContext(observer, `SELECT status FROM runs WHERE run_id=$1`, forkID).Scan(&status)
					if !errors.Is(stateErr, sql.ErrNoRows) {
						t.Fatalf("refused issuance retained execution: state=%q err=%v", state, stateErr)
					}
					if !errors.Is(statusErr, sql.ErrNoRows) {
						t.Fatalf("refused issuance retained run: status=%q err=%v", status, statusErr)
					}
				} else {
					requireSelectedRuntimeOutcomeDurableState(t, observer, selected, forkID, "final", tc.state, wantStatus)
				}
				var status string
				if tc.discard != nil {
					// Failed discard must retain its preparation/store lease until
					// the exact owner retries the acknowledged cleanup.
					if err := owner.RetireSelectedContexts(context.Background()); !errors.Is(err, tc.discard) {
						t.Fatalf("persistent discard lost its cleanup owner: %v", err)
					}
					probe.mu.Lock()
					probe.discardErr = nil
					probe.mu.Unlock()
					if err := owner.RetireSelectedContexts(context.Background()); err != nil {
						t.Fatalf("retry retained discard: %v", err)
					}
					if err := db.QueryRowContext(observer, `SELECT status FROM runs WHERE run_id=$1`, forkID).Scan(&status); err != nil || status != "cancelled" {
						t.Fatalf("acknowledged discard lost its completion tombstone: status=%q err=%v", status, err)
					}
					var count int
					if err := db.QueryRowContext(observer, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, forkID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("acknowledged discard retained installed construction: count=%d err=%v", count, err)
					}
				}
				if tc.materializationCommitted || tc.sourceCommitted {
					if err := owner.RetireSelectedContexts(context.Background()); err != nil {
						t.Fatal(err)
					}
					if err := owner.ports.contexts.capability.Release(context.Background()); err != nil {
						t.Fatal(err)
					}
					var restarted SelectedContractExecutionOwner
					if backend == "postgres" {
						restarted = newSelectedContractExecutionOwnerForTest(t, selected.(*store.PostgresStore))
					} else {
						restarted = newSelectedContractSQLiteExecutionOwnerForTest(t, selected.(*store.SQLiteRuntimeStore))
					}
					process := worklifetime.NewProcess()
					t.Cleanup(func() {
						process.Retire()
						if _, err := process.Join(context.Background()); err != nil {
							t.Error(err)
						}
					})
					capability := selectedContractTestProcessCapability(t, ctx, selected.(runtimestartupownership.Store))
					if err := restarted.BindSelectedProcess(ctx, process, capability); err != nil {
						t.Fatal(err)
					}
					recovered, err := restarted.RecoverSelectedForkContexts(ctx, runtimeeffects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{})
					if err != nil || len(recovered) != 1 || recovered[0].RunID != forkID || recovered[0].Disposition != runfork.SelectedForkRecoveryTerminal || recovered[0].Operation != nil || recovered[0].Continuation != nil {
						t.Fatalf("acknowledged fork restart recovery=%+v err=%v", recovered, err)
					}
					requireSelectedRuntimeOutcomeDurableState(t, ctx, selected, forkID, "terminal restart", "closed", "completed")
					var eventCount, executionCount int
					if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2`, forkID, activityidentity.ForkLineageEventID(forkID, eventID)).Scan(&eventCount); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, forkID).Scan(&executionCount); err != nil {
						t.Fatal(err)
					}
					if eventCount != 1 || executionCount != 1 {
						t.Fatalf("restart duplicated selected fork: events=%d executions=%d", eventCount, executionCount)
					}
				}
			})
		}
	}
}

func selectedRuntimeOutcomeSQLiteOwner(t *testing.T, selected *store.SQLiteRuntimeStore) SelectedContractExecutionOwner {
	return selectedContractSQLiteExecutionOwnerForTest(t, selected)
}

func seedSelectedRuntimeOutcomeSource(t *testing.T, ctx context.Context, backend string, selected SelectedContractForkLifecycle, loaded LoadedSelectedContractSource, runID, eventID string, route events.DeliveryRoute, at time.Time) {
	t.Helper()
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{
		RunID: runID, Origin: storetest.ScenarioSetupOrigin(), BundleHash: loaded.SourceArtifactFact.BundleHash(),
		Artifact: selectedExecutionSourceArtifact(t, loaded.SourceArtifactFact.BundleHash()), StartedAt: at.Add(-time.Minute),
	})
	payload, err := json.Marshal(map[string]any{"entity_id": runID})
	if err != nil {
		t.Fatal(err)
	}
	envelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: semanticview.RootExecutionFlowID(loaded.Source), FlowInstance: runID, EntityID: runID})
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "item.received", "source-runtime", "", payload, 0, runID,
		envelope, events.NoRoutingSource(), at, executionmode.Mock)
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	commitSelectedOperationRootFixture(t, ctx, selected, loaded, event)
	if backend == "postgres" {
		failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassConnectorFailure, "source_dead_letter", "run-fork-test", "seed", nil), "run-fork-test", "seed")
		if err := storetest.SeedSelectedSourceOutcome(ctx, selected, storetest.SelectedSourceOutcomeFixture{
			RunID: runID, EventID: eventID, EntityID: runID, CreatedAt: at, Failure: failure,
		}); err != nil {
			t.Fatal(err)
		}
	}
	storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
}

func materializeSelectedRuntimeOutcomeFork(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, loader SelectedContractSourceLoader, selection runfork.RunForkContractSelection, sourceID, eventID string) runfork.RunForkMaterialization {
	t.Helper()
	prepared, err := owner.Prepare(ctx, SelectedContractExecutionRequest{
		SourceRunID: sourceID, At: eventID, Owner: owner,
		SourceLoader: loader, ContractSelection: selection,
		AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: owner.ports.contexts.capability},
	})
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

type selectedActivationOutcomeStore struct {
	fakeSelectedContractActivationStore
	calls int
}

func (s *selectedActivationOutcomeStore) ActivateRunFork(context.Context, runfork.RunForkActivateRequest) (runfork.RunForkActivation, error) {
	s.calls++
	return s.activation, s.activationErr
}

func TestSelectedForkActivationGateRetainsDirectActivationOutcome(t *testing.T) {
	for _, selected := range []bool{false, true} {
		name := "non_selected"
		if selected {
			name = "selected_state_only"
		}
		t.Run(name, func(t *testing.T) {
			forkID := uuid.NewString()
			binding := testSelectedContractBinding(forkID)
			cause := errors.New("acknowledged activation cleanup")
			activation := runfork.RunForkActivation{SourceRunID: binding.SourceRunID, ForkRunID: forkID, Activated: true, SourceFrozen: true}
			store := &selectedActivationOutcomeStore{fakeSelectedContractActivationStore: fakeSelectedContractActivationStore{
				originalRunID: activation.SourceRunID,
				binding:       binding, bindingOK: selected, bundleAvailability: testSelectedContractBundleAvailability(forkID),
				plan: testSelectedContractStateOnlyPlan(binding), activation: activation, activationErr: cause,
			}}
			owner := selectedContractGateOwnerForTest(t)
			result, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
				ForkRunID: forkID, Store: store, ExecutionOwner: owner,
				AgentRuntime: SelectedContractAgentRuntimeOptions{ProcessCapability: owner.ports.contexts.capability},
				SourceLoader: &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection), original: originalActivationSourceFixture(t)},
			})
			if !errors.Is(err, cause) || store.calls != 1 || !reflect.DeepEqual(result.RunForkActivation, activation) {
				t.Fatalf("activation result=%#v err=%v calls=%d", result, err, store.calls)
			}
		})
	}
}
