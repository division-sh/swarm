package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rootruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Errors are injected after real successful store calls, not as simulated ACK loss.
type selectedRuntimeOutcomeProbe struct {
	SelectedContractRuntimeExecutionLifecycle
	SelectedContractForkLifecycle
	SelectedContractReplayPersistence
	t                                                    *testing.T
	issueErr, claimErr, failErr, closeErr, activationErr error
	sourceErr, discardErr                                error
	refuseIssue, refuseClaim                             bool
	cancelClaim                                          context.CancelFunc
	issued                                               runfork.SelectedContractRuntimeExecution
	authority                                            runtimeeffects.Authority
	issues, claims, fails, closes, activations, discards int
	loads, commits                                       int
}

func (p *selectedRuntimeOutcomeProbe) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, req runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error) {
	p.issues++
	if p.refuseIssue {
		return runfork.SelectedContractRuntimeExecution{}, p.issueErr
	}
	issued, err := p.SelectedContractRuntimeExecutionLifecycle.IssueRunForkSelectedContractRuntimeExecution(ctx, req)
	if err != nil {
		p.t.Fatalf("real issuance must succeed before outcome injection: %v", err)
	}
	p.issued = issued
	return issued, p.issueErr
}

func (p *selectedRuntimeOutcomeProbe) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, issued runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (runtimeeffects.Authority, error) {
	p.claims++
	if p.refuseClaim {
		return runtimeeffects.Authority{}, p.claimErr
	}
	authority, err := p.SelectedContractRuntimeExecutionLifecycle.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, owner, lease)
	if err != nil {
		p.t.Fatalf("real claim must succeed before outcome injection: %v", err)
	}
	p.authority = authority
	if p.cancelClaim != nil {
		p.cancelClaim()
	}
	return authority, p.claimErr
}

func (p *selectedRuntimeOutcomeProbe) FailRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, failure json.RawMessage) error {
	p.fails++
	if ctx.Err() != nil || !reflect.DeepEqual(authority, p.authority) {
		p.t.Fatal("failure settlement lost exact authority or inherited caller cancellation")
	}
	if p.failErr != nil {
		return p.failErr
	}
	return p.SelectedContractRuntimeExecutionLifecycle.FailRunForkSelectedContractRuntimeExecution(ctx, authority, failure)
}

func (p *selectedRuntimeOutcomeProbe) CloseRunForkSelectedContractRuntimeExecution(ctx context.Context, id string) error {
	p.closes++
	if ctx.Err() != nil || id != p.authority.ID {
		p.t.Fatal("close lost execution identity or inherited caller cancellation")
	}
	if err := p.SelectedContractRuntimeExecutionLifecycle.CloseRunForkSelectedContractRuntimeExecution(ctx, id); err != nil {
		p.t.Fatalf("real close must succeed before outcome injection: %v", err)
	}
	return p.closeErr
}

func (p *selectedRuntimeOutcomeProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	p.activations++
	activation, err := p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
	if err != nil {
		p.t.Fatalf("real activation must succeed before outcome injection: %v", err)
	}
	return activation, p.activationErr
}

func (p *selectedRuntimeOutcomeProbe) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, id string) error {
	p.discards++
	if ctx.Err() != nil {
		p.t.Fatal("discard inherited caller cancellation")
	}
	if p.discardErr != nil {
		return p.discardErr
	}
	return p.SelectedContractForkLifecycle.DiscardMaterializedSelectedContractExecutionFork(ctx, id)
}

func (p *selectedRuntimeOutcomeProbe) LoadRunForkSelectedContractSourceEvents(ctx context.Context, source, fork string, ids []string, original semanticview.OriginalLoopCarriage) ([]runfork.RunForkSelectedContractSourceEvent, error) {
	p.loads++
	events, err := p.SelectedContractReplayPersistence.LoadRunForkSelectedContractSourceEvents(ctx, source, fork, ids, original)
	if err != nil {
		p.t.Fatalf("real event preparation must succeed before outcome injection: %v", err)
	}
	return events, p.sourceErr
}

func (p *selectedRuntimeOutcomeProbe) CommitSelectedForkEvent(ctx context.Context, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	p.commits++
	return p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, req)
}

func TestSelectedForkRuntimeConsumersSettleReturnedOutcomes(t *testing.T) {
	operationErr := errors.New("acknowledged operation cleanup")
	settlementErr := errors.New("independent settlement failure")
	for _, target := range []string{"postgres/execute", "postgres/activation_gate", "sqlite/execute", "sqlite/activation_gate"} {
		backend, surface, _ := strings.Cut(target, "/")
		for _, tc := range []struct {
			name                                          string
			issue, claim, fail, close, activation, source error
			discard                                       error
			refuseIssue, refuseClaim, cancel              bool
			claims, fails, closes, activations, discards  int
			loads, commits                                int
			state, status                                 string
		}{
			{name: "healthy", claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running"},
			{name: "issue_refused", issue: operationErr, refuseIssue: true, discards: 1},
			{name: "issue_acknowledged_error", issue: operationErr, discards: 1, state: "prepared", status: "cancelled"},
			{name: "issue_discard_error", issue: operationErr, discard: settlementErr, discards: 1, state: "prepared", status: "paused"},
			{name: "claim_refused", claim: operationErr, refuseClaim: true, claims: 1, discards: 1, state: "prepared", status: "cancelled"},
			{name: "claim_acknowledged_error", claim: operationErr, claims: 1, fails: 1, closes: 1, discards: 1, state: "closed", status: "cancelled"},
			{name: "claim_acknowledged_cancel", claim: operationErr, cancel: true, claims: 1, fails: 1, closes: 1, discards: 1, state: "closed", status: "cancelled"},
			{name: "claim_fail_error", claim: operationErr, fail: settlementErr, claims: 1, fails: 1, state: "running", status: "paused"},
			{name: "claim_close_error", claim: operationErr, close: settlementErr, claims: 1, fails: 1, closes: 1, state: "closed", status: "paused"},
			{name: "prepared_events_error", source: operationErr, claims: 1, fails: 1, closes: 1, discards: 1, loads: 1, state: "closed", status: "cancelled"},
			{name: "activation_acknowledged_error", activation: operationErr, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running"},
			{name: "activation_and_close_errors", activation: operationErr, close: settlementErr, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running"},
			{name: "close_acknowledged_error", close: settlementErr, claims: 1, closes: 1, activations: 1, loads: 1, commits: 1, state: "closed", status: "running"},
		} {
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
				sourceID, entityID, eventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
				at := time.Unix(1700002200, 0).UTC()
				route := selectedExecutionEntitylessNodeRoute("source-only-node")
				if surface == "activation_gate" || backend == "sqlite" {
					route = selectedExecutionTestAgentRoute(t, sourceID, "source-agent-that-must-not-route", "flow-a/1")
				}
				if backend == "postgres" {
					seedSelectedExecutionSourceRunWithPrimaryRouteAndMode(t, db, sourceID, entityID, eventID, "item.received", at, "test_entity", executionmode.Mock, route, nil, loaded.SourceArtifactFact)
					seedSourceOutcomeThatMustNotSuppressFork(t, db, eventID, entityID, at)
					captureSelectedExecutionSourceRevision(t, db, sourceID)
				} else {
					seedSelectedRuntimeOutcomeSQLite(t, ctx, selected.(*store.SQLiteRuntimeStore), loaded, sourceID, entityID, eventID, route, at)
				}
				selection := runforkadmission.SelectedContractSelection(loaded.Source)
				probe := &selectedRuntimeOutcomeProbe{
					SelectedContractRuntimeExecutionLifecycle: owner.ports.runtimeExecution,
					SelectedContractForkLifecycle:             owner.ports.fork, SelectedContractReplayPersistence: owner.ports.replay,
					t: t, issueErr: tc.issue, claimErr: tc.claim, failErr: tc.fail, closeErr: tc.close,
					activationErr: tc.activation, sourceErr: tc.source, discardErr: tc.discard,
					refuseIssue: tc.refuseIssue, refuseClaim: tc.refuseClaim,
				}
				if tc.cancel {
					probe.cancelClaim = cancel
				}
				owner.ports.runtimeExecution, owner.ports.fork, owner.ports.replay = probe, probe, probe
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
				wantError := false
				for _, want := range []error{tc.issue, tc.claim, tc.fail, tc.close, tc.activation, tc.source, tc.discard} {
					if want != nil {
						wantError = true
						if !errors.Is(err, want) {
							t.Fatalf("returned error %v lost %v", err, want)
						}
					}
				}
				if !wantError && err != nil {
					t.Fatal(err)
				}
				gotCalls := []int{probe.issues, probe.claims, probe.fails, probe.closes, probe.activations, probe.discards, probe.loads, probe.commits}
				wantCalls := []int{1, tc.claims, tc.fails, tc.closes, tc.activations, tc.discards, tc.loads, tc.commits}
				if !reflect.DeepEqual(gotCalls, wantCalls) {
					t.Fatalf("issue/claim/fail/close/activate/discard/load/commit calls = %v, want %v; err=%v", gotCalls, wantCalls, err)
				}
				if tc.refuseIssue {
					if proof != nil {
						t.Fatalf("refused issuance fabricated proof: %#v", proof)
					}
				} else if proof == nil || proof.RuntimeExecutionID != probe.issued.ExecutionID || proof.RuntimeGeneration != probe.issued.Generation || proof.AuthorityExecutionOwner != probe.authority.ExecutionOwner {
					t.Fatalf("lost returned runtime identity: %#v, issued:%#v authority:%#v", proof, probe.issued, probe.authority)
				}
				if activation.Activated != (tc.activations == 1) || executed != tc.commits {
					t.Fatalf("lost activation/event evidence: %#v executed:%d", activation, executed)
				}
				if activation.Activated {
					contexts := owner.ports.contexts
					contexts.mu.Lock()
					retained := false
					for _, entry := range contexts.entries {
						if entry.binding.ForkRunID == forkID && entry.retained != nil {
							retained = true
						}
					}
					contexts.mu.Unlock()
					if !retained {
						t.Fatal("acknowledged activation lost its process-owned retained context")
					}
				}
				observer := context.Background()
				var state, status string
				stateErr := db.QueryRowContext(observer, `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, forkID).Scan(&state)
				statusErr := db.QueryRowContext(observer, `SELECT status FROM runs WHERE run_id=$1`, forkID).Scan(&status)
				if tc.refuseIssue {
					if !errors.Is(stateErr, sql.ErrNoRows) || !errors.Is(statusErr, sql.ErrNoRows) {
						t.Fatalf("refused issuance cleanup state:%v run:%v", stateErr, statusErr)
					}
				} else if stateErr != nil || statusErr != nil || state != tc.state || status != tc.status {
					t.Fatalf("durable state=%q/%v status=%q/%v, want %q/%q", state, stateErr, status, statusErr, tc.state, tc.status)
				}
			})
		}
	}
}

func selectedRuntimeOutcomeSQLiteOwner(t *testing.T, selected *store.SQLiteRuntimeStore) SelectedContractExecutionOwner {
	return selectedContractSQLiteExecutionOwnerForTest(t, selected)
}

func seedSelectedRuntimeOutcomeSQLite(t *testing.T, ctx context.Context, selected *store.SQLiteRuntimeStore, loaded LoadedSelectedContractSource, runID, entityID, eventID string, route events.DeliveryRoute, at time.Time) {
	t.Helper()
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
		RunID: runID, Origin: storetest.ScenarioSetupOrigin(), StartedAt: at.Add(-time.Minute),
		Artifact: selectedExecutionSourceArtifact(t, loaded.SourceArtifactFact.BundleHash()),
	})
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if err := selected.CreateEntity(ctx, runtimetools.EntityCreateRecord{
		Source: loaded.Source,
		RunID:  runID, EntityID: entityID, FlowInstance: ".", EntityType: "test_entity", Name: "Selected Execution Entity",
		CurrentState: "pending", FieldsJSON: json.RawMessage(`{}`), CreatedAt: at.Add(-time.Second),
		Writer: runtimetools.EntityMutationWriter{Type: "platform", ID: "selected-execution-test", HandlerStep: "seed"},
	}); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatal(err)
	}
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), ".")
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "item.received", "source-runtime", "", payload, 0, runID,
		envelope, eventtest.RootRoutingSource(runID), at, executionmode.Mock)
	event, err = eventfixture.BindPayload(event)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := selected.PipelineObligations().ClaimPublication(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := selected.PipelineObligations().Release(context.WithoutCancel(ctx), claim); err != nil {
			t.Fatal(err)
		}
	}()
	authority, err := runtimedelivery.NewNormalExecutionAuthority(loaded.SourceArtifactFact, runForkTestRuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = selected.CommitPublication(ctx, runtimebus.PublicationCommand{Commit: runtimebus.CommitPublishRequest{
		Event: admitted, RouteSettlement: settlement, ReplayScope: runtimepipelineobligation.ScopeSubscribed,
		PipelineClaim: claim, DeliveryRoutes: []events.DeliveryRoute{route}, DeliveryAuthority: authority,
	}})
	if err != nil {
		t.Fatal(err)
	}
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
	materialized, err := owner.materializePrepared(prepared.operation.PreparationContext(), prepared)
	if err != nil {
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
