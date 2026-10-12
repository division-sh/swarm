package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedHandlerFanOutCrashCheckpoint struct {
	Key       fanoutobligation.IntentKey
	Selection effects.SelectedContractForkAuthority
	Grant     *startupownership.GrantEvidence `json:"grant,omitempty"`
	Claim     *fanoutobligation.Claim         `json:"claim,omitempty"`
}

type selectedHandlerFanOutIntentCrash struct {
	pipeline.WorkflowPersistenceOwner
	test       *testing.T
	checkpoint func(context.Context, fanoutobligation.IntentKey)
}

func (p selectedHandlerFanOutIntentCrash) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	committed, err := p.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	if command.FanOutIntent != nil && committed.Committed && err == nil {
		if command.DeliverySuccess == nil || committed.DeliverySuccess == nil || !committed.DeliverySuccess.Same(command.DeliverySuccess.Claim) {
			p.test.Fatal("intent checkpoint lacks its atomically acknowledged handler settlement")
		}
		// This is after the real native commit and before the adapter can notify
		// the shared pump or finish the input's pipeline handoff.
		p.checkpoint(ctx, command.FanOutIntent.Key)
	}
	return committed, err
}

func runSelectedHandlerFanOutCrashHelper(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, capability startupownership.ProcessCapability) {
	t.Helper()
	repo := runForkExecutionRepoRoot(t)
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: canonicalrouting.CopyForkFanOutConsumer(t, false, false), PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{Selection: runfork.RunForkContractSelection{Mode: "selected_contracts"}})
	if err != nil {
		t.Fatal(err)
	}
	sourceRun, eventID := uuid.NewString(), uuid.NewString()
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "items.ready", "source-runtime", "",
		[]byte(`{"items":["one","two"]}`), 0, sourceRun, events.EventEnvelope{}, events.NoRoutingSource(), time.Now().UTC().Add(-time.Minute), executionmode.Mock)
	bundle, found := semanticview.Bundle(loaded.Source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("handler fan-out source lacks its admitted artifact")
	}
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{
		RunID: sourceRun, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact,
		BundleHash: loaded.SourceArtifactFact.BundleHash(), StartedAt: event.CreatedAt().Add(-time.Minute),
	})
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustRunForkRootNode("fan-out-source")),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: semanticview.RootExecutionFlowID(loaded.Source), FlowInstance: sourceRun, EntityID: sourceRun})}
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, pipelineobligation.ScopeSubscribed)
	commitSelectedOperationRootFixture(t, ctx, selected, loaded, event)
	storetest.CaptureRunForkSnapshot(t, ctx, selected, sourceRun)
	owner.ports.workflow = pipeline.NewWorkflowPersistence(selectedHandlerFanOutIntentCrash{
		WorkflowPersistenceOwner: selected.(pipeline.WorkflowPersistenceOwner), test: t,
		checkpoint: func(ctx context.Context, key fanoutobligation.IntentKey) {
			authority, found := effects.AuthorityFromContext(ctx)
			if !found || authority.Kind != effects.AuthoritySelectedContractFork || authority.SelectedFork.ForkRunID != key.RunID || key.DeploymentFeedID != "" {
				t.Fatal("handler intent lost its exact selected execution authority or origin")
			}
			pipe := os.NewFile(3, "selected-handler-fan-out-checkpoint")
			checkpoint := selectedForkCrashCheckpoint{SourceRun: sourceRun, ForkRun: key.RunID,
				FanOut: &selectedHandlerFanOutCrashCheckpoint{Key: key, Selection: authority.SelectedFork}}
			if os.Getenv("SWARM_SELECTED_CRASH_CUT") == "retained_handler_fan_out_claim_acquired" {
				grant := selectedHandlerFanOutGrant(t, owner, key.RunID)
				serving, err := selected.(selectedHandlerFanOutBinder).BindSelectedFanOutGrant(grant)
				if err != nil {
					t.Fatal(err)
				}
				intent, claim, found, err := serving.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
					Owner: uuid.NewString(), BundleHash: grant.BundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: 30 * time.Second,
				})
				if err != nil || !found || intent.Cursor != 0 || claim.Key != key || claim.Generation != 1 {
					t.Fatalf("native predecessor claim missed the unissued intent: intent=%+v claim=%+v found=%t error=%v", intent, claim, found, err)
				}
				checkpoint.FanOut.Grant, checkpoint.FanOut.Claim = &grant, &claim
			}
			if err := json.NewEncoder(pipe).Encode(checkpoint); err != nil {
				t.Fatal(err)
			}
			_ = pipe.Close()
			var b [1]byte
			_, err := os.Stdin.Read(b[:])
			t.Fatalf("acknowledged intent resumed without SIGKILL: %v", err)
		},
	})
	selection := runforkadmission.SelectedContractSelection(loaded.Source)
	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID: sourceRun, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader, ContractSelection: selection,
		AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
		ForkOperation: &runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "retained-handler-fan-out", IdempotencyKey: "intent-cut",
			TransportHash: "retained-handler-fan-out-transport", SourceRunID: sourceRun, ForkEventID: eventID,
			TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true, ContractSelection: selection},
	})
	if err != nil || !result.Activation.Activated {
		t.Fatalf("activate handler fan-out: %+v error=%v", result.Activation, err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(20 * time.Second):
	}
	diagnostic, diagnosticErr := storetest.ReadServedRunDebugSummary(context.WithoutCancel(ctx), selected, result.Materialization.ForkRunID)
	t.Fatalf("selected handler missed its committed-intent checkpoint: %s diagnostic_error=%v", diagnostic, diagnosticErr)
}

func TestIssue642SelectedHandlerFanOutCrashRestartWithoutWakeBothStores(t *testing.T) {
	runSelectedHandlerFanOutRecovery(t, false, false)
}

func TestIssue642SelectedHandlerFanOutExpiredClaimFencesPredecessorBothStores(t *testing.T) {
	runSelectedHandlerFanOutRecovery(t, true, false)
}

func TestIssue642SelectedHandlerFanOutLiveSuccessorClaimFencingBothStores(t *testing.T) {
	runSelectedHandlerFanOutRecovery(t, true, true)
}

type selectedHandlerFanOutBinder interface {
	BindSelectedFanOutGrant(startupownership.GrantEvidence) (pipeline.FanOutObligationOwner, error)
	ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
}

func selectedHandlerFanOutGrant(t *testing.T, owner SelectedContractExecutionOwner, runID string) startupownership.GrantEvidence {
	t.Helper()
	owner.ports.contexts.mu.Lock()
	var grant startupownership.GenerationGrant
	for _, entry := range owner.ports.contexts.entries {
		if entry.binding.ForkRunID == runID && entry.retained != nil && entry.retained.retainedRuntime != nil {
			grant = entry.retained.retainedRuntime.generationGrant
		}
	}
	owner.ports.contexts.mu.Unlock()
	if grant == nil {
		t.Fatal("handler claim proof lacks its exact retained native grant")
	}
	evidence, err := grant.Evidence()
	if err != nil || evidence.SelectedFork == nil || evidence.SelectedFork.ForkRunID != runID || evidence.State != startupownership.GrantAdmitted {
		t.Fatalf("handler claim proof grant: %+v error=%v", evidence, err)
	}
	return evidence
}

func runSelectedHandlerFanOutRecovery(t *testing.T, claimed, live bool) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var construct func() SelectedContractExecutionOwner
			var dsn string
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, dsn = s, s.Path()
				construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
			} else {
				dsn = testutil.StartPostgresDSN(t)
				s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
				selected = s
				construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
			}
			cut := "retained_handler_fan_out_intent_committed"
			if claimed {
				cut = "retained_handler_fan_out_claim_acquired"
			}
			checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, cut)
			if checkpoint.FanOut == nil || checkpoint.FanOut.Key.RunID != checkpoint.ForkRun {
				t.Fatal("process death lost the exact handler intent")
			}
			key := checkpoint.FanOut.Key
			before, err := storetest.ReadFanOutIntentCompletionStorage(context.Background(), selected, key)
			if err != nil || before.Status != string(fanoutobligation.StatusOpen) || before.Cardinality != 2 || before.Cursor != 0 || len(before.Outcomes) != 0 {
				t.Fatalf("crash missed the committed, unissued handler intent: %+v error=%v", before, err)
			}
			if claimed {
				claim := checkpoint.FanOut.Claim
				if claim == nil || checkpoint.FanOut.Grant == nil || claim.Validate() != nil || claim.Key != key || claim.Generation != 1 {
					t.Fatal("crash did not retain an actual native predecessor claim")
				}
				// Recovery eligibility follows real lease expiry. This is not a
				// synthetic clock advance or an immediate dead-owner takeover.
				if remaining := time.Until(claim.LeaseUntil); remaining > 0 && !live {
					timer := time.NewTimer(remaining)
					defer timer.Stop()
					select {
					case <-t.Context().Done():
						t.Fatal(context.Cause(t.Context()))
					case <-timer.C:
					}
				}
			}
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(context.Background(), selected, checkpoint.SourceRun)
			if err != nil {
				t.Fatal(err)
			}
			operations := selected.(interface {
				LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
			})
			original, found, err := operations.LoadForkOperation(context.Background(), "retained-handler-fan-out", "intent-cut", "retained-handler-fan-out-transport")
			if err != nil || !found || original.Status != runfork.ForkOperationActivated || original.ForkRunID != checkpoint.ForkRun || original.Result == nil {
				t.Fatalf("intent crash lost activation acknowledgment: %+v found=%t error=%v", original, found, err)
			}
			ctx := runForkTestContext(t)
			capability := selectedContractTestProcessCapability(t, ctx, selected)
			owner := construct()
			successor := make(chan effects.Authority, 1)
			owner.ports.runtimeExecution = selectedActivitySuccessorClaim{owner.ports.runtimeExecution, successor}
			process, _ := worklifetime.ProcessFromContext(ctx)
			if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
				t.Fatal(err)
			}
			baseline := process.ActiveCount()
			t.Cleanup(func() {
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Error(err)
				}
			})
			loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
			recoveryCtx := ctx
			var held *selectedHandlerFanOutClaimHold
			if live {
				held = newSelectedHandlerFanOutClaimHold(key)
				defer held.resume()
				recoveryCtx = startupownership.WithFanOutChunkObserverForTest(ctx, held.observe)
			}
			recovered, err := owner.RecoverSelectedForkContexts(recoveryCtx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
				SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
			})
			if err != nil || len(recovered) != 1 || recovered[0].RunID != checkpoint.ForkRun || recovered[0].Disposition != runfork.SelectedForkRecoveryResume {
				t.Fatalf("handler successor recovery: %+v error=%v", recovered, err)
			}
			select {
			case authority := <-successor:
				previous := checkpoint.FanOut.Selection
				next := authority.SelectedFork
				expected := previous
				expected.ExecutionID, expected.Generation = next.ExecutionID, next.Generation
				if !authority.Valid() || authority.Kind != effects.AuthoritySelectedContractFork || next != expected || next.ExecutionID == previous.ExecutionID || next.Generation != previous.Generation+1 {
					t.Fatalf("successor borrowed the predecessor's executor authority: %+v previous=%+v", authority, previous)
				}
			default:
				t.Fatal("recovery did not claim a fresh selected executor")
			}
			if live {
				if remaining := time.Until(checkpoint.FanOut.Claim.LeaseUntil); remaining > 0 {
					timer := time.NewTimer(remaining)
					defer timer.Stop()
					select {
					case <-t.Context().Done():
						t.Fatal(context.Cause(t.Context()))
					case <-timer.C:
					}
				}
			}
			// No Wake or manual ServeFanOutCandidate call occurs after reopening.
			wait, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer cancel()
			if live {
				select {
				case acquired := <-held.acquired:
					requireSelectedHandlerFanOutLiveClaimFenced(t, wait, selected, owner, checkpoint, acquired)
					held.resume()
				case <-wait.Done():
					t.Fatalf("successor never acquired its actual live claim: %v", wait.Err())
				}
			}
			inputID := activityidentity.ForkLineageEventID(checkpoint.ForkRun, original.Result.ForkEventID)
			for {
				progress, err := storetest.ReadFanOutIntentCompletionStorage(wait, selected, key)
				if err != nil {
					t.Fatal(err)
				}
				outputs, err := storetest.ReadLifecycleEventCardinality(wait, selected, checkpoint.ForkRun, "items.child")
				if err != nil || outputs > 2 {
					t.Fatalf("successor output cardinality=%d error=%v", outputs, err)
				}
				settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, checkpoint.ForkRun)
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := storetest.ReadLatestPlatformPipelineReceiptStorage(wait, selected, inputID)
				if err != nil {
					t.Fatal(err)
				}
				pending, err := storetest.ReadServedIncompletePipelineHandoffCount(wait, selected, checkpoint.ForkRun)
				if err != nil {
					t.Fatal(err)
				}
				if progress.Status == string(fanoutobligation.StatusClosed) && progress.Cursor == 2 && progress.Cardinality == 2 && len(progress.Outcomes) == 2 && outputs == 2 && settlement.Total == 3 && settlement.Delivered == 3 && receipt.Found && receipt.Outcome == "success" && pending == 0 {
					for ordinal, outcome := range progress.Outcomes {
						if outcome.Ordinal != ordinal || outcome.Kind != string(fanoutobligation.OutcomeCommitted) || outcome.EventID == "" {
							t.Fatalf("successor lost exact ordinal outcomes: %+v", progress.Outcomes)
						}
						output := storetest.LoadCanonicalEventRecord(t, wait, selected, outcome.EventID)
						want := []string{`{"value":"one"}`, `{"value":"two"}`}[ordinal]
						if output.RunID() != checkpoint.ForkRun || string(output.Payload()) != want {
							t.Fatalf("successor changed frozen ordinal payload: %s", output.Payload())
						}
					}
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("no-wake handler recovery: intent=%+v outputs=%d deliveries=%+v receipt=%+v pending=%d error=%v", progress, outputs, settlement, receipt, pending, wait.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer stop()
			if claimed {
				requireSelectedHandlerFanOutPredecessorFenced(t, cleanup, selected, owner, checkpoint)
			}
			if err := owner.RetireSelectedContexts(cleanup); err != nil || process.ActiveCount() != baseline {
				t.Fatalf("successor retirement leaked resources: leases=%d baseline=%d error=%v", process.ActiveCount(), baseline, err)
			}
			retry, found, err := operations.LoadForkOperation(cleanup, original.Request.Actor, original.Request.IdempotencyKey, original.Request.TransportHash)
			if err != nil || !found || !reflect.DeepEqual(original, retry) {
				t.Fatalf("successor rewrote the acknowledged fork operation: %+v error=%v", retry, err)
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(cleanup, selected, checkpoint.SourceRun)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("successor changed source business history: %v", err)
			}
		})
	}
}

func requireSelectedHandlerFanOutPredecessorFenced(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, checkpoint selectedForkCrashCheckpoint) {
	t.Helper()
	binder := selected.(selectedHandlerFanOutBinder)
	before, err := storetest.ReadFanOutIntentCompletionStorage(ctx, selected, checkpoint.FanOut.Key)
	if err != nil {
		t.Fatal(err)
	}
	if stale, err := binder.BindSelectedFanOutGrant(*checkpoint.FanOut.Grant); err == nil || stale != nil {
		t.Fatalf("successor admitted predecessor process/executor grant: owner=%T error=%v", stale, err)
	}
	grant := selectedHandlerFanOutGrant(t, owner, checkpoint.ForkRun)
	current, err := binder.BindSelectedFanOutGrant(grant)
	if err != nil || current == nil {
		t.Fatalf("exact successor grant positive control: owner=%T error=%v", current, err)
	}
	page, err := binder.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: checkpoint.ForkRun, Limit: 10})
	if err != nil || len(page.Intents) != 1 || page.Intents[0].Key != checkpoint.FanOut.Key || page.Intents[0].ClaimGeneration != checkpoint.FanOut.Claim.Generation+1 {
		t.Fatalf("successor did not replace the exact old claim once: %+v error=%v", page, err)
	}
	if _, err := current.LoadFanOutEvaluation(ctx, *checkpoint.FanOut.Claim); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
		t.Fatalf("current grant accepted stale predecessor evaluation: %v", err)
	}
	settlement, err := current.ReleaseFanOutClaim(ctx, *checkpoint.FanOut.Claim)
	if settlement.Acknowledged || !errors.Is(err, fanoutobligation.ErrStaleClaim) {
		t.Fatalf("stale predecessor released successor ownership: %+v error=%v", settlement, err)
	}
	after, err := storetest.ReadFanOutIntentCompletionStorage(ctx, selected, checkpoint.FanOut.Key)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected predecessor changed committed progress/outcomes: before=%+v after=%+v error=%v", before, after, err)
	}
}
