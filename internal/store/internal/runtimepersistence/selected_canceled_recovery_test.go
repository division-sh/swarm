package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestSelectedTerminatedOriginStartupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, originState := range []string{"launched", "launch_only", "unstarted", "prelaunch", "retained_success", "retained_uncertain", "two_open_rounds"} {
			for _, cut := range []string{"healthy", "lost_ack", "auxiliary_error"} {
				t.Run(backend+"/"+originState+"/"+cut, func(t *testing.T) {
					selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
					f := newSelectedProviderCompletionFixture(t, selected, db, sqlite)
					ctx := testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash)
					issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, f.request)
					if err != nil {
						t.Fatal(err)
					}
					authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "startup-terminate", time.Minute)
					if err != nil {
						t.Fatal(err)
					}
					plan := admitSelectedProviderFixture(t, ctx, f, issued, authority)
					authority.Target = selectedProviderTarget(f)
					ctx = effects.WithAuthority(effects.WithController(ctx, newCompletionControllerForTest(selected)), authority)
					ctx = selectedProviderClaimContext(t, ctx, f, authority)
					ctx = withManagedCompletionTestSurface(t, managedSelectedExecutionStoreTestContext(t, ctx, authority), authority, "anthropic_api")
					claim, ok := deliverylifecycle.ClaimFromContext(ctx)
					if !ok {
						t.Fatal("missing admitted selected claim")
					}
					origin, err := effects.DeliveryCompletionOrigin(claim)
					if err != nil {
						t.Fatal(err)
					}
					command := effects.CanceledTurnCommand{Origin: origin}
					var uncertain int
					var retained *effects.Handle
					if originState != "unstarted" {
						var handle *effects.Handle
						if originState == "prelaunch" {
							handle, err = beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("startup-terminate-prelaunch"))
							if err != nil {
								t.Fatal(err)
							}
						} else if originState == "launch_only" {
							handle, err = beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("startup-terminate-launch-only"))
							if err != nil {
								t.Fatal(err)
							}
							if err := handle.MarkLaunched(ctx); err != nil {
								t.Fatal(err)
							}
							uncertain = 1
						} else {
							handle = beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "startup-terminate")
							uncertain = 1
						}
						command = effects.CanceledTurnCommandForAttempt(handle.Attempt(), nil)
						if originState == "retained_success" || originState == "retained_uncertain" {
							retained, uncertain = handle, 0
						}
						if originState == "two_open_rounds" {
							roundAuthority := authority
							roundAuthority.Target.ID = uuid.NewString()
							roundCtx := effects.WithLogicalOperationIdentity(effects.WithAuthority(ctx, roundAuthority), "startup-terminate-second-round")
							beginObservedCompletionForSettlementTest(t, roundCtx, "anthropic_api", "startup-terminate-second-round")
							uncertain = 2
						}
					}
					requestSelectedTurnTerminationForTest(t, ctx, selected, f, plan, origin)
					if retained != nil {
						if originState == "retained_success" {
							settleSelectedCompletionForTest(t, ctx, retained, authority.Target, time.Now().UTC())
						} else {
							failure := failures.FromError(context.Canceled, "selected-turn-test", "physical_join").Failure
							if err := retained.Settle(ctx, effects.StateOutcomeUncertain, &failure, map[string]any{"physical_joined": true, "observed_response": "kept"}); err != nil {
								t.Fatal(err)
							}
						}
					}
					if err := f.process.Release(ctx); err != nil {
						t.Fatal(err)
					}
					capability := selectedPreparationProcessForTest(t, selected)
					process := worklifetime.NewProcess()
					startupCtx := worklifetime.WithProcess(testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash), process)
					defer func() {
						process.Retire()
						if _, err := process.Join(context.Background()); err != nil {
							t.Error(err)
						}
					}()
					responseFailure := errors.New("selected cancellation response cut")
					fork := &selectedCancellationCommitResponseCut{SelectedContractForkLifecycle: selected.(runforkexecution.SelectedContractForkLifecycle), cut: cut, err: responseFailure}
					owner := selectedStorePreparationOwnerWithForkForTest(t, selected, fork)
					if err := owner.BindSelectedProcess(startupCtx, process, capability); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := owner.RetireSelectedContexts(context.Background()); err != nil {
							t.Error(err)
						}
					}()
					results, err := owner.RecoverSelectedForkContexts(startupCtx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), runforkexecution.SelectedForkRecoveryEnvironment{})
					expectedSummary := effects.RecoverySummary{OutcomeUncertain: uncertain}
					if originState == "prelaunch" {
						expectedSummary.PrelaunchTerminal = 1
					}
					if cut != "healthy" && !errors.Is(err, responseFailure) || cut == "healthy" && err != nil || len(results) != 1 || results[0].Effects != expectedSummary || fork.commits != 1 {
						t.Fatalf("terminate startup handoff: %+v %v", results, err)
					}
					if cut == "lost_ack" {
						if len(results[0].CanceledTurns) != 0 {
							t.Fatal("lost response fabricated cancellation acknowledgment")
						}
						entries, err := fork.ListSelectedForkRecoveryEntries(startupCtx)
						if err != nil || len(entries) != 1 {
							t.Fatalf("lost response inventory: %+v %v", entries, err)
						}
						processEvidence, err := capability.Evidence()
						if err != nil {
							t.Fatal(err)
						}
						repeated, err := fork.SelectedContractForkLifecycle.RecoverSelectedFork(startupCtx, runcontrol.SelectedForkRecoveryRequest{
							Entry: entries[0], Process: processEvidence, Effects: effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live),
							Cancellations: []effects.CanceledTurnCommand{command},
						})
						if err != nil || len(repeated.CanceledTurns) != 1 {
							t.Fatalf("lost response did not resolve exact commit: %+v %v", repeated, err)
						}
						results[0] = repeated
					}
					if len(results[0].CanceledTurns) != 1 {
						t.Fatalf("acknowledged result lost settlement evidence: %+v", results[0])
					}
					commit := results[0].CanceledTurns[0]
					if commit.Validate() != nil || !commit.Origin.Same(origin) || commit.Publication != nil || commit.Delivery.Status != deliverylifecycle.StatusCanceled || commit.Delivery.ReasonCode != "terminate" {
						t.Fatalf("terminate startup substituted settlement: %+v", commit)
					}
					snapshot, err := selected.(deliverylifecycle.Store).Snapshot(startupCtx, commit.Delivery.DeliveryID)
					if err != nil || snapshot.ClaimVersion != origin.Delivery.Version() || !snapshot.SettledAt.Equal(commit.Delivery.SettledAt) {
						t.Fatalf("startup response cut changed exact origin: %+v %v", snapshot, err)
					}
					if retained != nil {
						state, rows := effects.StateOutcomeUncertain, 0
						if originState == "retained_success" {
							state, rows = effects.StateSettled, 1
						}
						requireExternalAttemptState(t, db, sqlite, retained.Attempt().AttemptID, state)
						requireCompletionSettlementRows(t, completionSettlementFixture{db: db, sqlite: sqlite}, retained.Attempt().AttemptID, authority.Target.ID, state, rows, 0)
					}
				})
			}
		}
	}
}

type selectedCancellationCommitResponseCut struct {
	runforkexecution.SelectedContractForkLifecycle
	cut     string
	err     error
	commits int
}

func (p *selectedCancellationCommitResponseCut) RecoverSelectedFork(ctx context.Context, request runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error) {
	result, err := p.SelectedContractForkLifecycle.RecoverSelectedFork(ctx, request)
	if len(request.Cancellations) == 0 || len(result.CanceledTurns) == 0 || err != nil {
		return result, err
	}
	p.commits++
	switch p.cut {
	case "lost_ack":
		return runfork.SelectedForkRecoveryResult{}, p.err
	case "auxiliary_error":
		return result, p.err
	default:
		return result, nil
	}
}

func TestSelectedCanceledOriginStartupReactionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			f := newSelectedProviderCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash)
			issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, f.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "startup-timeout", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			admitSelectedProviderFixture(t, ctx, f, issued, authority)
			authority.Target = selectedProviderTarget(f)
			ctx = effects.WithAuthority(effects.WithController(ctx, newCompletionControllerForTest(selected)), authority)
			ctx = selectedProviderClaimContext(t, ctx, f, authority)
			ctx = withManagedCompletionTestSurface(t, managedSelectedExecutionStoreTestContext(t, ctx, authority), authority, "anthropic_api")
			ctx = effects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.grant_receiver"})
			handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "startup-timeout")
			clock, found := handle.LogicalTurnClock()
			if !found {
				t.Fatal("selected launch lacks exact clock")
			}
			if _, err := selected.(effects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt); err != nil {
				t.Fatal(err)
			}
			if err := f.process.Release(ctx); err != nil {
				t.Fatal(err)
			}
			capability := selectedPreparationProcessForTest(t, selected)
			process := worklifetime.NewProcess()
			startupCtx := worklifetime.WithProcess(testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash), process)
			defer func() {
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			repo := canonicalrouting.RepoRoot(t)
			loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)}
			request := effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live)
			inspectionFailure := errors.New("source inspection cut")
			failed := selectedStorePreparationOwnerForTest(t, selected)
			if err := failed.BindSelectedProcess(startupCtx, process, capability); err != nil {
				t.Fatal(err)
			}
			if _, err := failed.RecoverSelectedForkContexts(startupCtx, request, runforkexecution.SelectedForkRecoveryEnvironment{
				SourceInspector: selectedCancellationInspectorCut{SourceArtifactSelectedContractSourceLoader: loader, err: inspectionFailure},
				AgentRuntime:    runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live},
			}); !errors.Is(err, inspectionFailure) {
				t.Fatalf("source inspection failure lost: %v", err)
			}
			if err := failed.RetireSelectedContexts(startupCtx); err != nil {
				t.Fatal(err)
			}
			snapshot, err := selected.(deliverylifecycle.Store).Snapshot(startupCtx, handle.Attempt().Origin.Delivery.DeliveryID())
			if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress {
				t.Fatalf("inspection failure settled origin: %+v %v", snapshot, err)
			}
			cleanupFailure := errors.New("inspection cleanup cut")
			cleanupCalls := 0
			cleanupOwner := selectedStorePreparationOwnerForTest(t, selected)
			if err := cleanupOwner.BindSelectedProcess(startupCtx, process, capability); err != nil {
				t.Fatal(err)
			}
			_, err = cleanupOwner.RecoverSelectedForkContexts(startupCtx, request, runforkexecution.SelectedForkRecoveryEnvironment{
				SourceInspector: selectedCancellationInspectorCut{SourceArtifactSelectedContractSourceLoader: loader, cleanup: func() error {
					cleanupCalls++
					if cleanupCalls == 1 {
						return cleanupFailure
					}
					return nil
				}}, AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live},
			})
			if !errors.Is(err, cleanupFailure) || cleanupCalls != 1 {
				t.Fatalf("cleanup failure lost ownership: calls=%d %v", cleanupCalls, err)
			}
			if err := cleanupOwner.RetireSelectedContexts(startupCtx); err != nil || cleanupCalls != 2 {
				t.Fatalf("retirement did not retry exact pending cleanup: calls=%d %v", cleanupCalls, err)
			}
			persistentOwner := selectedStorePreparationOwnerForTest(t, selected)
			if err := persistentOwner.BindSelectedProcess(startupCtx, process, capability); err != nil {
				t.Fatal(err)
			}
			cleanupAllowed := false
			_, err = persistentOwner.RecoverSelectedForkContexts(startupCtx, request, runforkexecution.SelectedForkRecoveryEnvironment{
				SourceInspector: selectedCancellationInspectorCut{SourceArtifactSelectedContractSourceLoader: loader, cleanup: func() error {
					if !cleanupAllowed {
						return cleanupFailure
					}
					return nil
				}}, AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live},
			})
			if !errors.Is(err, cleanupFailure) {
				t.Fatalf("persistent cleanup failure lost: %v", err)
			}
			if err := persistentOwner.RetireSelectedContexts(startupCtx); !errors.Is(err, cleanupFailure) {
				t.Fatalf("persistent cleanup falsely acknowledged retirement: %v", err)
			}
			canceledWait, cancelWait := context.WithCancel(context.Background())
			cancelWait()
			if err := process.Wait(canceledWait); !errors.Is(err, context.Canceled) {
				t.Fatalf("failed cleanup released process possession: %v", err)
			}
			cleanupAllowed = true
			if err := persistentOwner.RetireSelectedContexts(startupCtx); err != nil {
				t.Fatal(err)
			}
			if err := process.Wait(canceledWait); err != nil {
				t.Fatalf("successful cleanup retained a leaked process lease: %v", err)
			}
			fenced := selectedStorePreparationOwnerForTest(t, selected)
			if err := fenced.BindSelectedProcess(startupCtx, process, capability); err != nil {
				t.Fatal(err)
			}
			_, err = fenced.RecoverSelectedForkContexts(startupCtx, request, runforkexecution.SelectedForkRecoveryEnvironment{
				SourceInspector: selectedCancellationInspectorCut{SourceArtifactSelectedContractSourceLoader: loader, before: func() {
					if _, err := fenced.RecoverSelectedForkContexts(startupCtx, request, runforkexecution.SelectedForkRecoveryEnvironment{}); err == nil {
						t.Error("concurrent recovery admitted")
					}
					if err := fenced.FenceSelectedContexts(); err != nil {
						t.Error(err)
					}
				}}, AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live},
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("startup fence did not cancel exact preparation: %v", err)
			}
			if err := fenced.RetireSelectedContexts(startupCtx); err != nil {
				t.Fatal(err)
			}
			owner := selectedStorePreparationOwnerForTest(t, selected)
			if err := owner.BindSelectedProcess(startupCtx, process, capability); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			results, err := owner.RecoverSelectedForkContexts(startupCtx, request, runforkexecution.SelectedForkRecoveryEnvironment{
				SourceInspector: loader, AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live},
			})
			if err != nil || len(results) != 1 || len(results[0].CanceledTurns) != 1 || len(results[0].PendingCancellations) != 0 {
				t.Fatalf("startup cancellation handoff: %+v %v", results, err)
			}
			commit := results[0].CanceledTurns[0]
			if commit.Validate() != nil || !commit.Origin.Same(handle.Attempt().Origin) || commit.Publication == nil || commit.Publication.CommittedDurablePublicationEventID() != clock.TimeoutEvent {
				t.Fatalf("startup substituted original cancellation: %+v", commit)
			}
			inspection := completionSettlementFixture{store: selected.(completionSettlementTestStore), db: db, sqlite: sqlite, authority: authority}
			assertCanceledReactionCount(t, startupCtx, inspection, clock.TimeoutEvent, 1)
			if current, err := selected.IsExternalEffectAuthorityCurrent(startupCtx, authority); err != nil || current {
				t.Fatalf("startup restored executable predecessor: %v %v", current, err)
			}
		})
	}
}

type selectedCancellationInspectorCut struct {
	runforkexecution.SourceArtifactSelectedContractSourceLoader
	err     error
	cleanup func() error
	before  func()
}

func (c selectedCancellationInspectorCut) InspectRunForkSelectedContractSourceForRequest(ctx context.Context, request runforkexecution.SelectedContractSourceLoadRequest) (runforkexecution.LoadedSelectedContractSource, error) {
	if c.before != nil {
		c.before()
	}
	loaded, err := c.SourceArtifactSelectedContractSourceLoader.InspectRunForkSelectedContractSourceForRequest(ctx, request)
	loaded.Cleanup = c.cleanup
	return loaded, errors.Join(err, c.err)
}

func TestSelectedCanceledOriginRecoveryCommitsReactionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			f := newSelectedProviderCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash)
			issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, f.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "selected-reaction-recovery", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			admitSelectedProviderFixture(t, ctx, f, issued, authority)
			authority.Target = selectedProviderTarget(f)
			controller := newCompletionControllerForTest(selected)
			ctx = effects.WithAuthority(effects.WithController(ctx, controller), authority)
			ctx = selectedProviderClaimContext(t, ctx, f, authority)
			ctx = withManagedCompletionTestSurface(t, managedSelectedExecutionStoreTestContext(t, ctx, authority), authority, "anthropic_api")
			ctx = effects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.grant_receiver"})
			handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "selected-reaction-recovery")
			clock, found := handle.LogicalTurnClock()
			if !found {
				t.Fatal("selected launch lacks exact clock")
			}
			intent, err := selected.(effects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.process.Release(ctx); err != nil {
				t.Fatal(err)
			}
			process, err := selectedPreparationProcessForTest(t, selected).Evidence()
			if err != nil {
				t.Fatal(err)
			}
			recovery := selected.(interface {
				ListSelectedForkRecoveryEntries(context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
				RecoverSelectedFork(context.Context, runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error)
			})
			entries, err := recovery.ListSelectedForkRecoveryEntries(ctx)
			if err != nil || len(entries) != 1 {
				t.Fatalf("inventory: %+v %v", entries, err)
			}
			request := runcontrol.SelectedForkRecoveryRequest{Entry: entries[0], Process: process, Effects: liveExternalEffectRecoveryRequest(time.Now().UTC())}
			pending, err := recovery.RecoverSelectedFork(ctx, request)
			if err != nil || len(pending.PendingCancellations) != 1 {
				t.Fatalf("physical recovery: %+v %v", pending, err)
			}
			receiver, err := eventreceiver.SelectedRecoveryPublication(authority)
			if err != nil {
				t.Fatal(err)
			}
			deliveryAuthority, err := deliverylifecycle.NewSelectedExecutionAuthority(mustStoreTestSourceArtifactFact(f.request.DeclarationPlan.BundleHash), authority.ID, f.forkRun, authority.SelectedFork.Generation)
			if err != nil {
				t.Fatal(err)
			}
			publication, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore), bus.EventBusOptions{
				SourceArtifactFact: mustStoreTestSourceArtifactFact(f.request.DeclarationPlan.BundleHash),
				ContractBundle:     f.providerSource, ReceiverExecution: receiver, DeliveryAuthority: deliveryAuthority,
			})
			if err != nil {
				t.Fatal(err)
			}
			descriptors, err := rootruntime.AuthorActivityEventDescriptors(f.providerSource)
			if err != nil {
				t.Fatal(err)
			}
			scope, _ := authoractivity.ScopeFromContext(ctx)
			catalog, err := selected.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			defer catalog.Release()
			plan, err := publication.PrepareRecoveredTurnTimeoutReaction(ctx, pending.PendingCancellations[0])
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := publication.ReleaseTurnTimeoutReaction(context.WithoutCancel(ctx), plan); err != nil {
					t.Error(err)
				}
			}()
			request.Cancellations = []effects.CanceledTurnCommand{effects.CanceledTurnCommandForAttempt(handle.Attempt(), plan)}
			inspection := completionSettlementFixture{store: selected.(completionSettlementTestStore), db: db, sqlite: sqlite, authority: authority}
			expireProviderOriginLease(t, inspection, handle.Attempt().Origin.Delivery)
			expired, err := selected.(deliverylifecycle.Store).Snapshot(ctx, handle.Attempt().Origin.Delivery.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			foreignAttempt := handle.Attempt()
			foreignAttempt.Authority.ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
			foreignAttempt.Authority.SelectedFork.ExecutionID = foreignAttempt.Authority.ID
			for _, commands := range [][]effects.CanceledTurnCommand{
				{effects.CanceledTurnCommandForAttempt(handle.Attempt(), nil)},
				{effects.CanceledTurnCommandForAttempt(foreignAttempt, plan)},
				{request.Cancellations[0], request.Cancellations[0]},
			} {
				invalid := request
				invalid.Cancellations = commands
				result, err := recovery.RecoverSelectedFork(ctx, invalid)
				if err == nil || result.RunID != "" || len(result.CanceledTurns) != 0 {
					t.Fatalf("invalid recovery command acknowledged: %+v %v", result, err)
				}
				assertCanceledReactionCount(t, ctx, inspection, intent.CauseEvent, 0)
			}
			installCanceledReactionCut(t, ctx, inspection)
			result, err := recovery.RecoverSelectedFork(ctx, request)
			if err == nil || result.RunID != "" || len(result.CanceledTurns) != 0 {
				t.Fatalf("partial reaction acknowledged: %+v %v", result, err)
			}
			assertCanceledReactionCount(t, ctx, inspection, intent.CauseEvent, 0)
			snapshot, err := selected.(deliverylifecycle.Store).Snapshot(ctx, handle.Attempt().Origin.Delivery.DeliveryID())
			if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != handle.Attempt().Origin.Delivery.Version() || !snapshot.ClaimExpiresAt.Equal(expired.ClaimExpiresAt) {
				t.Fatalf("rollback changed original claim: %+v %v", snapshot, err)
			}
			removeCanceledReactionCut(t, ctx, inspection)
			result, err = recovery.RecoverSelectedFork(ctx, request)
			if err != nil || len(result.CanceledTurns) != 1 || len(result.PendingCancellations) != 0 || result.CanceledTurns[0].Validate() != nil {
				t.Fatalf("atomic selected cancellation: %+v %v", result, err)
			}
			committed := result.CanceledTurns[0]
			if !committed.Origin.Same(handle.Attempt().Origin) || committed.Delivery.Status != deliverylifecycle.StatusCanceled || committed.Publication == nil || committed.Publication.CommittedDurablePublicationEventID() != clock.TimeoutEvent {
				t.Fatalf("substituted settlement: %+v", committed)
			}
			assertCanceledReactionCount(t, ctx, inspection, intent.CauseEvent, 1)
			beforeRetry := snapshotForkHistoricalExecutionTables(t, db, !sqlite)
			repeated, err := recovery.RecoverSelectedFork(ctx, request)
			if err != nil || len(repeated.CanceledTurns) != 1 || !repeated.CanceledTurns[0].Delivery.SettledAt.Equal(committed.Delivery.SettledAt) {
				t.Fatalf("lost acknowledgment retry: %+v %v", repeated, err)
			}
			assertCanceledReactionCount(t, ctx, inspection, intent.CauseEvent, 1)
			afterRetry := snapshotForkHistoricalExecutionTables(t, db, !sqlite)
			for _, table := range []string{"run_fork_selected_contract_runtime_executions", "runtime_agent_turn_lifetimes", "event_delivery_attempts", "events", "agent_turns"} {
				if !reflect.DeepEqual(beforeRetry[table], afterRetry[table]) {
					t.Errorf("exact retry rewrote immutable evidence in %s", table)
				}
			}
			if current, err := selected.IsExternalEffectAuthorityCurrent(ctx, handle.Attempt().Authority); err != nil || current {
				t.Fatalf("recovery reopened selected execution permission: %v %v", current, err)
			}
		})
	}
}

func TestSelectedCanceledOriginRecoveryRetainsPendingIntentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			f := newSelectedProviderCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash)
			issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, f.request)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "selected-canceled-recovery", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			admitSelectedProviderFixture(t, ctx, f, issued, authority)
			authority.Target = selectedProviderTarget(f)
			ctx = effects.WithAuthority(effects.WithController(ctx, newCompletionControllerForTest(selected)), authority)
			ctx = selectedProviderClaimContext(t, ctx, f, authority)
			ctx = withManagedCompletionTestSurface(t, managedSelectedExecutionStoreTestContext(t, ctx, authority), authority, "anthropic_api")
			ctx = effects.WithTurnTimeout(ctx, &timeridentity.TurnTimeout{After: time.Minute, Emit: "test.grant_receiver"})
			handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "selected-canceled-recovery")
			clock, found := handle.LogicalTurnClock()
			if !found {
				t.Fatal("selected launch lacks its exact clock")
			}
			intent, err := selected.(effects.TurnLifetimeStore).RequestTurnTimeout(ctx, handle.Attempt(), clock.DeadlineAt)
			if err != nil || intent.ValidateIntent() != nil {
				t.Fatalf("record exact timeout: %+v %v", intent, err)
			}
			if err := f.process.Release(ctx); err != nil {
				t.Fatal(err)
			}
			process, err := selectedPreparationProcessForTest(t, selected).Evidence()
			if err != nil {
				t.Fatal(err)
			}
			recovery := selected.(interface {
				ListSelectedForkRecoveryEntries(context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
				RecoverSelectedFork(context.Context, runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error)
			})
			entries, err := recovery.ListSelectedForkRecoveryEntries(ctx)
			if err != nil || len(entries) != 1 {
				t.Fatalf("selected recovery inventory: %+v %v", entries, err)
			}
			request := runcontrol.SelectedForkRecoveryRequest{Entry: entries[0], Process: process, Effects: liveExternalEffectRecoveryRequest(time.Now().UTC())}
			result, err := recovery.RecoverSelectedFork(ctx, request)
			if err != nil || result.Effects.OutcomeUncertain != 1 {
				t.Fatalf("recover selected physical tail: %+v %v", result, err)
			}
			snapshot, err := selected.(deliverylifecycle.Store).Snapshot(ctx, handle.Attempt().Origin.Delivery.DeliveryID())
			if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != handle.Attempt().Origin.Delivery.Version() {
				t.Fatalf("recovery overwrote unresolved authored cancellation: %+v %v", snapshot, err)
			}
			if len(result.PendingCancellations) != 1 || result.PendingCancellations[0].Cancellation.ValidateIntent() != nil ||
				!result.PendingCancellations[0].Attempt.Origin.Same(handle.Attempt().Origin) || result.PendingCancellations[0].Clock == nil ||
				!reflect.DeepEqual(*result.PendingCancellations[0].Clock, clock) {
				t.Fatalf("selected recovery omitted exact cancellation/clock handoff: %+v", result)
			}
			normal, err := selected.(effects.CanceledTurnRecoveryStore).ListCanceledTurnRecoveries(ctx, request.Effects)
			if err != nil || len(normal) != 0 {
				t.Fatalf("normal recovery acquired selected cancellation: %+v %v", normal, err)
			}
			repeated, err := recovery.RecoverSelectedFork(ctx, request)
			if err != nil || repeated.Effects != (effects.RecoverySummary{}) || !reflect.DeepEqual(repeated.PendingCancellations, result.PendingCancellations) {
				t.Fatalf("selected acknowledgment retry changed pending evidence: %+v %v", repeated, err)
			}
		})
	}
}
