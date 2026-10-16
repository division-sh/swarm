package pipeline_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func workflowActivityNativeFixture(t *testing.T, backend string) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	return workflowActivityNativeFixtureFromSelected(t, selected, reopen)
}

func TestS1DirectoryConnectorDispatchUsesExactDeclarationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyS1DirectoryConnectorDispatchUsesExactDeclarationForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowActivityNativeSourceFixture(t, backend, source)
			}, composeS1NativeEffectiveSource)
		})
	}
}

func composeS1NativeEffectiveSource(t *testing.T, source semanticview.Source) semanticview.Source {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("native activity journey requires its compiled source")
	}
	admitted, err := packadmission.Admit(bundle.PackInventory, bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	bundle.PackAdmission = admitted
	projection, err := runtimepkg.AdmitEffectiveSourceProjection(runtimepkg.EffectiveSourceProjectionRequest{
		Source: source, SourceArtifactFact: sourceartifactfixture.FactFor(bundle.SourceArtifact),
	})
	if err != nil {
		t.Fatalf("compile real directory-owned effective source: %v", err)
	}
	return projection.Source()
}

func workflowActivityNativeFixtureFromSelected(t *testing.T, selected timerReplaySelectedStore, reopen func() timerReplaySelectedStore) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	rooted, _ := sourceartifactfixture.Rooted(t, "workflow-activity", sourceartifactfixture.Artifact())
	return workflowActivityNativeFixtureFromSelectedSource(t, selected, reopen, rooted.Artifact())
}

func workflowActivityNativeSourceFixture(t *testing.T, backend string, source semanticview.Source) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("native activity source fixture requires its compiled source artifact")
	}
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	return workflowActivityNativeFixtureFromSelectedSource(t, selected, reopen, bundle.SourceArtifact)
}

func workflowActivityNativeFixtureFromSelectedSource(t *testing.T, selected timerReplaySelectedStore, reopen func() timerReplaySelectedStore, artifact *sourceartifact.AdmittedSourceArtifact) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	// Keep the first artifact write inside RequireRun's accounting boundary.
	rooted := sourceartifactfixture.CapturedRoot(t, artifact)
	fact := sourceartifactfixture.FactFor(artifact)
	ctx := testAuthorActivityContextForSource(t, context.Background(), fact)
	ctx = runtimecorrelation.WithRootAdmission(ctx, rooted.Admission())
	persistence := pipeline.NewWorkflowPersistence(selected)
	var buses []*runtimebus.EventBus
	join := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, bus := range buses {
			if err := bus.WaitForQuiescence(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	coordinator := func(bus pipeline.Bus, options pipeline.PipelineCoordinatorOptions, workflow pipeline.WorkflowPersistence) *pipeline.PipelineCoordinator {
		if options.ExecutionPosture == "" {
			options.ExecutionPosture = executionposture.Live
		}
		deliveryBus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{
			ContractBundle: options.Module.SemanticSource(), ExecutionPosture: options.ExecutionPosture, SourceArtifactFact: fact,
		})
		if err != nil {
			t.Fatal(err)
		}
		buses = append(buses, deliveryBus)
		options.WorkOwner = pipelineExternalTestWorkOwnerForSource(t, fact)
		// The recording bus remains the result-publication oracle. Durable
		// delivery and obligation ports still belong to the native bus/store.
		options.ReceiverExecution = eventreceiver.NormalExecution()
		options.SourceArtifactFact = fact
		options.Persistence = workflow
		options.DeliveryStore, options.DeadLetters = selected, selected
		options.DecisionCards, options.ProposedEffects = selected, selected
		options.HumanTasks, options.DecisionCardDraftExpiry, options.HumanTaskExpiry = selected, selected, selected
		options.RunLifecycle = selected
		options.PipelineObligations = selected.PipelineObligations()
		options.DeliveryRuntime = deliveryBus
		if bus == nil {
			bus = deliveryBus
		}
		pc := pipeline.NewPipelineCoordinatorWithOptions(bus, options)
		if pc == nil {
			t.Fatal("native activity coordinator rejected its selected semantic owners")
		}
		// Register after the bus catalog lease so cleanup joins this bus
		// before releasing its catalog, runtime occurrence or selected store.
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := deliveryBus.WaitForQuiescence(ctx); err != nil {
				t.Error(err)
			}
		})
		return pc
	}
	return pipeline.WorkflowActivityNativeFixtureForTest{
		Persistence: persistence,
		Context:     ctx,
		Runs:        selected,
		RequireRun: func(ctx context.Context, runID string) error {
			return storetest.MaterializeRun(ctx, selected, storetest.RunFixture{
				RunID: runID, Origin: storetest.ScenarioSetupOrigin(), StartedAt: time.Now().UTC(), Artifact: artifact, RootAdmission: rooted.Admission(),
			})
		},
		NewCoordinator: func(bus pipeline.Bus, options pipeline.PipelineCoordinatorOptions) *pipeline.PipelineCoordinator {
			return coordinator(bus, options, persistence)
		},
		CleanupFault: func(fault error) (pipeline.WorkflowPersistence, func() int32) {
			return storetest.ActivityJournalCleanupPersistenceFault(t, selected, fault)
		},
		CleanupFaultCoordinator: func(bus pipeline.Bus, options pipeline.PipelineCoordinatorOptions, fault error) (*pipeline.PipelineCoordinator, func() int32) {
			faulted, count := storetest.ActivityJournalCleanupPersistenceFault(t, selected, fault)
			return coordinator(bus, options, faulted), count
		},
		Construct: func(ctx context.Context, instance pipeline.WorkflowInstance) error {
			command, err := flowactivationfixture.Command(ctx, instance, pipeline.WorkflowLifecycleMutationPlan{}, instance.CreatedAt)
			if err != nil {
				return err
			}
			committed, err := selected.(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
			if err != nil {
				return err
			}
			if !committed.Acknowledged || !committed.Created {
				return fmt.Errorf("loop component construction did not acknowledge exact native creation")
			}
			return nil
		},
		ClaimReplyLossCoordinator: func(ctx context.Context, bus pipeline.Bus, options pipeline.PipelineCoordinatorOptions, run, request string, fault error) (*pipeline.PipelineCoordinator, func() int32) {
			faulted, count := storetest.ActivityClaimReplyLossPersistenceFault(t, ctx, selected, run, request, fault)
			return coordinator(bus, options, faulted), count
		},
		ReadAttemptStatuses: func(ctx context.Context) ([]string, error) {
			return storetest.ReadWorkflowActivityAttemptStatuses(ctx, selected)
		},
		EventIDCount: func(ctx context.Context, event string) int {
			return storetest.ObserveEventCardinality(t, ctx, selected, event)
		},
		Publish: func(ctx context.Context, event events.Event) {
			storetest.CommitSemanticEvent(t, ctx, selected, event)
		},
		Reopen: func() pipeline.WorkflowActivityNativeFixtureForTest {
			if err := join(); err != nil {
				t.Fatal(err)
			}
			if err := selected.Close(); err != nil {
				t.Fatal(err)
			}
			return workflowActivityNativeFixtureFromSelectedSource(t, reopen(), reopen, artifact)
		},
		ReadJournal: func(ctx context.Context, runID string) (pipeline.WorkflowJournalStorageForTest, error) {
			observed, err := storetest.ReadWorkflowJournalStorage(ctx, selected, runID)
			return pipeline.WorkflowJournalStorageForTest(observed), err
		},
		CreateReply: func(ctx context.Context, runID, requestEventID, replyContextID string) error {
			at := time.Now().UTC()
			event := eventtest.PersistedRuntimeControlForProducer(requestEventID, events.EventType("provider.requested"),
				eventtest.Producer(events.EventProducerPlatform, "test"), "", []byte(`{}`), 0,
				runID, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, at)
			storetest.CommitSemanticEvent(t, ctx, selected, event)
			return selected.CreateReplyContext(ctx, replycontext.Record{
				ID: replyContextID, RunID: runID, RequestEventID: requestEventID,
				RequesterFlowID: "requester", RequestOutputPin: "provider_requested", ReplyInputPin: "provider_replied",
				ProviderFlowID: "provider", ProviderInputPin: "provider_requested", ProviderOutputPin: "provider_replied",
				Origin:               events.RouteIdentity{FlowID: "requester", FlowInstance: "requester/a", EntityID: "entity-a"},
				RequestCorrelationID: requestEventID, State: replycontext.StateOpen, CreatedAt: at, UpdatedAt: at,
			})
		},
	}
}

func TestActivityJournalFixtureTerminalNoopBothStores(t *testing.T) {
	pipeline.VerifyActivityJournalFixtureTerminalNoopBothStoresForTest(t, workflowActivityNativeFixture)
}

func TestHumanTaskExpiryUsesClosedSelectedStoreCommitEvidence(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyHumanTaskExpiryUsesClosedSelectedStoreCommitEvidenceForTest)
}

func TestHumanTaskExpiryAcknowledgementControlsPublicationEffects(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyHumanTaskExpiryAcknowledgementControlsPublicationEffectsForTest)
}

func TestForkedSourceWorkflowInstanceMutationsRefuseAndPreserveReadback(t *testing.T) {
	pipeline.VerifyForkedSourceWorkflowInstanceMutationsRefuseAndPreserveReadbackForTest(t, workflowActivityNativeFixture)
}

func TestForkedSourceActivityAttemptMutationsRefuseAndPreserveJournal(t *testing.T) {
	pipeline.VerifyForkedSourceActivityAttemptMutationsRefuseAndPreserveJournalForTest(t, workflowActivityNativeFixture)
}

func TestReceiverConfigAndRuntimeControlsRoundTripBothStores(t *testing.T) {
	pipeline.VerifyReceiverConfigAndRuntimeControlsRoundTripBothStoresForTest(t, workflowActivityNativeFixture)
}

func TestActivityBoringProofDuplicateRequestReusesRecordedReadResult(t *testing.T) {
	pipeline.VerifyNativeActivityBoringProofDuplicateRequestReusesRecordedReadResultForTest(t, workflowActivityNativeSourceFixture)
}

func TestActivityBoringProofReadOnlyForkReexecuteUsesForkLocalRequestIdentity(t *testing.T) {
	pipeline.VerifyNativeActivityBoringProofReadOnlyForkReexecuteUsesForkLocalRequestIdentityForTest(t, workflowActivityNativeSourceFixture)
}

func TestActivityBoringProofRetryIsBoundedAndTraced(t *testing.T) {
	pipeline.VerifyNativeActivityBoringProofRetryIsBoundedAndTracedForTest(t, workflowActivityNativeSourceFixture)
}

func TestScenarioExecutionProfileMismatchFencesTerminalReplay(t *testing.T) {
	pipeline.VerifyScenarioExecutionProfileMismatchFencesTerminalReplayForTest(t, func(t *testing.T) pipeline.WorkflowActivityNativeFixtureForTest {
		return workflowActivityNativeFixture(t, "sqlite")
	})
}

func TestPipelineActivityRequestTelegramConnectorRoundTripThroughInboundDelivery(t *testing.T) {
	pipeline.VerifyPipelineActivityRequestTelegramConnectorRoundTripThroughInboundDeliveryForTest(t, workflowActivityNativeFixture)
}

func TestActivityAttemptJournalSQLiteAndPostgres(t *testing.T) {
	pipeline.VerifyActivityAttemptJournalSQLiteAndPostgresForTest(t, workflowActivityNativeFixture)
}

func TestActivityAttemptJournalPreservesReplyContextAcrossRestart(t *testing.T) {
	pipeline.VerifyActivityAttemptJournalPreservesReplyContextAcrossRestartForTest(t, workflowActivityNativeFixture)
}

func TestChannelProjectedActivityResultJournalsAndReplaysAcrossSelectedStores(t *testing.T) {
	pipeline.VerifyChannelProjectedActivityResultJournalsAndReplaysAcrossSelectedStoresForTest(t, workflowActivityNativeFixture)
}

func TestChannelActivityPostCommitAcknowledgmentLossStateBlocksRedispatchAcrossSelectedStores(t *testing.T) {
	pipeline.VerifyChannelActivityPostCommitAcknowledgmentLossStateBlocksRedispatchAcrossSelectedStoresForTest(t, workflowActivityNativeFixture)
}

func TestActivityTerminalPostCommitErrorPublishesJournaledResultBothStores(t *testing.T) {
	pipeline.VerifyActivityTerminalPostCommitErrorPublishesJournaledResultBothStoresForTest(t, workflowActivityNativeFixture)
}

func TestActivityJournalFixtureTerminalAcknowledgementSurvivesPostCommitError(t *testing.T) {
	pipeline.VerifyActivityJournalFixtureTerminalAcknowledgementSurvivesPostCommitErrorForTest(t, workflowActivityNativeFixture)
}

func TestLoopActivityClaimOrdersAgainstRepeatAndCloseOnBothStores(t *testing.T) {
	pipeline.VerifyLoopActivityClaimOrdersAgainstRepeatAndCloseOnBothStoresForTest(t, workflowActivityNativeFixture)
}

func TestLoopActivityClaimCommitAcknowledgmentLossReconcilesWithoutDispatch(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyLoopActivityClaimCommitAcknowledgmentLossReconcilesWithoutDispatchForTest)
}

func TestPipelineActivityRequestMockFlowLocalProviderConnectorUsesGeneratedResponseAndJournal(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyPipelineActivityRequestMockFlowLocalProviderConnectorUsesGeneratedResponseAndJournalForTest)
}

func TestMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTP(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyMockOnlyPostureRejectsLiveActivityBeforeJournalCredentialsAndHTTPForTest)
}

func TestPipelineActivityRequestMockTerminalReplayDoesNotRequireCurrentResponsePlan(t *testing.T) {
	pipeline.VerifyPipelineActivityRequestMockTerminalReplayDoesNotRequireCurrentResponsePlanForTest(t, workflowActivityNativeFixture)
}

func TestPipelineActivityRequestMockAdmissionFailsBeforeJournalCredentialsAndHTTP(t *testing.T) {
	pipeline.VerifyPipelineActivityRequestMockAdmissionFailsBeforeJournalCredentialsAndHTTPForTest(t, workflowActivityNativeFixture)
}

func TestWorkflowActivityNativeReopenUsesFreshOwnerAndRetainsJournalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := workflowActivityNativeFixture(t, backend)
			runID := uuid.NewString()
			if err := fixture.RequireRun(fixture.Context, runID); err != nil {
				t.Fatal(err)
			}
			before, err := fixture.ReadJournal(fixture.Context, runID)
			if err != nil {
				t.Fatal(err)
			}
			reopened := fixture.Reopen()
			if reopened.Persistence == fixture.Persistence || !reopened.Persistence.Valid() {
				t.Fatal("reopen reused an old persistence owner or returned incomplete roles")
			}
			if got, err := fixture.ReadJournal(fixture.Context, runID); err == nil || got != (pipeline.WorkflowJournalStorageForTest{}) {
				t.Fatalf("old closed native owner still granted journal evidence: %+v %v", got, err)
			}
			if got, err := reopened.ReadJournal(reopened.Context, runID); err != nil || got != before {
				t.Fatalf("fresh native reopen changed durable storage: %+v want %+v err=%v", got, before, err)
			}
		})
	}
}

func verifyNativeActivityBothStores(t *testing.T, verify func(*testing.T, func(*testing.T) pipeline.WorkflowActivityNativeFixtureForTest)) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			verify(t, func(t *testing.T) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowActivityNativeFixture(t, backend)
			})
		})
	}
}

func TestPipelineActivityRequestExecutesNonIdempotentHTTPToolOnceWithStaticCredentials(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyPipelineActivityRequestExecutesNonIdempotentHTTPToolOnceWithStaticCredentialsForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowActivityNativeSourceFixture(t, backend, source)
			})
		})
	}
}

func TestS1ScopedActivityDispatchUsesExactDeclarationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyS1ScopedActivityDispatchUsesExactDeclarationForTest(t, func(t *testing.T, source semanticview.Source) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowActivityNativeSourceFixture(t, backend, source)
			}, composeS1NativeEffectiveSource)
		})
	}
}

func TestGeneratedSyntheticConnectorUsesCanonicalActivityJournalOnReplay(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyGeneratedSyntheticConnectorUsesCanonicalActivityJournalOnReplayForTest)
}

func TestPipelineActivityRequestNonIdempotentFailureDoesNotRetry(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyPipelineActivityRequestNonIdempotentFailureDoesNotRetryForTest)
}

func TestPipelineActivityRequestNonIdempotentTransportErrorMarksUncertain(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyPipelineActivityRequestNonIdempotentTransportErrorMarksUncertainForTest)
}

func TestPipelineActivityRequestStartedJournalBlocksProviderRedispatchWithoutTerminalizing(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyPipelineActivityRequestStartedJournalBlocksProviderRedispatchWithoutTerminalizingForTest)
}

func TestPipelineActivityRequestConcurrentDuplicatePreservesOriginalTerminalResult(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyPipelineActivityRequestConcurrentDuplicatePreservesOriginalTerminalResultForTest)
}

func TestPipelineActivityRequestMissingCredentialFailsAfterClaimBeforeDispatch(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyPipelineActivityRequestMissingCredentialFailsAfterClaimBeforeDispatchForTest)
}

func TestPipelineActivityRequestTelegramConnectorMissingTokenFailsAfterClaimBeforeDispatch(t *testing.T) {
	verifyNativeActivityBothStores(t, pipeline.VerifyPipelineActivityRequestTelegramConnectorMissingTokenFailsAfterClaimBeforeDispatchForTest)
}
