package pipeline_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

func workflowActivityNativeFixture(t *testing.T, backend string) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	return workflowActivityNativeFixtureFromSelected(t, selected, reopen)
}

func workflowActivityNativeFixtureFromSelected(t *testing.T, selected timerReplaySelectedStore, reopen func() timerReplaySelectedStore) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
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
			ContractBundle: options.Module.SemanticSource(), ExecutionPosture: options.ExecutionPosture,
		})
		if err != nil {
			t.Fatal(err)
		}
		buses = append(buses, deliveryBus)
		options.WorkOwner = pipelineExternalTestWorkOwner(t)
		// The recording bus remains the result-publication oracle. Durable
		// delivery and obligation ports still belong to the native bus/store.
		options.ReceiverExecution = eventreceiver.NormalExecution()
		options.SourceArtifactFact = authorActivityTestSourceArtifactFact
		options.Persistence = workflow
		options.DeliveryStore, options.DeadLetters = selected, selected
		options.DecisionCards, options.ProposedEffects = selected, selected
		options.HumanTasks, options.DecisionCardDraftExpiry, options.HumanTaskExpiry = selected, selected, selected
		options.RunLifecycle = selected
		options.PipelineObligations = selected.PipelineObligations()
		options.DeliveryRuntime = deliveryBus
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
		RequireRun: func(ctx context.Context, runID string) error {
			return storetest.MaterializeRun(ctx, selected, storetest.RunFixture{
				RunID: runID, Origin: storetest.ScenarioSetupOrigin(), StartedAt: time.Now().UTC(),
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
		Reopen: func() pipeline.WorkflowActivityNativeFixtureForTest {
			if err := join(); err != nil {
				t.Fatal(err)
			}
			if err := selected.Close(); err != nil {
				t.Fatal(err)
			}
			return workflowActivityNativeFixtureFromSelected(t, reopen(), reopen)
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
	verifyNativeActivityBothStores(t, pipeline.VerifyPipelineActivityRequestExecutesNonIdempotentHTTPToolOnceWithStaticCredentialsForTest)
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
