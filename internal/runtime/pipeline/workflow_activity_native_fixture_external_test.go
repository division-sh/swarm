package pipeline_test

import (
	"context"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func workflowActivityNativeFixture(t *testing.T, backend string) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	selected, _, _ := openTimerReplayNativeStore(t, backend)
	ctx := testAuthorActivityContext(t, context.Background())
	persistence := pipeline.NewWorkflowPersistence(selected)
	return pipeline.WorkflowActivityNativeFixtureForTest{
		Persistence: persistence,
		Context:     ctx,
		RequireRun: func(ctx context.Context, runID string) error {
			return storetest.MaterializeRun(ctx, selected, storetest.RunFixture{
				RunID: runID, Origin: storetest.ScenarioSetupOrigin(), StartedAt: time.Now().UTC(),
			})
		},
		NewCoordinator: func(bus pipeline.Bus, options pipeline.PipelineCoordinatorOptions) *pipeline.PipelineCoordinator {
			deliveryBus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: options.Module.SemanticSource()})
			if err != nil {
				t.Fatal(err)
			}
			options.WorkOwner = pipelineExternalTestWorkOwner(t)
			// The recording bus remains the result-publication oracle. Durable
			// delivery and obligation ports still belong to the native bus/store.
			options.ExecutionPosture = executionposture.Live
			options.ReceiverExecution = eventreceiver.NormalExecution()
			options.SourceArtifactFact = authorActivityTestSourceArtifactFact
			options.Persistence = persistence
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
			t.Cleanup(func() {
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := deliveryBus.WaitForQuiescence(join); err != nil {
					t.Error(err)
				}
			})
			return pc
		},
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
