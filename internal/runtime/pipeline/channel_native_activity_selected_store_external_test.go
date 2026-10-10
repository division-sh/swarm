package pipeline_test

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	sessionexecution "github.com/division-sh/swarm/internal/sessionprovider/execution"
	"github.com/google/uuid"
)

// This is the selected-journal refusal/replay proof, not an installed SDK send.
func TestNativeActivityWithoutOriginalActivationJournalsRefusalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, closeStore, reopen := openActivityReplayStore(t, backend)
			ctx := testAuthorActivityContext(t, context.Background())
			runID := uuid.NewString()
			requireActivityReplayRun(t, ctx, selected, runID)
			tool := contracts.MustToolSchemaEntry(
				contracts.WithToolHandler(contracts.ToolHandlerInProcess),
				contracts.WithToolCategory("provider_connector"),
				contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppSendText),
				contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
				contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
			)
			source := semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{"provider_write": tool}})
			var selectedSDK atomic.Int32
			options := pipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source},
				NativeChannelExecution: func(context.Context, channelonboarding.CompiledActivation) (sessionexecution.Channel, error) {
					selectedSDK.Add(1)
					return sessionexecution.Channel{}, nil
				}}
			bus := newActivityJournalProofBus(t, selected, source)
			pc := newGateRecoveryCoordinator(bus, selected, options)
			intent := pipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString())
			seedSelectedActivitySource(t, ctx, selected, intent)
			if err := pipeline.ExecuteActivityIntentForTest(ctx, pc, nil, intent); err != nil {
				t.Fatal(err)
			}
			record, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, pipeline.ActivityAttemptStartForTest(intent).RequestEventID)
			if err != nil || !found || record.Status != pipeline.ActivityAttemptStatusFailed || record.Failure == nil || selectedSDK.Load() != 0 {
				t.Fatal("uninstalled native activity reached SDK or lost its terminal refusal", record, found, err, selectedSDK.Load())
			}
			if err := bus.WaitForQuiescence(ctx); err != nil {
				t.Fatal(err)
			}
			if err := closeStore(); err != nil {
				t.Fatal(err)
			}
			selected = reopen()
			bus = newActivityJournalProofBus(t, selected, source)
			pc = newGateRecoveryCoordinator(bus, selected, options)
			if err := pipeline.ExecuteActivityIntentForTest(ctx, pc, nil, intent); err != nil {
				t.Fatal(err)
			}
			after, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, record.RequestEventID)
			if err != nil || !found || !reflect.DeepEqual(after, record) || selectedSDK.Load() != 0 {
				t.Fatal("restart replay changed refusal or reacquired native execution", after, found, err, selectedSDK.Load())
			}
		})
	}
}
