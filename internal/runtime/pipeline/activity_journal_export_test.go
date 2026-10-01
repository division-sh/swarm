package pipeline

import (
	"context"
	"net/http"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

// These bridges reach the existing dispatcher and pure request constructors;
// they do not expose transaction or storage authority.
func NonIdempotentActivityIntentForTest(runID, sourceEventID, entityID string) runtimeengine.ActivityIntent {
	intent := testNonIdempotentActivityIntent(runID, sourceEventID, entityID)
	source, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: intent.ExecutionFlowID.String(), FlowInstance: intent.FlowInstance, EntityID: entityID,
	})
	if err != nil {
		panic(err)
	}
	intent.RoutingSource = source
	return intent.Normalized()
}

func ActivityAttemptStartForTest(intent runtimeengine.ActivityIntent) ActivityAttemptRecord {
	return activityAttemptStartRecord(intent, activityInputHash(intent.Input))
}

func ActivityAttemptTerminalForTest(record ActivityAttemptRecord, status, eventID, eventType string, payload map[string]any, failure *runtimefailures.Envelope) ActivityAttemptRecord {
	return record.withTerminal(status, eventID, eventType, payload, failure)
}

func ActivitySuccessPayloadForTest(intent runtimeengine.ActivityIntent, result map[string]any) map[string]any {
	return activitySuccessPayload(intent, result)
}

func AdmitActivityResultForTest(intent runtimeengine.ActivityIntent, eventType string) (string, string, error) {
	publication, err := admitActivityPublication(intent, eventType)
	return publication.eventID, string(publication.eventType), err
}

func ExecuteActivityIntentForTest(ctx context.Context, coordinator *PipelineCoordinator, client *http.Client, intent runtimeengine.ActivityIntent) error {
	return (pipelineActivityDispatcher{coordinator: coordinator, client: client}).executeActivityIntent(ctx, intent)
}

func PublishJournaledActivityResultForTest(ctx context.Context, coordinator *PipelineCoordinator, intent runtimeengine.ActivityIntent, receipt ActivityAttemptRecord) error {
	return (pipelineActivityDispatcher{coordinator: coordinator}).publishJournaledActivityResult(ctx, intent, receipt)
}

func CompiledChannelActivityToolForTest(url string) runtimecontracts.ToolSchemaEntry {
	return testCompiledChannelActivityTool(url)
}

func TelegramConnectorToolForTest(url string) runtimecontracts.ToolSchemaEntry {
	return testTelegramConnectorTool(url)
}

func ActivityInputForTest(input map[string]any) semanticvalue.Value {
	return mustActivityInput(input)
}
