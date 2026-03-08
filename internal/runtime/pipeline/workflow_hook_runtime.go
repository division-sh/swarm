package pipeline

import "context"

func (pc *FactoryPipelineCoordinator) PublishWorkflowEvent(ctx context.Context, eventType, verticalID string, payload map[string]any) {
	pc.publish(ctx, eventType, verticalID, payload)
}

func (pc *FactoryPipelineCoordinator) WorkflowPayloadFactory() *PipelinePayloadFactory {
	if pc == nil {
		return nil
	}
	return pc.payloadFactory
}

func ToMap[T any](in T) map[string]any {
	return payloadMap(in)
}

func ScoringCompositeFromPayload(payload map[string]any) ScoringComposite {
	return scoringCompositeFromPayload(payload)
}
