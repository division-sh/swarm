package tools_test

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func (*humanTaskRuntimeLogBus) PublishEmit(context.Context, events.Event, pipeline.WorkflowPublicationStageRequest) (pipeline.WorkflowEmitResult, error) {
	return pipeline.WorkflowEmitResult{}, fmt.Errorf("human task log fixture does not admit workflow emits")
}

func (*entityToolRuntimeLogBus) PublishEmit(context.Context, events.Event, pipeline.WorkflowPublicationStageRequest) (pipeline.WorkflowEmitResult, error) {
	return pipeline.WorkflowEmitResult{}, fmt.Errorf("entity log fixture does not admit workflow emits")
}

func (entityToolPipelineBus) PublishEmit(context.Context, events.Event, pipeline.WorkflowPublicationStageRequest) (pipeline.WorkflowEmitResult, error) {
	return pipeline.WorkflowEmitResult{}, fmt.Errorf("entity mutation fixture does not admit workflow emits")
}
