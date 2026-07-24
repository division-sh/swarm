package manager

import (
	"context"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func emptyPipelineWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, nil
}

func (*recordingReceiptBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}

func (*recordingReceiptBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}

func (*partialOutputRetryBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}

func (*partialOutputRetryBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}

func (*flowActivationTestBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}

func (*flowActivationTestBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}

func (*directiveTestBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}

func (*directiveTestBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}

func (*resetTestBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}

func (*resetTestBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}
