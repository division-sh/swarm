package manager

import (
	"context"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func emptyPipelineWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, nil
}

func emptyPipelineSweep(context.Context, int) (runtimepipelineobligation.SweepResult, error) {
	return runtimepipelineobligation.SweepResult{Exhausted: true}, nil
}

func (*recordingReceiptBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}
func (*recordingReceiptBus) SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	return emptyPipelineSweep(ctx, limit)
}

func (*recordingReceiptBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}

func (*partialOutputRetryBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}
func (*partialOutputRetryBus) SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	return emptyPipelineSweep(ctx, limit)
}

func (*partialOutputRetryBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}

func (*flowActivationTestBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}
func (*flowActivationTestBus) SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	return emptyPipelineSweep(ctx, limit)
}

func (*flowActivationTestBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}

func (*directiveTestBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}
func (*directiveTestBus) SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	return emptyPipelineSweep(ctx, limit)
}

func (*directiveTestBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}

func (*resetTestBus) SweepUndispatched(context.Context, int) (int, error) {
	return 0, nil
}
func (*resetTestBus) SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	return emptyPipelineSweep(ctx, limit)
}

func (*resetTestBus) PipelineWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return emptyPipelineWorkPresence(ctx)
}
