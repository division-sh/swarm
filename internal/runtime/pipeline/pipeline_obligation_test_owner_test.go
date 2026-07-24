package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var errPipelineTestObligationUnavailable = errors.New("pipeline unit fixture has no selected-store obligation owner")

type unavailablePipelineTestObligationOwner struct{}

func (unavailablePipelineTestObligationOwner) ClaimPublication(context.Context, string) (runtimepipelineobligation.Claim, error) {
	return runtimepipelineobligation.Claim{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) ClaimEvent(context.Context, string, runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	return runtimepipelineobligation.ClaimedWork{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) ClaimNext(context.Context, runtimepipelineobligation.ClaimQuery) (runtimepipelineobligation.ClaimedWork, bool, error) {
	return runtimepipelineobligation.ClaimedWork{}, false, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) MarkDecisionProcessed(context.Context, runtimepipelineobligation.Claim) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) Settle(context.Context, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) Release(context.Context, runtimepipelineobligation.Claim) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) GlobalWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) SummarizeRun(context.Context, string) (runtimepipelineobligation.RunSummary, error) {
	return runtimepipelineobligation.RunSummary{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) TerminalizeRun(context.Context, string, runtimepipelineobligation.Disposition, time.Time) (int, error) {
	return 0, errPipelineTestObligationUnavailable
}

func (*recordingPipelineBus) PipelineObligationOwner() runtimepipelineobligation.Store {
	return unavailablePipelineTestObligationOwner{}
}

func (noopPipelineBus) PipelineObligationOwner() runtimepipelineobligation.Store {
	return unavailablePipelineTestObligationOwner{}
}

func (pipelineTestBus) PipelineObligationOwner() runtimepipelineobligation.Store {
	return unavailablePipelineTestObligationOwner{}
}

func TestPipelineCoordinatorRequiresCanonicalObligationOwner(t *testing.T) {
	module := staticSemanticWorkflowModule{source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})}
	if preview := newPreviewPipelineCoordinator(previewBus{}, PipelineCoordinatorOptions{Module: module}); preview == nil {
		t.Fatal("explicit preview coordinator was not constructed")
	}

	defer func() {
		if recover() == nil {
			t.Fatal("durable coordinator accepted an ownerless bus")
		}
	}()
	_ = NewPipelineCoordinatorWithOptions(previewBus{}, nil, PipelineCoordinatorOptions{Module: module})
}
