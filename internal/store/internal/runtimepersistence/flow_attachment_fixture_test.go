package runtimepersistence_test

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// Component fixtures have no process resources. Exercise every store step,
// rather than supplying the retired completion marker as execution authority.
func completeFlowAttachmentFixture(ctx context.Context, owner interface {
	AdvanceFlowAttachment(context.Context, pipeline.DynamicFlowRuntimeActivationAttempt, pipeline.FlowAttachmentPhase, time.Time) (pipeline.FlowAttachmentAdvanceResult, error)
}, attempt pipeline.DynamicFlowRuntimeActivationAttempt, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	var result pipeline.FlowAttachmentAdvanceResult
	for _, phase := range []pipeline.FlowAttachmentPhase{pipeline.FlowAttachmentPlanned, pipeline.FlowAttachmentAgentsRegistered, pipeline.FlowAttachmentRouteInstalled, pipeline.FlowAttachmentTimersArmed} {
		var err error
		result, err = owner.AdvanceFlowAttachment(ctx, attempt, phase, at)
		if err != nil || !result.Admitted() {
			return result, errors.Join(err, errors.New("fixture attachment step was not admitted"))
		}
	}
	return result, nil
}
