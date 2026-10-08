package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// PublishEmit uses the ordinary publication and receiver owners. It neither
// waits for queued work nor passes the producer's authority to receivers.
func resolveCommittedEmitStage(handoffCtx context.Context, owner pipeline.WorkflowEmitFeedbackOwner, eventID string, request pipeline.WorkflowPublicationStageRequest, candidate pipeline.WorkflowEmitFeedback) (pipeline.WorkflowEmitFeedback, error) {
	evidence, found, readErr := owner.ReadWorkflowPublicationStages(handoffCtx, eventID, request.Instance)
	if readErr != nil || !found {
		return candidate, errors.Join(readErr, fmt.Errorf("accepted emit lacks exact stage evidence"))
	}
	if evidence.Acceptance != candidate.Receipt {
		return candidate, errors.Join(fmt.Errorf("emit acceptance changed after dispatch"))
	}
	for _, receipt := range evidence.Handlers {
		if receipt.Stage().Revision > candidate.Receipt.Stage().Revision {
			candidate.Receipt, candidate.StageOrigin = receipt, pipeline.EmitStageHandler
		}
	}
	return candidate, nil
}

func (eb *EventBus) PublishEmit(ctx context.Context, event events.Event, request pipeline.WorkflowPublicationStageRequest) (result pipeline.WorkflowEmitResult, err error) {
	if err := request.ValidateEvent(event); err != nil {
		return result, err
	}
	owner := eb.durable.EmitFeedback
	if owner == nil {
		return result, errors.New("emit requires the selected publication feedback owner")
	}
	ctx, lease, err := eb.beginRuntimeWork(ctx)
	if err != nil {
		return result, err
	}
	if lease != nil {
		defer func() { err = errors.Join(err, lease.Done()) }()
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return result, err
	}
	prepared, ready, err := eb.commitPublish(ctx, eventBusCommitPublishPlan{bus: eb, event: event, stageFeedback: &request})
	acceptance, accepted := prepared.AcceptedPublicationStage()
	if !accepted {
		return result, err
	}
	result.Accepted = true
	handoffCtx := publicationHandoffContext(ctx, err)
	defer func() { err = errors.Join(err, prepared.publicationClaim.Release(handoffCtx)) }()
	if prepared.exactDuplicate {
		feedback, found, readErr := owner.ReadWorkflowEmitFeedback(handoffCtx, event.ID(), request.Instance)
		if readErr != nil {
			return result, errors.Join(err, readErr)
		}
		if found {
			result.Feedback, result.FeedbackCommitted, result.Replay = feedback, true, true
			if feedback.Dispatch == pipeline.EmitDispatchError {
				err = errors.Join(err, errors.New("acknowledged emit result records a post-commit dispatch error"))
			}
			return result, err
		}
		// A result lost before its feedback commit is not proof that its local
		// dispatch returned successfully. Consume exact receipts, never dispatch again.
		err = errors.Join(err, errors.New("accepted publication has no acknowledged emit result"))
	} else if ready {
		err = errors.Join(err, eb.dispatchPreparedPublish(handoffCtx, prepared))
	}
	handoffCtx = publicationHandoffContext(ctx, err)
	candidate := pipeline.WorkflowEmitFeedback{Receipt: acceptance, StageOrigin: pipeline.EmitStageAcceptance, Dispatch: pipeline.EmitDispatchReturned}
	if !ready || err != nil {
		candidate.Dispatch = pipeline.EmitDispatchError
	} else if prepared.dispatchQueued {
		candidate.Dispatch = pipeline.EmitDispatchQueued
	}
	if candidate.Dispatch != pipeline.EmitDispatchQueued {
		resolved, resolveErr := resolveCommittedEmitStage(handoffCtx, owner, event.ID(), request, candidate)
		if resolveErr != nil {
			return result, errors.Join(err, resolveErr)
		}
		candidate = resolved
	}
	committed, commitErr := owner.CommitWorkflowEmitFeedback(handoffCtx, candidate)
	if !committed.Acknowledged {
		return result, errors.Join(err, commitErr, errors.New("emit feedback commit was not acknowledged"))
	}
	result.Feedback, result.FeedbackCommitted, result.Replay = committed.Feedback, true, committed.Replay || prepared.exactDuplicate
	return result, errors.Join(err, commitErr)
}
