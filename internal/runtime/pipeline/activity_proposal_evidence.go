package pipeline

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

// ValidateRecordedActivityProposal checks the released request with the same
// request and continuation decoders used by execution, not a fork-only parser.
func ValidateRecordedActivityProposal(request events.Event, card decisioncard.Card, continuation decisioncard.ProposedEffectContinuation, decision, bundleHash string) error {
	if err := card.Validate(); err != nil {
		return err
	}
	if err := continuation.Validate(card); err != nil {
		return err
	}
	if decision == "" || card.Snapshot.Decision != decision || card.CardID != decisioncard.ProposedEffectCardID(request.ID(), decision) ||
		card.RunID != request.RunID() || continuation.RequestEventID != request.ID() || continuation.BundleHash != bundleHash {
		return fmt.Errorf("recorded activity proposal contradicts its originating declaration or request")
	}
	actual, err := activityIntentFromRequestEvent(request)
	if err != nil {
		return err
	}
	expected, err := activityIntentFromProposedEffect(continuation, request.RoutingSource())
	if err != nil {
		return err
	}
	if !recordedActivityRequestMatches(actual, expected) {
		return fmt.Errorf("recorded activity proposal changed its exact released request")
	}
	if card.Status != decisioncard.StatusDecided || card.Verdict != "approve" ||
		continuation.Verdict != card.Verdict || continuation.DecisionEventID != card.DecisionEventID ||
		continuation.State != decisioncard.ProposedEffectRequestReleased {
		return fmt.Errorf("recorded activity proposal is not settled approved evidence")
	}
	return nil
}

func recordedActivityRequestMatches(actual, expected runtimeengine.ActivityIntent) bool {
	return actual.ActivityID == expected.ActivityID && actual.Tool == expected.Tool && actual.BundleHash == expected.BundleHash &&
		actual.WorkflowVersion == expected.WorkflowVersion && actual.Input.Equal(expected.Input) &&
		actual.SuccessEvent == expected.SuccessEvent && actual.FailureEvent == expected.FailureEvent &&
		actual.RevisionEvent == expected.RevisionEvent && actual.RejectedEvent == expected.RejectedEvent &&
		actual.EntityID == expected.EntityID && actual.Owner == expected.Owner && actual.ExecutionFlowID == expected.ExecutionFlowID &&
		actual.FlowInstance == expected.FlowInstance && actual.HandlerEventKey == expected.HandlerEventKey &&
		actual.SourceEventID == expected.SourceEventID && actual.SourceRunID == expected.SourceRunID &&
		actual.SourceTaskID == expected.SourceTaskID && actual.ParentEventID == expected.ParentEventID &&
		actual.ChainDepth == expected.ChainDepth && actual.Generation == expected.Generation && actual.LoopStage == expected.LoopStage &&
		actual.ExecutionMode == expected.ExecutionMode && actual.Context.ReplyContextID() == expected.Context.ReplyContextID()
}
