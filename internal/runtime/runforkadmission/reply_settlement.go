package runforkadmission

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// Only an already accepted publication has a captured return destination.
// Future replies remain runtime-bound; neither provider identity nor correlation
// predicts their target. No physical source event is loaded to resolve a row.
func capturedFrontierReplySettlement(plan runfork.RunForkPlan, eventID string) (*replycontext.Record, error) {
	if eventID == "" {
		return nil, nil
	}
	var found *replycontext.Record
	for _, record := range plan.ReplyContexts {
		if record.AcceptedReplyEventID != eventID {
			continue
		}
		if found != nil || record.RunID != plan.SourceRunID || record.State != replycontext.StateTerminal ||
			record.Origin.FlowID != record.RequesterFlowID {
			return nil, fmt.Errorf("captured reply settlement has contradictory return ownership")
		}
		if err := record.Validate(); err != nil {
			return nil, err
		}
		copy := record.Normalized()
		found = &copy
	}
	return found, nil
}
