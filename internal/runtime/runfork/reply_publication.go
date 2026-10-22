package runfork

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/google/uuid"
)

type historicalReplyReturns struct {
	runID    string
	revision int64
	rows     map[string]replycontext.Record
}

// The native fixed-cut owner supplies physical-publication to own-row
// correspondence. Logical accepted IDs are not physical IDs after a transfer.
func (p RunForkPlan) WithHistoricalReplyReturns(revision int64, rows map[string]replycontext.Record) (RunForkPlan, error) {
	ids, known := p.HistoricalEventIDs(revision)
	if !known || revision != p.ForkPoint.Revision {
		return RunForkPlan{}, fmt.Errorf("reply returns require exact fixed-cut event membership")
	}
	members := make(map[string]bool, len(ids))
	for _, id := range ids {
		members[id] = true
	}
	bound := historicalReplyReturns{runID: p.SourceRunID, revision: revision, rows: make(map[string]replycontext.Record, len(rows))}
	seen := make(map[string]bool, len(rows))
	for eventID, row := range rows {
		id, err := uuid.Parse(eventID)
		if err != nil || id == uuid.Nil || id.String() != eventID || !members[eventID] || seen[row.ID] {
			return RunForkPlan{}, fmt.Errorf("reply return has foreign or duplicate historical publication")
		}
		if err := p.requireCapturedTerminalReply(row); err != nil {
			return RunForkPlan{}, err
		}
		seen[row.ID] = true
		bound.rows[eventID] = row.Normalized()
	}
	p.historicalReplyReturns = &bound
	return p, nil
}

func (p RunForkPlan) HistoricalReplyReturn(eventID string) (*replycontext.Record, error) {
	bound := p.historicalReplyReturns
	if eventID == "" || bound == nil {
		return nil, nil
	}
	if bound.runID != p.SourceRunID || bound.revision != p.ForkPoint.Revision || bound.revision != p.historicalRevision {
		return nil, fmt.Errorf("reply return correspondence does not belong to the fixed cut")
	}
	row, found := bound.rows[eventID]
	if !found {
		return nil, nil
	}
	members, known := p.HistoricalEventIDs(bound.revision)
	member := false
	for _, id := range members {
		member = member || id == eventID
	}
	if !known || !member {
		return nil, fmt.Errorf("reply return publication is outside fixed source history")
	}
	if err := p.requireCapturedTerminalReply(row); err != nil {
		return nil, err
	}
	copy := row.Normalized()
	return &copy, nil
}

func (p RunForkPlan) requireCapturedTerminalReply(row replycontext.Record) error {
	if row.RunID != p.SourceRunID || row.State != replycontext.StateTerminal || row.Origin.FlowID != row.RequesterFlowID {
		return fmt.Errorf("captured reply settlement has contradictory return ownership")
	}
	if err := row.ValidateContinuationFrom(row, row.CreatedAt); err != nil {
		return err
	}
	matched := false
	for _, captured := range p.ReplyContexts {
		if captured.ID != row.ID {
			continue
		}
		if matched {
			return fmt.Errorf("reply return has duplicate captured own contexts")
		}
		if err := captured.ValidateContinuationFrom(row, row.CreatedAt); err != nil {
			return err
		}
		matched = true
	}
	if !matched {
		return fmt.Errorf("reply return lacks its exact captured own context")
	}
	return nil
}
