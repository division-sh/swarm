package runforkpersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func capturedRunForkEnclosingReply(plan runfork.RunForkPlan, source replycontext.Record) (replycontext.Record, error) {
	var enclosing replycontext.Record
	for _, captured := range plan.ReplyContexts {
		if captured.ID != source.EnclosingReplyContextID {
			continue
		}
		if enclosing.ID != "" {
			return replycontext.Record{}, fmt.Errorf("fork reply repeats its captured enclosing context")
		}
		enclosing = captured
	}
	if enclosing.ID == "" || enclosing.RunID != plan.SourceRunID {
		return replycontext.Record{}, fmt.Errorf("fork reply lacks its exact captured enclosing obligation")
	}
	if err := source.ValidateEnclosing(enclosing); err != nil {
		return replycontext.Record{}, err
	}
	if err := validateRunForkReplyCanonicalRecord(enclosing); err != nil {
		return replycontext.Record{}, err
	}
	return enclosing, nil
}

// Parents precede their children so native self-reference constraints are
// effective during materialization, without deferred or repaired references.
func orderRunForkReplyContexts(plan runfork.RunForkPlan) ([]replycontext.Record, error) {
	byID := make(map[string]replycontext.Record, len(plan.ReplyContexts))
	for _, record := range plan.ReplyContexts {
		if _, duplicate := byID[record.ID]; duplicate {
			return nil, fmt.Errorf("fork reply repeats captured context %s", record.ID)
		}
		byID[record.ID] = record
	}
	ordered := make([]replycontext.Record, 0, len(byID))
	seen := make(map[string]uint8, len(byID))
	var visit func(replycontext.Record) error
	visit = func(record replycontext.Record) error {
		switch seen[record.ID] {
		case 1:
			return fmt.Errorf("fork reply contains cyclic enclosing obligations")
		case 2:
			return nil
		}
		seen[record.ID] = 1
		if record.EnclosingReplyContextID != "" {
			parent, err := capturedRunForkEnclosingReply(plan, record)
			if err != nil {
				return err
			}
			if err := visit(parent); err != nil {
				return err
			}
		}
		seen[record.ID] = 2
		ordered = append(ordered, record)
		return nil
	}
	for _, record := range plan.ReplyContexts {
		if err := visit(record); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}
