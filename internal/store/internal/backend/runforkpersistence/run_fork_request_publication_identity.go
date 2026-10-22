package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func (a runForkSourceStateAdmission) capturedRequestPublication(ctx context.Context, tx *sql.Tx, eventID string, visiting map[string]bool) ([]replycontext.Record, error) {
	key := a.snapshot.RunID + "/" + eventID
	if visiting[key] {
		return nil, fmt.Errorf("request publication lineage contains a cycle")
	}
	visiting[key] = true
	defer delete(visiting, key)
	cut, err := newRunForkReplayCut(a.snapshot)
	if err != nil {
		return nil, err
	}
	event, err := cut.event(eventID)
	if err != nil {
		return nil, err
	}
	if _, selected := event.SelectedForkLineage(); selected {
		return a.inheritedRequestPublication(ctx, tx, event, visiting)
	}
	var records []replycontext.Record
	for _, captured := range a.snapshot.ReplyContexts {
		if captured.Record.RequestEventID == event.ID() {
			records = append(records, captured.Record)
		}
	}
	return records, nil
}

// The exact retained publication lineage remains available when a settled
// provider is omitted. An ordinary observer never acquires that provider's ref.
func (a runForkSourceStateAdmission) inheritedRequestPublication(ctx context.Context, tx *sql.Tx, event events.Event, visiting map[string]bool) ([]replycontext.Record, error) {
	if tx == nil {
		return nil, fmt.Errorf("inherited request requires its native retained binding")
	}
	lineage, _ := event.SelectedForkLineage()
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, a.snapshot.RunID)
	if err != nil {
		return nil, err
	}
	if binding.SourceRunID != lineage.SourceRunID() {
		return nil, fmt.Errorf("inherited request has foreign source binding")
	}
	if binding.ForkPoint.Kind == runfork.RunForkPointRunStart {
		return nil, nil
	}
	parent, err := loadRunForkPointSnapshot(ctx, tx, binding.SourceRunID, binding.ForkPoint)
	if err != nil {
		return nil, err
	}
	parentAdmission := runForkSourceStateAdmission{snapshot: parent, point: binding.ForkPoint, forkRunID: binding.ForkRunID, postgres: a.postgres}
	records, err := parentAdmission.capturedRequestPublication(ctx, tx, lineage.SourceEventID(), visiting)
	if err != nil || len(records) == 0 {
		return nil, err
	}
	cut, err := newRunForkReplayCut(parent)
	if err != nil {
		return nil, err
	}
	previous, err := cut.event(lineage.SourceEventID())
	if err != nil {
		return nil, err
	}
	if err := requireSelectedPublicationCutLineage(ctx, tx, binding, event, previous, nil); err != nil {
		return nil, err
	}
	projected, err := projectRunForkCapturedReplies(parent, binding.ForkPoint, binding.ForkRunID, records)
	if err != nil {
		return nil, err
	}
	return capturedProjectedRequestRows(a.snapshot, event, projected)
}

func capturedProjectedRequestRows(snapshot *runForkRevisionSnapshot, event events.Event, expected []replycontext.Record) ([]replycontext.Record, error) {
	var rows []replycontext.Record
	for _, record := range expected {
		var matched *replycontext.Record
		for _, captured := range snapshot.ReplyContexts {
			actual := captured.Record
			if actual.ID != record.ID {
				continue
			}
			if matched != nil || !capturedReplyRequestEventMatches(event, actual) {
				return nil, fmt.Errorf("inherited request has contradictory own context identity")
			}
			if err := actual.ValidateContinuationFrom(record, record.CreatedAt); err != nil {
				return nil, err
			}
			matched = &actual
		}
		if matched == nil {
			return nil, fmt.Errorf("inherited request lacks its captured own context")
		}
		rows = append(rows, *matched)
	}
	return rows, nil
}
