package runforkpersistence

import (
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func historicalRouteSelectorPublications(snapshot *runForkRevisionSnapshot) ([]events.Event, error) {
	if snapshot == nil {
		_, err := newRunForkReplayCut(nil)
		return nil, err
	}
	// Only the captured event family supplies selectors. Diagnostic/internal
	// bodies and delivery rows retain their separate historical admission owners.
	selected := runForkRevisionSnapshot{RunID: snapshot.RunID, Revision: snapshot.Revision}
	for _, row := range snapshot.Events {
		if runfork.HistoricalRouteSelectorRequired(events.EventAdmissionClass(row.EventClass), row.PayloadSchemaClass, row.RoutingSource) {
			selected.Events = append(selected.Events, row)
		}
	}
	cut, err := newRunForkReplayCut(&selected)
	if err != nil {
		return nil, err
	}
	publications := make([]events.Event, 0, len(selected.Events))
	for _, row := range selected.Events {
		publication, err := cut.event(row.EventID)
		if err != nil {
			return nil, err
		}
		publications = append(publications, publication)
	}
	return publications, nil
}
