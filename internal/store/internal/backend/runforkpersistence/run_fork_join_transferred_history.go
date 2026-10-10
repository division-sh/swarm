package runforkpersistence

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func requireRunForkTransferredJoinHistory(snapshot *runForkRevisionSnapshot, join joinruntime.Activation, source genericschedule.TransferredJoinOccurrence, timers map[runForkJoinScheduleKey]runForkRevisionTimer) error {
	if snapshot == nil || source.Command.RunID != snapshot.RunID || snapshot.Revision <= 0 {
		return fmt.Errorf("transferred join requires its exact historical owner and cut")
	}
	scope, err := source.Command.ScopeKey()
	if err != nil {
		return err
	}
	if _, exists := timers[runForkJoinScheduleKey{scope, source.Command.ScheduleKey}]; exists {
		return fmt.Errorf("transferred publication cannot also own an executable schedule")
	}
	event, found, err := runForkTransferredJoinEvent(snapshot, source)
	if err != nil {
		return err
	}
	if !found {
		if join.OutcomeFired {
			return fmt.Errorf("settled transferred join lacks its retained publication")
		}
		return nil
	}
	_, err = requireRunForkPublishedJoinDelivery(snapshot, source.Command, event, false)
	return err
}

func runForkTransferredJoinEvent(snapshot *runForkRevisionSnapshot, source genericschedule.TransferredJoinOccurrence) (events.Event, bool, error) {
	var event events.Event
	found := false
	for _, row := range snapshot.Events {
		if row.EventID != source.Publication.EventID {
			continue
		}
		if found || row.RunID != snapshot.RunID || row.FirstRevision <= 0 || row.Revision < row.FirstRevision || row.Revision > snapshot.Revision {
			return events.Event{}, false, fmt.Errorf("transferred publication contradicts its exact historical cut")
		}
		admitted, err := decodeRunForkRevisionEvent(row)
		if err != nil {
			return events.Event{}, false, err
		}
		event, found = admitted.Event(), true
		if err := source.ValidateEvent(event); err != nil {
			return events.Event{}, false, err
		}
	}
	return event, found, nil
}

func runForkTransferredJoins(entities []runfork.RunForkEntityState) []genericschedule.TransferredJoinOccurrence {
	var sources []genericschedule.TransferredJoinOccurrence
	for _, entity := range entities {
		sources = append(sources, entity.TransferredJoins...)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Publication.EventID < sources[j].Publication.EventID })
	return sources
}

func transferredArrivalPublications(snapshot *runForkRevisionSnapshot, sources []genericschedule.TransferredJoinOccurrence) ([]runfork.PublishedArrival, error) {
	var publications []runfork.PublishedArrival
	for _, source := range sources {
		event, found, err := runForkTransferredJoinEvent(snapshot, source)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		publication, err := runfork.NewTransferredPublishedArrival(source, event)
		if err != nil {
			return nil, err
		}
		publications = append(publications, publication)
	}
	return publications, nil
}
