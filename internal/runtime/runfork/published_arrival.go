package runfork

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// PublishedArrival retains an accepted source occurrence, not a child arm or
// execution grant. Only the fixed-cut reader supplies historical membership.
type PublishedArrival struct {
	activation  genericschedule.Activation
	transferred *genericschedule.TransferredJoinOccurrence
	event       events.Event
}

func NewPublishedArrival(activation genericschedule.Activation, event events.Event) (PublishedArrival, error) {
	if _, err := activation.ValidatePublishedOccurrence(event); err != nil {
		return PublishedArrival{}, err
	}
	if _, present := event.PayloadAdmission(); !present {
		return PublishedArrival{}, fmt.Errorf("published arrival lacks its admitted payload schema")
	}
	return PublishedArrival{activation: activation.Canonical(), event: event.Clone()}, nil
}

func NewTransferredPublishedArrival(source genericschedule.TransferredJoinOccurrence, event events.Event) (PublishedArrival, error) {
	if err := source.ValidateEvent(event); err != nil {
		return PublishedArrival{}, err
	}
	if _, present := event.PayloadAdmission(); !present {
		return PublishedArrival{}, fmt.Errorf("transferred arrival lacks its admitted payload schema")
	}
	return PublishedArrival{transferred: &source, event: event.Clone()}, nil
}

func (p PublishedArrival) Command() genericschedule.AdmissionCommand {
	if p.transferred != nil {
		return p.transferred.Command
	}
	return p.activation.Command
}

func (p PublishedArrival) Continuation(child genericschedule.AdmissionCommand) (genericschedule.PublishedJoinContinuation, error) {
	if p.transferred != nil {
		return genericschedule.ProjectTransferredJoinContinuation(*p.transferred, p.event, child)
	}
	return genericschedule.ProjectPublishedJoinContinuation(p.activation, p.event, child)
}

func (p PublishedArrival) Event() events.Event { return p.event.Clone() }

func (p RunForkPlan) WithHistoricalArrivalPublications(revision int64, publications []PublishedArrival) (RunForkPlan, error) {
	ids, known := p.HistoricalEventIDs(revision)
	if !known {
		return RunForkPlan{}, fmt.Errorf("arrival publications require exact fixed-cut event membership")
	}
	members := make(map[string]bool, len(ids))
	for _, id := range ids {
		members[id] = true
	}
	byActivation := make(map[string]PublishedArrival, len(publications))
	p.historicalArrivals = make(map[string]PublishedArrival, len(publications))
	for _, publication := range publications {
		event := publication.event
		if event.RunID() != p.SourceRunID || !members[event.ID()] {
			return RunForkPlan{}, fmt.Errorf("arrival publication is outside fixed source history")
		}
		if publication.transferred == nil {
			if _, err := publication.activation.ValidatePublishedOccurrence(event); err != nil {
				return RunForkPlan{}, err
			}
		} else {
			if err := publication.transferred.ValidateEvent(event); err != nil {
				return RunForkPlan{}, err
			}
		}
		if _, duplicate := p.historicalArrivals[event.ID()]; duplicate {
			return RunForkPlan{}, fmt.Errorf("arrival history repeats a published event")
		}
		identity := publication.activation.ID
		if publication.transferred != nil {
			identity = publication.transferred.Publication.EventID
		}
		if _, duplicate := byActivation[identity]; duplicate {
			return RunForkPlan{}, fmt.Errorf("arrival history repeats a published activation")
		}
		byActivation[identity] = publication
		p.historicalArrivals[event.ID()] = publication
	}
	for _, source := range p.TransferredJoins {
		publication, found := byActivation[source.Publication.EventID]
		if !found {
			// A transferred obligation can be retained before its publication.
			if members[source.Publication.EventID] {
				return RunForkPlan{}, fmt.Errorf("arrival history omits a transferred publication")
			}
			continue
		}
		want, err := source.EvidenceDigest()
		if err != nil || publication.transferred == nil {
			return RunForkPlan{}, fmt.Errorf("arrival publication lacks its exact transferred owner")
		}
		got, err := publication.transferred.EvidenceDigest()
		if err != nil || got != want {
			return RunForkPlan{}, fmt.Errorf("arrival publication contradicts its transferred owner")
		}
		delete(byActivation, source.Publication.EventID)
	}
	for _, activation := range p.JoinSchedules {
		if activation.Status != genericschedule.StatusFired {
			continue
		}
		publication, found := byActivation[activation.ID]
		if !found {
			return RunForkPlan{}, fmt.Errorf("arrival history omits an accepted occurrence")
		}
		want, err := activation.EvidenceDigest()
		if err != nil {
			return RunForkPlan{}, err
		}
		got, err := publication.activation.EvidenceDigest()
		if err != nil || got != want {
			return RunForkPlan{}, fmt.Errorf("arrival publication contradicts its retained activation")
		}
		delete(byActivation, activation.ID)
	}
	if len(byActivation) != 0 {
		return RunForkPlan{}, fmt.Errorf("arrival history contains an unowned accepted occurrence")
	}
	return p, nil
}

func (p RunForkPlan) HistoricalArrivalPublication(eventID string) (PublishedArrival, bool) {
	publication, found := p.historicalArrivals[eventID]
	return publication, found
}

func (p RunForkPlan) HistoricalArrivalCoordinates() []InputPublicationCoordinates {
	ids := make([]string, 0, len(p.historicalArrivals))
	for id := range p.historicalArrivals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	coordinates := make([]InputPublicationCoordinates, 0, len(ids))
	for _, id := range ids {
		coordinates = append(coordinates, (InputPublication{event: p.historicalArrivals[id].event}).Coordinates())
	}
	return coordinates
}
