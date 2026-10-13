package runfork

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// PublishedJoinOccurrence retains an accepted source occurrence, not a child arm or
// execution grant. Only the fixed-cut reader supplies historical membership.
type PublishedJoinOccurrence struct {
	activation  genericschedule.Activation
	transferred *genericschedule.TransferredJoinOccurrence
	event       events.Event
}

func NewPublishedJoinOccurrence(activation genericschedule.Activation, event events.Event) (PublishedJoinOccurrence, error) {
	if _, err := activation.ValidatePublishedOccurrence(event); err != nil {
		return PublishedJoinOccurrence{}, err
	}
	if _, present := event.PayloadAdmission(); !present {
		return PublishedJoinOccurrence{}, fmt.Errorf("published join lacks its admitted payload schema")
	}
	return PublishedJoinOccurrence{activation: activation.Canonical(), event: event.Clone()}, nil
}

func NewTransferredPublishedJoinOccurrence(source genericschedule.TransferredJoinOccurrence, event events.Event) (PublishedJoinOccurrence, error) {
	if err := source.ValidateEvent(event); err != nil {
		return PublishedJoinOccurrence{}, err
	}
	if _, present := event.PayloadAdmission(); !present {
		return PublishedJoinOccurrence{}, fmt.Errorf("transferred join lacks its admitted payload schema")
	}
	return PublishedJoinOccurrence{transferred: &source, event: event.Clone()}, nil
}

func (p PublishedJoinOccurrence) Command() genericschedule.AdmissionCommand {
	if p.transferred != nil {
		return p.transferred.Command
	}
	return p.activation.Command
}

func (p PublishedJoinOccurrence) Continuation(child genericschedule.AdmissionCommand) (genericschedule.JoinContinuation, error) {
	if p.transferred != nil {
		return genericschedule.ProjectTransferredJoinContinuation(*p.transferred, p.event, child)
	}
	return genericschedule.ProjectPublishedJoinContinuation(p.activation, p.event, child)
}

func (p PublishedJoinOccurrence) Event() events.Event { return p.event.Clone() }

func (p RunForkPlan) WithHistoricalJoinPublications(revision int64, publications []PublishedJoinOccurrence) (RunForkPlan, error) {
	ids, known := p.HistoricalEventIDs(revision)
	if !known {
		return RunForkPlan{}, fmt.Errorf("join publications require exact fixed-cut event membership")
	}
	members := make(map[string]bool, len(ids))
	for _, id := range ids {
		members[id] = true
	}
	byActivation := make(map[string]PublishedJoinOccurrence, len(publications))
	p.historicalJoins = make(map[string]PublishedJoinOccurrence, len(publications))
	for _, publication := range publications {
		event := publication.event
		if event.RunID() != p.SourceRunID || !members[event.ID()] {
			return RunForkPlan{}, fmt.Errorf("join publication is outside fixed source history")
		}
		identity, err := publication.validatedIdentity()
		if err != nil {
			return RunForkPlan{}, err
		}
		if _, duplicate := p.historicalJoins[event.ID()]; duplicate {
			return RunForkPlan{}, fmt.Errorf("join history repeats a published event")
		}
		if _, duplicate := byActivation[identity]; duplicate {
			return RunForkPlan{}, fmt.Errorf("join history repeats a published activation")
		}
		byActivation[identity] = publication
		p.historicalJoins[event.ID()] = publication
	}
	for _, source := range p.TransferredJoins {
		if err := requireTransferredJoinPublication(p.SourceRunID, source, byActivation, members); err != nil {
			return RunForkPlan{}, err
		}
		delete(byActivation, source.Publication.EventID)
	}
	for _, activation := range p.JoinSchedules {
		if activation.Status != genericschedule.StatusFired {
			continue
		}
		publication, found := byActivation[activation.ID]
		if !found {
			return RunForkPlan{}, fmt.Errorf("join history omits an accepted occurrence")
		}
		want, err := activation.EvidenceDigest()
		if err != nil {
			return RunForkPlan{}, err
		}
		got, err := publication.activation.EvidenceDigest()
		if err != nil || got != want {
			return RunForkPlan{}, fmt.Errorf("join publication contradicts its retained activation")
		}
		delete(byActivation, activation.ID)
	}
	if err := requireHistoricalFanOutPublications(p.SourceRunID, p.FanOutObligations, byActivation, members); err != nil {
		return RunForkPlan{}, err
	}
	if len(byActivation) != 0 {
		return RunForkPlan{}, fmt.Errorf("join history contains an unowned accepted occurrence")
	}
	return p, nil
}

func requireHistoricalFanOutPublications(sourceRunID string, obligations []RunForkFanOutObligation, byActivation map[string]PublishedJoinOccurrence, members map[string]bool) error {
	for _, obligation := range obligations {
		barrier := obligation.Barrier
		if barrier == nil || barrier.Status != fanoutbarrier.StatusClosedPending {
			continue
		}
		if barrier.TransferredOccurrence != nil {
			source, err := genericschedule.NewTransferredFanOutBarrierOccurrence(*barrier)
			if err != nil {
				return err
			}
			if err := requireTransferredJoinPublication(sourceRunID, source, byActivation, members); err != nil {
				return err
			}
			delete(byActivation, source.Publication.EventID)
			continue
		}
		publication, present := byActivation[barrier.ScheduleActivationID]
		if !present {
			continue
		}
		if err := genericschedule.ValidateFanOutBarrierScheduleRelation(*barrier, publication.activation); err != nil {
			return err
		}
		delete(byActivation, barrier.ScheduleActivationID)
	}
	return nil
}

func (p PublishedJoinOccurrence) validatedIdentity() (string, error) {
	if p.transferred != nil {
		return p.transferred.Publication.EventID, p.transferred.ValidateEvent(p.event)
	}
	_, err := p.activation.ValidatePublishedOccurrence(p.event)
	return p.activation.ID, err
}

func requireTransferredJoinPublication(sourceRunID string, source genericschedule.TransferredJoinOccurrence, publications map[string]PublishedJoinOccurrence, members map[string]bool) error {
	if source.Command.RunID != sourceRunID {
		return fmt.Errorf("transferred join belongs to another source run")
	}
	publication, found := publications[source.Publication.EventID]
	if !found {
		if members[source.Publication.EventID] {
			return fmt.Errorf("join history omits a transferred publication")
		}
		return source.Validate()
	}
	want, err := source.EvidenceDigest()
	if err != nil || publication.transferred == nil {
		return fmt.Errorf("join publication lacks its exact transferred owner")
	}
	got, err := publication.transferred.EvidenceDigest()
	if err != nil || got != want {
		return fmt.Errorf("join publication contradicts its transferred owner")
	}
	return nil
}

func (p RunForkPlan) HistoricalJoinPublication(eventID string) (PublishedJoinOccurrence, bool) {
	publication, found := p.historicalJoins[eventID]
	return publication, found
}

func (p RunForkPlan) HistoricalJoinCoordinates() []InputPublicationCoordinates {
	ids := make([]string, 0, len(p.historicalJoins))
	for id := range p.historicalJoins {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	coordinates := make([]InputPublicationCoordinates, 0, len(ids))
	for _, id := range ids {
		coordinates = append(coordinates, (InputPublication{event: p.historicalJoins[id].event}).Coordinates())
	}
	return coordinates
}
