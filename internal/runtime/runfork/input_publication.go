package runfork

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
)

// InputPublication retains already-decoded publication facts, not the original
// request context or permission to execute a delivery.
type InputPublication struct {
	event events.Event
}

func InputPublicationFromEvent(event events.Event) (InputPublication, error) {
	switch event.AdmissionClass() {
	case events.EventAdmissionRootIngress, events.EventAdmissionOperatorInjected:
	default:
		return InputPublication{}, nil
	}
	payload, ok := event.PayloadAdmission()
	if !ok {
		return InputPublication{}, fmt.Errorf("input publication %s lacks admitted schema evidence", event.ID())
	}
	// An external event class does not replace an explicitly recorded producer
	// route or platform schema owner. Those retain their existing selected route
	// and activity admission; neither acquires public-input authority here.
	if payload.Binding().SchemaClass() == events.PayloadSchemaPlatform {
		return InputPublication{}, nil
	}
	switch event.RoutingSource().Kind() {
	case events.RoutingSourceAbsent, events.RoutingSourceExternalIngress:
	default:
		return InputPublication{}, nil
	}
	return InputPublication{event: event}, nil
}

func (p InputPublication) Event() (events.Event, bool) {
	return p.event, p.event.ID() != ""
}

// Coordinates includes fields deliberately omitted from Event's public JSON.
func (p InputPublication) Coordinates() InputPublicationCoordinates {
	a, ok := p.event.PayloadAdmission()
	if !ok {
		return InputPublicationCoordinates{}
	}
	b := a.Binding()
	var reference string
	if provenance, ok := p.event.OperatorReference(); ok {
		reference = provenance.ReferencedEventID()
	}
	return InputPublicationCoordinates{
		Event: p.event, RunID: p.event.RunID(), Class: p.event.AdmissionClass(), OperatorReferenceEventID: reference, Schema: events.PayloadSchemaBindingInput{
			BundleHash: b.BundleHash(), FlowID: b.FlowID(), EventKey: b.EventKey(), SchemaDigest: b.SchemaDigest(), SchemaClass: b.SchemaClass(),
		}, Envelope: p.event.NormalizedEnvelope(), RoutingSource: p.event.RoutingSource()}
}

type InputPublicationCoordinates struct {
	Event                    events.Event
	RunID                    string
	Class                    events.EventAdmissionClass
	Schema                   events.PayloadSchemaBindingInput
	Envelope                 events.EventEnvelope
	RoutingSource            events.RoutingSource
	OperatorReferenceEventID string
}

func (p RunForkPlan) WithHistoricalInputPublications(revision int64, inputs []InputPublication) (RunForkPlan, error) {
	ids, ok := p.HistoricalEventIDs(revision)
	if !ok {
		return RunForkPlan{}, fmt.Errorf("input publications require fixed historical event membership")
	}
	members := make(map[string]bool, len(ids))
	for _, id := range ids {
		members[id] = true
	}
	p.historicalInputs = make(map[string]InputPublication, len(inputs))
	for _, input := range inputs {
		event, ok := input.Event()
		if !ok || event.RunID() != p.SourceRunID || !members[event.ID()] {
			return RunForkPlan{}, fmt.Errorf("input publication is outside fixed source history")
		}
		if _, duplicate := p.historicalInputs[event.ID()]; duplicate {
			return RunForkPlan{}, fmt.Errorf("duplicate historical input publication %s", event.ID())
		}
		p.historicalInputs[event.ID()] = input
	}
	return p, nil
}

func (p RunForkPlan) HistoricalInputPublication(eventID string) (InputPublication, bool) {
	input, ok := p.historicalInputs[eventID]
	return input, ok
}

func (p RunForkPlan) HistoricalInputCoordinates() []InputPublicationCoordinates {
	ids := make([]string, 0, len(p.historicalInputs))
	for id := range p.historicalInputs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]InputPublicationCoordinates, 0, len(ids))
	for _, id := range ids {
		out = append(out, p.historicalInputs[id].Coordinates())
	}
	return out
}
