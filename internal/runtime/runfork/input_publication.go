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
	case events.RoutingSourceRoot, events.RoutingSourceExternalIngress:
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

// OriginalStartFirstTurn is a creation intention, not an event in the cut.
// It cannot add historical deliveries or authorize an arbitrary later ingress.
func (p RunForkPlan) OriginalStartFirstTurn() (InputPublication, bool, error) {
	if p.StartFirstTurn == nil {
		return InputPublication{}, false, nil
	}
	if p.ForkPoint.Kind != RunForkPointRunStart || p.ForkPoint.Validate() != nil {
		return InputPublication{}, false, fmt.Errorf("original first turn requires the exact run-start point")
	}
	event, present := p.StartFirstTurn.Event()
	if !present || event.RunID() != p.SourceRunID {
		return InputPublication{}, false, fmt.Errorf("original first turn contradicts source creation")
	}
	admitted, err := InputPublicationFromEvent(event)
	if err != nil {
		return InputPublication{}, false, err
	}
	if _, present := admitted.Event(); !present {
		return InputPublication{}, false, fmt.Errorf("original first turn is not admitted ingress")
	}
	if ids, known := p.HistoricalEventIDs(p.ForkPoint.Revision); known {
		for _, id := range ids {
			if id == event.ID() {
				return InputPublication{}, false, fmt.Errorf("original first turn must be outside the exclusive start cut")
			}
		}
	}
	return admitted, true, nil
}

// ExecutionInputPublication keeps the exclusive first-turn intention distinct
// from historical input evidence while sharing selected root admission.
func (p RunForkPlan) ExecutionInputPublication(eventID string) (InputPublication, bool) {
	if input, present := p.HistoricalInputPublication(eventID); present {
		return input, true
	}
	input, present, err := p.OriginalStartFirstTurn()
	if err != nil || !present {
		return InputPublication{}, false
	}
	event, _ := input.Event()
	return input, event.ID() == eventID
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
