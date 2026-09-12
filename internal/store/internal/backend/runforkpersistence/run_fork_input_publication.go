package runforkpersistence

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

func decodeRunForkRevisionEvent(fact runForkRevisionEvent) (events.AdmittedEvent, error) {
	// The revision JSON represents an absent SQL value as JSON null; Record
	// uses nil for that same absence and still rejects any non-null foreign arm.
	if bytes.Equal(fact.InheritedFanOutOrigin, []byte("null")) {
		fact.InheritedFanOutOrigin = nil
	}
	source, err := json.Marshal(fact.RoutingSource.Route())
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	return (eventrecord.Record{
		Class: events.EventAdmissionClass(fact.EventClass), EventID: fact.EventID, RunID: fact.RunID, EventName: fact.EventName,
		TaskID: fact.TaskID, EntityID: fact.EntityID, FlowInstance: fact.FlowInstance, Scope: events.EventScope(fact.Scope),
		Payload: fact.Payload, ExecutionMode: executionmode.Mode(fact.ExecutionMode), ChainDepth: fact.ChainDepth,
		ProducedBy: fact.ProducedBy, ProducedByType: events.EventProducerType(fact.ProducedByType), SourceEventID: fact.SourceEventID,
		OperatorReferencedEventID: fact.OperatorReferenceEventID,
		CreatedAt:                 fact.CreatedAt, RoutingSourceKind: fact.RoutingSource.Kind().StorageCode(), RoutingSourceAuthority: fact.RoutingSource.Authority().StorageCode(),
		SourceRoute: source, TargetRoute: fact.TargetRoute, TargetSet: fact.TargetSet, RouteSettlement: fact.RouteSettlement,
		InheritedFanOutOrigin:   fact.InheritedFanOutOrigin,
		PayloadSchemaBundleHash: fact.PayloadSchemaBundleHash, PayloadSchemaFlowID: fact.PayloadSchemaFlowID,
		PayloadSchemaEventKey: fact.PayloadSchemaEventKey, PayloadSchemaDigest: fact.PayloadSchemaDigest, PayloadSchemaClass: fact.PayloadSchemaClass,
	}).Decode()
}

func historicalInputPublications(snapshot *runForkRevisionSnapshot) ([]runfork.InputPublication, error) {
	var out []runfork.InputPublication
	for _, fact := range snapshot.Events {
		if fact.EventClass != string(events.EventAdmissionRootIngress) && fact.EventClass != string(events.EventAdmissionOperatorInjected) {
			continue
		}
		admitted, err := decodeRunForkRevisionEvent(fact)
		if err != nil {
			return nil, fmt.Errorf("decode historical input %s: %w", fact.EventID, err)
		}
		input, err := runfork.InputPublicationFromEvent(admitted.Event())
		if err != nil {
			return nil, err
		}
		if _, present := input.Event(); present {
			out = append(out, input)
		}
	}
	return out, nil
}
