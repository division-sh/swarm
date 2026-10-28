package semanticview

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// ResolveProducerEventSchema admits the two producer classes before schema
// lookup. A supported subtype reference is distinct from structural schema.
func ResolveProducerEventSchema(source Source, producer events.PayloadProducer, event events.Event) (EventSchemaResolution, error) {
	if err := producer.ValidateForEvent(event); err != nil {
		return EventSchemaResolution{}, err
	}
	if source == nil {
		return EventSchemaResolution{}, fmt.Errorf("event producer requires a semantic source")
	}
	if producer.IsAuthored() {
		flowID := producer.FlowID()
		if _, err := runtimeidentity.AdmitFlowIdentity(flowID); err != nil {
			return EventSchemaResolution{}, fmt.Errorf("event producer coordinate: %w", err)
		}
		if scope, found := source.FlowScopeByID(flowID); !found || scope.ID != flowID {
			return EventSchemaResolution{}, fmt.Errorf("event producer flow %q is not declared in the semantic source", flowID)
		}
		if event.AdmissionClass() == events.EventAdmissionRuntimeControl {
			if event.Producer().ID() == "runtime.generic_schedule" || event.Producer().ID() == "agent-turn-timeout" {
				if _, _, found := source.ResolveFlowEventCatalogEntry(flowID, string(event.Type())); !found {
					return EventSchemaResolution{}, fmt.Errorf("authored control event %q is not declared by flow %q", event.Type(), flowID)
				}
				return ResolveEventSchema(source, flowID, string(event.Type())), nil
			}
			declared := false
			for _, timer := range source.WorkflowTimers() {
				if timer.OwningFlowID() == flowID && source.ResolveFlowEventReference(flowID, timer.Event) == string(event.Type()) {
					declared = true
					break
				}
			}
			if !declared {
				return EventSchemaResolution{}, fmt.Errorf("workflow timer event %q is not declared by flow %q", event.Type(), flowID)
			}
		}
		return ResolveEventSchema(source, flowID, string(event.Type())), nil
	}
	entry, authority, err := runtimecontracts.ResolvePlatformEventAuthority(source.PlatformSpec(), event)
	if err != nil {
		return EventSchemaResolution{}, err
	}
	resolution := EventSchemaResolution{
		EventKey: string(event.Type()), Classification: runtimecontracts.CompiledEventSchemaPlatform, HasClassification: true,
	}
	if authority.Digest() != "" {
		resolution.Reference = authority
		return resolution, nil
	}
	registry := runtimecontracts.EventSchemaRegistryFromCatalog(map[string]runtimecontracts.EventCatalogEntry{resolution.EventKey: entry})
	schema, ok := registry[resolution.EventKey]
	if !ok {
		return EventSchemaResolution{}, fmt.Errorf("platform structural payload schema is unavailable")
	}
	resolution.Schema, resolution.HasSchema = schema, true
	resolution.UnresolvedTypes = UnsupportedJSONSchemaTypes(schema.Schema)
	bindStructuralEventSchema(&resolution)
	return resolution, nil
}
