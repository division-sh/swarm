package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// PlatformEventAuthority retains catalog reference law without projecting it as
// a structural schema. Values originate only in admitted platform source.
type PlatformEventAuthority struct {
	declared     bool
	producerType string
	producerID   string
	definedIn    string
	reference    string
	digest       string
	hasPayload   bool
}

func (a PlatformEventAuthority) Reference() string { return a.reference }
func (a PlatformEventAuthority) Digest() string    { return a.digest }

func admitPlatformEventAuthority(root yamlsource.Value, fields map[string]yamlsource.Value, eventKey string) (PlatformEventAuthority, error) {
	authority := PlatformEventAuthority{declared: true}
	_, authority.hasPayload = fields["payload"]
	if err := schemaValueTexts(fields, map[string]*string{
		"defined_in": &authority.definedIn, "schema_authority": &authority.reference,
	}, false); err != nil {
		return authority, err
	}
	if evidence, ok := fields["producer_evidence"]; ok {
		members, err := platformValueFields(evidence)
		if err != nil {
			return authority, err
		}
		if err := schemaValueTexts(members, map[string]*string{"produced_by_type": &authority.producerType, "produced_by": &authority.producerID}, false); err != nil {
			return authority, err
		}
	}
	if eventKey != string(events.EventTypePlatformRuntimeLog) || authority.reference != events.RuntimeLogSchemaReference || authority.definedIn != "platform_tables.diagnostics_encoding" {
		return authority, nil
	}
	// This is the one supported subtype reference, not a general reference loader.
	tables, err := root.Lookup("platform_tables")
	if err != nil {
		return authority, err
	}
	encoding, err := tables.Value.Lookup("diagnostics_encoding")
	if err != nil {
		return authority, err
	}
	law, err := encoding.Value.Lookup("runtime_log_encoding")
	if err != nil {
		return authority, err
	}
	if law.Value.Presence() != yamlsource.PresenceMapping {
		return authority, fmt.Errorf("runtime-log referenced law is missing or not a mapping")
	}
	members, err := platformValueFields(law.Value)
	if err != nil {
		return authority, err
	}
	var declaredEvent, declaredProducer string
	if err := schemaValueRequiredTexts(law.Value, members, map[string]*string{"event_name": &declaredEvent, "produced_by_type": &declaredProducer}); err != nil {
		return authority, err
	}
	if declaredEvent != eventKey || declaredProducer != authority.producerType {
		return authority, fmt.Errorf("runtime-log referenced law disagrees with catalog event/producer")
	}
	var definition map[string]any
	if err := law.Value.Project(&definition); err != nil {
		return authority, err
	}
	canonical, err := canonicaljson.Bytes(map[string]any{
		"reference": authority.reference, "event": eventKey, "producer_type": authority.producerType, "producer_id": authority.producerID, "law": definition,
	})
	if err != nil {
		return authority, err
	}
	authority.digest = canonicaljson.HashBytes(canonical)
	return authority, nil
}

// ResolvePlatformEventAuthority is shared by declaration, compiled and effective
// source projections. A platform label or an event name alone is not authority.
func ResolvePlatformEventAuthority(platform PlatformSpecDocument, event events.Event) (EventCatalogEntry, PlatformEventAuthority, error) {
	entry, key, ok := PlatformEventCatalogEntry(platform, string(event.Type()))
	if !ok || key != string(event.Type()) || entry.platformAuthority.producerType != string(event.Producer().Type()) || event.Producer().Type() != events.EventProducerPlatform {
		return EventCatalogEntry{}, PlatformEventAuthority{}, fmt.Errorf("event has no matching platform catalog producer authority")
	}
	if err := events.PlatformPayloadProducer(event).ValidateForEvent(event); err != nil {
		return EventCatalogEntry{}, PlatformEventAuthority{}, err
	}
	authority := entry.platformAuthority
	if authority.producerID != "" && authority.producerID != event.Producer().ID() {
		return EventCatalogEntry{}, PlatformEventAuthority{}, fmt.Errorf("platform catalog producer identity disagrees with event")
	}
	if event.Type() == events.EventTypePlatformRuntimeLog {
		if authority.producerID != "runtime" || authority.reference != events.RuntimeLogSchemaReference || authority.definedIn != "platform_tables.diagnostics_encoding" || authority.digest == "" || authority.hasPayload {
			return EventCatalogEntry{}, PlatformEventAuthority{}, fmt.Errorf("runtime-log catalog reference authority is missing or contradictory")
		}
	} else if len(entry.Payload.Properties) == 0 {
		return EventCatalogEntry{}, PlatformEventAuthority{}, fmt.Errorf("platform event has no supported payload schema authority")
	}
	return entry, authority, nil
}
