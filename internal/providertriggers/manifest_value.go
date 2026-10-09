package providertriggers

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// Manifest is an admitted BODY value. Copies share private, immutable policy;
// callers receive only defensive projections, never executable authoring DTOs.
type Manifest struct {
	value *manifestValue
}

type manifestValue struct {
	definition manifestDefinition
	source     yamlsource.Value
	body       []byte
}

func (m Manifest) Validate() error {
	if m.value == nil {
		return fmt.Errorf("provider trigger BODY has not been admitted")
	}
	return nil
}

func (m Manifest) Provider() string {
	if m.value == nil {
		return ""
	}
	return m.value.definition.Provider
}

func (m Manifest) Transport() packs.ChannelTransport {
	if m.value == nil {
		return ""
	}
	return m.value.definition.Transport
}

func (m Manifest) RequiresSecret() bool {
	return m.value != nil && m.value.definition.Secret.Required
}

func (m Manifest) Metadata() map[string]string {
	if m.value == nil {
		return nil
	}
	return cloneTextMap(m.value.definition.Metadata)
}

func (m Manifest) SourceBytes() []byte {
	if m.value == nil {
		return nil
	}
	return append([]byte(nil), m.value.body...)
}

func (m Manifest) OutputManifest() []OutputManifest {
	if m.value == nil {
		return nil
	}
	return cloneOutputs(m.value.definition.outputs)
}

func (m Manifest) EventCatalogEntries() map[string]runtimecontracts.EventCatalogEntry {
	if m.value == nil {
		return nil
	}
	return m.value.definition.eventCatalogEntries()
}

func (m Manifest) Accept(req Request) (Delivery, error) {
	admitted, err := m.admitRequest(req)
	if err != nil {
		return Delivery{}, err
	}
	return m.projectAdmission(admitted)
}

// ProjectNormalizedPayload consumes the admitted pack's existing projection
// rules, but grants no request or provider-output authorization. Session callers
// must separately prove their original account admission before publication.
func (m Manifest) ProjectNormalizedPayload(body []byte) ([]DeliveryEvent, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if _, err := canonicaljson.Decode(body); err != nil {
		return nil, err
	}
	var payload any
	if err := canonicaljson.DecodePreservingNumberLexemes(body, &payload); err != nil {
		return nil, err
	}
	if err := m.value.definition.validatePayloadObject(payload); err != nil {
		return nil, err
	}
	return m.value.definition.normalizedDeliveryEvents(payload)
}

func (m Manifest) admitRequest(req Request) (manifestAdmission, error) {
	if err := m.Validate(); err != nil {
		return manifestAdmission{}, err
	}
	if m.Transport() != packs.ChannelTransportWebhook {
		return manifestAdmission{}, unauthorized("session trigger requires its retained account and activation authority, not a webhook request")
	}
	return m.value.definition.admitRequest(req)
}

func (m Manifest) projectAdmission(admitted manifestAdmission) (Delivery, error) {
	if err := m.Validate(); err != nil {
		return Delivery{}, err
	}
	return m.value.definition.projectAdmission(admitted)
}

func cloneOutputs(in []OutputManifest) []OutputManifest {
	out := make([]OutputManifest, len(in))
	for i, item := range in {
		out[i] = item
		out[i].Fields = make(map[string]NormalizedEventFieldProjection, len(item.Fields))
		for name, field := range item.Fields {
			field.Values = cloneTextMap(field.Values)
			out[i].Fields[name] = field
		}
		out[i].When = cloneWhen(item.When)
	}
	return out
}

func cloneWhen(in NormalizedEventWhen) NormalizedEventWhen {
	out := NormalizedEventWhen{
		Exists: append([]string(nil), in.Exists...), Absent: append([]string(nil), in.Absent...),
		Equals: cloneTextMap(in.Equals),
	}
	if in.OneOf != nil {
		out.OneOf = make(map[string][]string, len(in.OneOf))
		for path, values := range in.OneOf {
			out.OneOf[path] = append([]string(nil), values...)
		}
	}
	return out
}
