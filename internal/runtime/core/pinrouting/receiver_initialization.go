package pinrouting

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ReceiverInitializationConfig preserves the producer's typed key and values.
// Route-match strings are address evidence, not configuration values.
func (p ConnectRoutePlan) ReceiverInitializationConfig(source semanticview.Source, payload map[string]any, eventID string) (map[string]any, error) {
	if p.instanceKey == nil {
		return nil, fmt.Errorf("receiver initialization requires an instance key")
	}
	pin, ok := source.FlowInputEventPin(p.receiver.flowID.value, p.receiver.pin.value)
	if !ok || pin.Digest() != p.receiver.pinDigest {
		return nil, fmt.Errorf("receiver initialization requires the exact compiled input pin")
	}
	key := p.instanceKey
	var identity map[string]any
	if key.RequiresDeliveryProjection() {
		material, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(p, eventID)
		if !failure.Empty() {
			return nil, fmt.Errorf("receiver instance source: %s", failure.Code())
		}
		identity = material.CanonicalValues()
	} else {
		if key.source.kind != connectInstanceSourcePayload {
			return nil, fmt.Errorf("receiver initialization has no admitted payload key source")
		}
		field := strings.TrimPrefix(key.source.path.value, "payload.")
		value, present := payload[field]
		if !present || value == nil {
			return nil, fmt.Errorf("receiver instance source %s is missing", key.source.path.value)
		}
		identity = map[string]any{key.field.Path(): value}
	}
	return pin.Initialization().EvaluateWithIdentity(payload, identity)
}
