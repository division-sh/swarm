package pinrouting

import (
	"fmt"
	"strings"
)

// ResolvedInstanceKey consumes the admitted connection's key source without
// converting typed payload values through address-match strings.
func (p ConnectRoutePlan) ResolvedInstanceKey(payload map[string]any, eventID string) (any, error) {
	if p.instanceKey == nil {
		return nil, fmt.Errorf("connection requires an instance key")
	}
	key := p.instanceKey
	if key.RequiresDeliveryProjection() {
		material, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(p, eventID)
		if !failure.Empty() {
			return nil, fmt.Errorf("receiver instance source: %s", failure.Code())
		}
		return material.CanonicalValues()[key.field.Path()], nil
	}
	if key.source.kind != connectInstanceSourcePayload {
		return nil, fmt.Errorf("receiver key has no admitted payload source")
	}
	field := strings.TrimPrefix(key.source.path.value, "payload.")
	value, present := payload[field]
	if !present || value == nil {
		return nil, fmt.Errorf("receiver instance source %s is missing", key.source.path.value)
	}
	return value, nil
}
