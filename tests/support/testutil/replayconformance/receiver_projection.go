package replayconformance

import (
	"encoding/json"

	"github.com/division-sh/swarm/internal/events"
)

// Generated event UUIDs differ across clean executions. Validate every exact
// construction receipt before replacing its event reference with the containing
// event's comparison-only causal key. Business and ownership facts stay intact.
func projectReceiverRoutes(event events.Event, routes []events.DeliveryRoute, eventKey string) ([]json.RawMessage, error) {
	if err := events.ValidateReceiverMaterializations(event, routes); err != nil {
		return nil, err
	}
	result := make([]json.RawMessage, len(routes))
	for i, route := range routes {
		if _, err := route.Identity(); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(route)
		if err != nil {
			return nil, err
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, err
		}
		if !route.Initialization.Empty() {
			var receipt map[string]json.RawMessage
			if err := json.Unmarshal(object["receiver_initialization"], &receipt); err != nil {
				return nil, err
			}
			receipt["event_id"], _ = json.Marshal(eventKey)
			object["receiver_initialization"], err = json.Marshal(receipt)
			if err != nil {
				return nil, err
			}
		}
		result[i], err = json.Marshal(object)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
