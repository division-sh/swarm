package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
)

// SelectedRecipient projects an already resolved local subscriber. It does not
// derive a route or transfer a historical delivery's authority.
func (subscriber Subscriber) SelectedRecipient(eventType events.EventType) (forkrecipient.Evidence, error) {
	handler := subscriber.HandlerForEvent(eventType)
	localEvent := eventType
	if subscriber.Recipient.IsNode() {
		var present bool
		localEvent, present = handler.EventOverride()
		if !present {
			return forkrecipient.Evidence{}, fmt.Errorf("selected local recipient %s lacks its admitted handler event", subscriber.Recipient.ID())
		}
	}
	return forkrecipient.NewLocal(forkrecipient.Input{
		Recipient: subscriber.Recipient, Path: subscriber.Path, AgentPlan: subscriber.AgentPlan,
		HandlerNode: handler.Node(), HandlerEvent: localEvent, RouteSource: subscriber.RouteSourceCode(),
	})
}
