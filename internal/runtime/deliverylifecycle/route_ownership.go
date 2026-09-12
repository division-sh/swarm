package deliverylifecycle

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
)

// validateDeliveryRouteOwningRun checks normalized route ownership without
// granting execution authority or projecting historical identity into a new run.
func validateDeliveryRouteOwningRun(runID string, route events.DeliveryRoute) error {
	if route.Recipient.IsAgent() && route.AgentIdentity.RunID != runID {
		return fmt.Errorf("delivery route agent run does not match obligation run")
	}
	return nil
}
