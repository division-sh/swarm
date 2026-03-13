package manager

import (
	"fmt"
)

func EntityScopedAgentID(role, entityID string) string {
	return fmt.Sprintf("%s-%s", role, entityID)
}

func OpCoAgentID(role, verticalID string) string {
	return EntityScopedAgentID(role, verticalID)
}

func EntityRouteRuleKey(entityID, eventPattern, subscriberID string) string {
	return entityID + "|" + eventPattern + "|" + subscriberID
}

func RouteRuleKey(verticalID, eventPattern, subscriberID string) string {
	return EntityRouteRuleKey(verticalID, eventPattern, subscriberID)
}
