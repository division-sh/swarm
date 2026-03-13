package manager

import (
	"fmt"
)

func EntityScopedAgentID(role, entityID string) string {
	return fmt.Sprintf("%s-%s", role, entityID)
}

func EntityRouteRuleKey(entityID, eventPattern, subscriberID string) string {
	return entityID + "|" + eventPattern + "|" + subscriberID
}

func RouteRuleKey(entityID, eventPattern, subscriberID string) string {
	return EntityRouteRuleKey(entityID, eventPattern, subscriberID)
}
