package manager

import (
	"fmt"
)

func OpCoAgentID(role, verticalID string) string {
	return fmt.Sprintf("%s-%s", role, verticalID)
}

func RouteRuleKey(verticalID, eventPattern, subscriberID string) string {
	return verticalID + "|" + eventPattern + "|" + subscriberID
}
