package bus_test

import (
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func testLocalSubscriptionEvents(subscriptions []string) map[string]struct{} {
	localEvents := make(map[string]struct{}, len(subscriptions))
	for _, subscription := range subscriptions {
		if local := runtimeidentity.EventLeafName(subscription); local != "" && !strings.Contains(local, "*") {
			localEvents[local] = struct{}{}
		}
	}
	return localEvents
}
