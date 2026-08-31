package bus_test

import (
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

func testLocalSubscriptionEvents(subscriptions []string) map[string]struct{} {
	localEvents := make(map[string]struct{}, len(subscriptions))
	for _, subscription := range subscriptions {
		if local := eventidentity.LeafName(subscription); local != "" && !strings.Contains(local, "*") {
			localEvents[local] = struct{}{}
		}
	}
	return localEvents
}
