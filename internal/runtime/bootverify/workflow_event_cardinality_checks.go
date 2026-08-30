package bootverify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkSingleNodePerEvent(c *checkerContext) []Finding { return c.singleNodePerEvent() }

func (c *checkerContext) singleNodePerEvent() []Finding {
	if c.singleNodeLoaded {
		return c.singleNodeFindings
	}
	c.singleNodeLoaded = true
	owners := map[string]map[string]struct{}{}
	labels := map[string]string{}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(c.source).Consumers() {
		if endpoint.Kind != semanticview.EventEndpointNodeHandler || endpoint.Pattern || !endpoint.Node.Valid() {
			continue
		}
		eventKey := endpoint.Node.FlowPath() + "\x00" + endpoint.Event.EventKey()
		labels[eventKey] = endpoint.Event.EventKey()
		if owners[eventKey] == nil {
			owners[eventKey] = map[string]struct{}{}
		}
		owners[eventKey][endpoint.Node.Key()] = struct{}{}
	}
	for _, eventType := range sortedSetKeysLocal(owners) {
		nodeIDs := make([]string, 0, len(owners[eventType]))
		for nodeID := range owners[eventType] {
			nodeIDs = append(nodeIDs, nodeID)
		}
		if len(nodeIDs) <= 1 {
			continue
		}
		sort.Strings(nodeIDs)
		c.singleNodeFindings = append(c.singleNodeFindings, Finding{
			CheckID:  "single_node_per_event",
			Severity: "error",
			Message:  fmt.Sprintf("event %s has multiple owning nodes: %s", labels[eventType], strings.Join(nodeIDs, ", ")),
			Location: labels[eventType],
		})
	}
	return c.singleNodeFindings
}
