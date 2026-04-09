package bootverify

import (
	"fmt"
	"sort"
	"strings"

	"swarm/internal/runtime/semanticview"
)

func checkSingleNodePerEvent(c *checkerContext) []Finding { return c.singleNodePerEvent() }

func (c *checkerContext) singleNodePerEvent() []Finding {
	if c.singleNodeLoaded {
		return c.singleNodeFindings
	}
	c.singleNodeLoaded = true
	eventNames := map[string]struct{}{}
	type subscriptionOwner struct {
		NodeID string
		FlowID string
		Event  string
	}
	subscriptions := make([]subscriptionOwner, 0)
	for eventType := range c.source.ResolvedEventCatalog() {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" && !strings.Contains(eventType, "*") {
			eventNames[eventType] = struct{}{}
		}
	}
	for nodeID := range c.source.NodeEntries() {
		sourceRef, _ := c.source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(sourceRef.FlowID)
		for _, eventType := range c.source.NodeHandlerSubscriptions(nodeID) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" || strings.Contains(eventType, "*") {
				continue
			}
			proof := semanticview.ResolveFlowEventProof(c.source, flowID, eventType)
			eventKey := proof.EventKey()
			if eventKey == "" {
				continue
			}
			subscriptions = append(subscriptions, subscriptionOwner{
				NodeID: strings.TrimSpace(nodeID),
				FlowID: flowID,
				Event:  eventKey,
			})
			eventNames[eventKey] = struct{}{}
		}
	}
	for _, eventType := range sortedSetKeysLocal(eventNames) {
		nodeIDs := make([]string, 0)
		for _, subscription := range subscriptions {
			if subscription.Event == eventType {
				nodeIDs = append(nodeIDs, subscription.NodeID)
			}
		}
		if len(nodeIDs) <= 1 {
			continue
		}
		sort.Strings(nodeIDs)
		c.singleNodeFindings = append(c.singleNodeFindings, Finding{
			CheckID:  "single_node_per_event",
			Severity: "error",
			Message:  fmt.Sprintf("event %s has multiple owning nodes: %s", eventType, strings.Join(nodeIDs, ", ")),
			Location: eventType,
		})
	}
	return c.singleNodeFindings
}
