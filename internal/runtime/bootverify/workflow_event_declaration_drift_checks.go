package bootverify

import (
	"fmt"
	"strings"
)

func checkProducesDrift(c *checkerContext) []Finding   { return c.producesDrift() }
func checkPhantomProduces(c *checkerContext) []Finding { return c.phantomProduces() }

func (c *checkerContext) producesDrift() []Finding {
	if c.producesDriftLoaded {
		return c.producesDriftFindings
	}
	c.producesDriftLoaded = true
	for nodeID, node := range c.source.NodeEntries() {
		produces := stringSet(node.Produces)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for _, emitted := range handlerEmits(handler) {
				emitted = strings.TrimSpace(emitted)
				if emitted == "" {
					continue
				}
				if _, ok := produces[emitted]; ok {
					continue
				}
				c.producesDriftFindings = append(c.producesDriftFindings, Finding{
					CheckID:  "produces_drift",
					Severity: "warning",
					Message:  fmt.Sprintf("node %s handler %s emits %s outside produces list", strings.TrimSpace(nodeID), eventType, emitted),
					Location: strings.TrimSpace(nodeID),
				})
			}
		}
	}
	return c.producesDriftFindings
}

func (c *checkerContext) phantomProduces() []Finding {
	if c.phantomLoaded {
		return c.phantomFindings
	}
	c.phantomLoaded = true
	for nodeID, node := range c.source.NodeEntries() {
		emitted := map[string]struct{}{}
		for _, handler := range node.EventHandlers {
			for _, eventType := range handlerEmits(handler) {
				eventType = strings.TrimSpace(eventType)
				if eventType != "" {
					emitted[eventType] = struct{}{}
				}
			}
		}
		for _, eventType := range node.Produces {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := emitted[eventType]; ok {
				continue
			}
			c.phantomFindings = append(c.phantomFindings, Finding{
				CheckID:  "phantom_produces",
				Severity: "warning",
				Message:  fmt.Sprintf("node %s produces lists %s but no handler emits it", strings.TrimSpace(nodeID), eventType),
				Location: strings.TrimSpace(nodeID),
			})
		}
	}
	return c.phantomFindings
}
