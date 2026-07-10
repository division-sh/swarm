package bootverify

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkProducesDrift(c *checkerContext) []Finding   { return c.producesDrift() }
func checkPhantomProduces(c *checkerContext) []Finding { return c.phantomProduces() }

func (c *checkerContext) producesDrift() []Finding {
	if c.producesDriftLoaded {
		return c.producesDriftFindings
	}
	c.producesDriftLoaded = true
	census := semanticview.BuildAuthoredEventEndpointCensus(c.source)
	for _, assertion := range census.ProducerAssertions() {
		if !assertion.Declared {
			continue
		}
		declared := stringSet(assertion.EventTypes)
		for _, endpoint := range census.Producers() {
			if endpoint.Kind != semanticview.EventEndpointNodeHandler || endpoint.NodeID != assertion.NodeID {
				continue
			}
			emitted := strings.TrimSpace(endpoint.Event.Authored)
			if _, ok := declared[emitted]; ok {
				continue
			}
			c.producesDriftFindings = append(c.producesDriftFindings, Finding{
				CheckID:  "produces_drift",
				Severity: "warning",
				Message:  fmt.Sprintf("node %s handler %s emits %s outside produces list", assertion.NodeID, endpoint.HandlerEvent, emitted),
				Location: assertion.NodeID,
			})
		}
	}
	return c.producesDriftFindings
}

func (c *checkerContext) phantomProduces() []Finding {
	if c.phantomLoaded {
		return c.phantomFindings
	}
	c.phantomLoaded = true
	census := semanticview.BuildAuthoredEventEndpointCensus(c.source)
	for _, assertion := range census.ProducerAssertions() {
		if !assertion.Declared {
			continue
		}
		emitted := map[string]struct{}{}
		for _, endpoint := range census.Producers() {
			if endpoint.Kind == semanticview.EventEndpointNodeHandler && endpoint.NodeID == assertion.NodeID {
				emitted[strings.TrimSpace(endpoint.Event.Authored)] = struct{}{}
			}
		}
		for _, eventType := range assertion.EventTypes {
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
				Message:  fmt.Sprintf("node %s produces lists %s but no handler emits it", assertion.NodeID, eventType),
				Location: assertion.NodeID,
			})
		}
	}
	return c.phantomFindings
}
