package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func checkSemanticDriftPayloadCompleteness(c *checkerContext) []Finding {
	return c.payloadCompleteness()
}

func (c *checkerContext) payloadCompleteness() []Finding {
	if c.payloadCompletenessLoaded {
		return c.payloadCompletenessFindings
	}
	c.payloadCompletenessLoaded = true

	entityFields := entitySchemaFields(c.source)
	forced := payloadCompletenessForcedFields()

	for nodeID := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		nodeSource, _ := c.source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(nodeSource.FlowID)

		for triggerEventType, handler := range c.source.NodeEventHandlers(nodeID) {
			triggerEventType = strings.TrimSpace(triggerEventType)
			triggerProof := semanticview.ResolveFlowEventProof(c.source, flowID, triggerEventType)
			emitFieldsByEvent := payloadCompletenessEmitFields(handler)

			for _, emitted := range uniquePayloadCompletenessStrings(handlerEmits(handler)...) {
				emitted = strings.TrimSpace(emitted)
				if emitted == "" {
					continue
				}
				emittedProof := semanticview.ResolveFlowEventProof(c.source, flowID, emitted)
				if !emittedProof.HasSchema {
					continue
				}
				required := payloadCompletenessRequiredFields(emittedProof.Entry)
				if len(required) == 0 {
					continue
				}

				for _, field := range required {
					if _, ok := forced[field]; ok {
						continue
					}
					if targets := emitFieldsByEvent[emitted]; len(targets) > 0 {
						if _, ok := targets[field]; ok {
							continue
						}
					}

					c.payloadCompletenessFindings = append(c.payloadCompletenessFindings, Finding{
						CheckID:  "semantic_drift_payload_completeness",
						Severity: "warning",
						Message: payloadCompletenessMessage(
							nodeID,
							triggerEventType,
							emittedProof.DisplayName(),
							field,
							emitFieldsByEvent[emitted],
							triggerProof.Entry,
							triggerProof.HasSchema,
							entityFields,
						),
						Location: nodeID,
					})
				}
			}
		}
	}

	sort.SliceStable(c.payloadCompletenessFindings, func(i, j int) bool {
		if c.payloadCompletenessFindings[i].Location == c.payloadCompletenessFindings[j].Location {
			return c.payloadCompletenessFindings[i].Message < c.payloadCompletenessFindings[j].Message
		}
		return c.payloadCompletenessFindings[i].Location < c.payloadCompletenessFindings[j].Location
	})
	return c.payloadCompletenessFindings
}

func payloadCompletenessRequiredFields(entry runtimecontracts.EventCatalogEntry) []string {
	return uniquePayloadCompletenessStrings(entry.Required...)
}

func payloadCompletenessForcedFields() map[string]struct{} {
	return map[string]struct{}{
		"entity_id":          {},
		"trigger_event_type": {},
		"current_state":      {},
	}
}

func payloadCompletenessEmitFields(handler runtimecontracts.SystemNodeEventHandler) map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}
	add := func(spec runtimecontracts.EmitSpec) {
		eventType := spec.EventType()
		if eventType == "" {
			return
		}
		targets := out[eventType]
		if targets == nil {
			targets = map[string]struct{}{}
		}
		for key := range spec.Fields {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			targets[key] = struct{}{}
		}
		out[eventType] = targets
	}
	add(handler.Emit)
	if handler.FanOut != nil {
		add(handler.FanOut.Emit)
	}
	for _, rule := range handler.Rules {
		add(rule.Emit)
		if rule.FanOut != nil {
			add(rule.FanOut.Emit)
		}
	}
	for _, rule := range handler.OnComplete {
		add(rule.Emit)
		if rule.FanOut != nil {
			add(rule.FanOut.Emit)
		}
	}
	if handler.Accumulate != nil {
		for _, rule := range handler.Accumulate.OnComplete {
			add(rule.Emit)
			if rule.FanOut != nil {
				add(rule.FanOut.Emit)
			}
		}
		if handler.Accumulate.OnTimeout != nil {
			add(handler.Accumulate.OnTimeout.Emit)
			if handler.Accumulate.OnTimeout.FanOut != nil {
				add(handler.Accumulate.OnTimeout.FanOut.Emit)
			}
		}
	}
	return out
}

func payloadCompletenessTriggerSchemaState(entry runtimecontracts.EventCatalogEntry, hasSchema bool, field string) string {
	if !hasSchema {
		return "no schema"
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return "no"
	}
	required := map[string]struct{}{}
	for _, item := range entry.Required {
		item = strings.TrimSpace(item)
		if item != "" {
			required[item] = struct{}{}
		}
	}
	if _, ok := required[field]; ok {
		return "yes (required)"
	}
	if _, ok := entry.Payload.Properties[field]; ok {
		return "yes (optional)"
	}
	return "no"
}

func payloadCompletenessEntitySchemaState(entityFields map[string]struct{}, field string) string {
	field = strings.TrimSpace(field)
	if field == "" {
		return "no"
	}
	if _, ok := entityFields[field]; ok {
		return "yes"
	}
	return "no"
}

func payloadCompletenessMessage(nodeID, triggerEventType, emittedEventType, field string, emitFieldTargets map[string]struct{}, triggerEntry runtimecontracts.EventCatalogEntry, hasTriggerSchema bool, entityFields map[string]struct{}) string {
	fieldState := "absent"
	fieldCovered := "N/A (no emit.fields)"
	if len(emitFieldTargets) > 0 {
		fieldState = "present"
		fieldCovered = strings.Join(sortedPayloadCompletenessKeys(emitFieldTargets), ", ")
		if fieldCovered == "" {
			fieldCovered = "(none)"
		}
	}
	return fmt.Sprintf(
		"event %s emitted by node %s handler %s: required field %s is not statically provable in the emitted payload. Payload construction: emit.fields: %s; emit.fields covers: %s; platform-forced: entity_id, trigger_event_type, current_state. Context (does not clear finding): trigger schema declares %s: %s; entity schema declares %s: %s.",
		strings.TrimSpace(emittedEventType),
		strings.TrimSpace(nodeID),
		strings.TrimSpace(triggerEventType),
		strings.TrimSpace(field),
		fieldState,
		fieldCovered,
		strings.TrimSpace(field),
		payloadCompletenessTriggerSchemaState(triggerEntry, hasTriggerSchema, field),
		strings.TrimSpace(field),
		payloadCompletenessEntitySchemaState(entityFields, field),
	)
}

func sortedPayloadCompletenessKeys(items map[string]struct{}) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}

func uniquePayloadCompletenessStrings(values ...string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
