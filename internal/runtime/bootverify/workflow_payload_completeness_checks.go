package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkSemanticDriftPayloadCompleteness(c *checkerContext) []Finding {
	return c.payloadCompleteness()
}

func (c *checkerContext) payloadCompleteness() []Finding {
	if c.payloadCompletenessLoaded {
		return c.payloadCompletenessFindings
	}
	c.payloadCompletenessLoaded = true

	for nodeID := range c.source.NodeEntries() {
		nodeID = strings.TrimSpace(nodeID)
		nodeSource, _ := c.source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(nodeSource.FlowID)
		entityFields := map[string]struct{}{}
		if contract := wave1EntityContractForFlow(c.source, flowID); contract.Defined {
			for field := range contract.Contract.Fields {
				field = strings.TrimSpace(field)
				if field != "" {
					entityFields[field] = struct{}{}
				}
			}
		}

		for triggerEventType, handler := range c.source.NodeEventHandlers(nodeID) {
			triggerEventType = strings.TrimSpace(triggerEventType)
			triggerProof := semanticview.ResolveFlowEventProof(c.source, flowID, triggerEventType)
			for _, emitSite := range payloadCompletenessEmitSites(c.source, nodeID, flowID, triggerEventType, handler) {
				if emitSite.Err != nil {
					c.payloadCompletenessFindings = append(c.payloadCompletenessFindings, Finding{
						CheckID:  "semantic_drift_payload_completeness",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s at %s %v", nodeID, triggerEventType, emitSite.Label, emitSite.Err),
						Location: nodeID,
					})
					continue
				}
				emitted := strings.TrimSpace(emitSite.EventType)
				if emitted == "" {
					continue
				}
				emittedProof := semanticview.ResolveFlowEventProof(c.source, flowID, emitted)
				emittedDisplayName := strings.TrimSpace(emittedProof.DisplayName())
				if emittedDisplayName == "" {
					emittedDisplayName = emitted
				}

				for _, authoredEnvelopeField := range payloadCompletenessEnvelopeFields(emitSite.Fields) {
					c.payloadCompletenessFindings = append(c.payloadCompletenessFindings, Finding{
						CheckID:  "semantic_drift_payload_completeness",
						Severity: "error",
						Message:  fmt.Sprintf("event %s emitted by node %s handler %s at %s authors envelope-owned field %s in emit.fields; envelope fields are platform-managed and must be accessed via event.*", emittedDisplayName, nodeID, triggerEventType, emitSite.Label, authoredEnvelopeField),
						Location: nodeID,
					})
				}
				if !emittedProof.HasSchema {
					continue
				}
				declared := payloadCompletenessDeclaredFields(emittedProof.Entry)
				for _, field := range payloadCompletenessUndeclaredFields(emitSite.Fields, declared) {
					c.payloadCompletenessFindings = append(c.payloadCompletenessFindings, Finding{
						CheckID:  "semantic_drift_payload_completeness",
						Severity: "error",
						Message:  fmt.Sprintf("event %s emitted by node %s handler %s at %s authors undeclared payload field %s in emit.fields", emittedDisplayName, nodeID, triggerEventType, emitSite.Label, field),
						Location: nodeID,
					})
				}
				required := payloadCompletenessRequiredFields(emittedProof.Entry)
				if len(required) == 0 {
					continue
				}

				for _, field := range required {
					if _, ok := emitSite.Fields[field]; ok {
						continue
					}

					c.payloadCompletenessFindings = append(c.payloadCompletenessFindings, Finding{
						CheckID:  "semantic_drift_payload_completeness",
						Severity: "error",
						Message: payloadCompletenessMessage(
							nodeID,
							triggerEventType,
							emittedDisplayName,
							field,
							emitSite.Label,
							emitSite.Fields,
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

type payloadCompletenessEmitSite struct {
	EventType string
	Label     string
	Fields    map[string]struct{}
	Err       error
}

func payloadCompletenessRequiredFields(entry runtimecontracts.EventCatalogEntry) []string {
	required := append([]string{}, entry.Required...)
	required = append(required, entry.Payload.Required...)
	return uniquePayloadCompletenessStrings(required...)
}

func payloadCompletenessEmitSites(source semanticview.Source, nodeID, flowID, triggerEventType string, handler runtimecontracts.SystemNodeEventHandler) []payloadCompletenessEmitSite {
	var out []payloadCompletenessEmitSite
	add := func(label string, spec runtimecontracts.EmitSpec) {
		if bundle, ok := semanticview.Bundle(source); ok && bundle != nil {
			lowered, err := bundle.LowerEmitSpecFields(runtimecontracts.EmitFieldLoweringContext{
				NodeID:           nodeID,
				FlowID:           flowID,
				TriggerEventType: triggerEventType,
				Site:             label,
			}, spec)
			if err != nil {
				out = append(out, payloadCompletenessEmitSite{
					Label: strings.TrimSpace(label),
					Err:   err,
				})
				return
			}
			spec = lowered
		}
		eventType := spec.EventType()
		if eventType == "" {
			return
		}
		targets := map[string]struct{}{}
		for key := range spec.Fields {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			targets[key] = struct{}{}
		}
		out = append(out, payloadCompletenessEmitSite{
			EventType: eventType,
			Label:     strings.TrimSpace(label),
			Fields:    targets,
		})
	}
	for _, site := range runtimecontracts.HandlerDeclarativeEmitSites(handler) {
		add(payloadCompletenessDeclarativeSiteLabel(site), site.Spec)
	}
	if handler.Guard != nil {
		if failureSpec, err := handler.Guard.FailureSpec(); err == nil {
			if parsed, err := runtimeengine.GuardFailureFromSpec(failureSpec); err == nil && parsed.Action == runtimeengine.GuardFailureEscalate {
				add("guard.on_fail.escalate", failureSpec.EscalationEmitSpec())
			}
		}
	}
	return out
}

func payloadCompletenessDeclaredFields(entry runtimecontracts.EventCatalogEntry) map[string]struct{} {
	out := map[string]struct{}{}
	for field := range entry.Payload.Properties {
		field = strings.TrimSpace(field)
		if field != "" {
			out[field] = struct{}{}
		}
	}
	for _, field := range append(entry.Required, entry.Payload.Required...) {
		field = strings.TrimSpace(field)
		if field != "" {
			out[field] = struct{}{}
		}
	}
	return out
}

func payloadCompletenessUndeclaredFields(fields, declared map[string]struct{}) []string {
	if len(fields) == 0 {
		return nil
	}
	out := make([]string, 0)
	for field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if _, ok := declared[field]; !ok {
			out = append(out, field)
		}
	}
	sort.Strings(out)
	return out
}

func payloadCompletenessEnvelopeFields(fields map[string]struct{}) []string {
	if len(fields) == 0 {
		return nil
	}
	out := make([]string, 0, len(fields))
	for field := range fields {
		field = strings.TrimSpace(field)
		if field == "" || !isRuntimeOwnedCanonicalContextField(field) {
			continue
		}
		out = append(out, field)
	}
	sort.Strings(out)
	return out
}

func isRuntimeOwnedCanonicalContextField(field string) bool {
	switch strings.TrimSpace(field) {
	case "entity_id", "flow_instance", "trigger_event_type", "current_state", "task_id", "timer_handle", "source_event_id", "emitted_at":
		return true
	default:
		return false
	}
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
	for _, item := range append(entry.Required, entry.Payload.Required...) {
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

func payloadCompletenessDeclarativeSiteLabel(site runtimecontracts.HandlerDeclarativeEmitSite) string {
	switch strings.TrimSpace(site.Source) {
	case "handler.rules.emit":
		return payloadCompletenessRuleLabel("rules", site.RuleIndex, site.RuleID, "emit")
	case "handler.rules.fan_out.emit":
		return payloadCompletenessRuleLabel("rules", site.RuleIndex, site.RuleID, "fan_out.emit")
	case "handler.rules.emit_template":
		return payloadCompletenessRuleLabel("rules", site.RuleIndex, site.RuleID, "emit_template")
	case "handler.on_complete.emit":
		return payloadCompletenessRuleLabel("on_complete", site.RuleIndex, site.RuleID, "emit")
	case "handler.on_complete.fan_out.emit":
		return payloadCompletenessRuleLabel("on_complete", site.RuleIndex, site.RuleID, "fan_out.emit")
	case "handler.accumulate.on_complete.emit":
		return payloadCompletenessRuleLabel("accumulate.on_complete", site.RuleIndex, site.RuleID, "emit")
	case "handler.accumulate.on_complete.fan_out.emit":
		return payloadCompletenessRuleLabel("accumulate.on_complete", site.RuleIndex, site.RuleID, "fan_out.emit")
	case "handler.accumulate.on_timeout.emit":
		return payloadCompletenessRuleLabel("accumulate.on_timeout", site.RuleIndex, site.RuleID, "emit")
	case "handler.accumulate.on_timeout.fan_out.emit":
		return payloadCompletenessRuleLabel("accumulate.on_timeout", site.RuleIndex, site.RuleID, "fan_out.emit")
	case "handler.branch.then.emit":
		return payloadCompletenessRuleLabel("branch", site.RuleIndex, site.RuleID, "then.emit")
	case "handler.branch.else.emit":
		return payloadCompletenessRuleLabel("branch", site.RuleIndex, site.RuleID, "else.emit")
	default:
		if label := strings.TrimSpace(site.SiteKey); label != "" {
			return label
		}
		return strings.TrimSpace(site.Source)
	}
}

func payloadCompletenessRuleLabel(scope string, index int, id, suffix string) string {
	scope = strings.TrimSpace(scope)
	id = strings.TrimSpace(id)
	suffix = strings.TrimSpace(suffix)
	if id != "" {
		return fmt.Sprintf("%s[%s].%s", scope, id, suffix)
	}
	return fmt.Sprintf("%s[%d].%s", scope, index, suffix)
}

func payloadCompletenessMessage(nodeID, triggerEventType, emittedEventType, field, emitSiteLabel string, emitFieldTargets map[string]struct{}, triggerEntry runtimecontracts.EventCatalogEntry, hasTriggerSchema bool, entityFields map[string]struct{}) string {
	fieldState := "absent"
	fieldCovered := "N/A (no emit.fields)"
	emitSiteLabel = strings.TrimSpace(emitSiteLabel)
	if emitSiteLabel == "" {
		emitSiteLabel = "emit"
	}
	if len(emitFieldTargets) > 0 {
		fieldState = "present"
		fieldCovered = strings.Join(sortedPayloadCompletenessKeys(emitFieldTargets), ", ")
		if fieldCovered == "" {
			fieldCovered = "(none)"
		}
	}
	return fmt.Sprintf(
		"event %s emitted by node %s handler %s at %s: required field %s is not statically provable in the emitted payload. Payload construction: emit.fields: %s; emit.fields covers: %s. Context (does not clear finding): trigger schema declares %s: %s; entity schema declares %s: %s.",
		strings.TrimSpace(emittedEventType),
		strings.TrimSpace(nodeID),
		strings.TrimSpace(triggerEventType),
		emitSiteLabel,
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
