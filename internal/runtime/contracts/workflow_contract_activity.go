package contracts

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

const (
	ActivityResultStatusSucceeded = "succeeded"
	ActivityResultStatusFailed    = "failed"
)

type ActivitySite struct {
	FlowID          string
	NodeID          string
	HandlerEventKey string
	Source          string
	RuleID          string
	RuleIndex       int
	Spec            ActivitySpec
}

type ActivityResultEvents struct {
	ActivityID   string
	SuccessEvent string
	FailureEvent string
}

type ActivityRetryDefaults struct {
	MaxAttempts int
	Backoff     string
}

type ActivityForkPolicy string

const (
	ActivityForkReuseRecordedResult ActivityForkPolicy = "reuse_recorded_result"
	ActivityForkReexecuteRead       ActivityForkPolicy = "reexecute_read"
	ActivityForkRequireConfirmation ActivityForkPolicy = "require_manual_confirmation"
	ActivityForkForbidReexecution   ActivityForkPolicy = "forbid_reexecution"
)

func SupportedActivityEffectClass(effectClass ActivityEffectClass) bool {
	switch effectClass {
	case ActivityEffectClassReadOnly:
		return true
	default:
		return false
	}
}

func ActivityRetryDefaultsForEffectClass(effectClass ActivityEffectClass) ActivityRetryDefaults {
	switch effectClass {
	case ActivityEffectClassReadOnly:
		return ActivityRetryDefaults{MaxAttempts: 3, Backoff: "exponential"}
	case ActivityEffectClassIdempotentWrite:
		return ActivityRetryDefaults{MaxAttempts: 2, Backoff: "exponential"}
	default:
		return ActivityRetryDefaults{MaxAttempts: 1, Backoff: "none"}
	}
}

func ActivityForkPolicyForEffectClass(effectClass ActivityEffectClass) ActivityForkPolicy {
	switch effectClass {
	case ActivityEffectClassReadOnly:
		return ActivityForkReexecuteRead
	case ActivityEffectClassIdempotentWrite:
		return ActivityForkReuseRecordedResult
	case ActivityEffectClassNonIdempotentWrite:
		return ActivityForkRequireConfirmation
	default:
		return ActivityForkForbidReexecution
	}
}

func ActivitySitesForNode(flowID, nodeID string, handlers map[string]SystemNodeEventHandler) []ActivitySite {
	flowID = strings.TrimSpace(flowID)
	nodeID = strings.TrimSpace(nodeID)
	handlerKeys := make([]string, 0, len(handlers))
	for handlerEventKey := range handlers {
		if strings.TrimSpace(handlerEventKey) != "" {
			handlerKeys = append(handlerKeys, handlerEventKey)
		}
	}
	sort.Strings(handlerKeys)
	out := []ActivitySite{}
	for _, handlerEventKey := range handlerKeys {
		handler := handlers[handlerEventKey]
		if !handler.Activity.Empty() {
			out = append(out, ActivitySite{
				FlowID:          flowID,
				NodeID:          nodeID,
				HandlerEventKey: handlerEventKey,
				Source:          "handler.activity",
				RuleIndex:       -1,
				Spec:            handler.Activity,
			})
		}
		for idx, rule := range handler.Rules {
			if rule.Activity.Empty() {
				continue
			}
			out = append(out, ActivitySite{
				FlowID:          flowID,
				NodeID:          nodeID,
				HandlerEventKey: handlerEventKey,
				Source:          indexedHandlerEmitSiteKey("handler.rules", idx, "activity"),
				RuleID:          strings.TrimSpace(rule.ID),
				RuleIndex:       idx,
				Spec:            rule.Activity,
			})
		}
	}
	return out
}

func ActivityResultEventsForSite(site ActivitySite) ActivityResultEvents {
	activityID := strings.TrimSpace(site.Spec.ID)
	if activityID == "" {
		activityID = DefaultActivityID(site.NodeID, site.HandlerEventKey, site.RuleID, site.RuleIndex, site.Spec.Tool)
	}
	base := activityID
	flowID := strings.TrimSpace(site.FlowID)
	if flowID != "" && !strings.HasPrefix(base, flowID+".") {
		base = flowID + "." + base
	}
	base = eventidentity.Normalize(base)
	return ActivityResultEvents{
		ActivityID:   activityID,
		SuccessEvent: eventidentity.Normalize(base + "." + ActivityResultStatusSucceeded),
		FailureEvent: eventidentity.Normalize(base + "." + ActivityResultStatusFailed),
	}
}

func DefaultActivityID(nodeID, handlerEventKey, ruleID string, ruleIndex int, tool string) string {
	parts := []string{nodeID, handlerEventKey}
	if strings.TrimSpace(ruleID) != "" {
		parts = append(parts, ruleID)
	} else if ruleIndex >= 0 {
		parts = append(parts, fmt.Sprintf("rule_%d", ruleIndex))
	}
	parts = append(parts, tool)
	return strings.Join(activityIDParts(parts...), "_")
}

func activityIDParts(parts ...string) []string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = activitySlug(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func activitySlug(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range raw {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

func ActivityResultEventCatalogEntry(site ActivitySite, tool ToolSchemaEntry, status string) EventCatalogEntry {
	description := "Generated durable activity success event"
	required := []string{"activity_id", "tool", "effect_class", "attempt", "result"}
	properties := map[string]EventFieldSpec{
		"activity_id":  {Type: "string", Description: "Generated durable activity id."},
		"tool":         {Type: "string", Description: "Authored tools.yaml tool id executed by the activity."},
		"effect_class": {Type: "string", Description: "Authored durable activity effect class."},
		"attempt":      {Type: "integer", Description: "One-based activity attempt number."},
		"result":       {Type: "object", Description: "Tool output shaped by the authored tool output schema."},
	}
	if status == ActivityResultStatusFailed {
		description = "Generated durable activity failure event"
		required = []string{"activity_id", "tool", "effect_class", "attempt", "error"}
		properties = map[string]EventFieldSpec{
			"activity_id":  {Type: "string", Description: "Generated durable activity id."},
			"tool":         {Type: "string", Description: "Authored tools.yaml tool id attempted by the activity."},
			"effect_class": {Type: "string", Description: "Authored durable activity effect class."},
			"attempt":      {Type: "integer", Description: "One-based activity attempt number."},
			"error":        {Type: "string", Description: "Durable activity execution error."},
		}
	}
	return EventCatalogEntry{
		Swarm: EventSwarmMetadata{
			Note:     description,
			Source:   "contract_derived_activity",
			Producer: []string{strings.TrimSpace(site.NodeID)},
			Status:   "generated",
		},
		Note:        description,
		Emitter:     EventEmitterRef{NodeID: strings.TrimSpace(site.NodeID)},
		EmitterType: "system_node",
		Producer:    []string{strings.TrimSpace(site.NodeID)},
		Source:      "contract_derived_activity",
		Status:      "generated",
		OwningNode:  strings.TrimSpace(site.NodeID),
		Payload: EventPayloadSpec{
			Type:       "object",
			Properties: properties,
			Required:   required,
		},
		Required: required,
		Consumer: []string{},
	}
}

func ActivityResultEventSchemasForSite(site ActivitySite, tool ToolSchemaEntry) map[string]EventSchema {
	resultEvents := ActivityResultEventsForSite(site)
	out := map[string]EventSchema{
		resultEvents.SuccessEvent: {
			Description: "Generated durable activity success event",
			Schema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"activity_id", "tool", "effect_class", "attempt", "result"},
				"properties": map[string]any{
					"activity_id":  map[string]any{"type": "string"},
					"tool":         map[string]any{"type": "string"},
					"effect_class": map[string]any{"type": "string"},
					"attempt":      map[string]any{"type": "integer"},
					"result":       toolInputSchemaToJSONSchema(tool.OutputSchema),
				},
			},
		},
		resultEvents.FailureEvent: {
			Description: "Generated durable activity failure event",
			Schema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"activity_id", "tool", "effect_class", "attempt", "error"},
				"properties": map[string]any{
					"activity_id":  map[string]any{"type": "string"},
					"tool":         map[string]any{"type": "string"},
					"effect_class": map[string]any{"type": "string"},
					"attempt":      map[string]any{"type": "integer"},
					"error":        map[string]any{"type": "string"},
				},
			},
		},
	}
	return out
}

func (b *WorkflowContractBundle) GeneratedActivityEventEntries() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	out := map[string]EventCatalogEntry{}
	for _, site := range b.ActivitySites() {
		tool, ok := b.ToolEntries()[strings.TrimSpace(site.Spec.Tool)]
		if !ok {
			continue
		}
		events := ActivityResultEventsForSite(site)
		out[events.SuccessEvent] = ActivityResultEventCatalogEntry(site, tool, ActivityResultStatusSucceeded)
		out[events.FailureEvent] = ActivityResultEventCatalogEntry(site, tool, ActivityResultStatusFailed)
	}
	return out
}

func (b *WorkflowContractBundle) GeneratedActivityEventSchemas() map[string]EventSchema {
	if b == nil {
		return nil
	}
	out := map[string]EventSchema{}
	for _, site := range b.ActivitySites() {
		tool, ok := b.ToolEntries()[strings.TrimSpace(site.Spec.Tool)]
		if !ok {
			continue
		}
		for eventType, schema := range ActivityResultEventSchemasForSite(site, tool) {
			out[eventType] = schema
		}
	}
	return out
}

func (b *WorkflowContractBundle) ActivitySites() []ActivitySite {
	if b == nil {
		return nil
	}
	nodes := b.NodeEntries()
	nodeIDs := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID != "" {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	sort.Strings(nodeIDs)
	out := []ActivitySite{}
	for _, nodeID := range nodeIDs {
		flowID := ""
		if source, ok := b.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(source.FlowID)
		}
		out = append(out, ActivitySitesForNode(flowID, nodeID, b.NodeEventHandlers(nodeID))...)
	}
	return out
}

func (b *WorkflowContractBundle) generatedActivityEventsForNode(nodeID string) []string {
	nodeID = strings.TrimSpace(nodeID)
	if b == nil || nodeID == "" {
		return nil
	}
	out := []string{}
	for _, site := range b.ActivitySites() {
		if strings.TrimSpace(site.NodeID) != nodeID {
			continue
		}
		events := ActivityResultEventsForSite(site)
		out = append(out, events.SuccessEvent, events.FailureEvent)
	}
	return uniqueOrderedStrings(out)
}

func toolInputSchemaToJSONSchema(schema ToolInputSchema) map[string]any {
	out := map[string]any{}
	if value := strings.TrimSpace(schema.Type); value != "" {
		out["type"] = value
	} else {
		out["type"] = "object"
	}
	if value := strings.TrimSpace(schema.Description); value != "" {
		out["description"] = value
	}
	if len(schema.Properties) > 0 {
		props := make(map[string]any, len(schema.Properties))
		for name, prop := range schema.Properties {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			props[name] = toolInputSchemaToJSONSchema(prop)
		}
		out["properties"] = props
	}
	if len(schema.Required) > 0 {
		out["required"] = normalizeStrings(schema.Required)
	}
	if schema.Items != nil {
		out["items"] = toolInputSchemaToJSONSchema(*schema.Items)
	}
	if schema.AdditionalProperties.Allowed != nil {
		out["additionalProperties"] = *schema.AdditionalProperties.Allowed
	} else if schema.AdditionalProperties.Schema != nil {
		out["additionalProperties"] = toolInputSchemaToJSONSchema(*schema.AdditionalProperties.Schema)
	} else if out["type"] == "object" {
		out["additionalProperties"] = true
	}
	if schema.Minimum != nil {
		out["minimum"] = *schema.Minimum
	}
	if schema.Maximum != nil {
		out["maximum"] = *schema.Maximum
	}
	return out
}
