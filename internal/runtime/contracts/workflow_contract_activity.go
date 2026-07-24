package contracts

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"gopkg.in/yaml.v3"
)

const (
	ActivityResultStatusSucceeded = "succeeded"
	ActivityResultStatusFailed    = "failed"
	PrivateChannelActivityPrefix  = "platform.channel_activity."
)

type ActivitySite struct {
	FlowID          string
	NodeID          string
	HandlerEventKey string
	Source          string
	RuleID          string
	RuleIndex       int
	Spec            ActivitySpec
	RevisionField   string
}

type ActivityResultEvents struct {
	ActivityID        string
	SuccessEvent      string
	FailureEvent      string
	RevisionRequested string
	Rejected          string
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
	case ActivityEffectClassReadOnly, ActivityEffectClassNonIdempotentWrite:
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
	if flowID != "" && !strings.HasPrefix(base, flowID+"/") {
		base = flowID + "/" + base
	}
	base = eventidentity.Normalize(base)
	return ActivityResultEvents{
		ActivityID:        activityID,
		SuccessEvent:      eventidentity.Normalize(base + "." + ActivityResultStatusSucceeded),
		FailureEvent:      eventidentity.Normalize(base + "." + ActivityResultStatusFailed),
		RevisionRequested: eventidentity.Normalize(base + ".revision_requested"),
		Rejected:          eventidentity.Normalize(base + ".rejected"),
	}
}

func ActivityApprovalEventCatalogEntry(site ActivitySite, revision bool) EventCatalogEntry {
	note := "Generated durable activity approval rejection event"
	required := []string{"card_id", "activity_id", "tool", "effect_class", "effect_content_hash", "decided_by", "decided_at"}
	properties := map[string]EventFieldSpec{
		"card_id":             {Type: "string", Description: "Decision-card identity that settled the proposed effect."},
		"activity_id":         {Type: "string", Description: "Generated durable activity id."},
		"tool":                {Type: "string", Description: "Authored tools.yaml tool id held by the proposal."},
		"effect_class":        {Type: "string", Description: "Authored durable activity effect class."},
		"effect_content_hash": {Type: "string", Description: "Canonical immutable proposed-effect digest."},
		"decided_by":          {Type: "string", Description: "Authenticated actor that settled the card."},
		"decided_at":          {Type: "string", Description: "Canonical decision timestamp."},
		"reason":              {Type: "text", Description: "Optional operator rejection reason."},
	}
	if revision {
		note = "Generated durable activity revision-request event"
		required = append(required, "feedback")
		properties = map[string]EventFieldSpec{
			"card_id":             {Type: "string", Description: "Decision-card identity that settled the proposed effect."},
			"activity_id":         {Type: "string", Description: "Generated durable activity id."},
			"tool":                {Type: "string", Description: "Authored tools.yaml tool id held by the proposal."},
			"effect_class":        {Type: "string", Description: "Authored durable activity effect class."},
			"effect_content_hash": {Type: "string", Description: "Canonical immutable proposed-effect digest."},
			"decided_by":          {Type: "string", Description: "Authenticated actor that settled the card."},
			"decided_at":          {Type: "string", Description: "Canonical decision timestamp."},
			"feedback":            {Type: "text", Description: "Required operator revision feedback."},
		}
	}
	return EventCatalogEntry{
		Swarm: EventSwarmMetadata{Note: note, Source: "contract_derived_activity_approval", Producer: []string{strings.TrimSpace(site.NodeID)}, Status: "generated"},
		Note:  note, Emitter: EventEmitterRef{NodeID: strings.TrimSpace(site.NodeID)}, EmitterType: "system_node",
		Producer: []string{strings.TrimSpace(site.NodeID)}, Source: "contract_derived_activity_approval", Status: "generated",
		OwningNode: strings.TrimSpace(site.NodeID), Payload: EventPayloadSpec{Type: "object", Properties: properties, Required: required}, Required: required,
		Consumer: []string{},
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
		required = []string{"activity_id", "tool", "effect_class", "attempt", "failure"}
		properties = map[string]EventFieldSpec{
			"activity_id":  {Type: "string", Description: "Generated durable activity id."},
			"tool":         {Type: "string", Description: "Authored tools.yaml tool id attempted by the activity."},
			"effect_class": {Type: "string", Description: "Authored durable activity effect class."},
			"attempt":      {Type: "integer", Description: "One-based activity attempt number."},
			"failure":      {Type: runtimefailures.EnvelopeSchemaVersion + " envelope", Description: "Canonical durable activity failure envelope."},
		}
	}
	if revisionField := strings.TrimSpace(site.RevisionField); revisionField != "" {
		properties[revisionField] = EventFieldSpec{Type: "text", Description: "Owning bounded-loop revision identity."}
		required = append(required, revisionField)
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
				"required":             []string{"activity_id", "tool", "effect_class", "attempt", "failure"},
				"properties": map[string]any{
					"activity_id":  map[string]any{"type": "string"},
					"tool":         map[string]any{"type": "string"},
					"effect_class": map[string]any{"type": "string"},
					"attempt":      map[string]any{"type": "integer"},
					"failure":      runtimefailures.EnvelopeJSONSchema(),
				},
			},
		},
	}
	if revisionField := strings.TrimSpace(site.RevisionField); revisionField != "" {
		for eventType, schema := range out {
			required, _ := schema.Schema["required"].([]string)
			schema.Schema["required"] = append(required, revisionField)
			properties, _ := schema.Schema["properties"].(map[string]any)
			properties[revisionField] = map[string]any{"type": "string"}
			out[eventType] = schema
		}
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
		if site.Spec.Approval != nil {
			out[events.RevisionRequested] = ActivityApprovalEventCatalogEntry(site, true)
			out[events.Rejected] = ActivityApprovalEventCatalogEntry(site, false)
		}
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
		if site.Spec.Approval != nil {
			events := ActivityResultEventsForSite(site)
			out[events.RevisionRequested] = activityApprovalEventSchema(true)
			out[events.Rejected] = activityApprovalEventSchema(false)
		}
	}
	return out
}

func activityApprovalEventSchema(revision bool) EventSchema {
	description := "Generated durable activity approval rejection event"
	required := []string{"card_id", "activity_id", "tool", "effect_class", "effect_content_hash", "decided_by", "decided_at"}
	properties := map[string]any{
		"card_id":             map[string]any{"type": "string"},
		"activity_id":         map[string]any{"type": "string"},
		"tool":                map[string]any{"type": "string"},
		"effect_class":        map[string]any{"type": "string"},
		"effect_content_hash": map[string]any{"type": "string"},
		"decided_by":          map[string]any{"type": "string"},
		"decided_at":          map[string]any{"type": "string"},
		"reason":              map[string]any{"type": "string"},
	}
	if revision {
		description = "Generated durable activity revision-request event"
		required = append(required, "feedback")
		properties = map[string]any{
			"card_id":             map[string]any{"type": "string"},
			"activity_id":         map[string]any{"type": "string"},
			"tool":                map[string]any{"type": "string"},
			"effect_class":        map[string]any{"type": "string"},
			"effect_content_hash": map[string]any{"type": "string"},
			"decided_by":          map[string]any{"type": "string"},
			"decided_at":          map[string]any{"type": "string"},
			"feedback":            map[string]any{"type": "string"},
		}
	}
	return EventSchema{Description: description, Schema: map[string]any{
		"type": "object", "additionalProperties": false, "required": required, "properties": properties,
	}}
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
		for _, site := range ActivitySitesForNode(flowID, nodeID, b.NodeEventHandlers(nodeID)) {
			handler := b.NodeEventHandlers(nodeID)[site.HandlerEventKey]
			if handler.Loop != nil {
				_, loopID, err := handler.Loop.Operation()
				if err == nil {
					for _, plan := range b.WorkflowLoops() {
						if strings.TrimSpace(plan.FlowID) == flowID && strings.TrimSpace(plan.ID) == loopID {
							site.RevisionField = strings.TrimSpace(plan.RevisionField)
							break
						}
					}
				}
			}
			out = append(out, site)
		}
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
		if site.Spec.Approval != nil {
			out = append(out, events.RevisionRequested, events.Rejected)
		}
	}
	return uniqueOrderedStrings(out)
}

func toolInputSchemaToJSONSchema(schema ToolInputSchema) map[string]any {
	out := map[string]any{}
	if schema.Type != "" {
		out["type"] = schema.Type
	} else {
		out["type"] = "object"
	}
	if schema.Description != "" {
		out["description"] = schema.Description
	}
	if len(schema.Properties) > 0 {
		props := make(map[string]any, len(schema.Properties))
		for name, prop := range schema.Properties {
			props[name] = toolInputSchemaToJSONSchema(prop)
		}
		out["properties"] = props
	}
	if len(schema.Required) > 0 {
		out["required"] = append([]string(nil), schema.Required...)
	}
	if schema.Items != nil {
		out["items"] = toolInputSchemaToJSONSchema(*schema.Items)
	}
	if enum, present, err := ToolInputSchemaEnumProjection(schema); err != nil {
		out["enum"] = []any{}
	} else if present {
		out["enum"] = enum
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
	if schema.Pattern != "" {
		out["pattern"] = schema.Pattern
	}
	if schema.MinLength != nil {
		out["minLength"] = *schema.MinLength
	}
	if schema.MaxLength != nil {
		out["maxLength"] = *schema.MaxLength
	}
	if schema.MinItems != nil {
		out["minItems"] = *schema.MinItems
	}
	if schema.MaxItems != nil {
		out["maxItems"] = *schema.MaxItems
	}
	return out
}

// ToolInputSchemaEnumProjection is the one typed enum projection shared by
// provider-visible schemas and runtime acceptance validation. The presence bit
// distinguishes an omitted enum from an explicitly authored empty enum.
func ToolInputSchemaEnumProjection(schema ToolInputSchema) ([]any, bool, error) {
	if schema.Enum == nil {
		return nil, false, nil
	}
	values := make([]any, 0, len(schema.Enum))
	for index, literal := range schema.Enum {
		value, err := toolInputSchemaEnumLiteralValue(&literal.Node)
		if err != nil {
			return nil, true, fmt.Errorf("enum[%d]: %w", index, err)
		}
		values = append(values, value)
	}
	return values, true, nil
}

func toolInputSchemaEnumLiteralValue(node *yaml.Node) (any, error) {
	if node == nil || node.Kind == 0 {
		return nil, fmt.Errorf("literal node is missing")
	}
	switch node.Kind {
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!str":
			if !utf8.ValidString(node.Value) {
				return nil, fmt.Errorf("string literal is not valid UTF-8")
			}
			return node.Value, nil
		case "!!bool", "!!int", "!!float", "!!null":
			var value any
			if err := canonicaljson.DecodeInto([]byte(node.Value), &value); err != nil {
				return nil, fmt.Errorf("%s literal %q is not an admitted JSON value: %w", node.Tag, node.Value, err)
			}
			switch node.Tag {
			case "!!bool":
				if _, ok := value.(bool); !ok {
					return nil, fmt.Errorf("!!bool literal %q must decode to a JSON boolean", node.Value)
				}
			case "!!int":
				number, ok := value.(float64)
				if !ok || math.Trunc(number) != number {
					return nil, fmt.Errorf("!!int literal %q must decode to an integral JSON number", node.Value)
				}
			case "!!float":
				if _, ok := value.(float64); !ok {
					return nil, fmt.Errorf("!!float literal %q must decode to a JSON number", node.Value)
				}
			case "!!null":
				if value != nil {
					return nil, fmt.Errorf("!!null literal %q must decode to JSON null", node.Value)
				}
			}
			return value, nil
		default:
			return nil, fmt.Errorf("YAML scalar tag %q is not a JSON value", node.Tag)
		}
	case yaml.SequenceNode:
		if node.Tag != "" && node.Tag != "!!seq" {
			return nil, fmt.Errorf("YAML sequence tag %q is not a JSON value", node.Tag)
		}
		values := make([]any, 0, len(node.Content))
		for index, child := range node.Content {
			value, err := toolInputSchemaEnumLiteralValue(child)
			if err != nil {
				return nil, fmt.Errorf("item[%d]: %w", index, err)
			}
			values = append(values, value)
		}
		return values, nil
	case yaml.MappingNode:
		if node.Tag != "" && node.Tag != "!!map" {
			return nil, fmt.Errorf("YAML mapping tag %q is not a JSON value", node.Tag)
		}
		if len(node.Content)%2 != 0 {
			return nil, fmt.Errorf("object literal has an unmatched key")
		}
		values := make(map[string]any, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			keyNode := node.Content[index]
			if keyNode == nil || keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" {
				return nil, fmt.Errorf("object key[%d] must be a JSON string", index/2)
			}
			if !utf8.ValidString(keyNode.Value) {
				return nil, fmt.Errorf("object key[%d] is not valid UTF-8", index/2)
			}
			if _, exists := values[keyNode.Value]; exists {
				return nil, fmt.Errorf("duplicate object key %q", keyNode.Value)
			}
			value, err := toolInputSchemaEnumLiteralValue(node.Content[index+1])
			if err != nil {
				return nil, fmt.Errorf("object property %q: %w", keyNode.Value, err)
			}
			values[keyNode.Value] = value
		}
		return values, nil
	default:
		return nil, fmt.Errorf("YAML node kind %d is not a JSON value", node.Kind)
	}
}

// ToolInputSchemaJSONSchema exposes the canonical ToolInputSchema projection
// for provider-visible definitions and runtime validators.
func ToolInputSchemaJSONSchema(schema ToolInputSchema) map[string]any {
	projected, err := ProjectToolInputSchema(schema)
	if err != nil {
		return map[string]any{"type": "null", "enum": []any{map[string]any{"invalid_schema": true}}}
	}
	return projected
}

// ProjectToolInputSchema validates and projects the exact admitted schema.
// Runtime registrations use this owner instead of maintaining a second
// interpretation of enum, regex, key, and additional-property semantics.
func ProjectToolInputSchema(schema ToolInputSchema) (map[string]any, error) {
	if err := ValidateToolInputSchema(schema); err != nil {
		return nil, err
	}
	return toolInputSchemaToJSONSchema(schema), nil
}
