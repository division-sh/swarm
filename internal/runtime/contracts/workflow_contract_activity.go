package contracts

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

const (
	ActivityResultStatusSucceeded = "succeeded"
	ActivityResultStatusFailed    = "failed"
	PrivateChannelActivityPrefix  = "platform.channel_activity."
)

type ActivitySite struct {
	Node            runtimeidentity.ExecutableNode
	HandlerEventKey string
	Source          string
	RuleID          string
	RuleRef         runtimeidentity.DeclarationIdentity
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

func ActivitySitesForNode(node runtimeidentity.ExecutableNode, handlers map[string]SystemNodeEventHandler) []ActivitySite {
	if !node.Valid() {
		return nil
	}
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
				Node:            node,
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
			ruleRef, _ := rule.DeclarationIdentity()
			out = append(out, ActivitySite{
				Node:            node,
				HandlerEventKey: handlerEventKey,
				Source:          indexedHandlerEmitSiteKey("handler.rules", idx, "activity"),
				RuleID:          strings.TrimSpace(rule.ID),
				RuleRef:         ruleRef,
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
		activityID = DefaultActivityID(site.Node.NodeID(), site.HandlerEventKey, site.RuleID, site.RuleIndex, site.Spec.Tool)
	}
	base := activityID
	flowID := site.Node.FlowPath()
	if flowID != "" && flowID != "." && !strings.HasPrefix(base, flowID+"/") {
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
		Payload: EventPayloadSpec{Type: "object", Properties: properties, Required: required},
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
	required := []string{"activity_id", "tool", "effect_class", "attempt", "result"}
	properties := map[string]EventFieldSpec{
		"activity_id":  {Type: "string", Description: "Generated durable activity id."},
		"tool":         {Type: "string", Description: "Authored tools.yaml tool id executed by the activity."},
		"effect_class": {Type: "string", Description: "Authored durable activity effect class."},
		"attempt":      {Type: "integer", Description: "One-based activity attempt number."},
		"result":       {Type: "object", Description: "Tool output shaped by the authored tool output schema."},
	}
	if status == ActivityResultStatusFailed {
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
		Payload: EventPayloadSpec{
			Type:       "object",
			Properties: properties,
			Required:   required,
		},
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
					"result":       toolInputSchemaToJSONSchema(tool.OutputSchema()),
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
	for _, scope := range b.compiledEventSchemas {
		for key, schema := range scope.bindings {
			if key == schema.EventName() && schema.Classification() == CompiledEventSchemaGenerated {
				out[key] = cloneEventCatalogEntry(schema.value.declaration)
			}
		}
	}
	return out
}

// Generated records retain the declaring flow before any catalog projection.
// A global event-name map cannot establish ownership for a scoped lookup.
func (b *WorkflowContractBundle) generatedActivityDeclarationRecords() []currentEventDeclarationRecord {
	var out []currentEventDeclarationRecord
	for _, site := range b.ActivitySites() {
		tools := b.ToolEntries()
		if b.activityToolsByFlow != nil {
			tools = b.activityToolsByFlow[site.Node.FlowPath()]
		}
		tool, ok := tools[strings.TrimSpace(site.Spec.Tool)]
		if !ok {
			continue
		}
		events := ActivityResultEventsForSite(site)
		entries := map[string]EventCatalogEntry{
			events.SuccessEvent: ActivityResultEventCatalogEntry(site, tool, ActivityResultStatusSucceeded),
			events.FailureEvent: ActivityResultEventCatalogEntry(site, tool, ActivityResultStatusFailed),
		}
		schemas := ActivityResultEventSchemasForSite(site, tool)
		if site.Spec.Approval != nil {
			entries[events.RevisionRequested] = ActivityApprovalEventCatalogEntry(site, true)
			entries[events.Rejected] = ActivityApprovalEventCatalogEntry(site, false)
			schemas[events.RevisionRequested] = activityApprovalEventSchema(true)
			schemas[events.Rejected] = activityApprovalEventSchema(false)
		}
		for _, name := range sortedContractKeys(entries) {
			entry := entries[name]
			properties, _ := schemas[name].Schema["properties"].(map[string]any)
			for fieldName, field := range entry.Payload.Properties {
				raw, _ := properties[fieldName].(map[string]any)
				exact, err := AdmitToolInputSchemaMap(raw)
				if err != nil {
					panic(fmt.Sprintf("generated activity schema %s.%s: %v", name, fieldName, err))
				}
				field.ExactSchema = &exact
				entry.Payload.Properties[fieldName] = field
			}
			file := ""
			if view, ok := b.exactFlowEventDeclarationView(site.Node.FlowPath()); ok {
				file = view.Paths.NodesFile
			}
			out = append(out, currentEventDeclarationRecord{
				flowPath: site.Node.FlowPath(), layer: "generated_activity", sourceFile: file,
				localName: eventidentity.LeafName(name), qualifiedName: name, entry: entry,
			})
		}
	}
	return out
}

func (b *WorkflowContractBundle) GeneratedActivityEventSchemas() map[string]EventSchema {
	if b == nil {
		return nil
	}
	out := map[string]EventSchema{}
	for _, scope := range b.compiledEventSchemas {
		for key, schema := range scope.bindings {
			if key == schema.EventName() && schema.Classification() == CompiledEventSchemaGenerated {
				out[key] = schema.EventSchema()
			}
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
	out := []ActivitySite{}
	for _, record := range b.ScopedNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for _, site := range ActivitySitesForNode(node, record.Entry.EventHandlers) {
			handler := record.Entry.EventHandlers[site.HandlerEventKey]
			if handler.Loop != nil {
				_, loopID, err := handler.Loop.Operation()
				if err == nil {
					for _, plan := range b.WorkflowLoops() {
						if strings.TrimSpace(plan.FlowID) == node.FlowPath() && strings.TrimSpace(plan.ID) == loopID {
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

func (b *WorkflowContractBundle) GeneratedActivityEventsForExecutableNode(node runtimeidentity.ExecutableNode) []string {
	if b == nil || !node.Valid() {
		return nil
	}
	out := []string{}
	for _, site := range b.ActivitySites() {
		if !site.Node.Equal(node) {
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
	return projectAdmittedToolInputSchema(schema)
}

// ToolInputSchemaEnumProjection is the one typed enum projection shared by
// provider-visible schemas and runtime acceptance validation. The presence bit
// distinguishes an omitted enum from an explicitly authored empty enum.
func ToolInputSchemaEnumProjection(schema ToolInputSchema) ([]any, bool, error) {
	enum, declared := schema.EnumValues()
	if !declared {
		return nil, false, nil
	}
	values := make([]any, 0, len(enum))
	for _, value := range enum {
		values = append(values, value.Interface())
	}
	return values, true, nil
}
