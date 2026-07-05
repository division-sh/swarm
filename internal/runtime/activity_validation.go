package runtime

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func validateDurableActivitySurface(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	var errs []error
	nodes := source.NodeEntries()
	for nodeID := range nodes {
		flowID := ""
		if sourceInfo, ok := source.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(sourceInfo.FlowID)
		}
		for handlerEventKey, handler := range source.NodeEventHandlers(nodeID) {
			errs = append(errs, validateHandlerActivitySurface(source, flowID, nodeID, handlerEventKey, handler)...)
		}
	}
	errs = append(errs, validateActivityResultEventNameCollisions(source)...)
	return errs
}

func validateHandlerActivitySurface(source semanticview.Source, flowID, nodeID, handlerEventKey string, handler runtimecontracts.SystemNodeEventHandler) []error {
	context := fmt.Sprintf("node %s handler %s", strings.TrimSpace(nodeID), strings.TrimSpace(handlerEventKey))
	var errs []error
	hasTopLevelActivity := !handler.Activity.Empty()
	hasRuleActivity := false
	for _, rule := range handler.Rules {
		if !rule.Activity.Empty() {
			hasRuleActivity = true
			break
		}
	}
	if !hasTopLevelActivity && !hasRuleActivity {
		errs = append(errs, rejectUnsupportedNestedActivityContexts(context, handler)...)
		return errs
	}
	if hasTopLevelActivity {
		if len(handler.Rules) > 0 {
			errs = append(errs, fmt.Errorf("%s activity: handler-level activity is only allowed on handlers without rules; move activity ownership to the selected rule", context))
		}
		if strings.TrimSpace(handler.Action.ID) != "" {
			errs = append(errs, fmt.Errorf("%s activity: activity and action are mutually exclusive", context))
		}
		if !handler.Emit.Empty() || !handler.OnSuccess.Empty() {
			errs = append(errs, fmt.Errorf("%s activity: activity and authored emit/on_success emit are mutually exclusive in Stage 1; use generated activity result events", context))
		}
		errs = append(errs, validateActivitySpec(source, flowID, nodeID, handlerEventKey, context+".activity", handler.Activity)...)
	}
	if hasRuleActivity {
		if !handler.Activity.Empty() {
			errs = append(errs, fmt.Errorf("%s activity: handler-level activity cannot be combined with rule activities", context))
		}
		if !handler.Emit.Empty() || !handler.OnSuccess.Empty() {
			errs = append(errs, fmt.Errorf("%s activity: rule activity and handler emit/on_success emit are mutually exclusive in Stage 1; use generated activity result events", context))
		}
		for idx, rule := range handler.Rules {
			ruleContext := fmt.Sprintf("%s.rules[%d]", context, idx)
			if strings.TrimSpace(rule.ID) != "" {
				ruleContext = fmt.Sprintf("%s.rules[%s]", context, strings.TrimSpace(rule.ID))
			}
			if rule.Activity.Empty() {
				continue
			}
			if strings.TrimSpace(rule.Action.ID) != "" {
				errs = append(errs, fmt.Errorf("%s activity: activity and action are mutually exclusive", ruleContext))
			}
			if !rule.Emit.Empty() || (rule.FanOut != nil && !rule.FanOut.Emit.Empty()) {
				errs = append(errs, fmt.Errorf("%s activity: activity and authored emit/fan_out emit are mutually exclusive in Stage 1; use generated activity result events", ruleContext))
			}
			errs = append(errs, validateActivitySpec(source, flowID, nodeID, handlerEventKey, ruleContext+".activity", rule.Activity)...)
		}
	}
	errs = append(errs, rejectUnsupportedNestedActivityContexts(context, handler)...)
	return errs
}

func rejectUnsupportedNestedActivityContexts(context string, handler runtimecontracts.SystemNodeEventHandler) []error {
	var errs []error
	for idx, rule := range handler.OnComplete {
		if !rule.Activity.Empty() {
			errs = append(errs, fmt.Errorf("%s.on_complete[%d].activity: durable activity is not supported in on_complete in Stage 1", context, idx))
		}
	}
	if handler.Accumulate != nil {
		for idx, rule := range handler.Accumulate.OnComplete {
			if !rule.Activity.Empty() {
				errs = append(errs, fmt.Errorf("%s.accumulate.on_complete[%d].activity: durable activity is not supported in accumulator completion in Stage 1", context, idx))
			}
		}
		if handler.Accumulate.OnTimeout != nil && !handler.Accumulate.OnTimeout.Activity.Empty() {
			errs = append(errs, fmt.Errorf("%s.accumulate.on_timeout.activity: durable activity is not supported in accumulator timeout in Stage 1", context))
		}
	}
	for idx, branch := range handler.Branch {
		if branch.Then != nil && !branch.Then.Activity.Empty() {
			errs = append(errs, fmt.Errorf("%s.branch[%d].then.activity: durable activity is not supported in branch rules in Stage 1", context, idx))
		}
		if branch.Else != nil && !branch.Else.Activity.Empty() {
			errs = append(errs, fmt.Errorf("%s.branch[%d].else.activity: durable activity is not supported in branch rules in Stage 1", context, idx))
		}
	}
	return errs
}

func validateActivitySpec(source semanticview.Source, flowID, nodeID, handlerEventKey, context string, activity runtimecontracts.ActivitySpec) []error {
	var errs []error
	toolID := strings.TrimSpace(activity.Tool)
	if toolID == "" {
		return []error{fmt.Errorf("%s: tool is required", context)}
	}
	tool, ok := source.ToolEntries()[toolID]
	if !ok {
		return []error{fmt.Errorf("%s: tool %q is not declared in tools.yaml", context, toolID)}
	}
	handlerType := strings.TrimSpace(strings.ToLower(tool.HandlerType))
	if handlerType == "" {
		if tool.HTTP == nil {
			errs = append(errs, fmt.Errorf("%s: tool %q resolves to unsupported platform/native surface; activities support authored HTTP tools only", context, toolID))
		}
	} else if handlerType != "http" {
		errs = append(errs, fmt.Errorf("%s: tool %q handler_type %q is not supported for activities; MCP/platform/native/generated tools fail closed in Stage 1", context, toolID, handlerType))
	}
	if tool.HTTP == nil {
		errs = append(errs, fmt.Errorf("%s: tool %q is missing http block; activities support authored HTTP tools only", context, toolID))
	}
	if strings.TrimSpace(tool.RateLimit) != "" || strings.TrimSpace(tool.RateLimitMaxWait) != "" {
		errs = append(errs, fmt.Errorf("%s: tool %q uses rate_limit; activity HTTP rate-limit admission is split until the activity dispatcher consumes the external dispatch owner", context, toolID))
	}
	if len(tool.ResponseMapping) > 0 {
		errs = append(errs, fmt.Errorf("%s: tool %q uses response_mapping; activity HTTP response mapping is split until the activity dispatcher consumes the HTTP tool response-mapping owner", context, toolID))
	}
	if len(tool.Credentials) > 0 || tool.ManagedCredential != nil {
		errs = append(errs, fmt.Errorf("%s: tool %q uses credentials; credentialed activity HTTP execution is split until activity credential authority is specified", context, toolID))
	}
	effectClass := runtimecontracts.NormalizeActivityEffectClass(tool.EffectClass)
	if effectClass == "" {
		errs = append(errs, fmt.Errorf("%s: tool %q must declare effect_class; the Stage 1 executable value is read_only", context, toolID))
	} else if effectClass == runtimecontracts.ActivityEffectClassLongRunning {
		errs = append(errs, fmt.Errorf("%s: tool %q effect_class long_running is split to later durable resume/cancel support", context, toolID))
	} else if effectClass == runtimecontracts.ActivityEffectClassIdempotentWrite || effectClass == runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		errs = append(errs, fmt.Errorf("%s: tool %q effect_class %q is split until the activity runtime owns stable attempt/result journaling and idempotency execution", context, toolID, effectClass))
	} else if !runtimecontracts.SupportedActivityEffectClass(effectClass) {
		errs = append(errs, fmt.Errorf("%s: tool %q effect_class %q is not supported for activities", context, toolID, tool.EffectClass))
	}
	errs = append(errs, validateActivityInputAgainstToolSchema(context, activity, tool.InputSchema)...)
	site := runtimecontracts.ActivitySite{
		FlowID:          flowID,
		NodeID:          nodeID,
		HandlerEventKey: handlerEventKey,
		Spec:            activity,
	}
	resultEvents := runtimecontracts.ActivityResultEventsForSite(site)
	if resultEvents.SuccessEvent == "" || resultEvents.FailureEvent == "" {
		errs = append(errs, fmt.Errorf("%s: generated result event names could not be derived", context))
	}
	return errs
}

func validateActivityResultEventNameCollisions(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	authored := map[string]string{}
	addAuthoredEvents := func(entries map[string]runtimecontracts.EventCatalogEntry, owner string) {
		for eventType := range entries {
			normalized := eventidentity.Normalize(eventType)
			if normalized == "" {
				continue
			}
			if _, exists := authored[normalized]; !exists {
				authored[normalized] = owner
			}
		}
	}
	addAuthoredEvents(source.AuthoredEventEntries(), "authored event")
	addAuthoredEvents(source.AuthoredResolvedEventCatalog(), "authored resolved event")

	nodes := source.NodeEntries()
	nodeIDs := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID != "" {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	sort.Strings(nodeIDs)

	var errs []error
	generated := map[string]string{}
	for _, nodeID := range nodeIDs {
		flowID := ""
		if sourceInfo, ok := source.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(sourceInfo.FlowID)
		}
		for _, site := range runtimecontracts.ActivitySitesForNode(flowID, nodeID, source.NodeEventHandlers(nodeID)) {
			context := activitySiteContext(site)
			resultEvents := runtimecontracts.ActivityResultEventsForSite(site)
			for _, eventType := range []string{resultEvents.SuccessEvent, resultEvents.FailureEvent} {
				normalized := eventidentity.Normalize(eventType)
				if normalized == "" {
					continue
				}
				if owner, ok := authored[normalized]; ok {
					errs = append(errs, fmt.Errorf("%s: generated activity result event %q collides with %s %q", context, normalized, owner, normalized))
					continue
				}
				if previous, ok := generated[normalized]; ok {
					errs = append(errs, fmt.Errorf("%s: generated activity result event %q collides with generated result event from %s", context, normalized, previous))
					continue
				}
				generated[normalized] = context
			}
		}
	}
	return errs
}

func activitySiteContext(site runtimecontracts.ActivitySite) string {
	context := fmt.Sprintf("node %s handler %s", strings.TrimSpace(site.NodeID), strings.TrimSpace(site.HandlerEventKey))
	if site.RuleIndex >= 0 {
		if strings.TrimSpace(site.RuleID) != "" {
			return fmt.Sprintf("%s.rules[%s].activity", context, strings.TrimSpace(site.RuleID))
		}
		return fmt.Sprintf("%s.rules[%d].activity", context, site.RuleIndex)
	}
	return context + ".activity"
}

func validateActivityInputAgainstToolSchema(context string, activity runtimecontracts.ActivitySpec, schema runtimecontracts.ToolInputSchema) []error {
	var errs []error
	input := activity.Input
	required := normalizeStrings(schema.Required)
	for _, field := range required {
		if _, ok := input[field]; !ok {
			errs = append(errs, fmt.Errorf("%s.input: required tool input field %q is not mapped", context, field))
		}
	}
	if len(schema.Properties) == 0 {
		return errs
	}
	allowAdditional := schema.AdditionalProperties.Allowed != nil && *schema.AdditionalProperties.Allowed
	hasAdditionalSchema := schema.AdditionalProperties.Schema != nil
	if allowAdditional || hasAdditionalSchema {
		return errs
	}
	for field := range input {
		if _, ok := schema.Properties[field]; !ok {
			errs = append(errs, fmt.Errorf("%s.input.%s: field is not declared by the tool input schema", context, field))
		}
	}
	return errs
}

func normalizeStrings(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, value := range in {
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
	return out
}
