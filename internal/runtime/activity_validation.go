package runtime

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/stringsutil"
)

func validateDurableActivitySurface(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	var errs []error
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for handlerEventKey, handler := range source.ExecutableNodeEventHandlers(node) {
			errs = append(errs, validateHandlerActivitySurface(source, node, handlerEventKey, handler)...)
		}
	}
	errs = append(errs, validateActivityResultEventNameCollisions(source)...)
	return errs
}

func validateHandlerActivitySurface(source semanticview.Source, node runtimeidentity.ExecutableNode, handlerEventKey string, handler runtimecontracts.SystemNodeEventHandler) []error {
	context := fmt.Sprintf("node %s handler %s", node.Key(), strings.TrimSpace(handlerEventKey))
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
		errs = append(errs, validateActivitySpec(source, node, handlerEventKey, "", -1, context+".activity", handler.Activity)...)
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
			errs = append(errs, validateActivitySpec(source, node, handlerEventKey, rule.ID, idx, ruleContext+".activity", rule.Activity)...)
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
	return errs
}

func validateActivitySpec(source semanticview.Source, node runtimeidentity.ExecutableNode, handlerEventKey, ruleID string, ruleIndex int, context string, activity runtimecontracts.ActivitySpec) []error {
	var errs []error
	toolID := strings.TrimSpace(activity.Tool)
	if toolID == "" {
		return []error{fmt.Errorf("%s: tool is required", context)}
	}
	tool, ok := source.ToolEntries()[toolID]
	if !ok {
		return []error{fmt.Errorf("%s: tool %q is not declared in tools.yaml", context, toolID)}
	}
	handler := tool.Handler()
	if handler != runtimecontracts.ToolHandlerHTTP {
		errs = append(errs, fmt.Errorf("%s: tool %q handler_type %q is not supported for activities; MCP/platform/native/generated tools fail closed in Stage 1", context, toolID, handler.String()))
	}
	if _, hasHTTP := tool.HTTPExecution(); !hasHTTP {
		errs = append(errs, fmt.Errorf("%s: tool %q is missing http block; activities support authored HTTP tools only", context, toolID))
	}
	if tool.RatePolicy().Enabled() {
		errs = append(errs, fmt.Errorf("%s: tool %q uses rate_limit; activity HTTP rate-limit admission is split until the activity dispatcher consumes the external dispatch owner", context, toolID))
	}
	effectClass := tool.Effect()
	if effectClass == "" {
		errs = append(errs, fmt.Errorf("%s: tool %q must declare effect_class; executable Stage 1 values are read_only and non_idempotent_write on the activity journal path", context, toolID))
	} else if effectClass == runtimecontracts.ActivityEffectClassLongRunning {
		errs = append(errs, fmt.Errorf("%s: tool %q effect_class long_running is split to later durable resume/cancel support", context, toolID))
	} else if effectClass == runtimecontracts.ActivityEffectClassIdempotentWrite {
		errs = append(errs, fmt.Errorf("%s: tool %q effect_class %q is split until idempotency execution ownership is specified and implemented", context, toolID, effectClass))
	} else if !runtimecontracts.SupportedActivityEffectClass(effectClass) {
		errs = append(errs, fmt.Errorf("%s: tool %q effect_class %q is not supported for activities", context, toolID, effectClass))
	}
	_, hasManagedCredential := tool.ManagedCredentialExecution()
	credentials := tool.Credentials()
	if hasManagedCredential {
		if len(credentials) > 0 {
			errs = append(errs, fmt.Errorf("%s: tool %q must not declare both static credentials and managed_credential for activity HTTP execution", context, toolID))
		}
		if tool.Category() != runtimecontracts.ToolCategoryProviderConnector || effectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
			errs = append(errs, fmt.Errorf("%s: tool %q uses managed_credential; managed credential activity HTTP execution is supported only for non_idempotent_write provider connector tools", context, toolID))
		}
	}
	if len(credentials) > 0 && effectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		errs = append(errs, fmt.Errorf("%s: tool %q uses static credentials; static credential activity HTTP execution is supported only for non_idempotent_write authored HTTP activities", context, toolID))
	}
	if activity.Approval != nil {
		decision := strings.TrimSpace(activity.Approval.Decision)
		if decision == "" || decision != activity.Approval.Decision {
			errs = append(errs, fmt.Errorf("%s.approval.decision: a canonical stable decision id is required", context))
		}
		if effectClass == runtimecontracts.ActivityEffectClassReadOnly {
			errs = append(errs, fmt.Errorf("%s.approval: read-only activities don't need approval - approvals guard outward effects", context))
		} else if effectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
			errs = append(errs, fmt.Errorf("%s.approval: activity approval requires effect_class non_idempotent_write", context))
		}
		approvalSite := runtimecontracts.ActivitySite{
			Node: node, HandlerEventKey: handlerEventKey, RuleID: ruleID, RuleIndex: ruleIndex, Spec: activity,
		}
		if !activityRevisionConsumerExists(source, approvalSite) {
			events := runtimecontracts.ActivityResultEventsForSite(approvalSite)
			errs = append(errs, fmt.Errorf("%s.approval: generated revision event %q has no consumer; add a handler so operator feedback cannot disappear", context, events.RevisionRequested))
		}
	}
	errs = append(errs, validateActivityInputAgainstToolSchema(context, activity, tool.InputSchema())...)
	site := runtimecontracts.ActivitySite{
		Node:            node,
		HandlerEventKey: handlerEventKey,
		Spec:            activity,
	}
	resultEvents := runtimecontracts.ActivityResultEventsForSite(site)
	if resultEvents.SuccessEvent == "" || resultEvents.FailureEvent == "" {
		errs = append(errs, fmt.Errorf("%s: generated result event names could not be derived", context))
	}
	return errs
}

func activityRevisionConsumerExists(source semanticview.Source, site runtimecontracts.ActivitySite) bool {
	if source == nil || site.Spec.Approval == nil {
		return false
	}
	want := eventidentity.Normalize(runtimecontracts.ActivityResultEventsForSite(site).RevisionRequested)
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		if semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, want).Matched {
			return true
		}
	}
	return false
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

	var errs []error
	generated := map[string]string{}
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, site := range runtimecontracts.ActivitySitesForNode(node, source.ExecutableNodeEventHandlers(node)) {
			context := activitySiteContext(site)
			resultEvents := runtimecontracts.ActivityResultEventsForSite(site)
			eventTypes := []string{resultEvents.SuccessEvent, resultEvents.FailureEvent}
			if site.Spec.Approval != nil {
				eventTypes = append(eventTypes, resultEvents.RevisionRequested, resultEvents.Rejected)
			}
			for _, eventType := range eventTypes {
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
	context := fmt.Sprintf("node %s handler %s", site.Node.Key(), strings.TrimSpace(site.HandlerEventKey))
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
	required := stringsutil.Unique(schema.RequiredProperties())
	for _, field := range required {
		if _, ok := input[field]; !ok {
			errs = append(errs, fmt.Errorf("%s.input: required tool input field %q is not mapped", context, field))
		}
	}
	if len(schema.PropertyNames()) == 0 {
		return errs
	}
	allowAdditional, additionalDeclared := schema.AdditionalPropertiesAllowed()
	_, hasAdditionalSchema := schema.AdditionalPropertiesSchema()
	allowAdditional = additionalDeclared && allowAdditional
	if allowAdditional || hasAdditionalSchema {
		return errs
	}
	for field := range input {
		if _, ok := schema.Property(field); !ok {
			errs = append(errs, fmt.Errorf("%s.input.%s: field is not declared by the tool input schema", context, field))
		}
	}
	return errs
}
