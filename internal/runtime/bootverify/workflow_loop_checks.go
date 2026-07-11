package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const loopValidationCheckID = "loop_validation"

func checkLoopValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	plans := semanticview.WorkflowLoops(c.source)
	findings := make([]Finding, 0)
	if len(plans) > 0 {
		for nodeID := range c.source.NodeEntries() {
			if strings.TrimSpace(nodeID) == loopruntime.BucketKey {
				findings = append(findings, loopFinding("workflow nodes", fmt.Sprintf("node id %s is reserved for durable loop control state", nodeID)))
			}
		}
	}
	stageOwners := map[string]string{}
	fieldOwners := map[string]string{}
	for _, plan := range plans {
		location := loopLocation(plan)
		topology, topologyOK := semanticview.WorkflowStageTopology(c.source, plan.FlowID)
		if !topologyOK {
			findings = append(findings, loopFinding(location, "canonical lifecycle topology is unavailable"))
			continue
		}
		if key := strings.TrimSpace(plan.MaxAttempts.PolicyRef); key != "" {
			value, ok := semanticview.PolicyValueForFlow(c.source, plan.FlowID, key)
			if !ok {
				findings = append(findings, loopFinding(location, fmt.Sprintf("max_attempts policy %s is not declared", key)))
			} else if !positiveLoopAttemptLimit(value.Value) {
				findings = append(findings, loopFinding(location, fmt.Sprintf("max_attempts policy %s must be a positive integer", key)))
			}
		}
		states := stringSet(c.source.FlowStates(plan.FlowID))
		if len(states) == 0 || !flowUsesAuthoredStages(c.source, plan.FlowID) {
			findings = append(findings, loopFinding(location, "loops require an authored stages: lifecycle"))
			continue
		}
		fieldKey := strings.TrimSpace(plan.FlowID) + "|" + strings.TrimSpace(plan.RevisionField)
		if prior, exists := fieldOwners[fieldKey]; exists {
			findings = append(findings, loopFinding(location, fmt.Sprintf("revision_field %s is already owned by %s", plan.RevisionField, prior)))
		} else {
			fieldOwners[fieldKey] = location
		}

		counts := map[runtimecontracts.LoopOperationKind]int{}
		regionStages := topology.StronglyConnectedComponent(plan.EntryStage)
		region := stringSet(regionStages)
		if strings.Join(sortedLoopStages(stringSet(plan.RegionStages)), "\x00") != strings.Join(regionStages, "\x00") {
			findings = append(findings, loopFinding(location, "lowered runtime loop region does not match the canonical transition SCC"))
		}
		for _, operation := range plan.Operations {
			counts[operation.Kind]++
			if _, ok := states[operation.From]; !ok {
				findings = append(findings, loopFinding(location, fmt.Sprintf("handler %s:%s from references unknown stage %s", operation.NodeID, operation.HandlerEvent, operation.From)))
			}
			if operation.AdvancesTo == "" && operation.Kind != runtimecontracts.LoopOperationAdmit {
				findings = append(findings, loopFinding(location, fmt.Sprintf("handler %s:%s %s requires advances_to", operation.NodeID, operation.HandlerEvent, operation.Kind)))
			} else if _, ok := states[operation.AdvancesTo]; !ok {
				findings = append(findings, loopFinding(location, fmt.Sprintf("handler %s:%s advances_to references unknown stage %s", operation.NodeID, operation.HandlerEvent, operation.AdvancesTo)))
			}
			handler, ok := c.source.NodeEventHandlers(operation.NodeID)[operation.HandlerEvent]
			if !ok {
				findings = append(findings, loopFinding(location, fmt.Sprintf("lowered operation %s:%s has no handler owner", operation.NodeID, operation.HandlerEvent)))
				continue
			}
			if err := runtimecontracts.ValidateLoopHandlerCombination(handler); err != nil {
				findings = append(findings, loopFinding(location, fmt.Sprintf("handler %s:%s %v", operation.NodeID, operation.HandlerEvent, err)))
			}
			if operation.Kind != runtimecontracts.LoopOperationStart {
				findings = append(findings, validateLoopInputRevision(c.source, plan, operation)...)
			}
			findings = append(findings, validateLoopEmitCarriage(c.source, plan, operation, handler)...)
			if operation.Kind == runtimecontracts.LoopOperationAdmit {
				for _, target := range runtimecontracts.HandlerAdvanceTargets(handler) {
					if _, ok := region[strings.TrimSpace(target)]; !ok {
						findings = append(findings, loopFinding(location, fmt.Sprintf("admit handler %s:%s outcome %s leaves the loop region; use close for lifecycle exit", operation.NodeID, operation.HandlerEvent, target)))
					}
				}
			}
			switch operation.Kind {
			case runtimecontracts.LoopOperationStart:
				if _, inside := region[operation.From]; inside {
					findings = append(findings, loopFinding(location, fmt.Sprintf("start source %s is inside the loop SCC", operation.From)))
				}
				if operation.AdvancesTo != plan.EntryStage {
					findings = append(findings, loopFinding(location, fmt.Sprintf("start handler %s:%s must enter loop entry stage %s", operation.NodeID, operation.HandlerEvent, plan.EntryStage)))
				}
			case runtimecontracts.LoopOperationAdmit:
				if _, inside := region[operation.From]; !inside {
					findings = append(findings, loopFinding(location, fmt.Sprintf("admit source %s is outside the loop SCC", operation.From)))
				}
			case runtimecontracts.LoopOperationRepeat:
				if _, inside := region[operation.From]; !inside {
					findings = append(findings, loopFinding(location, fmt.Sprintf("repeat source %s is outside the loop SCC", operation.From)))
				}
				if operation.AdvancesTo != plan.EntryStage {
					findings = append(findings, loopFinding(location, fmt.Sprintf("repeat handler %s:%s must return to entry stage %s", operation.NodeID, operation.HandlerEvent, plan.EntryStage)))
				}
			case runtimecontracts.LoopOperationClose:
				if _, inside := region[operation.From]; !inside {
					findings = append(findings, loopFinding(location, fmt.Sprintf("close source %s is outside the loop SCC", operation.From)))
				}
				if _, inside := region[operation.AdvancesTo]; inside {
					findings = append(findings, loopFinding(location, fmt.Sprintf("close target %s does not leave the loop SCC", operation.AdvancesTo)))
				}
			}
		}
		if counts[runtimecontracts.LoopOperationStart] != 1 {
			findings = append(findings, loopFinding(location, fmt.Sprintf("requires exactly one start operation, found %d", counts[runtimecontracts.LoopOperationStart])))
		}
		if counts[runtimecontracts.LoopOperationRepeat] == 0 {
			findings = append(findings, loopFinding(location, "requires at least one repeat operation"))
		}
		if counts[runtimecontracts.LoopOperationClose] == 0 {
			findings = append(findings, loopFinding(location, "requires at least one close operation"))
		}
		if plan.EntryStage == "" {
			findings = append(findings, loopFinding(location, "start operation must enter a loop stage"))
		}
		escape := strings.TrimSpace(plan.Escape.AdvancesTo)
		if _, ok := states[escape]; !ok {
			findings = append(findings, loopFinding(location, fmt.Sprintf("escape.advances_to references unknown stage %s", escape)))
		} else if _, inside := region[escape]; inside {
			findings = append(findings, loopFinding(location, fmt.Sprintf("escape target %s does not leave the loop SCC", escape)))
		}
		findings = append(findings, validateLoopEscapeEmit(c.source, plan)...)
		for stage := range region {
			key := strings.TrimSpace(plan.FlowID) + "|" + stage
			if prior, exists := stageOwners[key]; exists && prior != location {
				findings = append(findings, loopFinding(location, fmt.Sprintf("stage %s overlaps loop %s; nested or overlapping loops are unsupported", stage, prior)))
			} else {
				stageOwners[key] = location
			}
		}
		findings = append(findings, validateLoopRegionHandlers(c.source, plan, topology, region)...)
		findings = append(findings, validateLoopRecurringTimers(c.source, plan, region)...)
	}
	return findings
}

func positiveLoopAttemptLimit(value any) bool {
	switch typed := value.(type) {
	case int:
		return typed > 0
	case int32:
		return typed > 0
	case int64:
		return typed > 0
	case float64:
		return typed > 0 && typed == float64(int64(typed))
	default:
		return false
	}
}

func validateLoopInputRevision(source semanticview.Source, plan runtimecontracts.WorkflowLoopPlan, operation runtimecontracts.WorkflowLoopOperationPlan) []Finding {
	proof := semanticview.ResolveFlowEventProof(source, plan.FlowID, operation.HandlerEvent)
	if !proof.HasSchema {
		return []Finding{loopFinding(loopLocation(plan), fmt.Sprintf("handler %s:%s has no typed event schema for revision admission", operation.NodeID, operation.HandlerEvent))}
	}
	field, ok := proof.Entry.Payload.Properties[plan.RevisionField]
	if !ok || !joinTextType(field.Type) || !containsString(proof.Entry.Payload.Required, plan.RevisionField) {
		return []Finding{loopFinding(loopLocation(plan), fmt.Sprintf("handler %s:%s event must require text field %s", operation.NodeID, operation.HandlerEvent, plan.RevisionField))}
	}
	return nil
}

func validateLoopEmitCarriage(source semanticview.Source, plan runtimecontracts.WorkflowLoopPlan, operation runtimecontracts.WorkflowLoopOperationPlan, handler runtimecontracts.SystemNodeEventHandler) []Finding {
	findings := make([]Finding, 0)
	for _, site := range runtimecontracts.HandlerDeclarativeEmitSites(handler) {
		eventType := site.Spec.EventType()
		if eventType == "" {
			continue
		}
		proof := semanticview.ResolveFlowEventProof(source, plan.FlowID, eventType)
		field, declared := proof.Entry.Payload.Properties[plan.RevisionField]
		if !proof.HasSchema || !declared || !joinTextType(field.Type) || !containsString(proof.Entry.Payload.Required, plan.RevisionField) {
			findings = append(findings, loopFinding(loopLocation(plan), fmt.Sprintf("%s from %s:%s must emit an event requiring text field %s", site.Source, operation.NodeID, operation.HandlerEvent, plan.RevisionField)))
			continue
		}
		value, ok := site.Spec.Fields[plan.RevisionField]
		if !ok || !loopRevisionExpression(value) {
			findings = append(findings, loopFinding(loopLocation(plan), fmt.Sprintf("%s from %s:%s must carry %s from loop.revision_id", site.Source, operation.NodeID, operation.HandlerEvent, plan.RevisionField)))
		}
	}
	for _, action := range loopHandlerActions(handler) {
		if action.Mailbox == nil {
			continue
		}
		value, ok := action.Mailbox.Payload[plan.RevisionField]
		if !ok || !loopRevisionExpression(value) {
			findings = append(findings, loopFinding(loopLocation(plan), fmt.Sprintf("mailbox_write from %s:%s must carry %s from loop.revision_id in mailbox.payload", operation.NodeID, operation.HandlerEvent, plan.RevisionField)))
		}
	}
	return findings
}

func loopHandlerActions(handler runtimecontracts.SystemNodeEventHandler) []runtimecontracts.ActionSpec {
	out := []runtimecontracts.ActionSpec{handler.Action}
	appendRules := func(rules []runtimecontracts.HandlerRuleEntry) {
		for _, rule := range rules {
			out = append(out, rule.Action)
		}
	}
	appendRules(handler.Rules)
	appendRules(handler.OnComplete)
	if handler.Accumulate != nil {
		appendRules(handler.Accumulate.OnComplete)
		if handler.Accumulate.OnTimeout != nil {
			out = append(out, handler.Accumulate.OnTimeout.Action)
		}
	}
	if handler.Join != nil {
		out = append(out, handler.Join.OnComplete.Action, handler.Join.Timeout.Outcome.Action)
	}
	return out
}

func validateLoopRegionHandlers(source semanticview.Source, plan runtimecontracts.WorkflowLoopPlan, topology runtimecontracts.WorkflowStageTopology, region map[string]struct{}) []Finding {
	findings := make([]Finding, 0)
	for nodeID, node := range source.NodeEntries() {
		if strings.TrimSpace(nodeFlowID(source, nodeID)) != strings.TrimSpace(plan.FlowID) {
			continue
		}
		for eventType, handler := range node.EventHandlers {
			if !loopStagesIntersect(topology.HandlerStages(nodeID, eventType), region) {
				continue
			}
			proof := semanticview.ResolveFlowEventProof(source, plan.FlowID, eventType)
			if handler.Loop == nil {
				field, carries := proof.Entry.Payload.Properties[plan.RevisionField]
				typed := proof.HasSchema && carries && joinTextType(field.Type) && containsString(proof.Entry.Payload.Required, plan.RevisionField)
				if typed {
					findings = append(findings, loopFinding(loopLocation(plan), fmt.Sprintf("handler %s:%s may execute in the loop region but omits loop operation", nodeID, eventType)))
				} else {
					findings = append(findings, loopFinding(loopLocation(plan), fmt.Sprintf("handler %s:%s may execute in the loop region but omits both loop operation and required text revision field %s", nodeID, eventType, plan.RevisionField)))
				}
				continue
			}
			_, loopID, err := handler.Loop.Operation()
			if err != nil || loopID != plan.ID {
				findings = append(findings, loopFinding(loopLocation(plan), fmt.Sprintf("handler %s:%s revision field %s is bound to the wrong loop", nodeID, eventType, plan.RevisionField)))
			}
		}
	}
	return findings
}

func loopStagesIntersect(stages []string, region map[string]struct{}) bool {
	for _, stage := range stages {
		if _, ok := region[strings.TrimSpace(stage)]; ok {
			return true
		}
	}
	return false
}

func validateLoopEscapeEmit(source semanticview.Source, plan runtimecontracts.WorkflowLoopPlan) []Finding {
	spec := plan.Escape.Emit
	if spec.Empty() {
		return nil
	}
	location := loopLocation(plan)
	eventType := strings.TrimSpace(spec.EventType())
	proof := semanticview.ResolveFlowEventProof(source, plan.FlowID, eventType)
	if eventType == "" || !proof.HasSchema {
		return []Finding{loopFinding(location, fmt.Sprintf("escape.emit event %s has no typed event schema", eventType))}
	}
	findings := make([]Finding, 0)
	declared := payloadCompletenessDeclaredFields(proof.Entry)
	fields := map[string]struct{}{}
	for field := range spec.Fields {
		field = strings.TrimSpace(field)
		if field != "" {
			fields[field] = struct{}{}
		}
	}
	for _, field := range payloadCompletenessUndeclaredFields(fields, declared) {
		findings = append(findings, loopFinding(location, fmt.Sprintf("escape.emit event %s authors undeclared payload field %s", eventType, field)))
	}
	for _, field := range payloadCompletenessRequiredFields(proof.Entry) {
		if _, ok := spec.Fields[field]; !ok {
			findings = append(findings, loopFinding(location, fmt.Sprintf("escape.emit event %s omits required payload field %s", eventType, field)))
		}
	}
	revision, declaredRevision := proof.Entry.Payload.Properties[plan.RevisionField]
	if !declaredRevision || !joinTextType(revision.Type) || !containsString(proof.Entry.Payload.Required, plan.RevisionField) {
		findings = append(findings, loopFinding(location, fmt.Sprintf("escape.emit event %s must require text field %s", eventType, plan.RevisionField)))
	} else if value, ok := spec.Fields[plan.RevisionField]; !ok || !loopRevisionExpression(value) {
		findings = append(findings, loopFinding(location, fmt.Sprintf("escape.emit event %s must carry %s from loop.revision_id", eventType, plan.RevisionField)))
	}
	return findings
}

func validateLoopRecurringTimers(source semanticview.Source, plan runtimecontracts.WorkflowLoopPlan, region map[string]struct{}) []Finding {
	findings := make([]Finding, 0)
	loopEvents := map[string]struct{}{}
	for _, operation := range plan.Operations {
		loopEvents[strings.TrimSpace(operation.HandlerEvent)] = struct{}{}
	}
	for _, timer := range source.WorkflowTimers() {
		if !loopFlowMatches(source, plan.FlowID, timer.FlowID) {
			continue
		}
		connected := timer.StageOwned
		if _, ok := region[strings.TrimSpace(timer.Stage)]; ok {
			connected = true
		}
		if trigger, err := timeridentity.ParseStartTrigger(timer.StartOn); err == nil {
			if _, ok := region[strings.TrimSpace(trigger.Name)]; trigger.Kind == timeridentity.TriggerKindState && ok {
				connected = true
			}
		}
		if _, ok := loopEvents[strings.TrimSpace(timer.Event)]; ok {
			connected = true
		}
		if connected && timer.Recurring {
			findings = append(findings, loopFinding(loopLocation(plan), fmt.Sprintf("recurring timer %s is connected to the loop region; loop-owned timers must be one-shot", timer.ID)))
		}
		if connected && strings.TrimSpace(timer.AdvancesTo) != "" {
			if _, inside := region[strings.TrimSpace(timer.AdvancesTo)]; !inside {
				findings = append(findings, loopFinding(loopLocation(plan), fmt.Sprintf("timer %s advances_to %s leaves the loop region; emit a correlated event handled by repeat or close", timer.ID, timer.AdvancesTo)))
			}
		}
	}
	return findings
}

func loopRevisionExpression(value runtimecontracts.ExpressionValue) bool {
	switch value.Kind {
	case runtimecontracts.ExpressionKindRef:
		return strings.TrimSpace(value.Ref) == "loop.revision_id"
	case runtimecontracts.ExpressionKindCEL:
		return strings.TrimSpace(value.CEL) == "loop.revision_id"
	default:
		return false
	}
}

func loopFlowMatches(source semanticview.Source, planFlow, ownerFlow string) bool {
	planFlow, ownerFlow = strings.TrimSpace(planFlow), strings.TrimSpace(ownerFlow)
	if planFlow == ownerFlow {
		return true
	}
	return planFlow == "" && ownerFlow == strings.TrimSpace(source.WorkflowName())
}

func loopLocation(plan runtimecontracts.WorkflowLoopPlan) string {
	return fmt.Sprintf("flow %s loop %s", defaultFlowLabel(plan.FlowID), strings.TrimSpace(plan.ID))
}

func loopFinding(location, detail string) Finding {
	return NewHardInvalidityFinding(loopValidationCheckID, location, location+": "+detail,
		"Declare one bounded, disjoint lifecycle loop with exact from stages, typed revision carriage, a positive cap, and an escape outside its region.")
}

func sortedLoopStages(stages map[string]struct{}) []string {
	out := make([]string, 0, len(stages))
	for stage := range stages {
		out = append(out, stage)
	}
	sort.Strings(out)
	return out
}
