package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkTimerValidation(c *checkerContext) []Finding { return c.timerValidation() }

func (c *checkerContext) timerValidation() []Finding {
	if c.timerLoaded {
		return c.timerFindings
	}
	c.timerLoaded = true
	for _, timer := range c.source.WorkflowTimers() {
		owner := strings.TrimSpace(timer.Owner)
		if owner == "" {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s missing owner", timer.ID),
				Location: strings.TrimSpace(timer.ID),
			})
			continue
		}
		if owner != "runtime" {
			if _, systemNode := c.source.NodeEntries()[owner]; !systemNode {
				if !participantExistsLocal(c.source, owner) {
					c.timerFindings = append(c.timerFindings, Finding{
						CheckID:  "timer_validation",
						Severity: "error",
						Message:  fmt.Sprintf("timer %s owner %s missing from participants", timer.ID, owner),
						Location: strings.TrimSpace(timer.ID),
					})
				}
			}
		}
		fireEvent := semanticview.ResolveFlowEventProof(c.source, timer.FlowID, timer.Event)
		if !fireEvent.HasSchema {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s event %s missing from event catalog", timer.ID, timer.Event),
				Location: strings.TrimSpace(timer.ID),
			})
		} else {
			c.validateTimerFireEventConsumer(timer, fireEvent)
		}
		startTrigger, err := timeridentity.ParseStartTrigger(timer.StartOn)
		if err != nil {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s start_on %q is invalid: %v", timer.ID, timer.StartOn, err),
				Location: strings.TrimSpace(timer.ID),
			})
		} else {
			c.validateTimerTrigger(timer, "start_on", startTrigger)
		}
		cancelTrigger, err := timeridentity.ParseCancelTrigger(timer.CancelOn)
		if err != nil {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s cancel_on %q is invalid: %v", timer.ID, timer.CancelOn, err),
				Location: strings.TrimSpace(timer.ID),
			})
		} else {
			c.validateTimerTrigger(timer, "cancel_on", cancelTrigger)
		}
	}
	return c.timerFindings
}

func (c *checkerContext) validateTimerTrigger(timer runtimecontracts.WorkflowTimerContract, field string, trigger timeridentity.Trigger) {
	if !trigger.Valid() {
		return
	}
	switch trigger.Kind {
	case timeridentity.TriggerKindState:
		if !containsString(flowStatesForTimer(c.source, timer.FlowID), trigger.Name) {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s %s references unknown state %s", timer.ID, field, trigger.Name),
				Location: strings.TrimSpace(timer.ID),
			})
		}
	case timeridentity.TriggerKindEvent:
		ref := semanticview.ResolveFlowEventProof(c.source, timer.FlowID, trigger.Name)
		if !ref.HasSchema {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s %s references unknown event %s", timer.ID, field, trigger.Name),
				Location: strings.TrimSpace(timer.ID),
			})
			return
		}
		if !c.timerTriggerEventProduced(timer, ref) {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s %s event %s has no producer path", timer.ID, field, ref.DisplayName()),
				Location: strings.TrimSpace(timer.ID),
			})
		}
	}
}

func (c *checkerContext) validateTimerFireEventConsumer(timer runtimecontracts.WorkflowTimerContract, ref semanticview.FlowEventProof) {
	if timerFireEventHasConsumer(c.source, ref) {
		return
	}
	c.timerFindings = append(c.timerFindings, Finding{
		CheckID:  "timer_validation",
		Severity: "error",
		Message:  fmt.Sprintf("timer %s event %s has no executable consumer or explicit external/exported role", timer.ID, ref.DisplayName()),
		Location: strings.TrimSpace(timer.ID),
	})
}

func timerFireEventHasConsumer(source semanticview.Source, ref semanticview.FlowEventProof) bool {
	if source == nil {
		return false
	}
	if len(source.RuntimeEventOwners(ref.Canonical)) > 0 {
		return true
	}
	if timerFireEventHasAgentConsumer(source, ref) {
		return true
	}
	if eventHasExternalConsumerLocal(ref.Entry) {
		return true
	}
	return ref.CrossesDeclaredOutputBoundary(source)
}

func timerFireEventHasAgentConsumer(source semanticview.Source, ref semanticview.FlowEventProof) bool {
	subscribedRefs := map[string]semanticview.FlowEventProof{}
	subscriptionPatterns := map[string]eventPatternLocal{}
	for _, required := range source.RequiredAgents() {
		for _, eventType := range required.SubscribesTo {
			addTimerAgentSubscriptionProof(source, subscribedRefs, subscriptionPatterns, "", "", eventType)
		}
	}
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		for _, required := range source.FlowRequiredAgents(flowID) {
			for _, eventType := range required.SubscribesTo {
				addTimerAgentSubscriptionProof(source, subscribedRefs, subscriptionPatterns, scope.PackageKey, flowID, eventType)
			}
		}
	}
	for agentID, agent := range source.AgentEntries() {
		agentSource, _ := source.AgentContractSource(agentID)
		flowID := strings.TrimSpace(agentSource.FlowID)
		for _, eventType := range append(append([]string{}, agent.Subscriptions...), agent.SubscribesTo...) {
			addTimerAgentSubscriptionProof(source, subscribedRefs, subscriptionPatterns, agentSource.PackageKey, flowID, eventType)
		}
	}
	return eventRefConsumedLocal(source, ref.Canonical, subscribedRefs, subscriptionPatterns)
}

func addTimerAgentSubscriptionProof(source semanticview.Source, subscribedRefs map[string]semanticview.FlowEventProof, patterns map[string]eventPatternLocal, packageKey, flowID, eventType string) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return
	}
	if strings.Contains(eventType, "*") {
		addEventPatternLocal(patterns, packageKey, flowID, eventType)
		return
	}
	addEventProofLocal(subscribedRefs, source, flowID, eventType)
}

func (c *checkerContext) timerTriggerEventProduced(timer runtimecontracts.WorkflowTimerContract, ref semanticview.FlowEventProof) bool {
	if c.source == nil {
		return false
	}
	if timerEventProducedByPlatform(c.source, ref) || eventProducedExternallyLocal(ref.Entry) {
		return true
	}
	emittedRefs := timerLifecycleEmittedEventRefs(c.source, strings.TrimSpace(timer.ID))
	return eventRefProducedLocal(c.source, ref, emittedRefs)
}

func timerEventProducedByPlatform(source semanticview.Source, ref semanticview.FlowEventProof) bool {
	if source == nil {
		return false
	}
	for _, candidate := range []string{ref.Canonical, ref.CatalogKey, ref.Authored, ref.Local} {
		if runtimecontracts.PlatformEventCatalogContains(source.PlatformSpec(), strings.TrimSpace(candidate)) {
			return true
		}
	}
	return false
}

func timerLifecycleEmittedEventRefs(source semanticview.Source, excludeTimerID string) map[string]semanticview.FlowEventProof {
	emittedRefs := map[string]semanticview.FlowEventProof{}
	if source == nil {
		return emittedRefs
	}
	if bundle, ok := semanticview.Bundle(source); ok && bundle != nil && bundle.RootSchema != nil {
		addEventProofLocal(emittedRefs, source, "", bundle.RootSchema.AutoEmitOnCreate.Event)
	}
	for _, scope := range source.FlowScopes() {
		if eventType := strings.TrimSpace(scope.AutoEmitEvent); eventType != "" {
			addEventProofLocal(emittedRefs, source, scope.ID, eventType)
		}
		for _, required := range source.FlowRequiredAgents(scope.ID) {
			for _, eventType := range required.Emits {
				addEventProofLocal(emittedRefs, source, scope.ID, eventType)
			}
		}
	}
	for _, required := range source.RequiredAgents() {
		for _, eventType := range required.Emits {
			addEventProofLocal(emittedRefs, source, "", eventType)
		}
	}
	for nodeID := range source.NodeEntries() {
		nodeSource, _ := source.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(nodeSource.FlowID)
		for _, eventType := range source.NodeHandlerSubscriptions(nodeID) {
			handler, ok := source.NodeEventHandler(nodeID, eventType)
			if !ok {
				continue
			}
			for _, emitted := range handlerEmits(handler) {
				addEventProofLocal(emittedRefs, source, flowID, emitted)
			}
		}
	}
	for _, timer := range source.WorkflowTimers() {
		if strings.TrimSpace(timer.ID) == excludeTimerID {
			continue
		}
		addEventProofLocal(emittedRefs, source, timer.FlowID, timer.Event)
	}
	for agentID, agent := range source.AgentEntries() {
		agentSource, _ := source.AgentContractSource(agentID)
		flowID := strings.TrimSpace(agentSource.FlowID)
		for _, eventType := range agent.EmitEvents {
			addEventProofLocal(emittedRefs, source, flowID, eventType)
		}
	}
	addCompositionConnectEventProofsLocal(source, emittedRefs, map[string]semanticview.FlowEventProof{})
	return emittedRefs
}

func flowStatesForTimer(source semanticview.Source, flowID string) []string {
	flowID = strings.TrimSpace(flowID)
	if source == nil {
		return nil
	}
	if flowID != "" {
		return source.FlowStates(flowID)
	}
	stages := source.WorkflowStages()
	out := make([]string, 0, len(stages))
	for _, stage := range stages {
		name := strings.TrimSpace(stage.ID)
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

func participantExistsLocal(source semanticview.Source, participant string) bool {
	participant = strings.TrimSpace(participant)
	if participant == "" || source == nil {
		return false
	}
	if participant == "runtime" || participant == "human" {
		return true
	}
	if _, ok := source.NodeEntries()[participant]; ok {
		return true
	}
	for _, agent := range source.AgentEntries() {
		if strings.TrimSpace(agent.ID) == participant || strings.TrimSpace(agent.Role) == participant {
			return true
		}
	}
	return false
}
