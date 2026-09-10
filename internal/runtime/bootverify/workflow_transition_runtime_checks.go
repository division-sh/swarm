package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkTransitionReferenceValidation(c *checkerContext) []Finding { return c.transitionReferences() }
func checkTransitionOwnershipValidation(c *checkerContext) []Finding { return c.transitionOwnership() }
func checkEventRuntimeWiringValidation(c *checkerContext) []Finding  { return c.eventRuntimeWiring() }

func (c *checkerContext) transitionReferences() []Finding {
	if c.transitionRefLoaded {
		return c.transitionRefFindings
	}
	c.transitionRefLoaded = true
	// Handler declarations own action and guard references even without an advance.
	for _, record := range c.source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for event, handler := range c.source.ExecutableNodeEventHandlers(node) {
			location := node.Key() + ":" + event
			c.checkTransitionEventReference(node.FlowPath(), location, event)
			actions := []runtimecontracts.ActionSpec{handler.Action}
			for _, rule := range runtimecontracts.HandlerRuleEntries(handler) {
				actions = append(actions, rule.Action)
			}
			for _, spec := range actions {
				actionID := strings.TrimSpace(spec.ID)
				if actionID == "" {
					continue
				}
				action, ok := c.source.ActionInstructionByID(actionID)
				if !ok {
					if !isSupportedWorkflowHandlerActionID(actionID) {
						c.transitionReferenceFinding(location, "references unknown action %s", actionID)
					}
					continue
				}
				if emits := strings.TrimSpace(action.Emits); emits != "" && !flowEventExists(c.source, node.FlowPath(), emits) {
					c.transitionReferenceFinding(location, "action %s emits missing event %s", actionID, emits)
				}
			}
			for _, check := range handler.Guard.EffectiveChecks() {
				// Inline expressions are executable declarations, not registry references.
				if strings.TrimSpace(check.Check) != "" || strings.TrimSpace(check.ID) == "" {
					continue
				}
				if _, ok := c.source.GuardInstructionByID(check.ID); !ok {
					c.transitionReferenceFinding(location, "references unknown guard %s", check.ID)
				}
			}
		}
	}
	for _, flow := range lifecycleFlowSchemas(c.source) {
		topology, ok := semanticview.WorkflowStageTopology(c.source, flow.flowID)
		if !ok {
			continue
		}
		for _, edge := range topology.Edges {
			location := fmt.Sprintf("%s:%s:%s->%s", flow.flowID, edge.Source, edge.From, edge.To)
			switch edge.Source {
			case "timer":
				timer, ok := c.source.WorkflowStageTimerByID(flow.flowID, edge.TimerID)
				if !ok || !timer.StageOwned || timer.Stage != edge.From || timer.AdvancesTo != edge.To {
					c.transitionReferenceFinding(location, "references unknown or mismatched stage timer %s", edge.TimerID)
				}
			case "gate":
				gate, ok := c.source.WorkflowGateForStage(flow.flowID, edge.From)
				outcome, verdictOK := gate.Outcomes[edge.Verdict]
				if !ok || gate.Decision != edge.DecisionID || !verdictOK || outcome.AdvancesTo != edge.To {
					c.transitionReferenceFinding(location, "references unknown or mismatched gate %s verdict %s", edge.DecisionID, edge.Verdict)
				}
			default:
				c.checkTransitionEventReference(flow.flowID, location, edge.EventType)
				if edge.HandlerEvent != edge.EventType {
					c.checkTransitionEventReference(flow.flowID, location, edge.HandlerEvent)
				}
			}
		}
	}
	for flowID := range c.source.FlowSchemaEntries() {
		for _, eventType := range c.source.FlowInputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType != "" && !flowEventExists(c.source, flowID, eventType) {
				c.transitionRefFindings = append(c.transitionRefFindings, Finding{
					CheckID:  "transition_reference_validation",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s input event %s missing from event catalog", flowID, eventType),
					Location: strings.TrimSpace(flowID),
				})
			}
		}
		for _, eventType := range c.source.FlowOutputEvents(flowID) {
			eventType = strings.TrimSpace(eventType)
			if eventType != "" && !flowEventExists(c.source, flowID, eventType) {
				c.transitionRefFindings = append(c.transitionRefFindings, Finding{
					CheckID:  "transition_reference_validation",
					Severity: "error",
					Message:  fmt.Sprintf("flow %s output event %s missing from event catalog", flowID, eventType),
					Location: strings.TrimSpace(flowID),
				})
			}
		}
	}
	return c.transitionRefFindings
}

func flowEventExists(source semanticview.Source, flowID, eventType string) bool {
	if source == nil {
		return false
	}
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if runtimecontracts.IsIntrinsicWorkflowRuntimeEvent(eventType) {
		return true
	}
	if !strings.Contains(eventType, "*") {
		_, _, ok := source.ResolveFlowEventCatalogEntry(flowID, eventType)
		return ok
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return false
	}
	for candidate := range scope.Events {
		if source.FlowEventMatches(flowID, eventType, candidate) {
			return true
		}
	}
	return false
}

func (c *checkerContext) transitionReferenceFinding(location, format string, args ...any) {
	c.transitionRefFindings = append(c.transitionRefFindings, Finding{
		CheckID: "transition_reference_validation", Severity: "error",
		Message: "transition " + location + " " + fmt.Sprintf(format, args...), Location: location,
	})
}

func (c *checkerContext) checkTransitionEventReference(flowID, location, event string) {
	if strings.TrimSpace(event) == "" {
		c.transitionReferenceFinding(location, "missing trigger")
	} else if !flowEventExists(c.source, flowID, event) {
		c.transitionReferenceFinding(location, "trigger %s missing from event catalog", event)
	}
}

func (c *checkerContext) transitionOwnership() []Finding {
	if c.transitionOwnerLoaded {
		return c.transitionOwnerFindings
	}
	c.transitionOwnerLoaded = true
	for _, flow := range lifecycleFlowSchemas(c.source) {
		topology, ok := semanticview.WorkflowStageTopology(c.source, flow.flowID)
		if !ok {
			continue
		}
		for _, edge := range topology.Edges {
			location := fmt.Sprintf("%s:%s:%s->%s", flow.flowID, edge.Source, edge.From, edge.To)
			var problem string
			// Admission owns carrier shape/protocol validation, not declaration existence.
			compiled, admissionErr := topology.AdmitTransition(edge.Site(), edge.From, edge.To)
			switch {
			case topology.FlowID != flow.flowID:
				problem = "compiled topology belongs to another flow"
			case admissionErr != nil:
				problem = fmt.Sprintf("invalid compiled carrier: %v", admissionErr)
			case compiled.Edge() != edge:
				problem = "compiled carrier differs from admitted evidence"
			case edge.Source == "timer" || edge.Source == "gate":
				// Runtime carriers have no executable handler declaration.
			default:
				if _, ok := c.source.ExecutableNode(edge.Node); !ok {
					problem = "compiled carrier executable owner is missing"
				} else if handler, ok := c.source.ExecutableNodeEventHandler(edge.Node, edge.HandlerEvent); !ok {
					problem = fmt.Sprintf("workflow owner is %s but originating handler %s is missing", executableNodeDiagnostic(edge.Node), edge.HandlerEvent)
				} else if edge.Source == "loop.escape" {
					if !compiledLoopEscapeOwnsEdge(c.source, flow.flowID, edge) {
						problem = "compiled loop escape does not belong to its originating repeat operation"
					}
				} else {
					matched := false
					for _, carrier := range runtimecontracts.HandlerAdvanceCarriers(handler) {
						if carrier.Kind == edge.AdvanceCarrier && carrier.RuleRef == edge.RuleRef && carrier.AdvancesTo == edge.To {
							matched = true
							break
						}
					}
					if !matched {
						problem = "compiled advance carrier does not belong to its originating handler"
					}
				}
			}
			if problem != "" {
				c.transitionOwnerFindings = append(c.transitionOwnerFindings, Finding{
					CheckID: "transition_ownership_validation", Severity: "error",
					Message: "transition " + location + " " + problem, Location: location,
				})
			}
		}
	}
	return c.transitionOwnerFindings
}

func compiledLoopEscapeOwnsEdge(source semanticview.Source, flowID string, edge runtimecontracts.WorkflowStageTopologyEdge) bool {
	for _, plan := range semanticview.WorkflowLoops(source) {
		if plan.FlowID != flowID || plan.ID != edge.LoopID || plan.Escape.AdvancesTo != edge.To {
			continue
		}
		for _, operation := range plan.Operations {
			if operation.Kind == runtimecontracts.LoopOperationRepeat && edge.LoopOperation == operation.Kind &&
				operation.Node.Equal(edge.Node) && operation.HandlerEvent == edge.HandlerEvent && operation.From == edge.From {
				return true
			}
		}
	}
	return false
}

func (c *checkerContext) eventRuntimeWiring() []Finding {
	if c.eventRuntimeLoaded {
		return c.eventRuntimeFindings
	}
	c.eventRuntimeLoaded = true
	census := semanticview.BuildAuthoredEventEndpointCensus(c.source)
	for _, requirement := range runtimeHandledEventRequirements(c.source) {
		if !requirement.owner.Valid() {
			c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
				CheckID:  "event_runtime_wiring_validation",
				Severity: "error",
				Message:  fmt.Sprintf("event %s with runtime_handling=%s missing exact owning_node", requirement.eventType, requirement.handling),
				Location: requirement.eventType,
			})
			continue
		}
		if _, ok := c.source.ExecutableNode(requirement.owner); !ok {
			c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
				CheckID:  "event_runtime_wiring_validation",
				Severity: "error",
				Message:  fmt.Sprintf("event %s owning_node %s missing from system nodes", requirement.eventType, executableNodeDiagnostic(requirement.owner)),
				Location: requirement.eventType,
			})
			continue
		}
		if handlers := c.source.ExecutableNodeEventHandlers(requirement.owner); len(handlers) > 0 {
			matched := false
			for _, endpoint := range census.MatchingConsumers(requirement.owner.FlowPath(), requirement.eventType) {
				if endpoint.Kind == semanticview.EventEndpointNodeHandler && endpoint.Node.Equal(requirement.owner) {
					matched = true
					break
				}
			}
			if !matched {
				c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
					CheckID:  "event_runtime_wiring_validation",
					Severity: "error",
					Message:  fmt.Sprintf("event %s owning_node %s missing semantic event_handler", requirement.eventType, executableNodeDiagnostic(requirement.owner)),
					Location: requirement.eventType,
				})
			}
		}
	}
	return c.eventRuntimeFindings
}

type runtimeHandledEventRequirement struct {
	eventType string
	handling  string
	owner     runtimeidentity.ExecutableNode
}

func runtimeHandledEventRequirements(source semanticview.Source) []runtimeHandledEventRequirement {
	if source == nil || !contractBundleUsesOwningNodeModel(source) {
		return nil
	}
	out := make([]runtimeHandledEventRequirement, 0)
	appendEntries := func(flowID string, entries map[string]runtimecontracts.EventCatalogEntry) {
		for eventType, entry := range entries {
			eventType = strings.TrimSpace(eventType)
			handling := strings.TrimSpace(entry.RuntimeHandling)
			if eventType == "" || !requiresOwningNode(handling) {
				continue
			}
			owner, _ := runtimeidentity.AdmitExecutableNodeDeclaration(flowID, entry.OwningNode)
			out = append(out, runtimeHandledEventRequirement{
				eventType: eventType,
				handling:  handling,
				owner:     owner,
			})
		}
	}
	for _, scope := range source.FlowScopes() {
		appendEntries(scope.ID, scope.Events)
	}
	return out
}

func runtimeHandledEventsMissingExecutors(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	runtimeExecutors := supportedWorkflowRuntimeExecutorIDs(source)
	out := make([]Finding, 0)
	for _, requirement := range runtimeHandledEventRequirements(source) {
		if !requirement.owner.Valid() {
			continue
		}
		if _, ok := source.ExecutableNode(requirement.owner); !ok {
			continue
		}
		if _, ok := runtimeExecutors[requirement.owner.Key()]; ok {
			continue
		}
		out = append(out, Finding{
			CheckID:  "handler_field_compliance",
			Severity: "error",
			Message:  fmt.Sprintf("event %s owning_node %s has no runtime executor", requirement.eventType, executableNodeDiagnostic(requirement.owner)),
			Location: requirement.eventType,
		})
	}
	return out
}

func requiresOwningNode(runtimeHandling string) bool {
	switch strings.TrimSpace(runtimeHandling) {
	case "consuming", "dual_delivery", "projection", "stage_projection":
		return true
	default:
		return false
	}
}

func contractBundleUsesOwningNodeModel(source semanticview.Source) bool {
	if source == nil {
		return false
	}
	for _, scope := range source.FlowScopes() {
		for _, entry := range scope.Events {
			if strings.TrimSpace(entry.OwningNode) != "" {
				return true
			}
		}
	}
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err == nil && len(source.ExecutableNodeEventHandlers(node)) > 0 {
			return true
		}
	}
	return false
}
