package bootverify

import (
	"fmt"
	"sort"
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
	// Handler declarations own guard references even without an advance.
	for _, record := range c.source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for event, handler := range c.source.ExecutableNodeEventHandlers(node) {
			location := node.Key() + ":" + event
			c.checkTransitionEventReference(node.FlowPath(), location, event)
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
	for _, requirement := range runtimeHandledEventRequirements(c.source) {
		if _, ok := c.source.ExecutableNode(requirement.owner); !ok {
			c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
				CheckID: "event_runtime_wiring_validation", Severity: "error",
				Message:  fmt.Sprintf("event %s requires missing executable node %s", requirement.eventType, executableNodeDiagnostic(requirement.owner)),
				Location: requirement.eventType,
			})
			continue
		}
		// Join lifecycle handlers are generated from the exact compiled declaration.
		// A reserved event name alone does not establish a runtime owner.
		if runtimecontracts.IsIntrinsicWorkflowRuntimeEvent(requirement.eventType) {
			ownsJoin := false
			for _, plan := range c.source.WorkflowJoins() {
				if plan.Node.Equal(requirement.owner) {
					ownsJoin = true
					break
				}
			}
			if ownsJoin {
				continue
			}
		}
		if !semanticview.ResolveExecutableNodeSubscriptionHandler(c.source, requirement.owner, requirement.eventType).Matched {
			c.eventRuntimeFindings = append(c.eventRuntimeFindings, Finding{
				CheckID: "event_runtime_wiring_validation", Severity: "error",
				Message:  fmt.Sprintf("event %s on node %s has no matching executable handler", requirement.eventType, executableNodeDiagnostic(requirement.owner)),
				Location: requirement.eventType,
			})
		}
	}
	return c.eventRuntimeFindings
}

type runtimeHandledEventRequirement struct {
	eventType string
	owner     runtimeidentity.ExecutableNode
}

// Requirements come from executable declarations and compiled transitions, never
// from event schema annotations or the existence of another node's handler.
func runtimeHandledEventRequirements(source semanticview.Source) []runtimeHandledEventRequirement {
	if source == nil {
		return nil
	}
	requirements := map[string]runtimeHandledEventRequirement{}
	add := func(owner runtimeidentity.ExecutableNode, event string) {
		if !owner.Valid() || event == "" {
			return
		}
		requirements[owner.Key()+"\x00"+event] = runtimeHandledEventRequirement{eventType: event, owner: owner}
	}
	for _, record := range source.ExecutableNodeRecords() {
		owner, err := record.Identity()
		if err != nil {
			continue
		}
		for _, event := range source.ExecutableNodeRuntimeSubscriptions(owner) {
			add(owner, event)
		}
	}
	for flowID := range source.FlowSchemaEntries() {
		topology, ok := semanticview.WorkflowStageTopology(source, flowID)
		if !ok {
			continue
		}
		for _, edge := range topology.Edges {
			if edge.Source != "timer" && edge.Source != "gate" {
				add(edge.Node, edge.HandlerEvent)
			}
		}
	}
	keys := make([]string, 0, len(requirements))
	for key := range requirements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]runtimeHandledEventRequirement, 0, len(keys))
	for _, key := range keys {
		out = append(out, requirements[key])
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
			Message:  fmt.Sprintf("event %s on node %s has no runtime executor", requirement.eventType, executableNodeDiagnostic(requirement.owner)),
			Location: requirement.eventType,
		})
	}
	return out
}
