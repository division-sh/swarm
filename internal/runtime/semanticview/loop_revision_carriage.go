package semanticview

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

// OriginalLoopCarriage is detached declaration evidence from an admitted source
// artifact. It cannot acquire meaning from a selected replacement contract.
type OriginalLoopCarriage struct{ state *originalLoopCarriage }

type originalLoopCarriage struct {
	bundleHash string
	rootFlow   string
	inputs     map[loopInputScope]LoopRevisionRole
	outputs    map[loopOutputScope][]loopOutputMeaning
	activities map[loopActivityScope]LoopRevisionRole
	fanOuts    map[contracts.FanOutPlanRef]loopFanOutMeaning
}

type loopInputScope struct{ flow, event string }
type loopOutputScope struct {
	node           identity.ExecutableNode
	handler, event string
}
type loopOutputMeaning struct {
	role LoopRevisionRole
}
type loopActivityScope struct {
	node                    identity.ExecutableNode
	handler, activity, tool string
}

type loopFanOutMeaning struct {
	node    identity.ExecutableNode
	handler string
	role    LoopRevisionRole
}

// LoopRevisionRole proves a field's declared role, not the validity of its value
// or permission to execute a generation. Those belong to the activation owner.
type LoopRevisionRole struct{ flow, loop, field string }

func (r LoopRevisionRole) FlowID() string        { return r.flow }
func (r LoopRevisionRole) LoopID() string        { return r.loop }
func (r LoopRevisionRole) RevisionField() string { return r.field }

func (r LoopRevisionRole) AdmitRevision(c *loopruntime.ForkCorrespondence, revision string) (loopruntime.ForkSourceReference, error) {
	if r == (LoopRevisionRole{}) {
		return loopruntime.ForkSourceReference{}, fmt.Errorf("source revision has no original declaration role")
	}
	return c.AdmitSourceRevision(r.flow, r.loop, r.field, revision)
}

// LoopEventScope must come from the original event/producer at the fork revision.
// A missing node is normal for external and agent inputs; it is not a request to
// guess a node from the event's name.
type LoopEventScope struct {
	FlowID       string
	EventType    string
	Producer     identity.ExecutableNode
	HandlerEvent string
}

func CompileOriginalLoopCarriage(source Source) (OriginalLoopCarriage, error) {
	bundle, ok := Bundle(source)
	if !ok || bundle.SourceArtifact == nil || bundle.SourceArtifact.BundleHash() == "" {
		return OriginalLoopCarriage{}, fmt.Errorf("original loop carriage requires an admitted source artifact")
	}
	state, err := compileLoopCarriage(source)
	if err != nil {
		return OriginalLoopCarriage{}, err
	}
	state.bundleHash = bundle.SourceArtifact.BundleHash()
	return OriginalLoopCarriage{state: state}, nil
}

func (c OriginalLoopCarriage) RequireSource(bundleHash string) error {
	if c.state == nil || c.state.bundleHash == "" || bundleHash != c.state.bundleHash {
		return fmt.Errorf("loop carriage does not belong to the exact original source artifact")
	}
	return nil
}

func (c OriginalLoopCarriage) Resolve(scope LoopEventScope) (LoopRevisionRole, bool, error) {
	if c.state == nil || c.state.bundleHash == "" {
		return LoopRevisionRole{}, false, fmt.Errorf("original loop carriage is not admitted")
	}
	return c.state.resolve(scope)
}

// Routing sources and declaration scopes use different root projections. Bind
// that projection once from the original artifact, rather than from run names.
func (c OriginalLoopCarriage) ResolveEvent(source events.RoutingSource, eventType string, producer identity.ExecutableNode, handlerEvent string) (LoopRevisionRole, bool, error) {
	flow, err := c.ExecutionFlow(source)
	if err != nil {
		return LoopRevisionRole{}, false, err
	}
	return c.Resolve(LoopEventScope{FlowID: flow, EventType: eventType, Producer: producer, HandlerEvent: handlerEvent})
}

func (c OriginalLoopCarriage) ExecutionFlow(source events.RoutingSource) (string, error) {
	if c.state == nil || c.state.bundleHash == "" {
		return "", fmt.Errorf("original loop carriage is not admitted")
	}
	flow := source.Route().FlowID
	if source.Kind() == events.RoutingSourceRoot {
		flow = c.state.rootFlow
		if flow == "" {
			return "", fmt.Errorf("original root declaration is absent")
		}
	}
	return flow, nil
}

func (c OriginalLoopCarriage) ResolveActivity(node identity.ExecutableNode, handler, activity, tool string) (LoopRevisionRole, bool, error) {
	if c.state == nil || c.state.bundleHash == "" {
		return LoopRevisionRole{}, false, fmt.Errorf("original activity carriage is not admitted")
	}
	role, exists := c.state.activities[loopActivityScope{node, handler, activity, tool}]
	if !exists {
		return LoopRevisionRole{}, false, fmt.Errorf("activity has no exact original declaration")
	}
	return role, role != (LoopRevisionRole{}), nil
}

func (c OriginalLoopCarriage) ResolveFanOut(ref contracts.FanOutPlanRef, node identity.ExecutableNode, handler string) (LoopRevisionRole, bool, error) {
	if c.state == nil || c.state.bundleHash == "" || ref.BundleHash != c.state.bundleHash {
		return LoopRevisionRole{}, false, fmt.Errorf("fan-out carriage requires its exact original artifact")
	}
	meaning, found := c.state.fanOuts[ref]
	if !found || meaning.node != node || meaning.handler != handler {
		return LoopRevisionRole{}, false, fmt.Errorf("fan-out carriage has no exact original compiled declaration")
	}
	return meaning.role, meaning.role != (LoopRevisionRole{}), nil
}

func (c *originalLoopCarriage) resolve(scope LoopEventScope) (LoopRevisionRole, bool, error) {
	if scope.FlowID != strings.TrimSpace(scope.FlowID) || scope.EventType == "" || scope.EventType != strings.TrimSpace(scope.EventType) || scope.HandlerEvent != strings.TrimSpace(scope.HandlerEvent) {
		return LoopRevisionRole{}, false, fmt.Errorf("original loop event scope is not canonical")
	}
	role, found := c.inputs[loopInputScope{scope.FlowID, scope.EventType}]
	if !scope.Producer.Valid() {
		if scope.Producer != (identity.ExecutableNode{}) || scope.HandlerEvent != "" {
			return LoopRevisionRole{}, false, fmt.Errorf("original loop producer scope is incomplete")
		}
		return role, found, nil
	}
	if scope.Producer.FlowPath() != scope.FlowID || scope.HandlerEvent == "" {
		return LoopRevisionRole{}, false, fmt.Errorf("original loop producer disagrees with its flow or handler")
	}
	meanings := c.outputs[loopOutputScope{scope.Producer, scope.HandlerEvent, scope.EventType}]
	for _, meaning := range meanings {
		if meaning.role == (LoopRevisionRole{}) {
			if found || hasLoopMeaning(meanings) {
				return LoopRevisionRole{}, false, fmt.Errorf("original event has conflicting business and loop carriage")
			}
			continue
		}
		if found && role != meaning.role {
			return LoopRevisionRole{}, false, fmt.Errorf("original event has conflicting loop carriage")
		}
		role, found = meaning.role, true
	}
	return role, found, nil
}

func hasLoopMeaning(meanings []loopOutputMeaning) bool {
	for _, meaning := range meanings {
		if meaning.role != (LoopRevisionRole{}) {
			return true
		}
	}
	return false
}

func compileLoopCarriage(source Source) (*originalLoopCarriage, error) {
	if source == nil {
		return nil, fmt.Errorf("loop carriage requires a semantic source")
	}
	state := &originalLoopCarriage{rootFlow: RootExecutionFlowID(source), inputs: map[loopInputScope]LoopRevisionRole{}, outputs: map[loopOutputScope][]loopOutputMeaning{}, activities: map[loopActivityScope]LoopRevisionRole{}, fanOuts: map[contracts.FanOutPlanRef]loopFanOutMeaning{}}
	type operationScope struct {
		node    identity.ExecutableNode
		handler string
	}
	operations := map[operationScope]LoopRevisionRole{}
	plans := WorkflowLoops(source)
	for _, plan := range plans {
		role := LoopRevisionRole{plan.FlowID, plan.ID, plan.RevisionField}
		if role.loop == "" || role.field == "" || role.flow != strings.TrimSpace(role.flow) || role.loop != strings.TrimSpace(role.loop) || role.field != strings.TrimSpace(role.field) {
			return nil, fmt.Errorf("loop carriage declaration identity is incomplete")
		}
		for _, operation := range plan.Operations {
			if !operation.Node.Valid() || operation.Node.FlowPath() != plan.FlowID || operation.LoopID != plan.ID {
				return nil, fmt.Errorf("loop carriage operation contradicts its declaration owner")
			}
			input := ResolveFlowEventProof(source, plan.FlowID, operation.HandlerEvent)
			key := operationScope{operation.Node, input.Canonical}
			if _, exists := operations[key]; exists {
				return nil, fmt.Errorf("duplicate loop carriage operation")
			}
			operations[key] = role
			if operation.Kind != contracts.LoopOperationStart {
				if !input.HasSchema || !contracts.RequiresLoopRevision(input.Entry.Payload, role.field) {
					return nil, fmt.Errorf("original loop input %s must require text field %s", input.Canonical, role.field)
				}
				inputKey := loopInputScope{plan.FlowID, input.Canonical}
				if prior, exists := state.inputs[inputKey]; exists && prior != role {
					return nil, fmt.Errorf("original loop input has conflicting declaration roles")
				}
				state.inputs[inputKey] = role
			}
			if operation.Kind == contracts.LoopOperationRepeat && !plan.Escape.Emit.Empty() {
				if err := state.addOutput(source, operation.Node, input.Canonical, plan.Escape.Emit, role); err != nil {
					return nil, err
				}
			}
		}
	}
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			return nil, fmt.Errorf("original producer has no declaration identity")
		}
		handlers := source.ExecutableNodeEventHandlers(node)
		for _, site := range contracts.ActivitySitesForNode(node, handlers) {
			events := contracts.ActivityResultEventsForSite(site)
			handler := ResolveFlowEventProof(source, node.FlowPath(), site.HandlerEventKey).Canonical
			role := operations[operationScope{node, handler}]
			key := loopActivityScope{node, site.HandlerEventKey, events.ActivityID, site.Spec.Tool}
			if prior, exists := state.activities[key]; exists && prior != role {
				return nil, fmt.Errorf("activity has conflicting original loop roles")
			}
			state.activities[key] = role
			// Result carriage is generated by the canonical activity projection,
			// not by an authored emit expression.
			resultEvents := []string{events.SuccessEvent, events.FailureEvent}
			if site.Spec.Approval != nil {
				resultEvents = append(resultEvents, events.RevisionRequested, events.Rejected)
			}
			for _, event := range resultEvents {
				outputKey := loopOutputScope{node, handler, event}
				state.outputs[outputKey] = append(state.outputs[outputKey], loopOutputMeaning{role: role})
			}
		}
		keys := make([]string, 0, len(handlers))
		for event := range handlers {
			keys = append(keys, event)
		}
		sort.Strings(keys)
		for _, event := range keys {
			handlerEvent := ResolveFlowEventProof(source, node.FlowPath(), event).Canonical
			role := operations[operationScope{node, handlerEvent}]
			for _, plan := range source.FanOutPlansForHandler(node, event) {
				meaning := loopFanOutMeaning{node: node, handler: event, role: role}
				if previous, exists := state.fanOuts[plan.Ref]; exists && previous != meaning {
					return nil, fmt.Errorf("fan-out has conflicting original loop roles")
				}
				state.fanOuts[plan.Ref] = meaning
			}
			for _, site := range contracts.HandlerDeclarativeEmitSites(handlers[event]) {
				if err := state.addOutput(source, node, handlerEvent, site.Spec, role); err != nil {
					return nil, err
				}
			}
		}
	}
	return state, nil
}

func (c *originalLoopCarriage) addOutput(source Source, node identity.ExecutableNode, handler string, spec contracts.EmitSpec, role LoopRevisionRole) error {
	proof := ResolveFlowEventProof(source, node.FlowPath(), spec.EventType())
	if role != (LoopRevisionRole{}) {
		value, present := spec.Fields[role.field]
		if !proof.HasSchema || !contracts.RequiresLoopRevision(proof.Entry.Payload, role.field) || !present || !contracts.CarriesLoopRevision(value) {
			return fmt.Errorf("original loop output %s must carry declared text field %s from loop.revision_id", proof.Canonical, role.field)
		}
	}
	key := loopOutputScope{node, handler, proof.Canonical}
	c.outputs[key] = append(c.outputs[key], loopOutputMeaning{role: role})
	return nil
}
