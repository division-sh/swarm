package contracts

import (
	"fmt"
	"sort"
	"strings"
)

func populateWorkflowSemantics(bundle *WorkflowContractBundle) {
	if bundle == nil {
		return
	}
	name := strings.TrimSpace(bundle.Package.Name)
	version := strings.TrimSpace(bundle.Package.Version)
	entitySchema := legacyWorkflowEntitySchema(bundle)
	semantics := WorkflowSemanticView{
		Name:                   name,
		Version:                version,
		InitialStage:           rootSchemaInitialStage(bundle.RootSchema),
		EntitySchema:           entitySchema,
		Stages:                 deriveWorkflowStages(bundle.RootSchema, bundle.Paths.Flows, bundle.FlowSchemas),
		TerminalStages:         deriveWorkflowTerminalStages(bundle.RootSchema, bundle.Paths.Flows, bundle.FlowSchemas),
		Transitions:            deriveStageTimerTransitions(bundle),
		Timers:                 deriveWorkflowSemanticTimers(bundle),
		Guards:                 deriveWorkflowGuardEntries(bundle),
		Actions:                deriveWorkflowActionEntries(bundle),
		GuardByID:              map[string]GuardActionEntry{},
		ActionByID:             map[string]GuardActionEntry{},
		FlowInitial:            map[string]string{},
		FlowStates:             map[string][]string{},
		FlowTerminal:           map[string][]string{},
		FlowNamespace:          map[string]string{},
		FlowPrefix:             map[string]string{},
		FlowRules:              map[string]string{},
		FlowInputs:             map[string][]string{},
		FlowOutputs:            map[string][]string{},
		FlowInputEventPins:     map[string][]FlowInputEventPin{},
		FlowOutputEventPins:    map[string][]FlowOutputEventPin{},
		FlowReads:              map[string][]string{},
		FlowWrites:             map[string][]string{},
		CompositionConnects:    nil,
		FlowAgents:             map[string][]FlowRequiredAgent{},
		RootAgentFacts:         nil,
		FlowAgentFacts:         map[string][]RequiredAgentFact{},
		WritePinOwners:         map[string][]string{},
		EffectiveNodes:         map[string]SystemNodeEffectiveSemantics{},
		NodeHandlers:           map[string]map[string]SystemNodeEventHandler{},
		EventOwners:            map[string][]string{},
		HandlerTransitionIndex: map[string]map[string]HandlerTransitionSemantic{},
	}
	semantics.Guards = appendPlatformBuiltinGuardEntries(semantics.Guards, bundle.Platform.BuiltinHooks.Guards)
	semantics.Actions = appendPlatformBuiltinActionEntries(semantics.Actions, bundle.Platform.BuiltinHooks.Actions)
	semantics.RootAgentFacts = bundle.RootRequiredAgentFacts()
	for _, entry := range semantics.Guards {
		if id := strings.TrimSpace(entry.ID); id != "" {
			semantics.GuardByID[id] = entry
		}
	}
	for _, entry := range semantics.Actions {
		if id := strings.TrimSpace(entry.ID); id != "" {
			semantics.ActionByID[id] = entry
		}
	}
	for flowID, schema := range bundle.FlowSchemas {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		semantics.FlowInitial[flowID] = schema.LoweredInitialState()
		semantics.FlowStates[flowID] = schema.LoweredStates()
		semantics.FlowTerminal[flowID] = schema.LoweredTerminalStates()
		assignedNamespace := strings.TrimSpace(flowAssignedNamespace(bundle.Paths.Flows, flowID))
		if assignedNamespace == "" {
			assignedNamespace = strings.TrimSpace(schema.NamespacePrefix)
		}
		semantics.FlowNamespace[flowID] = assignedNamespace
		semantics.FlowPrefix[flowID] = strings.TrimSpace(schema.NamespacePrefix)
		semantics.FlowRules[flowID] = strings.TrimSpace(schema.NamespaceRule)
		semantics.FlowInputs[flowID] = append([]string{}, schema.Pins.Inputs.Events...)
		semantics.FlowOutputs[flowID] = append([]string{}, schema.Pins.Outputs.Events...)
		semantics.FlowInputEventPins[flowID] = semanticInputEventPins(schema.Pins.Inputs)
		semantics.FlowOutputEventPins[flowID] = semanticOutputEventPins(schema.Pins.Outputs)
		semantics.FlowReads[flowID] = append([]string{}, schema.Pins.Inputs.Reads...)
		semantics.FlowWrites[flowID] = append([]string{}, schema.Pins.Outputs.Writes...)
		facts := bundle.FlowRequiredAgentFacts(flowID)
		semantics.FlowAgentFacts[flowID] = facts
		semantics.FlowAgents[flowID] = FlowRequiredAgentsFromFacts(facts)
		for _, writePin := range schema.Pins.Outputs.Writes {
			writePin = strings.TrimSpace(writePin)
			if writePin == "" {
				continue
			}
			semantics.WritePinOwners[writePin] = appendIfMissingString(semantics.WritePinOwners[writePin], flowID)
		}
	}
	for _, pkg := range bundle.PackageTree {
		for _, connect := range pkg.Manifest.Connect {
			semantics.CompositionConnects = append(semantics.CompositionConnects, connect.WithPackageKey(pkg.Key))
		}
	}
	for nodeID, node := range bundle.Nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		effective := SystemNodeEffectiveSemantics{
			ID:                   EffectiveSystemNodeID(nodeID, node),
			ExecutionType:        EffectiveSystemNodeExecutionType(node),
			RuntimeSubscriptions: EffectiveSystemNodeSubscriptions(node),
			Produces:             EffectiveSystemNodeProduces(node),
		}
		semantics.EffectiveNodes[nodeID] = effective
		if strings.TrimSpace(node.ID) == "" || strings.TrimSpace(node.ExecutionType) == "" {
			node.ID = effective.ID
			node.ExecutionType = effective.ExecutionType
			bundle.Nodes[nodeID] = node
		}
		if len(node.EventHandlers) == 0 {
			continue
		}
		handlers := make(map[string]SystemNodeEventHandler, len(node.EventHandlers))
		source, _ := bundle.NodeContractSource(nodeID)
		for eventType, handler := range node.EventHandlers {
			rawEventType := strings.TrimSpace(eventType)
			if rawEventType == "" {
				continue
			}
			handler = DefaultSystemNodeHandlerSourceEvent(handler, rawEventType)
			handlers[rawEventType] = handler
			ownerEventType := strings.TrimSpace(bundle.resolveNodeEventOwnerPattern(nodeID, rawEventType))
			if ownerEventType == "" {
				ownerEventType = rawEventType
			}
			semantics.EventOwners[ownerEventType] = appendIfMissingString(semantics.EventOwners[ownerEventType], nodeID)
			transition := HandlerTransitionSemantic{
				ID:                   fmt.Sprintf("%s:%s", nodeID, rawEventType),
				NodeID:               nodeID,
				FlowID:               strings.TrimSpace(source.FlowID),
				EventType:            rawEventType,
				Action:               handler.Action,
				SelectEntity:         handler.SelectEntity,
				SelectOrCreateEntity: handler.SelectOrCreateEntity,
				Guard:                handler.Guard,
				AdvancesTo:           strings.TrimSpace(handler.AdvancesTo),
				SetsGate:             handler.SetsGate,
				ClearGates:           handler.ClearGates,
				DataAccumulation:     handler.DataAccumulation,
				Emit:                 cloneEmitSpec(handler.Emit),
				OnSuccess:            HandlerOnSuccessSpec{Emit: cloneEmitSpec(handler.OnSuccess.Emit)},
				Condition:            strings.TrimSpace(handler.Condition),
				CompletionRule:       strings.TrimSpace(handler.CompletionRule),
				OnComplete:           handler.OnComplete,
				Rules:                handler.Rules,
				Accumulate:           handler.Accumulate,
				Compute:              handler.Compute,
				Query:                handler.Query,
				FanOut:               handler.FanOut,
				GroupBy:              handler.GroupBy,
				Filter:               handler.Filter,
				Reduce:               handler.Reduce,
				Count:                handler.Count,
				Clear:                handler.Clear,
			}
			semantics.HandlerTransitions = append(semantics.HandlerTransitions, transition)
			if derivedTransition, ok := deriveWorkflowTransitionContract(transition); ok {
				semantics.Transitions = append(semantics.Transitions, derivedTransition)
			}
			semantics.Transitions = append(semantics.Transitions, deriveRuleTransitions(transition)...)
			if timeoutTransition, ok := deriveAccumulateTimeoutTransition(transition); ok {
				semantics.Transitions = append(semantics.Transitions, timeoutTransition)
			}
			if semantics.HandlerTransitionIndex[nodeID] == nil {
				semantics.HandlerTransitionIndex[nodeID] = map[string]HandlerTransitionSemantic{}
			}
			semantics.HandlerTransitionIndex[nodeID][rawEventType] = transition
		}
		semantics.NodeHandlers[nodeID] = handlers
	}
	bundle.Semantics = semantics
}

func semanticInputEventPins(pins FlowInputPins) []FlowInputEventPin {
	if len(pins.EventPins) > 0 {
		return cloneFlowInputEventPins(pins.EventPins)
	}
	return inputEventPinsFromEvents(pins.Events)
}

func semanticOutputEventPins(pins FlowOutputPins) []FlowOutputEventPin {
	if len(pins.EventPins) > 0 {
		return cloneFlowOutputEventPins(pins.EventPins)
	}
	return outputEventPinsFromEvents(pins.Events)
}

func legacyWorkflowEntitySchema(bundle *WorkflowContractBundle) EntitySchema {
	if bundle == nil {
		return EntitySchema{}
	}
	if !bundle.Package.EntitySchema.Empty() {
		return bundle.Package.EntitySchema
	}
	if len(bundle.RootEntities) > 0 {
		if entityType, entity, ok := bundle.RootPrimaryEntityContract(); ok {
			return entityContractsToLegacyEntitySchema(EntityContractsDocument{entityType: entity})
		}
		return EntitySchema{}
	}
	if len(bundle.flowEntities) == 1 {
		for flowID := range bundle.flowEntities {
			if entityType, entity, ok := bundle.FlowPrimaryEntityContract(flowID); ok {
				return entityContractsToLegacyEntitySchema(EntityContractsDocument{entityType: entity})
			}
		}
	}
	return EntitySchema{}
}

func entityContractsToLegacyEntitySchema(entities EntityContractsDocument) EntitySchema {
	if len(entities) == 0 {
		return EntitySchema{}
	}
	groups := make([]EntitySchemaGroup, 0, len(entities))
	for entityType, contract := range entities {
		group := EntitySchemaGroup{
			Name:   strings.TrimSpace(entityType),
			Fields: make([]EntitySchemaField, 0, len(contract.Fields)),
		}
		for fieldName, field := range contract.Fields {
			group.Fields = append(group.Fields, EntitySchemaField{
				Name:        strings.TrimSpace(fieldName),
				Type:        strings.TrimSpace(field.Type),
				Initial:     field.Initial,
				Indexed:     field.Indexed,
				Description: field.Description,
			})
		}
		sort.Slice(group.Fields, func(i, j int) bool {
			return strings.TrimSpace(group.Fields[i].Name) < strings.TrimSpace(group.Fields[j].Name)
		})
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool {
		return strings.TrimSpace(groups[i].Name) < strings.TrimSpace(groups[j].Name)
	})
	return EntitySchema{Groups: groups}
}
func deriveWorkflowGuardEntries(bundle *WorkflowContractBundle) []GuardActionEntry {
	if bundle == nil {
		return nil
	}
	seen := map[string]GuardActionEntry{}
	for _, nodeID := range sortedContractKeys(bundle.Nodes) {
		node := bundle.Nodes[nodeID]
		for _, eventType := range sortedContractKeys(node.EventHandlers) {
			handler := node.EventHandlers[eventType]
			if handler.Guard == nil {
				continue
			}
			id := strings.TrimSpace(handler.Guard.ID)
			if id == "" {
				continue
			}
			seen[id] = GuardActionEntry{
				ID:        id,
				Check:     strings.TrimSpace(handler.Guard.Check),
				PolicyRef: strings.TrimSpace(handler.Guard.PolicyRef),
			}
		}
	}
	return sortedGuardActionEntries(seen)
}
func deriveWorkflowActionEntries(bundle *WorkflowContractBundle) []GuardActionEntry {
	if bundle == nil {
		return nil
	}
	seen := map[string]GuardActionEntry{}
	for _, nodeID := range sortedContractKeys(bundle.Nodes) {
		node := bundle.Nodes[nodeID]
		for _, eventType := range sortedContractKeys(node.EventHandlers) {
			handler := node.EventHandlers[eventType]
			if id := strings.TrimSpace(handler.Action.ID); id != "" {
				seen[id] = GuardActionEntry{ID: id}
			}
			for _, rule := range handler.Rules {
				if id := strings.TrimSpace(rule.Action.ID); id != "" {
					seen[id] = GuardActionEntry{ID: id}
				}
			}
		}
	}
	return sortedGuardActionEntries(seen)
}
func sortedGuardActionEntries(entries map[string]GuardActionEntry) []GuardActionEntry {
	if len(entries) == 0 {
		return nil
	}
	ids := make([]string, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]GuardActionEntry, 0, len(ids))
	for _, id := range ids {
		out = append(out, entries[id])
	}
	return out
}
func deriveWorkflowTransitionContract(transition HandlerTransitionSemantic) (WorkflowTransitionContract, bool) {
	to := handlerLevelAdvanceTarget(transition)
	if to == "" {
		return WorkflowTransitionContract{}, false
	}
	out := WorkflowTransitionContract{
		ID:               strings.TrimSpace(transition.ID),
		From:             []string{"*"},
		To:               to,
		Trigger:          strings.TrimSpace(transition.EventType),
		Node:             strings.TrimSpace(transition.NodeID),
		DataAccumulation: transition.DataAccumulation,
	}
	if guardID := strings.TrimSpace(firstTransitionGuardID(transition.Guard)); guardID != "" {
		out.Guards = []string{guardID}
	}
	if actionID := strings.TrimSpace(transition.Action.ID); actionID != "" {
		out.Actions = []string{actionID}
	}
	return out, strings.TrimSpace(out.ID) != "" && strings.TrimSpace(out.Trigger) != ""
}

func handlerLevelAdvanceTarget(transition HandlerTransitionSemantic) string {
	for _, carrier := range HandlerTransitionAdvanceCarriers(transition) {
		if carrier.Kind == HandlerAdvanceCarrierHandler {
			return strings.TrimSpace(carrier.AdvancesTo)
		}
	}
	return ""
}

func deriveAccumulateTimeoutTransition(transition HandlerTransitionSemantic) (WorkflowTransitionContract, bool) {
	for _, carrier := range HandlerTransitionAdvanceCarriers(transition) {
		if carrier.Kind != HandlerAdvanceCarrierAccumulateOnTimeout {
			continue
		}
		return WorkflowTransitionContract{
			ID:      strings.TrimSpace(transition.ID) + ":on_timeout",
			From:    []string{"*"},
			To:      strings.TrimSpace(carrier.AdvancesTo),
			Trigger: "accumulate.timeout",
			Node:    strings.TrimSpace(transition.NodeID),
		}, true
	}
	return WorkflowTransitionContract{}, false
}

func deriveRuleTransitions(transition HandlerTransitionSemantic) []WorkflowTransitionContract {
	carriers := HandlerTransitionAdvanceCarriers(transition)
	out := make([]WorkflowTransitionContract, 0, len(carriers))
	defaultIDIndex := 0
	for _, carrier := range carriers {
		switch carrier.Kind {
		case HandlerAdvanceCarrierOnComplete, HandlerAdvanceCarrierRules, HandlerAdvanceCarrierAccumulateOnComplete:
		default:
			continue
		}
		rule := carrier.Rule
		id := strings.TrimSpace(rule.ID)
		if id == "" {
			id = fmt.Sprintf("%s:rule:%d", strings.TrimSpace(transition.ID), defaultIDIndex)
		}
		defaultIDIndex++
		out = append(out, WorkflowTransitionContract{
			ID:      id,
			From:    []string{"*"},
			To:      strings.TrimSpace(carrier.AdvancesTo),
			Trigger: strings.TrimSpace(transition.EventType),
			Node:    strings.TrimSpace(transition.NodeID),
			Actions: actionIDsForRule(rule),
		})
	}
	handlerAdvanceTo := handlerLevelAdvanceTarget(transition)
	for _, rule := range transition.Rules {
		if strings.TrimSpace(rule.AdvancesTo) != "" || strings.TrimSpace(rule.Action.ID) == "" {
			continue
		}
		to := handlerAdvanceTo
		if to == "" {
			continue
		}
		id := strings.TrimSpace(rule.ID)
		if id == "" {
			id = fmt.Sprintf("%s:rule:%d", strings.TrimSpace(transition.ID), defaultIDIndex)
		}
		defaultIDIndex++
		out = append(out, WorkflowTransitionContract{
			ID:      id,
			From:    []string{"*"},
			To:      to,
			Trigger: strings.TrimSpace(transition.EventType),
			Node:    strings.TrimSpace(transition.NodeID),
			Actions: actionIDsForRule(rule),
		})
	}
	return out
}

func actionIDsForRule(rule HandlerRuleEntry) []string {
	if id := strings.TrimSpace(rule.Action.ID); id != "" {
		return []string{id}
	}
	return nil
}

func firstTransitionGuardID(guard *GuardSpec) string {
	if guard == nil {
		return ""
	}
	return strings.TrimSpace(guard.ID)
}
func deriveWorkflowSemanticTimers(bundle *WorkflowContractBundle) []WorkflowTimerContract {
	if bundle == nil {
		return nil
	}
	out := make([]WorkflowTimerContract, 0, 8)
	indexByID := map[string]int{}
	addTimer := func(timer WorkflowTimerContract) {
		timer = normalizeWorkflowSemanticTimer(bundle, timer)
		id := strings.TrimSpace(timer.ID)
		if id == "" {
			return
		}
		if idx, ok := indexByID[id]; ok {
			out[idx] = mergeWorkflowSemanticTimer(out[idx], timer)
			return
		}
		indexByID[id] = len(out)
		out = append(out, timer)
	}
	for _, timer := range deriveNodeWorkflowTimers(bundle) {
		addTimer(timer)
	}
	for _, timer := range deriveStageWorkflowTimers(bundle) {
		addTimer(timer)
	}
	return out
}

const WorkflowStageTimerInternalEvent = "platform.stage_timer"

func deriveStageWorkflowTimers(bundle *WorkflowContractBundle) []WorkflowTimerContract {
	if bundle == nil {
		return nil
	}
	type stageSchema struct {
		FlowID string
		Schema FlowSchemaDocument
	}
	schemas := make([]stageSchema, 0, len(bundle.FlowViews())+1)
	if bundle.RootSchema != nil {
		schemas = append(schemas, stageSchema{Schema: *bundle.RootSchema})
	}
	for _, flow := range bundle.FlowViews() {
		flowID := strings.TrimSpace(flow.Paths.ID)
		if flowID == "" {
			continue
		}
		schemas = append(schemas, stageSchema{FlowID: flowID, Schema: flow.Schema})
	}
	out := make([]WorkflowTimerContract, 0)
	for _, schema := range schemas {
		if !schema.Schema.StageDeclarations.Declared {
			continue
		}
		for _, stage := range schema.Schema.StageDeclarations.Entries {
			stageID := strings.TrimSpace(stage.ID)
			if stageID == "" {
				continue
			}
			for _, row := range stage.Timers {
				eventType := strings.TrimSpace(row.Emit)
				if eventType == "" {
					eventType = WorkflowStageTimerInternalEvent
				}
				timerID := stageWorkflowTimerSemanticID(schema.FlowID, row.ID)
				out = append(out, WorkflowTimerContract{
					ID:         timerID,
					Stage:      stageID,
					Event:      eventType,
					Owner:      "runtime",
					FlowID:     strings.TrimSpace(schema.FlowID),
					StageOwned: true,
					AdvancesTo: strings.TrimSpace(row.AdvancesTo),
					Delay:      strings.TrimSpace(row.After),
					StartOn:    "state:" + stageID,
				})
			}
		}
	}
	return out
}

func stageWorkflowTimerSemanticID(flowID, rowID string) string {
	rowID = strings.TrimSpace(rowID)
	flowID = strings.TrimSpace(flowID)
	if rowID == "" || flowID == "" {
		return rowID
	}
	return flowID + "." + rowID
}

func deriveStageTimerTransitions(bundle *WorkflowContractBundle) []WorkflowTransitionContract {
	timers := deriveStageWorkflowTimers(bundle)
	out := make([]WorkflowTransitionContract, 0, len(timers))
	for _, timer := range timers {
		if strings.TrimSpace(timer.AdvancesTo) == "" {
			continue
		}
		out = append(out, WorkflowTransitionContract{
			ID:      "timer:" + strings.TrimSpace(timer.ID),
			From:    []string{strings.TrimSpace(timer.Stage)},
			To:      strings.TrimSpace(timer.AdvancesTo),
			Trigger: "timer:" + strings.TrimSpace(timer.ID),
			Node:    "runtime",
		})
	}
	return out
}

func deriveNodeWorkflowTimers(bundle *WorkflowContractBundle) []WorkflowTimerContract {
	if bundle == nil {
		return nil
	}
	type scopedNodeEntry struct {
		Key    string
		NodeID string
		Node   SystemNodeContract
		Source ContractItemSource
	}
	scopedNodes := make([]scopedNodeEntry, 0, len(bundle.scopedNodes))
	for scopedKey, node := range bundle.scopedNodes {
		source := bundle.scopedNodeSources[scopedKey]
		nodeID := strings.TrimSpace(node.ID)
		if nodeID == "" {
			parts := strings.Split(scopedKey, "::")
			if len(parts) > 0 {
				nodeID = strings.TrimSpace(parts[len(parts)-1])
			}
		}
		scopedNodes = append(scopedNodes, scopedNodeEntry{
			Key:    scopedKey,
			NodeID: nodeID,
			Node:   node,
			Source: source,
		})
	}
	if len(scopedNodes) == 0 {
		for nodeID, node := range bundle.Nodes {
			scopedNodes = append(scopedNodes, scopedNodeEntry{
				Key:    nodeID,
				NodeID: strings.TrimSpace(nodeID),
				Node:   node,
				Source: bundle.nodeSources[nodeID],
			})
		}
	}
	if len(scopedNodes) == 0 && len(bundle.Nodes) > 0 {
		for nodeID, node := range bundle.Nodes {
			scopedNodes = append(scopedNodes, scopedNodeEntry{
				Key:    nodeID,
				NodeID: strings.TrimSpace(nodeID),
				Node:   node,
			})
		}
	}
	if len(scopedNodes) == 0 {
		return nil
	}
	out := make([]WorkflowTimerContract, 0, 8)
	sort.Slice(scopedNodes, func(i, j int) bool {
		return strings.Compare(scopedNodes[i].Key, scopedNodes[j].Key) < 0
	})
	for _, item := range scopedNodes {
		nodeID := strings.TrimSpace(item.NodeID)
		node := item.Node
		if len(node.Timers) == 0 {
			continue
		}
		flowID := strings.TrimSpace(item.Source.FlowID)
		for _, timer := range node.Timers {
			timer.NodeID = nodeID
			timer.FlowID = flowID
			if strings.TrimSpace(timer.Owner) == "" {
				timer.Owner = nodeID
			}
			if strings.TrimSpace(timer.Event) == "" {
				timer.Event = inferWorkflowTimerEvent(bundle, node, timer)
			}
			out = append(out, timer)
		}
	}
	return out
}
func normalizeWorkflowSemanticTimer(bundle *WorkflowContractBundle, timer WorkflowTimerContract) WorkflowTimerContract {
	timer.ID = strings.TrimSpace(timer.ID)
	timer.Stage = strings.TrimSpace(timer.Stage)
	timer.Event = strings.TrimSpace(timer.Event)
	timer.Owner = strings.TrimSpace(timer.Owner)
	timer.FlowID = strings.TrimSpace(timer.FlowID)
	timer.NodeID = strings.TrimSpace(timer.NodeID)
	timer.AdvancesTo = strings.TrimSpace(timer.AdvancesTo)
	timer.Action = strings.TrimSpace(timer.Action)
	timer.Cancellation = strings.TrimSpace(timer.Cancellation)
	timer.Delay = strings.TrimSpace(timer.Delay)
	timer.StartOn = strings.TrimSpace(timer.StartOn)
	timer.CancelOn = strings.TrimSpace(timer.CancelOn)
	if timer.Event == "" && timer.NodeID != "" {
		node := bundle.Nodes[timer.NodeID]
		timer.Event = inferWorkflowTimerEvent(bundle, node, timer)
	}
	return timer
}
func mergeWorkflowSemanticTimer(existing, incoming WorkflowTimerContract) WorkflowTimerContract {
	if strings.TrimSpace(existing.ID) == "" {
		return incoming
	}
	if strings.TrimSpace(existing.Stage) == "" {
		existing.Stage = incoming.Stage
	}
	if strings.TrimSpace(existing.Event) == "" {
		existing.Event = incoming.Event
	}
	if strings.TrimSpace(existing.Owner) == "" {
		existing.Owner = incoming.Owner
	}
	if strings.TrimSpace(existing.FlowID) == "" {
		existing.FlowID = incoming.FlowID
	}
	if strings.TrimSpace(existing.NodeID) == "" {
		existing.NodeID = incoming.NodeID
	}
	existing.StageOwned = existing.StageOwned || incoming.StageOwned
	if strings.TrimSpace(existing.AdvancesTo) == "" {
		existing.AdvancesTo = incoming.AdvancesTo
	}
	if strings.TrimSpace(existing.Action) == "" {
		existing.Action = incoming.Action
	}
	if strings.TrimSpace(existing.Cancellation) == "" {
		existing.Cancellation = incoming.Cancellation
	}
	if strings.TrimSpace(existing.Delay) == "" {
		existing.Delay = incoming.Delay
	}
	if strings.TrimSpace(existing.StartOn) == "" {
		existing.StartOn = incoming.StartOn
	}
	if strings.TrimSpace(existing.CancelOn) == "" {
		existing.CancelOn = incoming.CancelOn
	}
	existing.Recurring = existing.Recurring || incoming.Recurring
	return existing
}
func inferWorkflowTimerEvent(bundle *WorkflowContractBundle, node SystemNodeContract, timer WorkflowTimerContract) string {
	if eventType := strings.TrimSpace(timer.Event); eventType != "" {
		return eventType
	}
	timerID := strings.TrimSpace(timer.ID)
	if timerID == "" {
		return ""
	}
	candidates := []string{timerID}
	if !strings.HasPrefix(timerID, "timer.") {
		candidates = append([]string{"timer." + timerID}, candidates...)
	}
	for _, candidate := range candidates {
		if _, ok := node.EventHandlers[candidate]; ok {
			return candidate
		}
	}
	for _, candidate := range candidates {
		if workflowTimerEventDefined(bundle, candidate) {
			return candidate
		}
	}
	for _, subscribed := range EffectiveSystemNodeSubscriptions(node) {
		subscribed = strings.TrimSpace(subscribed)
		if subscribed == "" {
			continue
		}
		for _, candidate := range candidates {
			if subscribed == candidate {
				return candidate
			}
		}
	}
	return ""
}
func workflowTimerEventDefined(bundle *WorkflowContractBundle, eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	if bundle == nil || eventType == "" {
		return false
	}
	for scopedKey := range bundle.scopedEvents {
		if strings.HasSuffix(scopedKey, "::"+eventType) {
			return true
		}
	}
	if _, ok := bundle.Events[eventType]; ok {
		return true
	}
	return false
}
func appendPlatformBuiltinGuardEntries(existing []GuardActionEntry, builtins []struct {
	ID string `yaml:"id"`
}) []GuardActionEntry {
	out := append([]GuardActionEntry{}, existing...)
	seen := make(map[string]struct{}, len(out))
	for _, entry := range out {
		if id := strings.TrimSpace(entry.ID); id != "" {
			seen[id] = struct{}{}
		}
	}
	for _, builtin := range builtins {
		id := strings.TrimSpace(builtin.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, GuardActionEntry{
			ID:              id,
			Category:        "platform",
			PlatformBuiltin: id,
		})
	}
	return out
}
func appendPlatformBuiltinActionEntries(existing []GuardActionEntry, builtins []struct {
	ID string `yaml:"id"`
}) []GuardActionEntry {
	out := append([]GuardActionEntry{}, existing...)
	seen := make(map[string]int, len(out))
	for i, entry := range out {
		if id := strings.TrimSpace(entry.ID); id != "" {
			seen[id] = i
		}
	}
	for _, builtin := range builtins {
		id := strings.TrimSpace(builtin.ID)
		if id == "" {
			continue
		}
		if idx, ok := seen[id]; ok {
			if strings.TrimSpace(out[idx].PlatformBuiltin) == "" {
				out[idx].PlatformBuiltin = id
				if strings.TrimSpace(out[idx].Category) == "" {
					out[idx].Category = "platform"
				}
			}
			continue
		}
		seen[id] = len(out)
		out = append(out, GuardActionEntry{
			ID:              id,
			Category:        "platform",
			PlatformBuiltin: id,
		})
	}
	return out
}
func rootSchemaInitialStage(root *FlowSchemaDocument) string {
	if root == nil {
		return ""
	}
	return root.LoweredInitialState()
}

func deriveWorkflowStages(root *FlowSchemaDocument, paths []FlowContractPaths, schemas map[string]FlowSchemaDocument) []WorkflowStageContract {
	out := make([]WorkflowStageContract, 0)
	seen := make(map[string]struct{})
	if root != nil {
		for _, stage := range root.LoweredWorkflowStages("") {
			key := strings.TrimSpace(stage.ID)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, stage)
		}
	}
	for _, flow := range paths {
		flowID := strings.TrimSpace(flow.ID)
		schema, ok := schemas[flowID]
		if !ok {
			continue
		}
		for _, stage := range schema.LoweredWorkflowStages(flowID) {
			key := strings.TrimSpace(stage.ID)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, stage)
		}
	}
	return out
}

func deriveWorkflowTerminalStages(root *FlowSchemaDocument, paths []FlowContractPaths, schemas map[string]FlowSchemaDocument) []string {
	out := make([]string, 0)
	seen := make(map[string]struct{})
	if root != nil {
		for _, state := range root.LoweredTerminalStates() {
			state = strings.TrimSpace(state)
			if state == "" {
				continue
			}
			if _, exists := seen[state]; exists {
				continue
			}
			seen[state] = struct{}{}
			out = append(out, state)
		}
	}
	for _, flow := range paths {
		flowID := strings.TrimSpace(flow.ID)
		schema, ok := schemas[flowID]
		if !ok {
			continue
		}
		for _, state := range schema.LoweredTerminalStates() {
			state = strings.TrimSpace(state)
			if state == "" {
				continue
			}
			if _, exists := seen[state]; exists {
				continue
			}
			seen[state] = struct{}{}
			out = append(out, state)
		}
	}
	return out
}
func flowAssignedNamespace(paths []FlowContractPaths, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return ""
	}
	for _, flow := range paths {
		if strings.TrimSpace(flow.ID) == flowID {
			return strings.TrimSpace(flow.Namespace)
		}
	}
	return ""
}
