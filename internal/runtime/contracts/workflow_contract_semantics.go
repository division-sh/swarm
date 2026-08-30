package contracts

import (
	"fmt"
	"sort"
	"strings"
)

func populateWorkflowSemantics(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	name := "."
	version := ""
	if bundle.SourceArtifact != nil {
		version = bundle.SourceArtifact.BundleHash()
	}
	entitySchema := legacyWorkflowEntitySchema(bundle)
	compositionConnects := make([]FlowConnect, 0)
	for _, source := range sortedFlowSources(bundle.FlowSources) {
		var schema FlowSchemaDocument
		if source.FlowPath == "." && bundle.RootSchema != nil {
			schema = *bundle.RootSchema
		} else {
			schema = bundle.FlowSchemas[source.FlowPath]
		}
		for _, connect := range schema.Connect {
			compositionConnects = append(compositionConnects, connect.WithOwnerSource(source.FlowPath, source.Schema))
		}
	}
	semantics := WorkflowSemanticView{
		Name:                   name,
		Version:                version,
		InitialStage:           rootSchemaInitialStage(bundle.RootSchema),
		EntitySchema:           entitySchema,
		Stages:                 deriveWorkflowStages(bundle.RootSchema, bundle.FlowSchemas),
		TerminalStages:         deriveWorkflowTerminalStages(bundle.RootSchema, bundle.FlowSchemas),
		Transitions:            deriveStageTimerTransitions(bundle),
		Timers:                 deriveWorkflowSemanticTimers(bundle),
		Joins:                  nil,
		Loops:                  nil,
		Gates:                  deriveWorkflowGatePlans(bundle),
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
		flowInputEventPins:     map[string][]CompiledFlowInputPin{},
		flowOutputEventPins:    map[string][]CompiledFlowOutputPin{},
		flowReads:              map[string]CompiledFlowEntityPermissions{},
		flowWrites:             map[string]CompiledFlowEntityPermissions{},
		CompositionConnects:    compositionConnects,
		FlowAgents:             map[string][]FlowRequiredAgent{},
		RootAgentFacts:         nil,
		FlowAgentFacts:         map[string][]RequiredAgentFact{},
		writePinOwners:         map[string][]string{},
		EffectiveNodes:         map[string]SystemNodeEffectiveSemantics{},
		NodeHandlers:           map[string]map[string]SystemNodeEventHandler{},
		EventOwners:            map[string][]string{},
		HandlerTransitionIndex: map[string]map[string]HandlerTransitionSemantic{},
		StageTopologies:        map[string]WorkflowStageTopology{},
	}
	// Connected producer ownership is an input to independent pin compilation,
	// not a reader-side reconstruction performed by route-plan consumers.
	bundle.Semantics = semantics
	populateEventSchemaOwnershipIndex(bundle)
	semantics.Guards = appendPlatformBuiltinGuardEntries(semantics.Guards, bundle.Platform.BuiltinHooks.Guards)
	semantics.Actions = appendPlatformBuiltinActionEntries(semantics.Actions, bundle.Platform.BuiltinHooks.Actions)
	semantics.RootAgentFacts = bundle.RootRequiredAgentFacts()
	if bundle.RootSchema != nil {
		var err error
		rootSchemaFile := ""
		if root, ok := bundle.FlowSources["."]; ok {
			rootSchemaFile = root.Schema
		}
		semantics.flowInputEventPins["."], err = compileFlowInputPins(bundle, ".", ".", rootSchemaFile, bundle.RootSchema.Pins.Inputs.EventPins)
		if err != nil {
			return fmt.Errorf("compile root input pins: %w", err)
		}
		semantics.flowOutputEventPins["."], err = compileFlowOutputPins(bundle, ".", ".", rootSchemaFile, bundle.RootSchema.Pins.Outputs.EventPins)
		if err != nil {
			return fmt.Errorf("compile root output pins: %w", err)
		}
		semantics.flowReads["."], err = CompileFlowEntityPermissions(bundle.RootSchema.Pins.Inputs.Reads)
		if err != nil {
			return fmt.Errorf("compile root input entity permissions: %w", err)
		}
		semantics.flowWrites["."], err = CompileFlowEntityPermissions(bundle.RootSchema.Pins.Outputs.Writes)
		if err != nil {
			return fmt.Errorf("compile root output entity permissions: %w", err)
		}
	}
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
		assignedNamespace := strings.TrimSpace(bundle.FlowPath(flowID))
		semantics.FlowNamespace[flowID] = assignedNamespace
		semantics.FlowPrefix[flowID] = assignedNamespace
		semantics.FlowRules[flowID] = "path-derived"
		flowPath := bundle.FlowPath(flowID)
		schemaFile := ""
		if view, ok := bundle.FlowViewByID(flowID); ok && view != nil {
			schemaFile = view.Paths.SchemaFile
		}
		var err error
		semantics.flowInputEventPins[flowID], err = compileFlowInputPins(bundle, flowID, flowPath, schemaFile, schema.Pins.Inputs.EventPins)
		if err != nil {
			return fmt.Errorf("compile flow %s input pins: %w", flowID, err)
		}
		semantics.flowOutputEventPins[flowID], err = compileFlowOutputPins(bundle, flowID, flowPath, schemaFile, schema.Pins.Outputs.EventPins)
		if err != nil {
			return fmt.Errorf("compile flow %s output pins: %w", flowID, err)
		}
		semantics.flowReads[flowID], err = CompileFlowEntityPermissions(schema.Pins.Inputs.Reads)
		if err != nil {
			return fmt.Errorf("compile flow %s input entity permissions: %w", flowID, err)
		}
		semantics.flowWrites[flowID], err = CompileFlowEntityPermissions(schema.Pins.Outputs.Writes)
		if err != nil {
			return fmt.Errorf("compile flow %s output entity permissions: %w", flowID, err)
		}
		facts := bundle.FlowRequiredAgentFacts(flowID)
		semantics.FlowAgentFacts[flowID] = facts
		semantics.FlowAgents[flowID] = FlowRequiredAgentsFromFacts(facts)
		for _, writePin := range semantics.flowWrites[flowID].Fields() {
			semantics.writePinOwners[writePin] = appendIfMissingString(semantics.writePinOwners[writePin], flowID)
		}
	}
	for _, record := range bundle.ScopedNodeRecords() {
		node, _ := record.Identity()
		flowID := strings.TrimSpace(record.Source.FlowPath)
		for eventType, handler := range record.Entry.EventHandlers {
			handler, _ = QualifySystemNodeHandlerRuleRefsForEvent(node, eventType, handler)
			eventType = strings.TrimSpace(eventType)
			handler = DefaultSystemNodeHandlerSourceEvent(handler, eventType)
			if handler.Join == nil {
				continue
			}
			joinPlan := WorkflowJoinPlan{
				Node:         node,
				HandlerEvent: eventType,
				Mode:         handler.Join.Mode(),
				Spec:         *handler.Join,
			}
			if handler.Join.IsFanOutDeliveryBarrier() {
				if handler.FanOut == nil {
					return fmt.Errorf("node %s handler %s fan-out delivery join requires paired top-level fan_out", node.Key(), eventType)
				}
				fanOutPlan, err := bundle.CompileFanOutPlan(node, eventType, handler, WorkflowFanOutSite{
					Source: "handler.fan_out", Kind: FanOutSiteHandler, Index: -1, Spec: handler.FanOut, Writes: handler.DataAccumulation.Writes,
				})
				if err != nil {
					return fmt.Errorf("compile fan-out delivery join %s: %w", handler.Join.EffectiveID(), err)
				}
				joinPlan.FanOut = WorkflowFanOutDeliveryJoinPlan{FanOut: fanOutPlan.Ref}
			} else {
				resultType, _ := ResolveEventFieldType(bundle, flowID, eventType, joinOutputField(handler.Join.Output))
				joinPlan.ResultType = resultType
			}
			semantics.Joins = append(semantics.Joins, joinPlan)
		}
	}
	for _, record := range bundle.ScopedNodeRecords() {
		nodeRef, err := record.Identity()
		if err != nil {
			continue
		}
		nodeID := nodeRef.NodeID()
		node := record.Entry
		effective := SystemNodeEffectiveSemantics{
			ID:                   EffectiveSystemNodeID(nodeID, node),
			ExecutionType:        EffectiveSystemNodeExecutionType(node),
			RuntimeSubscriptions: EffectiveSystemNodeSubscriptions(node),
			Produces:             EffectiveSystemNodeProduces(node),
		}
		semantics.EffectiveNodes[nodeRef.Key()] = effective
		if len(node.EventHandlers) == 0 {
			continue
		}
		handlers := make(map[string]SystemNodeEventHandler, len(node.EventHandlers))
		for eventType, handler := range node.EventHandlers {
			handler, _ = QualifySystemNodeHandlerRuleRefsForEvent(nodeRef, eventType, handler)
			rawEventType := strings.TrimSpace(eventType)
			if rawEventType == "" {
				continue
			}
			handler = DefaultSystemNodeHandlerSourceEvent(handler, rawEventType)
			handlers[rawEventType] = handler
			ownerEventType := strings.TrimSpace(bundle.ResolveExecutableNodeEventPattern(nodeRef, rawEventType))
			if ownerEventType == "" {
				ownerEventType = rawEventType
			}
			semantics.EventOwners[ownerEventType] = appendIfMissingString(semantics.EventOwners[ownerEventType], nodeRef.Key())
			transition := HandlerTransitionSemantic{
				ID:                   fmt.Sprintf("%s:%s", nodeRef.Key(), rawEventType),
				Node:                 nodeRef,
				EventType:            rawEventType,
				CreateEntity:         handler.CreateEntity,
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
				Loop:                 handler.Loop,
				OnComplete:           handler.OnComplete,
				Rules:                handler.Rules,
				Accumulate:           handler.Accumulate,
				Join:                 handler.Join,
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
			semantics.Transitions = append(semantics.Transitions, deriveJoinTransitions(transition)...)
			if semantics.HandlerTransitionIndex[nodeRef.Key()] == nil {
				semantics.HandlerTransitionIndex[nodeRef.Key()] = map[string]HandlerTransitionSemantic{}
			}
			semantics.HandlerTransitionIndex[nodeRef.Key()][rawEventType] = transition
		}
		semantics.NodeHandlers[nodeRef.Key()] = handlers
	}
	semantics.Loops = deriveWorkflowLoopPlans(bundle, semantics.HandlerTransitions)
	semantics.StageTopologies = deriveWorkflowStageTopologies(semantics)
	semantics.Loops = BindWorkflowLoopRegions(semantics.Loops, semantics.StageTopologies)
	bundle.Semantics = semantics
	populateEventSchemaOwnershipIndex(bundle)
	return nil
}

// CompileWorkflowSemantics compiles a fully admitted in-memory bundle through
// the same immutable semantic owner used by the loader. Callers cannot inject
// or mutate compiled pins directly.
func CompileWorkflowSemantics(bundle *WorkflowContractBundle) error {
	return populateWorkflowSemantics(bundle)
}

func deriveWorkflowGatePlans(bundle *WorkflowContractBundle) []WorkflowGatePlan {
	if bundle == nil {
		return nil
	}
	out := make([]WorkflowGatePlan, 0)
	if bundle.RootSchema != nil {
		out = append(out, bundle.RootSchema.StageDeclarations.GatePlans(".")...)
	}
	flowIDs := make([]string, 0, len(bundle.FlowSchemas))
	for flowID := range bundle.FlowSchemas {
		flowIDs = append(flowIDs, flowID)
	}
	sort.Strings(flowIDs)
	for _, flowID := range flowIDs {
		schema := bundle.FlowSchemas[flowID]
		out = append(out, schema.StageDeclarations.GatePlans(flowID)...)
	}
	return out
}

func deriveWorkflowLoopPlans(bundle *WorkflowContractBundle, transitions []HandlerTransitionSemantic) []WorkflowLoopPlan {
	if bundle == nil {
		return nil
	}
	plans := make([]WorkflowLoopPlan, 0)
	add := func(flowID string, declarations FlowLoopDeclarations) {
		for _, declaration := range declarations.Entries {
			plans = append(plans, WorkflowLoopPlan{
				FlowID:        strings.TrimSpace(flowID),
				ID:            strings.TrimSpace(declaration.ID),
				RevisionField: strings.TrimSpace(declaration.RevisionField),
				MaxAttempts:   declaration.MaxAttempts,
				Escape:        declaration.Escape,
			})
		}
	}
	if bundle.RootSchema != nil {
		add(".", bundle.RootSchema.LoopDeclarations)
	}
	for flowID, schema := range bundle.FlowSchemas {
		add(flowID, schema.LoopDeclarations)
	}
	for _, transition := range transitions {
		if transition.Loop == nil {
			continue
		}
		kind, loopID, err := transition.Loop.Operation()
		if err != nil {
			continue
		}
		for idx := range plans {
			if plans[idx].ID != loopID || plans[idx].FlowID != transition.Node.FlowPath() {
				continue
			}
			operation := WorkflowLoopOperationPlan{
				Node:         transition.Node,
				HandlerEvent: strings.TrimSpace(transition.EventType),
				Kind:         kind,
				LoopID:       loopID,
				From:         strings.TrimSpace(transition.Loop.From),
				AdvancesTo:   strings.TrimSpace(transition.AdvancesTo),
				Emit:         cloneEmitSpec(transition.Emit),
			}
			plans[idx].Operations = append(plans[idx].Operations, operation)
			if operation.Kind == LoopOperationStart && plans[idx].EntryStage == "" {
				plans[idx].EntryStage = operation.AdvancesTo
			}
		}
	}
	return plans
}

func deriveWorkflowStageTopologies(semantics WorkflowSemanticView) map[string]WorkflowStageTopology {
	out := map[string]WorkflowStageTopology{}
	rootStages := make([]string, 0, len(semantics.Stages))
	for _, stage := range semantics.Stages {
		if id := strings.TrimSpace(stage.ID); id != "" {
			rootStages = append(rootStages, id)
		}
	}
	build := func(flowID, initial string, stages, terminal []string) {
		timers := make([]WorkflowTimerContract, 0)
		for _, timer := range semantics.Timers {
			owner := strings.TrimSpace(timer.FlowID)
			if flowID == "." {
				if owner != "" && owner != "." {
					continue
				}
			} else if owner != flowID {
				continue
			}
			timers = append(timers, timer)
		}
		out[flowID] = BuildWorkflowStageTopology(flowID, initial, stages, terminal, semantics.HandlerTransitions, timers, semantics.Loops, semantics.Gates)
	}
	build(".", semantics.InitialStage, rootStages, semantics.TerminalStages)
	for flowID, stages := range semantics.FlowStates {
		build(flowID, semantics.FlowInitial[flowID], stages, semantics.FlowTerminal[flowID])
	}
	return out
}

func joinOutputField(path string) string {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "payload.") {
		return ""
	}
	field := strings.TrimPrefix(path, "payload.")
	if field == "" || strings.Contains(field, ".") {
		return ""
	}
	return field
}

func legacyWorkflowEntitySchema(bundle *WorkflowContractBundle) EntitySchema {
	if bundle == nil {
		return EntitySchema{}
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
	for _, record := range bundle.ScopedNodeRecords() {
		node := record.Entry
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
	for _, record := range bundle.ScopedNodeRecords() {
		node := record.Entry
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
		ExecutableNode:   transition.Node,
		DataAccumulation: transition.DataAccumulation,
	}
	if transition.Loop != nil && strings.TrimSpace(transition.Loop.From) != "" {
		out.From = []string{strings.TrimSpace(transition.Loop.From)}
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

func deriveJoinTransitions(transition HandlerTransitionSemantic) []WorkflowTransitionContract {
	if transition.Join == nil {
		return nil
	}
	join := transition.Join
	from := []string{strings.TrimSpace(join.Stage)}
	out := make([]WorkflowTransitionContract, 0, 2)
	if target := strings.TrimSpace(join.OnComplete.AdvancesTo); target != "" {
		out = append(out, WorkflowTransitionContract{
			ID:             strings.TrimSpace(transition.ID) + ":join:" + join.EffectiveID() + ":complete",
			From:           from,
			To:             target,
			Trigger:        strings.TrimSpace(transition.EventType),
			ExecutableNode: transition.Node,
		})
	}
	if target := strings.TrimSpace(join.Timeout.Outcome.AdvancesTo); target != "" {
		out = append(out, WorkflowTransitionContract{
			ID:             strings.TrimSpace(transition.ID) + ":join:" + join.EffectiveID() + ":timeout",
			From:           from,
			To:             target,
			Trigger:        "platform.join_timeout",
			ExecutableNode: transition.Node,
		})
	}
	return out
}

func deriveRuleTransitions(transition HandlerTransitionSemantic) []WorkflowTransitionContract {
	carriers := HandlerTransitionAdvanceCarriers(transition)
	out := make([]WorkflowTransitionContract, 0, len(carriers))
	defaultIDIndex := 0
	for _, carrier := range carriers {
		switch carrier.Kind {
		case HandlerAdvanceCarrierOnComplete, HandlerAdvanceCarrierRules:
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
			ID:             id,
			From:           []string{"*"},
			To:             strings.TrimSpace(carrier.AdvancesTo),
			Trigger:        strings.TrimSpace(transition.EventType),
			ExecutableNode: transition.Node,
			Actions:        actionIDsForRule(rule),
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
			ID:             id,
			From:           []string{"*"},
			To:             to,
			Trigger:        strings.TrimSpace(transition.EventType),
			ExecutableNode: transition.Node,
			Actions:        actionIDsForRule(rule),
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
		key := timer.SemanticKey()
		if key == "" {
			return
		}
		if idx, ok := indexByID[key]; ok {
			out[idx] = mergeWorkflowSemanticTimer(out[idx], timer)
			return
		}
		indexByID[key] = len(out)
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
		schemas = append(schemas, stageSchema{FlowID: ".", Schema: *bundle.RootSchema})
	}
	for _, flow := range bundle.FlowViews() {
		flowID := strings.TrimSpace(flow.Paths.FlowPath)
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
	if rowID == "" || flowID == "" || flowID == "." {
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
			ID:            "timer:" + strings.TrimSpace(timer.ID),
			From:          []string{strings.TrimSpace(timer.Stage)},
			To:            strings.TrimSpace(timer.AdvancesTo),
			Trigger:       "timer:" + strings.TrimSpace(timer.ID),
			FlowID:        strings.TrimSpace(timer.FlowID),
			InternalOwner: "runtime",
		})
	}
	return out
}

func deriveNodeWorkflowTimers(bundle *WorkflowContractBundle) []WorkflowTimerContract {
	if bundle == nil {
		return nil
	}
	scopedNodes := bundle.ScopedNodeRecords()
	if len(scopedNodes) == 0 {
		return nil
	}
	out := make([]WorkflowTimerContract, 0, 8)
	for _, item := range scopedNodes {
		nodeRef, _ := item.Identity()
		nodeID := nodeRef.NodeID()
		node := item.Entry
		if len(node.Timers) == 0 {
			continue
		}
		for _, timer := range node.Timers {
			timer.Node = nodeRef
			timer.FlowID = ""
			timer.StageOwned = false
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
	if timer.Node.Valid() {
		timer.FlowID = ""
	} else {
		timer.FlowID = strings.TrimSpace(timer.FlowID)
	}
	timer.AdvancesTo = strings.TrimSpace(timer.AdvancesTo)
	timer.Action = strings.TrimSpace(timer.Action)
	timer.Cancellation = strings.TrimSpace(timer.Cancellation)
	timer.Delay = strings.TrimSpace(timer.Delay)
	timer.StartOn = strings.TrimSpace(timer.StartOn)
	timer.CancelOn = strings.TrimSpace(timer.CancelOn)
	if timer.Event == "" && timer.Node.Valid() {
		if record, ok := bundle.ExecutableNode(timer.Node); ok {
			timer.Event = inferWorkflowTimerEvent(bundle, record.Entry, timer)
		}
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
	if strings.TrimSpace(existing.FlowID) == "" && !existing.Node.Valid() {
		existing.FlowID = incoming.FlowID
	}
	if !existing.Node.Valid() {
		existing.Node = incoming.Node
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

func deriveWorkflowStages(root *FlowSchemaDocument, schemas map[string]FlowSchemaDocument) []WorkflowStageContract {
	out := make([]WorkflowStageContract, 0)
	seen := make(map[string]struct{})
	if root != nil {
		for _, stage := range root.LoweredWorkflowStages(".") {
			key := strings.TrimSpace(stage.ID)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, stage)
		}
	}
	flowIDs := make([]string, 0, len(schemas))
	for flowID := range schemas {
		flowIDs = append(flowIDs, flowID)
	}
	sort.Strings(flowIDs)
	for _, flowID := range flowIDs {
		schema := schemas[flowID]
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

func deriveWorkflowTerminalStages(root *FlowSchemaDocument, schemas map[string]FlowSchemaDocument) []string {
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
	flowIDs := make([]string, 0, len(schemas))
	for flowID := range schemas {
		flowIDs = append(flowIDs, flowID)
	}
	sort.Strings(flowIDs)
	for _, flowID := range flowIDs {
		schema := schemas[flowID]
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
