package engine

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/accprojection"
	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

// EntityAssignmentPoint identifies an executed reader, including its selected
// outcome and its position inside an ordered mutation list.
type EntityAssignmentPoint struct {
	Step               Step
	RuleKind           c.HandlerAdvanceCarrierKind
	RuleIndex          int
	Condition          bool
	WriteIndex         int
	HasWriteIndex      bool
	FanOut             bool
	FanOutAfterWrites  bool
	RuleCompute        bool
	GuardCheckIndex    int
	HasGuardCheckIndex bool
	PolicyCompute      bool
}

type entityAssignmentOutcome struct {
	kind       c.HandlerAdvanceCarrierKind
	index      int
	rule       *c.HandlerRuleEntry
	conditions []entityAssignmentCondition
}

type entityAssignmentCondition struct {
	expression string
	truth      bool
}

// EntityAssignmentAnalysis consumes the existing stage graph and executor step
// order. It never treats writer grants, event spelling or a destination-only
// graph edge as evidence of an executed assignment.
type EntityAssignmentAnalysis struct {
	source   semanticview.Source
	entity   c.ResolvedCatalogType
	initial  entityruntime.AssignmentFacts
	topology c.WorkflowStageTopology
	stages   map[string]entityruntime.AssignmentFacts
}

func BuildEntityAssignmentAnalysis(source semanticview.Source, flowID string) (*EntityAssignmentAnalysis, error) {
	entityScope := flowID
	if flowID == "." {
		entityScope = ""
	}
	contract, ok := entityruntime.ResolveForFlow(source, entityScope)
	if !ok {
		return nil, fmt.Errorf("flow %s has no entity assignment contract", flowID)
	}
	typ, err := semanticview.ResolveEntityStructuralType(source, flowID)
	if err != nil || typ == nil {
		return nil, fmt.Errorf("flow %s has no structural entity type", flowID)
	}
	a := &EntityAssignmentAnalysis{source: source, entity: typ.Clone(), initial: entityruntime.AssignmentFacts{}, stages: map[string]entityruntime.AssignmentFacts{}}
	for name, decl := range contract.Entity.Fields {
		if decl.Initial == nil {
			continue
		}
		if field, found := a.entity.Field(name); found {
			a.initial.AssignValue(name, field.Type, decl.Initial)
		}
	}
	a.topology, _ = semanticview.WorkflowStageTopology(source, flowID)
	if a.topology.InitialStage == "" {
		// Without stages, preserve only initializer facts invariant under every
		// possible handler outcome. A prior clear/replacement remains possible.
		for changed := true; changed; {
			changed = false
			for _, record := range source.ExecutableNodeRecords() {
				node, err := record.Identity()
				if err != nil || node.FlowPath() != flowID {
					continue
				}
				for event, handler := range record.Entry.EventHandlers {
					for _, outcome := range entityAssignmentOutcomes(handler) {
						next := a.initial.Intersect(a.transfer(node, event, handler, outcome, a.initial, nil))
						if !maps.Equal(next, a.initial) {
							a.initial, changed = next, true
						}
					}
				}
			}
		}
		return a, nil
	}
	a.stages[a.topology.InitialStage] = a.initial.Clone()
	// A first reachable predecessor initializes a stage; every additional
	// predecessor intersects it. The immutable first-entry seed participates on
	// every iteration, so a loop backedge cannot certify first entry.
	for changed := true; changed; {
		changed = false
		merge := func(stage string, facts entityruntime.AssignmentFacts) {
			old, reached := a.stages[stage]
			if reached {
				facts = old.Intersect(facts)
			}
			if !reached || !maps.Equal(old, facts) {
				a.stages[stage] = facts.Clone()
				changed = true
			}
		}
		merge(a.topology.InitialStage, a.initial)
		for _, edge := range a.topology.Edges {
			before, reached := a.stages[edge.From]
			if !reached {
				continue
			}
			if edge.CarrierKind == "" {
				// Timer/gate/loop escape transitions have no ordinary handler
				// write success to borrow from the handler that scheduled them.
				merge(edge.To, before)
				continue
			}
			handler, found := source.ExecutableNodeEventHandler(edge.Node, edge.HandlerEvent)
			if !found || !entityAssignmentGuardAllows(handler, edge.From) {
				continue
			}
			for _, outcome := range entityAssignmentOutcomes(handler) {
				if !entityAssignmentEdgeSelects(edge, handler, outcome) || !entityAssignmentOutcomeAllows(outcome, edge.From) {
					continue
				}
				merge(edge.To, a.transfer(edge.Node, edge.HandlerEvent, handler, outcome, before, nil))
			}
		}
		// A committed non-transitioning handler can invalidate a previously
		// established optional path while staying in its current stage.
		for _, scope := range a.topology.Handlers {
			handler, found := source.ExecutableNodeEventHandler(scope.Node, scope.EventType)
			if !found {
				continue
			}
			for _, stage := range scope.Stages {
				before, reached := a.stages[stage]
				if !reached || !entityAssignmentGuardAllows(handler, stage) {
					continue
				}
				for _, outcome := range entityAssignmentOutcomes(handler) {
					if entityAssignmentDestination(handler, outcome) == "" && entityAssignmentOutcomeAllows(outcome, stage) {
						merge(stage, a.transfer(scope.Node, scope.EventType, handler, outcome, before, nil))
					}
				}
			}
		}
	}
	return a, nil
}

// StageFacts is the committed-state fact at a stage-owned read boundary. Gate
// context freezing consumes this same result, not a separate writer census.
func (a *EntityAssignmentAnalysis) StageFacts(stage string) entityruntime.AssignmentFacts {
	if facts, reached := a.stages[stage]; reached {
		return facts.Clone()
	}
	if a.topology.InitialStage == "" {
		return a.initial.Clone()
	}
	return entityruntime.AssignmentFacts{}
}

func (a *EntityAssignmentAnalysis) BeforeRead(node identity.ExecutableNode, event string, handler c.SystemNodeEventHandler, point EntityAssignmentPoint) entityruntime.AssignmentFacts {
	stages := a.topology.HandlerStages(node, event)
	if len(stages) == 0 {
		stages = []string{""}
	}
	return a.beforeReadInStages(node, event, handler, point, stages)
}

func (a *EntityAssignmentAnalysis) beforeReadInStages(node identity.ExecutableNode, event string, handler c.SystemNodeEventHandler, point EntityAssignmentPoint, stages []string) entityruntime.AssignmentFacts {
	var result entityruntime.AssignmentFacts
	for _, stage := range stages {
		before, reached := a.stages[stage]
		if !reached {
			// Stageless or unreachable sites cannot borrow unreachable writes.
			before = a.initial
		}
		if point.Step != StepQuery && point.Step != StepGuard && !entityAssignmentGuardAllows(handler, stage) {
			continue
		}
		for _, outcome := range entityAssignmentOutcomes(handler) {
			if point.RuleKind != "" && (outcome.kind != point.RuleKind || outcome.index != point.RuleIndex) {
				continue
			}
			if (entityAssignmentStepAfter(point.Step, StepRules) || point.RuleKind != "" && !point.Condition) && !entityAssignmentOutcomeAllows(outcome, stage) {
				continue
			}
			facts := a.transfer(node, event, handler, outcome, before, &point)
			if result == nil {
				result = facts
			} else {
				result = result.Intersect(facts)
			}
		}
	}
	if result == nil {
		return entityruntime.AssignmentFacts{}
	}
	return result
}

func (a *EntityAssignmentAnalysis) UnassignedReads(node identity.ExecutableNode, event string, handler c.SystemNodeEventHandler, point EntityAssignmentPoint, expression string) []string {
	if point.Step != StepGuard && !point.Condition {
		return workflowexpr.RequiredEntityReferences(expression, EntityAssignmentPresencePaths(a.BeforeRead(node, event, handler, point)))
	}
	stages := a.topology.HandlerStages(node, event)
	if len(stages) == 0 {
		stages = []string{""}
	}
	missing := map[string]struct{}{}
	for _, stage := range stages {
		if point.HasGuardCheckIndex {
			reachable := true
			for index, check := range handler.Guard.EffectiveChecks() {
				if index >= point.GuardCheckIndex {
					break
				}
				reachable = reachable && workflowexpr.ConditionCanHoldAtStage(check.Check, stage, true)
			}
			if !reachable {
				continue
			}
		}
		facts := a.beforeReadInStages(node, event, handler, point, []string{stage})
		for _, path := range workflowexpr.RequiredEntityReferencesAtStage(expression, EntityAssignmentPresencePaths(facts), stage) {
			missing[path] = struct{}{}
		}
	}
	out := make([]string, 0, len(missing))
	for path := range missing {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

func (a *EntityAssignmentAnalysis) transfer(node identity.ExecutableNode, event string, handler c.SystemNodeEventHandler, outcome entityAssignmentOutcome, before entityruntime.AssignmentFacts, point *EntityAssignmentPoint) entityruntime.AssignmentFacts {
	facts := before.Clone()
	assign := func(target string) {
		path, owned, err := entityruntime.EntityWritePath(target)
		if field, ok := a.entity.FieldPath(path); err == nil && owned && ok {
			facts.Assign(path, field.Type)
		}
	}
	assume := func(condition string, truth bool) {
		for _, path := range workflowexpr.ConditionPresenceFacts(condition, truth) {
			if strings.HasPrefix(path, "entity.") {
				facts[strings.TrimPrefix(path, "entity.")] = struct{}{}
			}
		}
	}
	assumeOutcome := func() {
		for _, condition := range outcome.conditions {
			assume(condition.expression, condition.truth)
		}
	}
	writes := func(spec c.WorkflowDataAccumulation, selected bool) bool {
		for index, write := range spec.Writes {
			if point != nil && point.Step == StepDataWrites && point.HasWriteIndex && (point.RuleKind != "") == selected && point.WriteIndex == index {
				return false
			}
			facts.TransferWrite(a.entity, write)
		}
		return true
	}
	topWritten, ruleWritten, projected := false, false, false
	applyWrites := func(selected bool) bool {
		if selected && outcome.rule != nil && !ruleWritten {
			if !writes(outcome.rule.DataAccumulation, true) {
				return false
			}
			ruleWritten = true
		}
		if !topWritten {
			if !writes(handler.DataAccumulation, false) {
				return false
			}
			topWritten = true
		}
		return true
	}
	applyProjection := func() {
		if projected {
			return
		}
		bindings, _ := accprojection.ForExecutableHandler(a.source, node, event)
		for _, binding := range bindings {
			assign("entity." + binding.TargetField)
		}
		projected = true
	}
	fanOut := func(step Step, selected bool) bool {
		kind, index := c.FanOutSiteHandler, -1
		if selected {
			index = outcome.index
			switch outcome.kind {
			case c.HandlerAdvanceCarrierRules:
				kind = c.FanOutSiteRule
			case c.HandlerAdvanceCarrierOnComplete:
				kind = c.FanOutSiteOnComplete
			default:
				return true
			}
		}
		site, err := c.NewFanOutSiteRef(node, event, kind, index)
		if err != nil || a.source == nil {
			return true
		}
		if _, found := a.source.FanOutPlanForSite(site); !found {
			return true
		}
		if point != nil && point.Step == step && point.FanOut && !point.FanOutAfterWrites {
			return false
		}
		if !applyWrites(selected) {
			return false
		}
		applyProjection()
		return point == nil || point.Step != step || !point.FanOut
	}
	for _, step := range OrderedSteps {
		if point != nil && point.Step == step && !point.HasWriteIndex && !point.FanOut && !point.RuleCompute && !point.HasGuardCheckIndex && !point.PolicyCompute {
			if outcome.rule != nil && entityAssignmentSelectionStep(outcome.kind) == step {
				if point.Condition {
					for _, condition := range outcome.conditions[:len(outcome.conditions)-1] {
						assume(condition.expression, condition.truth)
					}
				} else {
					assumeOutcome()
				}
			}
			return facts
		}
		switch step {
		case StepQuery:
			if handler.Query != nil {
				assign(handler.Query.StoreAs)
			}
		case StepGuard:
			for index, check := range handler.Guard.EffectiveChecks() {
				if point != nil && point.HasGuardCheckIndex && point.GuardCheckIndex == index {
					return facts
				}
				assume(check.Check, true)
			}
		case StepFilter:
			if handler.Filter != nil {
				assign(handler.Filter.StoreAs)
			}
		case StepGroupBy:
			if handler.GroupBy != nil {
				assign(handler.GroupBy.StoreAs)
			}
		case StepReduce:
			if handler.Reduce != nil {
				assign(handler.Reduce.StoreAs)
			}
		case StepCount:
			if handler.Count != nil {
				assign(handler.Count.StoreAs)
			}
		case StepCompute:
			var selected *c.HandlerRuleEntry
			if outcome.rule != nil && entityAssignmentSelectionStep(outcome.kind) == StepJoin {
				if point != nil && point.Step == step && point.RuleCompute {
					return facts
				}
				selected = outcome.rule
			}
			for _, site := range handlerComputations(handler, selected) {
				if point != nil && point.PolicyCompute && site.ruleIndex == point.RuleIndex {
					return facts
				}
				assign(site.spec.StoreAs)
			}
		case StepFanOut:
			if outcome.rule == nil || entityAssignmentSelectionStep(outcome.kind) != StepJoin {
				if !fanOut(step, false) {
					return facts
				}
			}
		case StepJoin, StepOnComplete, StepRules:
			if outcome.rule != nil && entityAssignmentSelectionStep(outcome.kind) == step {
				assumeOutcome()
				if step != StepJoin {
					if !fanOut(step, true) {
						return facts
					}
					if point != nil && point.Step == step && point.RuleCompute {
						return facts
					}
				}
				if step != StepJoin && outcome.rule.Compute != nil {
					assign(outcome.rule.Compute.StoreAs)
				}
			}
		case StepDataWrites:
			if !applyWrites(true) {
				return facts
			}
		case StepProjection:
			applyProjection()
		case StepAction:
			ruleSource := handlerRuleSourceNone
			if outcome.kind == c.HandlerAdvanceCarrierRules {
				ruleSource = handlerRuleSourceRules
			}
			action := selectedActionSpec(handler, outcome.rule, ruleSource)
			if action.ArtifactRepo != nil {
				// Artifact success, handled failure and duplicate recovery have
				// different outputs. None preserves optional descendants of a
				// replaced value merely because they existed before the action.
				for _, target := range action.ArtifactRepo.Output.Fields() {
					path, owned, err := entityruntime.EntityWritePath(target)
					if err != nil || !owned {
						continue
					}
					present := facts.Has(path)
					facts.Forget(path)
					if present {
						assign(target)
					}
				}
			}
		case StepClear:
			if handler.Clear == nil {
				continue
			}
			for _, target := range handler.Clear.Targets {
				if target != "accumulator_state" {
					continue
				}
				for _, binding := range accprojection.Resolve(a.source).Bindings {
					if binding.SourceNode.Equal(node) {
						assign("entity." + binding.TargetField)
					}
				}
			}
		}
	}
	return facts
}

func entityAssignmentStepAfter(current, before Step) bool {
	seen := false
	for _, step := range OrderedSteps {
		if step == current {
			return seen
		}
		if step == before {
			seen = true
		}
	}
	return false
}

func entityAssignmentSelectionStep(kind c.HandlerAdvanceCarrierKind) Step {
	switch kind {
	case c.HandlerAdvanceCarrierJoinOnComplete, c.HandlerAdvanceCarrierJoinTimeout:
		return StepJoin
	case c.HandlerAdvanceCarrierOnComplete:
		return StepOnComplete
	default:
		return StepRules
	}
}

func entityAssignmentOutcomes(handler c.SystemNodeEventHandler) []entityAssignmentOutcome {
	var out []entityAssignmentOutcome
	var prefix []entityAssignmentCondition
	appendRules := func(kind c.HandlerAdvanceCarrierKind, rules []c.HandlerRuleEntry) bool {
		for index := range rules {
			if !handlerRuleSelectable(rules[index]) {
				continue
			}
			conditions := append([]entityAssignmentCondition{}, prefix...)
			conditions = append(conditions, entityAssignmentCondition{expression: rules[index].Condition, truth: true})
			out = append(out, entityAssignmentOutcome{kind: kind, index: index, rule: &rules[index], conditions: conditions})
			if handlerRuleUnconditional(rules[index]) {
				return true
			}
			prefix = append(prefix, entityAssignmentCondition{expression: rules[index].Condition, truth: false})
		}
		return false
	}
	exhaustive := appendRules(c.HandlerAdvanceCarrierOnComplete, handler.OnComplete)
	if !exhaustive {
		exhaustive = appendRules(c.HandlerAdvanceCarrierRules, handler.Rules)
	}
	if !exhaustive {
		out = append(out, entityAssignmentOutcome{conditions: append([]entityAssignmentCondition{}, prefix...)})
	}
	if handler.Join != nil {
		prefix = nil
		appendRules(c.HandlerAdvanceCarrierJoinOnComplete, []c.HandlerRuleEntry{handler.Join.OnComplete})
		prefix = nil
		appendRules(c.HandlerAdvanceCarrierJoinTimeout, []c.HandlerRuleEntry{handler.Join.Timeout.Outcome})
	}
	return out
}

// Selection and assignment analysis share the exact executable-row boundary.
func handlerRuleSelectable(rule c.HandlerRuleEntry) bool {
	switch rule.PolicyRow.Kind {
	case c.PolicySheetRowKindLookup, c.PolicySheetRowKindValidate, c.PolicySheetRowKindModule:
		return false
	default:
		return true
	}
}

func handlerRuleUnconditional(rule c.HandlerRuleEntry) bool {
	condition := strings.TrimSpace(rule.Condition)
	return condition == "" || strings.EqualFold(condition, "else")
}

func entityAssignmentGuardAllows(handler c.SystemNodeEventHandler, stage string) bool {
	for _, check := range handler.Guard.EffectiveChecks() {
		if !workflowexpr.ConditionCanHoldAtStage(check.Check, stage, true) {
			return false
		}
	}
	return true
}

func entityAssignmentOutcomeAllows(outcome entityAssignmentOutcome, stage string) bool {
	for _, condition := range outcome.conditions {
		if !workflowexpr.ConditionCanHoldAtStage(condition.expression, stage, condition.truth) {
			return false
		}
	}
	return true
}

func entityAssignmentDestination(handler c.SystemNodeEventHandler, outcome entityAssignmentOutcome) string {
	if outcome.rule != nil && outcome.rule.AdvancesTo != "" {
		return outcome.rule.AdvancesTo
	}
	return handler.AdvancesTo
}

func entityAssignmentEdgeSelects(edge c.WorkflowStageTopologyEdge, handler c.SystemNodeEventHandler, outcome entityAssignmentOutcome) bool {
	if entityAssignmentDestination(handler, outcome) != edge.To {
		return false
	}
	if edge.CarrierKind == c.HandlerAdvanceCarrierHandler {
		return outcome.rule == nil || outcome.rule.AdvancesTo == ""
	}
	return edge.CarrierKind == outcome.kind && edge.RuleIndex == outcome.index
}

func EntityAssignmentPresencePaths(facts entityruntime.AssignmentFacts) []string {
	paths := make([]string, 0, len(facts))
	for path := range facts {
		paths = append(paths, "entity."+path)
	}
	sort.Strings(paths)
	return paths
}
