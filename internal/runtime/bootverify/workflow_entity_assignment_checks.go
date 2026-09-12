package bootverify

import (
	"fmt"
	"strings"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func (checker *checkerContext) entityAssignmentReaders(node identity.ExecutableNode, event string, handler c.SystemNodeEventHandler) []expressionReference {
	readers := handlerExecutableReaderExpressionsForSource(checker.source, node, event, handler)
	analysis := checker.entityAssignmentAnalysis(node.FlowPath())
	if analysis == nil {
		return readers
	}
	return checker.annotateEntityAssignmentReaders(node, event, handler, analysis, readers)
}

func (checker *checkerContext) entityAssignmentAnalysis(flowID string) *engine.EntityAssignmentAnalysis {
	if checker.entityAssignments == nil {
		checker.entityAssignments = map[string]*engine.EntityAssignmentAnalysis{}
	}
	analysis, cached := checker.entityAssignments[flowID]
	if !cached {
		// Entityless flows have no entity reads to prove; unresolved entity
		// declarations remain hard errors in the declaration/reference checks.
		analysis, _ = engine.BuildEntityAssignmentAnalysis(checker.source, flowID)
		checker.entityAssignments[flowID] = analysis
	}
	return analysis
}

func (checker *checkerContext) annotateEntityAssignmentReaders(node identity.ExecutableNode, event string, handler c.SystemNodeEventHandler, analysis *engine.EntityAssignmentAnalysis, readers []expressionReference) []expressionReference {
	for index := range readers {
		reader := &readers[index]
		if reader.CommittedStage != "" {
			// Join membership/window snapshots are evaluated by lifecycle planning
			// after the entering handler's writes, not by the arrival handler.
			reader.KnownPresence = engine.EntityAssignmentPresencePaths(analysis.StageFacts(reader.CommittedStage))
			if missing := workflowexpr.RequiredEntityReferences(reader.Expression, reader.KnownPresence); len(missing) != 0 {
				reader.AssignmentError = fmt.Sprintf("entity paths %s are not definitely assigned at stage %s entry", strings.Join(missing, ", "), reader.CommittedStage)
			}
			continue
		}
		point, err := entityAssignmentReaderPoint(*reader)
		if err != nil {
			if len(workflowexpr.EntityReferences(reader.Expression)) != 0 {
				reader.AssignmentError = err.Error()
			}
			continue
		}
		if reader.HasRuleIndex && reader.RuleCollection == "rules" && reader.RuleField == "Compute" && reader.RuleIndex < len(handler.Rules) {
			switch handler.Rules[reader.RuleIndex].PolicyRow.Kind {
			case c.PolicySheetRowKindLookup, c.PolicySheetRowKindValidate, c.PolicySheetRowKindModule:
				point.Step, point.RuleKind, point.RuleCompute, point.PolicyCompute = engine.StepCompute, "", false, true
			}
		}
		facts := analysis.BeforeRead(node, event, handler, point)
		reader.KnownPresence = engine.EntityAssignmentPresencePaths(facts)
		if len(reader.RequiredEntityPaths) != 0 {
			// These are typed operation prerequisites, not invented CEL reads of
			// map-value paths. Target/type admission remains in mutation checking.
			var absent []string
			for _, path := range reader.RequiredEntityPaths {
				if !facts.Has(path) {
					absent = append(absent, "entity."+path)
				}
			}
			if len(absent) != 0 {
				reader.AssignmentError = fmt.Sprintf("entity paths %s are not definitely assigned before %s", strings.Join(absent, ", "), point.Step)
			}
			continue
		}
		required := analysis.UnassignedReads(node, event, handler, point, reader.Expression)
		missing := map[string]bool{}
		for _, path := range required {
			missing[path] = true
		}
		for _, path := range workflowexpr.RequiredEntityReferences(reader.Expression, reader.KnownPresence) {
			if !missing[path] {
				reader.KnownPresence = append(reader.KnownPresence, "entity."+path)
			}
		}
		if len(required) != 0 {
			for i := range required {
				required[i] = "entity." + required[i]
			}
			reader.AssignmentError = fmt.Sprintf("entity paths %s are not definitely assigned before %s; assign them on every reaching stage/outcome before this read, or explicitly decide absence with has/optional selection", strings.Join(required, ", "), point.Step)
		}
	}
	return readers
}

func entityAssignmentReaderPoint(reader expressionReference) (engine.EntityAssignmentPoint, error) {
	point := engine.EntityAssignmentPoint{
		WriteIndex: reader.WriteIndex, HasWriteIndex: reader.HasWriteIndex,
		GuardCheckIndex: reader.GuardCheckIndex, HasGuardCheckIndex: reader.HasGuardCheckIndex,
	}
	if reader.EmitSite != nil {
		site := reader.EmitSite
		point.Step = engine.StepEmits
		point.RuleIndex = site.RuleIndex
		switch site.Source {
		case "handler.emit", "handler.on_success":
		case "handler.rules.emit":
			point.RuleKind = c.HandlerAdvanceCarrierRules
		case "handler.on_complete.emit":
			point.RuleKind = c.HandlerAdvanceCarrierOnComplete
		case "handler.join.on_complete.emit":
			point.RuleKind = c.HandlerAdvanceCarrierJoinOnComplete
		case "handler.join.timeout.emit":
			point.RuleKind = c.HandlerAdvanceCarrierJoinTimeout
		case "handler.fan_out.emit":
			point.Step = engine.StepFanOut
			point.FanOut, point.FanOutAfterWrites = true, true
		case "handler.rules.fan_out.emit":
			point.Step, point.RuleKind = engine.StepRules, c.HandlerAdvanceCarrierRules
			point.FanOut, point.FanOutAfterWrites = true, true
		case "handler.on_complete.fan_out.emit":
			point.Step, point.RuleKind = engine.StepOnComplete, c.HandlerAdvanceCarrierOnComplete
			point.FanOut, point.FanOutAfterWrites = true, true
		default:
			return point, fmt.Errorf("emit site %s has no assignment program point", site.Source)
		}
		return point, nil
	}
	field := reader.HandlerField
	if reader.HasRuleIndex {
		point.RuleIndex = reader.RuleIndex
		field = reader.RuleField
		switch reader.RuleCollection {
		case "rules":
			point.RuleKind = c.HandlerAdvanceCarrierRules
		case "on_complete":
			point.RuleKind = c.HandlerAdvanceCarrierOnComplete
		case "join.on_complete":
			point.RuleKind = c.HandlerAdvanceCarrierJoinOnComplete
		case "join.timeout":
			point.RuleKind = c.HandlerAdvanceCarrierJoinTimeout
		default:
			return point, fmt.Errorf("rule collection %s has no assignment program point", reader.RuleCollection)
		}
	}
	if reader.Phase == pipeline.WorkflowEntityFieldLifecycleGuardEscalation {
		point.Step = engine.StepGuard
		return point, nil
	}
	switch field {
	case "Query":
		point.Step = engine.StepQuery
	case "Guard":
		point.Step = engine.StepGuard
	case "Join":
		point.Step = engine.StepJoin
	case "Accumulate":
		point.Step = engine.StepAccumulate
	case "Filter":
		point.Step = engine.StepFilter
	case "GroupBy":
		point.Step = engine.StepGroupBy
	case "Reduce":
		point.Step = engine.StepReduce
	case "Count":
		point.Step = engine.StepCount
	case "Compute":
		point.Step = engine.StepCompute
	case "FanOut":
		point.Step = engine.StepFanOut
		point.FanOut, point.FanOutAfterWrites = true, reader.FanOutAfterWrites
	case "Condition", "Logic":
		point.Step, point.Condition = engine.StepRules, true
	case "DataAccumulation":
		point.Step = engine.StepDataWrites
	case "Action":
		point.Step = engine.StepAction
	case "Activity":
		point.Step = engine.StepActivity
	default:
		return point, fmt.Errorf("handler field %s has no assignment program point", field)
	}
	if point.RuleKind != "" && (field == "Condition" || field == "Compute" || field == "FanOut") {
		point.RuleCompute = field == "Compute"
		switch point.RuleKind {
		case c.HandlerAdvanceCarrierRules:
			point.Step = engine.StepRules
		case c.HandlerAdvanceCarrierOnComplete:
			point.Step = engine.StepOnComplete
		default:
			point.Step = engine.StepJoin
			if field == "Compute" {
				point.Step = engine.StepCompute
			}
		}
	}
	return point, nil
}
