package bootverify

import (
	"fmt"
	"regexp"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

const joinValidationCheckID = "join_validation"

var joinPolicyDelayPattern = regexp.MustCompile(`^\{\{\s*([a-zA-Z_][a-zA-Z0-9_.-]*)\s*\}\}(ms|s|m|h|d)$`)

func checkJoinValidation(c *checkerContext) []Finding {
	if c == nil || c.source == nil {
		return nil
	}
	findings := make([]Finding, 0)
	seenIDs := map[string]string{}
	for _, record := range wave1ScopedNodeRecords(c.source) {
		nodeID := strings.TrimSpace(record.LogicalID)
		flowID := strings.TrimSpace(record.Source.FlowPath)
		nodeRef, nodeErr := record.Identity()
		declarationLocation := nodeID
		if nodeErr == nil {
			declarationLocation = nodeRef.Key()
		}
		node := record.Entry
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if handler.Join == nil {
				continue
			}
			if err := runtimecontracts.ValidateJoinHandlerIsolation(handler); err != nil {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, err.Error()))
			}
			if err := handler.Join.ValidateAuthoredShape(); err != nil {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, err.Error()))
			}
			if err := runtimecontracts.ValidateJoinClosedOutcomeScope(handler); err != nil {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, err.Error()))
			}
			if nodeErr != nil {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, "join has incomplete executable node identity: "+nodeErr.Error()))
				continue
			}
			if handler.Loop == nil {
				for _, expression := range handlerExecutableReaderExpressionsForSource(c.source, nodeRef, eventType, handler) {
					if expression.AllowJoin && workflowexpr.ExpressionReferencesRoot(expression.Expression, "loop") {
						findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, expression.Kind+" requires a loop-owned join for captured loop.* context"))
					}
				}
			}
			compiledPlan, found := semanticview.WorkflowJoinPlanForExecutionHandler(c.source, nodeRef, eventType, *handler.Join)
			if !found {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, "join has no effective WorkflowJoinPlan; reload the workflow contract and declare exactly one canonical join row"))
				continue
			}
			var ref timeridentity.JoinRef
			var refErr error
			switch compiledPlan.Mode {
			case runtimecontracts.WorkflowJoinModeArrival:
				ref, refErr = timeridentity.NewJoinRef(nodeRef, eventType, handler.Join.Stage, handler.Join.EffectiveID())
			case runtimecontracts.WorkflowJoinModeFanOutDelivery:
				fanOutDeclaration, identityErr := compiledPlan.FanOut.FanOut.ElementRef.DeclarationIdentity()
				if identityErr != nil {
					refErr = identityErr
				} else {
					ref, refErr = timeridentity.NewFanOutDeliveryJoinRef(
						nodeRef, eventType, handler.Join.EffectiveID(), fanOutDeclaration,
						compiledPlan.FanOut.FanOut.BundleHash, compiledPlan.FanOut.FanOut.SemanticDigest,
					)
				}
			default:
				refErr = fmt.Errorf("join has invalid compiled mode")
			}
			if refErr != nil {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, "join has incomplete declaration identity: "+refErr.Error()))
				continue
			}
			plan, ok := semanticview.WorkflowJoinPlanForRef(c.source, ref)
			if !ok {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, "join has no effective WorkflowJoinPlan; reload the workflow contract and declare exactly one canonical join row"))
				continue
			}
			spec := plan.Spec
			resultType := plan.ResultType
			prefix := fmt.Sprintf("join %s", spec.EffectiveID())
			identityKey := ref.Declaration().Key()
			duplicateLocation := nodeID + ":" + eventType
			if prior, duplicate := seenIDs[identityKey]; duplicate {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, fmt.Sprintf("%s has duplicate effective identity; already declared at %s (add explicit unique id values)", prefix, prior)))
			} else {
				seenIDs[identityKey] = duplicateLocation
			}
			if plan.Mode == runtimecontracts.WorkflowJoinModeFanOutDelivery {
				if !spec.OnCompleteFound || joinRuleEmpty(spec.OnComplete) {
					findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, prefix+" requires a non-empty on_complete outcome"))
				}
				findings = append(findings, validateJoinOutcome(c.source, declarationLocation, flowID, nodeID, eventType, "on_complete", spec.OnComplete, compiledStageIDsForFlow(c.source, flowID), resultType, true)...)
				continue
			}
			if !flowUsesAuthoredStages(c.source, flowID) {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, prefix+" requires an authored stages: lifecycle"))
			}
			if spec.Stage == "" || !containsString(compiledStageIDsForFlow(c.source, flowID), spec.Stage) {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, fmt.Sprintf("%s references unknown stage %q", prefix, spec.Stage)))
			}
			findings = append(findings, c.validateJoinPaths(declarationLocation, flowID, nodeID, eventType, spec)...)
			if !spec.OnCompleteFound || joinRuleEmpty(spec.OnComplete) {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, prefix+" requires a non-empty on_complete outcome"))
			}
			if spec.Deadline != nil && !joinDelayValid(c.source, flowID, spec.Deadline.After) {
				findings = append(findings, joinFinding(declarationLocation, flowID, nodeID, eventType, fmt.Sprintf("%s deadline.after %q must be a positive duration or resolved policy-scalar duration", prefix, spec.Deadline.After)))
			}
			findings = append(findings, validateJoinOutcome(c.source, declarationLocation, flowID, nodeID, eventType, "on_complete", spec.OnComplete, compiledStageIDsForFlow(c.source, flowID), resultType, false, spec.Members.Count != nil)...)
			if spec.Deadline != nil {
				findings = append(findings, validateJoinOutcome(c.source, declarationLocation, flowID, nodeID, eventType, "on_deadline", spec.OnDeadline, compiledStageIDsForFlow(c.source, flowID), resultType, false, spec.Members.Count != nil)...)
			}
		}
	}
	return findings
}

func (c *checkerContext) validateJoinPaths(location, flowID, nodeID, eventType string, spec runtimecontracts.JoinSpec) []Finding {
	out := make([]Finding, 0, 5)
	if spec.Members.Count == nil {
		memberField := joinPathField(spec.Members.From, "state")
		entityType, err := semanticview.ResolveEntityStructuralType(c.source, flowID)
		if memberField == "" {
			out = append(out, joinFinding(location, flowID, nodeID, eventType, "join.members.from must be a top-level state.<field> path"))
		} else if err != nil || entityType == nil {
			out = append(out, joinFinding(location, flowID, nodeID, eventType, "join.members.from requires a catalog-typed instance state"))
		} else if field, ok := entityType.Field(memberField); !ok {
			out = append(out, joinFinding(location, flowID, nodeID, eventType, fmt.Sprintf("join.members.from references undeclared state field %s", memberField)))
		} else if bundle, ok := semanticview.Bundle(c.source); ok {
			primary, primaryErr := bundle.ResolveFlowPrimaryEntity(flowID)
			projection, projectionErr := runtimecontracts.AdmitCollectionProjection(runtimecontracts.CatalogTypeReference{Type: field.TypeRef, Catalog: primary.Types})
			if primaryErr != nil || projectionErr != nil || projection.ItemType().Kind != runtimecontracts.CatalogTypeText {
				out = append(out, joinFinding(location, flowID, nodeID, eventType, "join.members.from requires list<text> or map[text]T"))
			}
		}
	}
	proof := semanticview.ResolveFlowEventProof(c.source, flowID, eventType)
	memberBy := joinPathField(spec.Members.By, "payload")
	if memberBy == "" {
		out = append(out, joinFinding(location, flowID, nodeID, eventType, "join.members.by must be a top-level payload.<field> path"))
	} else if !proof.HasSchema || !joinPayloadFieldIsText(c.source, flowID, eventType, memberBy) {
		out = append(out, joinFinding(location, flowID, nodeID, eventType, fmt.Sprintf("join.members.by must reference a declared text payload field %s", memberBy)))
	}
	output := joinPathField(spec.Output, "payload")
	if output == "" {
		out = append(out, joinFinding(location, flowID, nodeID, eventType, "join.output must be a top-level payload.<field> path"))
	} else if _, ok := proof.Entry.Payload.Properties[output]; !proof.HasSchema || !ok {
		out = append(out, joinFinding(location, flowID, nodeID, eventType, fmt.Sprintf("join.output references undeclared payload field %s", output)))
	}
	if spec.Until != "" {
		untilProof := semanticview.ResolveFlowEventProof(c.source, flowID, spec.Until)
		if !untilProof.HasSchema {
			out = append(out, joinFinding(location, flowID, nodeID, eventType, "join.until requires a catalog-declared event"))
		} else if eventidentity.MatchPattern(c.source.ResolveFlowEventPattern(flowID, eventType), untilProof.Canonical) {
			out = append(out, joinFinding(location, flowID, nodeID, eventType, "join.until must be distinct from the member-arrival event"))
		}
	}
	return out
}

func validateJoinOutcome(source semanticview.Source, location, flowID, nodeID, eventType, label string, rule runtimecontracts.HandlerRuleEntry, states []string, resultType runtimecontracts.CatalogTypeReference, fanOutDelivery bool, countArrival ...bool) []Finding {
	out := make([]Finding, 0)
	context := workflowexpr.JoinContextArrival
	if fanOutDelivery {
		context = workflowexpr.JoinContextFanOutDelivery
	} else if len(countArrival) > 0 && countArrival[0] {
		context = workflowexpr.JoinContextCountArrival
	}
	if target := strings.TrimSpace(rule.AdvancesTo); target != "" && !containsString(states, target) {
		out = append(out, joinFinding(location, flowID, nodeID, eventType, fmt.Sprintf("join.%s advances_to references unknown stage %s", label, target)))
	}
	for field, expr := range rule.Emit.Fields {
		if text := joinExpressionText(expr); text != "" {
			out = append(out, validateJoinExpression(source, location, flowID, nodeID, eventType, label+" emit.fields."+field, text, resultType, context)...)
		}
	}
	for idx, write := range rule.DataAccumulation.Writes {
		if text := joinExpressionText(write.Value); text != "" {
			out = append(out, validateJoinExpression(source, location, flowID, nodeID, eventType, fmt.Sprintf("%s data_accumulation.writes[%d]", label, idx), text, resultType, context)...)
		}
	}
	return out
}

func validateJoinExpression(source semanticview.Source, location, flowID, nodeID, eventType, label, expression string, resultType runtimecontracts.CatalogTypeReference, context workflowexpr.JoinContext) []Finding {
	entityType, _ := semanticview.ResolveEntityStructuralType(source, flowID)
	options := workflowexpr.ValueExpressionOptions{EntityType: entityType, AllowJoin: true, JoinResultType: resultType, JoinContext: context}
	if err := workflowexpr.ValidateValueExpressionWithOptions(expression, options); err != nil {
		return []Finding{joinFinding(location, flowID, nodeID, eventType, fmt.Sprintf("join.%s expression %q is invalid: %v", label, expression, err))}
	}
	return nil
}

func joinFinding(location, flowID, nodeID, eventType, detail string) Finding {
	return NewHardInvalidityFinding(joinValidationCheckID, location,
		fmt.Sprintf("flow %s node %s handler %s: %s", defaultFlowLabel(flowID), nodeID, eventType, detail),
		"Use the canonical staged handler.join contract with explicit or bounded-count membership, stage-entry closure, and typed state/join/captured-loop outcomes.")
}

func joinRuleEmpty(rule runtimecontracts.HandlerRuleEntry) bool {
	return strings.TrimSpace(rule.AdvancesTo) == "" && rule.Emit.Empty() && !rule.DataAccumulation.HasWrites()
}

func joinPathField(path, root string) string {
	path = strings.TrimSpace(path)
	prefix := root + "."
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	field := strings.TrimPrefix(path, prefix)
	if field == "" || strings.Contains(field, ".") {
		return ""
	}
	return field
}

func joinPayloadFieldIsText(source semanticview.Source, flowID, eventType, field string) bool {
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return false
	}
	typeRef, found := runtimecontracts.ResolveEventFieldType(bundle, flowID, eventType, field)
	resolved, err := typeRef.Resolve()
	return found && err == nil && resolved.Kind == runtimecontracts.CatalogTypeText
}

func joinExpressionText(expr runtimecontracts.ExpressionValue) string {
	if expr.Kind == runtimecontracts.ExpressionKindRef {
		return strings.TrimSpace(expr.Ref)
	}
	if expr.Kind == runtimecontracts.ExpressionKindCEL {
		return strings.TrimSpace(expr.CEL)
	}
	return ""
}

func joinDelayValid(source semanticview.Source, flowID, raw string) bool {
	raw = strings.TrimSpace(raw)
	if _, ok := timeridentity.ParseDelayDuration(raw); ok {
		return true
	}
	match := joinPolicyDelayPattern.FindStringSubmatch(raw)
	if len(match) != 3 {
		return false
	}
	value, ok := source.ResolvedPolicyForFlow(flowID).Values[match[1]]
	if !ok {
		return false
	}
	_, ok = timeridentity.ParseDelayDuration(fmt.Sprint(value.Value) + match[2])
	return ok
}
