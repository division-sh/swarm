package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

type WorkflowJoinPlan struct {
	Node                        runtimeidentity.ExecutableNode
	HandlerEvent                string
	Mode                        WorkflowJoinMode
	Spec                        JoinSpec
	ResultType                  CatalogTypeReference
	MembersCollectionProjection CollectionProjection
	UntilEvent                  string
	FanOut                      WorkflowFanOutDeliveryJoinPlan
}

type WorkflowFanOutDeliveryJoinPlan struct {
	FanOut FanOutPlanRef
}

// CompileWorkflowJoinPlan admits membership and closure declarations, not an
// arm. Lifecycle supplies the exact stage-entry occurrence at runtime.
func (b *WorkflowContractBundle) CompileWorkflowJoinPlan(node runtimeidentity.ExecutableNode, eventType string, handler SystemNodeEventHandler) (WorkflowJoinPlan, error) {
	if b == nil || !node.Valid() || strings.TrimSpace(eventType) == "" || handler.Join == nil {
		return WorkflowJoinPlan{}, fmt.Errorf("join requires an exact node and handler declaration")
	}
	if err := handler.Join.ValidateAuthoredShape(); err != nil {
		return WorkflowJoinPlan{}, err
	}
	if err := ValidateJoinHandlerIsolation(handler); err != nil {
		return WorkflowJoinPlan{}, err
	}
	if err := rejectEventlessEmitSpecs(handler); err != nil {
		return WorkflowJoinPlan{}, err
	}
	spec := *handler.Join
	spec.Members.FromPath = joinMembersSourcePath(spec.Members.From)
	spec.Members.ByPath = paths.Parse(spec.Members.By)
	spec.OutputPath = paths.Parse(spec.Output)
	plan := WorkflowJoinPlan{Node: node, HandlerEvent: strings.TrimSpace(eventType), Mode: spec.Mode(), Spec: spec}
	if spec.IsFanOutDeliveryBarrier() {
		qualified, err := QualifySystemNodeHandlerRuleRefsForEvent(node, eventType, handler)
		if err != nil {
			return WorkflowJoinPlan{}, err
		}
		actual, actualFound := handler.FanOut.DeclarationIdentity()
		expected, _ := qualified.FanOut.DeclarationIdentity()
		if !actualFound || !actual.Equal(expected) {
			return WorkflowJoinPlan{}, fmt.Errorf("fan-out delivery join requires the exact same-handler top-level compiled fan_out site")
		}
		fanOut, err := b.CompileFanOutPlan(node, eventType, handler, WorkflowFanOutSite{
			Source: "handler.fan_out", Kind: FanOutSiteHandler, Index: -1, Spec: handler.FanOut, Writes: handler.DataAccumulation.Writes,
		})
		if err != nil {
			return WorkflowJoinPlan{}, err
		}
		plan.FanOut = WorkflowFanOutDeliveryJoinPlan{FanOut: fanOut.Ref}
		return plan.Clone(), nil
	}
	if spec.Members.Count == nil {
		field := joinTopLevelField(spec.Members.From, "state")
		primary, err := b.ResolveFlowPrimaryEntity(node.FlowPath())
		if err != nil {
			return WorkflowJoinPlan{}, fmt.Errorf("join.members.from state owner: %w", err)
		}
		if field == "" {
			return WorkflowJoinPlan{}, fmt.Errorf("join.members.from requires a catalog-declared top-level state.<field>")
		}
		declaration, ok := primary.Contract.Fields[field]
		if !ok {
			return WorkflowJoinPlan{}, fmt.Errorf("join.members.from references undeclared state field %s", field)
		}
		projection, err := AdmitCollectionProjection(CatalogTypeReference{Type: declaration.Type, Catalog: primary.Types})
		if err != nil || projection.ItemType().Kind != CatalogTypeText {
			return WorkflowJoinPlan{}, fmt.Errorf("join.members.from requires list<text> or map[text]T: %v", err)
		}
		plan.MembersCollectionProjection = projection
	}
	by := joinTopLevelField(spec.Members.By, "payload")
	byType, found := ResolveExecutableNodeEventFieldType(b, node, eventType, by)
	resolvedBy, err := byType.Resolve()
	if by == "" || !found || err != nil || resolvedBy.Kind != CatalogTypeText {
		return WorkflowJoinPlan{}, fmt.Errorf("join.members.by requires a catalog-declared text payload.<field> (field=%q, declared=%v, type=%s, error=%v)", by, found, resolvedBy.Kind, err)
	}
	output := joinTopLevelField(spec.Output, "payload")
	resultType, found := ResolveExecutableNodeEventFieldType(b, node, eventType, output)
	resolvedResult, err := resultType.Resolve()
	if output == "" || !found || err != nil || resolvedResult.Kind == CatalogTypeDynamic {
		return WorkflowJoinPlan{}, fmt.Errorf("join.output requires a catalog-typed payload.<field>")
	}
	plan.ResultType = resultType
	if spec.Until != "" {
		if _, _, found := b.ResolveExecutableNodeEventCatalogEntry(node, spec.Until); !found {
			return WorkflowJoinPlan{}, fmt.Errorf("join.until requires a catalog-declared event")
		}
		plan.UntilEvent = b.ResolveExecutableNodeEventReference(node, spec.Until)
		if eventidentity.MatchPattern(b.ResolveExecutableNodeEventPattern(node, eventType), plan.UntilEvent) {
			return WorkflowJoinPlan{}, fmt.Errorf("join.until must be distinct from the member-arrival event")
		}
	}
	return plan.Clone(), nil
}

func joinTopLevelField(path, root string) string {
	field, found := strings.CutPrefix(strings.TrimSpace(path), root+".")
	if !found || field == "" || strings.ContainsAny(field, ".[]() ") {
		return ""
	}
	return field
}

func joinMembersSourcePath(raw string) paths.Path {
	if field := joinTopLevelField(raw, "state"); field != "" {
		return paths.Parse("entity." + field)
	}
	return paths.Parse(raw)
}
