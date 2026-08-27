package bootverify

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type SingletonCoordinatorDemand struct {
	FlowID        string
	Node          runtimeidentity.ExecutableNode
	SourceFile    string
	Location      string
	AgentID       string
	EventType     string
	Kind          string
	Field         string
	Target        string
	Operation     string
	WriteIndex    int
	HasWriteIndex bool
}

func (d SingletonCoordinatorDemand) Detail() string {
	parts := []string{"flow " + defaultFlowLabel(d.FlowID)}
	if d.Node.Valid() {
		parts = append(parts, "node "+d.Node.Key())
	}
	if d.AgentID != "" {
		parts = append(parts, "agent "+d.AgentID)
	}
	if d.EventType != "" {
		parts = append(parts, "handler "+d.EventType)
	}
	if d.Kind != "" {
		parts = append(parts, d.Kind)
	}
	if d.HasWriteIndex {
		parts = append(parts, fmt.Sprintf("write[%d]", d.WriteIndex))
	}
	if d.Operation != "" {
		parts = append(parts, "op "+d.Operation)
	}
	if d.Target != "" {
		parts = append(parts, "target "+d.Target)
	} else if d.Field != "" {
		parts = append(parts, "field entity."+d.Field)
	}
	return strings.Join(parts, " ")
}

type singletonDemandFlow struct {
	fields map[string]runtimecontracts.SingletonCoordinatorContainedField
}

// BuildSingletonCoordinatorDemandProjection derives coordinator authority from
// exact contract consumers. Typed map/list declaration shape alone is inert.
func BuildSingletonCoordinatorDemandProjection(source semanticview.Source) []SingletonCoordinatorDemand {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil
	}
	flows := map[string]singletonDemandFlow{}
	for flowID, schema := range source.FlowSchemaEntries() {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" || strings.TrimSpace(schema.Mode) != runtimecontracts.FlowModeSingleton {
			continue
		}
		singleton, err := bundle.ResolveFlowSingleton(flowID)
		if err != nil {
			continue
		}
		contained, err := runtimecontracts.SingletonContainedFields(singleton.PrimaryEntity)
		if err != nil {
			continue
		}
		fields := make(map[string]runtimecontracts.SingletonCoordinatorContainedField, len(contained))
		for _, field := range contained {
			fields[strings.TrimSpace(field.Name)] = field
		}
		flows[flowID] = singletonDemandFlow{fields: fields}
	}
	if len(flows) == 0 {
		return nil
	}

	demands := make([]SingletonCoordinatorDemand, 0)
	seen := map[string]struct{}{}
	add := func(d SingletonCoordinatorDemand, requireTypedField bool) {
		d.FlowID = strings.TrimSpace(d.FlowID)
		d.Field = strings.TrimSpace(d.Field)
		flow, singleton := flows[d.FlowID]
		if !singleton {
			return
		}
		if requireTypedField {
			if _, typed := flow.fields[d.Field]; !typed {
				return
			}
		}
		key := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%t", d.FlowID, d.SourceFile, d.Location, d.Node.Key(), d.AgentID, d.EventType, d.Kind, d.Field, d.Target, d.WriteIndex, d.HasWriteIndex)
		if _, duplicate := seen[key]; duplicate {
			return
		}
		seen[key] = struct{}{}
		demands = append(demands, d)
	}

	for _, target := range wave1AllEntityWriteTargets(source) {
		if !target.Entity || wave1SpecialClearTarget(target.Field) {
			continue
		}
		_, ownerFlowID, rootField, err := wave1ResolveWriteTargetPath(source, target)
		if err != nil {
			continue
		}
		add(SingletonCoordinatorDemand{
			FlowID: ownerFlowID, Node: target.Node, SourceFile: target.SourceFile, Location: target.nodeID(),
			EventType: target.EventType, Kind: target.Kind,
			Field: rootField, Target: target.Target, WriteIndex: target.WriteIndex, HasWriteIndex: target.HasWriteIndex,
		}, true)
	}

	for _, ref := range wave1ContainedStateOperations(source) {
		contract, ok := entityruntime.ResolveForFlow(source, ref.flowID())
		if !ok {
			continue
		}
		target, err := entityruntime.ResolveContainedOperationTarget(contract, ref.Write.Target(), string(ref.Write.Operation), !ref.Write.Key.IsZero(), !ref.Write.Index.IsZero())
		if err != nil {
			continue
		}
		add(SingletonCoordinatorDemand{
			FlowID: ref.flowID(), Node: ref.Node, SourceFile: ref.SourceFile, Location: ref.nodeID(),
			EventType: ref.EventType, Kind: "contained_operation." + ref.Kind,
			Field: target.RootField, Target: ref.Write.Target(), Operation: string(ref.Write.Operation),
			WriteIndex: ref.WriteIndex, HasWriteIndex: true,
		}, true)
	}

	for _, record := range wave1ScopedNodeRecords(source) {
		node, _ := record.Identity()
		flowID := node.FlowID()
		nodeID := node.Key()
		for eventType, handler := range record.Entry.EventHandlers {
			for _, expr := range handlerExecutableReaderExpressionsForSource(source, node, eventType, handler) {
				for _, ref := range singletonDemandEntityRefs(source, flows, flowID, expr.Expression) {
					add(SingletonCoordinatorDemand{
						FlowID: ref.OwnerFlowID, SourceFile: record.Source.File, Location: nodeID,
						Node: node, EventType: eventType, Kind: "entity_read." + expr.Kind,
						Field: ref.Field, Target: "entity." + ref.Ref,
					}, true)
				}
			}
		}
	}

	for _, plan := range source.WorkflowGates() {
		flowID := strings.TrimSpace(plan.FlowID)
		sourceFile := singletonFlowSchemaFile(bundle, flowID)
		for key, expression := range plan.Context {
			expr := expressionReference{Kind: "gate context " + strings.TrimSpace(key), Expression: stageGateExpressionText(expression), Phase: runtimepipeline.WorkflowEntityFieldLifecycleGate}
			for _, ref := range singletonDemandEntityRefs(source, flows, flowID, expr.Expression) {
				add(SingletonCoordinatorDemand{FlowID: ref.OwnerFlowID, SourceFile: sourceFile, Location: flowID, Kind: "entity_read." + expr.Kind, Field: ref.Field, Target: "entity." + ref.Ref}, true)
			}
		}
	}

	for _, plan := range source.WorkflowJoins() {
		flowID := plan.Node.FlowID()
		nodeID := plan.Node.Key()
		sourceRef, _ := source.ExecutableNodeSource(plan.Node)
		target := strings.TrimSpace(plan.Spec.ID)
		if target == "" {
			target = strings.TrimSpace(plan.Spec.Stage)
		}
		add(SingletonCoordinatorDemand{
			FlowID: flowID, Node: plan.Node, SourceFile: sourceRef.File, Location: nodeID,
			EventType: strings.TrimSpace(plan.HandlerEvent), Kind: "workflow_join", Target: target,
		}, false)
	}

	for _, record := range wave1ScopedAgentRecords(source) {
		for entityType, decl := range record.Entry.EntityWrites {
			contract, ok := wave1ResolveEntityWriteContract(source, record.Source, entityType)
			if !ok {
				continue
			}
			appendAgentSingletonWriteDemands(add, flows, contract, record, "create", decl.Create)
			appendAgentSingletonWriteDemands(add, flows, contract, record, "save", decl.Save)
		}
	}

	for flowID := range source.FlowSchemaEntries() {
		if _, singleton := flows[flowID]; !singleton {
			continue
		}
		for _, pin := range source.FlowInputEventPins(flowID) {
			if pin.Resolution().Mode != runtimecontracts.FlowInputResolutionModeFanIn {
				continue
			}
			add(SingletonCoordinatorDemand{
				FlowID: flowID, SourceFile: singletonFlowSchemaFile(bundle, flowID), Location: flowID,
				Kind: "fan_in_input", Target: pin.EventType() + " (" + pin.EventType() + ")",
			}, false)
		}
	}

	sort.Slice(demands, func(i, j int) bool {
		left, right := demands[i], demands[j]
		return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%06d", left.FlowID, left.SourceFile, left.Node.Key(), left.EventType, left.Kind, left.Target, left.WriteIndex) <
			fmt.Sprintf("%s|%s|%s|%s|%s|%s|%06d", right.FlowID, right.SourceFile, right.Node.Key(), right.EventType, right.Kind, right.Target, right.WriteIndex)
	})
	return demands
}

func singletonDemandEntityRefs(source semanticview.Source, flows map[string]singletonDemandFlow, flowID, expression string) []wave1ResolvedExpressionRef {
	refs := runtimepipeline.WorkflowEntityReferences(expression)
	out := make([]wave1ResolvedExpressionRef, 0, len(refs))
	for _, ref := range refs {
		field := runtimepipeline.WorkflowEntityReferenceField(ref)
		if flow, ok := flows[strings.TrimSpace(flowID)]; ok {
			if _, typed := flow.fields[field]; typed {
				out = append(out, wave1ResolvedExpressionRef{Ref: ref, Field: field, OwnerFlowID: strings.TrimSpace(flowID)})
				continue
			}
		}
		leaf, ownerFlowID, err := wave1ResolveEntityPathWithOwner(source, flowID, ref)
		if err != nil {
			continue
		}
		out = append(out, wave1ResolvedExpressionRef{Ref: ref, Field: field, OwnerFlowID: ownerFlowID, Leaf: leaf})
	}
	return out
}

func appendAgentSingletonWriteDemands(add func(SingletonCoordinatorDemand, bool), flows map[string]singletonDemandFlow, contract wave1EntityContractView, record wave1ScopedAgentRecord, action string, rule runtimecontracts.AgentEntityWriteRule) {
	fields := append([]string{}, rule.Fields...)
	if rule.All {
		for field := range flows[contract.FlowID].fields {
			fields = append(fields, field)
		}
	}
	for _, field := range fields {
		root, _, _ := strings.Cut(strings.TrimSpace(field), ".")
		add(SingletonCoordinatorDemand{
			FlowID: contract.FlowID, SourceFile: record.Source.File, Location: record.LogicalID,
			AgentID: record.LogicalID, Kind: "agent_entity_writes." + action,
			Field: root, Target: strings.TrimSpace(field),
		}, true)
	}
}

func singletonFlowSchemaFile(bundle *runtimecontracts.WorkflowContractBundle, flowID string) string {
	for _, view := range bundle.FlowViews() {
		if strings.TrimSpace(view.Paths.ID) == strings.TrimSpace(flowID) {
			return strings.TrimSpace(view.Paths.SchemaFile)
		}
	}
	return ""
}
