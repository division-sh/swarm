package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkContainedStateOperationCompliance(c *checkerContext) []Finding {
	findings := make([]Finding, 0)
	bundle, _ := semanticview.Bundle(c.source)
	for _, ref := range wave1ContainedStateOperations(c.source) {
		if schema, ok := c.source.FlowSchemaByID(ref.FlowID); ok && strings.TrimSpace(schema.Mode) == runtimecontracts.FlowModeSingleton {
			if bundle == nil {
				findings = append(findings, containedStateOperationFinding(ref, "singleton coordinator owner is unavailable"))
				continue
			}
			if _, err := bundle.ResolveFlowSingletonCoordinator(ref.FlowID); err != nil {
				findings = append(findings, containedStateOperationFinding(ref, fmt.Sprintf("singleton coordinator demand is unsatisfied: %v", err)))
				continue
			}
		}
		contract, ok := entityruntime.ResolveForFlow(c.source, ref.FlowID)
		if !ok {
			findings = append(findings, containedStateOperationFinding(ref, fmt.Sprintf("flow %s has no declared entity contract", defaultFlowLabel(ref.FlowID))))
			continue
		}
		target, err := entityruntime.ResolveContainedOperationTarget(contract, ref.Write.Target(), string(ref.Write.Operation), !ref.Write.Key.IsZero(), !ref.Write.Index.IsZero())
		if err != nil {
			findings = append(findings, containedStateOperationFinding(ref, err.Error()))
			continue
		}
		if ref.Write.Key.HasLiteralValue() {
			if _, err := entityruntime.NormalizeContainedOperationKey(contract, target.MapKeyType, ref.Write.Key.Literal); err != nil {
				findings = append(findings, containedStateOperationFinding(ref, fmt.Sprintf("key: %v", err)))
			}
		}
		if ref.Write.Index.HasLiteralValue() {
			if _, err := entityruntime.NormalizeContainedOperationIndex(ref.Write.Index.Literal); err != nil {
				findings = append(findings, containedStateOperationFinding(ref, fmt.Sprintf("index: %v", err)))
			}
		}
		if ref.Write.Operation != runtimecontracts.WorkflowDataOperationDelete && ref.Write.Value.HasLiteralValue() {
			if _, err := entityruntime.NormalizeContainedOperationValue(contract, target, string(ref.Write.Operation), ref.Write.Value.Literal); err != nil {
				findings = append(findings, containedStateOperationFinding(ref, fmt.Sprintf("value: %v", err)))
			}
		}
	}
	return findings
}

type wave1ContainedStateOperationRef struct {
	FlowID     string
	SourceFile string
	NodeID     string
	EventType  string
	Kind       string
	WriteIndex int
	Write      runtimecontracts.WorkflowDataWrite
}

func wave1ContainedStateOperations(source semanticview.Source) []wave1ContainedStateOperationRef {
	out := make([]wave1ContainedStateOperationRef, 0)
	for _, record := range wave1ScopedNodeRecords(source) {
		flowID := strings.TrimSpace(record.Source.FlowID)
		nodeID := strings.TrimSpace(record.LogicalID)
		for eventType, handler := range record.Entry.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			refs := wave1HandlerContainedStateOperations(flowID, nodeID, eventType, "handler", handler.DataAccumulation.Writes)
			out = append(out, wave1ContainedStateOperationsWithSource(refs, record.Source.File)...)
			for idx, rule := range handler.Rules {
				scope := fmt.Sprintf("handler.rules[%d]", idx)
				if id := strings.TrimSpace(rule.ID); id != "" {
					scope = "handler.rules[" + id + "]"
				}
				refs := wave1HandlerContainedStateOperations(flowID, nodeID, eventType, scope, rule.DataAccumulation.Writes)
				out = append(out, wave1ContainedStateOperationsWithSource(refs, record.Source.File)...)
			}
			for idx, rule := range handler.OnComplete {
				scope := fmt.Sprintf("handler.on_complete[%d]", idx)
				if id := strings.TrimSpace(rule.ID); id != "" {
					scope = "handler.on_complete[" + id + "]"
				}
				refs := wave1HandlerContainedStateOperations(flowID, nodeID, eventType, scope, rule.DataAccumulation.Writes)
				out = append(out, wave1ContainedStateOperationsWithSource(refs, record.Source.File)...)
			}
		}
	}
	return out
}

func wave1ContainedStateOperationsWithSource(refs []wave1ContainedStateOperationRef, sourceFile string) []wave1ContainedStateOperationRef {
	for i := range refs {
		refs[i].SourceFile = strings.TrimSpace(sourceFile)
	}
	return refs
}

func wave1HandlerContainedStateOperations(flowID, nodeID, eventType, kind string, writes []runtimecontracts.WorkflowDataWrite) []wave1ContainedStateOperationRef {
	out := make([]wave1ContainedStateOperationRef, 0)
	for writeIndex, write := range writes {
		if !write.IsContainedOperation() {
			continue
		}
		out = append(out, wave1ContainedStateOperationRef{
			FlowID:     flowID,
			NodeID:     nodeID,
			EventType:  eventType,
			Kind:       kind + ".data_accumulation",
			WriteIndex: writeIndex,
			Write:      write,
		})
	}
	return out
}

func containedStateOperationFinding(ref wave1ContainedStateOperationRef, detail string) Finding {
	return Finding{
		CheckID:  "contained_state_operation_compliance",
		Severity: SeverityHardInvalidity,
		Message:  fmt.Sprintf("flow %s node %s handler %s %s write[%d] op %q target %q invalid: %s", defaultFlowLabel(ref.FlowID), ref.NodeID, ref.EventType, ref.Kind, ref.WriteIndex, ref.Write.Operation, ref.Write.Target(), strings.TrimSpace(detail)),
		Location: ref.NodeID,
	}
}
