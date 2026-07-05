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
	for _, ref := range wave1ContainedStateOperations(c.source) {
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
	FlowID    string
	NodeID    string
	EventType string
	Kind      string
	Write     runtimecontracts.WorkflowDataWrite
}

func wave1ContainedStateOperations(source semanticview.Source) []wave1ContainedStateOperationRef {
	out := make([]wave1ContainedStateOperationRef, 0)
	nodes := source.NodeEntries()
	for _, nodeID := range sortedNodeIDs(source) {
		node, ok := nodes[nodeID]
		if !ok {
			continue
		}
		flowID := ""
		if sourceRef, ok := source.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(sourceRef.FlowID)
		}
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			out = append(out, wave1HandlerContainedStateOperations(flowID, strings.TrimSpace(nodeID), eventType, "handler", handler.DataAccumulation.Writes)...)
			for idx, rule := range handler.Rules {
				scope := fmt.Sprintf("handler.rules[%d]", idx)
				if id := strings.TrimSpace(rule.ID); id != "" {
					scope = "handler.rules[" + id + "]"
				}
				out = append(out, wave1HandlerContainedStateOperations(flowID, strings.TrimSpace(nodeID), eventType, scope, rule.DataAccumulation.Writes)...)
			}
			for idx, rule := range handler.OnComplete {
				scope := fmt.Sprintf("handler.on_complete[%d]", idx)
				if id := strings.TrimSpace(rule.ID); id != "" {
					scope = "handler.on_complete[" + id + "]"
				}
				out = append(out, wave1HandlerContainedStateOperations(flowID, strings.TrimSpace(nodeID), eventType, scope, rule.DataAccumulation.Writes)...)
			}
			if handler.Accumulate != nil {
				for idx, rule := range handler.Accumulate.OnComplete {
					scope := fmt.Sprintf("handler.accumulate.on_complete[%d]", idx)
					if id := strings.TrimSpace(rule.ID); id != "" {
						scope = "handler.accumulate.on_complete[" + id + "]"
					}
					out = append(out, wave1HandlerContainedStateOperations(flowID, strings.TrimSpace(nodeID), eventType, scope, rule.DataAccumulation.Writes)...)
				}
				if handler.Accumulate.OnTimeout != nil {
					scope := "handler.accumulate.on_timeout"
					if id := strings.TrimSpace(handler.Accumulate.OnTimeout.ID); id != "" {
						scope = "handler.accumulate.on_timeout[" + id + "]"
					}
					out = append(out, wave1HandlerContainedStateOperations(flowID, strings.TrimSpace(nodeID), eventType, scope, handler.Accumulate.OnTimeout.DataAccumulation.Writes)...)
				}
			}
		}
	}
	return out
}

func wave1HandlerContainedStateOperations(flowID, nodeID, eventType, kind string, writes []runtimecontracts.WorkflowDataWrite) []wave1ContainedStateOperationRef {
	out := make([]wave1ContainedStateOperationRef, 0)
	for _, write := range writes {
		if !write.IsContainedOperation() {
			continue
		}
		out = append(out, wave1ContainedStateOperationRef{
			FlowID:    flowID,
			NodeID:    nodeID,
			EventType: eventType,
			Kind:      kind + ".data_accumulation",
			Write:     write,
		})
	}
	return out
}

func containedStateOperationFinding(ref wave1ContainedStateOperationRef, detail string) Finding {
	return Finding{
		CheckID:  "contained_state_operation_compliance",
		Severity: SeverityHardInvalidity,
		Message:  fmt.Sprintf("flow %s node %s handler %s %s op %q target %q invalid: %s", defaultFlowLabel(ref.FlowID), ref.NodeID, ref.EventType, ref.Kind, ref.Write.Operation, ref.Write.Target(), strings.TrimSpace(detail)),
		Location: ref.NodeID,
	}
}
