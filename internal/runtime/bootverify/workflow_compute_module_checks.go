package bootverify

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const computeModuleCheckID = "compute_module_value_rows"

func checkComputeModuleValueRows(c *checkerContext) []Finding {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	findings := make([]Finding, 0)
	nodes := c.source.NodeEntries()
	for _, nodeID := range sortedNodeIDs(c.source) {
		node, ok := nodes[nodeID]
		if !ok {
			continue
		}
		flowID := nodeFlowID(c.source, nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			for idx, rule := range handler.Rules {
				if !policySheetRuleIsComputeModuleValueRow(rule) {
					continue
				}
				ref := computeModuleRef{
					FlowID:    flowID,
					NodeID:    nodeID,
					EventType: eventType,
					RuleIndex: idx,
					RuleID:    strings.TrimSpace(rule.ID),
				}
				findings = append(findings, validateComputeModuleValueRow(ctx, c.source, ref, handler, rule)...)
			}
		}
	}
	return findings
}

type computeModuleRef struct {
	FlowID    string
	NodeID    string
	EventType string
	RuleIndex int
	RuleID    string
}

func computeModuleFinding(ref computeModuleRef, detail string) Finding {
	return Finding{
		CheckID:  computeModuleCheckID,
		Severity: SeverityHardInvalidity,
		Message:  fmt.Sprintf("flow %s node %s handler %s compute_module row %s: %s", defaultFlowLabel(ref.FlowID), ref.NodeID, ref.EventType, ref.RowLabel(), detail),
		Location: ref.NodeID,
	}
}

func (r computeModuleRef) RowLabel() string {
	if id := strings.TrimSpace(r.RuleID); id != "" {
		return id
	}
	return fmt.Sprintf("#%d", r.RuleIndex)
}

func policySheetRuleIsComputeModuleValueRow(rule runtimecontracts.HandlerRuleEntry) bool {
	if rule.PolicyRow.Kind == runtimecontracts.PolicySheetRowKindModule {
		return true
	}
	return rule.Compute != nil && rule.Compute.Operation == runtimecontracts.ComputeOpModule
}

func validateComputeModuleValueRow(ctx context.Context, source semanticview.Source, ref computeModuleRef, handler runtimecontracts.SystemNodeEventHandler, rule runtimecontracts.HandlerRuleEntry) []Finding {
	findings := make([]Finding, 0)
	if rule.PolicyRow.Kind != runtimecontracts.PolicySheetRowKindModule {
		findings = append(findings, computeModuleFinding(ref, "compute_module compute must originate from a policy-sheet compute_module row"))
	}
	if rule.Compute == nil || rule.Compute.Operation != runtimecontracts.ComputeOpModule {
		findings = append(findings, computeModuleFinding(ref, "compute_module row must lower to compute-owned module operation"))
		return findings
	}
	spec := rule.Compute.Module
	if spec == nil {
		findings = append(findings, computeModuleFinding(ref, "compute_module row has no canonical module plan"))
		return findings
	}
	storeAs := strings.TrimSpace(rule.Compute.StoreAs)
	storePath := runtimepaths.Parse(storeAs)
	if storePath.Root != runtimepaths.RootComputed || len(storePath.Segments) == 0 {
		findings = append(findings, computeModuleFinding(ref, fmt.Sprintf("compute_module.into must target computed.*, got %q", storeAs)))
	} else if !policySheetLookupPathIsSimple(storePath) {
		findings = append(findings, computeModuleFinding(ref, fmt.Sprintf("compute_module.into %q must be a simple computed.* path", storeAs)))
	}
	policy := source.ResolvedPolicyForFlow(ref.FlowID)
	moduleID := strings.TrimSpace(spec.Module)
	module, ok := policy.Modules[moduleID]
	if !ok {
		findings = append(findings, computeModuleFinding(ref, fmt.Sprintf("compute_module.module %q does not resolve in policy.modules", moduleID)))
	} else {
		bundle, hasBundle := semanticview.Bundle(source)
		if !hasBundle || bundle == nil {
			findings = append(findings, computeModuleFinding(ref, "compute_module boot verification requires a workflow contract bundle source"))
		} else {
			moduleBytes, _, err := runtimecontracts.PolicyModuleBytes(bundle, module)
			if err != nil {
				findings = append(findings, computeModuleFinding(ref, fmt.Sprintf("module bytes unavailable: %v", err)))
			} else if err := validateComputeModuleArtifact(ctx, ref, moduleID, module, moduleBytes); err != nil {
				findings = append(findings, computeModuleFinding(ref, fmt.Sprintf("module validation failed: %v", err)))
			}
		}
	}
	if !policySheetLookupBindingConsumed(source, ref.FlowID, ref.EventType, handler, rule, storeAs) {
		findings = append(findings, computeModuleFinding(ref, fmt.Sprintf("compute_module.into %q is not consumed by a supported downstream condition, emit field, activity input, fan_out, or expression", storeAs)))
	}
	return findings
}

func validateComputeModuleArtifact(ctx context.Context, ref computeModuleRef, moduleID string, module runtimecontracts.PolicyModule, moduleBytes []byte) error {
	switch computeModuleKind(module) {
	case "wasm":
		return computemodule.ValidateCoreJSONModule(moduleBytes, module.Entry, module.Limits.MemoryPages)
	case pythonmodule.Kind:
		return pythonmodule.ValidateSource(ctx, pythonmodule.Request{
			ModuleID:    moduleID,
			RowID:       ref.RowLabel(),
			Digest:      strings.TrimSpace(module.Digest),
			Entry:       strings.TrimSpace(module.Entry),
			Source:      moduleBytes,
			Fuel:        module.Limits.Gas,
			MemoryPages: module.Limits.MemoryPages,
			OutputBytes: module.Limits.OutputBytes,
		})
	default:
		return fmt.Errorf("unsupported module kind %q", strings.TrimSpace(module.Kind))
	}
}

func computeModuleKind(module runtimecontracts.PolicyModule) string {
	kind := strings.TrimSpace(module.Kind)
	if kind == "" {
		return "wasm"
	}
	return kind
}
