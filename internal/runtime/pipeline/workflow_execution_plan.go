package pipeline

import (
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

type workflowTriggerContext struct {
	Event           events.Event
	HandlerEventKey string
	State           WorkflowState
}

type handlerExecutionPlan struct {
	NodeID           string
	EventType        string
	Guard            string
	GuardSpec        *runtimecontracts.GuardSpec
	Action           string
	EvidenceTarget   string
	Template         string
	InstanceIDFrom   string
	InstanceIDPath   paths.Path
	ConfigFrom       *runtimecontracts.ConfigFromSpec
	CompletionRule   string
	Accumulate       *runtimecontracts.AccumulateSpec
	Compute          *runtimecontracts.ComputeSpec
	FanOut           *runtimecontracts.FanOutSpec
	BatchAgent       *runtimecontracts.BatchAgentSpec
	AdvancesTo       string
	SetsGate         string
	ClearGates       bool
	DataAccumulation runtimecontracts.WorkflowDataAccumulation
	Emit             runtimecontracts.EmitSpec
	EmitEvents       []string
	Rules            []runtimecontracts.HandlerRuleEntry
	OnComplete       []runtimecontracts.HandlerRuleEntry
	ExecutionOrder   []string
}

func workflowEventEntityID(evt events.Event) string {
	return workflowEventEntityIDWithPayload(evt, nil)
}

func workflowEventEntityIDWithPayload(evt events.Event, payload map[string]any) string {
	return strings.TrimSpace(evt.EntityID())
}

func handlerGuardID(spec *runtimecontracts.GuardSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.ID)
}

func gateSpecString(spec *runtimecontracts.GateSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.Name)
}

func handlerExecutionPlanFromNodeHandler(nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) handlerExecutionPlan {
	plan := handlerExecutionPlan{
		NodeID:           strings.TrimSpace(nodeID),
		EventType:        strings.TrimSpace(eventType),
		Guard:            handlerGuardID(handler.Guard),
		GuardSpec:        handler.Guard,
		Action:           strings.TrimSpace(handler.Action.ID),
		EvidenceTarget:   strings.TrimSpace(handler.EvidenceTarget),
		Template:         strings.TrimSpace(handler.Action.Template),
		InstanceIDFrom:   strings.TrimSpace(handler.Action.InstanceIDFrom),
		InstanceIDPath:   handler.Action.InstanceIDPath,
		ConfigFrom:       handler.Action.ConfigFrom,
		CompletionRule:   strings.TrimSpace(handler.CompletionRule),
		Accumulate:       handler.Accumulate,
		Compute:          handler.Compute,
		FanOut:           handler.FanOut,
		BatchAgent:       handler.BatchAgent,
		AdvancesTo:       strings.TrimSpace(handler.AdvancesTo),
		SetsGate:         gateSpecString(handler.SetsGate),
		ClearGates:       len(handler.ClearGates) > 0,
		DataAccumulation: handler.DataAccumulation,
		Emit:             handler.Emit,
		EmitEvents:       runtimecontracts.HandlerEmitEvents(handler),
		Rules:            append([]runtimecontracts.HandlerRuleEntry(nil), handler.Rules...),
		OnComplete:       append([]runtimecontracts.HandlerRuleEntry(nil), handler.OnComplete...),
	}
	plan.ExecutionOrder = handlerExecutionOrderForPlan(plan)
	return plan
}

func handlerExecutionOrderForPlan(plan handlerExecutionPlan) []string {
	steps := make([]string, 0, 12)
	if plan.ClearGates {
		steps = append(steps, "clear_gates")
	}
	if strings.TrimSpace(plan.Guard) != "" || plan.GuardSpec != nil {
		steps = append(steps, "guard")
	}
	if plan.Accumulate != nil {
		steps = append(steps, "accumulate")
	}
	if plan.Compute != nil {
		steps = append(steps, "compute")
	}
	if plan.FanOut != nil {
		steps = append(steps, "fan_out")
	}
	if plan.BatchAgent != nil {
		steps = append(steps, "batch_agent")
	}
	if len(plan.OnComplete) > 0 {
		steps = append(steps, "on_complete")
	}
	if len(plan.Rules) > 0 {
		steps = append(steps, "rules")
	}
	if plan.AdvancesTo != "" {
		steps = append(steps, "advances_to")
	}
	if plan.SetsGate != "" {
		steps = append(steps, "sets_gate")
	}
	if plan.DataAccumulation.HasWrites() || strings.TrimSpace(plan.DataAccumulation.SourceEvent) != "" {
		steps = append(steps, "data_accumulation")
	}
	if handlerPlanHasEmitFields(plan) {
		steps = append(steps, "emit_fields")
	}
	if len(plan.EmitEvents) > 0 {
		steps = append(steps, "emits")
	}
	if plan.Action != "" || handlerPlanHasRuleActions(plan) {
		steps = append(steps, "action")
	}
	return steps
}

func handlerPlanHasRuleActions(plan handlerExecutionPlan) bool {
	for _, rule := range plan.Rules {
		if strings.TrimSpace(rule.Action.ID) != "" {
			return true
		}
	}
	return false
}

func handlerPlanHasEmitFields(plan handlerExecutionPlan) bool {
	if plan.Emit.HasFields() {
		return true
	}
	if plan.FanOut != nil && plan.FanOut.Emit.HasFields() {
		return true
	}
	if plan.BatchAgent != nil && plan.BatchAgent.Emit.HasFields() {
		return true
	}
	for _, rule := range plan.Rules {
		if rule.Emit.HasFields() {
			return true
		}
		if rule.FanOut != nil && rule.FanOut.Emit.HasFields() {
			return true
		}
		if rule.BatchAgent != nil && rule.BatchAgent.Emit.HasFields() {
			return true
		}
	}
	for _, rule := range plan.OnComplete {
		if rule.Emit.HasFields() {
			return true
		}
		if rule.FanOut != nil && rule.FanOut.Emit.HasFields() {
			return true
		}
		if rule.BatchAgent != nil && rule.BatchAgent.Emit.HasFields() {
			return true
		}
	}
	if plan.Accumulate != nil {
		for _, rule := range plan.Accumulate.OnComplete {
			if rule.Emit.HasFields() {
				return true
			}
			if rule.FanOut != nil && rule.FanOut.Emit.HasFields() {
				return true
			}
			if rule.BatchAgent != nil && rule.BatchAgent.Emit.HasFields() {
				return true
			}
		}
		if plan.Accumulate.OnTimeout != nil {
			if plan.Accumulate.OnTimeout.Emit.HasFields() {
				return true
			}
			if plan.Accumulate.OnTimeout.FanOut != nil && plan.Accumulate.OnTimeout.FanOut.Emit.HasFields() {
				return true
			}
			if plan.Accumulate.OnTimeout.BatchAgent != nil && plan.Accumulate.OnTimeout.BatchAgent.Emit.HasFields() {
				return true
			}
		}
	}
	return false
}
