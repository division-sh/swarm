package engine

import (
	"fmt"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func validateHandlerLoopRuntime(handler runtimecontracts.SystemNodeEventHandler) error {
	return runtimecontracts.ValidateLoopHandlerCombination(handler)
}

func (e *Executor) stepLoop(frame *executionFrame) error {
	operation := frame.req.Handler.Loop
	if operation == nil {
		return e.rejectUnadmittedLoopHandler(frame)
	}
	kind, loopID, err := operation.Operation()
	if err != nil {
		return err
	}
	plan, ok := workflowLoopPlan(e.deps.Source, frame.req.FlowID.String(), loopID)
	if !ok {
		return failures.New(failures.ClassUnexpectedArrival, "loop_not_declared", "runtime.engine", "loop", map[string]any{"loop_id": loopID})
	}
	frame.loopPlan = &plan
	now := loopEventTime(frame)
	activation, found, err := loopruntime.Load(frame.state.State.StateCarrier.StateBuckets, plan.FlowID, plan.ID)
	if err != nil {
		return fmt.Errorf("load loop activation %s: %w", plan.ID, err)
	}
	switch kind {
	case runtimecontracts.LoopOperationStart:
		if strings.TrimSpace(frame.state.State.CurrentState) != strings.TrimSpace(operation.From) {
			return loopAdmissionFailure(failures.ClassEarlyArrival, "loop_source_stage_mismatch", plan, "", frame.state.State.CurrentState, operation.From, nil, frame.req.Event.ID())
		}
		if found {
			if activation.Status == loopruntime.StatusOpen && activation.StartedByEvent == frame.req.Event.ID() {
				frame.loopActivation = &activation
				frame.state.SetLoop(activation.Context())
				setLoopExecutionTrace(frame, kind, activation)
				return nil
			}
			return loopAdmissionFailure(failures.ClassUnexpectedArrival, "loop_already_started", plan, activation.RevisionID, activation.CurrentStage, operation.From, &activation, frame.req.Event.ID())
		}
		maxAttempts, err := resolveLoopMaxAttempts(plan, frame.base.Policy.Raw())
		if err != nil {
			return err
		}
		activation, err = loopruntime.New(
			frame.req.Event.RunID(), frame.req.EntityID.String(), plan.FlowID, plan.ID, plan.RevisionField,
			frame.req.Event.ID(), strings.TrimSpace(frame.req.Handler.AdvancesTo), maxAttempts, now,
		)
		if err != nil {
			return err
		}
	case runtimecontracts.LoopOperationAdmit, runtimecontracts.LoopOperationRepeat, runtimecontracts.LoopOperationClose:
		if !found {
			return loopAdmissionFailure(failures.ClassUnexpectedArrival, "loop_not_active", plan, "", frame.state.State.CurrentState, operation.From, nil, frame.req.Event.ID())
		}
		supplied := strings.TrimSpace(asString(frame.payload[plan.RevisionField]))
		disposition := activation.Admit(supplied, operation.From)
		if disposition != loopruntime.AdmissionAccepted {
			return loopDispositionFailure(disposition, plan, supplied, activation, operation.From, frame.req.Event.ID())
		}
		switch kind {
		case runtimecontracts.LoopOperationRepeat:
			target := strings.TrimSpace(frame.req.Handler.AdvancesTo)
			escaped, err := activation.Repeat(target, frame.req.Event.ID(), now)
			if err != nil {
				return err
			}
			if escaped {
				activation.CurrentStage = strings.TrimSpace(plan.Escape.AdvancesTo)
				escape := runtimecontracts.HandlerRuleEntry{AdvancesTo: plan.Escape.AdvancesTo, Emit: plan.Escape.Emit}
				frame.rule = &escape
			}
		case runtimecontracts.LoopOperationClose:
			if err := activation.Close(strings.TrimSpace(frame.req.Handler.AdvancesTo), frame.req.Event.ID(), now); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported loop operation %q", kind)
	}
	if err := storeLoopActivation(frame, activation); err != nil {
		return err
	}
	setLoopExecutionTrace(frame, kind, activation)
	return nil
}

func setLoopExecutionTrace(frame *executionFrame, kind runtimecontracts.LoopOperationKind, activation loopruntime.Activation) {
	if frame == nil {
		return
	}
	frame.result.LoopTrace = &LoopExecutionTrace{
		LoopID: activation.LoopID, Operation: string(kind), RevisionID: activation.RevisionID,
		Attempt: activation.Attempt, MaxAttempts: activation.MaxAttempts, CurrentStage: activation.CurrentStage,
		Status: activation.Status, CloseReason: activation.CloseReason,
	}
}

func (e *Executor) rejectUnadmittedLoopHandler(frame *executionFrame) error {
	if frame == nil {
		return nil
	}
	activations, err := loopruntime.List(frame.state.State.StateCarrier.StateBuckets)
	if err != nil {
		return fmt.Errorf("list loop activations: %w", err)
	}
	currentStage := strings.TrimSpace(frame.state.State.CurrentState)
	for _, activation := range activations {
		if activation.Status != loopruntime.StatusOpen || activation.CurrentStage != currentStage {
			continue
		}
		plan, ok := workflowLoopPlan(e.deps.Source, activation.FlowID, activation.LoopID)
		if !ok || !containsLoopStage(plan.RegionStages, currentStage) {
			continue
		}
		return loopAdmissionFailure(failures.ClassUnexpectedArrival, "loop_operation_missing", plan, "", currentStage, currentStage, &activation, frame.req.Event.ID())
	}
	return nil
}

func (e *Executor) advanceAdmittedLoop(frame *executionFrame, next string) error {
	if frame == nil || frame.loopPlan == nil || frame.loopActivation == nil || frame.req.Handler.Loop == nil {
		return nil
	}
	kind, _, err := frame.req.Handler.Loop.Operation()
	if err != nil || kind != runtimecontracts.LoopOperationAdmit {
		return err
	}
	activation := *frame.loopActivation
	if err := activation.AdvanceWithin(next, frame.req.Event.ID(), loopEventTime(frame)); err != nil {
		return err
	}
	return storeLoopActivation(frame, activation)
}

func storeLoopActivation(frame *executionFrame, activation loopruntime.Activation) error {
	if frame.state.State.StateCarrier.StateBuckets == nil {
		frame.state.State.StateCarrier.StateBuckets = map[string]map[string]any{}
	}
	if err := loopruntime.Store(frame.state.State.StateCarrier.StateBuckets, activation); err != nil {
		return fmt.Errorf("store loop activation: %w", err)
	}
	frame.loopActivation = &activation
	frame.state.SetLoop(activation.Context())
	frame.result.StateMutation.SetStateBuckets(frame.state.State.StateCarrier.StateBuckets)
	return nil
}

func workflowLoopPlan(source semanticview.Source, flowID, loopID string) (runtimecontracts.WorkflowLoopPlan, bool) {
	flowID = strings.TrimSpace(flowID)
	loopID = strings.TrimSpace(loopID)
	for _, plan := range semanticview.WorkflowLoops(source) {
		planFlowID := strings.TrimSpace(plan.FlowID)
		flowMatches := planFlowID == flowID
		if planFlowID == "" {
			flowMatches = flowID == "" || flowID == strings.TrimSpace(source.WorkflowName())
		}
		if flowMatches && strings.TrimSpace(plan.ID) == loopID {
			return plan, true
		}
	}
	return runtimecontracts.WorkflowLoopPlan{}, false
}

func resolveLoopMaxAttempts(plan runtimecontracts.WorkflowLoopPlan, policy map[string]any) (int, error) {
	if plan.MaxAttempts.Literal > 0 {
		return plan.MaxAttempts.Literal, nil
	}
	key := strings.TrimSpace(plan.MaxAttempts.PolicyRef)
	value, ok := lookupPath(policy, key)
	if !ok {
		return 0, fmt.Errorf("loop %s max_attempts policy %s is unavailable", plan.ID, key)
	}
	max, ok := positiveLoopMaxAttempts(value)
	if !ok {
		return 0, fmt.Errorf("loop %s max_attempts policy %s must resolve to a positive integer", plan.ID, key)
	}
	return max, nil
}

func positiveLoopMaxAttempts(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, typed > 0
	case int32:
		return int(typed), typed > 0
	case int64:
		return int(typed), typed > 0
	case float64:
		if typed > 0 && typed == float64(int64(typed)) {
			return int(typed), true
		}
		return 0, false
	default:
		return 0, false
	}
}

func loopDispositionFailure(disposition loopruntime.AdmissionDisposition, plan runtimecontracts.WorkflowLoopPlan, supplied string, activation loopruntime.Activation, from, triggerEvent string) error {
	class := failures.ClassUnexpectedArrival
	code := "loop_revision_unexpected"
	switch disposition {
	case loopruntime.AdmissionEarly:
		class, code = failures.ClassEarlyArrival, "loop_stage_early"
	case loopruntime.AdmissionStale:
		class, code = failures.ClassStaleArrival, "loop_revision_stale"
	}
	return loopAdmissionFailure(class, code, plan, supplied, activation.CurrentStage, from, &activation, triggerEvent)
}

func loopAdmissionFailure(class failures.Class, code string, plan runtimecontracts.WorkflowLoopPlan, supplied, currentStage, expectedStage string, activation *loopruntime.Activation, triggerEvent string) error {
	details := map[string]any{
		"loop_id": plan.ID, "revision_field": plan.RevisionField, "supplied_revision_id": strings.TrimSpace(supplied),
		"current_stage": strings.TrimSpace(currentStage), "expected_stage": strings.TrimSpace(expectedStage),
		"trigger_event_id": strings.TrimSpace(triggerEvent),
	}
	if activation != nil {
		details["current_revision_id"] = strings.TrimSpace(activation.RevisionID)
		details["attempt"] = activation.Attempt
		details["status"] = activation.Status
		details["close_reason"] = activation.CloseReason
	}
	return failures.New(class, code, "runtime.engine", "loop", details)
}

func loopEventTime(frame *executionFrame) time.Time {
	if frame != nil {
		if created := frame.req.Event.CreatedAt(); !created.IsZero() {
			return created.UTC()
		}
	}
	return time.Unix(0, 0).UTC()
}

func containsLoopStage(stages []string, stage string) bool {
	stage = strings.TrimSpace(stage)
	for _, candidate := range stages {
		if strings.TrimSpace(candidate) == stage {
			return true
		}
	}
	return false
}
