package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	joinTimeoutEvent  = "platform.join_timeout"
	joinCompleteEvent = "platform.join_complete"
)

func (pc *PipelineCoordinator) applyWorkflowJoinIntents(ctx context.Context, route runtimeflowidentity.Route, entityID, currentStage, nextStage string, occurredAt time.Time) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() || pc.SemanticSource() == nil {
		return nil
	}
	if pc.workflowStore.engineMutations == nil {
		return fmt.Errorf("workflow join lifecycle requires the selected workflow engine mutation owner")
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	entityID = strings.TrimSpace(entityID)
	currentStage = strings.TrimSpace(currentStage)
	nextStage = strings.TrimSpace(nextStage)
	if entityID == "" || nextStage == "" || currentStage == nextStage {
		return nil
	}

	now := occurredAt.UTC()
	if now.IsZero() {
		return fmt.Errorf("workflow join lifecycle requires an exact occurrence time")
	}
	instance, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil {
		return err
	}
	if !found {
		return &WorkflowInstanceLookupMiss{RequestedKey: route.InstancePath}
	}
	expectedState, expectedRevision := strings.TrimSpace(instance.CurrentState), instance.Revision
	plan := WorkflowLifecycleMutationPlan{}
	if err := pc.planWorkflowJoinEffect(ctx, &instance, entityID, currentStage, nextStage, now, &plan); err != nil {
		return err
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		return fmt.Errorf("workflow join lifecycle requires run identity")
	}
	updatedAt := time.Now().UTC()
	if updatedAt.Before(instance.CreatedAt) {
		updatedAt = instance.CreatedAt
	}
	state, err := workflowEngineStateRecord(runID, route, instance, expectedState, expectedRevision, false, updatedAt)
	if err != nil {
		return err
	}
	committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: state, Lifecycle: plan})
	if err != nil {
		return err
	}
	return pc.finalizeWorkflowLifecycleMutation(ctx, committed.Lifecycle)
}

func (pc *PipelineCoordinator) reconcileClosedJoinSchedules(ctx context.Context, route runtimeflowidentity.Route, entityID string, carrier runtimeengine.StateCarrier) error {
	activations, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		return fmt.Errorf("list join activations: %w", err)
	}
	if !route.Valid() {
		return fmt.Errorf("join schedule reconciliation requires an exact workflow instance route")
	}
	instance, ok, err := pc.workflowStore.Load(ctx, route)
	if err != nil || !ok {
		return err
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		return fmt.Errorf("join schedule reconciliation requires run identity")
	}
	changed := false
	plan := WorkflowLifecycleMutationPlan{}
	cancelledAt := time.Now().UTC()
	for _, activation := range activations {
		if activation.Status != joinruntime.StatusClosed || activation.TimerTaskID == "" || activation.TimerCancelled {
			continue
		}
		if activation.TimerEventType == joinCompleteEvent && activation.OutcomePending && !activation.OutcomeFired {
			continue
		}
		kind := timeridentity.TimerHandleJoinTimeout
		if activation.TimerEventType == joinCompleteEvent {
			kind = timeridentity.TimerHandleJoinComplete
		}
		command, err := joinSchedule(pc.SemanticSource(), instance.WorkflowName, entityID, instance, activation, kind)
		if err != nil {
			return err
		}
		command.RunID = runID
		plan.Schedules = append(plan.Schedules, WorkflowScheduleMutation{
			Kind: WorkflowScheduleMutationCancel, Command: command,
			CancelCause: "join_closed_reconciliation", CancelledAt: cancelledAt,
		})
		activation.TimerCancelled = true
		if err := joinruntime.Store(carrier.StateBuckets, activation); err != nil {
			return fmt.Errorf("persist join timer cancellation %s: %w", activation.Key(), err)
		}
		changed = true
	}
	if changed {
		expectedState, expectedRevision := strings.TrimSpace(instance.CurrentState), instance.Revision
		instance.StateBuckets = carrier.PersistedStateBuckets()
		updatedAt := time.Now().UTC()
		if updatedAt.Before(instance.CreatedAt) {
			updatedAt = instance.CreatedAt
		}
		state, err := workflowEngineStateRecord(runID, route, instance, expectedState, expectedRevision, false, updatedAt)
		if err != nil {
			return err
		}
		committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: state, Lifecycle: plan})
		if err != nil {
			return err
		}
		return pc.finalizeWorkflowLifecycleMutation(ctx, committed.Lifecycle)
	}
	return nil
}

func workflowJoinPlansForStage(source semanticview.Source, flowID, stage string) []runtimecontracts.WorkflowJoinPlan {
	if source == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	stage = strings.TrimSpace(stage)
	out := make([]runtimecontracts.WorkflowJoinPlan, 0, 1)
	for _, plan := range source.WorkflowJoins() {
		planFlowID := strings.TrimSpace(plan.FlowID)
		flowMatches := planFlowID == flowID
		if planFlowID == "" {
			flowMatches = flowID == "" || flowID == strings.TrimSpace(source.WorkflowName())
		}
		if flowMatches && strings.TrimSpace(plan.Spec.Stage) == stage {
			out = append(out, plan)
		}
	}
	return out
}

func joinMemberSnapshot(metadata map[string]any, path string) ([]string, bool) {
	value, ok := metadata[joinTopLevelField(path, "entity")]
	if !ok {
		return nil, false
	}
	var raw []any
	switch typed := value.(type) {
	case []any:
		raw = typed
	case []string:
		raw = make([]any, len(typed))
		for i := range typed {
			raw[i] = typed[i]
		}
	default:
		return nil, false
	}
	members := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		member, ok := item.(string)
		member = strings.TrimSpace(member)
		if !ok || member == "" {
			return nil, false
		}
		if _, duplicate := seen[member]; duplicate {
			return nil, false
		}
		seen[member] = struct{}{}
		members = append(members, member)
	}
	return members, true
}

func joinTopLevelField(path, root string) string {
	path = strings.TrimSpace(path)
	prefix := strings.TrimSpace(root) + "."
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	field := strings.TrimPrefix(path, prefix)
	if field == "" || strings.Contains(field, ".") {
		return ""
	}
	return field
}

func joinSchedule(source semanticview.Source, flowID, entityID string, instance WorkflowInstance, activation joinruntime.Activation, kind timeridentity.TimerHandleKind) (runtimegenericschedule.AdmissionCommand, error) {
	ref := timeridentity.NewJoinRefForGeneration(flowID, activation.NodeID, activation.HandlerEvent, activation.Stage, activation.JoinID, activation.Window, activation.Generation)
	var handle timeridentity.TimerHandle
	var eventType string
	switch kind {
	case timeridentity.TimerHandleJoinTimeout:
		handle = timeridentity.JoinTimeoutHandle(ref)
		eventType = joinTimeoutEvent
	case timeridentity.TimerHandleJoinComplete:
		handle = timeridentity.JoinCompleteHandle(ref)
		eventType = joinCompleteEvent
	default:
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("join schedule handle kind %q is invalid", kind)
	}
	if activation.TimerTaskID != handle.TaskID() || activation.TimerEventType != eventType {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf(
			"join schedule declaration does not match persisted activation (task=%q want=%q event=%q want=%q)",
			activation.TimerTaskID, handle.TaskID(), activation.TimerEventType, eventType,
		)
	}
	payload := handle.PayloadMetadata()
	if generation := activation.Generation.Normalize(); generation.Valid() {
		payload[generation.RevisionField] = generation.RevisionID
	}
	flowID = strings.TrimSpace(flowID)
	route := events.RouteIdentity{EntityID: entityID}
	scheduleFlowInstance := ""
	if flowID != "" {
		route.FlowID = flowID
		route.FlowInstance = instance.StorageRef
		scheduleFlowInstance = instance.StorageRef
	}
	executionSource, err := runtimepinrouting.AdmitFlowExecutionRoutingSource(source, flowID, route)
	if err != nil {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("admit join schedule source: %w", err)
	}
	routingSource := executionSource
	if executionSource.Kind() != events.RoutingSourceRoot {
		routingSource, err = events.NewFlowOwnedControlRoutingSource(executionSource.Route())
		if err != nil {
			return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("admit join schedule control source: %w", err)
		}
	}
	semanticPayload, err := canonicaljson.FromGo(payload)
	if err != nil {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("admit join schedule payload: %w", err)
	}
	return runtimegenericschedule.AdmissionCommand{
		ScheduleKey:   handle.TaskID(),
		OwnerID:       runtimeWorkflowID,
		OwnerKind:     runtimegenericschedule.OwnerSystem,
		EventType:     eventType,
		EntityID:      strings.TrimSpace(entityID),
		FlowInstance:  strings.Trim(strings.TrimSpace(scheduleFlowInstance), "/"),
		TaskID:        handle.TaskID(),
		Payload:       semanticPayload,
		RoutingSource: routingSource,
		Due:           runtimegenericschedule.AbsoluteDue(activation.FireAt),
	}, nil
}
