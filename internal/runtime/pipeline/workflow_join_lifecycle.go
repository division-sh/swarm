package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	joinTimeoutEvent  = "platform.join_timeout"
	joinCompleteEvent = "platform.join_complete"
)

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

func joinSchedule(source semanticview.Source, flowID, entityID string, instanceRoute runtimeflowidentity.Route, activation joinruntime.Activation, kind timeridentity.TimerHandleKind, mode executionmode.Mode) (runtimegenericschedule.AdmissionCommand, error) {
	if !mode.Valid() {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("join schedule requires exact causal execution mode")
	}
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
		route.FlowInstance = instanceRoute.InstancePath
		scheduleFlowInstance = instanceRoute.InstancePath
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
		ExecutionMode: mode,
		Due:           runtimegenericschedule.AbsoluteDue(activation.FireAt),
	}, nil
}
