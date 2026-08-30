package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	joinTimeoutEvent  = "platform.join_timeout"
	joinCompleteEvent = "platform.join_complete"
)

func workflowJoinPlansForStage(source semanticview.Source, route runtimeflowidentity.Route, stage string) []runtimecontracts.WorkflowJoinPlan {
	if source == nil || !route.Valid() {
		return nil
	}
	stage = strings.TrimSpace(stage)
	ownerScope := strings.Trim(strings.TrimSpace(route.ScopeKey), "/")
	hasFlowOwner := false
	for _, plan := range source.WorkflowJoins() {
		flowID := plan.Node.FlowPath()
		if flowID != semanticview.RootExecutionFlowID(source) && runtimeflowidentity.ScopeKey(source, flowID) == ownerScope {
			hasFlowOwner = true
			break
		}
	}
	out := make([]runtimecontracts.WorkflowJoinPlan, 0, 1)
	for _, plan := range source.WorkflowJoins() {
		planFlowID := plan.Node.FlowPath()
		flowMatches := planFlowID == semanticview.RootExecutionFlowID(source) && !hasFlowOwner ||
			planFlowID != semanticview.RootExecutionFlowID(source) && runtimeflowidentity.ScopeKey(source, planFlowID) == ownerScope
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

func joinSchedule(source semanticview.Source, entityID string, instanceRoute runtimeflowidentity.Route, activation joinruntime.Activation, mode executionmode.Mode) (runtimegenericschedule.AdmissionCommand, error) {
	if !mode.Valid() {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("join schedule requires exact causal execution mode")
	}
	handle := activation.TimerHandle()
	ref, ok := handle.JoinRef()
	if !ok || activation.TimerTaskID() == "" || activation.TimerEventType() == "" {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("join schedule requires the activation's typed declaration handle")
	}
	payload := handle.PayloadMetadata()
	flowID := ref.FlowPath()
	route := events.RouteIdentity{EntityID: entityID}
	scheduleFlowInstance := ""
	if flowID != semanticview.RootExecutionFlowID(source) {
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
		EventType:     handle.EventType(),
		EntityID:      strings.TrimSpace(entityID),
		FlowInstance:  strings.Trim(strings.TrimSpace(scheduleFlowInstance), "/"),
		TaskID:        handle.TaskID(),
		Payload:       semanticPayload,
		RoutingSource: routingSource,
		ExecutionMode: mode,
		Due:           runtimegenericschedule.AbsoluteDue(activation.FireAt),
	}, nil
}
