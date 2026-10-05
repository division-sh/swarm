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

func workflowJoinPlansForStage(source semanticview.Source, owner runtimeflowidentity.RunScopedFlowInstance, stage string) []runtimecontracts.WorkflowJoinPlan {
	if source == nil || owner.Validate() != nil {
		return nil
	}
	stage = strings.TrimSpace(stage)
	ownerScope := owner.Route.ScopeKey
	root := owner.Route.InstancePath == owner.RunID
	out := make([]runtimecontracts.WorkflowJoinPlan, 0, 1)
	for _, plan := range source.WorkflowJoins() {
		flowMatches := root && plan.Node.FlowPath() == semanticview.RootExecutionFlowID(source) ||
			!root && runtimeflowidentity.ScopeKey(source, plan.Node.FlowPath()) == ownerScope
		if flowMatches && strings.TrimSpace(plan.Spec.Stage) == stage {
			out = append(out, plan)
		}
	}
	return out
}

func joinMemberSnapshot(metadata map[string]any, plan runtimecontracts.WorkflowJoinPlan) ([]string, bool) {
	value, ok := metadata[joinTopLevelField(plan.Spec.Members.From, "state")]
	if !ok {
		return nil, false
	}
	raw, err := plan.MembersCollectionProjection.Project(value)
	if err != nil {
		return nil, false
	}
	members := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		member, ok := item.(string)
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

func joinSchedule(source semanticview.Source, owner runtimeflowidentity.RunScopedFlowInstance, instance WorkflowInstance, activation joinruntime.Activation, mode executionmode.Mode) (runtimegenericschedule.AdmissionCommand, error) {
	constructed, err := instance.ConstructionIdentity(owner)
	if err != nil {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("join schedule constructed owner: %w", err)
	}
	entityID, instanceRoute := constructed.EntityID, constructed.Route()
	if !mode.Valid() {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("join schedule requires exact causal execution mode")
	}
	handle := activation.TimerHandle()
	ref, ok := handle.JoinRef()
	if !ok || activation.TimerTaskID() == "" || activation.TimerEventType() == "" {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("join schedule requires the activation's typed declaration handle")
	}
	entry := ref.StageEntry()
	if ref.Mode() == timeridentity.JoinRefModeArrival {
		if err := entry.RequireOwner(owner.RunID, instanceRoute.ScopeKey, instanceRoute.InstanceID, instanceRoute.InstancePath, entityID, ref.Stage()); err != nil {
			return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("join schedule contradicts its retained lifecycle entry: %w", err)
		}
	}
	payload := handle.PayloadMetadata()
	flowID := ref.FlowPath()
	if flowID != constructed.TemplateID {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("join schedule declaration conflicts with its constructed owner")
	}
	route := events.RouteIdentity{FlowID: flowID, FlowInstance: instanceRoute.InstancePath, EntityID: entityID}
	scheduleFlowInstance := ""
	if flowID != semanticview.RootExecutionFlowID(source) {
		scheduleFlowInstance = instanceRoute.InstancePath
	}
	executionSource, err := runtimepinrouting.AdmitFlowExecutionRoutingSource(source, owner.RunID, constructed, route)
	if err != nil {
		return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("admit join schedule source: %w", err)
	}
	routingSource := executionSource
	if flowID == semanticview.RootExecutionFlowID(source) {
		routingSource, err = events.NewRootRoutingSource(entityID)
		if err != nil {
			return runtimegenericschedule.AdmissionCommand{}, fmt.Errorf("admit root join control source: %w", err)
		}
	} else {
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
