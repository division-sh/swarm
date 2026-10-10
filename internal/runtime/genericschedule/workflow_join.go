package genericschedule

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
)

// WorkflowJoinAdmission projects an already admitted arrival arm. It neither
// re-evaluates membership nor calculates a deadline from the current contract.
func WorkflowJoinAdmission(activation joinruntime.Activation, mode executionmode.Mode) (AdmissionCommand, error) {
	if err := activation.Validate(); err != nil {
		return AdmissionCommand{}, err
	}
	ref := activation.JoinRef()
	entry := ref.StageEntry()
	if ref.Mode() != timeridentity.JoinRefModeArrival || entry.Empty() || entry.FlowScope != ref.FlowPath() || activation.FireAt.IsZero() {
		return AdmissionCommand{}, fmt.Errorf("arrival join schedule requires its exact lifecycle entry and due coordinate")
	}
	var source events.RoutingSource
	var err error
	flowInstance := ""
	if ref.FlowPath() == "." {
		source, err = events.NewRootRoutingSource(entry.EntityID)
	} else {
		flowInstance = entry.InstancePath
		source, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
			FlowID: ref.FlowPath(), FlowInstance: flowInstance, EntityID: entry.EntityID,
		})
	}
	if err != nil {
		return AdmissionCommand{}, err
	}
	handle := activation.TimerHandle()
	payload, err := canonicaljson.FromGo(handle.PayloadMetadata())
	if err != nil {
		return AdmissionCommand{}, err
	}
	command := AdmissionCommand{
		ScheduleKey: handle.TaskID(), RunID: entry.RunID, EntityID: entry.EntityID, FlowInstance: flowInstance,
		OwnerKind: OwnerSystem, OwnerID: "workflow-runtime", EventType: handle.EventType(), Payload: payload,
		RoutingSource: source, ExecutionMode: mode, Due: AbsoluteDue(activation.FireAt), TaskID: handle.TaskID(),
	}
	return command, command.Validate()
}

// ValidateWorkflowJoinScheduleRelation proves immutable correspondence, not
// permission to execute. A closed completion arm can retain its canceled timeout
// alongside its completion schedule; both belong to the same exact stage entry.
func ValidateWorkflowJoinScheduleRelation(join joinruntime.Activation, schedule Activation) error {
	if err := schedule.Validate(); err != nil {
		return err
	}
	expected, err := WorkflowJoinAdmission(join, schedule.Command.ExecutionMode)
	if err != nil {
		return err
	}
	if schedule.Command.TaskID != expected.TaskID && join.TimerHandle().Kind() == timeridentity.TimerHandleJoinComplete && !join.DeadlineAt.IsZero() {
		handle, err := timeridentity.JoinTimeoutHandle(join.JoinRef())
		if err != nil {
			return err
		}
		deadline, err := join.WithTimerHandle(handle, join.DeadlineAt)
		if err != nil {
			return err
		}
		expected, err = WorkflowJoinAdmission(deadline, schedule.Command.ExecutionMode)
		if err != nil {
			return err
		}
	}
	hash, err := expected.ImmutableHash()
	if err != nil {
		return err
	}
	if hash != schedule.ImmutableHash {
		return fmt.Errorf("arrival join schedule contradicts its exact arm, owner, handle, payload or due coordinate")
	}
	return nil
}
