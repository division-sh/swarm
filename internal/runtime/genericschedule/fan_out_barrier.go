package genericschedule

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
)

// FanOutBarrierAdmission is shared by live closure, fork materialization, and
// historical admission. A label match cannot substitute for this full command.
func FanOutBarrierAdmission(registration fanoutbarrier.Registration, summary fanoutbarrier.Summary, at time.Time) (AdmissionCommand, error) {
	if err := registration.Validate(); err != nil {
		return AdmissionCommand{}, err
	}
	if err := summary.Validate(); err != nil {
		return AdmissionCommand{}, err
	}
	payload := registration.Handle.PayloadMetadata()
	payload["join"] = summary.Context()
	semanticPayload, err := canonicaljson.FromGo(payload)
	if err != nil {
		return AdmissionCommand{}, err
	}
	flowInstance := ""
	if registration.RoutingSource.Kind() == events.RoutingSourceFlowOwnedControl {
		flowInstance = registration.RoutingSource.Route().FlowInstance
	}
	command := AdmissionCommand{
		ScheduleKey: registration.Handle.TaskID(), RunID: registration.IntentKey.RunID, EntityID: registration.EntityID, FlowInstance: flowInstance,
		OwnerKind: OwnerSystem, OwnerID: "workflow-runtime", EventType: registration.Handle.EventType(), Payload: semanticPayload,
		RoutingSource: registration.RoutingSource, ExecutionMode: registration.ExecutionMode, Due: AbsoluteDue(at), TaskID: registration.Handle.TaskID(),
	}
	return command, command.Validate()
}

func ValidateFanOutBarrierScheduleRelation(barrier fanoutbarrier.Barrier, activation Activation) error {
	if err := barrier.Validate(); err != nil {
		return err
	}
	if err := activation.Validate(); err != nil {
		return err
	}
	if barrier.Summary == nil || barrier.ScheduleActivationID != activation.ID || barrier.ScheduleKey != activation.Command.ScheduleKey {
		return fmt.Errorf("fan-out barrier lacks its exact schedule activation")
	}
	expected, err := FanOutBarrierAdmission(barrier.Registration, *barrier.Summary, activation.InitialDueAt)
	if err != nil {
		return err
	}
	hash, err := expected.ImmutableHash()
	if err != nil {
		return err
	}
	if hash != activation.ImmutableHash {
		return fmt.Errorf("fan-out barrier schedule contradicts its exact owner, handle, payload or due basis")
	}
	switch barrier.Status {
	case fanoutbarrier.StatusClosedPending:
		if activation.Status == StatusActive || activation.Status == StatusFired {
			return nil
		}
	case fanoutbarrier.StatusFired, fanoutbarrier.StatusOutcomeDeadLettered:
		if activation.Status == StatusFired {
			return nil
		}
	case fanoutbarrier.StatusSuppressedRunTerminal, fanoutbarrier.StatusSuppressedGenerationSuperseded:
		if activation.Status == StatusCancelled || activation.Status == StatusFired {
			return nil
		}
	}
	return fmt.Errorf("fan-out barrier state %s contradicts schedule state %s", barrier.Status, activation.Status)
}
