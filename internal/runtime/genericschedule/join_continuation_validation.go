package genericschedule

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
)

func validateExactJoinContinuationCommand(command AdmissionCommand, handle timeridentity.TimerHandle) error {
	ref, valid := handle.JoinRef()
	if !valid {
		return fmt.Errorf("join continuation requires a typed handle")
	}
	if ref.Mode() == timeridentity.JoinRefModeArrival {
		return validateExactWorkflowJoinAdmissionCommand(command, handle)
	}
	if ref.Mode() != timeridentity.JoinRefModeFanOutDelivery {
		return fmt.Errorf("join continuation kind is unsupported")
	}
	if err := command.Validate(); err != nil {
		return err
	}
	summary, err := joinContinuationBarrierSummary(command)
	if err != nil {
		return err
	}
	expected, err := fanOutBarrierPayload(handle, summary)
	if err != nil {
		return err
	}
	if !command.Payload.Equal(expected) {
		return fmt.Errorf("barrier continuation payload contradicts its exact handle and frozen summary")
	}
	return nil
}

func joinContinuationBarrierSummary(command AdmissionCommand) (fanoutbarrier.Summary, error) {
	payload, object := command.Payload.Interface().(map[string]any)
	if !object {
		return fanoutbarrier.Summary{}, fmt.Errorf("barrier continuation payload must be an object")
	}
	context, object := payload["join"].(map[string]any)
	if !object {
		return fanoutbarrier.Summary{}, fmt.Errorf("barrier continuation requires its frozen terminal summary")
	}
	return fanoutbarrier.SummaryFromContext(context)
}
