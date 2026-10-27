package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Serve calls this only after construction, attachment and context publication
// have completed. Finite/offline entry points never acquire this binding.
func (rt *Runtime) ArmDeclaredFlowClocks(ctx context.Context, activations []StandingActivation) error {
	if rt == nil || rt.Options.WorkflowModule == nil {
		return fmt.Errorf("clock deployment requires its runtime source")
	}
	if !rt.Options.EnableDeclaredClockBinding {
		return nil
	}
	source := rt.Options.WorkflowModule.SemanticSource()
	for _, activation := range activations {
		if activation.RunState != runlifecycle.StateLive && activation.RunState != runlifecycle.StatePaused {
			continue
		}
		for _, declaration := range semanticview.ClockSchedules(source) {
			if declaration.FlowID != activation.FlowPath {
				continue
			}
			if err := rt.armFlowClock(ctx, source, activation, declaration); err != nil {
				return err
			}
		}
	}
	return nil
}

// A local finite host may use the serve bootstrap, but it is not a deployment.
// Independently admitted ingress remains separate from clock binding.
func selectStandingClockBindings(declarations []StandingTargetDeclaration, enabled bool) []StandingTargetDeclaration {
	if enabled {
		return declarations
	}
	selected := make([]StandingTargetDeclaration, 0, len(declarations))
	for _, declaration := range declarations {
		declaration.Clocks = nil
		if len(declaration.Ingress) == 0 {
			continue
		}
		selected = append(selected, declaration)
	}
	return selected
}

func (rt *Runtime) armFlowClock(ctx context.Context, source semanticview.Source, activation StandingActivation, declaration semanticview.ClockSchedule) error {
	if rt.GenericSchedules == nil || rt.workOccurrence == nil {
		return fmt.Errorf("clock deployment requires its durable schedule and execution owners")
	}
	command, err := flowClockCommand(source, activation, declaration, rt.ExecutionPosture.RootMode())
	if err != nil {
		return err
	}
	if activation.BundleHash != rt.Options.SourceArtifactFact.BundleHash() {
		return fmt.Errorf("clock deployment activation belongs to another source")
	}
	lease, err := rt.workOccurrence.Begin(rt.authorActivityContext(ctx))
	if err != nil {
		return fmt.Errorf("acquire clock deployment: %w", err)
	}
	runCtx := correlation.WithRunID(lease.Context(), activation.RunID)
	runCtx = correlation.WithSourceArtifactFact(runCtx, rt.Options.SourceArtifactFact)
	_, admitErr := rt.GenericSchedules.Admit(runCtx, command)
	return errors.Join(admitErr, lease.Done())
}

func flowClockCommand(source semanticview.Source, activation StandingActivation, schedule semanticview.ClockSchedule, mode executionmode.Mode) (genericschedule.AdmissionCommand, error) {
	if (activation.RunState != runlifecycle.StateLive && activation.RunState != runlifecycle.StatePaused) || activation.RunID == "" || schedule.FlowID != activation.FlowPath {
		return genericschedule.AdmissionCommand{}, fmt.Errorf("clock requires its exact active deployment declaration")
	}
	instance, err := flowidentity.StandingForGeneration(source, schedule.FlowID, activation.RunID)
	if err != nil {
		return genericschedule.AdmissionCommand{}, err
	}
	if instance != activation.Construction {
		return genericschedule.AdmissionCommand{}, fmt.Errorf("clock requires the acknowledged constructed instance")
	}
	declaration, err := eventidentity.AdmitPublicationDeclaration(schedule.FlowID, schedule.Declaration.Emit)
	if err != nil {
		return genericschedule.AdmissionCommand{}, err
	}
	scope, publicationInstance := eventidentity.PublicationStatic, instance.InstancePath
	if schedule.FlowID == "." {
		scope, publicationInstance = eventidentity.PublicationRoot, ""
	}
	eventName, err := eventidentity.ProjectPublication(declaration, scope, schedule.FlowID, publicationInstance)
	if err != nil {
		return genericschedule.AdmissionCommand{}, err
	}
	routingSource, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
		FlowID: schedule.FlowID, FlowInstance: instance.InstancePath, EntityID: instance.EntityID,
	})
	if err != nil {
		return genericschedule.AdmissionCommand{}, err
	}
	due := genericschedule.CronDue(schedule.Declaration.Cron)
	if schedule.Declaration.Every != "" {
		interval, err := time.ParseDuration(schedule.Declaration.Every)
		if err != nil {
			return genericschedule.AdmissionCommand{}, err
		}
		due = genericschedule.EveryDue(interval)
	}
	command := genericschedule.AdmissionCommand{
		ScheduleKey: schedule.Name, OwnerKind: genericschedule.OwnerInstance, OwnerID: schedule.FlowID,
		RunID: activation.RunID, EntityID: instance.EntityID, FlowInstance: instance.InstancePath,
		EventType: eventName, Payload: semanticvalue.EmptyObject(), RoutingSource: routingSource,
		ExecutionMode: mode, Due: due,
	}
	return command, command.Validate()
}
