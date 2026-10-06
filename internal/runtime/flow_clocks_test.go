package runtime

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestDeclaredClockBindingRequiresDeploymentSelection(t *testing.T) {
	for _, nested := range []bool{false, true} {
		source := loadWorkflowValidationSourceAt(t, canonicalrouting.CopyClockDeployment(t, nested))
		for _, enabled := range []bool{false, true} {
			rt := &Runtime{Options: RuntimeOptions{
				WorkflowModule:     semanticOnlyWorkflowRuntime{source: source},
				SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA), EnableDeclaredClockBinding: enabled,
			}}
			candidates, err := rt.PlanStandingServiceCandidates()
			want := 0
			if enabled {
				want = 1
			}
			if err != nil || len(candidates) != want {
				t.Fatalf("nested=%t enabled=%t standing candidates=%+v err=%v", nested, enabled, candidates, err)
			}
			if !enabled {
				if err := rt.ArmDeclaredFlowClocks(t.Context(), []StandingActivation{{FlowPath: "."}}, nil); err != nil {
					t.Fatalf("finite host acquired clock execution: %v", err)
				}
			}
			if len(semanticview.ClockSchedules(source)) != 1 {
				t.Fatal("deployment selection removed the compiled declaration")
			}
		}
	}
}

func TestFiniteClockSelectionPreservesIndependentStandingDeclarations(t *testing.T) {
	source := loadWorkflowValidationSourceAt(t, canonicalrouting.CopyClockDeployment(t, false))
	declarations, err := ResolveStandingTargetDeclarations(source, nil)
	if err != nil {
		t.Fatal(err)
	}
	before := append([]StandingTargetDeclaration(nil), declarations...)
	independent := StandingTargetDeclaration{FlowPath: "legacy-standing"}
	selected := selectStandingClockBindings(append(declarations, independent), false)
	if len(selected) != 1 || !reflect.DeepEqual(selected[0], independent) {
		t.Fatalf("finite host changed an independent standing declaration: %+v", selected)
	}
	declarations[0].AuthoredStanding = true
	selected = selectStandingClockBindings(declarations, false)
	if len(selected) != 1 || len(selected[0].Clocks) != 0 || !selected[0].AuthoredStanding {
		t.Fatalf("finite host erased independent standing authority or enabled its clocks: %+v", selected)
	}
	declarations[0].AuthoredStanding = false
	if !reflect.DeepEqual(declarations, before) {
		t.Fatal("binding selection mutated declaration ownership")
	}
}

func TestClockDeploymentDeclarationsAndExactPublication(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "root", true: "nested"}[nested], func(t *testing.T) {
			source := loadWorkflowValidationSourceAt(t, canonicalrouting.CopyClockDeployment(t, nested))
			declarations, err := ResolveStandingTargetDeclarations(source, nil)
			if err != nil || len(declarations) != 1 || len(declarations[0].Clocks) != 1 || len(declarations[0].Ingress) != 0 || declarations[0].Alias != "" {
				t.Fatalf("clock-only declaration=%+v err=%v", declarations, err)
			}
			schedule := declarations[0].Clocks[0]
			runID := uuid.NewString()
			instance, err := flowidentity.StandingForGeneration(source, schedule.FlowID, runID)
			if err != nil {
				t.Fatal(err)
			}
			serviceID := flowidentity.StandingServiceID(schedule.FlowID)
			disposition, err := runlifecycle.ClassifyStandingRestart(runlifecycle.StandingRestartFact{
				ExactCurrent: true, ServiceID: serviceID, RunID: runID, Generation: 1,
				DeclarationPresent: true, BindingEnabled: true, EffectiveState: "active", OperatorOverride: "none", RunState: "running",
			})
			if err != nil {
				t.Fatal(err)
			}
			activation := StandingActivation{
				ServiceID: serviceID, RunID: runID, Generation: 1, FlowPath: schedule.FlowID,
				InstanceID: instance.InstanceID, FlowInstance: instance.InstancePath, EntityID: instance.EntityID,
				RestartDisposition: disposition,
			}
			command, err := flowClockCommand(source, activation, schedule, executionmode.Live)
			if err != nil || command.OwnerKind != genericschedule.OwnerInstance || command.OwnerID != schedule.FlowID ||
				command.RoutingSource.Kind() != events.RoutingSourceStaticFlow || command.FlowInstance != instance.InstancePath || command.RunID != runID || command.Payload.Len() != 0 {
				t.Fatalf("ordinary exact clock command=%+v err=%v", command, err)
			}
			wantEvent := "poll.tick"
			if nested {
				wantEvent = instance.InstancePath + "/poll.tick"
			}
			if command.EventType != wantEvent {
				t.Fatalf("clock event=%s want=%s", command.EventType, wantEvent)
			}
			identities, err := StandingExecutionIdentities(nil, []StandingActivation{activation})
			if err != nil || len(identities) != 1 || identities[0].RunID != runID || identities[0].ServiceID != serviceID || identities[0].Generation != 1 {
				t.Fatalf("transport-independent work identities=%+v err=%v", identities, err)
			}
			before := activation
			for _, mutation := range []func(*StandingActivation){
				func(a *StandingActivation) { a.RunID = uuid.NewString() },
				func(a *StandingActivation) { a.Generation++ },
				func(a *StandingActivation) { a.InstanceID = "foreign" },
				func(a *StandingActivation) { a.FlowInstance = "foreign" },
				func(a *StandingActivation) { a.EntityID = uuid.NewString() },
				func(a *StandingActivation) { a.ServiceID = uuid.NewString() },
			} {
				invalid := activation
				mutation(&invalid)
				if _, err := flowClockCommand(source, invalid, schedule, executionmode.Live); err == nil {
					t.Fatalf("foreign activation admitted: %+v", invalid)
				}
			}
			conflict := StandingTarget{ServiceID: serviceID, RunID: uuid.NewString(), Generation: 1}
			if _, err := StandingExecutionIdentities([]StandingTarget{conflict}, []StandingActivation{activation}); err == nil {
				t.Fatal("conflicting transport coordinate overruled acknowledged clock identity")
			}
			if !reflect.DeepEqual(activation, before) || len(semanticview.ClockSchedules(source)) != 1 {
				t.Fatal("clock projection mutated its input or source")
			}
		})
	}
}
