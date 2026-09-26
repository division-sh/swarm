package runforkexecution

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedRuntimeContainerBindsExactTypedForkPoint(t *testing.T) {
	sourceRun, forkRun := uuid.NewString(), uuid.NewString()
	for _, point := range []runfork.RunForkPoint{
		{Kind: runfork.RunForkPointEvent, EventID: uuid.NewString(), Revision: 2},
		{Kind: runfork.RunForkPointDeploymentRevision, Revision: 2},
	} {
		plan := runfork.RunForkPlan{SourceRunID: sourceRun, ForkPoint: point}
		admission := runfork.RunForkSelectedContractExecutionAdmission{
			SourceRunID: sourceRun, ForkRunID: forkRun, ForkPoint: point, ForkEventID: point.EventID,
		}
		got, err := validateSelectedContractRuntimeContainerForkPoint(plan, admission, sourceRun, forkRun, point.EventID)
		if err != nil || got != point {
			t.Fatalf("exact point %s rejected: got=%+v err=%v", point.Kind, got, err)
		}
		for name, mutate := range map[string]func(*runfork.RunForkSelectedContractExecutionAdmission){
			"wrong kind": func(a *runfork.RunForkSelectedContractExecutionAdmission) {
				a.ForkPoint.Kind = runfork.RunForkPointEvent
			},
			"wrong revision": func(a *runfork.RunForkSelectedContractExecutionAdmission) { a.ForkPoint.Revision++ },
			"wrong event":    func(a *runfork.RunForkSelectedContractExecutionAdmission) { a.ForkEventID = uuid.NewString() },
		} {
			t.Run(string(point.Kind)+"/"+name, func(t *testing.T) {
				bad := admission
				mutate(&bad)
				if name == "wrong kind" && point.Kind == runfork.RunForkPointEvent {
					bad.ForkPoint.Kind = runfork.RunForkPointDeploymentRevision
				}
				if _, err := validateSelectedContractRuntimeContainerForkPoint(plan, bad, sourceRun, forkRun, point.EventID); err == nil {
					t.Fatal("contradictory selected fork point accepted")
				}
			})
		}
		if _, err := validateSelectedContractRuntimeContainerForkPoint(plan, admission, sourceRun, forkRun, uuid.NewString()); err == nil {
			t.Fatal("invented event ID accepted")
		}
	}
}
