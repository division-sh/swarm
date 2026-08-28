package runtime

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRunLifecycleRequiresGenericSchedulesOnlyForFanOutDeliveryBarriers(t *testing.T) {
	tests := []struct {
		name  string
		joins []runtimecontracts.WorkflowJoinPlan
		want  bool
	}{
		{name: "no joins"},
		{name: "arrival join", joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeArrival}}},
		{name: "fan-out delivery barrier", joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeFanOutDelivery}}, want: true},
		{name: "mixed joins", joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeArrival}, {Mode: runtimecontracts.WorkflowJoinModeFanOutDelivery}}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{Joins: test.joins},
			})
			if got := runLifecycleRequiresGenericSchedules(source); got != test.want {
				t.Fatalf("runLifecycleRequiresGenericSchedules() = %t, want %t", got, test.want)
			}
		})
	}
}
