package runforkexecution

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestLocalFrontierProofCannotOverrideUnresolvedHistoricalTopology(t *testing.T) {
	f := newRecipientModelEqualityValidatorFixture(t)
	f.routeAdmission.SourceRouteFactsPresent = true
	f.routeAdmission.UnsupportedBlockers = append(f.routeAdmission.UnsupportedBlockers,
		runfork.RunForkUnsupportedBlocker{Code: runfork.RunForkBlockerFlowRouteHistoryUnproven},
		runfork.RunForkUnsupportedBlocker{Code: runfork.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven},
	)
	for _, instances := range [][]string{f.routeAdmission.DynamicFlowInstances, nil} {
		f.routeAdmission.DynamicFlowInstances = instances
		topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{Admission: f.frontier, RouteAdmission: f.routeAdmission})
		if err != nil {
			t.Fatal(err)
		}
		if topology.DynamicTopologySupported || topology.DynamicTopologyDisposition != runfork.RunForkSelectedContractDispositionFailClosed {
			t.Fatalf("local frontier overrode historical refusal: %+v", topology)
		}
		for _, code := range []string{runfork.RunForkBlockerFlowRouteHistoryUnproven, runfork.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven} {
			if !unsupportedBlockerHas(topology.UnsupportedBlockers, code) {
				t.Fatalf("topology lost historical blocker %s: %+v", code, topology)
			}
		}
		model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{Admission: f.frontier, RouteAdmission: f.routeAdmission, RouteTopology: topology})
		if err != nil {
			t.Fatal(err)
		}
		if model.RecipientPlanning == nil || model.RecipientPlanning.RecipientPlanningSupported {
			t.Fatalf("unresolved history acquired supported recipient planning: %+v", model)
		}
	}
}
