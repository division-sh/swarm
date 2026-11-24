package cataloge2e

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestInheritedProducerReforkRecoveryBothStores(t *testing.T) {
	runWrappedC53ReforkBothStores(t, executeStagedC53Refork)
}

func executeStagedC53Refork(t *testing.T, h *runtimeHarness, ctx context.Context, selected c53ReforkStore, source semanticview.Source, plan runfork.RunForkPlan, request forkexecution.SelectedContractExecutionRequest) (forkexecution.SelectedContractExecutionResult, error) {
	proveC53WitnessRefusals(t, source, plan, request.ContractSelection)
	operation := catalogStagedForkOperation(t, ctx, selected, request.ContractSelection, request.SourceRunID, request.At)
	request.ForkOperation = &operation
	staged := stageCatalogSelectedContractFork(t, ctx, selected, request.Owner, request.SourceLoader, request.ContractSelection, request.AgentRuntime, request.SourceRunID, request.At, operation)
	recovery, found, err := selected.LoadRunForkSelectedContractRouteRecovery(ctx, staged.ForkRunID)
	if err != nil || !found || recovery.DynamicTopologyProofCount == 0 {
		t.Fatalf("staged refork did not retain its route proof: found=%t error=%v", found, err)
	}
	result, err := executeCatalogLiveFork(t, h, ctx, request)
	if err == nil {
		stored, found, readErr := selected.LoadRunForkSelectedContractRouteRecovery(ctx, result.Materialization.ForkRunID)
		if readErr != nil || !found || result.Materialization.ForkRunID != staged.ForkRunID || stored.RouteTopologyFingerprint != recovery.RouteTopologyFingerprint {
			t.Fatalf("refork recovery changed its staged child or topology proof: found=%t error=%v", found, readErr)
		}
	}
	return result, err
}

func proveC53WitnessRefusals(t *testing.T, source semanticview.Source, plan runfork.RunForkPlan, selection runfork.RunForkContractSelection) {
	t.Helper()
	witness, found := plan.InheritedConstructionWitness()
	selector, physical, err := plan.HistoricalRouteSelector(plan.ForkPoint.EventID)
	if err != nil || !found || !physical || len(witness.Ancestor.PendingWork) == 0 {
		t.Fatalf("native refork lacks its separate proof and physical cut: witness=%t physical=%t error=%v", found, physical, err)
	}
	base, err := plan.WithHistoricalRouteSelectors(plan.ForkPoint.Revision, []events.Event{selector})
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source, ContractSelection: selection})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing_witness", "missing_recipient", "crossed_cut", "crossed_lineage", "wrong_pin", "wrong_parent", "wrong_key"} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			witness, _ := plan.InheritedConstructionWitness()
			switch name {
			case "missing_recipient":
				witness.Ancestor.PendingWork = nil
			case "crossed_cut":
				witness.Lineage[0].SourceCut = runfork.RunForkPoint{Kind: runfork.RunForkPointStateRevision, Revision: witness.Lineage[0].SourceCut.Revision}
			case "crossed_lineage":
				witness.Lineage[0].SourceRunID = witness.Lineage[0].ChildRunID
			case "wrong_pin":
				witness.Ancestor.PendingWork[0].DeliveryRoute.ConnectClaim = events.ConnectExecutionClaim{}
			case "wrong_parent", "wrong_key":
				for i, entity := range witness.Ancestor.Entities {
					if entity.EntityID == witness.OriginalEntityID {
						meta := *entity.MaterializationMetadata
						if name == "wrong_parent" {
							meta.ParentInstance += "/foreign"
						} else {
							meta.InstanceKey += "-foreign"
						}
						witness.Ancestor.Entities[i].MaterializationMetadata = &meta
					}
				}
			}
			if name != "missing_witness" {
				var err error
				candidate, err = base.WithInheritedConstructionWitness(witness)
				if err != nil {
					return
				}
			}
			admitted, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
				Plan: candidate, Source: source, ContractSelection: selection, FrontierAdmission: frontier,
			})
			if err != nil {
				return
			}
			topology, err := forkexecution.BuildSelectedContractRouteTopology(forkexecution.SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: admitted})
			if err == nil && topology.DynamicTopologySupported {
				t.Fatalf("invalid ancestor witness proved topology: %s", name)
			}
		})
	}
}
