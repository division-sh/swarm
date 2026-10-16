package runforkexecution

import (
	"reflect"
	"testing"
)

func TestSelectedBranchPointComponentDoesNotBypassPublicExecutionAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, entry := range []string{"state_only_executable", "eventless_staged"} {
			t.Run(backend+"/"+entry, func(t *testing.T) {
				fixture := newActivityForkFixture(t, backend)
				if entry == "eventless_staged" {
					proveQ1EventlessStagedAdmission(t, fixture.owner)
					return
				}
				ctx, request, _ := q1StateOnlyGateComponentSource(t, fixture.owner)
				before := q1ComponentSnapshot(t, fixture.owner.ports.fork)
				result, err := ExecuteSelectedContractRunFork(ctx, request)
				const want = "selected-contract event-point execution requires selected frontier events or an exact owed schedule"
				if err == nil || err.Error() != want || result.Materialization.ForkRunID != "" || result.Activation.Activated {
					t.Fatalf("public executable entry accepted component-only work: %+v err=%v, want %s", result, err, want)
				}
				if !reflect.DeepEqual(before, q1ComponentSnapshot(t, fixture.owner.ports.fork)) {
					t.Fatal("public executable refusal changed durable state")
				}
			})
		}
	}
}

func proveQ1EventlessStagedAdmission(t *testing.T, owner SelectedContractExecutionOwner) {
	t.Helper()
	ctx, request, _ := q1DeploymentComponentSource(t, owner)
	prepared, err := owner.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	materialized, acknowledged, err := owner.materializePrepared(prepared.operation.PreparationContext(), prepared)
	if err != nil || !acknowledged {
		t.Fatalf("original eventless native child: %+v err=%v", materialized, err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	before := q1ComponentSnapshot(t, owner.ports.fork)
	result, err := ActivateSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{
		ForkRunID: materialized.ForkRunID, AllowSourceFreeze: true, Store: owner.ports.fork.(SelectedContractActivationStore),
		ExecutionOwner: owner, SourceLoader: request.SourceLoader, AgentRuntime: request.AgentRuntime,
	})
	const want = "contract-swap boot/resume admission requires fork/source/event identity from selected binding"
	if err == nil || err.Error() != want || result.Activated {
		t.Fatalf("public staged entry accepted eventless component activation: %+v err=%v, want %s", result, err, want)
	}
	if !reflect.DeepEqual(before, q1ComponentSnapshot(t, owner.ports.fork)) {
		t.Fatal("eventless staged refusal changed durable state")
	}
}
