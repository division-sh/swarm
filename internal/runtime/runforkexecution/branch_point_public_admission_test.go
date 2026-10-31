package runforkexecution

import (
	"reflect"
	"testing"
)

func TestSelectedBranchPointComponentDoesNotBypassPublicExecutionAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, entry := range []string{"state_only_executable"} {
			t.Run(backend+"/"+entry, func(t *testing.T) {
				fixture := newActivityForkFixture(t, backend)
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
