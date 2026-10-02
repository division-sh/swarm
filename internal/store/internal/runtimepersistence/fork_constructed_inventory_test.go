package runtimepersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestForkActivationInventoriesConstructedHeadersBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, shape := range []string{"fieldless", "fields"} {
				t.Run(shape, func(t *testing.T) {
					fixture := backend.open(t)
					_, child, _ := seedSelectedOrdinaryRootProjectionFixture(t, fixture, backend.name == "postgres", shape)
					var headers, fields int
					if err := fixture.db.QueryRowContext(testAuthorActivityContext(), `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, child.ForkRunID).Scan(&headers); err != nil {
						t.Fatal(err)
					}
					if err := fixture.db.QueryRowContext(testAuthorActivityContext(), `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, child.ForkRunID).Scan(&fields); err != nil {
						t.Fatal(err)
					}
					if headers == 0 || (shape == "fieldless" && headers <= fields) {
						t.Fatalf("fixture headers=%d fields=%d", headers, fields)
					}
					owner := fixture.store.(interface {
						ActivateRunFork(context.Context, runfork.RunForkActivateRequest) (runfork.RunForkActivation, error)
					})
					result, err := owner.ActivateRunFork(testAuthorActivityContext(), runfork.RunForkActivateRequest{ForkRunID: child.ForkRunID})
					if result.MaterializedEntityCount != headers {
						t.Fatalf("constructed inventory=%d headers=%d fields=%d later admission=%v", result.MaterializedEntityCount, headers, fields, err)
					}
					if err == nil || result.Activated {
						t.Fatalf("inventory proof unexpectedly crossed source-freeze admission: result=%+v err=%v", result, err)
					}
					selected := fixture.store.(interface {
						ActivateRunForkForSelectedContractExecution(context.Context, runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)
					})
					selectedResult, selectedErr := selected.ActivateRunForkForSelectedContractExecution(testAuthorActivityContext(), runfork.RunForkSelectedContractExecutionActivateRequest{ForkRunID: child.ForkRunID})
					if selectedResult.MaterializedEntityCount != headers || selectedErr == nil || selectedResult.Activated {
						t.Fatalf("selected constructed inventory=%d headers=%d fields=%d later admission=%v", selectedResult.MaterializedEntityCount, headers, fields, selectedErr)
					}
				})
			}
		})
	}
}
