package runtimepersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedForkFixedPlanRefusalClassifiesComparisonsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			postgres := backend.name == "postgres"
			ctx, request := forkWorkflowOwnershipRequest(t, fixture, postgres)
			store := fixture.store.(forkWorkflowOwnershipStore)
			before := snapshotForkHistoricalExecutionTables(t, fixture.db, postgres)
			for _, cut := range []struct {
				name   string
				mutate func(*runfork.SelectedForkPreparation)
				flags  string
			}{
				{
					name: "plan_fingerprint",
					mutate: func(preparation *runfork.SelectedForkPreparation) {
						prefix := "0"
						if preparation.Coordinates.AdmittedPlanFingerprint[0] == '0' {
							prefix = "1"
						}
						preparation.Coordinates.AdmittedPlanFingerprint = prefix + preparation.Coordinates.AdmittedPlanFingerprint[1:]
					},
					flags: "source_run_equal=true fork_point_equal=true timestamp_representation_equal=true timestamp_instant_equal=true fork_event_equal=true bundle_equal=true plan_fingerprint_equal=false",
				},
				{
					name: "timestamp_representation",
					mutate: func(preparation *runfork.SelectedForkPreparation) {
						preparation.ForkPoint.Timestamp = preparation.ForkPoint.Timestamp.In(time.FixedZone("diagnostic", 3600))
					},
					flags: "source_run_equal=true fork_point_equal=false timestamp_representation_equal=false timestamp_instant_equal=true fork_event_equal=true bundle_equal=true plan_fingerprint_equal=true",
				},
			} {
				t.Run(cut.name, func(t *testing.T) {
					changed := request
					cut.mutate(&changed.Preparation)
					if err := changed.Preparation.Validate(); err != nil {
						t.Fatalf("diagnostic cut must reach the fixed-plan guard: %v", err)
					}
					materialized, err := store.MaterializeRunForkForSelectedContractExecution(ctx, changed)
					want := "selected preparation differs from transaction's fixed admitted plan [" + cut.flags + "]"
					if err == nil || err.Error() != want || materialized.ForkRunID != "" {
						t.Fatalf("fixed-plan refusal = %+v, %v; want %s and no fork", materialized, err, want)
					}
					if after := snapshotForkHistoricalExecutionTables(t, fixture.db, postgres); !reflect.DeepEqual(before, after) {
						t.Fatal("diagnostic refusal changed durable evidence")
					}
				})
			}
			materialized, err := store.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil || materialized.ForkRunID == "" {
				t.Fatalf("unchanged preparation must still materialize: %+v, %v", materialized, err)
			}
		})
	}
}
