package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestQ10DataOnlySetupAdmissionAndEmptyBirthBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, _ := openStateOnlyAcquisitionStore(t, backend)
			setup, ok := selected.(interface {
				SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
			})
			if !ok {
				t.Fatal("selected store has no setup owner")
			}
			at := time.Now().UTC().Truncate(time.Microsecond)
			request := pipeline.ScenarioSetupRequest{RunID: uuid.NewString(), CreatedAt: at, Entities: []pipeline.ScenarioSetupEntityRequest{{Alias: "empty", EntityID: uuid.NewString(), FlowInstance: "import/one", EntityType: "review_item", Fields: map[string]any{}}}}
			before := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
			for _, change := range []string{"stage", "blank_stage", "empty_gates", "gate", "unused_second_row"} {
				t.Run(change, func(t *testing.T) {
					hostile := request
					hostile.Entities = append([]pipeline.ScenarioSetupEntityRequest(nil), request.Entities...)
					switch change {
					case "stage":
						hostile.Entities[0].CurrentState = mutationlog.PresentLifecycleState("unknown")
					case "blank_stage":
						hostile.Entities[0].CurrentState = mutationlog.PresentLifecycleState("")
					case "empty_gates":
						hostile.Entities[0].Gates = map[string]bool{}
					case "gate":
						hostile.Entities[0].Gates = map[string]bool{"approved": true}
					case "unused_second_row":
						hostile.Entities = append(hostile.Entities, pipeline.ScenarioSetupEntityRequest{Alias: "unused", EntityID: uuid.NewString(), EntityType: "review_item", CurrentState: mutationlog.PresentLifecycleState("ready")})
					}
					alias := "empty"
					if change == "unused_second_row" {
						alias = "unused"
					}
					wantError := "data-only entity " + alias + " cannot carry lifecycle inputs"
					result, err := setup.SetupScenarioEntities(ctx, hostile)
					if err == nil || err.Error() != wantError || result.Acknowledged || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
						t.Fatalf("data-only input mutated state before refusal: %+v %v", result, err)
					}
				})
			}
			result, err := setup.SetupScenarioEntities(ctx, request)
			if err != nil || !result.Acknowledged || len(result.Entities) != 1 || result.Entities[0].CurrentState != nil {
				t.Fatalf("genuine empty import rejected or acquired lifecycle: %+v %v", result, err)
			}
			var headerRows, mutationRows int
			for table, count := range map[string]*int{"flow_instances": &headerRows, "entity_mutations": &mutationRows} {
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", request.RunID).Scan(count); err != nil {
					t.Fatal(err)
				}
			}
			if headerRows != 0 || mutationRows != 0 {
				t.Fatalf("empty data birthed lifecycle or fake diff: headers=%d mutations=%d", headerRows, mutationRows)
			}
			var raw string
			if err := db.QueryRowContext(ctx, `SELECT CAST(fact AS TEXT) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 AND present=true`, request.RunID, request.Entities[0].EntityID).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var fact map[string]any
			if err := json.Unmarshal([]byte(raw), &fact); err != nil {
				t.Fatal(err)
			}
			if fact["construction_kind"] != "imported_state" || fact["current_state"] != nil || fact["created_at"] != nil || fact["updated_at"] != nil || fact["data_created_at"] == nil || fact["data_updated_at"] == nil {
				t.Fatalf("immutable empty birth has missing data clocks or invented lifecycle: %s", raw)
			}
			committed := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
			if replay, err := setup.SetupScenarioEntities(ctx, request); err != nil || !replay.Acknowledged || !reflect.DeepEqual(committed, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
				t.Fatalf("empty import replay changed committed evidence: %+v %v", replay, err)
			}
		})
	}
}
