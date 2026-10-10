package runforkpersistence

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// Fault injection at native readback, not proof of an ordinary corrupt writer.
func TestTransferredInventoryBindsPhysicalOwnerBothStores(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		for _, flow := range []struct {
			name string
			root bool
		}{{"root", true}, {"flow", false}} {
			for _, fault := range []string{"control", "progress", "entity", "path", "template"} {
				t.Run(dialect+"/"+flow.name+"/"+fault, func(t *testing.T) {
					snapshot, plan, _, _, child := publishedArrivalTransferFixture(t, flow.root, true, deliverylifecycle.StatusPending)
					if err := attachRunForkPublishedArrivals(snapshot, plan.Entities, plan.JoinSchedules); err != nil {
						t.Fatal(err)
					}
					entity := plan.Entities[0]
					projection, err := projectRunForkEntityOwnership(plan.SourceRunID, child, entity.EntityID, entity.MaterializationMetadata.FlowInstance)
					if err != nil {
						t.Fatal(err)
					}
					_, raw, _, err := projectRunForkEntityExecutionState(entity, plan.SourceRunID, child, projection)
					if err != nil {
						t.Fatal(err)
					}
					transfers, err := runForkTransferredJoinAccumulator(raw)
					if err != nil || len(transfers) != 1 {
						t.Fatalf("fixture transfer: %d %v", len(transfers), err)
					}
					body, err := json.Marshal(raw)
					if err != nil {
						t.Fatal(err)
					}
					id, path, template := projection.Fork.EntityID, projection.Fork.FlowInstance, entity.MaterializationMetadata.FlowTemplate
					stage, revision := "pending", 1
					switch fault {
					case "progress":
						stage, revision = "completed", 9
					case "entity":
						id = uuid.NewString()
					case "path":
						path += "/wrong"
					case "template":
						template += ".wrong"
					}
					f := openTimerHistoryNativeFixture(t, dialect)
					at := time.Now().UTC().Truncate(time.Microsecond)
					ctx := f.runContext(t, child, at)
					_, err = f.db.ExecContext(ctx, `INSERT INTO flow_instances
				(run_id, instance_path, entity_id, flow_template, mode, stage_defined, current_state, gates, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at)
				VALUES ($1,$2,$3,$4,'static',true,$7,'{}','{}',$5,$8,$6,$6,$6)`, child, path, id, template, string(body), at, stage, revision)
					if err != nil {
						t.Fatal(err)
					}
					rollback := errors.New("transferred-owner read-only probe")
					result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
						rows, gotErr := readRunForkTransferredJoinInventory(ctx, attempt, plan, child)
						if fault == "control" || fault == "progress" {
							if gotErr != nil || len(rows) != 1 {
								t.Errorf("valid native owner: %d %v", len(rows), gotErr)
							}
						} else if gotErr == nil {
							t.Errorf("native readback accepted transferred evidence beneath mismatched physical %s", fault)
						}
						return runfork.RunForkReplayResumeAdmission{}, rollback
					})
					if !errors.Is(result.Err(), rollback) {
						t.Fatal(result.Err())
					}
				})
			}
		}
	}
}
