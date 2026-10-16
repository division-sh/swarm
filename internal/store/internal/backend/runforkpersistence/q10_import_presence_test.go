package runforkpersistence

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestQ10EmptyImportHistoricalMembershipAndClockProjection(t *testing.T) {
	at := time.Unix(100, 0).UTC()
	updated := at.Add(time.Minute)
	reversed := at.Add(-time.Second)
	for _, change := range []string{"exact", "missing_creation_clock", "missing_update_clock", "reversed_clocks", "lifecycle_history", "gate_history", "bookkeeping_history", "accumulator_history", "duplicate_birth"} {
		t.Run(change, func(t *testing.T) {
			fact := runForkRevisionEntityMetadata{EntityID: "entity-1", FlowInstance: "import/one", EntityType: "task", ConstructionKind: "imported_state", DataCreatedAt: &at, DataUpdatedAt: &updated}
			snapshot := &runForkRevisionSnapshot{EntityMetadata: []runForkRevisionEntityMetadata{fact}}
			switch change {
			case "missing_creation_clock":
				snapshot.EntityMetadata[0].DataCreatedAt = nil
			case "missing_update_clock":
				snapshot.EntityMetadata[0].DataUpdatedAt = nil
			case "reversed_clocks":
				snapshot.EntityMetadata[0].DataUpdatedAt = &reversed
			case "lifecycle_history":
				snapshot.EntityMutations = []runForkRevisionEntityMutation{{EntityID: fact.EntityID, Domain: string(mutationlog.DomainLifecycleState), NewValue: []byte(`"unknown"`), CreatedAt: updated}}
			case "gate_history":
				snapshot.EntityMutations = []runForkRevisionEntityMutation{{EntityID: fact.EntityID, Domain: string(mutationlog.DomainGate), Path: "approved", NewValue: []byte(`true`), CreatedAt: updated}}
			case "bookkeeping_history":
				snapshot.EntityMutations = []runForkRevisionEntityMutation{{EntityID: fact.EntityID, Domain: string(mutationlog.DomainBookkeeping), Path: "step", NewValue: []byte(`"retained"`), CreatedAt: updated}}
			case "accumulator_history":
				snapshot.EntityMutations = []runForkRevisionEntityMutation{{EntityID: fact.EntityID, Domain: string(mutationlog.DomainAccumulator), Path: "total", NewValue: []byte(`7`), CreatedAt: updated}}
			case "duplicate_birth":
				snapshot.EntityMetadata = append(snapshot.EntityMetadata, fact)
			}
			history := strings.HasSuffix(change, "_history")
			entities, err := loadRunForkEntityStates(snapshot)
			if history && err != nil {
				t.Fatalf("valid domain history failed before imported-owner admission: %v", err)
			}
			if err == nil {
				var admission runForkMaterializedEntitySnapshotMetadataAdmission
				entities, admission, err = attachRunForkMaterializedEntitySnapshotMetadata(snapshot, entities)
				if len(admission.Blockers) != 0 {
					if change == "exact" {
						t.Fatalf("genuine empty import refused: %+v", admission.Blockers)
					}
					if history {
						if err != nil || len(entities) != 1 || entities[0].MaterializationMetadata != nil || len(admission.Blockers) != 1 || admission.Blockers[0].Code != runfork.RunForkBlockerEntitySnapshotMetadataUnproven || admission.Blockers[0].Message != "imported entity entity-1 cannot carry lifecycle history" {
							t.Fatalf("imported domain history lost exact metadata-owner refusal: entities=%+v admission=%+v err=%v", entities, admission, err)
						}
					}
					return
				}
			}
			if history {
				t.Fatalf("imported domain history did not reach its required metadata-owner blocker: %v", err)
			}
			if change != "exact" {
				if err == nil {
					t.Fatal("malformed birth or invented lifecycle was admitted")
				}
				return
			}
			if err != nil || len(entities) != 1 {
				t.Fatalf("empty import lost historical membership: %+v %v", entities, err)
			}
			entity := entities[0]
			if entity.CurrentState != nil || entity.EnteredStateAt != nil || entity.Gates != nil || entity.Bookkeeping != nil || entity.Accumulator != nil || !reflect.DeepEqual(entity.Fields, map[string]any{}) {
				t.Fatalf("empty import acquired lifecycle or lost empty fields: %+v", entity)
			}
			meta, err := loadRunForkEntityMetadata(runfork.RunForkPlan{Entities: entities})
			if err != nil {
				t.Fatal(err)
			}
			projection, err := projectRunForkHistoricalFields("source", "child", entity.EntityID, "", entity, meta[entity.EntityID])
			if err != nil || projection.Mutation.CurrentState != nil || projection.Mutation.Gates != nil || projection.Correspondence != nil || len(projection.GateBindings) != 0 || string(projection.Record.Fields) != `{}` || !projection.Record.CreatedAt.Equal(at) || !projection.Record.UpdatedAt.Equal(updated) {
				t.Fatalf("data-only projection gained execution or changed data clocks: %+v %v", projection, err)
			}
		})
	}
}
