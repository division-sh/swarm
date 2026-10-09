package runforkpersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestQ10EmptyImportHistoricalMembershipAndClockProjection(t *testing.T) {
	at := time.Unix(100, 0).UTC()
	updated := at.Add(time.Minute)
	reversed := at.Add(-time.Second)
	for _, change := range []string{"exact", "missing_creation_clock", "missing_update_clock", "reversed_clocks", "lifecycle_history", "gate_history", "duplicate_birth"} {
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
			case "duplicate_birth":
				snapshot.EntityMetadata = append(snapshot.EntityMetadata, fact)
			}
			entities, err := loadRunForkEntityStates(snapshot)
			if err == nil {
				var admission runForkMaterializedEntitySnapshotMetadataAdmission
				entities, admission, err = attachRunForkMaterializedEntitySnapshotMetadata(snapshot, entities)
				if len(admission.Blockers) != 0 {
					if change == "exact" {
						t.Fatalf("genuine empty import refused: %+v", admission.Blockers)
					}
					return
				}
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
