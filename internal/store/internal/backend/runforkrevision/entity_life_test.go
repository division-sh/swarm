package runforkrevision

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEntityLifeReadReconstructsExactRetainedCuts(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "life.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE run_fork_fact_revisions (run_id TEXT, family TEXT, fact_key TEXT, revision INTEGER, fact TEXT, present BOOLEAN)`,
		`CREATE TABLE entity_mutations (run_id TEXT, entity_id TEXT, mutation_id TEXT)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	runID, entityID, otherRun, otherEntity := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	at := time.Unix(1700000000, 0).UTC()
	insert := func(run string, family Family, key string, revision int64, fact any) {
		t.Helper()
		raw, err := json.Marshal(fact)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO run_fork_fact_revisions VALUES ($1,$2,$3,$4,$5,true)`, run, family, key, revision, string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	mutation := func(run, entity string, revision int64, domain, path string, value any) {
		t.Helper()
		id := uuid.NewString()
		if _, err := db.Exec(`INSERT INTO entity_mutations VALUES ($1,$2,$3)`, run, entity, id); err != nil {
			t.Fatal(err)
		}
		insert(run, FamilyEntityMutations, id, revision, map[string]any{
			"mutation_id": id, "entity_id": entity, "domain": domain, "path": path, "new_value": value, "created_at": at,
		})
	}
	for revision, state := range []string{"active", "done", "active", "done"} {
		life := int64(revision/2 + 1)
		insert(runID, FamilyEntityMetadata, entityID, int64(revision+1), EntityMetadataFact{
			EntityID: entityID, FlowInstance: "worker/one", EntityType: "state", ConstructionKind: "constructed",
			Life: life, InstanceRevision: int64(revision + 7), CurrentState: state, StageDefined: true,
			CreatedAt: at, UpdatedAt: at.Add(time.Duration(revision) * time.Second),
		})
		mutation(runID, entityID, int64(revision+1), "lifecycle_state", "", state)
		mutation(runID, entityID, int64(revision+1), "authored_field", "note", state+"-life-"+string(rune('0'+life)))
	}
	mutation(runID, entityID, 1, "authored_field", "old_only", "retained")
	mutation(runID, entityID, 3, "authored_field", "old_only", nil)
	mutation(otherRun, entityID, 1, "authored_field", "foreign_run", true)
	mutation(runID, otherEntity, 1, "authored_field", "foreign_entity", true)
	for _, test := range []struct {
		life     int64
		first    bool
		revision int64
		state    string
		note     string
		old      bool
	}{
		{1, true, 7, "active", "active-life-1", true},
		{1, false, 8, "done", "done-life-1", true},
		{2, true, 9, "active", "active-life-2", false},
		{2, false, 10, "done", "done-life-2", false},
	} {
		metadata, projection, found, err := ReadEntityLife(context.Background(), db, runID, entityID, test.life, test.first)
		if err != nil || !found || metadata.InstanceRevision != test.revision || projection.CurrentState == nil || *projection.CurrentState != test.state || projection.Fields["note"] != test.note {
			t.Fatalf("life=%d first=%t metadata=%+v projection=%+v found=%t err=%v", test.life, test.first, metadata, projection, found, err)
		}
		_, old := projection.Fields["old_only"]
		if old != test.old || projection.Fields["foreign_run"] != nil || projection.Fields["foreign_entity"] != nil {
			t.Fatalf("life cut mixed fields: %+v", projection.Fields)
		}
	}
	if _, _, found, err := ReadEntityLife(context.Background(), db, runID, entityID, 3, false); err != nil || found {
		t.Fatalf("future life was invented: found=%t err=%v", found, err)
	}
	if _, _, _, err := ReadEntityLife(context.Background(), db, runID, entityID, 0, false); err == nil {
		t.Fatal("nonpositive life was accepted")
	}
	bad, _ := json.Marshal(EntityMetadataFact{EntityID: entityID, ConstructionKind: "constructed", Life: 1, InstanceRevision: 8, CurrentState: "foreign"})
	if _, err := db.Exec(`UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND family=$3 AND fact_key=$4 AND revision=2`, string(bad), runID, FamilyEntityMetadata, entityID); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := ReadEntityLife(context.Background(), db, runID, entityID, 1, false); err == nil {
		t.Fatal("metadata/mutation disagreement was accepted")
	}
}
