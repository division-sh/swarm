package pipelinepersistence

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

// These exercise the commit fence on both SQL dialects. Real publication and
// lifecycle races are separate integration obligations, not inferred here.
func TestA2FirstPublicationRevisionFenceOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := fanOutReadbackTestDB(t, backend)
			idType := "TEXT"
			if backend == "postgres" {
				idType = "UUID"
			}
			if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE entity_state (run_id %s, entity_id %s, flow_instance TEXT, revision BIGINT, PRIMARY KEY (run_id, entity_id))`, idType, idType)); err != nil {
				t.Fatal(err)
			}
			run, entity := uuid.NewString(), uuid.NewString()
			owner, err := flowidentity.NewRunScopedFlowInstance(run, flowidentity.StoredRoute("collector", "one", "collector/one"))
			if err != nil {
				t.Fatal(err)
			}
			fence := pipeline.WorkflowJoinAdmissionFence{Owner: owner, EntityID: entity}
			check := func(expected int64, valid bool) {
				t.Helper()
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				fence.Revision = expected
				err = requireWorkflowJoinAdmissionTx(context.Background(), tx, []pipeline.WorkflowJoinAdmissionFence{fence}, backend == "postgres")
				if valid {
					if err != nil {
						t.Fatalf("exact admission rejected: %v", err)
					}
				} else {
					envelope, typed := failures.As(err)
					if !typed || envelope.Failure.Class != failures.ClassLifecycleConflict || envelope.Failure.Detail.Code != "join_publication_entry_changed" || engine.FailureDispositionFor(err) != engine.FailureDispositionRetry {
						t.Fatalf("stale admission was not a typed precommit retry: %v", err)
					}
				}
			}
			check(0, true)
			if _, err := db.Exec(`INSERT INTO entity_state VALUES ($1,$2,$3,1)`, run, entity, owner.Route.InstancePath); err != nil {
				t.Fatal(err)
			}
			check(0, false)
			check(1, true)
			if _, err := db.Exec(`UPDATE entity_state SET revision=2 WHERE run_id=$1 AND entity_id=$2`, run, entity); err != nil {
				t.Fatal(err)
			}
			check(1, false)
			check(2, true)
			// The containing business commit may create/advance the receiver before
			// admission; the fence must consume that transaction's authoritative row.
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.Exec(`UPDATE entity_state SET revision=3 WHERE run_id=$1 AND entity_id=$2`, run, entity); err != nil {
				t.Fatal(err)
			}
			fence.Revision = 3
			if err := requireWorkflowJoinAdmissionTx(context.Background(), tx, []pipeline.WorkflowJoinAdmissionFence{fence}, backend == "postgres"); err != nil {
				t.Fatalf("same-commit entry rejected: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			check(2, true)
		})
	}
}
