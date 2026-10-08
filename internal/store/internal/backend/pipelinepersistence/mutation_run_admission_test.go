package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

// These replace raw-transaction fixture controls with the actual selected writer.
// Observe before rollback so an accidental write cannot be hidden by cleanup.
func missingMutationRunProof(t *testing.T, initial bool) {
	t.Helper()
	db := fanOutReadbackTestDB(t, "sqlite")
	for _, ddl := range []string{
		`CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT NOT NULL, bundle_hash TEXT)`,
		`CREATE TABLE entity_mutations (mutation_id TEXT PRIMARY KEY)`,
		`CREATE TABLE author_activity_order (singleton_id INTEGER PRIMARY KEY, last_sequence INTEGER NOT NULL)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	store := &PipelineSQLiteOwner{RunLifecycleSQLiteOwner: &runlifecycle.RunLifecycleSQLiteOwner{}}
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
	result := mutationprotocol.RunSQLite(ctx, backend, "missing mutation run", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		before := runtimemutationlog.EntityStateProjection{}
		after := runtimemutationlog.EntityStateProjection{Fields: map[string]any{"status": "ready"}}
		entityID := uuid.NewString()
		var writeErr error
		if initial {
			_, writeErr = commitWorkflowEngineInitialValues(ctx, attempt, store, false, runtimepipeline.WorkflowEngineStateRecord{
				EntityID: entityID, Transition: runtimepipeline.WorkflowEngineStateTransitionCreateStateAndCompanion,
				InitialFields: []byte(`{"region":"west"}`), UpdatedAt: time.Now().UTC(),
			}, before)
		} else {
			writeErr = insertWorkflowEngineStateDiff(ctx, attempt, store, false, entityID, before, after, runtimemutationlog.Writer{Type: "platform", ID: "hostile-proof", HandlerStep: "diff"}, time.Now().UTC())
		}
		if !errors.Is(writeErr, runtimerunlifecycle.ErrRunNotFound) {
			t.Fatalf("writer error = %v, want ErrRunNotFound", writeErr)
		}
		if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			for _, table := range []string{"runs", "entity_mutations"} {
				var count int
				if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
					return err
				}
				if count != 0 {
					t.Fatalf("%s contains %d rows before refusal rollback", table, count)
				}
			}
			return nil
		}); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, writeErr
	})
	if result.Acknowledged() || !errors.Is(result.Err(), runtimerunlifecycle.ErrRunNotFound) {
		t.Fatalf("missing-run mutation settled unexpectedly: ack=%v err=%v", result.Acknowledged(), result.Err())
	}
}

func TestSQLiteEntityStateDiffRequiresExistingCanonicalRunBeforeMutation(t *testing.T) {
	missingMutationRunProof(t, false)
}

func TestSQLiteInitialValueMutationRequiresExistingCanonicalRunBeforeMutation(t *testing.T) {
	missingMutationRunProof(t, true)
}
