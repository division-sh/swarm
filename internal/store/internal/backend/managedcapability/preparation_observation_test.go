package managedcapabilitystore_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	managedcapabilitystore "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	_ "modernc.org/sqlite"
)

func TestRunlessPreparationStoragePreservesEveryPhysicalPredicate(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "preflight-scope.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE managed_agent_capability_surfaces(surface_id TEXT, authority_kind TEXT, execution_kind TEXT, execution_authority_id TEXT, run_id TEXT, actor_id TEXT, surface BLOB)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO managed_agent_capability_surfaces VALUES
		('valid','startup_probe','selected_fork_preparation','preparation',NULL,'actor','exact bytes'),
		('wrong-authority','provider_turn','selected_fork_preparation','preparation',NULL,'actor','foreign'),
		('wrong-kind','startup_probe','selected_contract_fork','preparation',NULL,'actor','foreign'),
		('wrong-preparation','startup_probe','selected_fork_preparation','foreign',NULL,'actor','foreign'),
		('with-run','startup_probe','selected_fork_preparation','preparation','run','actor','foreign'),
		('empty-run','startup_probe','selected_fork_preparation','preparation','','actor','foreign'),
		('wrong-actor','startup_probe','selected_fork_preparation','preparation',NULL,'foreign','foreign')`); err != nil {
		t.Fatal(err)
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		if raw, err := managedcapabilitystore.ReadRunlessSelectedPreparationSurfaceStorageTx(ctx, tx, "valid", "preparation", "actor"); err != nil || string(raw) != "exact bytes" {
			t.Fatalf("physical surface bytes changed: %q/%v", raw, err)
		}
		for _, id := range []string{"wrong-authority", "wrong-kind", "wrong-preparation", "with-run", "empty-run", "wrong-actor", "absent"} {
			if raw, err := managedcapabilitystore.ReadRunlessSelectedPreparationSurfaceStorageTx(ctx, tx, id, "preparation", "actor"); !errors.Is(err, sql.ErrNoRows) || raw != nil {
				t.Fatalf("foreign physical surface passed exact predicate: %s/%q/%v", id, raw, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPreflightGlobalPhysicalCountsDoNotFilterLifecycleState(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "preflight-counts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"runs", "agents", "run_fork_selected_contract_runtime_executions", "runtime_generation_grants"} {
		if _, err := db.Exec("CREATE TABLE " + table + "(state TEXT)"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("INSERT INTO " + table + " VALUES('active'),('closed'),('foreign')"); err != nil {
			t.Fatal(err)
		}
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		for _, read := range []func(context.Context, *sql.Tx) (int, error){
			runlifecycle.CountGlobalRunStorageTx, agentpersistence.CountGlobalAgentStorageTx,
			runforkpersistence.CountGlobalSelectedRuntimeExecutionStorageTx, generationauthority.CountGlobalGenerationGrantStorageTx,
		} {
			if count, err := read(ctx, tx); err != nil || count != 3 {
				t.Fatalf("global physical count narrowed lifecycle state: %d/%v", count, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
