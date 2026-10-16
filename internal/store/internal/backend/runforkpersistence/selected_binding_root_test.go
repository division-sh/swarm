package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/sourceartifact"
	artifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestS1SelectedBindingFreezesItsChildsExactRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := selectedBindingRootDatabase(t, backend)
			ctx := context.Background()
			a, pathA := sourceartifactfixture.Rooted(t, "target-a", sourceartifactfixture.Artifact())
			b, pathB := sourceartifactfixture.Rooted(t, "target-b", sourceartifactfixture.Artifact())
			unretained, _ := sourceartifactfixture.Rooted(t, "unretained", sourceartifactfixture.Artifact())
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			for _, source := range []sourceartifact.RootedSource{a, b} {
				if backend == "postgres" {
					_, err = artifactstore.EnsurePostgresRootedSourceTx(ctx, tx, source, time.Now())
				} else {
					_, err = artifactstore.EnsureSQLiteRootedSourceTx(ctx, tx, source, time.Now())
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, path := range []string{pathA, pathB} {
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
			for _, cell := range []struct {
				name      string
				root      sourceartifact.RootAdmission
				wantError bool
			}{
				{"exact", a.Admission(), false},
				{"same-content-foreign-root", b.Admission(), true},
				{"missing", sourceartifact.RootAdmission{}, true},
				{"not-retained", unretained.Admission(), true},
			} {
				t.Run(cell.name, func(t *testing.T) {
					request := runfork.RunForkSelectedContractBindingRequest{
						ForkRunID: uuid.NewString(), SourceRunID: uuid.NewString(),
						ForkPoint:           runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7},
						ContractSelection:   runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
						TargetRootAdmission: cell.root,
					}
					// Reduced binding-writer fixture: the native run writer and
					// full materialization journeys are proved separately.
					if _, err := tx.ExecContext(ctx, `INSERT INTO runs (run_id, bundle_hash, source_root_directory) VALUES ($1, $2, $3)`,
						request.ForkRunID, a.Admission().BundleHash(), a.Admission().RootDirectory()); err != nil {
						t.Fatal(err)
					}
					binding, err := insertRunForkSelectedContractBinding(ctx, tx, request, time.Now())
					if cell.wantError {
						if err == nil || binding.BindingID != "" {
							t.Errorf("unbound target gained selected binding authority: %+v, %v", binding, err)
						}
						if _, err := loadRunForkSelectedContractBinding(ctx, tx, request.ForkRunID); !errors.Is(err, sql.ErrNoRows) {
							t.Errorf("refused root left selected binding evidence: %v", err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					loaded, err := loadRunForkSelectedContractBinding(ctx, tx, request.ForkRunID)
					if err != nil || binding.TargetRootReference != a.Admission().Reference() || loaded.TargetRootReference != binding.TargetRootReference {
						t.Errorf("binding lost its frozen target root after tree removal: inserted=%+v read=%+v err=%v", binding, loaded, err)
					}
				})
			}
		})
	}
}

func selectedBindingRootDatabase(t *testing.T, backend string) *sql.DB {
	t.Helper()
	var db *sql.DB
	blobType, timestamp := "BLOB", "TEXT DEFAULT CURRENT_TIMESTAMP"
	if backend == "postgres" {
		_, opened, cleanup := testutil.StartEmptyPostgres(t)
		db = opened
		t.Cleanup(cleanup)
		blobType, timestamp = "BYTEA", "TIMESTAMPTZ DEFAULT NOW()"
	} else {
		opened, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		db = opened
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = db.Close() })
	}
	for _, ddl := range []string{
		`CREATE TABLE source_artifacts (bundle_hash TEXT PRIMARY KEY, source_blob ` + blobType + `, member_count BIGINT, total_bytes BIGINT, created_at ` + timestamp + `)`,
		`CREATE TABLE source_root_admissions (bundle_hash TEXT, root_directory TEXT, PRIMARY KEY (bundle_hash, root_directory))`,
		`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT, source_root_directory TEXT)`,
		`CREATE TABLE run_fork_selected_contract_bindings (
			binding_id TEXT DEFAULT '00000000-0000-0000-0000-000000000001', fork_run_id TEXT,
			source_run_id TEXT, fork_point_kind TEXT, fork_revision BIGINT, fork_event_id TEXT,
			mode TEXT, bundle_hash TEXT, target_bundle_hash TEXT, source_root_directory TEXT, created_at ` + timestamp + `)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
