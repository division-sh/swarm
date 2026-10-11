package selected

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestAdmissionPossessionDoesNotMaskCorruptProcessGenerationBothStores(t *testing.T) {
	spec, err := contracts.LoadPlatformSpecDocument(filepath.Join(selectedStoreRepoRoot(t), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	schema := store.SchemaBootstrapRequest{PlatformPlans: plans, Origin: store.RuntimeStoreOrigin{
		SwarmVersion: "possession-admission-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}
	for _, backend := range []backendselection.Backend{backendselection.BackendSQLite, backendselection.BackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			request := AuthorityRequest{Selection: backendselection.Selection{Backend: backend}}
			var owner interface {
				store.SchemaBootstrapper
				startupownership.Store
				Close() error
			}
			var db *sql.DB
			if backend == backendselection.BackendSQLite {
				request.Selection.SQLitePath = filepath.Join(t.TempDir(), "store.db")
				selected, handle, err := construction.OpenSQLiteRuntimeForServing(ctx, request.Selection.SQLitePath)
				if err != nil {
					t.Fatal(err)
				}
				owner, db = selected, handle
			} else {
				request.PostgresDSN, _, _ = testutil.StartEmptyPostgres(t)
				selected, handle, err := construction.OpenPostgres(request.PostgresDSN)
				if err != nil {
					t.Fatal(err)
				}
				owner, db = selected, handle
			}
			defer func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := owner.BootstrapSchema(ctx, schema); err != nil {
				t.Fatal(err)
			}
			capability, err := owner.AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "possession-test", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()})
			if err != nil {
				t.Fatal(err)
			}
			if err := capability.Release(ctx); err != nil {
				t.Fatal(err)
			}
			query := "UPDATE runtime_process_generation SET owner_id=''"
			if _, err := db.ExecContext(ctx, query); err != nil {
				t.Fatal(err)
			}
			var before int
			if err := db.QueryRowContext(ctx, "SELECT generation FROM runtime_process_generation").Scan(&before); err != nil {
				t.Fatal(err)
			}
			inspection, err := OpenAdmissionInspection(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := inspection.Close(); err != nil {
					t.Error(err)
				}
			}()
			possession, err := inspection.ProbePossession(ctx)
			if err != nil || !possession.Available {
				t.Fatalf("free kernel observation: %+v %v", possession, err)
			}
			_, err = inspection.Inspect(ctx, schema, func(snapshot *AdmissionSnapshot) error {
				_, err := snapshot.InspectAuthority(ctx)
				if err == nil || !strings.Contains(err.Error(), "process lease owner_id must be nonempty and canonical") {
					t.Fatalf("free kernel state masked corrupt process stamp: %v", err)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			var after int
			if err := db.QueryRowContext(ctx, "SELECT generation FROM runtime_process_generation").Scan(&after); err != nil || after != before {
				t.Fatalf("observation altered lineage: %d -> %d %v", before, after, err)
			}
			if _, err := owner.AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "must-refuse", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}); err == nil || !strings.Contains(err.Error(), "process lease owner_id must be nonempty and canonical") {
				t.Fatalf("boot accepted or misclassified corrupt process stamp after a free observation: %v", err)
			}
		})
	}
}
