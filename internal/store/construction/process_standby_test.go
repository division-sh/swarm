package construction_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/google/uuid"
)

func TestRuntimeConstructorWaitsBeforeBootstrapBothStores(t *testing.T) {
	spec, err := contracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	schema := store.SchemaBootstrapRequest{PlatformPlans: plans, Origin: store.RuntimeStoreOrigin{
		SwarmVersion: "process-wait-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			type runtimeStore interface {
				store.SchemaBootstrapper
				startupownership.Store
				Close() error
			}
			var open func(context.Context) (runtimeStore, *sql.DB, error)
			var request selected.RuntimeRequest
			query := `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`
			if backend == "sqlite" {
				path := filepath.Join(t.TempDir(), "runtime.db")
				request.Selection = backendselection.Selection{Backend: backendselection.BackendSQLite, SQLitePath: path}
				t.Run("appeared_before_native_construction", func(t *testing.T) {
					other := filepath.Join(t.TempDir(), "fresh.db")
					input, err := construction.CaptureSQLiteInspectionInput(t.Context(), other)
					if err != nil || !input.Absent() {
						t.Fatalf("noncreating inspection: %v", err)
					}
					body := []byte("uninspected replacement")
					if err := os.WriteFile(other, body, 0600); err != nil {
						t.Fatal(err)
					}
					if owner, _, err := construction.OpenSQLiteRuntimeForServing(t.Context(), other, input); err == nil {
						_ = owner.Close()
						t.Fatal("native exclusion consumed newly appeared state")
					}
					if actual, err := os.ReadFile(other); err != nil || string(actual) != string(body) {
						t.Fatalf("refusal touched database bytes: %v", err)
					}
				})
				open = func(ctx context.Context) (runtimeStore, *sql.DB, error) {
					owner, db, err := construction.OpenSQLiteRuntimeForServing(ctx, path)
					if err != nil {
						return nil, nil, err
					}
					return owner, db, nil
				}
			} else {
				dsn, _, _ := testutil.StartEmptyPostgres(t)
				request.Selection.Backend, request.PostgresDSN = backendselection.BackendPostgres, dsn
				open = func(ctx context.Context) (runtimeStore, *sql.DB, error) {
					owner, db, err := construction.OpenPostgresRuntime(ctx, dsn)
					if err != nil {
						return nil, nil, err
					}
					return owner, db, nil
				}
				query = `SELECT COUNT(*) FROM pg_tables WHERE schemaname='public'`
			}
			holder, db, err := open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Close()
			inspection, err := selected.OpenAdmissionInspection(t.Context(), selected.AuthorityRequest{Selection: request.Selection, PostgresDSN: request.PostgresDSN})
			if err != nil {
				t.Fatal(err)
			}
			_, err = inspection.Inspect(t.Context(), schema, func(snapshot *selected.AdmissionSnapshot) error {
				var err error
				request.BootCheckInput, err = snapshot.CaptureBootInput(t.Context())
				return err
			})
			if err := errors.Join(err, inspection.Close()); err != nil {
				t.Fatal(err)
			}
			type result struct {
				owner runtimeStore
				err   error
			}
			returned := make(chan result, 1)
			waitCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				owner, _, err := open(waitCtx)
				returned <- result{owner, err}
			}()
			select {
			case got := <-returned:
				if got.owner != nil {
					_ = got.owner.Close()
				}
				t.Fatalf("contender did not wait: %v", got.err)
			case <-time.After(30 * time.Millisecond):
			}
			var tables int
			if err := db.QueryRowContext(context.Background(), query).Scan(&tables); err != nil || tables != 0 {
				t.Fatalf("standby bootstrapped an unowned store: tables=%d err=%v", tables, err)
			}
			cancel()
			if err := holder.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-returned:
				if got.owner != nil || !errors.Is(got.err, context.Canceled) {
					t.Fatalf("cancelled contender published a store: %+v", got)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("construction waiter did not join")
			}
			checked, err := selected.OpenRuntime(t.Context(), request)
			if err != nil {
				t.Fatalf("unchanged inspected state refused under native exclusion: %v", err)
			}
			if err := checked.CloseUnactivated(); err != nil {
				t.Fatal(err)
			}
			owner, _, err := open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Close()
			if err := owner.BootstrapSchema(context.Background(), schema); err != nil {
				t.Fatal(err)
			}
			capability, err := owner.AcquireProcessCapability(context.Background(), startupownership.AcquireRequest{
				OwnerID: "construction-adoption", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := capability.ProveCurrent(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := capability.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			if stale, err := selected.OpenRuntime(t.Context(), request); err == nil {
				_ = stale.CloseUnactivated()
				t.Fatal("changed schema/generation reused the pre-bootstrap inspection")
			}
		})
	}
}
