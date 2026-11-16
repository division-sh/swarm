package construction_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/construction"
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
			query := `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`
			if backend == "sqlite" {
				path := filepath.Join(t.TempDir(), "runtime.db")
				t.Run("checks_before_writable_open", func(t *testing.T) {
					refusal := errors.New("exact prewrite refusal")
					calls := 0
					checks := constructionChecksForTest{inspect: func(target bootverify.ConstructionTarget) error {
						calls++
						expected, err := bootverify.SQLiteConstructionTarget(path)
						if err != nil || target != expected {
							t.Fatalf("native target: %+v, %v", target, err)
						}
						if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("checks followed a writable database open: %v", err)
						}
						if _, err := os.Stat(path + ".possession"); err != nil {
							t.Fatalf("checks preceded native coordinate acquisition: %v", err)
						}
						return refusal
					}}
					owner, handle, err := construction.OpenSQLiteRuntimeWithChecks(t.Context(), path, checks)
					if owner != nil || handle != nil || !errors.Is(err, refusal) || calls != 1 {
						t.Fatalf("refusal changed/constructed/replayed: owner=%v handle=%v calls=%d err=%v", owner, handle, calls, err)
					}
					if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("refused checks created a database: %v", err)
					}
					original, _, err := construction.OpenSQLiteRuntimeForServing(t.Context(), path)
					if err != nil {
						t.Fatalf("refused stage retained its coordinate: %v", err)
					}
					if err := original.Close(); err != nil {
						t.Fatal(err)
					}
					for _, replaced := range []string{path, path + ".possession"} {
						t.Run(filepath.Base(replaced), func(t *testing.T) {
							input, err := construction.CaptureSQLiteInspectionInput(t.Context(), path)
							if err != nil {
								t.Fatal(err)
							}
							if err := os.Rename(replaced, replaced+".original"); err != nil {
								t.Fatal(err)
							}
							defer func() {
								if err := errors.Join(os.Remove(replaced), os.Rename(replaced+".original", replaced)); err != nil {
									t.Error(err)
								}
							}()
							if err := os.WriteFile(replaced, []byte("replacement"), 0600); err != nil {
								t.Fatal(err)
							}
							if owner, _, err := construction.OpenSQLiteRuntimeForServing(t.Context(), path, input); err == nil {
								_ = owner.Close()
								t.Fatal("native construction accepted a replaced observed identity")
							}
						})
					}
				})
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
				t.Run("checks_under_native_possession", func(t *testing.T) {
					refusal := errors.New("exact prewrite refusal")
					calls := 0
					checks := constructionChecksForTest{inspect: func(target bootverify.ConstructionTarget) error {
						calls++
						other, err := construction.OpenPostgresReadOnly(dsn)
						if err != nil {
							return err
						}
						observed, err := other.ProbePossession(t.Context())
						if closeErr := other.Close(); err != nil || closeErr != nil || observed.Available {
							t.Fatalf("checks did not retain native exclusion: %+v, %v, %v", observed, err, closeErr)
						}
						expected, err := construction.PostgresCheckTarget(dsn)
						if err != nil || target != expected {
							t.Fatalf("native target: %+v, %v", target, err)
						}
						return refusal
					}}
					owner, handle, err := construction.OpenPostgresRuntime(t.Context(), dsn, checks)
					if owner != nil || handle != nil || !errors.Is(err, refusal) || calls != 1 {
						t.Fatalf("refusal changed/constructed/replayed: owner=%v handle=%v calls=%d err=%v", owner, handle, calls, err)
					}
				})
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
		})
	}
}

type constructionChecksForTest struct {
	inspect func(bootverify.ConstructionTarget) error
}

func (c constructionChecksForTest) InspectConstruction(_ context.Context, target bootverify.ConstructionTarget) (bootverify.Report, error) {
	return bootverify.Report{}, c.inspect(target)
}
