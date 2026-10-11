package generationauthority_test

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestNativeProcessGenerationTransactionsBothStores(t *testing.T) {
	spec, err := contracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	request := schemastore.SchemaBootstrapRequest{PlatformPlans: plans, Origin: schemastore.RuntimeStoreOrigin{
		SwarmVersion: "process-generation-native", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}
	for _, sqlite := range []bool{true, false} {
		t.Run(map[bool]string{true: "sqlite", false: "postgres"}[sqlite], func(t *testing.T) {
			ctx := context.Background()
			var db *sql.DB
			var transact func(context.Context, func(context.Context, *sql.Tx) error) error
			var bootstrap func(context.Context, schemastore.SchemaBootstrapRequest) error
			backendName := processbinding.SQLiteLeaseBackend
			if sqlite {
				owner, handle, err := construction.OpenSQLiteRuntimeForServing(ctx, filepath.Join(t.TempDir(), "process.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := owner.Close(); err != nil {
						t.Error(err)
					}
				})
				db, bootstrap = handle, owner.BootstrapSchema
				backend, err := sqlitebackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				transact = func(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
					return backend.RunTransaction(ctx, "native process-generation proof", operation)
				}
			} else {
				_, handle, _ := testutil.StartEmptyPostgres(t)
				db, backendName = handle, processbinding.PostgresLeaseBackend
				backend, err := postgresbackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				lease, err := postgresbackend.AcquireBlockingAdvisoryLockLease(ctx, backend, "swarm:runtime:shared-store-owner")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := lease.Release(context.Background()); err != nil {
						t.Error(err)
					}
				})
				schema, err := schemastore.NewPostgres(backend)
				if err != nil {
					t.Fatal(err)
				}
				if err := schema.BindProcessLease(lease); err != nil {
					t.Fatal(err)
				}
				bootstrap, transact = schema.BootstrapSchema, lease.RunTransaction
			}
			if err := bootstrap(ctx, request); err != nil {
				t.Fatal(err)
			}
			read := func(t *testing.T, want processbinding.LeaseStamp) {
				t.Helper()
				got, err := generationauthority.ReadProcessGeneration(ctx, db, sqlite)
				if err != nil || got != want {
					t.Fatalf("current singleton = %+v, want %+v; err=%v", got, want, err)
				}
			}
			identity := processbinding.LeaseIdentity{LeaseID: uuid.NewString(), OwnerID: "native-owner", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: backendName}
			var current processbinding.LeaseStamp
			t.Run("fresh_seed", func(t *testing.T) { read(t, current) })
			t.Run("first_advance", func(t *testing.T) {
				err := transact(ctx, func(txctx context.Context, tx *sql.Tx) error {
					var err error
					current, err = generationauthority.AdvanceProcessGeneration(txctx, tx, identity, sqlite)
					return err
				})
				if err != nil || current.Generation != 1 || current.LeaseIdentity != identity {
					t.Fatalf("first advance: %+v err=%v", current, err)
				}
				read(t, current)
			})
			t.Run("exact_replay", func(t *testing.T) {
				err := transact(ctx, func(txctx context.Context, tx *sql.Tx) error {
					got, err := generationauthority.AdvanceProcessGeneration(txctx, tx, identity, sqlite)
					if err == nil && got != current {
						t.Fatal("exact replay changed the complete stamp")
					}
					return err
				})
				if err != nil {
					t.Fatal(err)
				}
				read(t, current)
			})
			t.Run("conflicting_replay", func(t *testing.T) {
				conflict := identity
				conflict.OwnerID = "another-owner"
				err := transact(ctx, func(txctx context.Context, tx *sql.Tx) error {
					_, err := generationauthority.AdvanceProcessGeneration(txctx, tx, conflict, sqlite)
					return err
				})
				if err == nil || !strings.Contains(err.Error(), "replay conflicts") {
					t.Fatalf("conflicting replay: %v", err)
				}
				read(t, current)
			})
			previous := current
			t.Run("successor_and_full_stamp_fence", func(t *testing.T) {
				identity.LeaseID, identity.BootID = uuid.NewString(), uuid.NewString()
				err := transact(ctx, func(txctx context.Context, tx *sql.Tx) error {
					var err error
					current, err = generationauthority.AdvanceProcessGeneration(txctx, tx, identity, sqlite)
					if err != nil {
						return err
					}
					crossed := current
					crossed.RuntimeInstanceID = uuid.NewString()
					for _, pair := range []struct {
						stamp processbinding.LeaseStamp
						want  bool
					}{{current, true}, {previous, false}, {crossed, false}} {
						got, err := generationauthority.ProcessGenerationCurrent(txctx, tx, pair.stamp, sqlite)
						if err != nil || got != pair.want {
							t.Fatalf("complete-stamp fence: got=%t want=%t err=%v", got, pair.want, err)
						}
					}
					return nil
				})
				if err != nil || current.Generation != 2 || current.LeaseIdentity != identity {
					t.Fatalf("successor: %+v err=%v", current, err)
				}
				read(t, current)
			})
			t.Run("rollback", func(t *testing.T) {
				refusal := errors.New("injected before commit")
				next := identity
				next.LeaseID = uuid.NewString()
				err := transact(ctx, func(txctx context.Context, tx *sql.Tx) error {
					if _, err := generationauthority.AdvanceProcessGeneration(txctx, tx, next, sqlite); err != nil {
						return err
					}
					return refusal
				})
				if !errors.Is(err, refusal) {
					t.Fatalf("rollback lost its cause: %v", err)
				}
				read(t, current)
			})
			for _, before := range []bool{true, false} {
				t.Run(map[bool]string{true: "cancel_before", false: "cancel_after_row_before_publication"}[before], func(t *testing.T) {
					cancelCtx, cancel := context.WithCancel(ctx)
					defer cancel()
					if before {
						cancel()
					}
					called := false
					err := transact(cancelCtx, func(txctx context.Context, tx *sql.Tx) error {
						called = true
						next := identity
						next.LeaseID = uuid.NewString()
						if _, err := generationauthority.AdvanceProcessGeneration(txctx, tx, next, sqlite); err != nil {
							return err
						}
						cancel()
						return nil
					})
					if !errors.Is(err, context.Canceled) || called == before {
						t.Fatalf("cancelled transaction: called=%t before=%t err=%v", called, before, err)
					}
					read(t, current)
				})
			}
			t.Run("exhausted", func(t *testing.T) {
				if _, err := db.ExecContext(ctx, `UPDATE runtime_process_generation SET generation=$1 WHERE singleton_id=1`, int64(math.MaxInt64)); err != nil {
					t.Fatal(err)
				}
				current.Generation = math.MaxInt64
				err := transact(ctx, func(txctx context.Context, tx *sql.Tx) error {
					next := identity
					next.LeaseID = uuid.NewString()
					_, err := generationauthority.AdvanceProcessGeneration(txctx, tx, next, sqlite)
					return err
				})
				if err == nil || !strings.Contains(err.Error(), "generation is exhausted") {
					t.Fatalf("exhausted generation: %v", err)
				}
				read(t, current)
			})
			t.Run("initialized_missing_never_reseeds", func(t *testing.T) {
				if _, err := db.ExecContext(ctx, `DELETE FROM runtime_process_generation`); err != nil {
					t.Fatal(err)
				}
				if err := bootstrap(ctx, request); err != nil {
					t.Fatal(err)
				}
				if _, err := generationauthority.ReadProcessGeneration(ctx, db, sqlite); !errors.Is(err, generationauthority.ErrProcessGenerationMissing) {
					t.Fatalf("initialized missing row was reseeded/hidden: %v", err)
				}
			})
		})
	}
}
