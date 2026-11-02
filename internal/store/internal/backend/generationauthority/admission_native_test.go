package generationauthority_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
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

type generationNativeFixture struct {
	db       *sql.DB
	mutate   func(context.Context, func(context.Context, *sql.Tx) error) error
	read     func(context.Context, func(context.Context, *sql.Tx) error) error
	acquire  func(context.Context, func(context.Context, *sql.Tx) error) (bool, error)
	queued   func(context.Context) (bool, error)
	identity processbinding.LeaseIdentity
}

func nativeGenerationAdmissionFixture(t *testing.T, sqlite bool) generationNativeFixture {
	t.Helper()
	ctx := context.Background()
	var fixture generationNativeFixture
	var bootstrap func(context.Context, schemastore.SchemaBootstrapRequest) error
	if sqlite {
		owner, db, err := construction.OpenSQLiteRuntimeForServing(ctx, filepath.Join(t.TempDir(), "admission.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		})
		backend, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		fixture.db, bootstrap = db, owner.BootstrapSchema
		fixture.mutate = func(ctx context.Context, write func(context.Context, *sql.Tx) error) error {
			return backend.RunTransaction(ctx, "generation admission proof", write)
		}
		fixture.read, fixture.acquire = backend.RunReadTransaction, backend.RunProcessAcquisitionOutcome
		fixture.queued = func(context.Context) (bool, error) { return backend.QueuedWritersForTest() != 0, nil }
		fixture.identity.Backend = processbinding.SQLiteLeaseBackend
	} else {
		_, db, _ := testutil.StartEmptyPostgres(t)
		backend, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewPostgres(backend)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := postgresbackend.AcquireBlockingAdvisoryLockLease(ctx, backend, "generation-admission-construction")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := lease.Release(context.Background()); err != nil {
				t.Error(err)
			}
		})
		fixture.db, bootstrap = db, schema.BootstrapSchema
		fixture.mutate, fixture.read, fixture.acquire = backend.RunTransaction, backend.RunReadTransaction, lease.RunProcessAcquisitionOutcome
		fixture.queued = func(ctx context.Context) (bool, error) {
			var waiting bool
			err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks
				WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
				AND classid=1398231629 AND objid=1 AND objsubid=2 AND NOT granted)`).Scan(&waiting)
			return waiting, err
		}
		fixture.identity.Backend = processbinding.PostgresLeaseBackend
	}
	spec, err := contracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap(ctx, schemastore.SchemaBootstrapRequest{PlatformPlans: plans, Origin: schemastore.RuntimeStoreOrigin{
		SwarmVersion: "generation-admission-proof", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}); err != nil {
		t.Fatal(err)
	}
	fixture.identity.LeaseID, fixture.identity.OwnerID = uuid.NewString(), "generation-admission-owner"
	fixture.identity.BootID, fixture.identity.RuntimeInstanceID = uuid.NewString(), uuid.NewString()
	return fixture
}

// Invariant/reach: native mutation admission holds off process advancement until
// physical settlement, then refuses predecessor work before its callback.
func TestNativeGenerationAdmissionSerializesAdvancementBothStores(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		t.Run(map[bool]string{true: "sqlite", false: "postgres"}[sqlite], func(t *testing.T) {
			fixture := nativeGenerationAdmissionFixture(t, sqlite)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var original processbinding.LeaseStamp
			ack, err := fixture.acquire(ctx, func(txctx context.Context, tx *sql.Tx) error {
				var err error
				original, err = generationauthority.AdvanceProcessGeneration(txctx, tx, fixture.identity, sqlite)
				return err
			})
			if err != nil || !ack {
				t.Fatalf("initial acquisition: acknowledged=%v err=%v", ack, err)
			}
			originalCtx, err := generationauthority.MutationContext(ctx, original, false)
			if err != nil {
				t.Fatal(err)
			}
			entered, release, settled := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			settledDone := make(chan struct{})
			var advancedDone chan struct{}
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer func() {
				unblock()
				cancel()
				<-settledDone
				if advancedDone != nil {
					<-advancedDone
				}
			}()
			go func() {
				defer close(settledDone)
				settled <- fixture.mutate(originalCtx, func(txctx context.Context, tx *sql.Tx) error {
					if current, err := generationauthority.ProcessGenerationCurrent(txctx, tx, original, sqlite); err != nil || !current {
						return fmt.Errorf("admitted original: current=%v err=%w", current, err)
					}
					if current, err := generationauthority.ReadProcessGeneration(txctx, tx, sqlite); err != nil || current != original {
						return fmt.Errorf("shared admitted read: got=%+v err=%w", current, err)
					}
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					_, err := tx.ExecContext(txctx, `UPDATE runtime_store_metadata SET swarm_version='admitted-before-advance' WHERE id=1`)
					return err
				})
			}()
			select {
			case <-entered:
			case err := <-settled:
				t.Fatalf("original did not enter: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if !sqlite {
				if err := fixture.mutate(originalCtx, func(context.Context, *sql.Tx) error { return nil }); err != nil {
					t.Fatalf("shared ordinary admission did not overlap: %v", err)
				}
			}
			type advanceResult struct {
				stamp processbinding.LeaseStamp
				err   error
			}
			advanced := make(chan advanceResult, 1)
			advancedDone = make(chan struct{})
			nextIdentity := fixture.identity
			nextIdentity.LeaseID, nextIdentity.BootID = uuid.NewString(), uuid.NewString()
			go func() {
				defer close(advancedDone)
				var next processbinding.LeaseStamp
				ack, err := fixture.acquire(ctx, func(txctx context.Context, tx *sql.Tx) error {
					var err error
					next, err = generationauthority.AdvanceProcessGeneration(txctx, tx, nextIdentity, sqlite)
					return err
				})
				if !ack && err == nil {
					err = errors.New("advance has no acknowledged commit")
				}
				advanced <- advanceResult{stamp: next, err: err}
			}()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				waiting, err := fixture.queued(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case result := <-advanced:
					t.Fatalf("advance passed unsettled admission: %+v", result)
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			unblock()
			if err := <-settled; err != nil {
				t.Fatal(err)
			}
			next := <-advanced
			if next.err != nil || next.stamp.Generation != original.Generation+1 {
				t.Fatalf("advance after settlement: %+v", next)
			}
			var version string
			if err := fixture.db.QueryRowContext(ctx, `SELECT swarm_version FROM runtime_store_metadata WHERE id=1`).Scan(&version); err != nil || version != "admitted-before-advance" {
				t.Fatalf("physical predecessor commit: version=%q err=%v", version, err)
			}
			for _, staleCtx := range []context.Context{ctx, originalCtx} {
				called := false
				err := fixture.mutate(staleCtx, func(context.Context, *sql.Tx) error { called = true; return nil })
				if err == nil || called {
					t.Fatalf("missing/predecessor attribution reached domain: called=%v err=%v", called, err)
				}
			}
			freshCtx, err := generationauthority.MutationContext(ctx, next.stamp, false)
			if err != nil {
				t.Fatal(err)
			}
			var endedCtx context.Context
			var endedTx *sql.Tx
			if err := fixture.mutate(freshCtx, func(txctx context.Context, tx *sql.Tx) error {
				endedCtx, endedTx = txctx, tx
				_, err := tx.ExecContext(txctx, `UPDATE runtime_store_metadata SET swarm_version='successor-write' WHERE id=1`)
				return err
			}); err != nil {
				t.Fatalf("successor admission: %v", err)
			}
			if err := fixture.db.QueryRowContext(ctx, `SELECT swarm_version FROM runtime_store_metadata WHERE id=1`).Scan(&version); err != nil || version != "successor-write" {
				t.Fatalf("physical successor commit: version=%q err=%v", version, err)
			}
			if err := generationauthority.FenceMutation(endedCtx, endedTx, sqlite); !errors.Is(err, sql.ErrTxDone) {
				t.Fatalf("settled transaction retained admission: %v", err)
			}
			if _, err := generationauthority.ReadProcessGeneration(endedCtx, endedTx, sqlite); !errors.Is(err, sql.ErrTxDone) {
				t.Fatalf("settled transaction retained admitted read: %v", err)
			}
			if err := fixture.read(freshCtx, func(txctx context.Context, tx *sql.Tx) error {
				if err := generationauthority.FenceMutation(txctx, tx, sqlite); err == nil {
					return errors.New("read-only transaction minted mutation admission")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
