package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type durableDataFixtureStore interface {
	sourceartifactfixture.Writer
	GetDeclarationImportShape(context.Context, string, runtimedata.DeclarationRef) (runtimedata.ImportShape, error)
	ExecuteDataSourceOperation(context.Context, runtimedata.SourceCommand) (runtimedata.SourceOperationResult, error)
	LoadDataPins(context.Context, runtimedata.VersionID) ([]runtimedata.Pin, error)
}

func durableDataFixtureCatalog(t *testing.T) runtimedata.Catalog {
	t.Helper()
	ref := runtimedata.DeclarationRef{FlowPath: ".", EventName: "fixture.created"}
	schema, err := canonicaljson.Bytes(map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{"label": map[string]any{"type": "string"}},
		"required":   []string{"label"},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := runtimedata.SchemaDigestFor(schema)
	return runtimedata.Catalog{
		BundleHash: authorActivityTestBundleHash,
		Declarations: []runtimedata.Declaration{{
			Ref: ref, Name: ref.EventName, SchemaDigest: digest, CanonicalSchema: schema,
		}},
		ImportShapes: []runtimedata.ImportShape{{
			BundleHash: authorActivityTestBundleHash, Declaration: ref, SchemaDigest: digest,
			Fields: []runtimedata.ImportShapeField{{Name: "label", Required: true, Text: true}},
		}},
	}
}

func newDurableDataFixtureStore(t *testing.T, backend string) (durableDataFixtureStore, *sql.DB) {
	t.Helper()
	if backend == "sqlite" {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		sourceartifactfixture.Require(t, testAuthorActivityContext(), store)
		return store, store.backend.ConstructionHandle()
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := newTestPostgresStore(t, db)
	sourceartifactfixture.Require(t, testAuthorActivityContext(), store)
	return store, db
}

func TestDurableDataCatalogFixtureRollsBackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db := newDurableDataFixtureStore(t, backend)
			ctx := testAuthorActivityContext()
			if backend == "sqlite" {
				if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_fixture_shape BEFORE INSERT ON resource_bundle_import_shapes
					BEGIN SELECT RAISE(ABORT, 'fixture shape fault'); END`); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := db.ExecContext(ctx, `CREATE FUNCTION reject_fixture_shape() RETURNS trigger LANGUAGE plpgsql AS
					$$ BEGIN RAISE EXCEPTION 'fixture shape fault'; END $$;
					CREATE TRIGGER reject_fixture_shape BEFORE INSERT ON resource_bundle_import_shapes
					FOR EACH ROW EXECUTE FUNCTION reject_fixture_shape()`); err != nil {
					t.Fatal(err)
				}
			}
			catalog := durableDataFixtureCatalog(t)
			if err := RegisterDurableDataCatalogForTest(ctx, store, catalog); err == nil {
				t.Fatal("catalog acknowledged a failed shape insert")
			}
			for _, query := range []string{
				`SELECT COUNT(*) FROM resource_declarations`,
				`SELECT COUNT(*) FROM resource_bundle_declarations`,
				`SELECT COUNT(*) FROM resource_bundle_import_shapes`,
			} {
				var count int
				if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil || count != 0 {
					t.Fatalf("failed catalog left %d rows: %v (%s)", count, err, query)
				}
			}
			if backend == "sqlite" {
				if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_fixture_shape`); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_fixture_shape ON resource_bundle_import_shapes; DROP FUNCTION reject_fixture_shape()`); err != nil {
				t.Fatal(err)
			}
			if err := RegisterDurableDataCatalogForTest(ctx, store, catalog); err != nil {
				t.Fatalf("failed fixture poisoned subsequent selected operation: %v", err)
			}
			if got := db.Stats().InUse; got != 0 {
				t.Fatalf("failed fixture retained %d connections", got)
			}
		})
	}
}

func TestDurableDataFixturesRejectMissingSelectedOwner(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, struct{}{}} {
		if err := RegisterDurableDataCatalogForTest(context.Background(), selected, runtimedata.Catalog{}); err == nil {
			t.Errorf("catalog accepted missing selected owner %T", selected)
		}
		if pins, err := MaterializeDataForkPinsForTest(context.Background(), selected, "", "", "", nil, false); err == nil || pins != nil {
			t.Errorf("pins accepted missing selected owner %T: %#v, %v", selected, pins, err)
		}
	}
}

// The second Done evaluation is after the occupied mutation token queued the
// operation. The gate exposes that precise cut without replacing admission.
type durableFixtureAdmissionCut struct {
	context.Context
	calls   atomic.Int32
	entered chan struct{}
	resume  chan struct{}
}

func (c *durableFixtureAdmissionCut) Done() <-chan struct{} {
	if c.calls.Add(1) == 2 {
		close(c.entered)
		<-c.resume
	}
	return c.Context.Done()
}

func TestDurableDataFixturesUseExactSQLiteCoordinator(t *testing.T) {
	for _, name := range []string{"catalog", "fork-pins", "reconstructed-coordinator-control"} {
		t.Run(name, func(t *testing.T) {
			store := newBootstrappedSQLiteRuntimeStoreForTest(t)
			db := store.backend.ConstructionHandle()
			db.SetMaxOpenConns(4)
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
			defer cancel()
			catalog := durableDataFixtureCatalog(t)
			holderEntered, release := make(chan struct{}), make(chan struct{})
			holderDone := make(chan error, 1)
			go func() {
				holderDone <- store.backend.RunTransaction(ctx, "durable fixture holder", func(context.Context, *sql.Tx) error {
					close(holderEntered)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}()
			defer func() {
				close(release)
				if err := <-holderDone; err != nil {
					t.Errorf("holder failed: %v", err)
				}
			}()
			select {
			case <-holderEntered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			queued, cancelQueued := context.WithCancel(ctx)
			cut := &durableFixtureAdmissionCut{Context: queued, entered: make(chan struct{}), resume: make(chan struct{})}
			fixtureDone := make(chan error, 1)
			go func() {
				switch name {
				case "catalog":
					fixtureDone <- RegisterDurableDataCatalogForTest(cut, store, catalog)
				case "fork-pins":
					pins, err := MaterializeDataForkPinsForTest(cut, store, uuid.NewString(), uuid.NewString(), authorActivityTestBundleHash, nil, false)
					if pins != nil {
						t.Errorf("queued cancellation leaked pins: %#v", pins)
					}
					fixtureDone <- err
				default:
					independent, err := sqlitebackend.New(db)
					if err == nil {
						err = independent.RunTransaction(cut, "old coordinator control", func(context.Context, *sql.Tx) error { return nil })
					}
					fixtureDone <- err
				}
			}()
			joined := false
			var resumeOnce sync.Once
			resume := func() { resumeOnce.Do(func() { close(cut.resume) }) }
			defer func() {
				cancelQueued()
				resume()
				if !joined {
					<-fixtureDone
				}
			}()
			select {
			case <-cut.entered:
			case err := <-fixtureDone:
				joined = true
				t.Fatalf("fixture bypassed admission cut: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			want := 1
			if name == "reconstructed-coordinator-control" {
				want = 0
			}
			if got := store.backend.QueuedWritersForTest(); got != want {
				t.Errorf("exact selected queue = %d, want %d", got, want)
			}
			cancelQueued()
			resume()
			err := <-fixtureDone
			joined = true
			if !errors.Is(err, context.Canceled) || store.backend.QueuedWritersForTest() != 0 {
				t.Errorf("cancelled fixture did not leave selected queue: %v", err)
			}
			for _, query := range []string{`SELECT COUNT(*) FROM resource_bundle_declarations`, `SELECT COUNT(*) FROM resource_version_pins`} {
				var count int
				if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil || count != 0 {
					t.Errorf("cancelled fixture changed storage: %d, %v", count, err)
				}
			}
		})
	}
}
