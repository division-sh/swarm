package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
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

func TestDurableDataFixturesCommitReplayAndRefuseBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db := newDurableDataFixtureStore(t, backend)
			ctx := testAuthorActivityContext()
			catalog := durableDataFixtureCatalog(t)
			collector, restore, err := InstallTransactionProbeForTest(store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if err := RegisterDurableDataCatalogForTest(ctx, store, catalog); err != nil {
				t.Fatal(err)
			}
			if got := collector.Snapshot(); got.Active != 0 || got.Total.WriteCommits != 1 {
				t.Fatalf("catalog did not settle through exact selected coordinator: %#v", got)
			}
			shape, err := store.GetDeclarationImportShape(ctx, catalog.BundleHash, catalog.Declarations[0].Ref)
			if err != nil || !reflect.DeepEqual(shape, catalog.ImportShapes[0]) {
				t.Fatalf("catalog readback = %#v, %v", shape, err)
			}
			if err := RegisterDurableDataCatalogForTest(ctx, store, catalog); err != nil {
				t.Fatalf("exact catalog replay: %v", err)
			}
			imported, err := store.ExecuteDataSourceOperation(ctx, runtimedata.SourceCommand{
				Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "fixture", BundleHash: catalog.BundleHash,
				Declaration: catalog.Declarations[0].Ref, ExpectedHead: runtimedata.AbsentHead(),
				InputFormat: "jsonl", Input: []byte("{\"label\":\"exact\"}\n"),
			})
			if err != nil || imported.Outcome != "accepted" {
				t.Fatalf("source import = %#v, %v", imported, err)
			}
			source, target := uuid.NewString(), uuid.NewString()
			for _, runID := range []string{source, target} {
				requireRunFixtureForTest(t, ctx, store, semanticRunFixture{RunID: runID, Origin: semanticScenarioSetupRunOriginForTest()})
			}
			overrides := []runtimedata.ExplicitPin{{Declaration: catalog.Declarations[0].Ref, VersionID: imported.Candidate.VersionID}}
			before := collector.Snapshot().Total.WriteCommits
			pins, err := MaterializeDataForkPinsForTest(ctx, store, source, target, catalog.BundleHash, overrides, false)
			if err != nil || len(pins) != 1 || pins[0].Selection != "fork_override" || pins[0].VersionID != imported.Candidate.VersionID {
				t.Fatalf("materialize = %#v, %v", pins, err)
			}
			if got := collector.Snapshot(); got.Active != 0 || got.Total.WriteCommits != before+1 {
				t.Fatalf("pins did not settle through exact selected coordinator: %#v", got)
			}
			replayed, err := MaterializeDataForkPinsForTest(ctx, store, source, target, catalog.BundleHash, overrides, true)
			// Readback reports the current run state, not the materializer's planned paused state.
			pins[0].RunState = "running"
			if err != nil || !reflect.DeepEqual(replayed, pins) {
				t.Fatalf("exact pin replay = %#v, %v; want %#v", replayed, err, pins)
			}
			loaded, err := store.LoadDataPins(ctx, imported.Candidate.VersionID)
			if err != nil || !reflect.DeepEqual(loaded, replayed) {
				t.Fatalf("pin readback = %#v, %v; want %#v", loaded, err, replayed)
			}
			before = collector.Snapshot().Total.WriteCommits
			refused, err := MaterializeDataForkPinsForTest(ctx, store, source, target, catalog.BundleHash, nil, true)
			if err == nil || refused != nil {
				t.Fatalf("changed replay leaked pins: %#v, %v", refused, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := RegisterDurableDataCatalogForTest(cancelled, store, catalog); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled catalog: %v", err)
			}
			if pins, err := MaterializeDataForkPinsForTest(cancelled, store, source, target, catalog.BundleHash, overrides, true); !errors.Is(err, context.Canceled) || pins != nil {
				t.Fatalf("cancelled pins: %#v, %v", pins, err)
			}
			if got := collector.Snapshot(); got.Active != 0 || got.Total.WriteCommits != before {
				t.Fatalf("refusal committed or retained work: %#v", got)
			}
			after, err := store.LoadDataPins(ctx, imported.Candidate.VersionID)
			if err != nil || !reflect.DeepEqual(after, loaded) {
				t.Fatalf("refusal changed pins: %#v, %v", after, err)
			}
			failedTarget := uuid.NewString()
			requireRunFixtureForTest(t, ctx, store, semanticRunFixture{RunID: failedTarget, Origin: semanticScenarioSetupRunOriginForTest()})
			if backend == "sqlite" {
				if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_fixture_pin BEFORE INSERT ON resource_version_pins
					BEGIN SELECT RAISE(ABORT, 'fixture pin fault'); END`); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.ExecContext(ctx, `CREATE FUNCTION reject_fixture_pin() RETURNS trigger LANGUAGE plpgsql AS
				$$ BEGIN RAISE EXCEPTION 'fixture pin fault'; END $$;
				CREATE TRIGGER reject_fixture_pin BEFORE INSERT ON resource_version_pins
				FOR EACH ROW EXECUTE FUNCTION reject_fixture_pin()`); err != nil {
				t.Fatal(err)
			}
			before = collector.Snapshot().Total.WriteCommits
			if pins, err := MaterializeDataForkPinsForTest(ctx, store, source, failedTarget, catalog.BundleHash, overrides, false); err == nil || pins != nil {
				t.Fatalf("failed pin insert returned uncommitted pins: %#v, %v", pins, err)
			}
			if got := collector.Snapshot(); got.Active != 0 || got.Total.WriteCommits != before {
				t.Fatalf("failed pin insert committed or retained work: %#v", got)
			}
			after, err = store.LoadDataPins(ctx, imported.Candidate.VersionID)
			if err != nil || !reflect.DeepEqual(after, loaded) {
				t.Fatalf("failed pin insert changed global pins: %#v, %v", after, err)
			}
			if backend == "sqlite" {
				if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_fixture_pin`); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_fixture_pin ON resource_version_pins; DROP FUNCTION reject_fixture_pin()`); err != nil {
				t.Fatal(err)
			}
			if pins, err := MaterializeDataForkPinsForTest(ctx, store, source, failedTarget, catalog.BundleHash, overrides, false); err != nil || len(pins) != 1 {
				t.Fatalf("failed fixture poisoned subsequent pin operation: %#v, %v", pins, err)
			}
			if got := db.Stats().InUse; got != 0 {
				t.Fatalf("fixture retained %d connections", got)
			}
		})
	}
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
