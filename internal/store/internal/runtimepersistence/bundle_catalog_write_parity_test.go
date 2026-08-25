package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/testutil"
)

type bundleCatalogWriteParityStore interface {
	UpsertBundleCatalog(context.Context, bundlecatalog.Upsert) (bundlecatalog.UpsertResult, error)
	LoadBundleCatalog(context.Context, string) (bundlecatalog.Detail, error)
}

type bundleCatalogWriteParityFixture struct {
	store    bundleCatalogWriteParityStore
	db       *sql.DB
	postgres bool
}

func TestBundleCatalogWriteParity(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) bundleCatalogWriteParityFixture
	}{
		{name: "postgres", open: func(t *testing.T) bundleCatalogWriteParityFixture {
			_, db, _ := testutil.StartPostgres(t)
			return bundleCatalogWriteParityFixture{store: admitTestPostgresStore(t, db), db: db, postgres: true}
		}},
		{name: "sqlite", open: func(t *testing.T) bundleCatalogWriteParityFixture {
			store := newBootstrappedSQLiteRuntimeStoreForTest(t)
			return bundleCatalogWriteParityFixture{store: store, db: store.backend.ConstructionHandle()}
		}},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			req := bundleCatalogWriteParityRequest("7")

			first, err := fixture.store.UpsertBundleCatalog(ctx, req)
			if err != nil {
				t.Fatalf("first upsert: %v", err)
			}
			if !first.Registered || first.Detail.BundleHash != req.BundleHash || !first.Detail.HasData || first.Detail.DataSizeBytes != int64(len(req.DataBlob)) || first.Detail.Metadata["source"] != "serve-ingest" {
				t.Fatalf("first upsert = %#v", first)
			}

			replay, err := fixture.store.UpsertBundleCatalog(ctx, req)
			if err != nil {
				t.Fatalf("idempotent replay: %v", err)
			}
			if replay.Registered || replay.Detail.BundleHash != req.BundleHash {
				t.Fatalf("replay = %#v, want existing row", replay)
			}

			conflicts := []struct {
				name   string
				mutate func(*bundlecatalog.Upsert)
			}{
				{name: "content", mutate: func(req *bundlecatalog.Upsert) { req.ContentYAML += "changed: true\n" }},
				{name: "parsed_json", mutate: func(req *bundlecatalog.Upsert) { req.ParsedJSON = map[string]any{"projection_version": "changed"} }},
				{name: "data_blob", mutate: func(req *bundlecatalog.Upsert) { req.DataBlob = []byte("changed") }},
				{name: "metadata", mutate: func(req *bundlecatalog.Upsert) { req.Metadata = map[string]any{"source": "bundle.register"} }},
			}
			for _, conflict := range conflicts {
				conflict := conflict
				t.Run("conflict_"+conflict.name, func(t *testing.T) {
					changed := req
					conflict.mutate(&changed)
					if _, err := fixture.store.UpsertBundleCatalog(ctx, changed); !errors.Is(err, bundlecatalog.ErrConflict) {
						t.Fatalf("upsert error = %v, want ErrConflict", err)
					}
					stored, err := fixture.store.LoadBundleCatalog(ctx, req.BundleHash)
					if err != nil {
						t.Fatalf("load original after conflict: %v", err)
					}
					if stored.ContentYAML != req.ContentYAML || stored.Metadata["source"] != "serve-ingest" || stored.DataSizeBytes != int64(len(req.DataBlob)) {
						t.Fatalf("conflict changed stored row: %#v", stored)
					}
				})
			}

			invalid := []struct {
				name   string
				mutate func(*bundlecatalog.Upsert)
				want   string
			}{
				{name: "hash", mutate: func(req *bundlecatalog.Upsert) { req.BundleHash = "not-canonical" }, want: "canonical bundle_hash"},
				{name: "content", mutate: func(req *bundlecatalog.Upsert) {
					req.BundleHash = bundleCatalogWriteParityHash("8")
					req.ContentYAML = ""
				}, want: "content_yaml"},
				{name: "parsed_json", mutate: func(req *bundlecatalog.Upsert) {
					req.BundleHash = bundleCatalogWriteParityHash("8")
					req.ParsedJSON = map[string]any{"invalid": math.NaN()}
				}, want: "parsed_json"},
				{name: "metadata", mutate: func(req *bundlecatalog.Upsert) {
					req.BundleHash = bundleCatalogWriteParityHash("8")
					req.Metadata = map[string]any{"invalid": math.NaN()}
				}, want: "metadata"},
			}
			for _, malformed := range invalid {
				malformed := malformed
				t.Run("malformed_"+malformed.name, func(t *testing.T) {
					changed := req
					malformed.mutate(&changed)
					if _, err := fixture.store.UpsertBundleCatalog(ctx, changed); err == nil || !strings.Contains(err.Error(), malformed.want) {
						t.Fatalf("upsert error = %v, want %q", err, malformed.want)
					}
				})
			}

			requireBundleCatalogInsertRollback(t, ctx, fixture)
		})
	}
}

func requireBundleCatalogInsertRollback(t *testing.T, ctx context.Context, fixture bundleCatalogWriteParityFixture) {
	t.Helper()
	hash := bundleCatalogWriteParityHash("9")
	var trigger string
	if fixture.postgres {
		trigger = `
			CREATE FUNCTION swarm_test_reject_bundle_insert() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				IF NEW.bundle_hash = '` + hash + `' THEN
					RAISE EXCEPTION 'forced bundle catalog rollback';
				END IF;
				RETURN NEW;
			END
			$$;
			CREATE TRIGGER swarm_test_reject_bundle_insert
			AFTER INSERT ON bundles
			FOR EACH ROW EXECUTE FUNCTION swarm_test_reject_bundle_insert()
		`
	} else {
		trigger = `
			CREATE TRIGGER swarm_test_reject_bundle_insert
			AFTER INSERT ON bundles
			WHEN NEW.bundle_hash = '` + hash + `'
			BEGIN
				SELECT RAISE(ABORT, 'forced bundle catalog rollback');
			END
		`
	}
	if _, err := fixture.db.ExecContext(ctx, trigger); err != nil {
		t.Fatalf("install rollback trigger: %v", err)
	}
	req := bundleCatalogWriteParityRequest("9")
	if _, err := fixture.store.UpsertBundleCatalog(ctx, req); err == nil || !strings.Contains(err.Error(), "forced bundle catalog rollback") {
		t.Fatalf("forced rollback error = %v", err)
	}
	if _, err := fixture.store.LoadBundleCatalog(ctx, hash); !errors.Is(err, bundlecatalog.ErrNotFound) {
		t.Fatalf("load after forced rollback error = %v, want ErrNotFound", err)
	}
}

func bundleCatalogWriteParityRequest(digit string) bundlecatalog.Upsert {
	return bundlecatalog.Upsert{
		BundleHash:  bundleCatalogWriteParityHash(digit),
		ContentYAML: "projection_version: swarm.bundle.catalog.v2\nfiles: []\n",
		ParsedJSON: map[string]any{
			"projection_version": "swarm.bundle.catalog.v2",
			"agents":             []any{},
		},
		DataBlob: []byte(`{"projection_version":"swarm.bundle.catalog.v2","entries":[]}`),
		Metadata: map[string]any{"source": "serve-ingest", "nested": map[string]any{"enabled": true}},
	}
}

func bundleCatalogWriteParityHash(digit string) string {
	return "bundle-v1:sha256:" + strings.Repeat(digit, 64)
}
