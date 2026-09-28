package durabledata

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil"
	_ "modernc.org/sqlite"
)

func TestImportShapeDetailIsExactImmutableAndIntegrityChecked(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, db := importShapeTestOwner(t, backend)
			ctx := context.Background()
			bundleHash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			ref := runtimedata.DeclarationRef{FlowPath: ".", EventName: "candidate.created"}
			schema, err := canonicaljson.Bytes(map[string]any{
				"type": "object", "additionalProperties": false,
				"properties": map[string]any{
					"id":     map[string]any{"type": "string"},
					"resume": map[string]any{"type": "string"},
					"score":  map[string]any{"type": "integer"},
				},
				"required":            []string{"id", "resume", "score"},
				"x-swarm-dataset-key": "id",
			})
			if err != nil {
				t.Fatal(err)
			}
			digest := runtimedata.SchemaDigestFor(schema)
			catalog := runtimedata.Catalog{BundleHash: bundleHash, Declarations: []runtimedata.Declaration{{
				Name: ref.EventName, Ref: ref, BusinessKey: "id", SchemaDigest: digest, CanonicalSchema: schema,
			}}}
			shape := runtimedata.ImportShape{BundleHash: bundleHash, Declaration: ref, SchemaDigest: digest, BusinessKey: "id", Fields: []runtimedata.ImportShapeField{
				{Name: "id", Required: true, Text: true}, {Name: "resume", Required: true, Text: true}, {Name: "score", Required: true},
			}}
			shapes := runtimedata.ImportShapeCatalog{BundleHash: bundleHash, Shapes: []runtimedata.ImportShape{shape}}
			if _, err := db.Exec(`INSERT INTO source_artifacts (bundle_hash) VALUES ($1)`, bundleHash); err != nil {
				t.Fatal(err)
			}
			unshaped, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := RegisterCatalogTx(owner, ctx, unshaped, catalog, time.Now()); err == nil || !strings.Contains(err.Error(), "import shapes must cover") {
				t.Fatalf("unshaped nonempty catalog = %v, want strict refusal", err)
			}
			_ = unshaped.Rollback()
			var identities int
			if err := db.QueryRow(`SELECT COUNT(*) FROM resource_declarations`).Scan(&identities); err != nil || identities != 0 {
				t.Fatalf("unshaped registration persisted %d identities: %v", identities, err)
			}
			register := func(values runtimedata.ImportShapeCatalog) error {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer func() { _ = tx.Rollback() }()
				if err := RegisterCatalogWithImportShapesTx(owner, ctx, tx, catalog, values, time.Now()); err != nil {
					return err
				}
				return tx.Commit()
			}
			if err := register(shapes); err != nil {
				t.Fatal(err)
			}
			if err := register(shapes); err != nil {
				t.Fatalf("identical catalog refused: %v", err)
			}
			got, err := owner.GetDeclarationImportShape(ctx, bundleHash, ref)
			if err != nil || !reflect.DeepEqual(got, shape) {
				t.Fatalf("detail = %#v, %v", got, err)
			}
			conflict := shapes
			conflict.Shapes = append([]runtimedata.ImportShape(nil), shapes.Shapes...)
			conflict.Shapes[0] = shape
			conflict.Shapes[0].Fields = append([]runtimedata.ImportShapeField(nil), shape.Fields...)
			conflict.Shapes[0].Fields[1].Text = false
			if err := register(conflict); err == nil || !strings.Contains(err.Error(), "conflicts") {
				t.Fatalf("conflicting typed fact = %v", err)
			}
			tampered := shape
			tampered.Fields = append([]runtimedata.ImportShapeField(nil), shape.Fields...)
			tampered.Fields[2].Text = true
			tamperedBytes, err := canonicaljson.Bytes(tampered)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE resource_bundle_import_shapes SET shape_json=$1 WHERE bundle_hash=$2`, tamperedBytes, bundleHash); err != nil {
				t.Fatal(err)
			}
			var domain *runtimedata.DomainError
			if _, err := owner.GetDeclarationImportShape(ctx, bundleHash, ref); !errors.As(err, &domain) || domain.Code != runtimedata.CodeIntegrity {
				t.Fatalf("tampered text eligibility = %v, want integrity", err)
			}
			if _, err := db.Exec(`UPDATE resource_bundle_import_shapes SET shape_json=$1 WHERE bundle_hash=$2`, []byte(`{"bad":true}`), bundleHash); err != nil {
				t.Fatal(err)
			}
			domain = nil
			if _, err := owner.GetDeclarationImportShape(ctx, bundleHash, ref); !errors.As(err, &domain) || domain.Code != runtimedata.CodeIntegrity {
				t.Fatalf("corrupt detail = %v, want integrity", err)
			}
			if _, err := db.Exec(`DELETE FROM resource_bundle_import_shapes WHERE bundle_hash=$1`, bundleHash); err != nil {
				t.Fatal(err)
			}
			domain = nil
			if _, err := owner.GetDeclarationImportShape(ctx, bundleHash, ref); !errors.As(err, &domain) || domain.Code != runtimedata.CodeIntegrity {
				t.Fatalf("missing detail = %v, want integrity", err)
			}
			domain = nil
			if err := register(shapes); !errors.As(err, &domain) || domain.Code != runtimedata.CodeIntegrity {
				t.Fatalf("re-registration backfilled missing immutable shape: %v", err)
			}
			var shapeRows int
			if err := db.QueryRow(`SELECT COUNT(*) FROM resource_bundle_import_shapes WHERE bundle_hash=$1`, bundleHash).Scan(&shapeRows); err != nil || shapeRows != 0 {
				t.Fatalf("refused backfill left %d shape rows: %v", shapeRows, err)
			}
		})
	}
}

func importShapeTestOwner(t *testing.T, backend string) (*Owner, *sql.DB) {
	t.Helper()
	var db *sql.DB
	var owner *Owner
	var err error
	if backend == "postgres" {
		_, db, _ = testutil.StartEmptyPostgres(t)
		store, createErr := postgresbackend.New(db)
		if createErr != nil {
			t.Fatal(createErr)
		}
		owner, err = NewPostgres(store, func() error { return nil })
	} else {
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "import-shape.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		store, createErr := sqlitebackend.New(db)
		if createErr != nil {
			t.Fatal(createErr)
		}
		owner, err = NewSQLite(store, func() error { return nil }, time.Now)
	}
	if err != nil {
		t.Fatal(err)
	}
	blob := "BLOB"
	if backend == "postgres" {
		blob = "BYTEA"
	}
	for _, statement := range []string{
		`CREATE TABLE source_artifacts (bundle_hash TEXT PRIMARY KEY)`,
		`CREATE TABLE resource_declarations (flow_path TEXT, event_name TEXT, admitted_at TIMESTAMP, PRIMARY KEY (flow_path, event_name))`,
		`CREATE TABLE resource_heads (flow_path TEXT, event_name TEXT, version_id TEXT, revision INTEGER, updated_at TIMESTAMP)`,
		`CREATE TABLE resource_bundle_declarations (bundle_hash TEXT, flow_path TEXT, event_name TEXT, display_name TEXT, owner_flow_id TEXT, business_key_field TEXT, schema_digest TEXT, canonical_schema_bytes ` + blob + `, admitted_at TIMESTAMP, PRIMARY KEY (bundle_hash, flow_path, event_name))`,
		`CREATE TABLE resource_bundle_import_shapes (bundle_hash TEXT, flow_path TEXT, event_name TEXT, schema_digest TEXT, shape_digest TEXT, shape_json ` + blob + `, admitted_at TIMESTAMP, PRIMARY KEY (bundle_hash, flow_path, event_name))`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	return owner, db
}
