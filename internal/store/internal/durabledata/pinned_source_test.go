package durabledata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil"
	_ "modernc.org/sqlite"
)

const pinnedTestBundle = "test-bundle"
const pinnedTestRun = "test-run"

func pinnedTestVersion(t *testing.T, rows int, value string) runtimedata.CompiledVersion {
	t.Helper()
	ref, err := runtimedata.ParseDeclarationRef(".", "items")
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{"value": map[string]any{"type": "string"}},
		"required":   []string{"value"},
	}
	compiled, defects := runtimedata.CompileJSONL(ref, schema, "", []byte(strings.Repeat("{\"value\":\""+value+"\"}\n", rows)))
	if len(defects) != 0 {
		t.Fatalf("compile admitted version: %+v", defects)
	}
	return compiled
}

func pinnedTestOwner(t *testing.T, backend string, compiled runtimedata.CompiledVersion) (*Owner, *sql.DB, *sqlitebackend.Backend) {
	t.Helper()
	var db *sql.DB
	var sqlite *sqlitebackend.Backend
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
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "pinned.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		db.SetMaxOpenConns(4)
		sqlite, err = sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		owner, err = NewSQLite(sqlite, func() error { return nil }, time.Now)
	}
	if err != nil {
		t.Fatal(err)
	}
	blob := "BLOB"
	if backend == "postgres" {
		blob = "BYTEA"
	}
	for _, statement := range []string{
		`CREATE TABLE resource_declarations (flow_path TEXT, event_name TEXT)`,
		`CREATE TABLE resource_bundle_declarations (bundle_hash TEXT, flow_path TEXT, event_name TEXT, schema_digest TEXT, canonical_schema_bytes ` + blob + `, business_key_field TEXT)`,
		`CREATE TABLE resource_version_pins (run_id TEXT, flow_path TEXT, event_name TEXT, schema_digest TEXT, version_id TEXT)`,
		`CREATE TABLE resource_versions (version_id TEXT, flow_path TEXT, event_name TEXT, sequence_alias INTEGER, schema_digest TEXT, canonical_schema_bytes ` + blob + `, manifest_json ` + blob + `, row_count INTEGER, business_key_field TEXT, content_digest TEXT, row_codec TEXT, canonical_jsonl ` + blob + `, pruned_at TIMESTAMP)`,
		`CREATE TABLE resource_version_provenance (version_id TEXT)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	ref := compiled.Manifest.Declaration
	manifest, err := json.Marshal(compiled.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO resource_declarations VALUES ($1,$2)`, []any{ref.FlowPath, ref.EventName}},
		{`INSERT INTO resource_bundle_declarations VALUES ($1,$2,$3,$4,$5,$6)`, []any{pinnedTestBundle, ref.FlowPath, ref.EventName, compiled.Manifest.SchemaDigest, compiled.CanonicalSchema, ""}},
		{`INSERT INTO resource_version_pins VALUES ($1,$2,$3,$4,$5)`, []any{pinnedTestRun, ref.FlowPath, ref.EventName, compiled.Manifest.SchemaDigest, compiled.VersionID}},
		{`INSERT INTO resource_versions VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULL)`, []any{compiled.VersionID, ref.FlowPath, ref.EventName, 1, compiled.Manifest.SchemaDigest, compiled.CanonicalSchema, manifest, len(compiled.Rows), "", compiled.Manifest.ContentDigest, compiled.Manifest.RowCodec, compiled.CanonicalJSONL}},
		{`INSERT INTO resource_version_provenance VALUES ($1)`, []any{compiled.VersionID}},
	} {
		if _, err := db.Exec(statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	return owner, db, sqlite
}

func TestExpandedCanonicalPayloadIsReadableAndIntegrityCheckedBothStores(t *testing.T) {
	const rows = 8
	compiled := pinnedTestVersion(t, rows, strings.Repeat("<", 16_000))
	if len(compiled.CanonicalJSONL) <= runtimedata.MaxDecodedImportBytes {
		t.Fatalf("canonical bytes %d did not exceed import-wire limit %d", len(compiled.CanonicalJSONL), runtimedata.MaxDecodedImportBytes)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, db, _ := pinnedTestOwner(t, backend, compiled)
			ctx := context.Background()
			ref := compiled.Manifest.Declaration
			pinned, err := owner.LoadPinnedSource(ctx, pinnedTestRun, pinnedTestBundle, ref)
			if err != nil || pinned.VersionID != compiled.VersionID || pinned.RowCount != rows {
				t.Fatalf("load exact source = %+v, %v", pinned, err)
			}
			items, err := owner.LoadPinnedRowsRange(ctx, pinnedTestRun, pinnedTestBundle, pinned, 2, 4)
			if err != nil || len(items) != 2 {
				t.Fatalf("load canonical row range = %+v, %v", items, err)
			}
			_, payload, err := owner.ResolveVersionPayload(ctx, ref, runtimedata.VersionSelector{Kind: "version", VersionID: compiled.VersionID})
			if err != nil || len(payload.CanonicalJSONL) != len(compiled.CanonicalJSONL) {
				t.Fatalf("resolve canonical payload bytes = %d, %v", len(payload.CanonicalJSONL), err)
			}
			altered := append([]byte(nil), compiled.CanonicalJSONL...)
			altered[0] = 'x'
			if _, err := db.Exec(`UPDATE resource_versions SET canonical_jsonl=$1 WHERE version_id=$2`, altered, compiled.VersionID); err != nil {
				t.Fatal(err)
			}
			var integrity *runtimedata.DomainError
			if _, err := owner.LoadPinnedRowsRange(ctx, pinnedTestRun, pinnedTestBundle, pinned, 2, 4); !errors.As(err, &integrity) || integrity.Code != runtimedata.CodeIntegrity {
				t.Fatalf("corrupt pinned payload = %v, want integrity refusal", err)
			}
			integrity = nil
			if _, _, err := owner.ResolveVersionPayload(ctx, ref, runtimedata.VersionSelector{Kind: "version", VersionID: compiled.VersionID}); !errors.As(err, &integrity) || integrity.Code != runtimedata.CodeIntegrity {
				t.Fatalf("corrupt resolved payload = %v, want integrity refusal", err)
			}
			noncanonical := bytes.Replace(compiled.CanonicalJSONL, []byte(`\u003c`), []byte(`<`), 1)
			if bytes.Equal(noncanonical, compiled.CanonicalJSONL) {
				t.Fatal("noncanonical payload probe did not change stored bytes")
			}
			if _, err := db.Exec(`UPDATE resource_versions SET canonical_jsonl=$1 WHERE version_id=$2`, noncanonical, compiled.VersionID); err != nil {
				t.Fatal(err)
			}
			integrity = nil
			if _, err := owner.LoadPinnedRowsRange(ctx, pinnedTestRun, pinnedTestBundle, pinned, 2, 4); !errors.As(err, &integrity) || integrity.Code != runtimedata.CodeIntegrity {
				t.Fatalf("noncanonical pinned payload = %v, want integrity refusal", err)
			}
			integrity = nil
			if _, _, err := owner.ResolveVersionPayload(ctx, ref, runtimedata.VersionSelector{Kind: "version", VersionID: compiled.VersionID}); !errors.As(err, &integrity) || integrity.Code != runtimedata.CodeIntegrity {
				t.Fatalf("noncanonical resolved payload = %v, want integrity refusal", err)
			}
		})
	}
}

func TestPinnedImmutableReadsBypassSQLiteMutationAdmission(t *testing.T) {
	compiled := pinnedTestVersion(t, 1, "one")
	owner, _, backend := pinnedTestOwner(t, "sqlite", compiled)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- backend.RunTransaction(ctx, "held mutation admission", func(context.Context, *sql.Tx) error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatalf("mutation admission did not enter: %v", err)
	case <-ctx.Done():
		t.Fatalf("mutation admission did not enter: %v", ctx.Err())
	}
	defer func() {
		close(release)
		if err := <-finished; err != nil {
			t.Errorf("release held mutation: %v", err)
		}
	}()
	ref := compiled.Manifest.Declaration
	pinned, err := owner.LoadPinnedSource(ctx, pinnedTestRun, pinnedTestBundle, ref)
	if err != nil {
		t.Fatalf("read pinned metadata while mutation admission held: %v", err)
	}
	items, err := owner.LoadPinnedRowsRange(ctx, pinnedTestRun, pinnedTestBundle, pinned, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("read pinned rows while mutation admission held: %+v, %v", items, err)
	}
	if _, _, err := owner.ResolveVersionPayload(ctx, ref, runtimedata.VersionSelector{Kind: "version", VersionID: compiled.VersionID}); err != nil {
		t.Fatalf("resolve version payload while mutation admission held: %v", err)
	}
}
