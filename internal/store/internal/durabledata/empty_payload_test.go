package durabledata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

func TestSourceEvaluationReadsPresentEmptyPayloadBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			compiled := pinnedTestVersion(t, 0, "")
			owner, _, _ := pinnedTestOwner(t, backend, compiled)
			head := runtimedata.VersionHead(compiled.VersionID)
			err := owner.runReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
				base, err := owner.loadSourceEvaluationBase(ctx, tx, runtimedata.HeadResult{Before: head, After: head, Revision: 1}, compiled.Manifest.Declaration)
				if err != nil {
					return err
				}
				if base.CanonicalJSONL == nil || len(base.CanonicalJSONL) != 0 || base.Manifest.RowCount != 0 {
					t.Fatalf("present empty base: %#v", base)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEmptyPayloadReadersRefuseMissingPrunedAndContradictoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			compiled := pinnedTestVersion(t, 0, "")
			wrongManifest := compiled.Manifest
			wrongManifest.Declaration.EventName = "other"
			wrongManifestJSON, err := json.Marshal(wrongManifest)
			if err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct {
				name   string
				query  string
				args   []any
				listed string
			}{
				{"missing", `DELETE FROM resource_versions`, nil, ""},
				{"sql_null", `UPDATE resource_versions SET canonical_jsonl = NULL`, nil, "has no payload or tombstone"},
				{"pruned", `UPDATE resource_versions SET canonical_jsonl = NULL, pruned_at = $1`, []any{time.Now().UTC()}, ""},
				{"tombstone_with_empty_payload", `UPDATE resource_versions SET pruned_at = $1`, []any{time.Now().UTC()}, "retains payload after prune"},
				{"schema_digest", `UPDATE resource_versions SET schema_digest = 'wrong'`, nil, ""},
				{"schema_bytes", `UPDATE resource_versions SET canonical_schema_bytes = $1`, []any{[]byte(`{"type":"object"}`)}, "immutable evidence is contradictory"},
				{"manifest_declaration", `UPDATE resource_versions SET manifest_json = $1`, []any{wrongManifestJSON}, "immutable evidence is contradictory"},
				{"payload_digest", `UPDATE resource_versions SET canonical_jsonl = $1`, []any{[]byte("{\"value\":\"unexpected\"}\n")}, "payload failed immutable verification"},
			} {
				t.Run(test.name, func(t *testing.T) {
					owner, db, _ := pinnedTestOwner(t, backend, compiled)
					if _, err := db.Exec(test.query, test.args...); err != nil {
						t.Fatal(err)
					}
					head := runtimedata.VersionHead(compiled.VersionID)
					err := owner.runReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
						_, err := owner.loadSourceEvaluationBase(ctx, tx, runtimedata.HeadResult{Before: head, After: head, Revision: 1}, compiled.Manifest.Declaration)
						return err
					})
					var domainErr *runtimedata.DomainError
					if !errors.As(err, &domainErr) || domainErr.Code != runtimedata.CodeIntegrity {
						t.Fatalf("invalid source base admitted: %v", err)
					}
					if test.listed != "" {
						err := owner.runReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
							_, err := owner.loadVersions(ctx, tx, runtimedata.Declaration{Ref: compiled.Manifest.Declaration})
							return err
						})
						if err == nil || !strings.Contains(err.Error(), test.listed) {
							t.Fatalf("version list error = %v, want %q", err, test.listed)
						}
					}
					if test.name == "sql_null" || test.name == "tombstone_with_empty_payload" {
						err := owner.runTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
							_, err := owner.persistVersion(ctx, tx, compiled, runtimedata.Declaration{CanonicalSchema: compiled.CanonicalSchema}, runtimedata.SourceCommand{}, time.Now().UTC())
							return err
						})
						if !errors.As(err, &domainErr) || domainErr.Code != runtimedata.CodeIntegrity {
							t.Fatalf("invalid existing version reused: %v", err)
						}
					}
				})
			}
		})
	}
}
