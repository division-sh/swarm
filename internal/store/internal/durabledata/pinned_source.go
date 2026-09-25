package durabledata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

// LoadPinnedSource reads only immutable source metadata. Triggering a fan-out
// never loads the row collection into the handler's transaction.
func (o *Owner) LoadPinnedSource(ctx context.Context, runID, bundleHash string, ref runtimedata.DeclarationRef) (runtimedata.PinnedSource, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.PinnedSource{}, err
	}
	var source runtimedata.PinnedSource
	err := o.runTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		source, err = o.pinnedSourceTx(ctx, tx, runID, bundleHash, ref)
		return err
	})
	return source, err
}

// BindPinnedSourceTx validates the exact source in the owning handler
// transaction, before any fan-out intent or handler effect can commit.
func (o *Owner) BindPinnedSourceTx(ctx context.Context, tx *sql.Tx, runID, bundleHash string, ref runtimedata.DeclarationRef, versionID runtimedata.VersionID, cardinality int) error {
	if o == nil || tx == nil {
		return fmt.Errorf("durable data source transaction owner is required")
	}
	source, err := o.pinnedSourceTx(ctx, tx, runID, bundleHash, ref)
	if err != nil {
		return err
	}
	if source.VersionID != versionID || source.RowCount != cardinality {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource source %s version/cardinality disagrees with the exact run pin", ref.Key())
	}
	return nil
}

func (o *Owner) pinnedSourceTx(ctx context.Context, tx *sql.Tx, runID, bundleHash string, ref runtimedata.DeclarationRef) (runtimedata.PinnedSource, error) {
	if err := ref.Validate(); err != nil {
		return runtimedata.PinnedSource{}, err
	}
	if runID == "" || bundleHash == "" {
		return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "resource source requires exact run and bundle identities")
	}
	var bundleDigest runtimedata.SchemaDigest
	var bundleSchema []byte
	var bundleKey string
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT schema_digest, canonical_schema_bytes, COALESCE(business_key_field, '') FROM resource_bundle_declarations
		WHERE bundle_hash=%s AND flow_path=%s AND event_name=%s
	`, 3), bundleHash, ref.FlowPath, ref.EventName).Scan(&bundleDigest, &bundleSchema, &bundleKey)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "bundle has no exact resource declaration %s", ref.Key())
	}
	if err != nil {
		return runtimedata.PinnedSource{}, err
	}
	if runtimedata.SchemaDigestFor(bundleSchema) != bundleDigest {
		return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "bundle resource schema contradicts its digest for %s", ref.Key())
	}
	if _, err := schemaBusinessKey(bundleSchema, bundleKey); err != nil {
		return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "bundle resource row key contradicts its schema for %s", ref.Key())
	}
	var pinDigest runtimedata.SchemaDigest
	var versionID runtimedata.VersionID
	err = tx.QueryRowContext(ctx, o.query(`
		SELECT schema_digest, version_id FROM resource_version_pins
		WHERE run_id=%s AND flow_path=%s AND event_name=%s
	`, 3), runID, ref.FlowPath, ref.EventName).Scan(&pinDigest, &versionID)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "run %s has no exact pin for resource %s", runID, ref.Key())
	}
	if err != nil {
		return runtimedata.PinnedSource{}, err
	}
	var versionDigest runtimedata.SchemaDigest
	var contentDigest runtimedata.ContentDigest
	var versionSchema, manifestJSON []byte
	var versionKey, rowCodec string
	var rowCount int
	var retained bool
	err = tx.QueryRowContext(ctx, o.query(`
		SELECT schema_digest, canonical_schema_bytes, manifest_json, row_count,
		       COALESCE(business_key_field, ''), content_digest, row_codec,
		       (canonical_jsonl IS NOT NULL AND pruned_at IS NULL)
		FROM resource_versions
		WHERE version_id=%s AND flow_path=%s AND event_name=%s
	`, 3), versionID, ref.FlowPath, ref.EventName).Scan(&versionDigest, &versionSchema, &manifestJSON, &rowCount, &versionKey, &contentDigest, &rowCodec, &retained)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource pin references missing exact version %s", versionID)
	}
	if err != nil {
		return runtimedata.PinnedSource{}, err
	}
	if !retained {
		return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodePayloadPruned, "resource source version %s has no retained payload", versionID)
	}
	var manifest runtimedata.Manifest
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource source manifest is invalid: %v", err)
	}
	derived, err := manifest.VersionID()
	if err != nil || derived != versionID || manifest.Declaration != ref ||
		manifest.SchemaDigest != versionDigest || int(manifest.RowCount) != rowCount ||
		manifest.ContentDigest != contentDigest || manifest.RowCodec != rowCodec || bundleKey != versionKey ||
		versionDigest != pinDigest || versionDigest != bundleDigest ||
		runtimedata.SchemaDigestFor(versionSchema) != versionDigest ||
		!bytes.Equal(versionSchema, bundleSchema) {
		return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource source metadata disagrees with exact bundle, pin or version for %s", ref.Key())
	}
	return runtimedata.PinnedSource{Declaration: ref, VersionID: versionID, SchemaDigest: versionDigest, RowCount: rowCount}, nil
}

// LoadPinnedRowsRange returns a bounded result but currently reads the whole
// canonical JSONL blob from the selected store. The v1 storage layout has no
// row-offset index; callers must not report O(chunk) database I/O.
func (o *Owner) LoadPinnedRowsRange(ctx context.Context, runID, bundleHash string, want runtimedata.PinnedSource, start, end int) ([]any, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	if start < 0 || end < start || end > want.RowCount {
		return nil, fmt.Errorf("resource source range [%d,%d) is invalid for cardinality %d", start, end, want.RowCount)
	}
	var items []any
	err := o.runTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		current, err := o.pinnedSourceTx(ctx, tx, runID, bundleHash, want.Declaration)
		if err != nil {
			return err
		}
		if current != want {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource source metadata changed after intent commit")
		}
		var raw, schemaJSON []byte
		var key string
		if err := tx.QueryRowContext(ctx, o.query(`
			SELECT canonical_jsonl, canonical_schema_bytes, COALESCE(business_key_field, '')
			FROM resource_versions WHERE version_id=%s AND flow_path=%s AND event_name=%s
		`, 3), want.VersionID, want.Declaration.FlowPath, want.Declaration.EventName).Scan(&raw, &schemaJSON, &key); err != nil {
			return err
		}
		items, err = decodeCanonicalRowsRange(raw, schemaJSON, key, want, start, end)
		return err
	})
	return items, err
}

func decodeCanonicalRowsRange(raw, schemaJSON []byte, key string, want runtimedata.PinnedSource, start, end int) ([]any, error) {
	var schema map[string]any
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return nil, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource source schema is invalid: %v", err)
	}
	compiled, defects := runtimedata.CompileJSONL(want.Declaration, schema, key, raw)
	if len(defects) != 0 || compiled.VersionID != want.VersionID || len(compiled.Rows) != want.RowCount || !bytes.Equal(compiled.CanonicalJSONL, raw) {
		return nil, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource source payload disagrees with exact pinned version %s", want.VersionID)
	}
	items := make([]any, 0, end-start)
	for index := start; index < end; index++ {
		admitted, err := canonicaljson.Decode(compiled.Rows[index].Canonical)
		if err != nil {
			return nil, err
		}
		item, err := workflowexpr.ProjectSemanticValue(admitted)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}
