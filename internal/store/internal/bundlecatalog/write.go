package bundlecatalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	bundlecatalogcontract "github.com/division-sh/swarm/internal/bundlecatalog"

	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
)

func (s *Postgres) UpsertBundleCatalog(ctx context.Context, req bundlecatalogcontract.Upsert) (bundlecatalogcontract.UpsertResult, error) {
	if err := s.requireBundleCatalogAccess(); err != nil {
		return bundlecatalogcontract.UpsertResult{}, err
	}
	req.BundleHash = strings.TrimSpace(req.BundleHash)
	if err := bundleidentity.ValidateCanonicalHash(req.BundleHash); err != nil {
		return bundlecatalogcontract.UpsertResult{}, fmt.Errorf("bundle catalog upsert requires canonical bundle_hash bundle-v1:sha256:<64 lowercase hex>")
	}
	if strings.TrimSpace(req.ContentYAML) == "" {
		return bundlecatalogcontract.UpsertResult{}, fmt.Errorf("bundle catalog upsert requires content_yaml")
	}
	parsedRaw, err := normalizedBundleCatalogJSON(req.ParsedJSON)
	if err != nil {
		return bundlecatalogcontract.UpsertResult{}, fmt.Errorf("bundle catalog parsed_json: %w", err)
	}
	metadataRaw, err := normalizedBundleCatalogJSON(req.Metadata)
	if err != nil {
		return bundlecatalogcontract.UpsertResult{}, fmt.Errorf("bundle catalog metadata: %w", err)
	}

	tx, err := s.backend.BeginTx(ctx, nil)
	if err != nil {
		return bundlecatalogcontract.UpsertResult{}, err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, data_blob, metadata)
		VALUES ($1, $2, $3::jsonb, $4::bytea, $5::jsonb)
		ON CONFLICT (bundle_hash) DO NOTHING
	`, req.BundleHash, req.ContentYAML, parsedRaw, nullableBytes(req.DataBlob), metadataRaw)
	if err != nil {
		return bundlecatalogcontract.UpsertResult{}, fmt.Errorf("upsert bundle catalog: %w", err)
	}
	registered := false
	if rows, err := result.RowsAffected(); err == nil {
		registered = rows > 0
	}
	if err := assertBundleCatalogUpsertIdempotent(ctx, tx, req.BundleHash, req.ContentYAML, parsedRaw, req.DataBlob, metadataRaw); err != nil {
		return bundlecatalogcontract.UpsertResult{}, err
	}
	detail, err := loadBundleCatalogInTx(ctx, tx, req.BundleHash)
	if err != nil {
		return bundlecatalogcontract.UpsertResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return bundlecatalogcontract.UpsertResult{}, err
	}
	return bundlecatalogcontract.UpsertResult{Detail: detail, Registered: registered}, nil
}

func assertBundleCatalogUpsertIdempotent(ctx context.Context, tx bundleCatalogTx, bundleHash, contentYAML string, parsedRaw, dataBlob, metadataRaw []byte) error {
	var gotContent string
	var gotParsed []byte
	var gotData []byte
	var gotMetadata []byte
	if err := tx.QueryRowContext(ctx, `
			SELECT content_yaml, COALESCE(parsed_json, '{}'::jsonb), data_blob, COALESCE(metadata, '{}'::jsonb)
			FROM bundles
			WHERE bundle_hash = $1
			FOR SHARE
		`, bundleHash).Scan(&gotContent, &gotParsed, &gotData, &gotMetadata); err != nil {
		return fmt.Errorf("load bundle catalog upsert result: %w", err)
	}
	gotParsed, err := normalizedBundleCatalogJSONBytes(gotParsed)
	if err != nil {
		return fmt.Errorf("stored bundle catalog parsed_json: %w", err)
	}
	gotMetadata, err = normalizedBundleCatalogJSONBytes(gotMetadata)
	if err != nil {
		return fmt.Errorf("stored bundle catalog metadata: %w", err)
	}
	if gotContent != contentYAML || !bytes.Equal(gotParsed, parsedRaw) || !bytes.Equal(nullableBytes(gotData), nullableBytes(dataBlob)) || !bytes.Equal(gotMetadata, metadataRaw) {
		return &bundlecatalogcontract.ConflictError{BundleHash: strings.TrimSpace(bundleHash)}
	}
	return nil
}

func loadBundleCatalogInTx(ctx context.Context, tx bundleCatalogTx, bundleHash string) (bundlecatalogcontract.Detail, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT
			bundle_hash,
			content_yaml,
			COALESCE(parsed_json, '{}'::jsonb),
			COALESCE(metadata, '{}'::jsonb),
			data_blob IS NOT NULL,
			COALESCE(octet_length(data_blob), 0)::bigint,
			ingested_at
		FROM bundles
		WHERE bundle_hash = $1
	`, strings.TrimSpace(bundleHash))
	scanned, err := scanBundleCatalogRow(row)
	if err != nil {
		return bundlecatalogcontract.Detail{}, err
	}
	return scanned.toDetail()
}

type bundleCatalogTx interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func normalizedBundleCatalogJSON(values map[string]any) ([]byte, error) {
	if values == nil {
		values = map[string]any{}
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return normalizedBundleCatalogJSONBytes(raw)
}

func normalizedBundleCatalogJSONBytes(raw []byte) ([]byte, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte(`{}`)
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	if decoded == nil {
		decoded = map[string]any{}
	}
	return json.Marshal(decoded)
}

func nullableBytes(raw []byte) []byte {
	if len(raw) == 0 {
		return nil
	}
	return raw
}
