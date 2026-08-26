package bundlecatalog

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	bundlecatalogcontract "github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
)

func (s *SQLite) UpsertBundleCatalog(ctx context.Context, req bundlecatalogcontract.Upsert) (bundlecatalogcontract.UpsertResult, error) {
	if err := s.requireBundleCatalogAccess(); err != nil {
		return bundlecatalogcontract.UpsertResult{}, err
	}
	var result bundlecatalogcontract.UpsertResult
	err := s.backend.RunTransaction(ctx, "sqlite bundle catalog upsert", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = UpsertSQLiteBundleCatalogTx(s, txctx, tx, req)
		return err
	})
	return result, err
}

func UpsertSQLiteBundleCatalogTx(s *SQLite, ctx context.Context, tx *sql.Tx, req bundlecatalogcontract.Upsert) (bundlecatalogcontract.UpsertResult, error) {
	if s == nil {
		return bundlecatalogcontract.UpsertResult{}, fmt.Errorf("bundle catalog sqlite owner is required")
	}
	if tx == nil {
		return bundlecatalogcontract.UpsertResult{}, fmt.Errorf("bundle catalog transaction is required")
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

	result, err := tx.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, data_blob, metadata)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (bundle_hash) DO NOTHING
	`, req.BundleHash, req.ContentYAML, parsedRaw, nullableBytes(req.DataBlob), metadataRaw)
	if err != nil {
		return bundlecatalogcontract.UpsertResult{}, fmt.Errorf("upsert sqlite bundle catalog: %w", err)
	}
	registered := false
	if rows, rowsErr := result.RowsAffected(); rowsErr == nil {
		registered = rows > 0
	}
	if err := assertSQLiteBundleCatalogUpsertIdempotent(ctx, tx, req.BundleHash, req.ContentYAML, parsedRaw, req.DataBlob, metadataRaw); err != nil {
		return bundlecatalogcontract.UpsertResult{}, err
	}
	detail, err := loadSQLiteBundleCatalogInTx(ctx, tx, req.BundleHash)
	if err != nil {
		return bundlecatalogcontract.UpsertResult{}, err
	}
	return bundlecatalogcontract.UpsertResult{Detail: detail, Registered: registered}, nil
}

func assertSQLiteBundleCatalogUpsertIdempotent(ctx context.Context, tx bundleCatalogTx, bundleHash, contentYAML string, parsedRaw, dataBlob, metadataRaw []byte) error {
	var gotContent string
	var gotParsed, gotData, gotMetadata []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT content_yaml, COALESCE(parsed_json, '{}'), data_blob, COALESCE(metadata, '{}')
		FROM bundles WHERE bundle_hash = ?
	`, bundleHash).Scan(&gotContent, &gotParsed, &gotData, &gotMetadata); err != nil {
		return fmt.Errorf("load sqlite bundle catalog upsert result: %w", err)
	}
	gotParsed, err := normalizedBundleCatalogJSONBytes(gotParsed)
	if err != nil {
		return fmt.Errorf("stored sqlite bundle catalog parsed_json: %w", err)
	}
	gotMetadata, err = normalizedBundleCatalogJSONBytes(gotMetadata)
	if err != nil {
		return fmt.Errorf("stored sqlite bundle catalog metadata: %w", err)
	}
	if gotContent != contentYAML || !bytes.Equal(gotParsed, parsedRaw) || !bytes.Equal(nullableBytes(gotData), nullableBytes(dataBlob)) || !bytes.Equal(gotMetadata, metadataRaw) {
		return &bundlecatalogcontract.ConflictError{BundleHash: strings.TrimSpace(bundleHash)}
	}
	return nil
}

func loadSQLiteBundleCatalogInTx(ctx context.Context, tx bundleCatalogTx, bundleHash string) (bundlecatalogcontract.Detail, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT bundle_hash, content_yaml, COALESCE(parsed_json, '{}'), COALESCE(metadata, '{}'),
			data_blob IS NOT NULL, COALESCE(length(data_blob), 0), ingested_at
		FROM bundles WHERE bundle_hash = ?
	`, strings.TrimSpace(bundleHash))
	scanned, err := scanSQLiteBundleCatalogRow(row)
	if err != nil {
		return bundlecatalogcontract.Detail{}, err
	}
	return scanned.toDetail()
}
