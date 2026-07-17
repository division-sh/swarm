package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func (s *SQLiteRuntimeStore) requireBundleCatalogAccess() error {
	return s.requireCurrentSchema()
}

func (s *SQLiteRuntimeStore) ListBundleCatalog(ctx context.Context, opts BundleCatalogListOptions) (BundleCatalogListResult, error) {
	if err := s.requireBundleCatalogAccess(); err != nil {
		return BundleCatalogListResult{}, err
	}
	opts = defaultBundleCatalogListOptions(opts)
	args := make([]any, 0, 4)
	where := []string{"1=1"}
	if opts.Cursor != "" {
		ingestedAt, bundleHash, err := decodeBundleCatalogCursor(opts.Cursor)
		if err != nil {
			return BundleCatalogListResult{}, err
		}
		where = append(where, "(ingested_at < ? OR (ingested_at = ? AND bundle_hash < ?))")
		args = append(args, ingestedAt.UTC(), ingestedAt.UTC(), bundleHash)
	}
	args = append(args, opts.Limit+1)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			bundle_hash,
			content_yaml,
			COALESCE(parsed_json, '{}'),
			COALESCE(metadata, '{}'),
			data_blob IS NOT NULL,
			COALESCE(length(data_blob), 0),
			ingested_at
		FROM bundles
		WHERE %s
		ORDER BY ingested_at DESC, bundle_hash DESC
		LIMIT ?
	`, strings.Join(where, " AND ")), args...)
	if err != nil {
		return BundleCatalogListResult{}, fmt.Errorf("list sqlite bundle catalog: %w", err)
	}
	defer rows.Close()

	bundles := make([]BundleCatalogSummary, 0, opts.Limit)
	for rows.Next() {
		row, err := scanSQLiteBundleCatalogRow(rows)
		if err != nil {
			return BundleCatalogListResult{}, err
		}
		detail, err := row.toDetail()
		if err != nil {
			return BundleCatalogListResult{}, err
		}
		bundles = append(bundles, BundleCatalogSummary{
			BundleHash:    detail.BundleHash,
			AgentCount:    detail.AgentCount,
			HasData:       detail.HasData,
			DataSizeBytes: detail.DataSizeBytes,
			Metadata:      detail.Metadata,
			IngestedAt:    detail.IngestedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return BundleCatalogListResult{}, fmt.Errorf("read sqlite bundle catalog: %w", err)
	}

	nextCursor := ""
	if len(bundles) > opts.Limit {
		bundles = bundles[:opts.Limit]
		nextCursor = encodeBundleCatalogCursor(bundles[len(bundles)-1])
	}
	if bundles == nil {
		bundles = []BundleCatalogSummary{}
	}
	return BundleCatalogListResult{Bundles: bundles, NextCursor: nextCursor}, nil
}

func (s *SQLiteRuntimeStore) LoadBundleCatalog(ctx context.Context, bundleHash string) (BundleCatalogDetail, error) {
	if err := s.requireBundleCatalogAccess(); err != nil {
		return BundleCatalogDetail{}, err
	}
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return BundleCatalogDetail{}, ErrBundleNotFound
	}
	row := s.DB.QueryRowContext(ctx, `
		SELECT
			bundle_hash,
			content_yaml,
			COALESCE(parsed_json, '{}'),
			COALESCE(metadata, '{}'),
			data_blob IS NOT NULL,
			COALESCE(length(data_blob), 0),
			ingested_at
		FROM bundles
		WHERE bundle_hash = ?
	`, bundleHash)
	scanned, err := scanSQLiteBundleCatalogRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return BundleCatalogDetail{}, ErrBundleNotFound
	}
	if err != nil {
		return BundleCatalogDetail{}, err
	}
	return scanned.toDetail()
}

func (s *SQLiteRuntimeStore) ListBundleCatalogAgents(ctx context.Context, bundleHash string) (BundleCatalogAgentsResult, error) {
	detail, err := s.LoadBundleCatalog(ctx, bundleHash)
	if err != nil {
		return BundleCatalogAgentsResult{}, err
	}
	agents, err := projectBundleCatalogAgents(detail.ParsedJSON, detail.ContentYAML)
	if err != nil {
		return BundleCatalogAgentsResult{}, err
	}
	if agents == nil {
		agents = []BundleCatalogAgentDefinition{}
	}
	return BundleCatalogAgentsResult{Agents: agents}, nil
}

func scanSQLiteBundleCatalogRow(row bundleCatalogScanner) (bundleCatalogRow, error) {
	var (
		out         bundleCatalogRow
		ingestedRaw any
	)
	if err := row.Scan(
		&out.BundleHash,
		&out.ContentYAML,
		&out.ParsedJSONRaw,
		&out.MetadataRaw,
		&out.HasData,
		&out.DataSizeBytes,
		&ingestedRaw,
	); err != nil {
		return bundleCatalogRow{}, err
	}
	out.BundleHash = strings.TrimSpace(out.BundleHash)
	if at, ok, err := sqliteTimeValue(ingestedRaw); err != nil {
		return bundleCatalogRow{}, fmt.Errorf("scan sqlite bundle catalog ingested_at: %w", err)
	} else if ok {
		out.IngestedAt = at
	}
	return out, nil
}
