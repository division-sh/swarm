package bundlecatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	bundlecatalogcontract "github.com/division-sh/swarm/internal/bundlecatalog"
)

func (s *SQLite) requireBundleCatalogAccess() error {
	return s.requireCurrentSchema()
}

func (s *SQLite) ListBundleCatalog(ctx context.Context, opts bundlecatalogcontract.ListOptions) (bundlecatalogcontract.ListResult, error) {
	if err := s.requireBundleCatalogAccess(); err != nil {
		return bundlecatalogcontract.ListResult{}, err
	}
	opts = defaultBundleCatalogListOptions(opts)
	args := make([]any, 0, 4)
	where := []string{"1=1"}
	if opts.Cursor != "" {
		ingestedAt, bundleHash, err := decodeBundleCatalogCursor(opts.Cursor)
		if err != nil {
			return bundlecatalogcontract.ListResult{}, err
		}
		where = append(where, "(ingested_at < ? OR (ingested_at = ? AND bundle_hash < ?))")
		args = append(args, ingestedAt.UTC(), ingestedAt.UTC(), bundleHash)
	}
	args = append(args, opts.Limit+1)
	rows, err := s.backend.QueryContext(ctx, fmt.Sprintf(`
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
		return bundlecatalogcontract.ListResult{}, fmt.Errorf("list sqlite bundle catalog: %w", err)
	}
	defer rows.Close()

	bundles := make([]bundlecatalogcontract.Summary, 0, opts.Limit)
	for rows.Next() {
		row, err := scanSQLiteBundleCatalogRow(rows)
		if err != nil {
			return bundlecatalogcontract.ListResult{}, err
		}
		detail, err := row.toDetail()
		if err != nil {
			return bundlecatalogcontract.ListResult{}, err
		}
		bundles = append(bundles, bundlecatalogcontract.Summary{
			BundleHash:    detail.BundleHash,
			AgentCount:    detail.AgentCount,
			HasData:       detail.HasData,
			DataSizeBytes: detail.DataSizeBytes,
			Metadata:      detail.Metadata,
			IngestedAt:    detail.IngestedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return bundlecatalogcontract.ListResult{}, fmt.Errorf("read sqlite bundle catalog: %w", err)
	}

	nextCursor := ""
	if len(bundles) > opts.Limit {
		bundles = bundles[:opts.Limit]
		nextCursor = encodeBundleCatalogCursor(bundles[len(bundles)-1])
	}
	if bundles == nil {
		bundles = []bundlecatalogcontract.Summary{}
	}
	return bundlecatalogcontract.ListResult{Bundles: bundles, NextCursor: nextCursor}, nil
}

func (s *SQLite) LoadBundleCatalog(ctx context.Context, bundleHash string) (bundlecatalogcontract.Detail, error) {
	if err := s.requireBundleCatalogAccess(); err != nil {
		return bundlecatalogcontract.Detail{}, err
	}
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return bundlecatalogcontract.Detail{}, bundlecatalogcontract.ErrNotFound
	}
	row := s.backend.QueryRowContext(ctx, `
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
		return bundlecatalogcontract.Detail{}, bundlecatalogcontract.ErrNotFound
	}
	if err != nil {
		return bundlecatalogcontract.Detail{}, err
	}
	return scanned.toDetail()
}

func (s *SQLite) ListBundleCatalogAgents(ctx context.Context, bundleHash string) (bundlecatalogcontract.AgentsResult, error) {
	detail, err := s.LoadBundleCatalog(ctx, bundleHash)
	if err != nil {
		return bundlecatalogcontract.AgentsResult{}, err
	}
	agents, err := projectBundleCatalogAgents(detail.ParsedJSON, detail.ContentYAML)
	if err != nil {
		return bundlecatalogcontract.AgentsResult{}, err
	}
	if agents == nil {
		agents = []bundlecatalogcontract.AgentDefinition{}
	}
	return bundlecatalogcontract.AgentsResult{Agents: agents}, nil
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
