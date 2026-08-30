package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"
)

func ServeBundleHashes(opts ServeOptions) ([]string, error) {
	candidates := []string{}
	if hash := strings.TrimSpace(opts.BundleHash); hash != "" {
		candidates = append(candidates, hash)
	}
	candidates = append(candidates, opts.BundleHashes...)
	out := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		hash := strings.TrimSpace(candidate)
		if hash == "" {
			return nil, fmt.Errorf("--bundle-hash must be non-empty")
		}
		if err := runtimecontracts.ValidateBundleHash(hash); err != nil {
			return nil, fmt.Errorf("--bundle-hash must be bundle-v2:sha256:<64 lowercase hex>")
		}
		if _, ok := seen[hash]; ok {
			return nil, fmt.Errorf("--bundle-hash values must be unique")
		}
		seen[hash] = struct{}{}
		out = append(out, hash)
	}
	return out, nil
}

func NormalizeSourceRoot(path string) (string, error) {
	root := strings.TrimSpace(path)
	if root == "" {
		return "", fmt.Errorf("source directory is unavailable after source-root selection")
	}
	root = filepath.Clean(root)
	info, err := os.Stat(root)
	if err != nil {
		return "", fmt.Errorf("source directory %q: %w", path, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("source root %q must be a directory", path)
	}
	return root, nil
}

func ResolvePath(RepoRoot, path string) string {
	path = strings.TrimSpace(path)
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(RepoRoot, path)
}

type serveSchemaPlanSummary struct {
	tableCount  int
	columnCount int
	tables      []serveSchemaTableSummary
}

type serveSchemaTableSummary struct {
	Name        string `json:"name"`
	ColumnCount int    `json:"column_count"`
}

func SummarizeServeSchemaPlans(plans []store.SchemaTableDDL) string {
	summary := newServeSchemaPlanSummary(plans)
	return summary.text()
}

func newServeSchemaPlanSummary(plans []store.SchemaTableDDL) serveSchemaPlanSummary {
	tables := make([]serveSchemaTableSummary, 0, len(plans))
	totalColumns := 0
	for _, plan := range plans {
		tables = append(tables, serveSchemaTableSummary{Name: strings.TrimSpace(plan.TableName), ColumnCount: plan.ColumnCount})
		totalColumns += plan.ColumnCount
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	return serveSchemaPlanSummary{
		tableCount:  len(plans),
		columnCount: totalColumns,
		tables:      tables,
	}
}

func (summary serveSchemaPlanSummary) text() string {
	if summary.tableCount == 0 {
		return "verified 0 generated tables"
	}
	return fmt.Sprintf("verified %d generated tables", summary.tableCount)
}
