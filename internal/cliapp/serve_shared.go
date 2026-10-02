package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/store"
)

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
