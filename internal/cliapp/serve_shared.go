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
			return nil, fmt.Errorf("--bundle-hash must be bundle-v1:sha256:<64 lowercase hex>")
		}
		if _, ok := seen[hash]; ok {
			return nil, fmt.Errorf("--bundle-hash values must be unique")
		}
		seen[hash] = struct{}{}
		out = append(out, hash)
	}
	return out, nil
}

func NormalizeContractsRoot(path string) (string, error) {
	root := strings.TrimSpace(path)
	if root == "" {
		return "", runtimecontracts.NewContractsPathRequiredDiagnostic()
	}
	root = filepath.Clean(root)
	if regularFileExists(filepath.Join(root, "package.yaml")) {
		return root, nil
	}
	if filepath.Base(root) == "package.yaml" && regularFileExists(root) {
		return filepath.Dir(root), nil
	}
	return "", runtimecontracts.NewMissingPackageDiagnostic(path)
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

func DiscoverRepoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func assetCommandRepoRoot(repo string) string {
	if repo = strings.TrimSpace(repo); repo != "" {
		return repo
	}
	return DiscoverRepoRoot()
}
