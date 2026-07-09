package testchanged

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

// ChangedFile is one git-reported path that may affect local Go tests.
type ChangedFile struct {
	Path   string
	Status string
}

// Package is the subset of go list metadata needed for local test selection.
type Package struct {
	ImportPath   string
	Dir          string
	RelDir       string
	Imports      []string
	TestImports  []string
	XTestImports []string
}

// Plan describes the exact local test selection decision.
type Plan struct {
	ChangedFiles      []ChangedFile
	SeedPackages      []Package
	DependentPackages []Package
	Packages          []Package
	FullSuite         bool
	FullSuiteReasons  []string
	UnownedFiles      []ChangedFile
	DocsOnly          bool
}

// PlanChanged maps changed files to Go packages and expands in-repository
// reverse dependencies. Global files fall back to the full suite.
func PlanChanged(repoRoot string, packages []Package, changedFiles []ChangedFile) (Plan, error) {
	normalized, err := normalizePackages(repoRoot, packages)
	if err != nil {
		return Plan{}, err
	}

	plan := Plan{
		ChangedFiles: normalizeChangedFiles(changedFiles),
	}
	if len(plan.ChangedFiles) == 0 {
		plan.DocsOnly = true
		return plan, nil
	}

	byImport := map[string]Package{}
	for _, pkg := range normalized {
		byImport[pkg.ImportPath] = pkg
	}

	seedImports := map[string]bool{}
	unownedNonDocs := 0
	for _, file := range plan.ChangedFiles {
		path := cleanRel(file.Path)
		if path == "" {
			continue
		}
		if fullSuiteReason(path) != "" {
			plan.FullSuite = true
			plan.FullSuiteReasons = append(plan.FullSuiteReasons, fullSuiteReason(path))
			continue
		}
		if pkg, ok := packageForChangedPath(normalized, path); ok {
			seedImports[pkg.ImportPath] = true
			continue
		}
		if isDocsOnlyPath(path) {
			plan.UnownedFiles = append(plan.UnownedFiles, file)
			continue
		}
		plan.UnownedFiles = append(plan.UnownedFiles, file)
		unownedNonDocs++
		plan.FullSuite = true
		plan.FullSuiteReasons = append(plan.FullSuiteReasons, fmt.Sprintf("%s has no owning Go package", path))
	}

	if len(seedImports) == 0 && !plan.FullSuite && unownedNonDocs == 0 {
		plan.DocsOnly = true
		return plan, nil
	}
	if plan.FullSuite {
		plan.Packages = []Package{{
			ImportPath: "./...",
			RelDir:     "...",
		}}
		plan.FullSuiteReasons = uniqueStrings(plan.FullSuiteReasons)
		return plan, nil
	}

	dependentImports := reverseDependencyClosure(normalized, seedImports)
	for importPath := range seedImports {
		if pkg, ok := byImport[importPath]; ok {
			plan.SeedPackages = append(plan.SeedPackages, pkg)
		}
	}
	for importPath := range dependentImports {
		if seedImports[importPath] {
			continue
		}
		if pkg, ok := byImport[importPath]; ok {
			plan.DependentPackages = append(plan.DependentPackages, pkg)
		}
	}
	sortPackages(plan.SeedPackages)
	sortPackages(plan.DependentPackages)
	plan.Packages = append(plan.Packages, plan.SeedPackages...)
	plan.Packages = append(plan.Packages, plan.DependentPackages...)
	return plan, nil
}

// TestCommand returns the exact go test command represented by plan.
func TestCommand(plan Plan, extraArgs []string) []string {
	if len(plan.Packages) == 0 && !plan.FullSuite {
		return nil
	}
	args := []string{"go", "test"}
	args = append(args, extraArgs...)
	if plan.FullSuite {
		return append(args, "./...")
	}
	for _, pkg := range plan.Packages {
		args = append(args, pkg.Pattern())
	}
	return args
}

// Pattern returns a stable package pattern suitable for go test.
func (p Package) Pattern() string {
	rel := cleanRel(p.RelDir)
	switch rel {
	case "", ".":
		return "."
	case "...":
		return "./..."
	default:
		return "./" + rel
	}
}

func normalizePackages(repoRoot string, packages []Package) ([]Package, error) {
	root := filepath.Clean(repoRoot)
	out := make([]Package, 0, len(packages))
	for _, pkg := range packages {
		if strings.TrimSpace(pkg.ImportPath) == "" {
			return nil, fmt.Errorf("package missing import path")
		}
		if strings.TrimSpace(pkg.RelDir) == "" {
			if strings.TrimSpace(pkg.Dir) == "" {
				return nil, fmt.Errorf("package %s missing Dir/RelDir", pkg.ImportPath)
			}
			rel, err := filepath.Rel(root, filepath.Clean(pkg.Dir))
			if err != nil {
				return nil, fmt.Errorf("rel package %s: %w", pkg.ImportPath, err)
			}
			pkg.RelDir = rel
		}
		pkg.RelDir = cleanRel(pkg.RelDir)
		out = append(out, pkg)
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i].RelDir) != len(out[j].RelDir) {
			return len(out[i].RelDir) > len(out[j].RelDir)
		}
		return out[i].RelDir < out[j].RelDir
	})
	return out, nil
}

func normalizeChangedFiles(files []ChangedFile) []ChangedFile {
	out := make([]ChangedFile, 0, len(files))
	seen := map[string]bool{}
	for _, file := range files {
		path := cleanRel(file.Path)
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		file.Path = path
		out = append(out, file)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Path < out[j].Path
	})
	return out
}

func packageForChangedPath(packages []Package, path string) (Package, bool) {
	dir := cleanRel(filepath.Dir(path))
	if strings.HasSuffix(path, ".go") {
		for _, pkg := range packages {
			if pkg.RelDir == dir {
				return pkg, true
			}
		}
		return Package{}, false
	}
	for _, pkg := range packages {
		if pkg.RelDir == "." {
			if !isDocsOnlyPath(path) && !strings.Contains(path, "/") {
				return pkg, true
			}
			continue
		}
		if dir == pkg.RelDir || strings.HasPrefix(dir, pkg.RelDir+"/") {
			return pkg, true
		}
	}
	return Package{}, false
}

func reverseDependencyClosure(packages []Package, seeds map[string]bool) map[string]bool {
	selected := map[string]bool{}
	queue := make([]string, 0, len(seeds))
	for seed := range seeds {
		selected[seed] = true
		queue = append(queue, seed)
	}
	sort.Strings(queue)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, pkg := range packages {
			if selected[pkg.ImportPath] {
				continue
			}
			if packageImports(pkg, current) {
				selected[pkg.ImportPath] = true
				queue = append(queue, pkg.ImportPath)
				sort.Strings(queue)
			}
		}
	}
	return selected
}

func packageImports(pkg Package, importPath string) bool {
	for _, value := range append(append(pkg.Imports, pkg.TestImports...), pkg.XTestImports...) {
		if value == importPath {
			return true
		}
	}
	return false
}

func fullSuiteReason(path string) string {
	switch path {
	case "go.mod", "go.sum", "go.work", "go.work.sum", "platform-spec.yaml":
		return path + " changed"
	default:
		return ""
	}
}

func isDocsOnlyPath(path string) bool {
	path = cleanRel(path)
	if strings.HasPrefix(path, "docs/") {
		return true
	}
	if strings.Contains(path, "/") {
		return false
	}
	return strings.HasSuffix(path, ".md") || strings.HasSuffix(path, ".txt")
}

func sortPackages(packages []Package) {
	sort.Slice(packages, func(i, j int) bool {
		return packages[i].Pattern() < packages[j].Pattern()
	})
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func cleanRel(path string) string {
	path = filepath.ToSlash(filepath.Clean(strings.TrimSpace(path)))
	if path == "." {
		return "."
	}
	path = strings.TrimPrefix(path, "./")
	return path
}
