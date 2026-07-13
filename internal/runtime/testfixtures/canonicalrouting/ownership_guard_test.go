package canonicalrouting

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type artifactRegistry struct {
	OpenSplitIssues []int                   `yaml:"open_split_issues"`
	Artifacts       []artifactRegistryEntry `yaml:"artifacts"`
}

type artifactRegistryEntry struct {
	Root        string `yaml:"root"`
	Disposition string `yaml:"disposition"`
	Owner       string `yaml:"owner"`
	Proof       string `yaml:"proof"`
	Issue       int    `yaml:"issue"`
}

var authoredRoutingLine = regexp.MustCompile(`^\s*(source:\s*external|connect:|resolution:|delivery:.*|on_missing:.*|on_conflict:.*)\s*$`)

var (
	goAuthoredRouting = regexp.MustCompile(`(?m)(?:^\s*source:\s*external(?:\s|$)|[,{]\s*source:\s*external(?:\s|$)|^\s*(?:connect:|resolution:|delivery:|on_missing:|on_conflict:|broadcast:\s*true(?:\s|$)))`)
	censusAnnotation  = regexp.MustCompile(`routing-example-census:\s*([a-z-]+)\s+issue=(none|[0-9]+)\s+owner=([^\s]+)\s+proof=([^\s]+)`)
)

type goRoutingFamily struct {
	File      string
	Function  string
	Source    string
	Canonical bool
	Literals  int
	Marker    *goCensusMarker
}

type goCensusMarker struct {
	Disposition string
	Issue       string
	Owner       string
	Proof       string
}

func TestCheckedYAMLRoutingArtifactRegistryEqualsLiveCensus(t *testing.T) {
	repo := RepoRoot(t)
	registry := loadArtifactRegistry(t, repo)
	openSplits := integerSet(registry.OpenSplitIssues)

	registered := map[string]artifactRegistryEntry{}
	for _, entry := range registry.Artifacts {
		entry.Root = filepath.ToSlash(filepath.Clean(strings.TrimSpace(entry.Root)))
		if entry.Root == "." || entry.Owner == "" || entry.Proof == "" {
			t.Fatalf("incomplete artifact registry entry: %#v", entry)
		}
		if _, exists := registered[entry.Root]; exists {
			t.Fatalf("duplicate artifact registry root %s", entry.Root)
		}
		switch entry.Disposition {
		case "canonical-overlay", "negative-mutation", "different-concept":
			if entry.Issue != 0 {
				t.Fatalf("artifact %s disposition %s must not carry split issue %d", entry.Root, entry.Disposition, entry.Issue)
			}
		case "tracked-split":
			if entry.Issue <= 0 {
				t.Fatalf("tracked split %s is missing issue", entry.Root)
			}
			if _, ok := openSplits[entry.Issue]; !ok {
				t.Fatalf("tracked split %s references issue #%d absent from open_split_issues", entry.Root, entry.Issue)
			}
		default:
			t.Fatalf("artifact %s has unknown disposition %q", entry.Root, entry.Disposition)
		}
		if _, err := os.Stat(filepath.Join(repo, filepath.FromSlash(entry.Root), "package.yaml")); err != nil {
			t.Fatalf("artifact %s package: %v", entry.Root, err)
		}
		proofFile := strings.SplitN(entry.Proof, ":", 2)[0]
		if _, err := os.Stat(filepath.Join(repo, filepath.FromSlash(proofFile))); err != nil {
			t.Fatalf("artifact %s proof %s: %v", entry.Root, entry.Proof, err)
		}
		registered[entry.Root] = entry
	}

	live := liveCheckedYAMLRoutingRoots(t, repo)
	missing := difference(live, registered)
	stale := difference(registered, live)
	if len(missing) != 0 || len(stale) != 0 {
		t.Fatalf("checked YAML routing census drifted\nunregistered live roots: %v\nstale registry roots: %v", missing, stale)
	}
}

func TestCanonicalRoutingExamplesOwnGoAuthoredFixtures(t *testing.T) {
	repo := RepoRoot(t)
	families := goRoutingFamilies(t, repo)
	proofs := goFunctionNames(t, repo)
	registry := loadArtifactRegistry(t, repo)
	openSplits := integerSet(registry.OpenSplitIssues)
	var problems []string
	for _, family := range families {
		if family.Canonical && family.Marker == nil {
			continue
		}
		if !family.Canonical && family.Marker == nil {
			problems = append(problems, fmt.Sprintf("%s:%s authors routing YAML without a canonical loader or typed routing-example-census annotation", family.File, family.Function))
			continue
		}
		marker := family.Marker
		switch marker.Disposition {
		case "parser-only", "negative-mutation", "provider-ingress", "harness", "different-concept":
		default:
			problems = append(problems, fmt.Sprintf("%s:%s has unknown census disposition %q", family.File, family.Function, marker.Disposition))
		}
		if marker.Owner == "" || marker.Proof == "" {
			problems = append(problems, fmt.Sprintf("%s:%s has incomplete census owner/proof", family.File, family.Function))
		}
		if _, ok := proofs[marker.Proof]; !ok {
			problems = append(problems, fmt.Sprintf("%s:%s names missing proof function %s", family.File, family.Function, marker.Proof))
		}
		if marker.Disposition == "parser-only" && completeBundleProducer(family.Source) {
			problems = append(problems, fmt.Sprintf("%s:%s produces a complete bundle and cannot be parser-only", family.File, family.Function))
		}
		if marker.Disposition == "negative-mutation" && !strings.Contains(family.Source, "canonicalrouting.") {
			problems = append(problems, fmt.Sprintf("%s:%s is marked negative-mutation but does not use the typed canonical mutator", family.File, family.Function))
		}
		if marker.Issue != "none" {
			issue, err := strconv.Atoi(marker.Issue)
			if err != nil || issue <= 0 {
				problems = append(problems, fmt.Sprintf("%s:%s has invalid issue %q", family.File, family.Function, marker.Issue))
			} else if _, ok := openSplits[issue]; !ok {
				problems = append(problems, fmt.Sprintf("%s:%s references issue #%d absent from open_split_issues", family.File, family.Function, issue))
			}
		}
	}
	if len(problems) != 0 {
		sort.Strings(problems)
		t.Fatalf("Go-authored routing ownership census failed:\n%s", strings.Join(problems, "\n"))
	}
}

func goRoutingFamilies(t testing.TB, repo string) []goRoutingFamily {
	t.Helper()
	fset := token.NewFileSet()
	var families []goRoutingFamily
	for _, root := range []string{"cmd", "internal"} {
		err := filepath.Walk(filepath.Join(repo, root), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			parsed, err := parser.ParseFile(fset, path, raw, parser.ParseComments)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(repo, path)
			if err != nil {
				return err
			}
			for _, decl := range parsed.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				start := fset.Position(fn.Pos()).Offset
				end := fset.Position(fn.End()).Offset
				if start < 0 || end > len(raw) || start >= end {
					continue
				}
				source := string(raw[start:end])
				literalCount := routingLiteralCount(fn)
				rawMatch := goAuthoredRouting.MatchString(source)
				marker, markerCount := censusMarker(source)
				if markerCount > 0 && literalCount == 0 && !rawMatch {
					t.Fatalf("%s:%s has a routing-example-census annotation with no matching routing literal", filepath.ToSlash(rel), fn.Name.Name)
				}
				if literalCount == 0 && !rawMatch {
					continue
				}
				if markerCount > 1 {
					t.Fatalf("%s:%s has duplicate routing-example-census annotations", filepath.ToSlash(rel), fn.Name.Name)
				}
				families = append(families, goRoutingFamily{
					File:      filepath.ToSlash(rel),
					Function:  fn.Name.Name,
					Source:    source,
					Canonical: callsCanonicalRoutingOwner(fn),
					Literals:  literalCount,
					Marker:    marker,
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan Go routing sources under %s: %v", root, err)
		}
	}
	sort.Slice(families, func(i, j int) bool {
		if families[i].File == families[j].File {
			return families[i].Function < families[j].Function
		}
		return families[i].File < families[j].File
	})
	return families
}

func loadArtifactRegistry(t testing.TB, repo string) artifactRegistry {
	t.Helper()
	registryPath := filepath.Join(repo, "internal", "runtime", "testfixtures", "canonicalrouting", "artifact_registry.yaml")
	raw, err := os.ReadFile(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	var registry artifactRegistry
	if err := yaml.Unmarshal(raw, &registry); err != nil {
		t.Fatalf("parse artifact registry: %v", err)
	}
	if len(registry.OpenSplitIssues) == 0 {
		t.Fatal("artifact registry must declare open_split_issues")
	}
	return registry
}

func integerSet(values []int) map[int]struct{} {
	result := make(map[int]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func goFunctionNames(t testing.TB, repo string) map[string]struct{} {
	t.Helper()
	result := map[string]struct{}{}
	fset := token.NewFileSet()
	for _, root := range []string{"cmd", "internal"} {
		err := filepath.Walk(filepath.Join(repo, root), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			parsed, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			for _, decl := range parsed.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok {
					result[fn.Name.Name] = struct{}{}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan Go proof functions under %s: %v", root, err)
		}
	}
	return result
}

func routingLiteralCount(fn *ast.FuncDecl) int {
	count := 0
	parents := map[ast.Node]ast.Node{}
	var stack []ast.Node
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if len(stack) != 0 {
			parents[node] = stack[len(stack)-1]
		}
		stack = append(stack, node)
		return true
	})
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		expr, ok := node.(ast.Expr)
		if !ok {
			return true
		}
		if _, nested := parents[expr].(*ast.BinaryExpr); nested {
			return true
		}
		value, ok := constantString(expr)
		if ok && goAuthoredRouting.MatchString(value) {
			count++
		}
		return true
	})
	return count
}

func constantString(expr ast.Expr) (string, bool) {
	switch value := expr.(type) {
	case *ast.BasicLit:
		if value.Kind != token.STRING {
			return "", false
		}
		decoded, err := strconv.Unquote(value.Value)
		return decoded, err == nil
	case *ast.BinaryExpr:
		if value.Op != token.ADD {
			return "", false
		}
		left, leftOK := constantString(value.X)
		right, rightOK := constantString(value.Y)
		return left + right, leftOK && rightOK
	default:
		return "", false
	}
}

func callsCanonicalRoutingOwner(fn *ast.FuncDecl) bool {
	found := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if ident, ok := call.Fun.(*ast.Ident); ok {
			switch ident.Name {
			case "CopyExample", "ExampleRoot", "CopyTree", "ReplaceFile", "WriteFile":
				found = true
			}
			return !found
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if ok && pkg.Name == "canonicalrouting" {
			switch selector.Sel.Name {
			case "CopyExample", "ExampleRoot", "CopyTree", "ReplaceFile", "WriteFile":
				found = true
			}
		}
		return !found
	})
	return found
}

func censusMarker(source string) (*goCensusMarker, int) {
	matches := censusAnnotation.FindAllStringSubmatch(source, -1)
	if len(matches) == 0 {
		return nil, 0
	}
	match := matches[0]
	return &goCensusMarker{Disposition: match[1], Issue: match[2], Owner: match[3], Proof: match[4]}, len(matches)
}

func completeBundleProducer(source string) bool {
	return strings.Contains(source, "package.yaml") &&
		(strings.Contains(source, "WriteFile") || strings.Contains(source, "writeFile") || strings.Contains(source, "os.WriteFile"))
}

func liveCheckedYAMLRoutingRoots(t testing.TB, repo string) map[string]artifactRegistryEntry {
	t.Helper()
	live := map[string]artifactRegistryEntry{}
	for _, scanRoot := range []string{"tests", "internal"} {
		err := filepath.Walk(filepath.Join(repo, scanRoot), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
				return nil
			}
			if !fileContainsAuthoredRoutingLine(t, path) {
				return nil
			}
			root := outerBundleRoot(repo, path)
			if root == "" {
				t.Fatalf("routing YAML %s has no containing package.yaml", path)
			}
			live[root] = artifactRegistryEntry{Root: root}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", scanRoot, err)
		}
	}
	return live
}

func fileContainsAuthoredRoutingLine(t testing.TB, path string) bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if authoredRoutingLine.MatchString(scanner.Text()) {
			return true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return false
}

func outerBundleRoot(repo, path string) string {
	dir := filepath.Dir(path)
	var candidate string
	for {
		if _, err := os.Stat(filepath.Join(dir, "package.yaml")); err == nil {
			candidate = dir
		}
		if dir == repo {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir || !strings.HasPrefix(parent, repo) {
			break
		}
		dir = parent
	}
	if candidate == "" {
		return ""
	}
	rel, err := filepath.Rel(repo, candidate)
	if err != nil {
		return ""
	}
	return filepath.ToSlash(rel)
}

func difference[A any, B any](left map[string]A, right map[string]B) []string {
	var out []string
	for key := range left {
		if _, ok := right[key]; !ok {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}
