package canonicalrouting

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type artifactRegistry struct {
	Artifacts []artifactRegistryEntry `yaml:"artifacts"`
}

type artifactRegistryEntry struct {
	Root        string `yaml:"root"`
	Disposition string `yaml:"disposition"`
	Owner       string `yaml:"owner"`
	Proof       string `yaml:"proof"`
	Issue       int    `yaml:"issue"`
}

var (
	goAuthoredRouting  = regexp.MustCompile(`(?m)(?:^\s*source:\s*external(?:\s|$)|[,{]\s*source:\s*external(?:\s|$)|^\s*(?:connect:|resolution:|delivery:|on_missing:|on_conflict:|broadcast:\s*true(?:\s|$)))`)
	routingReplacement = regexp.MustCompile(`(?m)^\s*(?:pins:|connect:|flows:|instance:|resolution:|delivery:|on_missing:|on_conflict:|subscribes_to:|produces:|source:\s*external(?:\s|$)|broadcast:\s*true(?:\s|$))`)
	censusAnnotation   = regexp.MustCompile(`routing-example-census:\s*([a-z-]+)\s+issue=(none|[0-9]+)\s+owner=([^\s]+)\s+proof=([^\s]+)`)
)

type goRoutingFamily struct {
	File            string
	Function        string
	Source          string
	CanonicalLoader bool
	Literals        int
	UnownedLiterals int
	Marker          *goCensusMarker
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
		case "canonical-owner", "canonical-overlay", "negative-mutation", "different-concept":
			if entry.Issue != 0 {
				t.Fatalf("artifact %s disposition %s must not carry split issue %d", entry.Root, entry.Disposition, entry.Issue)
			}
		case "tracked-split":
			if entry.Issue <= 0 {
				t.Fatalf("tracked split %s is missing issue", entry.Root)
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

func TestCheckedYAMLRoutingCensusIsRepoWideAndStructural(t *testing.T) {
	repo := t.TempDir()
	root := filepath.Join(repo, "cmd", "hidden", "testdata", "flow-style")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "package.yaml"), []byte("name: adversarial\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	routing := "{pins: {inputs: {events: [{name: ingress, event: ingress.received, " + "source" + ": external}]}}}\n"
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte(routing), 0o644); err != nil {
		t.Fatal(err)
	}
	live := liveCheckedYAMLRoutingRoots(t, repo)
	const want = "cmd/hidden/testdata/flow-style"
	if _, ok := live[want]; !ok {
		t.Fatalf("repo-wide structural census missed %s: %#v", want, live)
	}
}

func TestCanonicalRoutingExamplesOwnGoAuthoredFixtures(t *testing.T) {
	repo := RepoRoot(t)
	families := goRoutingFamilies(t, repo)
	proofs := goFunctionNames(t, repo)
	var problems []string
	for _, family := range families {
		if family.CanonicalLoader && family.UnownedLiterals == 0 && family.Marker == nil {
			continue
		}
		if family.Marker == nil {
			detail := "without a canonical loader"
			if family.CanonicalLoader {
				detail = fmt.Sprintf("with %d routing literal(s) not owned by a non-replacing canonical mutation", family.UnownedLiterals)
			}
			problems = append(problems, fmt.Sprintf("%s:%s authors routing YAML %s or typed routing-example-census annotation", family.File, family.Function, detail))
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
			}
		}
	}
	if len(problems) != 0 {
		sort.Strings(problems)
		t.Fatalf("Go-authored routing ownership census failed:\n%s", strings.Join(problems, "\n"))
	}
}

func TestCanonicalRoutingGoProvenanceRejectsCeremonialOwnerCall(t *testing.T) {
	const source = `package fixture
import "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
func writeIndependentBundle(t T) string {
	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	canonicalrouting.WriteFile(t, root, "schema.yaml", "pins:\n  inputs:\n    events:\n      - name: bypass\n        event: bypassed\n        source: external\n")
	return root
}`
	file, err := parser.ParseFile(token.NewFileSet(), "adversarial.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[1].(*ast.FuncDecl)
	if !callsCanonicalRoutingOwner(fn) {
		t.Fatal("adversarial fixture must contain the ceremonial canonical owner call")
	}
	if got := unownedRoutingLiteralCount(fn); got != 1 {
		t.Fatalf("unowned routing literals = %d, want 1", got)
	}
}

func TestCanonicalRoutingGoProvenanceRejectsRouteBearingReplacement(t *testing.T) {
	const source = `package fixture
import "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
func replaceCanonicalRoute(t T) string {
	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	canonicalrouting.ReplaceFile(t, join(root, "schema.yaml"), "pins:\n  inputs: []\n", "pins:\n  inputs:\n    events:\n      - name: bypass\n        event: bypassed\n        source: external\n")
	return root
}`
	file, err := parser.ParseFile(token.NewFileSet(), "adversarial.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[1].(*ast.FuncDecl)
	if got := unownedRoutingLiteralCount(fn) + unownedRoutingArtifactReplacementCount(fn); got == 0 {
		t.Fatal("route-bearing ReplaceFile must not be treated as a non-replacing canonical mutation")
	}
}

func TestCanonicalRoutingGoProvenanceRejectsUnqualifiedLookalikeLoader(t *testing.T) {
	const source = `package fixture
func CopyExample(t T, name string) string { return "" }
func writeIndependentBundle(t T) string {
	root := CopyExample(t, "root-ingress")
	writeFile(root, "schema.yaml", "pins:\n  inputs:\n    events:\n      - name: bypass\n        event: bypassed\n        source: external\n")
	return root
}`
	file, err := parser.ParseFile(token.NewFileSet(), "adversarial.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[1].(*ast.FuncDecl)
	if callsCanonicalRoutingOwner(fn) {
		t.Fatal("an unqualified lookalike loader must not establish canonical provenance")
	}
}

func TestCanonicalRoutingTrackedSplitsRemainOpen(t *testing.T) {
	if os.Getenv("SWARM_CANONICAL_ROUTING_VERIFY_TRACKER_REMOTE") != "1" {
		t.Skip("remote tracker verification is enforced by CI")
	}
	token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
	if token == "" {
		t.Fatal("GITHUB_TOKEN is required for authoritative tracker verification")
	}
	repo := RepoRoot(t)
	issues := map[int]struct{}{}
	for _, entry := range loadArtifactRegistry(t, repo).Artifacts {
		if entry.Disposition == "tracked-split" && entry.Issue > 0 {
			issues[entry.Issue] = struct{}{}
		}
	}
	for _, family := range goRoutingFamilies(t, repo) {
		if family.Marker == nil || family.Marker.Issue == "none" {
			continue
		}
		issue, err := strconv.Atoi(family.Marker.Issue)
		if err != nil || issue <= 0 {
			t.Fatalf("%s:%s has invalid tracked issue %q", family.File, family.Function, family.Marker.Issue)
		}
		issues[issue] = struct{}{}
	}
	ordered := make([]int, 0, len(issues))
	for issue := range issues {
		ordered = append(ordered, issue)
	}
	sort.Ints(ordered)
	client := &http.Client{Timeout: 15 * time.Second}
	for _, issue := range ordered {
		request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://api.github.com/repos/division-sh/swarm/issues/%d", issue), nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Accept", "application/vnd.github+json")
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("read tracker #%d: %v", issue, err)
		}
		var result struct {
			State string `json:"state"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&result)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("read tracker #%d: GitHub returned %s", issue, response.Status)
		}
		if decodeErr != nil {
			t.Fatalf("decode tracker #%d: %v", issue, decodeErr)
		}
		if result.State != "open" {
			t.Fatalf("tracked split issue #%d is %s; repair artifact classifications before merging", issue, result.State)
		}
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
				canonicalLoader := callsCanonicalRoutingOwner(fn)
				artifactReplacements := 0
				if canonicalLoader {
					artifactReplacements = unownedRoutingArtifactReplacementCount(fn)
				}
				marker, markerCount := censusMarker(source)
				if markerCount > 0 && literalCount == 0 && !rawMatch && artifactReplacements == 0 {
					t.Fatalf("%s:%s has a routing-example-census annotation with no matching routing literal", filepath.ToSlash(rel), fn.Name.Name)
				}
				if literalCount == 0 && !rawMatch && artifactReplacements == 0 {
					continue
				}
				if markerCount > 1 {
					t.Fatalf("%s:%s has duplicate routing-example-census annotations", filepath.ToSlash(rel), fn.Name.Name)
				}
				families = append(families, goRoutingFamily{
					File:            filepath.ToSlash(rel),
					Function:        fn.Name.Name,
					Source:          source,
					CanonicalLoader: canonicalLoader,
					Literals:        literalCount,
					UnownedLiterals: unownedRoutingLiteralCount(fn) + artifactReplacements,
					Marker:          marker,
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
	return registry
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
	return len(routingStringExpressions(fn))
}

func unownedRoutingLiteralCount(fn *ast.FuncDecl) int {
	parents := astParents(fn.Body)
	count := 0
	for _, expr := range routingStringExpressions(fn) {
		if !canonicalMutationOwnsExpression(expr, parents) {
			count++
		}
	}
	return count
}

func routingStringExpressions(fn *ast.FuncDecl) []ast.Expr {
	parents := astParents(fn.Body)
	var expressions []ast.Expr
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
			expressions = append(expressions, expr)
		}
		return true
	})
	return expressions
}

func astParents(root ast.Node) map[ast.Node]ast.Node {
	parents := map[ast.Node]ast.Node{}
	var stack []ast.Node
	ast.Inspect(root, func(node ast.Node) bool {
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
	return parents
}

func canonicalMutationOwnsExpression(expr ast.Expr, parents map[ast.Node]ast.Node) bool {
	for node := ast.Node(expr); node != nil; node = parents[node] {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			continue
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "canonicalrouting" {
			continue
		}
		switch selector.Sel.Name {
		case "MergeMappingFile":
			return true
		default:
			continue
		}
	}
	return false
}

func unownedRoutingArtifactReplacementCount(fn *ast.FuncDecl) int {
	count := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if canonicalNonReplacingMutation(call) {
			return true
		}
		if canonicalRouteBearingReplacement(call) {
			count++
			return true
		}
		if !potentialArtifactWriter(call) {
			return true
		}
		for _, arg := range call.Args {
			path, ok := constantString(arg)
			if !ok {
				continue
			}
			switch filepath.Base(filepath.ToSlash(strings.TrimSpace(path))) {
			case "package.yaml", "schema.yaml", "nodes.yaml":
				count++
				return true
			}
		}
		return true
	})
	return count
}

func potentialArtifactWriter(call *ast.CallExpr) bool {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		name := strings.ToLower(fn.Name)
		return strings.Contains(name, "write") || strings.Contains(name, "fixture")
	case *ast.SelectorExpr:
		name := strings.ToLower(fn.Sel.Name)
		return strings.Contains(name, "write")
	default:
		return false
	}
}

func canonicalNonReplacingMutation(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "canonicalrouting" {
		return false
	}
	switch selector.Sel.Name {
	case "MergeMappingFile", "AppendRootExternalInputs":
		return true
	default:
		return false
	}
}

func canonicalRouteBearingReplacement(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "ReplaceFile" {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "canonicalrouting" {
		return false
	}
	for _, arg := range call.Args {
		value, ok := constantString(arg)
		if ok && routingReplacement.MatchString(value) {
			return true
		}
	}
	return false
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
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if ok && pkg.Name == "canonicalrouting" {
			switch selector.Sel.Name {
			case "CopyExample", "ExampleRoot":
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
	for _, bundleRoot := range outerPackageRoots(t, repo) {
		err := filepath.Walk(filepath.Join(repo, filepath.FromSlash(bundleRoot)), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
				return nil
			}
			if !fileContainsAuthoredRouting(t, path) {
				return nil
			}
			live[bundleRoot] = artifactRegistryEntry{Root: bundleRoot}
			return nil
		})
		if err != nil {
			t.Fatalf("scan bundle %s: %v", bundleRoot, err)
		}
	}
	return live
}

func outerPackageRoots(t testing.TB, repo string) []string {
	t.Helper()
	packageDirs := map[string]struct{}{}
	err := filepath.Walk(repo, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "vendor", "node_modules", ".swarm", "data":
				if path != repo {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if info.Name() == "package.yaml" {
			packageDirs[filepath.Dir(path)] = struct{}{}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("discover package.yaml roots: %v", err)
	}
	var roots []string
	for dir := range packageDirs {
		outer := true
		for parent := filepath.Dir(dir); strings.HasPrefix(parent, repo); parent = filepath.Dir(parent) {
			if _, exists := packageDirs[parent]; exists {
				outer = false
				break
			}
			if parent == repo || filepath.Dir(parent) == parent {
				break
			}
		}
		if !outer {
			continue
		}
		rel, err := filepath.Rel(repo, dir)
		if err != nil {
			t.Fatalf("relative package root %s: %v", dir, err)
		}
		roots = append(roots, filepath.ToSlash(rel))
	}
	sort.Strings(roots)
	return roots
}

func fileContainsAuthoredRouting(t testing.TB, path string) bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse routing census candidate %s: %v", path, err)
	}
	for _, node := range doc.Content {
		if yamlNodeContainsAuthoredRouting(node) {
			return true
		}
	}
	return false
}

func yamlNodeContainsAuthoredRouting(node *yaml.Node) bool {
	if node == nil {
		return false
	}
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			value := node.Content[i+1]
			switch key {
			case "connect", "resolution", "delivery", "on_missing", "on_conflict":
				return true
			case "source":
				if value.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(value.Value), "external") {
					return true
				}
			}
			if yamlNodeContainsAuthoredRouting(value) {
				return true
			}
		}
		return false
	}
	for _, child := range node.Content {
		if yamlNodeContainsAuthoredRouting(child) {
			return true
		}
	}
	return false
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
