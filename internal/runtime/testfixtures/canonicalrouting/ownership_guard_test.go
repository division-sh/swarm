package canonicalrouting

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
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
	Artifacts []artifactRegistryEntry   `yaml:"artifacts"`
	Groups    []artifactRegistryGroup   `yaml:"groups"`
	GoGroups  []goArtifactRegistryGroup `yaml:"go_groups"`
}

type goArtifactRegistryGroup struct {
	File        string   `yaml:"file"`
	Functions   []string `yaml:"functions"`
	Disposition string   `yaml:"disposition"`
	Owner       string   `yaml:"owner"`
	Proofs      []string `yaml:"proofs"`
	Issue       string   `yaml:"issue"`
}

type artifactRegistryGroup struct {
	Roots       []string `yaml:"roots"`
	Disposition string   `yaml:"disposition"`
	Owner       string   `yaml:"owner"`
	Proof       string   `yaml:"proof"`
	Issue       int      `yaml:"issue"`
}

type artifactRegistryEntry struct {
	Root        string `yaml:"root"`
	Disposition string `yaml:"disposition"`
	Owner       string `yaml:"owner"`
	Proof       string `yaml:"proof"`
	Issue       int    `yaml:"issue"`
}

var (
	censusAnnotation = regexp.MustCompile(`routing-example-census:\s*([a-z-]+)\s+issue=(none|[0-9]+)\s+owner=([^\s]+)\s+proof=([^\s]+)`)
)

type goRoutingFamily struct {
	File            string
	Function        string
	Source          string
	PackageScope    bool
	CanonicalLoader bool
	Literals        int
	UnownedLiterals int
	Marker          *goCensusMarker
}

type goCensusMarker struct {
	Disposition string
	Issue       string
	Owner       string
	Proofs      []string
}

type goSourceSymbol struct {
	Key        string
	File       string
	Package    string
	Name       string
	Node       ast.Node
	Imports    map[string]string
	IsFunction bool
}

type goSourceIndex struct {
	Symbols        map[string]goSourceSymbol
	ByPackage      map[string]map[string][]string
	ByDirectory    map[string]map[string][]string
	References     map[string]map[string]struct{}
	FunctionProofs map[string]struct{}
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
	// routing-example-census: different-concept issue=none owner=canonicalrouting.structural_yaml_census proof=internal/runtime/testfixtures/canonicalrouting/ownership_guard_test.go:TestCheckedYAMLRoutingCensusIsRepoWideAndStructural
	cases := map[string]string{
		"flow-style-external": `{pins: {inputs: {events: [{name: ingress, event: ingress.received, source: external}]}}}`,
		"broadcast":           "emit:\n  event: result.ready\n  broadcast: true\n",
		"node-subscription":   "worker:\n  subscribes_to: [work.requested]\n",
		"agent-subscription":  "worker:\n  subscriptions: [work.requested]\n",
		"producer":            "worker:\n  produces: [work.ready]\n",
		"handler-derived":     "worker:\n  event_handlers:\n    work.requested: {}\n",
		"second-document":     "name: inert\n---\nworker:\n  subscribes_to: [work.requested]\n",
	}
	for name, routing := range cases {
		t.Run(name, func(t *testing.T) {
			repo := t.TempDir()
			root := filepath.Join(repo, "cmd", "hidden", "testdata", name)
			if err := os.MkdirAll(root, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "package.yaml"), []byte("name: adversarial\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "routing.yaml"), []byte(routing), 0o644); err != nil {
				t.Fatal(err)
			}
			live := liveCheckedYAMLRoutingRoots(t, repo)
			want := "cmd/hidden/testdata/" + name
			if _, ok := live[want]; !ok {
				t.Fatalf("repo-wide structural census missed %s: %#v", want, live)
			}
		})
	}
	t.Run("nested-data-directory", func(t *testing.T) {
		repo := t.TempDir()
		root := filepath.Join(repo, "cmd", "hidden", "data", "routing")
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "package.yaml"), []byte("name: adversarial\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("pins: {inputs: {events: [{name: ingress, event: ingress.received, source: external}]}}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		live := liveCheckedYAMLRoutingRoots(t, repo)
		if _, ok := live["cmd/hidden/data/routing"]; !ok {
			t.Fatalf("repo-wide structural census skipped nested data directory: %#v", live)
		}
	})
}

func TestCanonicalRoutingExamplesOwnGoAuthoredFixtures(t *testing.T) {
	repo := RepoRoot(t)
	families := goRoutingFamilies(t, repo)
	sources := indexGoSources(t, repo)
	registered := loadGoArtifactRegistry(t, repo)
	validateGoRegistryProofBindings(t, repo, sources)
	seen := map[string]struct{}{}
	var problems []string
	for _, family := range families {
		key := goArtifactRegistryKey(family.File, family.Function)
		marker := family.Marker
		if registeredMarker, ok := registered[key]; ok {
			if marker != nil {
				problems = append(problems, fmt.Sprintf("%s:%s has both source-local and registry census classifications", family.File, family.Function))
			}
			marker = registeredMarker
			seen[key] = struct{}{}
		}
		if family.CanonicalLoader && family.UnownedLiterals == 0 && marker == nil {
			continue
		}
		if marker == nil {
			detail := "without a canonical loader"
			if family.CanonicalLoader {
				detail = fmt.Sprintf("with %d routing literal(s) not owned by a non-replacing canonical mutation", family.UnownedLiterals)
			}
			problems = append(problems, fmt.Sprintf("%s:%s authors routing YAML %s or typed routing-example-census annotation", family.File, family.Function, detail))
			continue
		}
		switch marker.Disposition {
		case "canonical-overlay", "parser-only", "negative-mutation", "provider-ingress", "harness", "different-concept":
		default:
			problems = append(problems, fmt.Sprintf("%s:%s has unknown census disposition %q", family.File, family.Function, marker.Disposition))
		}
		if marker.Owner == "" || len(marker.Proofs) == 0 {
			problems = append(problems, fmt.Sprintf("%s:%s has incomplete census owner/proof", family.File, family.Function))
		}
		proofBound := false
		for _, proof := range marker.Proofs {
			if _, ok := sources.FunctionProofs[proof]; !ok {
				problems = append(problems, fmt.Sprintf("%s:%s names missing path-qualified proof function %s", family.File, family.Function, proof))
				continue
			}
			if sources.proofConsumes(proof, key) {
				proofBound = true
			}
		}
		if !proofBound {
			problems = append(problems, fmt.Sprintf("%s:%s proofs %v do not consume the classified symbol; actual consumers: %v", family.File, family.Function, marker.Proofs, sources.proofConsumers(key)))
		}
		if marker.Disposition == "parser-only" && (completeBundleProducer(family.Source) || family.PackageScope && strings.Contains(family.Source, "package.yaml")) {
			problems = append(problems, fmt.Sprintf("%s:%s produces a complete bundle and cannot be parser-only", family.File, family.Function))
		}
		if marker.Disposition == "negative-mutation" && !strings.Contains(family.Source, "canonicalrouting.") {
			problems = append(problems, fmt.Sprintf("%s:%s is marked negative-mutation but does not use the typed canonical mutator", family.File, family.Function))
		}
		if marker.Disposition == "canonical-overlay" && !strings.Contains(family.Source, "canonicalrouting.") {
			problems = append(problems, fmt.Sprintf("%s:%s is marked canonical-overlay but does not use the typed canonical mutator", family.File, family.Function))
		}
		if marker.Issue != "none" {
			issue, err := strconv.Atoi(marker.Issue)
			if err != nil || issue <= 0 {
				problems = append(problems, fmt.Sprintf("%s:%s has invalid issue %q", family.File, family.Function, marker.Issue))
			}
		}
	}
	for key := range registered {
		if _, ok := seen[key]; !ok {
			problems = append(problems, fmt.Sprintf("stale Go routing registry entry %s", key))
		}
	}
	if len(problems) != 0 {
		sort.Strings(problems)
		t.Fatalf("Go-authored routing ownership census failed:\n%s", strings.Join(problems, "\n"))
	}
}

func validateGoRegistryProofBindings(t testing.TB, repo string, sources goSourceIndex) {
	t.Helper()
	for _, group := range loadArtifactRegistry(t, repo).GoGroups {
		file := filepath.ToSlash(filepath.Clean(strings.TrimSpace(group.File)))
		seen := map[string]struct{}{}
		for _, registeredProof := range group.Proofs {
			proof := strings.TrimSpace(registeredProof)
			if _, duplicate := seen[proof]; duplicate {
				t.Fatalf("Go routing registry group %s contains duplicate proof %s", file, proof)
			}
			seen[proof] = struct{}{}
			bound := false
			for _, function := range group.Functions {
				if sources.proofConsumes(proof, goArtifactRegistryKey(file, function)) {
					bound = true
					break
				}
			}
			if !bound {
				t.Fatalf("Go routing registry proof %s does not consume any classified symbol in %s", proof, file)
			}
		}
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

func TestCanonicalRoutingGoProvenanceUsesStructuralRoutingOwner(t *testing.T) {
	cases := map[string]string{
		"emit":       "emit:\n  event: bypassed\n",
		"replies_to": "resolution:\n  mode: reply\n  replies_to: request\n",
		"carries":    "carries:\n  - account_id\n",
	}
	for name, replacement := range cases {
		t.Run(name, func(t *testing.T) {
			source := fmt.Sprintf(`package fixture
import "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
func replaceCanonicalRoute(t T) string {
	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	canonicalrouting.ReplaceFile(t, join(root, "schema.yaml"), "old", %q)
	return root
}`, replacement)
			file, err := parser.ParseFile(token.NewFileSet(), "adversarial.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			fn := file.Decls[1].(*ast.FuncDecl)
			if got := unownedRoutingLiteralCount(fn) + unownedRoutingArtifactReplacementCount(fn); got == 0 {
				t.Fatalf("%s-only replacement escaped structural routing provenance", name)
			}
		})
	}
}

func TestCanonicalRoutingGoProvenanceRequiresHelperMutationClassification(t *testing.T) {
	const source = `package fixture
import "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
func load(t T) string {
	root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateReply)
	mutateRoute(t, root)
	return root
}
func mutateRoute(t T, root string) {
	canonicalrouting.ReplaceFile(t, join(root, "schema.yaml"), "old", "emit:\n  event: bypassed\n  replies_to: request\n  carries: [account_id]\n")
}`
	file, err := parser.ParseFile(token.NewFileSet(), "adversarial.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	helper := file.Decls[2].(*ast.FuncDecl)
	if callsCanonicalRoutingOwner(helper) {
		t.Fatal("helper must not inherit canonical provenance merely because its caller loads an example")
	}
	if got := unownedRoutingLiteralCount(helper) + unownedRoutingArtifactReplacementCount(helper); got == 0 {
		t.Fatal("route-bearing helper mutation escaped independent structural classification")
	}
}

func TestCanonicalRoutingGoCensusIncludesCompleteStructuralBundle(t *testing.T) {
	repo := RepoRoot(t)
	for _, family := range goRoutingFamilies(t, repo) {
		if family.File == "internal/apiv1/operator_run_completion_system_node_test.go" && family.Function == "runCompletionSystemNodeBundle" {
			return
		}
	}
	t.Fatal("structural Go routing census omitted runCompletionSystemNodeBundle")
}

func TestCanonicalRoutingGoCensusIncludesPackageScopeBundle(t *testing.T) {
	repo := t.TempDir()
	root := filepath.Join(repo, "cmd", "hidden", "data", "routing")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	const source = `package fixture
var bundleFiles = map[string]string{
	"package.yaml": "name: adversarial\n",
	"schema.yaml": "pins: {inputs: {events: [{name: ingress, event: ingress.received, source: external}]}}\n",
}
func build() { materialize(bundleFiles) }
`
	if err := os.WriteFile(filepath.Join(root, "fixture_test.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, family := range goRoutingFamilies(t, repo) {
		if family.File == "cmd/hidden/data/routing/fixture_test.go" && family.Function == "bundleFiles" && family.PackageScope {
			return
		}
	}
	t.Fatal("structural Go routing census omitted package-scope bundle declaration")
}

func TestCanonicalRoutingProofsArePathQualifiedAndConsumerBound(t *testing.T) {
	repo := t.TempDir()
	root := filepath.Join(repo, "fixture")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	const source = `package fixture
var routedFiles = map[string]string{
	"schema.yaml": "pins: {inputs: {events: [{name: ingress, event: ingress.received, source: external}]}}\n",
}
func loadRoutedFiles() { materialize(routedFiles) }
func TestConsumesRoute() { loadRoutedFiles() }
func TestUnrelated() {}
`
	if err := os.WriteFile(filepath.Join(root, "fixture_test.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	index := indexGoSources(t, repo)
	artifact := "fixture/fixture_test.go:routedFiles"
	if !index.proofConsumes("fixture/fixture_test.go:TestConsumesRoute", artifact) {
		t.Fatal("transitive proof consumer was not bound to package-scope artifact")
	}
	if index.proofConsumes("fixture/fixture_test.go:TestUnrelated", artifact) {
		t.Fatal("unrelated existing test was accepted as artifact proof")
	}
	if _, ok := index.FunctionProofs["TestConsumesRoute"]; ok {
		t.Fatal("bare proof names must not be accepted")
	}
}

func TestCanonicalRoutingGoProvenanceRejectsRouteBearingGenericMerge(t *testing.T) {
	const source = `package fixture
import "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
func mergeIndependentRoute(t T) string {
	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	canonicalrouting.MergeMappingFile(t, root, "nodes.yaml", "bypass:\n  source: external\n  subscribes_to: [bypassed]\n  event_handlers:\n    bypassed: {}\n")
	return root
}`
	file, err := parser.ParseFile(token.NewFileSet(), "adversarial.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[1].(*ast.FuncDecl)
	if got := unownedRoutingLiteralCount(fn); got == 0 {
		t.Fatal("route-bearing MergeMappingFile must not establish canonical provenance")
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
	for _, marker := range loadGoArtifactRegistry(t, repo) {
		if marker.Issue == "none" {
			continue
		}
		issue, err := strconv.Atoi(marker.Issue)
		if err != nil || issue <= 0 {
			t.Fatalf("Go routing registry has invalid tracked issue %q", marker.Issue)
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
	err := walkRepositoryGoFiles(repo, func(path string, raw []byte) error {
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
			if ok && fn.Body != nil {
				function := goFunctionSymbolName(fn)
				start := fset.Position(fn.Pos()).Offset
				end := fset.Position(fn.End()).Offset
				if start < 0 || end > len(raw) || start >= end {
					continue
				}
				source := string(raw[start:end])
				literalCount := routingLiteralCount(fn)
				canonicalLoader := callsCanonicalRoutingOwner(fn)
				artifactReplacements := unownedRoutingArtifactReplacementCount(fn)
				marker, markerCount := censusMarker(source)
				if markerCount > 0 && literalCount == 0 && artifactReplacements == 0 {
					t.Fatalf("%s:%s has a routing-example-census annotation with no matching routing literal", filepath.ToSlash(rel), function)
				}
				if literalCount == 0 && artifactReplacements == 0 {
					continue
				}
				if markerCount > 1 {
					t.Fatalf("%s:%s has duplicate routing-example-census annotations", filepath.ToSlash(rel), function)
				}
				families = append(families, goRoutingFamily{
					File:            filepath.ToSlash(rel),
					Function:        function,
					Source:          source,
					CanonicalLoader: canonicalLoader,
					Literals:        literalCount,
					UnownedLiterals: unownedRoutingLiteralCount(fn) + artifactReplacements,
					Marker:          marker,
				})
				continue
			}
			gen, ok := decl.(*ast.GenDecl)
			if !ok || (gen.Tok != token.VAR && gen.Tok != token.CONST) {
				continue
			}
			for _, spec := range gen.Specs {
				valueSpec, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				literalCount := len(routingStringExpressions(valueSpec))
				if literalCount == 0 {
					continue
				}
				start := fset.Position(valueSpec.Pos()).Offset
				end := fset.Position(valueSpec.End()).Offset
				if start < 0 || end > len(raw) || start >= end {
					continue
				}
				source := string(raw[start:end])
				marker, markerCount := censusMarker(source)
				if markerCount > 1 {
					t.Fatalf("%s:%s has duplicate routing-example-census annotations", filepath.ToSlash(rel), valueSpec.Names[0].Name)
				}
				for _, name := range valueSpec.Names {
					if name.Name == "_" {
						continue
					}
					families = append(families, goRoutingFamily{
						File:            filepath.ToSlash(rel),
						Function:        name.Name,
						Source:          source,
						PackageScope:    true,
						Literals:        literalCount,
						UnownedLiterals: literalCount,
						Marker:          marker,
					})
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan repository Go routing sources: %v", err)
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
	for _, group := range registry.Groups {
		for _, root := range group.Roots {
			registry.Artifacts = append(registry.Artifacts, artifactRegistryEntry{
				Root:        root,
				Disposition: group.Disposition,
				Owner:       group.Owner,
				Proof:       group.Proof,
				Issue:       group.Issue,
			})
		}
	}
	return registry
}

func loadGoArtifactRegistry(t testing.TB, repo string) map[string]*goCensusMarker {
	t.Helper()
	result := map[string]*goCensusMarker{}
	for _, group := range loadArtifactRegistry(t, repo).GoGroups {
		file := filepath.ToSlash(filepath.Clean(strings.TrimSpace(group.File)))
		if file == "." || len(group.Functions) == 0 || strings.TrimSpace(group.Disposition) == "" || strings.TrimSpace(group.Owner) == "" || len(group.Proofs) == 0 {
			t.Fatalf("incomplete Go routing registry group: %#v", group)
		}
		issue := strings.TrimSpace(group.Issue)
		if issue == "" {
			issue = "none"
		}
		for _, function := range group.Functions {
			function = strings.TrimSpace(function)
			if function == "" {
				t.Fatalf("Go routing registry group %s contains an empty function", file)
			}
			key := goArtifactRegistryKey(file, function)
			if _, exists := result[key]; exists {
				t.Fatalf("duplicate Go routing registry entry %s", key)
			}
			proofs := make([]string, 0, len(group.Proofs))
			for _, registeredProof := range group.Proofs {
				proof := strings.TrimSpace(registeredProof)
				if !pathQualifiedGoSymbol(proof) {
					t.Fatalf("Go routing registry proof %q for %s must be path-qualified", proof, key)
				}
				proofs = append(proofs, proof)
			}
			result[key] = &goCensusMarker{
				Disposition: strings.TrimSpace(group.Disposition),
				Issue:       issue,
				Owner:       strings.TrimSpace(group.Owner),
				Proofs:      proofs,
			}
		}
	}
	return result
}

func goArtifactRegistryKey(file, function string) string {
	return filepath.ToSlash(filepath.Clean(strings.TrimSpace(file))) + ":" + strings.TrimSpace(function)
}

func pathQualifiedGoSymbol(symbol string) bool {
	separator := strings.LastIndex(symbol, ":")
	if separator <= 0 || separator == len(symbol)-1 {
		return false
	}
	file := filepath.ToSlash(filepath.Clean(strings.TrimSpace(symbol[:separator])))
	name := strings.TrimSpace(symbol[separator+1:])
	return strings.HasSuffix(file, ".go") && file != "." && name != "" && name != "$self"
}

func indexGoSources(t testing.TB, repo string) goSourceIndex {
	t.Helper()
	index := goSourceIndex{
		Symbols:        map[string]goSourceSymbol{},
		ByPackage:      map[string]map[string][]string{},
		ByDirectory:    map[string]map[string][]string{},
		References:     map[string]map[string]struct{}{},
		FunctionProofs: map[string]struct{}{},
	}
	fset := token.NewFileSet()
	err := walkRepositoryGoFiles(repo, func(path string, raw []byte) error {
		parsed, err := parser.ParseFile(fset, path, raw, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repo, path)
		if err != nil {
			return err
		}
		file := filepath.ToSlash(rel)
		directory := filepath.ToSlash(filepath.Dir(rel))
		packageKey := directory + "|" + parsed.Name.Name
		imports := goFileImports(parsed)
		for _, decl := range parsed.Decls {
			switch declaration := decl.(type) {
			case *ast.FuncDecl:
				name := goFunctionSymbolName(declaration)
				index.addSourceSymbol(t, goSourceSymbol{
					Key:        goArtifactRegistryKey(file, name),
					File:       file,
					Package:    packageKey,
					Name:       name,
					Node:       declaration,
					Imports:    imports,
					IsFunction: true,
				})
			case *ast.GenDecl:
				if declaration.Tok != token.VAR && declaration.Tok != token.CONST {
					continue
				}
				for _, spec := range declaration.Specs {
					valueSpec, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, name := range valueSpec.Names {
						if name.Name == "_" {
							continue
						}
						index.addSourceSymbol(t, goSourceSymbol{
							Key:     goArtifactRegistryKey(file, name.Name),
							File:    file,
							Package: packageKey,
							Name:    name.Name,
							Node:    valueSpec,
							Imports: imports,
						})
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("index repository Go proof symbols: %v", err)
	}
	for key, symbol := range index.Symbols {
		index.References[key] = index.referencesFrom(symbol)
	}
	return index
}

func goFunctionSymbolName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := goReceiverTypeName(function.Recv.List[0].Type)
	if receiver == "" {
		return function.Name.Name
	}
	return receiver + "." + function.Name.Name
}

func goReceiverTypeName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return goReceiverTypeName(value.X)
	case *ast.IndexExpr:
		return goReceiverTypeName(value.X)
	case *ast.IndexListExpr:
		return goReceiverTypeName(value.X)
	default:
		return ""
	}
}

func (index *goSourceIndex) addSourceSymbol(t testing.TB, symbol goSourceSymbol) {
	t.Helper()
	if _, exists := index.Symbols[symbol.Key]; exists {
		t.Fatalf("duplicate Go source symbol %s", symbol.Key)
	}
	index.Symbols[symbol.Key] = symbol
	if index.ByPackage[symbol.Package] == nil {
		index.ByPackage[symbol.Package] = map[string][]string{}
	}
	index.ByPackage[symbol.Package][symbol.Name] = append(index.ByPackage[symbol.Package][symbol.Name], symbol.Key)
	directory := filepath.ToSlash(filepath.Dir(symbol.File))
	if index.ByDirectory[directory] == nil {
		index.ByDirectory[directory] = map[string][]string{}
	}
	index.ByDirectory[directory][symbol.Name] = append(index.ByDirectory[directory][symbol.Name], symbol.Key)
	if symbol.IsFunction {
		index.FunctionProofs[symbol.Key] = struct{}{}
	}
}

func goFileImports(file *ast.File) map[string]string {
	imports := map[string]string{}
	const modulePrefix = "github.com/division-sh/swarm/"
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || !strings.HasPrefix(path, modulePrefix) {
			continue
		}
		alias := filepath.Base(path)
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		if alias == "_" || alias == "." {
			continue
		}
		imports[alias] = filepath.ToSlash(strings.TrimPrefix(path, modulePrefix))
	}
	return imports
}

func (index goSourceIndex) referencesFrom(symbol goSourceSymbol) map[string]struct{} {
	references := map[string]struct{}{}
	parents := astParents(symbol.Node)
	ast.Inspect(symbol.Node, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.SelectorExpr:
			qualifier, ok := value.X.(*ast.Ident)
			if !ok {
				return true
			}
			directory, imported := symbol.Imports[qualifier.Name]
			if !imported {
				return true
			}
			for _, target := range index.ByDirectory[directory][value.Sel.Name] {
				references[target] = struct{}{}
			}
		case *ast.Ident:
			if selector, ok := parents[value].(*ast.SelectorExpr); ok && selector.Sel == value {
				return true
			}
			for _, target := range index.ByPackage[symbol.Package][value.Name] {
				candidate := index.Symbols[target]
				if value.Obj != nil && value.Obj.Decl != candidate.Node {
					continue
				}
				references[target] = struct{}{}
			}
		}
		return true
	})
	delete(references, symbol.Key)
	return references
}

func (index goSourceIndex) proofConsumes(proof, artifact string) bool {
	if proof == artifact {
		return true
	}
	if _, ok := index.FunctionProofs[proof]; !ok {
		return false
	}
	if _, ok := index.Symbols[artifact]; !ok {
		return false
	}
	seen := map[string]struct{}{proof: {}}
	queue := []string{proof}
	for len(queue) != 0 {
		current := queue[0]
		queue = queue[1:]
		for target := range index.References[current] {
			if target == artifact {
				return true
			}
			if _, ok := seen[target]; ok {
				continue
			}
			seen[target] = struct{}{}
			queue = append(queue, target)
		}
	}
	return false
}

func (index goSourceIndex) proofConsumers(artifact string) []string {
	reverse := map[string][]string{}
	for source, targets := range index.References {
		for target := range targets {
			reverse[target] = append(reverse[target], source)
		}
	}
	seen := map[string]struct{}{artifact: {}}
	queue := []string{artifact}
	var proofs []string
	for len(queue) != 0 {
		current := queue[0]
		queue = queue[1:]
		for _, source := range reverse[current] {
			if _, ok := seen[source]; ok {
				continue
			}
			seen[source] = struct{}{}
			queue = append(queue, source)
			symbol := index.Symbols[source]
			if symbol.IsFunction && strings.HasPrefix(symbol.Name, "Test") {
				proofs = append(proofs, source)
			}
		}
	}
	sort.Strings(proofs)
	return proofs
}

func walkRepositoryGoFiles(repo string, visit func(path string, raw []byte) error) error {
	return filepath.Walk(repo, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if repositoryGeneratedDir(repo, path) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return visit(path, raw)
	})
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

func routingStringExpressions(root ast.Node) []ast.Expr {
	parents := astParents(root)
	var expressions []ast.Expr
	ast.Inspect(root, func(node ast.Node) bool {
		expr, ok := node.(ast.Expr)
		if !ok {
			return true
		}
		if parent, nested := parents[expr].(*ast.BinaryExpr); nested {
			if _, parentIsConstant := constantString(parent); parentIsConstant {
				return true
			}
		}
		value, ok := constantString(expr)
		if ok && yamlTextContainsAuthoredRouting(value) {
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
			value, ok := constantString(expr)
			return ok && !yamlTextContainsAuthoredRouting(value)
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
	case "MergeMappingFile":
		for _, arg := range call.Args {
			value, ok := constantString(arg)
			if ok && yamlTextContainsAuthoredRouting(value) {
				return false
			}
		}
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
		if ok && yamlTextContainsAuthoredRouting(value) {
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
	return &goCensusMarker{Disposition: match[1], Issue: match[2], Owner: match[3], Proofs: []string{match[4]}}, len(matches)
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
			if repositoryGeneratedDir(repo, path) {
				return filepath.SkipDir
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

func repositoryGeneratedDir(repo, path string) bool {
	if path == repo {
		return false
	}
	rel, err := filepath.Rel(repo, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return false
	}
	switch filepath.ToSlash(filepath.Clean(rel)) {
	case ".git", "vendor", "node_modules", ".swarm", "data":
		return true
	default:
		return false
	}
}

func fileContainsAuthoredRouting(t testing.TB, path string) bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	for {
		var doc yaml.Node
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				return false
			}
			t.Fatalf("parse routing census candidate %s: %v", path, err)
		}
		for _, node := range doc.Content {
			if yamlNodeContainsAuthoredRouting(node) {
				return true
			}
		}
	}
}

func yamlTextContainsAuthoredRouting(text string) bool {
	decoder := yaml.NewDecoder(strings.NewReader(text))
	for {
		var doc yaml.Node
		if err := decoder.Decode(&doc); err != nil {
			return false
		}
		for _, node := range doc.Content {
			if yamlNodeContainsAuthoredRouting(node) {
				return true
			}
		}
	}
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
