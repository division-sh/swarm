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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

type artifactRegistry struct {
	Artifacts []artifactRegistryEntry  `yaml:"artifacts"`
	Groups    []artifactRegistryGroup  `yaml:"groups"`
	GoGroups  []goSourceExceptionGroup `yaml:"go_groups"`
}

type goSourceExceptionGroup struct {
	File        string   `yaml:"file"`
	Functions   []string `yaml:"functions"`
	Disposition string   `yaml:"disposition"`
	Owner       string   `yaml:"owner"`
	Proofs      []string `yaml:"proofs"`
	Issue       string   `yaml:"issue"`
}

type rawBundleSite struct {
	Name string
}

type artifactRegistryGroup struct {
	Roots       []ArtifactID `yaml:"roots"`
	Disposition string       `yaml:"disposition"`
	Owner       string       `yaml:"owner"`
	Proof       string       `yaml:"proof"`
	Issue       int          `yaml:"issue"`
}

type artifactRegistryEntry struct {
	Root        ArtifactID `yaml:"root"`
	Disposition string     `yaml:"disposition"`
	Owner       string     `yaml:"owner"`
	Proof       string     `yaml:"proof"`
	Issue       int        `yaml:"issue"`
}

func TestCheckedYAMLRoutingArtifactRegistryEqualsLiveCensus(t *testing.T) {
	repo := RepoRoot(t)
	registry := loadArtifactRegistry(t, repo)
	proofs := directExecutableArtifactProofs(t, repo)
	registered := map[ArtifactID]artifactRegistryEntry{}
	for _, entry := range registry.Artifacts {
		entry.Root = normalizeArtifactID(entry.Root)
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
		if _, err := os.Stat(filepath.Join(repo, filepath.FromSlash(string(entry.Root)), "package.yaml")); err != nil {
			t.Fatalf("artifact %s package: %v", entry.Root, err)
		}
		if !pathQualifiedGoSymbol(entry.Proof) {
			t.Fatalf("artifact %s proof %q must be path-qualified as *_test.go:TestXxx", entry.Root, entry.Proof)
		}
		declared, executable := proofs[entry.Proof]
		if !executable {
			t.Fatalf("artifact %s proof %s is not an executable Go test", entry.Root, entry.Proof)
		}
		if _, exact := declared[entry.Root]; !exact {
			t.Fatalf("artifact %s proof %s does not directly call canonicalrouting.Prove with its exact ArtifactID", entry.Root, entry.Proof)
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
			want := ArtifactID("cmd/hidden/testdata/" + name)
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
		if _, ok := live[ArtifactID("cmd/hidden/data/routing")]; !ok {
			t.Fatalf("repo-wide structural census skipped nested data directory: %#v", live)
		}
	})
}

func TestCanonicalRoutingProofsRequireDirectExecutableDeclaration(t *testing.T) {
	repo := t.TempDir()
	root := filepath.Join(repo, "fixture")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	const source = `package fixture
import (
    "testing"
    "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)
func proofHelper(t *testing.T) {
    canonicalrouting.Prove(t, canonicalrouting.ArtifactID("examples/routing/root-ingress"))
}
func TestUnrelated(t *testing.T) {
    canonicalrouting.Prove(t, canonicalrouting.ArtifactID("examples/routing/parent-connect"))
}
func TestExact(t *testing.T) {
    canonicalrouting.Prove(t, canonicalrouting.ArtifactID("examples/routing/root-ingress"))
}
func TestClosureOnly(t *testing.T) {
    hidden := func() {
        canonicalrouting.Prove(t, canonicalrouting.ArtifactID("examples/routing/root-ingress"))
    }
    _ = hidden
}
func TestConditionalOnly(t *testing.T) {
    if false {
        canonicalrouting.Prove(t, canonicalrouting.ArtifactID("examples/routing/root-ingress"))
    }
}
func TestWrongHandle(t *testing.T) {
    other := t
    canonicalrouting.Prove(other, canonicalrouting.ArtifactID("examples/routing/root-ingress"))
}
`
	if err := os.WriteFile(filepath.Join(root, "fixture_test.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	proofs := directExecutableArtifactProofs(t, repo)
	rootIngress := ArtifactID("examples/routing/root-ingress")
	if _, ok := proofs["fixture/fixture_test.go:proofHelper"]; ok {
		t.Fatal("ordinary helper was accepted as executable proof")
	}
	if _, ok := proofs["fixture/fixture_test.go:TestUnrelated"][rootIngress]; ok {
		t.Fatal("unrelated existing test was accepted as exact artifact proof")
	}
	if _, ok := proofs["fixture/fixture_test.go:TestMissing"]; ok {
		t.Fatal("missing test function was accepted as proof")
	}
	if _, ok := proofs["fixture/fixture_test.go:TestClosureOnly"][rootIngress]; ok {
		t.Fatal("Prove hidden in an uncalled closure was accepted as direct execution proof")
	}
	if _, ok := proofs["fixture/fixture_test.go:TestConditionalOnly"][rootIngress]; ok {
		t.Fatal("conditionally unreachable Prove was accepted as direct execution proof")
	}
	if _, ok := proofs["fixture/fixture_test.go:TestWrongHandle"][rootIngress]; ok {
		t.Fatal("Prove through a substituted testing handle was accepted as direct execution proof")
	}
	if _, ok := proofs["fixture/fixture_test.go:TestExact"][rootIngress]; !ok {
		t.Fatal("direct executable proof declaration was not indexed")
	}
}

func TestCanonicalRoutingOverlaysRejectRoutingAuthority(t *testing.T) {
	for name, source := range map[string]string{
		"pins":             "pins: {inputs: {events: []}}\n",
		"connect":          "connect: [{from: a.out, to: b.in}]\n",
		"emit":             "node: {emit: {event: result.ready}}\n",
		"subscription":     "node: {subscribes_to: [work.ready]}\n",
		"reply-carries":    "resolution: {mode: reply, replies_to: request}\ncarries: [request_id]\n",
		"false-broadcast":  "emit: {event: result.ready, broadcast: false}\n",
		"package-flows":    "flows: [{id: child, flow: child, mode: template}]\n",
		"package-bind":     "bind: {inputs: {work.ready: parent.work_ready}}\n",
		"package-requires": "requires: {inputs: [work.ready]}\n",
		"template-mode":    "name: child\nmode: template\n",
		"event-source":     "work.ready: {swarm: {source: external}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if err := routingOverlayError(source); err == nil {
				t.Fatalf("routing overlay %q was accepted", source)
			}
		})
	}
	if err := routingOverlayError("types:\n  Account:\n    score: number\n"); err != nil {
		t.Fatalf("non-routing overlay was rejected: %v", err)
	}
}

func TestCanonicalRoutingParserSnippetCannotMaterializeBundle(t *testing.T) {
	snippet := NewParserSnippet(t, "pins: {inputs: {events: []}}\n")
	var decoded map[string]any
	if err := snippet.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["pins"]; !ok {
		t.Fatalf("decoded parser snippet = %#v, want pins", decoded)
	}
	typeOfSnippet := reflect.TypeOf(snippet)
	if typeOfSnippet.NumMethod() != 1 || typeOfSnippet.Method(0).Name != "Decode" {
		methods := make([]string, 0, typeOfSnippet.NumMethod())
		for i := 0; i < typeOfSnippet.NumMethod(); i++ {
			methods = append(methods, typeOfSnippet.Method(i).Name)
		}
		t.Fatalf("parser-only snippet exposes methods %v, want Decode only", methods)
	}
}

func TestCanonicalRoutingRawPositiveBundleConstructionFailsClosed(t *testing.T) {
	for name, source := range map[string]string{
		"direct": `package fixture
func build() {
  write("package.yaml", "name: fixture")
  write("schema.yaml", "pins: {inputs: {events: [{source: external}]}}")
}`,
		"constant-indirected": `package fixture
const pinsKey = "pins"
const sourceKey = "source"
var bundleFiles = map[string]string{
  "package.yaml": "name: fixture",
  "schema.yaml": pinsKey + ": {inputs: {events: [{" + sourceKey + ": external}]}}",
}
func build() { materialize(bundleFiles) }`,
		"ceremonial-canonical-copy": `package fixture
func build(t T) {
  root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
  write(root, "package.yaml", "name: fixture")
  write(root, "schema.yaml", "pins: {inputs: {events: [{source: external}]}}")
}`,
	} {
		t.Run(name, func(t *testing.T) {
			if !prohibitedRawBundleSource([]byte(source)) {
				t.Fatalf("raw positive bundle construction %s escaped the closed API guard", name)
			}
		})
	}
}

func TestCanonicalRoutingRepositoryUsesClosedConstructionAPI(t *testing.T) {
	repo := RepoRoot(t)
	executableProofs := directExecutableArtifactProofs(t, repo)
	registryClassifications := registeredRawSourceExceptions(t, repo, executableProofs)
	var violations []string
	err := walkRepositoryGoFiles(repo, func(path string, raw []byte) error {
		rel, err := filepath.Rel(repo, path)
		if err != nil {
			return err
		}
		file := filepath.ToSlash(rel)
		if strings.HasPrefix(file, "internal/runtime/testfixtures/canonicalrouting/") {
			return nil
		}
		sites := prohibitedRawBundleSites(path, raw)
		classifications := sourceClassifiedRawBundleSites(t, path, raw, sites, executableProofs)
		for _, site := range sites {
			key := file + ":" + site.Name
			_, sourceClassified := classifications[site.Name]
			_, registryClassified := registryClassifications[key]
			if sourceClassified && registryClassified {
				violations = append(violations, key+" has duplicate source and registry declarations")
				continue
			}
			if !sourceClassified && !registryClassified {
				violations = append(violations, key)
			}
		}
		for _, retired := range []string{
			"canonicalrouting.CopyTree",
			"canonicalrouting.WriteFile",
			"canonicalrouting.ReplaceFile",
			"canonicalrouting.MergeMappingFile",
		} {
			if bytes.Contains(raw, []byte(retired)) {
				violations = append(violations, file+" uses retired "+retired)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("raw routing fixtures bypass the closed canonical API:\n%s", strings.Join(violations, "\n"))
	}
}

func sourceClassifiedRawBundleSites(
	t testing.TB,
	path string,
	raw []byte,
	sites []rawBundleSite,
	executableProofs map[string]map[ArtifactID]struct{},
) map[string]struct{} {
	t.Helper()
	result := map[string]struct{}{}
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, path, raw, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse raw source declarations %s: %v", path, err)
	}
	prohibited := map[string]struct{}{}
	for _, site := range sites {
		prohibited[site.Name] = struct{}{}
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if _, required := prohibited[function.Name.Name]; !required {
			continue
		}
		start := fset.Position(function.Pos()).Offset
		end := fset.Position(function.End()).Offset
		if function.Doc != nil {
			start = fset.Position(function.Doc.Pos()).Offset
		}
		if start < 0 || end > len(raw) || start >= end {
			continue
		}
		marker, found := routingSourceDeclaration(string(raw[start:end]))
		if !found {
			continue
		}
		switch marker["disposition"] {
		case "different-concept", "provider-ingress", "harness":
		default:
			t.Fatalf("raw source declaration %s:%s has unsupported disposition %q", path, function.Name.Name, marker["disposition"])
		}
		if marker["owner"] == "" || marker["issue"] == "" || marker["proof"] == "" {
			t.Fatalf("raw source declaration %s:%s is incomplete", path, function.Name.Name)
		}
		if !pathQualifiedGoSymbol(marker["proof"]) {
			t.Fatalf("raw source declaration %s:%s proof %q must be path-qualified as *_test.go:TestXxx", path, function.Name.Name, marker["proof"])
		}
		if _, exists := executableProofs[marker["proof"]]; !exists {
			t.Fatalf("raw source declaration %s:%s proof %q is not an executable test", path, function.Name.Name, marker["proof"])
		}
		result[function.Name.Name] = struct{}{}
	}
	return result
}

func registeredRawSourceExceptions(
	t testing.TB,
	repo string,
	executableProofs map[string]map[ArtifactID]struct{},
) map[string]struct{} {
	t.Helper()
	result := map[string]struct{}{}
	for _, group := range loadArtifactRegistry(t, repo).GoGroups {
		switch group.Disposition {
		case "different-concept", "provider-ingress", "harness":
		default:
			continue
		}
		if strings.TrimSpace(group.Owner) == "" || len(group.Proofs) == 0 {
			t.Fatalf("raw source registry entry has incomplete owner/proof: %#v", group)
		}
		file := filepath.ToSlash(filepath.Clean(strings.TrimSpace(group.File)))
		functions := goFunctionsInFile(t, filepath.Join(repo, filepath.FromSlash(file)))
		for _, proof := range group.Proofs {
			if !pathQualifiedGoSymbol(proof) {
				t.Fatalf("raw source registry entry %s proof %q must be path-qualified as *_test.go:TestXxx", file, proof)
			}
			if _, exists := executableProofs[proof]; !exists {
				t.Fatalf("raw source registry entry %s proof %q is not an executable test", file, proof)
			}
		}
		for _, function := range group.Functions {
			function = strings.TrimSpace(function)
			if _, exists := functions[function]; !exists {
				t.Fatalf("raw source registry entry %s names missing function %s", file, function)
			}
			key := file + ":" + function
			if _, duplicate := result[key]; duplicate {
				t.Fatalf("duplicate raw source registry entry %s", key)
			}
			result[key] = struct{}{}
		}
	}
	return result
}

func goFunctionsInFile(t testing.TB, path string) map[string]struct{} {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse raw source registry file %s: %v", path, err)
	}
	result := map[string]struct{}{}
	for _, declaration := range parsed.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			result[function.Name.Name] = struct{}{}
		}
	}
	return result
}

func routingSourceDeclaration(source string) (map[string]string, bool) {
	const prefix = "routing-example-census:"
	for _, line := range strings.Split(source, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "//"))
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, prefix)))
		if len(fields) == 0 {
			return map[string]string{}, true
		}
		result := map[string]string{"disposition": fields[0]}
		for _, field := range fields[1:] {
			key, value, ok := strings.Cut(field, "=")
			if ok {
				result[strings.TrimSpace(key)] = strings.TrimSpace(value)
			}
		}
		return result, true
	}
	return nil, false
}

func TestCanonicalRoutingTrackedSplitsRemainOpen(t *testing.T) {
	if os.Getenv("SWARM_CANONICAL_ROUTING_VERIFY_TRACKER_REMOTE") != "1" {
		t.Skip("remote tracker verification is enforced by CI")
	}
	token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
	if token == "" {
		t.Fatal("GITHUB_TOKEN is required for authoritative tracker verification")
	}
	issues := map[int]struct{}{}
	for _, entry := range loadArtifactRegistry(t, RepoRoot(t)).Artifacts {
		if entry.Disposition == "tracked-split" && entry.Issue > 0 {
			issues[entry.Issue] = struct{}{}
		}
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

func directExecutableArtifactProofs(t testing.TB, repo string) map[string]map[ArtifactID]struct{} {
	t.Helper()
	proofs := map[string]map[ArtifactID]struct{}{}
	fset := token.NewFileSet()
	err := walkRepositoryGoFiles(repo, func(path string, raw []byte) error {
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, raw, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repo, path)
		if err != nil {
			return err
		}
		for _, decl := range parsed.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || !executableGoTest(function) {
				continue
			}
			key := filepath.ToSlash(rel) + ":" + function.Name.Name
			proofs[key] = directProveCalls(function, parsed.Name.Name)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("index direct routing proofs: %v", err)
	}
	return proofs
}

func executableGoTest(function *ast.FuncDecl) bool {
	if function.Recv != nil || function.Body == nil || !goTestName(function.Name.Name) || function.Type.Results != nil {
		return false
	}
	if function.Type.Params == nil || len(function.Type.Params.List) != 1 || len(function.Type.Params.List[0].Names) != 1 {
		return false
	}
	star, ok := function.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "testing" && selector.Sel.Name == "T"
}

func goTestName(name string) bool {
	if !strings.HasPrefix(name, "Test") || len(name) == len("Test") {
		return false
	}
	next, _ := utf8RuneInString(name[len("Test"):])
	return !unicode.IsLower(next)
}

func utf8RuneInString(value string) (rune, int) {
	for _, item := range value {
		return item, 1
	}
	return 0, 0
}

func directProveCalls(function *ast.FuncDecl, packageName string) map[ArtifactID]struct{} {
	result := map[ArtifactID]struct{}{}
	testParameter := function.Type.Params.List[0].Names[0].Name
	for _, statement := range function.Body.List {
		expression, ok := statement.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := expression.X.(*ast.CallExpr)
		if !ok || len(call.Args) < 2 || !isCanonicalProveCall(call.Fun, packageName) {
			continue
		}
		handle, ok := call.Args[0].(*ast.Ident)
		if !ok || handle.Name != testParameter {
			continue
		}
		for _, argument := range call.Args[1:] {
			if id, ok := directArtifactID(argument); ok {
				result[id] = struct{}{}
			}
		}
	}
	return result
}

func isCanonicalProveCall(function ast.Expr, packageName string) bool {
	switch value := function.(type) {
	case *ast.Ident:
		return packageName == "canonicalrouting" && value.Name == "Prove"
	case *ast.SelectorExpr:
		pkg, ok := value.X.(*ast.Ident)
		return ok && pkg.Name == "canonicalrouting" && value.Sel.Name == "Prove"
	default:
		return false
	}
}

func directArtifactID(expression ast.Expr) (ArtifactID, bool) {
	if identifier, ok := expression.(*ast.Ident); ok {
		return canonicalArtifactConstant(identifier.Name)
	}
	if selector, ok := expression.(*ast.SelectorExpr); ok {
		pkg, packageOK := selector.X.(*ast.Ident)
		if packageOK && pkg.Name == "canonicalrouting" {
			return canonicalArtifactConstant(selector.Sel.Name)
		}
	}
	call, ok := expression.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 || !artifactIDConversion(call.Fun) {
		return "", false
	}
	literal, ok := call.Args[0].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	decoded, err := strconv.Unquote(literal.Value)
	return normalizeArtifactID(ArtifactID(decoded)), err == nil
}

func artifactIDConversion(function ast.Expr) bool {
	switch value := function.(type) {
	case *ast.Ident:
		return value.Name == "ArtifactID"
	case *ast.SelectorExpr:
		pkg, ok := value.X.(*ast.Ident)
		return ok && pkg.Name == "canonicalrouting" && value.Sel.Name == "ArtifactID"
	default:
		return false
	}
}

func canonicalArtifactConstant(name string) (ArtifactID, bool) {
	ids := map[string]ArtifactID{
		"RootIngress":             "examples/routing/root-ingress",
		"ParentConnect":           "examples/routing/parent-connect",
		"TemplateSelectExisting":  "examples/routing/template-select-existing",
		"TemplateSelectOrCreate":  "examples/routing/template-select-or-create",
		"TemplateReply":           "examples/routing/template-reply",
		"TemplateCreateMintedKey": "examples/routing/template-create-minted-key",
	}
	id, ok := ids[name]
	return id, ok
}

func pathQualifiedGoSymbol(symbol string) bool {
	separator := strings.LastIndex(symbol, ":")
	if separator <= 0 || separator == len(symbol)-1 {
		return false
	}
	file := filepath.ToSlash(filepath.Clean(strings.TrimSpace(symbol[:separator])))
	name := strings.TrimSpace(symbol[separator+1:])
	return strings.HasSuffix(file, "_test.go") && file != "." && goTestName(name)
}

func prohibitedRawBundleSource(raw []byte) bool {
	return prohibitedBundleFileSet(raw) && rawContainsRoutingVocabulary(raw) && rawMaterializesBundle(raw)
}

func prohibitedRawBundleSites(path string, raw []byte) []rawBundleSite {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, path, raw, 0)
	if err != nil {
		return []rawBundleSite{{Name: "<unparseable>"}}
	}
	var sites []rawBundleSite
	for _, declaration := range parsed.Decls {
		start := fset.Position(declaration.Pos()).Offset
		end := fset.Position(declaration.End()).Offset
		if start < 0 || end > len(raw) || start >= end {
			continue
		}
		source := raw[start:end]
		switch value := declaration.(type) {
		case *ast.FuncDecl:
			if prohibitedRawBundleSource(source) {
				sites = append(sites, rawBundleSite{Name: value.Name.Name})
			}
		case *ast.GenDecl:
			if (value.Tok == token.VAR || value.Tok == token.CONST) &&
				prohibitedBundleFileSet(source) && rawContainsRoutingVocabulary(raw) {
				sites = append(sites, rawBundleSite{Name: "package-scope"})
			}
		}
	}
	return sites
}

func prohibitedBundleFileSet(raw []byte) bool {
	source := string(raw)
	if !strings.Contains(source, "package.yaml") {
		return false
	}
	return strings.Contains(source, "schema.yaml") || strings.Contains(source, "nodes.yaml")
}

func rawMaterializesBundle(raw []byte) bool {
	source := string(raw)
	for _, token := range []string{
		"os.WriteFile(", "materialize(", "loadWorkflowTemp", "writeFile(", "write(",
		"FixtureFile(", "fixtureFile(", "BundleFile(", "bundleFile(",
	} {
		if strings.Contains(source, token) {
			return true
		}
	}
	return false
}

func rawContainsRoutingVocabulary(raw []byte) bool {
	source := string(raw)
	for _, token := range []string{
		`"pins"`, "pins:", `"connect"`, "connect:", `"resolution"`, "resolution:",
		`"instance"`, "instance:", `"subscribes_to"`, "subscribes_to:",
		`"subscriptions"`, "subscriptions:", `"event_handlers"`, "event_handlers:",
		`"emit"`, "emit:", `"source"`, "source: external", `"replies_to"`, "replies_to:",
		`"carries"`, "carries:",
	} {
		if strings.Contains(source, token) {
			return true
		}
	}
	return false
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

func normalizeArtifactID(root ArtifactID) ArtifactID {
	return ArtifactID(filepath.ToSlash(filepath.Clean(strings.TrimSpace(string(root)))))
}

func liveCheckedYAMLRoutingRoots(t testing.TB, repo string) map[ArtifactID]artifactRegistryEntry {
	t.Helper()
	live := map[ArtifactID]artifactRegistryEntry{}
	for _, bundleRoot := range outerPackageRoots(t, repo) {
		err := filepath.Walk(filepath.Join(repo, filepath.FromSlash(bundleRoot)), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
				return nil
			}
			if fileContainsAuthoredRouting(t, path) {
				id := ArtifactID(bundleRoot)
				live[id] = artifactRegistryEntry{Root: id}
			}
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

func difference[A any, B any](left map[ArtifactID]A, right map[ArtifactID]B) []string {
	var out []string
	for key := range left {
		if _, ok := right[key]; !ok {
			out = append(out, string(key))
		}
	}
	sort.Strings(out)
	return out
}
