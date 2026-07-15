package canonicalrouting

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

type artifactRegistry struct {
	Artifacts []artifactRegistryEntry `yaml:"artifacts"`
	Groups    []artifactRegistryGroup `yaml:"groups"`
	Sources   []sourceRegistryEntry   `yaml:"sources"`
}

type sourceRegistryEntry struct {
	ID          SourceID `yaml:"id"`
	File        string   `yaml:"file"`
	Function    string   `yaml:"function"`
	Disposition string   `yaml:"disposition"`
	Owner       string   `yaml:"owner"`
	Proof       string   `yaml:"proof"`
	Issue       int      `yaml:"issue"`
}

type goPackageBundleSource struct {
	File     string
	Function string
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

func TestCanonicalRoutingSourceProofsRequireDirectExactDeclaration(t *testing.T) {
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
	canonicalrouting.ProveSource(t, buildSource(t))
}
func build() {}
func other() {}
func buildSource(t *testing.T) canonicalrouting.SourceToken {
	return canonicalrouting.ExecuteSource(t,
		canonicalrouting.SourceID("fixture/fixture_test.go:buildSource"), func() { build() })
}
func otherSource(t *testing.T) canonicalrouting.SourceToken {
	return canonicalrouting.ExecuteSource(t,
		canonicalrouting.SourceID("fixture/fixture_test.go:otherSource"), func() { other() })
}
func TestUnrelated(t *testing.T) {
	canonicalrouting.ProveSource(t, otherSource(t))
}
func TestExact(t *testing.T) {
	canonicalrouting.ProveSource(t, buildSource(t))
}
func TestConditional(t *testing.T) {
	if false {
		canonicalrouting.ProveSource(t, buildSource(t))
	}
}
`
	if err := os.WriteFile(filepath.Join(root, "fixture_test.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	proofs := directExecutableSourceProofs(t, repo)
	sourceID := SourceID("fixture/fixture_test.go:buildSource")
	if _, ok := proofs["fixture/fixture_test.go:proofHelper"]; ok {
		t.Fatal("ordinary helper was accepted as executable source proof")
	}
	if _, ok := proofs["fixture/fixture_test.go:TestUnrelated"][sourceID]; ok {
		t.Fatal("unrelated test was accepted as exact source proof")
	}
	if _, ok := proofs["fixture/fixture_test.go:TestConditional"][sourceID]; ok {
		t.Fatal("conditional source declaration was accepted as direct proof")
	}
	if _, ok := proofs["fixture/fixture_test.go:TestExact"][sourceID]; !ok {
		t.Fatal("direct exact source proof declaration was not indexed")
	}
}

func TestCanonicalRoutingSourceProofRegistryEqualsDirectDeclarations(t *testing.T) {
	repo := RepoRoot(t)
	registry := loadArtifactRegistry(t, repo)
	proofs := directExecutableSourceProofs(t, repo)
	registered := map[SourceID]sourceRegistryEntry{}
	registeredFunctions := map[string]SourceID{}
	for _, entry := range registry.Sources {
		entry.ID = SourceID(strings.TrimSpace(string(entry.ID)))
		entry.File = filepath.ToSlash(filepath.Clean(strings.TrimSpace(entry.File)))
		entry.Function = strings.TrimSpace(entry.Function)
		entry.Proof = strings.TrimSpace(entry.Proof)
		if entry.ID == "" || entry.File == "." || entry.Function == "" || entry.Owner == "" || !pathQualifiedGoSymbol(entry.Proof) {
			t.Fatalf("incomplete routing source registry entry: %#v", entry)
		}
		wantID := SourceID(entry.File + ":" + entry.Function)
		if entry.ID != wantID {
			t.Fatalf("routing source %q must use function-derived ID %q", entry.ID, wantID)
		}
		switch entry.Disposition {
		case "parser-only", "different-concept", "provider-ingress", "harness":
		default:
			t.Fatalf("routing source %q has unsupported disposition %q", entry.ID, entry.Disposition)
		}
		if entry.Issue < 0 {
			t.Fatalf("routing source %q has invalid issue %d", entry.ID, entry.Issue)
		}
		if _, duplicate := registered[entry.ID]; duplicate {
			t.Fatalf("duplicate routing source ID %q", entry.ID)
		}
		functionKey := entry.File + ":" + entry.Function
		if previous, duplicate := registeredFunctions[functionKey]; duplicate {
			t.Fatalf("routing source function %q has duplicate IDs %q and %q", functionKey, previous, entry.ID)
		}
		if !goFunctionExists(t, filepath.Join(repo, filepath.FromSlash(entry.File)), entry.Function) {
			t.Fatalf("routing source %q names a missing source function", entry.ID)
		}
		declared, executable := proofs[entry.Proof]
		if !executable {
			t.Fatalf("routing source %q proof %q is not an executable test", entry.ID, entry.Proof)
		}
		if _, exact := declared[entry.ID]; !exact {
			t.Fatalf("routing source %q proof %q does not execute the exact source constructor", entry.ID, entry.Proof)
		}
		registered[entry.ID] = entry
		registeredFunctions[functionKey] = entry.ID
	}
	declared := map[SourceID]struct{}{}
	for _, ids := range proofs {
		for id := range ids {
			declared[id] = struct{}{}
		}
	}
	for id, owner := range registered {
		if _, exists := declared[id]; !exists {
			t.Fatalf("routing source %q owner %q has no direct executable proof declaration", id, owner)
		}
	}
	for id := range declared {
		if _, exists := registered[id]; !exists {
			t.Fatalf("direct executable proof declares stale or unregistered routing source %q", id)
		}
	}
}

func goFunctionExists(t testing.TB, path, name string) bool {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse routing source file %s: %v", path, err)
	}
	for _, declaration := range parsed.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == name {
			return true
		}
	}
	return false
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

func TestCanonicalRoutingPublicAPIHasNoGenericPositiveMutation(t *testing.T) {
	forbidden := map[string]struct{}{
		"ApplyOverlayMutation":  {},
		"ApplyNegativeMutation": {},
		"CopyTree":              {},
		"MergeMappingFile":      {},
		"ReplaceFile":           {},
		"WriteFile":             {},
	}
	packageRoot := filepath.Join(RepoRoot(t), "internal", "runtime", "testfixtures", "canonicalrouting")
	entries, err := os.ReadDir(packageRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(packageRoot, entry.Name())
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if _, prohibited := forbidden[function.Name.Name]; prohibited {
				t.Fatalf("generic positive mutation API %s survives in %s", function.Name.Name, path)
			}
		}
	}
}

func TestCanonicalRoutingParserSnippetDecodesOneDocument(t *testing.T) {
	snippet := NewParserSnippet(t, "pins: {inputs: {events: []}}\n")
	var decoded map[string]any
	if err := snippet.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["pins"]; !ok {
		t.Fatalf("decoded parser snippet = %#v, want pins", decoded)
	}
}

func TestCanonicalRoutingRawPositiveBundleConstructionFailsClosed(t *testing.T) {
	cases := map[string]map[string]string{
		"unknown-helper": {"fixture_test.go": `package fixture
func build() {
  saveArtifacts(map[string]string{
    "package.yaml": "name: fixture",
    "schema.yaml": "pins: {inputs: {events: [{source: external}]}}",
  })
}`},
		"cross-file": {
			"names_test.go": `package fixture
const packageFile = "package.yaml"
const schemaFile = "schema.yaml"
const pinsKey = "pins"
`,
			"bundle_test.go": `package fixture
var bundleFiles = map[string]string{
  packageFile: "name: fixture",
  schemaFile: pinsKey + ": {inputs: {events: [{source: external}]}}",
}
func build() { saveArtifacts(bundleFiles) }
`,
		},
		"split-helper-returns": {"fixture_test.go": `package fixture
func packageFiles() map[string]string {
  return map[string]string{"package.yaml": "name: fixture"}
}
func routingFiles() map[string]string {
  return map[string]string{"schema.yaml": "pins: {inputs: {events: [{source: external}]}}"}
}
func build() { saveArtifacts(mergeFiles(packageFiles(), routingFiles())) }
`},
		"external-webhook": {"fixture_test.go": `package fixture
var files = map[string]string{
  "package.yaml": "name: fixture",
  "schema.yaml": "pins: {inputs: {events: [{source: external webhook}]}}",
}
`},
		"external-webhook-case-whitespace": {"fixture_test.go": `package fixture
var files = map[string]string{
  "package.yaml": "name: fixture",
  "schema.yaml": "pins: {inputs: {events: [{source: '  ExTeRnAl webhook  '}]}}",
}
`},
		"byte-map-helper-returns": {"fixture_test.go": `package fixture
func packageFiles() map[string][]byte {
  return map[string][]byte{"package.yaml": []byte("name: fixture")}
}
func routingFiles() map[string][]byte {
  return map[string][]byte{"schema.yaml": []byte("pins: {inputs: {events: [{source: external}]}}")}
}
func build() { saveArtifacts(mergeFiles(packageFiles(), routingFiles())) }
`},
		"typed-artifact-slice": {"fixture_test.go": `package fixture
type artifact struct {
  name string
  contents []byte
}
func files() []artifact {
  return []artifact{
    {name: "package.yaml", contents: []byte("name: fixture")},
    {name: "schema.yaml", contents: []byte("pins: {inputs: {events: [{source: external}]}}")},
  }
}
func build() { saveArtifacts(files()) }
`},
		"string-helper-documents": {"fixture_test.go": `package fixture
func packageDocument() string { return "name: fixture" }
func routingDocument() string {
  return "pins: {inputs: {events: [{source: external}]}}"
}
func build() {
  writeFile("package.yaml", packageDocument())
  writeFile("schema.yaml", routingDocument())
}
`},
		"parser-selector-name-spoof": {"fixture_test.go": `package fixture
type parserOwner struct{}
func (parserOwner) NewParserSnippet(source string) {}
var canonicalrouting parserOwner
func build() {
  canonicalrouting.NewParserSnippet("package.yaml")
  canonicalrouting.NewParserSnippet("schema.yaml")
  canonicalrouting.NewParserSnippet("pins: {inputs: {events: [{source: external}]}}")
}
`},
		"parser-decode-materialize": {"fixture_test.go": `package fixture
import (
  "os"
  "testing"
  "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
  "gopkg.in/yaml.v3"
)
func build(t testing.TB) {
  packageSnippet := canonicalrouting.NewParserSnippet(t, "name: fixture")
  routingSnippet := canonicalrouting.NewParserSnippet(t, "pins: {inputs: {events: [{source: external}]}}")
  var packageDocument any
  var routingDocument any
  _ = packageSnippet.Decode(&packageDocument)
  _ = routingSnippet.Decode(&routingDocument)
  packageBytes, _ := yaml.Marshal(packageDocument)
  routingBytes, _ := yaml.Marshal(routingDocument)
  _ = os.WriteFile("package.yaml", packageBytes, 0o600)
  _ = os.WriteFile("schema.yaml", routingBytes, 0o600)
}
`},
	}
	for name, files := range cases {
		t.Run(name, func(t *testing.T) {
			if !goPackageContainsCompleteRoutingBundle(files) {
				t.Fatalf("complete raw routing bundle escaped package capability guard: %s", name)
			}
		})
	}
}

func TestCanonicalRoutingExternalSourceMatchesRuntimeSemantics(t *testing.T) {
	tests := map[string]bool{
		"external":             true,
		"external webhook":     true,
		"  ExTeRnAl webhook  ": true,
		"internal":             false,
		"webhook external":     false,
		"":                     false,
	}
	for source, want := range tests {
		if got := canonicalRoutingExternalSource(source); got != want {
			t.Errorf("canonicalRoutingExternalSource(%q) = %t, want runtime parity %t", source, got, want)
		}
	}
}

func TestCanonicalRoutingRepositoryUsesClosedConstructionAPI(t *testing.T) {
	repo := RepoRoot(t)
	var violations []string
	packages, err := repositoryGoPackages(repo)
	if err != nil {
		t.Fatal(err)
	}
	for key, files := range packages {
		if strings.HasPrefix(key, "internal/runtime/testfixtures/canonicalrouting:") {
			continue
		}
		for _, source := range completeGoPackageRoutingBundleSources(files) {
			violations = append(violations, key+":"+source.File+":"+source.Function+" contains a complete Go-authored routing bundle")
		}
	}
	err = walkRepositoryGoFiles(repo, func(path string, raw []byte) error {
		rel, err := filepath.Rel(repo, path)
		if err != nil {
			return err
		}
		file := filepath.ToSlash(rel)
		if strings.HasPrefix(file, "internal/runtime/testfixtures/canonicalrouting/") {
			return nil
		}
		for _, retired := range []string{
			"canonicalrouting.ApplyOverlayMutation",
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

func TestCanonicalRoutingTrackedSplitsRemainOpen(t *testing.T) {
	if os.Getenv("SWARM_CANONICAL_ROUTING_VERIFY_TRACKER_REMOTE") != "1" {
		t.Skip("remote tracker verification is enforced by CI")
	}
	token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
	if token == "" {
		t.Fatal("GITHUB_TOKEN is required for authoritative tracker verification")
	}
	issues := map[int]struct{}{}
	registry := loadArtifactRegistry(t, RepoRoot(t))
	for _, entry := range registry.Artifacts {
		if entry.Disposition == "tracked-split" && entry.Issue > 0 {
			issues[entry.Issue] = struct{}{}
		}
	}
	for _, source := range registry.Sources {
		if source.Issue > 0 {
			issues[source.Issue] = struct{}{}
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

func directExecutableSourceProofs(t testing.TB, repo string) map[string]map[SourceID]struct{} {
	t.Helper()
	packages, err := repositoryGoPackages(repo)
	if err != nil {
		t.Fatalf("index routing source packages: %v", err)
	}
	constructors := directSourceConstructors(t, packages)
	proofs := map[string]map[SourceID]struct{}{}
	for packageKey, files := range packages {
		paths := make([]string, 0, len(files))
		for path := range files {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, file := range paths {
			if !strings.HasSuffix(file, "_test.go") {
				continue
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), file, files[file], 0)
			if err != nil {
				t.Fatalf("parse source proof file %s: %v", file, err)
			}
			for _, declaration := range parsed.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok || !executableGoTest(function) {
					continue
				}
				key := filepath.ToSlash(file) + ":" + function.Name.Name
				proofs[key] = directSourceConstructorProofs(function, packageKey, constructors)
			}
		}
	}
	return proofs
}

func directSourceConstructors(t testing.TB, packages map[string]map[string]string) map[string]SourceID {
	t.Helper()
	constructors := map[string]SourceID{}
	for packageKey, files := range packages {
		for file, source := range files {
			parsed, err := parser.ParseFile(token.NewFileSet(), file, source, 0)
			if err != nil {
				t.Fatalf("parse source constructor file %s: %v", file, err)
			}
			for _, declaration := range parsed.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok {
					continue
				}
				id, ok := directSourceConstructorID(function, filepath.ToSlash(file), parsed.Name.Name)
				if !ok {
					continue
				}
				constructors[packageKey+":"+function.Name.Name] = id
			}
		}
	}
	return constructors
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

func directSourceConstructorProofs(
	function *ast.FuncDecl,
	packageKey string,
	constructors map[string]SourceID,
) map[SourceID]struct{} {
	result := map[SourceID]struct{}{}
	testParameter := function.Type.Params.List[0].Names[0].Name
	for _, statement := range function.Body.List {
		expression, ok := statement.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := expression.X.(*ast.CallExpr)
		if !ok || len(call.Args) < 2 || !isCanonicalSourceProveCall(call.Fun, packageNameForKey(packageKey)) {
			continue
		}
		handle, ok := call.Args[0].(*ast.Ident)
		if !ok || handle.Name != testParameter {
			continue
		}
		for _, argument := range call.Args[1:] {
			constructorCall, ok := argument.(*ast.CallExpr)
			if !ok || len(constructorCall.Args) != 1 {
				continue
			}
			constructor, ok := constructorCall.Fun.(*ast.Ident)
			if !ok {
				continue
			}
			constructorHandle, ok := constructorCall.Args[0].(*ast.Ident)
			if !ok || constructorHandle.Name != testParameter {
				continue
			}
			if id, exists := constructors[packageKey+":"+constructor.Name]; exists {
				result[id] = struct{}{}
			}
		}
	}
	return result
}

func packageNameForKey(packageKey string) string {
	separator := strings.LastIndex(packageKey, ":")
	if separator < 0 {
		return packageKey
	}
	return packageKey[separator+1:]
}

func directSourceConstructorID(function *ast.FuncDecl, file, packageName string) (SourceID, bool) {
	if function.Recv != nil || function.Body == nil || function.Type.Results == nil ||
		len(function.Type.Results.List) != 1 || !sourceTokenResult(function.Type.Results.List[0].Type, packageName) ||
		function.Type.Params == nil || len(function.Type.Params.List) != 1 ||
		len(function.Type.Params.List[0].Names) != 1 {
		return "", false
	}
	testParameter := function.Type.Params.List[0].Names[0].Name
	for _, statement := range function.Body.List {
		returned, ok := statement.(*ast.ReturnStmt)
		if !ok || len(returned.Results) != 1 {
			continue
		}
		call, ok := returned.Results[0].(*ast.CallExpr)
		if !ok || len(call.Args) != 3 || !isCanonicalExecuteSourceCall(call.Fun, packageName) {
			continue
		}
		handle, handleOK := call.Args[0].(*ast.Ident)
		id, idOK := directSourceID(call.Args[1])
		constructor, constructorOK := call.Args[2].(*ast.FuncLit)
		if !handleOK || handle.Name != testParameter || !idOK || !constructorOK ||
			constructor.Type.Params == nil || len(constructor.Type.Params.List) != 0 ||
			constructor.Type.Results != nil {
			continue
		}
		want := SourceID(filepath.ToSlash(file) + ":" + function.Name.Name)
		if id != want {
			return "", false
		}
		return id, true
	}
	return "", false
}

func sourceTokenResult(expression ast.Expr, packageName string) bool {
	switch value := expression.(type) {
	case *ast.Ident:
		return packageName == "canonicalrouting" && value.Name == "SourceToken"
	case *ast.SelectorExpr:
		pkg, ok := value.X.(*ast.Ident)
		return ok && pkg.Name == "canonicalrouting" && value.Sel.Name == "SourceToken"
	default:
		return false
	}
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

func isCanonicalSourceProveCall(function ast.Expr, packageName string) bool {
	switch value := function.(type) {
	case *ast.Ident:
		return packageName == "canonicalrouting" && value.Name == "ProveSource"
	case *ast.SelectorExpr:
		pkg, ok := value.X.(*ast.Ident)
		return ok && pkg.Name == "canonicalrouting" && value.Sel.Name == "ProveSource"
	default:
		return false
	}
}

func isCanonicalExecuteSourceCall(function ast.Expr, packageName string) bool {
	switch value := function.(type) {
	case *ast.Ident:
		return packageName == "canonicalrouting" && value.Name == "ExecuteSource"
	case *ast.SelectorExpr:
		pkg, ok := value.X.(*ast.Ident)
		return ok && pkg.Name == "canonicalrouting" && value.Sel.Name == "ExecuteSource"
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

func directSourceID(expression ast.Expr) (SourceID, bool) {
	call, ok := expression.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 || !sourceIDConversion(call.Fun) {
		return "", false
	}
	literal, ok := call.Args[0].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	decoded, err := strconv.Unquote(literal.Value)
	return SourceID(strings.TrimSpace(decoded)), err == nil
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

func sourceIDConversion(function ast.Expr) bool {
	switch value := function.(type) {
	case *ast.Ident:
		return value.Name == "SourceID"
	case *ast.SelectorExpr:
		pkg, ok := value.X.(*ast.Ident)
		return ok && pkg.Name == "canonicalrouting" && value.Sel.Name == "SourceID"
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
		"FanInStream":             "examples/routing/fan-in/stream",
		"FanInBarrier":            "examples/routing/fan-in/barrier",
		"HarnessInjection":        "examples/routing/harness-injection",
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

func repositoryGoPackages(repo string) (map[string]map[string]string, error) {
	packages := map[string]map[string]string{}
	err := walkRepositoryGoFiles(repo, func(path string, raw []byte) error {
		parsed, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repo, path)
		if err != nil {
			return err
		}
		file := filepath.ToSlash(rel)
		dir := filepath.ToSlash(filepath.Dir(rel))
		key := dir + ":" + parsed.Name.Name
		if packages[key] == nil {
			packages[key] = map[string]string{}
		}
		packages[key][file] = string(raw)
		return nil
	})
	return packages, err
}

func goPackageContainsCompleteRoutingBundle(files map[string]string) bool {
	return len(completeGoPackageRoutingBundleSources(files)) != 0
}

func completeGoPackageRoutingBundleSources(files map[string]string) []goPackageBundleSource {
	fset := token.NewFileSet()
	parsedFiles := make([]*ast.File, 0, len(files))
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		parsed, err := parser.ParseFile(fset, path, files[path], 0)
		if err != nil {
			return []goPackageBundleSource{{File: path, Function: "<unparseable>"}}
		}
		parsedFiles = append(parsedFiles, parsed)
	}
	if len(parsedFiles) == 0 {
		return nil
	}

	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{},
	}
	config := types.Config{Importer: importer.Default(), Error: func(error) {}}
	_, _ = config.Check("canonicalrouting/sourcecensus", fset, parsedFiles, info)

	// Aggregate every compiler-resolved string across the package. Storage types,
	// helper boundaries, and writer calls are not semantic boundaries for a
	// complete positive routing bundle.
	values := map[string]struct{}{}
	routingEvidenceFiles := map[string]struct{}{}
	for _, file := range parsedFiles {
		for value := range compilerResolvedStrings(file, info) {
			values[value] = struct{}{}
			if yamlTextContainsCanonicalRoutingAuthority(value) {
				routingEvidenceFiles[filepath.ToSlash(fset.Position(file.Pos()).Filename)] = struct{}{}
			}
		}
	}

	hasPackageFile := false
	hasSchemaFile := false
	hasRoutingAuthority := false
	for value := range values {
		hasPackageFile = hasPackageFile || strings.Contains(value, "package.yaml")
		hasSchemaFile = hasSchemaFile || strings.Contains(value, "schema.yaml") || strings.Contains(value, "nodes.yaml")
		hasRoutingAuthority = hasRoutingAuthority || yamlTextContainsCanonicalRoutingAuthority(value)
	}
	if !hasPackageFile || !hasSchemaFile || !hasRoutingAuthority {
		return nil
	}
	evidenceFiles := make([]string, 0, len(routingEvidenceFiles))
	for path := range routingEvidenceFiles {
		evidenceFiles = append(evidenceFiles, path)
	}
	sort.Strings(evidenceFiles)
	result := make([]goPackageBundleSource, 0, len(evidenceFiles))
	for _, path := range evidenceFiles {
		result = append(result, goPackageBundleSource{File: path, Function: "package-scope"})
	}
	return result
}

func compilerResolvedStrings(node ast.Node, info *types.Info) map[string]struct{} {
	values := map[string]struct{}{}
	ast.Inspect(node, func(candidate ast.Node) bool {
		expression, ok := candidate.(ast.Expr)
		if !ok {
			return true
		}
		typed, exists := info.Types[expression]
		if exists && typed.Value != nil && typed.Value.Kind() == constant.String {
			values[constant.StringVal(typed.Value)] = struct{}{}
		}
		return true
	})
	return values
}

func yamlTextContainsCanonicalRoutingAuthority(text string) bool {
	decoder := yaml.NewDecoder(strings.NewReader(text))
	for {
		var doc yaml.Node
		if err := decoder.Decode(&doc); err != nil {
			return false
		}
		for _, node := range doc.Content {
			if yamlNodeContainsCanonicalRoutingAuthority(node) {
				return true
			}
		}
	}
}

func yamlNodeContainsCanonicalRoutingAuthority(node *yaml.Node) bool {
	return yamlNodeContainsCanonicalRoutingAuthorityAt(node, nil)
}

func yamlNodeContainsCanonicalRoutingAuthorityAt(node *yaml.Node, path []string) bool {
	if node == nil {
		return false
	}
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			key := strings.TrimSpace(node.Content[index].Value)
			value := node.Content[index+1]
			switch key {
			case "connect":
				if value.Kind == yaml.SequenceNode {
					return true
				}
			case "resolution":
				if value.Kind == yaml.MappingNode {
					return true
				}
			case "source":
				if value.Kind == yaml.ScalarNode && canonicalRoutingExternalSource(value.Value) && canonicalRoutingSourcePath(path) {
					return true
				}
			}
			if yamlNodeContainsCanonicalRoutingAuthorityAt(value, append(path, key)) {
				return true
			}
		}
		return false
	}
	for _, child := range node.Content {
		if yamlNodeContainsCanonicalRoutingAuthorityAt(child, path) {
			return true
		}
	}
	return false
}

func canonicalRoutingSourcePath(path []string) bool {
	for _, part := range path {
		if part == "swarm" {
			return true
		}
	}
	for index := 0; index+2 < len(path); index++ {
		if path[index] == "pins" && path[index+1] == "inputs" && path[index+2] == "events" {
			return true
		}
	}
	return false
}

func canonicalRoutingExternalSource(source string) bool {
	// Keep this mirror exact with semanticview.eventMetadataExternalSource.
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(source)), "external")
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
