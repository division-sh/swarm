package conformance

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type conformanceRepoSnapshotFile struct {
	Path string
	Raw  []byte
}

type conformanceRepoSnapshotIO struct {
	walkDir  func(string, fs.WalkDirFunc) error
	readFile func(string) ([]byte, error)
	relPath  func(string, string) (string, error)
}

type conformanceRepoSnapshot struct {
	files          []conformanceRepoSnapshotFile
	filesByPath    map[string][]byte
	goTests        map[string]bool
	matches        sync.Map
	pathMatchCache sync.Map
}

type conformanceRepoSnapshotMatch struct {
	once    sync.Once
	matches []string
}

type conformanceRepoSnapshotPathMatch struct {
	once    sync.Once
	matches bool
}

type conformanceRepoSnapshotCacheEntry struct {
	once     sync.Once
	snapshot *conformanceRepoSnapshot
	err      error
}

type conformanceRepoSnapshotCache struct {
	mu      sync.Mutex
	entries map[string]*conformanceRepoSnapshotCacheEntry
	build   func(string) (*conformanceRepoSnapshot, error)
}

var conformanceRepoSnapshotBuilds atomic.Int64

var canonicalConformanceRepoSnapshots = newConformanceRepoSnapshotCache(func(root string) (*conformanceRepoSnapshot, error) {
	conformanceRepoSnapshotBuilds.Add(1)
	return buildConformanceRepoSnapshot(root, defaultConformanceRepoSnapshotIO())
})

func newConformanceRepoSnapshotCache(build func(string) (*conformanceRepoSnapshot, error)) *conformanceRepoSnapshotCache {
	return &conformanceRepoSnapshotCache{
		entries: map[string]*conformanceRepoSnapshotCacheEntry{},
		build:   build,
	}
}

func (c *conformanceRepoSnapshotCache) load(root string) (*conformanceRepoSnapshot, error) {
	key, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil, fmt.Errorf("resolve repository snapshot root %q: %w", root, err)
	}

	c.mu.Lock()
	entry := c.entries[key]
	if entry == nil {
		entry = &conformanceRepoSnapshotCacheEntry{}
		c.entries[key] = entry
	}
	c.mu.Unlock()

	entry.once.Do(func() {
		entry.snapshot, entry.err = c.build(key)
		if entry.err == nil && entry.snapshot == nil {
			entry.err = fmt.Errorf("repository snapshot builder returned nil without an error")
		}
	})
	return entry.snapshot, entry.err
}

func defaultConformanceRepoSnapshotIO() conformanceRepoSnapshotIO {
	return conformanceRepoSnapshotIO{
		walkDir:  filepath.WalkDir,
		readFile: os.ReadFile,
		relPath:  filepath.Rel,
	}
}

func buildConformanceRepoSnapshot(root string, source conformanceRepoSnapshotIO) (*conformanceRepoSnapshot, error) {
	if source.walkDir == nil || source.readFile == nil || source.relPath == nil {
		return nil, fmt.Errorf("repository snapshot source is incomplete")
	}

	files := make([]conformanceRepoSnapshotFile, 0)
	filesByPath := map[string][]byte{}
	goTests := map[string]bool{}
	fset := token.NewFileSet()

	err := source.walkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("traverse %s: %w", path, walkErr)
		}
		if entry == nil {
			return fmt.Errorf("traverse %s: missing directory entry", path)
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !conformanceRepoSnapshotScannableFile(path) {
			return nil
		}
		rel, err := source.relPath(root, path)
		if err != nil {
			return fmt.Errorf("derive repository-relative path for %s: %w", path, err)
		}
		rel = filepath.ToSlash(filepath.Clean(rel))
		raw, err := source.readFile(path)
		if err != nil {
			return fmt.Errorf("read scannable repository file %s: %w", rel, err)
		}
		raw = append([]byte(nil), raw...)
		files = append(files, conformanceRepoSnapshotFile{
			Path: rel,
			Raw:  raw,
		})
		filesByPath[rel] = raw

		if strings.HasSuffix(rel, "_test.go") {
			file, err := parser.ParseFile(fset, path, raw, 0)
			if err != nil {
				return fmt.Errorf("parse Go test file %s: %w", rel, err)
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Test") {
					goTests[fn.Name.Name] = true
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("acquire complete repository snapshot rooted at %s: %w", root, err)
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return &conformanceRepoSnapshot{
		files:       files,
		filesByPath: filesByPath,
		goTests:     goTests,
	}, nil
}

func mustConformanceRepoSnapshot(t *testing.T, root string) *conformanceRepoSnapshot {
	t.Helper()
	snapshot, err := canonicalConformanceRepoSnapshots.load(root)
	if err != nil {
		t.Fatalf("acquire conformance repository snapshot: %v", err)
	}
	return snapshot
}

func conformanceGoTestNames(t *testing.T, root string) map[string]bool {
	t.Helper()
	return mustConformanceRepoSnapshot(t, root).goTestNames()
}

func (s *conformanceRepoSnapshot) goTestNames() map[string]bool {
	out := make(map[string]bool, len(s.goTests))
	for name, present := range s.goTests {
		out[name] = present
	}
	return out
}

func (s *conformanceRepoSnapshot) file(path string) ([]byte, error) {
	cleaned := filepath.ToSlash(filepath.Clean(path))
	raw, ok := s.filesByPath[cleaned]
	if !ok {
		return nil, fmt.Errorf("required path %s is unavailable in the complete repository snapshot", cleaned)
	}
	return append([]byte(nil), raw...), nil
}

func (s *conformanceRepoSnapshot) fileList() []conformanceRepoSnapshotFile {
	out := make([]conformanceRepoSnapshotFile, len(s.files))
	for i, file := range s.files {
		out[i] = conformanceRepoSnapshotFile{Path: file.Path, Raw: append([]byte(nil), file.Raw...)}
	}
	return out
}

func (s *conformanceRepoSnapshot) matchingFiles(pattern string, re *regexp.Regexp) []string {
	loaded, _ := s.matches.LoadOrStore(pattern, &conformanceRepoSnapshotMatch{})
	entry := loaded.(*conformanceRepoSnapshotMatch)
	entry.once.Do(func() {
		for _, file := range s.files {
			if re.Match(file.Raw) {
				entry.matches = append(entry.matches, file.Path)
			}
		}
		sort.Strings(entry.matches)
	})
	return append([]string(nil), entry.matches...)
}

func (s *conformanceRepoSnapshot) pathMatches(path, pattern string, re *regexp.Regexp) (bool, error) {
	cleaned := filepath.ToSlash(filepath.Clean(path))
	raw, ok := s.filesByPath[cleaned]
	if !ok {
		return false, fmt.Errorf("required path %s is unavailable in the complete repository snapshot", cleaned)
	}
	key := pattern + "\x00" + cleaned
	loaded, _ := s.pathMatchCache.LoadOrStore(key, &conformanceRepoSnapshotPathMatch{})
	entry := loaded.(*conformanceRepoSnapshotPathMatch)
	entry.once.Do(func() { entry.matches = re.Match(raw) })
	return entry.matches, nil
}

func conformanceRepoSnapshotScannableFile(path string) bool {
	switch filepath.Ext(path) {
	case ".go", ".yaml", ".yml", ".json", ".md":
		return true
	default:
		return false
	}
}

func TestBuildConformanceRepoSnapshotFailsClosed(t *testing.T) {
	root := t.TempDir()
	writeConformanceSnapshotFixture(t, root, "fixture_test.go", "package fixture\nfunc TestFixture(t *testing.T) {}\n")

	tests := []struct {
		name   string
		source func(error) conformanceRepoSnapshotIO
		want   string
	}{
		{
			name: "root walk failure",
			source: func(sentinel error) conformanceRepoSnapshotIO {
				source := defaultConformanceRepoSnapshotIO()
				source.walkDir = func(string, fs.WalkDirFunc) error { return sentinel }
				return source
			},
			want: "root walk failed",
		},
		{
			name: "callback traversal failure",
			source: func(sentinel error) conformanceRepoSnapshotIO {
				source := defaultConformanceRepoSnapshotIO()
				source.walkDir = func(root string, callback fs.WalkDirFunc) error {
					return callback(filepath.Join(root, "broken.go"), nil, sentinel)
				}
				return source
			},
			want: "traverse",
		},
		{
			name: "relative path failure",
			source: func(sentinel error) conformanceRepoSnapshotIO {
				source := defaultConformanceRepoSnapshotIO()
				source.relPath = func(string, string) (string, error) { return "", sentinel }
				return source
			},
			want: "derive repository-relative path",
		},
		{
			name: "scannable file read failure",
			source: func(sentinel error) conformanceRepoSnapshotIO {
				source := defaultConformanceRepoSnapshotIO()
				source.readFile = func(string) ([]byte, error) { return nil, sentinel }
				return source
			},
			want: "read scannable repository file fixture_test.go",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sentinel := fmt.Errorf("%s", tc.want)
			snapshot, err := buildConformanceRepoSnapshot(root, tc.source(sentinel))
			if snapshot != nil {
				t.Fatalf("snapshot = %#v, want nil after acquisition failure", snapshot)
			}
			if err == nil || !errors.Is(err, sentinel) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want wrapped sentinel containing %q", err, tc.want)
			}
		})
	}
}

func TestBuildConformanceRepoSnapshotRejectsInvalidGoTest(t *testing.T) {
	root := t.TempDir()
	writeConformanceSnapshotFixture(t, root, "broken_test.go", "package fixture\nfunc TestBroken(")

	snapshot, err := buildConformanceRepoSnapshot(root, defaultConformanceRepoSnapshotIO())
	if snapshot != nil {
		t.Fatalf("snapshot = %#v, want nil after Go parse failure", snapshot)
	}
	if err == nil || !strings.Contains(err.Error(), "parse Go test file broken_test.go") {
		t.Fatalf("error = %v, want fail-closed Go parse diagnostic", err)
	}
}

func TestBuildConformanceRepoSnapshotReadsOnlyScannableInputs(t *testing.T) {
	root := t.TempDir()
	writeConformanceSnapshotFixture(t, root, "fixture.go", "package fixture\n")
	writeConformanceSnapshotFixture(t, root, "artifact.zip", "not a semantic input")

	source := defaultConformanceRepoSnapshotIO()
	readFile := source.readFile
	relPath := source.relPath
	nonScannableReads := 0
	nonScannableRelPaths := 0
	source.relPath = func(root, path string) (string, error) {
		if filepath.Ext(path) == ".zip" {
			nonScannableRelPaths++
			return "", errors.New("irrelevant path must not be derived")
		}
		return relPath(root, path)
	}
	source.readFile = func(path string) ([]byte, error) {
		if filepath.Ext(path) == ".zip" {
			nonScannableReads++
			return nil, errors.New("irrelevant file must not be read")
		}
		return readFile(path)
	}
	snapshot, err := buildConformanceRepoSnapshot(root, source)
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	if nonScannableReads != 0 {
		t.Fatalf("non-scannable reads = %d, want 0", nonScannableReads)
	}
	if nonScannableRelPaths != 0 {
		t.Fatalf("non-scannable relative-path derivations = %d, want 0", nonScannableRelPaths)
	}
	if _, err := snapshot.file("artifact.zip"); err == nil {
		t.Fatal("non-scannable artifact unexpectedly became a semantic snapshot input")
	}
	if _, err := snapshot.file("fixture.go"); err != nil {
		t.Fatalf("scannable fixture unavailable: %v", err)
	}
}

func TestConformanceRepoSnapshotSeparatesCanonicalAndRouteExclusions(t *testing.T) {
	root := t.TempDir()
	writeConformanceSnapshotFixture(t, root, "tmp/hidden_test.go", "package fixture\n// route-needle\nfunc TestHiddenInTmp(t *testing.T) {}\n")
	writeConformanceSnapshotFixture(t, root, "test-results/hidden_test.go", "package fixture\n// route-needle\nfunc TestHiddenInResults(t *testing.T) {}\n")
	writeConformanceSnapshotFixture(t, root, "visible.go", "package fixture\n// route-needle\n")

	snapshot, err := buildConformanceRepoSnapshot(root, defaultConformanceRepoSnapshotIO())
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	names := snapshot.goTestNames()
	for _, name := range []string{"TestHiddenInTmp", "TestHiddenInResults"} {
		if !names[name] {
			t.Fatalf("canonical Go-test index omitted %s", name)
		}
	}

	corpus := &routeAuthorityDriftValidationCorpus{snapshot: snapshot, overlays: map[string][]byte{}}
	matches := routeAuthorityDriftMatchingFiles(corpus, "route-needle", regexp.MustCompile("route-needle"))
	if len(matches) != 1 || matches[0] != "visible.go" {
		t.Fatalf("route matches = %#v, want only visible.go", matches)
	}
}

func TestConformanceRepoSnapshotCacheRetainsBuildError(t *testing.T) {
	sentinel := errors.New("snapshot unavailable")
	builds := 0
	cache := newConformanceRepoSnapshotCache(func(string) (*conformanceRepoSnapshot, error) {
		builds++
		return nil, sentinel
	})

	for i := 0; i < 3; i++ {
		snapshot, err := cache.load(t.TempDir())
		if snapshot != nil || !errors.Is(err, sentinel) {
			t.Fatalf("load %d = (%#v, %v), want retained sentinel error", i, snapshot, err)
		}
	}
	if builds != 3 {
		t.Fatalf("builds = %d, want one retained build error per distinct root", builds)
	}

	root := t.TempDir()
	builds = 0
	cache = newConformanceRepoSnapshotCache(func(string) (*conformanceRepoSnapshot, error) {
		builds++
		return nil, sentinel
	})
	var first error
	for i := 0; i < 3; i++ {
		snapshot, err := cache.load(root)
		if snapshot != nil || !errors.Is(err, sentinel) {
			t.Fatalf("same-root load %d = (%#v, %v), want retained sentinel error", i, snapshot, err)
		}
		if i == 0 {
			first = err
		} else if err != first {
			t.Fatalf("same-root load %d returned a different retained error", i)
		}
	}
	if builds != 1 {
		t.Fatalf("same-root builds = %d, want 1", builds)
	}
}

func TestConformanceRepoSnapshotViewsAreImmutable(t *testing.T) {
	root := t.TempDir()
	writeConformanceSnapshotFixture(t, root, "fixture_test.go", "package fixture\n// needle\nfunc TestFixture(t *testing.T) {}\n")
	writeConformanceSnapshotFixture(t, root, "notes.md", "needle\n")
	snapshot, err := buildConformanceRepoSnapshot(root, defaultConformanceRepoSnapshotIO())
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}

	files := snapshot.fileList()
	files[0].Path = "mutated"
	files[0].Raw[0] = 'X'
	if got := snapshot.fileList()[0]; got.Path == "mutated" || got.Raw[0] == 'X' {
		t.Fatalf("file list mutation leaked into snapshot: %#v", got)
	}

	raw, err := snapshot.file("notes.md")
	if err != nil {
		t.Fatalf("lookup notes.md: %v", err)
	}
	raw[0] = 'X'
	pristine, err := snapshot.file("notes.md")
	if err != nil || pristine[0] == 'X' {
		t.Fatalf("file byte mutation leaked into snapshot: %q, %v", pristine, err)
	}

	names := snapshot.goTestNames()
	delete(names, "TestFixture")
	names["TestInjected"] = true
	pristineNames := snapshot.goTestNames()
	if !pristineNames["TestFixture"] || pristineNames["TestInjected"] {
		t.Fatalf("Go test map mutation leaked into snapshot: %#v", pristineNames)
	}

	re := regexp.MustCompile("needle")
	matches := snapshot.matchingFiles("needle", re)
	matches[0] = "mutated"
	if got := snapshot.matchingFiles("needle", re); got[0] == "mutated" {
		t.Fatalf("match-list mutation leaked into snapshot: %#v", got)
	}

	baseMatches := snapshot.matchingFiles("synthetic", regexp.MustCompile("synthetic"))
	corpus := (&routeAuthorityDriftValidationCorpus{snapshot: snapshot, overlays: map[string][]byte{}}).withFile(routeAuthorityDriftRepoFile{
		Path: "synthetic.go",
		Raw:  []byte("synthetic"),
	})
	overlayMatches := routeAuthorityDriftMatchingFiles(corpus, "synthetic", regexp.MustCompile("synthetic"))
	if len(baseMatches) != 0 || len(overlayMatches) != 1 || overlayMatches[0] != "synthetic.go" {
		t.Fatalf("base/overlay matches = %#v/%#v, want isolated synthetic overlay", baseMatches, overlayMatches)
	}
	corpus.overlays["synthetic.go"][0] = 'X'
	if got := snapshot.matchingFiles("synthetic", regexp.MustCompile("synthetic")); len(got) != 0 {
		t.Fatalf("overlay byte mutation changed base matches: %#v", got)
	}
}

func TestRouteAuthorityRequiredPathLookupDistinguishesUnavailableFromNonMatch(t *testing.T) {
	root := t.TempDir()
	writeConformanceSnapshotFixture(t, root, "present.go", "package fixture\n")
	snapshot, err := buildConformanceRepoSnapshot(root, defaultConformanceRepoSnapshotIO())
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	corpus := &routeAuthorityDriftValidationCorpus{snapshot: snapshot, overlays: map[string][]byte{}}
	re := regexp.MustCompile("ConnectRoutePlan")

	matched, err := routeAuthorityDriftPathMatches(corpus, "missing.go", re)
	if matched || err == nil || !strings.Contains(err.Error(), "required path missing.go is unavailable") {
		t.Fatalf("missing lookup = (%t, %v), want explicit unavailable-path error", matched, err)
	}
	matched, err = routeAuthorityDriftPathMatches(corpus, "present.go", re)
	if matched || err != nil {
		t.Fatalf("valid non-match = (%t, %v), want false without error", matched, err)
	}

	unavailable := validateRouteAuthorityDriftSearchDimension(corpus, routeAuthorityDriftSearchDimension{
		ID:             "lookup",
		Pattern:        "ConnectRoutePlan",
		RequiredPaths:  []string{"missing.go"},
		CanonicalLayer: "spec_authority",
	})
	if !routeAuthorityProblemsContain(unavailable, "lookup required_path missing.go unavailable:") {
		t.Fatalf("unavailable-path problems = %#v, want explicit unavailable diagnostic", unavailable)
	}
	nonMatch := validateRouteAuthorityDriftSearchDimension(corpus, routeAuthorityDriftSearchDimension{
		ID:             "lookup",
		Pattern:        "ConnectRoutePlan",
		RequiredPaths:  []string{"present.go"},
		CanonicalLayer: "spec_authority",
	})
	if !routeAuthorityProblemsContain(nonMatch, "lookup required_path present.go does not match pattern") {
		t.Fatalf("valid non-match problems = %#v, want pattern-mismatch diagnostic", nonMatch)
	}
}

func TestCanonicalConformanceRepoSnapshotBuildsOnce(t *testing.T) {
	root := conformanceRepoRoot(t)
	first := mustConformanceRepoSnapshot(t, root)
	second := routeAuthorityDriftNewValidationCorpus(t, root).snapshot
	names := conformanceGoTestNames(t, root)
	if first != second {
		t.Fatal("canonical snapshot consumers received different owners")
	}
	for _, name := range []string{
		"TestCanonicalConformanceRepoSnapshotBuildsOnce",
		"TestRouteAuthorityDriftInventoryCoversRepoWideSearchDimensions",
	} {
		if !names[name] {
			t.Fatalf("shared Go-test index omitted route-view self-audit test %s", name)
		}
	}
	if got := conformanceRepoSnapshotBuilds.Load(); got != 1 {
		t.Fatalf("canonical snapshot builds = %d, want 1", got)
	}
}

func TestConformanceMutableValidationInputsAreDeepCloned(t *testing.T) {
	root := conformanceRepoRoot(t)

	inventory := loadRouteAuthorityDriftInventory(t, root)
	inventoryClone := cloneRouteAuthorityDriftInventory(inventory)
	inventoryClone.SearchDimensions[0].RequiredPaths[0] = "mutated"
	inventoryClone.SeamFamilies[0].Paths[0] = "mutated"
	inventoryClone.GuardrailProposals[0].Prevents[0] = "mutated"
	if inventory.SearchDimensions[0].RequiredPaths[0] == "mutated" || inventory.SeamFamilies[0].Paths[0] == "mutated" || inventory.GuardrailProposals[0].Prevents[0] == "mutated" {
		t.Fatal("route inventory nested mutation leaked into source")
	}

	matrix := loadRouteAuthorityMatrix(t, root)
	matrixClone := cloneRouteAuthorityMatrix(matrix)
	matrixClone.Rows[0].ProofDimensions[0] = "mutated"
	matrixClone.Rows[0].OwnerRefs[0].Kind = "mutated"
	matrixClone.Rows[0].ProofRefs[0].Kind = "mutated"
	if matrix.Rows[0].ProofDimensions[0] == "mutated" || matrix.Rows[0].OwnerRefs[0].Kind == "mutated" || matrix.Rows[0].ProofRefs[0].Kind == "mutated" {
		t.Fatal("route matrix nested mutation leaked into source")
	}

	record := loadForkReplayResumeDesignRecord(t, root)
	recordClone := cloneForkReplayResumeDesignRecord(record)
	recordClone.Rows[0].BlockerCodes[0] = "mutated"
	recordClone.Rows[0].ProofRefs[0].Kind = "mutated"
	if record.Rows[0].BlockerCodes[0] == "mutated" || record.Rows[0].ProofRefs[0].Kind == "mutated" {
		t.Fatal("fork replay record nested mutation leaked into source")
	}
}

func writeConformanceSnapshotFixture(t *testing.T, root, rel, contents string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
}
