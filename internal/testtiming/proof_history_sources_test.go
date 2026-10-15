package testtiming

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

type proofHistorySourcePackage struct {
	ordinary, tests              map[string]bool
	ordinaryImports, testImports map[string]bool
	hasTests                     bool
}

// Enumerate the real consumers and their helper imports, not a second list of
// shard names or host-active files. Excluded platform sources remain accounted.
func proofHistorySourceConsumers(t *testing.T, root string) map[string][]string {
	t.Helper()
	packages := map[string]*proofHistorySourcePackage{}
	pinned := regexp.MustCompile(`"([0-9a-f]{40})(?:"|:)`)
	for _, scope := range []string{"internal/store", "tools/fixture-codemod/pipeline-observations"} {
		start := filepath.Join(root, scope)
		err := checkoutsource.WalkDir(root, start, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				name := entry.Name()
				if path != start && (scope == "internal/store" || name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
					return fs.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".go" {
				return nil
			}
			return collectProofHistorySource(root, path, pinned, packages)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	consumers := map[string][]string{}
	for name, pkg := range packages {
		if !pkg.hasTests {
			continue
		}
		commits := map[string]bool{}
		collectProofHistoryImports(name, true, packages, map[string]bool{}, commits)
		if len(commits) == 0 {
			continue
		}
		for commit := range commits {
			consumers[name] = append(consumers[name], commit)
		}
		sort.Strings(consumers[name])
	}
	return consumers
}

func collectProofHistorySource(root, path string, pinned *regexp.Regexp, packages map[string]*proofHistorySourcePackage) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, body, parser.AllErrors)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(root, filepath.Dir(path))
	if err != nil {
		return err
	}
	name := "github.com/division-sh/swarm/" + filepath.ToSlash(relative)
	pkg := packages[name]
	if pkg == nil {
		pkg = &proofHistorySourcePackage{ordinary: map[string]bool{}, tests: map[string]bool{}, ordinaryImports: map[string]bool{}, testImports: map[string]bool{}}
		packages[name] = pkg
	}
	commits, imports := pkg.ordinary, pkg.ordinaryImports
	if strings.HasSuffix(path, "_test.go") {
		pkg.hasTests = true
		commits, imports = pkg.tests, pkg.testImports
	}
	for _, match := range pinned.FindAllSubmatch(body, -1) {
		commits[string(match[1])] = true
	}
	for _, imported := range file.Imports {
		value, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			return err
		}
		imports[value] = true
	}
	return nil
}

func collectProofHistoryImports(name string, tests bool, packages map[string]*proofHistorySourcePackage, seen, commits map[string]bool) {
	if seen[name] || packages[name] == nil {
		return
	}
	seen[name] = true
	pkg := packages[name]
	for commit := range pkg.ordinary {
		commits[commit] = true
	}
	for imported := range pkg.ordinaryImports {
		collectProofHistoryImports(imported, false, packages, seen, commits)
	}
	if tests {
		for commit := range pkg.tests {
			commits[commit] = true
		}
		for imported := range pkg.testImports {
			collectProofHistoryImports(imported, false, packages, seen, commits)
		}
	}
}

func TestProofHistorySourcesFollowNewHelpersWithoutForeignOrTestOnlyLeakage(t *testing.T) {
	root := t.TempDir()
	base := "tools/fixture-codemod/pipeline-observations"
	module := "github.com/division-sh/swarm/" + base
	refs := []string{strings.Repeat("1", 40), strings.Repeat("2", 40), strings.Repeat("3", 40), strings.Repeat("4", 40)}
	files := map[string]string{
		"go.mod":                                 "module github.com/division-sh/swarm\n\ngo 1.26.0\n",
		base + "/main_test.go":                   "package main\nimport _ \"" + module + "/internal/reader\"\n",
		base + "/internal/reader/current.go":     "package reader\nconst current = \"" + refs[0] + "\"\n",
		base + "/internal/reader/new_windows.go": "package reader\nconst excluded = \"" + refs[1] + "\"\n",
		base + "/internal/reader/reader_test.go": "package reader\nconst testOnly = \"" + refs[2] + "\"\n",
		base + "/foreign/.git":                   "foreign checkout\n",
		base + "/foreign/foreign_test.go":        "package foreign\nconst foreign = \"" + refs[3] + "\"\n",
	}
	if err := os.MkdirAll(filepath.Join(root, "internal/store"), 0700); err != nil {
		t.Fatal(err)
	}
	for path, body := range files {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	want := map[string][]string{module: {refs[0], refs[1]}, module + "/internal/reader": {refs[0], refs[1], refs[2]}}
	if got := proofHistorySourceConsumers(t, root); !reflect.DeepEqual(got, want) {
		t.Fatalf("helper/platform/ownership source routing=%v want=%v", got, want)
	}
	broken := filepath.Join(root, base, "broken.go")
	if err := os.WriteFile(broken, []byte("package main\nfunc broken(){"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := collectProofHistorySource(root, broken, regexp.MustCompile(`"([0-9a-f]{40})(?:"|:)`), map[string]*proofHistorySourcePackage{}); err == nil {
		t.Fatal("incomplete owned source was accepted")
	}
}
