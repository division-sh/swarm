package runtime_test

import (
	"bufio"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeSubpackagesDoNotImportRuntimeRoot(t *testing.T) {
	t.Helper()

	repoRoot := projectRootFromArchitectureTest(t)
	checks := map[string]string{
		filepath.Join(repoRoot, "internal", "runtime", "pipeline"):  "empireai/internal/runtime",
		filepath.Join(repoRoot, "internal", "runtime", "bus"):       "empireai/internal/runtime",
		filepath.Join(repoRoot, "internal", "runtime", "manager"):   "empireai/internal/runtime",
		filepath.Join(repoRoot, "internal", "runtime", "mcp"):       "empireai/internal/runtime",
		filepath.Join(repoRoot, "internal", "runtime", "tools"):     "empireai/internal/runtime",
		filepath.Join(repoRoot, "internal", "runtime", "contracts"): "empireai/internal/runtime",
	}

	for dir, forbidden := range checks {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse imports %s: %v", path, err)
			}
			for _, imp := range file.Imports {
				got := strings.Trim(imp.Path.Value, `"`)
				if got == forbidden {
					t.Fatalf("%s imports forbidden root package %s", path, forbidden)
				}
			}
		}
	}
}

func TestRuntimeRootFileCountStaysBounded(t *testing.T) {
	t.Helper()

	repoRoot := projectRootFromArchitectureTest(t)
	root := filepath.Join(repoRoot, "internal", "runtime")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}

	prodCount := 0
	testCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") {
			testCount++
			continue
		}
		prodCount++
	}

	if prodCount > 15 {
		t.Fatalf("runtime root has too many production files: got=%d want<=15", prodCount)
	}
	if testCount > 20 {
		t.Fatalf("runtime root has too many test files: got=%d want<=20", testCount)
	}
}

func TestRuntimeRootHasNoZZZOmnibusTests(t *testing.T) {
	t.Helper()

	root := filepath.Join(projectRootFromArchitectureTest(t), "internal", "runtime")
	matches, err := filepath.Glob(filepath.Join(root, "zzz*.go"))
	if err != nil {
		t.Fatalf("glob zzz files: %v", err)
	}
	if len(matches) > 0 {
		t.Fatalf("runtime root still contains omnibus zzz files: %v", matches)
	}
}

func TestRuntimeRootWrapperFilesStayThin(t *testing.T) {
	t.Helper()

	root := filepath.Join(projectRootFromArchitectureTest(t), "internal", "runtime")
	limits := map[string]int{
		"eventbus.go":  140,
		"helpers.go":   320,
		"mcp_hooks.go": 320,
	}
	for name, maxLines := range limits {
		path := filepath.Join(root, name)
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lines := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lines++
		}
		_ = f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
		if lines > maxLines {
			t.Fatalf("runtime root wrapper grew too large: %s has %d lines, want <= %d", name, lines, maxLines)
		}
	}
}

func projectRootFromArchitectureTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func TestRuntimeRootNoLegacyAgentLLM(t *testing.T) {
	t.Helper()
	path := filepath.Join(projectRootFromArchitectureTest(t), "internal", "runtime", "agent_llm.go")
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("legacy root agent_llm.go still present: %s", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", path, err)
	}
}

func TestRuntimeRootHasNoAliasesShim(t *testing.T) {
	t.Helper()
	path := filepath.Join(projectRootFromArchitectureTest(t), "internal", "runtime", "aliases.go")
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("legacy root aliases.go still present: %s", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", path, err)
	}
}

func TestNoRepoWideLegacyTestDumpFiles(t *testing.T) {
	t.Helper()
	repoRoot := projectRootFromArchitectureTest(t)
	var matches []string
	err := filepath.Walk(filepath.Join(repoRoot, "internal"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		name := info.Name()
		if name == "zzz_more_consolidated_test.go" || strings.HasSuffix(name, "more_coverage_test.go") || strings.HasSuffix(name, "_more_test.go") {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal: %v", err)
	}
	err = filepath.Walk(filepath.Join(repoRoot, "cmd"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(info.Name(), "more_coverage_test.go") || strings.HasSuffix(info.Name(), "_more_test.go") {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk cmd: %v", err)
	}
	if len(matches) > 0 {
		t.Fatalf("legacy test dump files still present: %v", matches)
	}
}

func TestRuntimeHotspotFilesStayBounded(t *testing.T) {
	t.Helper()
	repoRoot := projectRootFromArchitectureTest(t)
	limits := map[string]int{
		filepath.Join("internal", "runtime", "pipeline", "coordinator.go"):           950,
		filepath.Join("internal", "runtime", "pipeline", "coordinator_discovery.go"): 1150,
		filepath.Join("internal", "runtime", "pipeline", "coordinator_scoring.go"):   1050,
		filepath.Join("internal", "runtime", "tools", "executor.go"):                 900,
		filepath.Join("internal", "runtime", "tools", "executor_emit.go"):            850,
	}
	for rel, maxLines := range limits {
		path := filepath.Join(repoRoot, rel)
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lines := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lines++
		}
		_ = f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
		if lines > maxLines {
			t.Fatalf("hotspot file grew too large: %s has %d lines, want <= %d", rel, lines, maxLines)
		}
	}
}
