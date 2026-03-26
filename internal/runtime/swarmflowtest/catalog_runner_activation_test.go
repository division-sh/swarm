package swarmflowtest

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogRunner_ExecutesCurrentlySupportedCatalogCases(t *testing.T) {
	perTier := map[string]int{}
	total := 0
	for _, dir := range discoveredCatalogCaseDirs(t) {
		root := filepath.Join(repoRootFromMASTest(t), "tests", filepath.FromSlash(dir))
		var expected catalogExpectedDocument
		loadExpectedYAMLForCatalogTest(t, filepath.Join(root, "expected.yaml"), &expected)
		if !catalogCaseExecutableNow(t, dir, expected) {
			continue
		}
		t.Run(dir, func(t *testing.T) {
			result, expected := runSimpleCatalogCase(t, root)
			assertCatalogRunResult(t, result, expected)
		})
		total++
		tier := dir
		if idx := strings.IndexByte(dir, '/'); idx > 0 {
			tier = dir[:idx]
		}
		perTier[tier]++
	}
	if total < 25 {
		t.Fatalf("expected at least 25 supported catalog cases, got %d", total)
	}
	if perTier["tier1-primitives"] == 0 {
		t.Fatal("no tier1 primitive cases executed")
	}
	if perTier["tier2-accumulation"] == 0 {
		t.Fatal("no tier2 accumulation cases executed")
	}
	if perTier["tier3-list-processing"] == 0 {
		t.Fatal("no tier3 list-processing cases executed")
	}
	if perTier["tier4-cross-entity"] == 0 {
		t.Fatal("no tier4 cross-entity cases executed")
	}
	if perTier["tier5-flow-lifecycle"] == 0 {
		t.Fatal("no tier5 flow-lifecycle cases executed")
	}
	if perTier["tier6-event-loop"] == 0 {
		t.Fatal("no tier6 event-loop cases executed")
	}
	if perTier["tier7-composition"] == 0 {
		t.Fatal("no tier7 composition cases executed")
	}
	if perTier["tier8-boot-verification"] == 0 {
		t.Fatal("no tier8 boot-verification cases executed")
	}
}
