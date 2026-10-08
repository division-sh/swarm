package store_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func debtHeadProbeRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/authorityprobe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "probe.go", "package probe;import \"database/sql\";func legacy(db *sql.DB){db.QueryRow(\"one\")}\n")
	return root
}

func TestPersistenceAuthorityHeadCensusSharedOnceAndResultIsolated(t *testing.T) {
	root := debtHeadProbeRoot(t)
	var memo debtHeadCensusMemo
	calls := 0
	collect := func(t *testing.T, root string) []authorityFinding {
		calls++
		return debtLoadPersistenceAuthorityFindings(t, root)
	}
	first := memo.load(t, root, collect)
	if len(first) == 0 {
		t.Fatal("complete census produced no expected authority witnesses")
	}
	expected := slices.Clone(first)
	first[0].Member = "caller mutation must not affect another consumer"
	second := memo.load(t, root, collect)
	if calls != 1 || !slices.Equal(second, expected) {
		t.Fatalf("unchanged consumers did not share exactly one isolated census: calls=%d", calls)
	}
	second[0].Member = "another independent mutation"
	if third := memo.load(t, root, collect); calls != 1 || !slices.Equal(third, expected) {
		t.Fatal("memo returned mutable shared findings")
	}
}

func TestPersistenceAuthorityHeadCensusInvalidatesEveryLiveSourceFamily(t *testing.T) {
	for _, name := range []string{"modified-go", "untracked-go", "ignored-go", "inactive-go", "augmented-test", "embedded-data", "context", "root"} {
		t.Run(name, func(t *testing.T) {
			root := debtHeadProbeRoot(t)
			if name == "embedded-data" {
				debtWriteModuleSource(t, root, "data.txt", "one\n")
				debtWriteModuleSource(t, root, "embed.go", "package probe\nimport _ \"embed\"\n//go:embed data.txt\nvar data string\n")
			}
			var memo debtHeadCensusMemo
			calls := 0
			collect := func(t *testing.T, root string) []authorityFinding {
				calls++
				return debtLoadPersistenceAuthorityFindings(t, root)
			}
			before := memo.load(t, root, collect)
			source := "package probe;import \"database/sql\";func additional(db *sql.DB){db.Exec(\"two\")}\n"
			switch name {
			case "modified-go":
				debtWriteModuleSource(t, root, "probe.go", strings.Replace(source, "additional", "legacy", 1))
			case "untracked-go":
				debtWriteModuleSource(t, root, "untracked.go", source)
			case "ignored-go":
				debtWriteModuleSource(t, root, ".gitignore", "ignored.go\n")
				debtWriteModuleSource(t, root, "ignored.go", source)
			case "inactive-go":
				debtWriteModuleSource(t, root, "additional_windows.go", source)
			case "augmented-test":
				debtWriteModuleSource(t, root, "additional_test.go", source)
			case "embedded-data":
				debtWriteModuleSource(t, root, "data.txt", "two\n")
			case "context":
				t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",gotypesalias=0")
			case "root":
				root = debtHeadProbeRoot(t)
			}
			after := memo.load(t, root, collect)
			fresh := debtLoadPersistenceAuthorityFindings(t, root)
			if calls != 2 || !slices.Equal(after, fresh) {
				t.Fatalf("source/context change reused stale findings: calls=%d", calls)
			}
			if name != "embedded-data" && name != "context" && name != "root" && debtSitesEqual(authorityDebtSites(before), authorityDebtSites(after)) {
				t.Fatal("changed authority was not observed")
			}
		})
	}
}

func TestPersistenceAuthorityHeadCensusInvalidatesLocalDependency(t *testing.T) {
	parent := t.TempDir()
	root, dependency := filepath.Join(parent, "root"), filepath.Join(parent, "dependency")
	debtWriteModuleSource(t, dependency, "go.mod", "module example.com/dependency\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, dependency, "dependency.go", "package dependency;const Marker = 1\n")
	debtWriteModuleSource(t, root, "go.mod", "module example.com/authorityprobe\n\ngo 1.25.0\nrequire example.com/dependency v0.0.0\nreplace example.com/dependency => ../dependency\n")
	debtWriteModuleSource(t, root, "probe.go", "package probe;import \"example.com/dependency\";var marker = dependency.Marker\n")
	var memo debtHeadCensusMemo
	calls := 0
	collect := func(t *testing.T, root string) []authorityFinding {
		calls++
		return debtLoadPersistenceAuthorityFindings(t, root)
	}
	memo.load(t, root, collect)
	debtWriteModuleSource(t, dependency, "dependency.go", "package dependency;const Marker = 2\n")
	memo.load(t, root, collect)
	if calls != 2 {
		t.Fatal("local dependency mutation reused a stale type census")
	}
}

func TestPersistenceAuthorityHeadCensusDoesNotCacheChangingScan(t *testing.T) {
	root := debtHeadProbeRoot(t)
	var memo debtHeadCensusMemo
	calls := 0
	collect := func(t *testing.T, root string) []authorityFinding {
		calls++
		findings := debtLoadPersistenceAuthorityFindings(t, root)
		if calls == 1 {
			debtWriteModuleSource(t, root, "late.go", "package probe;import \"database/sql\";func late(db *sql.DB){db.Exec(\"two\")}\n")
		}
		return findings
	}
	memo.load(t, root, collect)
	got := memo.load(t, root, collect)
	if calls != 2 || !slices.Equal(got, debtLoadPersistenceAuthorityFindings(t, root)) {
		t.Fatal("source changed during scan but partial observation was reused")
	}
}
