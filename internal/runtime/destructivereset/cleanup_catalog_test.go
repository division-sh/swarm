package destructivereset

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDefaultPlatformCleanupCatalogClassifiesEveryPlatformTable(t *testing.T) {
	specTables := loadPlatformTableNamesForCleanupCatalogTest(t)
	catalog := DefaultPlatformCleanupCatalog()
	seen := map[string]CleanupCatalogEntry{}
	for _, entry := range catalog {
		if entry.TableKind != CleanupTableKindPlatform {
			t.Fatalf("platform catalog entry %s kind = %q", entry.Table, entry.TableKind)
		}
		if entry.Table == "" || entry.Classification == "" || entry.PredicateOwner == "" {
			t.Fatalf("incomplete catalog entry: %#v", entry)
		}
		if _, exists := seen[entry.Table]; exists {
			t.Fatalf("duplicate cleanup catalog entry for table %s", entry.Table)
		}
		seen[entry.Table] = entry
	}
	for table := range specTables {
		if _, ok := seen[table]; !ok {
			t.Fatalf("platform table %s lacks destructive reset cleanup classification", table)
		}
	}
	for table := range seen {
		if _, ok := specTables[table]; !ok {
			t.Fatalf("cleanup catalog classifies unknown platform table %s", table)
		}
	}
}

func TestDefaultGeneratedCleanupCatalogSplitPreservesGeneratedTables(t *testing.T) {
	for _, entry := range DefaultGeneratedCleanupCatalog() {
		if entry.TableKind != CleanupTableKindGenerated {
			t.Fatalf("generated cleanup catalog entry %s kind = %q", entry.Table, entry.TableKind)
		}
		if entry.Classification != CleanupSplitPreserve {
			t.Fatalf("generated cleanup catalog entry %s classification = %q, want split_preserve", entry.Table, entry.Classification)
		}
		if entry.PredicateOwner == "" || entry.PreservationProof == "" {
			t.Fatalf("generated cleanup catalog entry %s missing proof: %#v", entry.Table, entry)
		}
	}
}

func TestDefaultPlatformCleanupCatalogMatchesPlatformSpecPolicy(t *testing.T) {
	specTables := loadPlatformTableNamesForCleanupCatalogTest(t)
	specClassifications := loadPlatformCleanupClassificationsForCleanupCatalogTest(t)
	catalog := DefaultPlatformCleanupCatalog()
	for _, entry := range catalog {
		want, ok := specClassifications[entry.Table]
		if !ok {
			t.Fatalf("platform-spec destructive_reset_cleanup_policy lacks table %s", entry.Table)
		}
		if entry.Classification != want {
			t.Fatalf("cleanup catalog table %s classification = %q, want platform-spec %q", entry.Table, entry.Classification, want)
		}
	}
	for table := range specClassifications {
		if _, ok := specTables[table]; !ok {
			t.Fatalf("platform-spec cleanup policy classifies non-platform table %s as a platform entry", table)
		}
	}
}

func loadPlatformTableNamesForCleanupCatalogTest(t *testing.T) map[string]struct{} {
	t.Helper()
	repo := cleanupCatalogTestRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(repo, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform-spec.yaml: %v", err)
	}
	var doc struct {
		PlatformTables struct {
			Tables map[string]struct {
				DDL string `yaml:"ddl"`
			} `yaml:"tables"`
		} `yaml:"platform_tables"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse platform-spec.yaml: %v", err)
	}
	if len(doc.PlatformTables.Tables) == 0 {
		t.Fatal("platform-spec.yaml platform_tables.tables is empty")
	}
	out := make(map[string]struct{}, len(doc.PlatformTables.Tables))
	for table := range doc.PlatformTables.Tables {
		out[table] = struct{}{}
	}
	return out
}

func loadPlatformCleanupClassificationsForCleanupCatalogTest(t *testing.T) map[string]string {
	t.Helper()
	repo := cleanupCatalogTestRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(repo, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform-spec.yaml: %v", err)
	}
	var doc struct {
		PlatformTables struct {
			DestructiveResetCleanupPolicy struct {
				Classifications map[string][]string `yaml:"classifications"`
			} `yaml:"destructive_reset_cleanup_policy"`
		} `yaml:"platform_tables"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse platform-spec.yaml: %v", err)
	}
	if len(doc.PlatformTables.DestructiveResetCleanupPolicy.Classifications) == 0 {
		t.Fatal("platform-spec.yaml destructive_reset_cleanup_policy.classifications is empty")
	}
	specTables := loadPlatformTableNamesForCleanupCatalogTest(t)
	out := map[string]string{}
	for classification, tables := range doc.PlatformTables.DestructiveResetCleanupPolicy.Classifications {
		for _, table := range tables {
			if _, ok := specTables[table]; !ok {
				continue
			}
			if existing, ok := out[table]; ok {
				t.Fatalf("platform-spec cleanup policy table %s classified as both %q and %q", table, existing, classification)
			}
			out[table] = classification
		}
	}
	return out
}

func cleanupCatalogTestRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		next := filepath.Dir(dir)
		if next == dir {
			t.Fatal("could not find repo root")
		}
		dir = next
	}
}
