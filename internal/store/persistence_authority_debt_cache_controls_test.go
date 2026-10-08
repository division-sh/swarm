package store_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func debtControlCache(t *testing.T) debtAnalysisCache {
	t.Helper()
	return debtAnalysisCache{directory: t.TempDir(), analyzer: strings.Repeat("a", 64), toolchain: strings.Repeat("b", 64)}
}

func TestPersistenceAuthorityDebtAnalysisCacheRoundTripAndIsolation(t *testing.T) {
	cache := debtControlCache(t)
	source := strings.Repeat("1", 40)
	sites := debtControlSet(debtControlSite("call:QueryRow", 2), debtControlSite("call:Exec", 1))
	cache.write(source, sites)
	got, ok := cache.read(source)
	if !ok || !debtSitesEqual(got, sites) {
		t.Fatal("cache lost exact site identity or multiplicity")
	}
	delete(got, debtControlSite("call:QueryRow", 2).identity())
	got, ok = cache.read(source)
	if !ok || !debtSitesEqual(got, sites) {
		t.Fatal("caller mutation changed the cached census")
	}
	for _, change := range []string{"source", "analyzer", "toolchain"} {
		t.Run(change, func(t *testing.T) {
			other, key := cache, source
			switch change {
			case "source":
				key = strings.Repeat("2", 40)
			case "analyzer":
				other.analyzer = strings.Repeat("c", 64)
			case "toolchain":
				other.toolchain = strings.Repeat("d", 64)
			}
			if _, ok := other.read(key); ok {
				t.Fatal("foreign analysis reused")
			}
		})
	}
}

func TestPersistenceAuthorityDebtAnalysisCacheRejectsInvalidEntries(t *testing.T) {
	for _, name := range []string{"schema", "source", "analyzer", "toolchain", "checksum", "truncated", "duplicate-site", "wrong-payload-source", "wrong-payload-analyzer", "zero-multiplicity", "unknown-field", "duplicate-field", "trailing-json"} {
		t.Run(name, func(t *testing.T) {
			cache := debtControlCache(t)
			source := strings.Repeat("1", 40)
			cache.write(source, debtControlSet(debtControlSite("call:QueryRow", 2)))
			data, err := os.ReadFile(cache.path(source))
			if err != nil {
				t.Fatal(err)
			}
			var record debtAnalysisRecord
			if err := json.Unmarshal(data, &record); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "schema":
				record.Schema = "unknown"
			case "source":
				record.Source = strings.Repeat("2", 40)
			case "analyzer":
				record.Analyzer = strings.Repeat("c", 64)
			case "toolchain":
				record.Toolchain = strings.Repeat("d", 64)
			case "checksum":
				record.Census = append(record.Census, 'x')
			case "duplicate-site":
				row := bytes.Split(record.Census, []byte("\n"))[4]
				record.Census = append(record.Census, append(row, '\n')...)
			case "wrong-payload-source":
				record.Census = bytes.ReplaceAll(record.Census, []byte(source), []byte(strings.Repeat("2", 40)))
			case "wrong-payload-analyzer":
				record.Census = bytes.ReplaceAll(record.Census, []byte(cache.analyzer), []byte(strings.Repeat("c", 64)))
			case "zero-multiplicity":
				record.Census = bytes.ReplaceAll(record.Census, []byte("\t2\n"), []byte("\t0\n"))
			}
			if name != "checksum" {
				checksum := sha256.Sum256(record.Census)
				record.Checksum = hex.EncodeToString(checksum[:])
			}
			data, err = json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "truncated":
				data = data[:len(data)/2]
			case "unknown-field":
				data = append([]byte(`{"foreign":true,`), data[1:]...)
			case "duplicate-field":
				data = append([]byte(`{"Schema":"persistence-authority-analysis/v1",`), data[1:]...)
			case "trailing-json":
				data = append(data, []byte(" {}")...)
			}
			if err := os.WriteFile(cache.path(source), data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, ok := cache.read(source); ok {
				t.Fatal("invalid entry substituted for a complete source scan")
			}
		})
	}
}

func TestPersistenceAuthorityDebtAnalysisCacheKeepsFreshSourcesAndResurrection(t *testing.T) {
	cache := debtControlCache(t)
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/authorityprobe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "probe.go", "package probe;import \"database/sql\";func legacy(db *sql.DB){db.QueryRow(\"one\")}\n")
	oldSource := strings.Repeat("1", 40)
	cold := cache.baseSites(t, root, oldSource)
	warm := cache.baseSites(t, root, oldSource)
	if !debtSitesEqual(cold, warm) || len(cold) == 0 {
		t.Fatal("warm census differs from complete cold census")
	}
	// Head is not passed through the cache, including modified/untracked input.
	debtWriteModuleSource(t, root, "untracked.go", "package probe;import \"database/sql\";func newSite(db *sql.DB){db.Exec(\"two\")}\n")
	head := authorityDebtSites(append(debtLoadPersistenceAuthorityFindings(t, root), debtSelectedBoundaryFindings(t, root)...))
	if !debtSitesEqual(cold, warm) || len(authorityDebtRatchet(head, head, head, warm)) == 0 {
		t.Fatal("cached base hid head's newly added/resurrected authority")
	}
	newSource := strings.Repeat("2", 40)
	if next := cache.baseSites(t, root, newSource); !debtSitesEqual(next, head) {
		t.Fatal("new immutable source was not independently scanned")
	}
	if got, ok := cache.read(oldSource); !ok || !debtSitesEqual(got, cold) {
		t.Fatal("new source overwrote the predecessor's exact census")
	}
}

func TestPersistenceAuthorityDebtAnalysisCacheCannotPublishDirtyHead(t *testing.T) {
	cache := debtControlCache(t)
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		out, err := debtGit(root, args...)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("config", "user.name", "Debt cache control")
	git("config", "user.email", "cache@example.invalid")
	debtWriteModuleSource(t, root, "source.txt", "original\n")
	git("add", ".")
	git("commit", "-qm", "test: cache source")
	source := debtCommittedCensusSource(root)
	if source == "" {
		t.Fatal("clean committed source missing")
	}
	debtWriteModuleSource(t, root, "untracked.go", "package probe\n")
	cache.publishHead(root, source, debtControlSet(debtControlSite("call:Exec", 1)))
	if _, ok := cache.read(source); ok {
		t.Fatal("dirty checkout was published as an immutable source")
	}
	if err := os.Remove(filepath.Join(root, "untracked.go")); err != nil {
		t.Fatal(err)
	}
	git("commit", "-q", "--allow-empty", "-m", "test: changed head during scan")
	cache.publishHead(root, source, debtControlSet(debtControlSite("call:Exec", 1)))
	if _, ok := cache.read(source); ok {
		t.Fatal("changed head was published under the old identity")
	}
	newSource := debtCommittedCensusSource(root)
	sites := debtControlSet(debtControlSite("call:Exec", 1))
	cache.publishHead(root, newSource, sites)
	if got, ok := cache.read(newSource); !ok || !debtSitesEqual(got, sites) {
		t.Fatal("successful clean-head census was not published for future base use")
	}
}

func TestPersistenceAuthorityDebtAnalysisCacheRecomputesCorruptAndUnavailableEntries(t *testing.T) {
	for _, name := range []string{"corrupt", "unavailable", "local-replacement"} {
		t.Run(name, func(t *testing.T) {
			cache := debtControlCache(t)
			root := t.TempDir()
			module := "module example.com/authorityprobe\n\ngo 1.25.0\n"
			if name == "local-replacement" {
				module += "\nreplace example.com/unused => ../external\n"
			}
			debtWriteModuleSource(t, root, "go.mod", module)
			debtWriteModuleSource(t, root, "probe.go", "package probe;import \"database/sql\";func legacy(db *sql.DB){db.QueryRow(\"one\")}\n")
			source := strings.Repeat("1", 40)
			switch name {
			case "corrupt":
				if err := os.WriteFile(cache.path(source), []byte("truncated"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unavailable":
				if err := os.WriteFile(filepath.Join(cache.directory, "not-a-directory"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				cache.directory = filepath.Join(cache.directory, "not-a-directory")
			case "local-replacement":
				cache.write(source, debtControlSet(debtControlSite("call:Exec", 99)))
			}
			got := cache.baseSites(t, root, source)
			fresh := authorityDebtSites(append(debtLoadPersistenceAuthorityFindings(t, root), debtSelectedBoundaryFindings(t, root)...))
			if !debtSitesEqual(got, fresh) || len(got) == 0 {
				t.Fatal("cache failure/local dependency changed the complete census result")
			}
		})
	}
}

func TestPersistenceAuthorityDebtAnalysisCacheFingerprintsOwnersAndBuildContext(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{"internal/store/persistence_authority_debt_census_test.go", "internal/store/persistence_authority_debt_test.go", "internal/store/persistence_authority_debt_cache_test.go", "internal/store/persistence_authority_registry_test.go", "internal/checkoutsource/checkoutsource.go"} {
		debtWriteModuleSource(t, root, path, "package probe\n")
	}
	debtWriteModuleSource(t, root, "go.mod", "module example.com/authorityprobe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "go.sum", "")
	original := debtAnalysisCacheForCheckout(t, root)
	if original.directory == "" || original.toolchain == "" {
		t.Fatal("cache identity could not be computed")
	}
	for _, path := range []string{"internal/store/persistence_authority_debt_census_test.go", "internal/store/persistence_authority_debt_test.go", "internal/store/persistence_authority_debt_cache_test.go", "internal/store/persistence_authority_registry_test.go", "internal/checkoutsource/checkoutsource.go", "go.sum"} {
		prior, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, root, path, string(prior)+"// changed\n")
		if changed := debtAnalysisCacheForCheckout(t, root); changed.analyzer == original.analyzer {
			t.Fatalf("owner/dependency change did not invalidate cache: %s", path)
		}
		debtWriteModuleSource(t, root, path, string(prior))
	}
	t.Setenv("CGO_CFLAGS", "-O1 -DSWARM_DEBT_CACHE_CONTEXT_CONTROL=1")
	if changed := debtAnalysisCacheForCheckout(t, root); changed.toolchain == original.toolchain {
		t.Fatal("effective build context did not invalidate the cache")
	}
	t.Setenv("GODEBUG", "gotypesalias=0")
	first := debtAnalysisCacheForCheckout(t, root)
	t.Setenv("GODEBUG", "gotypesalias=1")
	if second := debtAnalysisCacheForCheckout(t, root); first.toolchain == second.toolchain {
		t.Fatal("type-checker settings did not invalidate the cache")
	}
}
