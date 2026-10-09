package store_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"golang.org/x/mod/modfile"
)

// The cache is disposable analysis, never an allowed-debt baseline. In CI only
// protected master publishes it; PR jobs restore exact entries read-only.
type debtAnalysisCache struct {
	directory, analyzer, toolchain string
	readOnly                       bool
}

type debtAnalysisRecord struct {
	Schema, Source, Analyzer, Toolchain, Checksum string
	Census                                        []byte
}

func debtAnalysisCacheForCheckout(t *testing.T, root string) debtAnalysisCache {
	t.Helper()
	var cache debtAnalysisCache
	directory, err := os.UserCacheDir()
	if err != nil {
		return cache
	}
	cache.directory = filepath.Join(directory, "swarm", "persistence-authority-v1")
	if override := os.Getenv("SWARM_DEBT_CACHE_DIR"); override != "" {
		cache.directory = override
	}
	cache.readOnly = os.Getenv("GITHUB_EVENT_NAME") == "pull_request"
	hash := sha256.New()
	// These owners contain the collector, selected-boundary scan, projection,
	// finding representation and serialization; also cover their live walker.
	paths := []string{
		filepath.Join(root, "internal/store/persistence_authority_debt_census_test.go"),
		filepath.Join(root, "internal/store/persistence_authority_debt_test.go"),
		filepath.Join(root, "internal/store/persistence_authority_debt_cache_test.go"),
		filepath.Join(root, "internal/store/persistence_authority_debt_sidecar_test.go"),
		filepath.Join(root, "internal/store/persistence_authority_registry_test.go"),
	}
	walker, err := filepath.Glob(filepath.Join(root, "internal/checkoutsource/*.go"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, walker...)
	paths = append(paths, filepath.Join(root, "go.mod"), filepath.Join(root, "go.sum"))
	sort.Strings(paths)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(hash, "%s\x00%d\x00", filepath.ToSlash(rel), len(data))
		hash.Write(data)
	}
	cache.analyzer = hex.EncodeToString(hash.Sum(nil))
	command := exec.Command("go", "env", "-json", "GOVERSION", "GOROOT", "GOTOOLCHAIN", "GOOS", "GOARCH", "GOAMD64", "GOEXPERIMENT", "CGO_ENABLED", "CC", "CXX", "CGO_CFLAGS", "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_FFLAGS", "CGO_LDFLAGS")
	command.Dir, command.Env = root, debtCensusEnvironment()
	context, err := command.Output()
	if err != nil {
		// Cache availability cannot change which sources the census admits.
		return debtAnalysisCache{}
	}
	hash.Reset()
	fmt.Fprintf(hash, "%s\x00%s\x00", runtime.Version(), os.Getenv("GODEBUG"))
	hash.Write(context)
	cache.toolchain = hex.EncodeToString(hash.Sum(nil))
	return cache
}

func (cache debtAnalysisCache) path(source string) string {
	return filepath.Join(cache.directory, source+"-"+cache.analyzer+"-"+cache.toolchain+".json")
}

func (cache debtAnalysisCache) read(source string) (map[string]authorityDebtSite, bool) {
	if cache.directory == "" {
		return nil, false
	}
	file, err := os.Open(cache.path(source))
	if err != nil {
		return nil, false
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 32<<20))
	if err != nil {
		return nil, false
	}
	var record debtAnalysisRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nil, false
	}
	if decoder.Decode(new(any)) != io.EOF || record.Schema != "persistence-authority-analysis/v1" || record.Source != source || record.Analyzer != cache.analyzer || record.Toolchain != cache.toolchain {
		return nil, false
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, data) {
		return nil, false
	}
	checksum := sha256.Sum256(record.Census)
	if hex.EncodeToString(checksum[:]) != record.Checksum {
		return nil, false
	}
	census, err := parseAuthorityDebtBaseline(record.Census)
	if err != nil || census.BootstrapSource != source || census.Collector != cache.analyzer {
		return nil, false
	}
	return census.Sites, true
}

func (cache debtAnalysisCache) write(source string, sites map[string]authorityDebtSite) {
	if cache.directory == "" || cache.readOnly {
		return
	}
	census := marshalAuthorityDebtBaseline(authorityDebtBaseline{BootstrapSource: source, Collector: cache.analyzer, Sites: sites})
	checksum := sha256.Sum256(census)
	data, err := json.Marshal(debtAnalysisRecord{Schema: "persistence-authority-analysis/v1", Source: source, Analyzer: cache.analyzer, Toolchain: cache.toolchain, Checksum: hex.EncodeToString(checksum[:]), Census: census})
	if err != nil || os.MkdirAll(cache.directory, 0700) != nil {
		return
	}
	file, err := os.CreateTemp(cache.directory, ".analysis-*")
	if err != nil {
		return
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr == nil && closeErr == nil {
		// A reader sees either the previous complete result or this complete one.
		_ = os.Rename(file.Name(), cache.path(source))
	}
}

func (cache debtAnalysisCache) baseSites(t *testing.T, root, source string) map[string]authorityDebtSite {
	t.Helper()
	data, readErr := os.ReadFile(filepath.Join(root, "go.mod"))
	module, err := modfile.Parse("go.mod", data, nil)
	if readErr != nil || err != nil {
		cache.directory = ""
	}
	if module != nil {
		for _, replacement := range module.Replace {
			if replacement.New.Version == "" {
				// A local replacement's contents are not pinned by this tree SHA.
				cache.directory = ""
			}
		}
	}
	if sites, ok := cache.read(source); ok {
		return sites
	}
	findings := debtLoadPersistenceAuthorityFindings(t, root)
	findings = append(findings, debtSelectedBoundaryFindings(t, root)...)
	sites := authorityDebtSites(findings)
	cache.write(source, sites)
	return sites
}

func TestPersistenceAuthorityDebtAnalysisCacheIdentity(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	cache := debtAnalysisCacheForCheckout(t, root)
	if !authorityDebtHex(cache.analyzer, 64) || !authorityDebtHex(cache.toolchain, 64) {
		t.Fatal("analysis cache identity is unavailable")
	}
	output := os.Getenv("SWARM_DEBT_CACHE_OUTPUT")
	if output == "" {
		return
	}
	source, err := debtTrustedBase(root)
	if os.Getenv("SWARM_DEBT_CACHE_PUBLISH") == "1" {
		data, headErr := debtGit(root, "rev-parse", "HEAD")
		source, err = strings.TrimSpace(string(data)), headErr
	}
	if err != nil || !authorityDebtHex(source, 40) {
		t.Fatalf("immutable cache source: %s %v", source, err)
	}
	key := "debt-analysis-v1-" + source + "-" + cache.analyzer + "-" + cache.toolchain
	file, err := os.OpenFile(output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := fmt.Fprintf(file, "key=%s\nsource=%s\n", key, source)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("export cache identity: %v %v", writeErr, closeErr)
	}
}

func TestPersistenceAuthorityDebtAnalysisCachePublishMaster(t *testing.T) {
	if os.Getenv("SWARM_DEBT_CACHE_PUBLISH") != "1" {
		t.Skip("protected-master immutable-archive producer only")
	}
	if os.Getenv("GITHUB_ACTIONS") != "true" || os.Getenv("GITHUB_REF") != "refs/heads/master" ||
		(os.Getenv("GITHUB_EVENT_NAME") != "push" && os.Getenv("GITHUB_EVENT_NAME") != "schedule") {
		t.Fatal("shared analysis publication requires protected master")
	}
	root := persistenceAuthorityRepoRoot(t)
	head, err := debtGit(root, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	source := strings.TrimSpace(string(head))
	cache := debtAnalysisCacheForCheckout(t, root)
	archive := materializeDebtBase(t, root, source)
	sites := cache.baseSites(t, archive, source)
	retained, ok := cache.read(source)
	if !ok || !debtSitesEqual(sites, retained) {
		t.Fatal("protected-master archive analysis was not durably published")
	}
}
