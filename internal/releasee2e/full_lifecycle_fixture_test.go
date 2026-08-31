package releasee2e

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type fullLifecycleFixtureDisposition string

const (
	fullLifecycleExecutableFixture fullLifecycleFixtureDisposition = "executable"
	fullLifecycleSupportFixture    fullLifecycleFixtureDisposition = "support"
)

type fullLifecycleFixtureRegistration struct {
	Root        string
	Disposition fullLifecycleFixtureDisposition
}

var fullLifecycleFixtureRegistry = []fullLifecycleFixtureRegistration{
	{Root: "standing_telegram", Disposition: fullLifecycleExecutableFixture},
	{Root: "standing_telegram/bot", Disposition: fullLifecycleSupportFixture},
}

func TestFullLifecycleFixtureRegistryIsTotal(t *testing.T) {
	if err := validateFullLifecycleFixtureRegistry(fullLifecycleFixtureRoot(t), fullLifecycleFixtureRegistry); err != nil {
		t.Fatal(err)
	}
}

func TestFullLifecycleJourneyMatrixIsClosed(t *testing.T) {
	want := map[string]fullLifecycleJourney{
		"J1-sqlite-graceful":                  {name: "J1-sqlite-graceful", backend: "sqlite", kind: fullLifecycleGraceful},
		"J2-postgres-graceful":                {name: "J2-postgres-graceful", backend: "postgres", kind: fullLifecycleGraceful},
		"J3-sqlite-crash-intrinsic-recover":   {name: "J3-sqlite-crash-intrinsic-recover", backend: "sqlite", kind: fullLifecycleCrashIntrinsic},
		"J4-postgres-crash-intrinsic-recover": {name: "J4-postgres-crash-intrinsic-recover", backend: "postgres", kind: fullLifecycleCrashIntrinsic},
		"J5-sqlite-dev-fresh":                 {name: "J5-sqlite-dev-fresh", backend: "sqlite", kind: fullLifecycleDevFresh},
	}
	if len(fullLifecycleJourneys) != len(want) {
		t.Fatalf("full lifecycle journey count = %d, want %d", len(fullLifecycleJourneys), len(want))
	}
	seen := make(map[string]bool, len(fullLifecycleJourneys))
	for _, journey := range fullLifecycleJourneys {
		if seen[journey.name] {
			t.Fatalf("duplicate full lifecycle journey %s", journey.name)
		}
		seen[journey.name] = true
		if expected, ok := want[journey.name]; !ok || journey != expected {
			t.Fatalf("unsupported full lifecycle journey %#v", journey)
		}
	}
}

func TestFullLifecycleFixtureRegistryFailsClosed(t *testing.T) {
	root := t.TempDir()
	copyReleaseTree(t, fullLifecycleFixtureRoot(t), root)

	t.Run("missing", func(t *testing.T) {
		registry := append([]fullLifecycleFixtureRegistration(nil), fullLifecycleFixtureRegistry[:1]...)
		err := validateFullLifecycleFixtureRegistry(root, registry)
		if err == nil || !strings.Contains(err.Error(), "unclassified package roots: standing_telegram/bot") {
			t.Fatalf("missing registration error = %v", err)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		registry := append([]fullLifecycleFixtureRegistration(nil), fullLifecycleFixtureRegistry...)
		registry = append(registry, fullLifecycleFixtureRegistry[0])
		err := validateFullLifecycleFixtureRegistry(root, registry)
		if err == nil || !strings.Contains(err.Error(), "duplicate lifecycle fixture registration: standing_telegram") {
			t.Fatalf("duplicate registration error = %v", err)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		registry := append([]fullLifecycleFixtureRegistration(nil), fullLifecycleFixtureRegistry...)
		registry[1].Disposition = "future"
		err := validateFullLifecycleFixtureRegistry(root, registry)
		if err == nil || !strings.Contains(err.Error(), `unsupported disposition "future"`) {
			t.Fatalf("unknown disposition error = %v", err)
		}
	})

	t.Run("multiply-classified", func(t *testing.T) {
		registry := append([]fullLifecycleFixtureRegistration(nil), fullLifecycleFixtureRegistry...)
		registry[1].Disposition = fullLifecycleExecutableFixture
		err := validateFullLifecycleFixtureRegistry(root, registry)
		if err == nil || !strings.Contains(err.Error(), "has 2 executable fixtures, want exactly 1") {
			t.Fatalf("multiple executable registration error = %v", err)
		}
	})

	t.Run("stale", func(t *testing.T) {
		registry := append([]fullLifecycleFixtureRegistration(nil), fullLifecycleFixtureRegistry...)
		registry = append(registry, fullLifecycleFixtureRegistration{Root: "removed", Disposition: fullLifecycleSupportFixture})
		err := validateFullLifecycleFixtureRegistry(root, registry)
		if err == nil || !strings.Contains(err.Error(), "registered package roots not found: removed") {
			t.Fatalf("stale registration error = %v", err)
		}
	})
}

func fullLifecycleFixtureRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(releaseE2ERepoRoot(t), "internal", "releasee2e", "testdata", "full_lifecycle")
}

func validateFullLifecycleFixtureRegistry(root string, registry []fullLifecycleFixtureRegistration) error {
	discovered, err := discoverFullLifecyclePackages(root)
	if err != nil {
		return err
	}
	registered := make(map[string]fullLifecycleFixtureDisposition, len(registry))
	for _, entry := range registry {
		path := filepath.ToSlash(filepath.Clean(strings.TrimSpace(entry.Root)))
		if path == "." || path == "" || strings.HasPrefix(path, "../") {
			return fmt.Errorf("invalid lifecycle fixture root %q", entry.Root)
		}
		switch entry.Disposition {
		case fullLifecycleExecutableFixture, fullLifecycleSupportFixture:
		default:
			return fmt.Errorf("lifecycle fixture %s has unsupported disposition %q", path, entry.Disposition)
		}
		if _, duplicate := registered[path]; duplicate {
			return fmt.Errorf("duplicate lifecycle fixture registration: %s", path)
		}
		registered[path] = entry.Disposition
	}

	var unclassified, missing []string
	for _, path := range discovered {
		if _, ok := registered[path]; !ok {
			unclassified = append(unclassified, path)
		}
	}
	for path := range registered {
		if !fullLifecycleContains(discovered, path) {
			missing = append(missing, path)
		}
	}
	sort.Strings(unclassified)
	sort.Strings(missing)
	if len(unclassified) != 0 {
		return fmt.Errorf("unclassified package roots: %s", strings.Join(unclassified, ", "))
	}
	if len(missing) != 0 {
		return fmt.Errorf("registered package roots not found: %s", strings.Join(missing, ", "))
	}

	executable := 0
	for _, disposition := range registered {
		if disposition == fullLifecycleExecutableFixture {
			executable++
		}
	}
	if executable != 1 {
		return fmt.Errorf("full lifecycle registry has %d executable fixtures, want exactly 1", executable)
	}
	return nil
}

func discoverFullLifecyclePackages(root string) ([]string, error) {
	var packages []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "package.yaml" {
			return nil
		}
		relative, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		packages = append(packages, filepath.ToSlash(relative))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("discover lifecycle packages: %w", err)
	}
	sort.Strings(packages)
	return packages, nil
}

func fullLifecycleContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func fullLifecycleExecutableSource(t *testing.T) string {
	t.Helper()
	for _, entry := range fullLifecycleFixtureRegistry {
		if entry.Disposition == fullLifecycleExecutableFixture {
			return filepath.Join(fullLifecycleFixtureRoot(t), filepath.FromSlash(entry.Root))
		}
	}
	t.Fatal("full lifecycle registry has no executable fixture")
	return ""
}

func mutateFullLifecycleFixtureWithoutExactConnectorResponse(t *testing.T, contracts string) {
	t.Helper()
	manifestPath := filepath.Join(contracts, "bot", "package.yaml")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read lifecycle bot manifest: %v", err)
	}
	const connectorImport = "connector_packs:\n  imports:\n    - provider: telegram\n      tool: telegram.send_message\n"
	if !strings.Contains(string(raw), connectorImport) {
		t.Fatalf("lifecycle bot manifest has no exact Telegram connector import:\n%s", raw)
	}
	writeReleaseFile(t, manifestPath, strings.Replace(string(raw), connectorImport, "", 1))
	writeReleaseFile(t, filepath.Join(contracts, "bot", "tools.yaml"), `telegram.send_message:
  category: provider_connector
  description: Deliberately unmocked transport for source-admission proof.
  handler_type: http
  http:
    method: POST
    url: https://unreachable.invalid/send
  input_schema:
    type: object
    properties:
      chat_id: {type: string}
      text: {type: string}
    required: [chat_id, text]
  output_schema:
    type: object
`)
}
