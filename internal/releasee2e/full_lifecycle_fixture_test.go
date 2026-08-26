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
	root := fullLifecycleFixtureRoot(t)
	if err := validateFullLifecycleFixtureRegistry(root, fullLifecycleFixtureRegistry); err != nil {
		t.Fatal(err)
	}
}

func TestFullLifecycleFixtureRegistryRejectsUnclassifiedPackage(t *testing.T) {
	root := t.TempDir()
	copyReleaseTree(t, fullLifecycleFixtureRoot(t), root)
	writeReleaseFile(t, filepath.Join(root, "unclassified", "package.yaml"), "name: unclassified\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\n")
	err := validateFullLifecycleFixtureRegistry(root, fullLifecycleFixtureRegistry)
	if err == nil || !strings.Contains(err.Error(), "unclassified package roots: unclassified") {
		t.Fatalf("fixture registry mutation error = %v, want exact unclassified root", err)
	}
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
		if !containsReleaseString(discovered, path) {
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

func containsReleaseString(values []string, want string) bool {
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
		t.Fatalf("lifecycle bot manifest no longer has exact Telegram connector import:\n%s", raw)
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
