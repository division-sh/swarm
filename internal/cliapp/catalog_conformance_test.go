package cliapp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/testcatalog"
)

type requiredConformanceBundle struct {
	ID         string
	Root       string
	ConfigPath string
}

func TestCatalogRequiredVerifyAll(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	setRequiredConformanceCredentials(t)
	repoRoot := RepoRoot()
	inventory, err := testcatalog.Load(repoRoot)
	if err != nil {
		t.Fatalf("load catalog inventory: %v", err)
	}
	configPath := writeTestVerifyRuntimeConfig(t)
	verified := 0
	for _, fixture := range inventory.Fixtures {
		fixture := fixture
		t.Run(fixture.RelativePath, func(t *testing.T) {
			t.Setenv("SWARM_BOOT_WARNINGS_FATAL", catalogWarningsFatal(fixture))
			opts := defaultVerifyCommandOptions()
			opts.contractsPath = fixture.Root
			opts.configPath = configPath
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runVerifyCommandWithOutput(context.Background(), repoRoot, opts, &stdout, &stderr)
			verified++

			switch fixture.Metadata.Disposition {
			case testcatalog.DispositionRuntime:
				if code != 0 {
					t.Fatalf("runtime fixture failed supported verify: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
				}
			case testcatalog.DispositionVerifyOnly:
				assertCatalogVerifyOnlyResult(t, repoRoot, fixture, code, stdout.String(), stderr.String())
			case testcatalog.DispositionRetired:
				// Retired rows are still verified for census completeness but receive no result or claim credit.
			default:
				t.Fatalf("unsupported catalog disposition %q", fixture.Metadata.Disposition)
			}
		})
	}
	if verified != len(inventory.Fixtures) {
		t.Fatalf("supported verify executions = %d, want %d", verified, len(inventory.Fixtures))
	}

	exampleBundles, err := discoverRequiredExampleBundles(repoRoot, configPath)
	if err != nil {
		t.Fatalf("discover required example bundles: %v", err)
	}
	archetypeBundles := materializeRequiredArchetypeBundles(t)
	passingBundles := append(exampleBundles, archetypeBundles...)
	for _, failure := range verifyRequiredPassingBundles(context.Background(), repoRoot, passingBundles) {
		t.Error(failure)
	}
}

func setRequiredConformanceCredentials(t testing.TB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", path)
	store, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		t.Fatalf("create required conformance credential store: %v", err)
	}
	if err := store.Set(context.Background(), "telegram_bot_token", "catalog-required-test-token"); err != nil {
		t.Fatalf("set required conformance credential: %v", err)
	}
}

func TestCatalogRequiredVerifyGateMutationDiscoversAndNamesBrokenBundles(t *testing.T) {
	setRequiredConformanceCredentials(t)
	repoRoot := RepoRoot()
	corpusRoot := t.TempDir()
	mutations := []struct {
		path     string
		field    string
		identity string
	}{
		{path: "broken-first", field: "unknown_wave0_first", identity: "examples/broken-first"},
		{path: filepath.Join("nested", "broken-second"), field: "unknown_wave0_second", identity: "examples/nested/broken-second"},
	}
	for _, mutation := range mutations {
		root := filepath.Join(corpusRoot, "examples", mutation.path)
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatalf("create broken bundle %s: %v", mutation.identity, err)
		}
		body := fmt.Sprintf("name: broken-example\n%s: true\n", mutation.field)
		if err := os.WriteFile(filepath.Join(root, "package.yaml"), []byte(body), 0o600); err != nil {
			t.Fatalf("write broken bundle %s: %v", mutation.identity, err)
		}
	}
	configPath := writeTestVerifyRuntimeConfig(t)
	bundles, err := discoverRequiredExampleBundles(corpusRoot, configPath)
	if err != nil {
		t.Fatalf("discover mutated bundles: %v", err)
	}
	failures := verifyRequiredPassingBundles(context.Background(), repoRoot, bundles)
	if len(failures) != len(mutations) {
		t.Fatalf("mutation failures = %#v, want %d discovered failures", failures, len(mutations))
	}
	joined := strings.Join(failures, "\n")
	for _, mutation := range mutations {
		if !strings.Contains(joined, mutation.identity) || !strings.Contains(joined, mutation.field) {
			t.Fatalf("mutation failures = %#v, want %s and exact loader evidence %s", failures, mutation.identity, mutation.field)
		}
	}
}

func TestCatalogRequiredVerifyGateRejectsDiscoveredPresentZeroOptionalFile(t *testing.T) {
	setRequiredConformanceCredentials(t)
	repoRoot := RepoRoot()
	corpusRoot := t.TempDir()
	bundleRoot := filepath.Join(corpusRoot, "examples", "nested", "present-zero")
	if err := os.MkdirAll(bundleRoot, 0o755); err != nil {
		t.Fatalf("create mutated bundle: %v", err)
	}
	files := map[string]string{
		"package.yaml": "name: present-zero\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n",
		"schema.yaml":  "name: present-zero\n",
		"agents.yaml":  "{}\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(bundleRoot, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	bundles, err := discoverRequiredExampleBundles(corpusRoot, writeTestVerifyRuntimeConfig(t))
	if err != nil {
		t.Fatalf("discover mutated bundles: %v", err)
	}
	failures := verifyRequiredPassingBundles(context.Background(), repoRoot, bundles)
	if len(failures) != 1 ||
		!strings.Contains(failures[0], "examples/nested/present-zero") ||
		!strings.Contains(failures[0], "agents.yaml declares nothing") ||
		!strings.Contains(failures[0], "delete the file") {
		t.Fatalf("present-zero failures = %#v, want bundle, file, and omission remediation", failures)
	}
}

func TestCatalogRequiredVerifyGateRejectsInvalidGeneratedArchetypeConfig(t *testing.T) {
	setRequiredConformanceCredentials(t)
	repoRoot := RepoRoot()
	bundles := materializeRequiredArchetypeBundles(t)
	var mutated *requiredConformanceBundle
	for i := range bundles {
		if bundles[i].ID == "archetypes/zero-agent-automation" {
			mutated = &bundles[i]
			break
		}
	}
	if mutated == nil {
		t.Fatal("zero-agent-automation is missing from admittedArchetypes")
	}
	if err := os.WriteFile(mutated.ConfigPath, []byte("runtime:\n  execution_posture: invalid\n"), 0o600); err != nil {
		t.Fatalf("mutate generated archetype config: %v", err)
	}
	failures := verifyRequiredPassingBundles(context.Background(), repoRoot, []requiredConformanceBundle{*mutated})
	if len(failures) != 1 || !strings.Contains(failures[0], mutated.ID) || !strings.Contains(failures[0], `runtime.execution_posture must be exactly live or mock_only`) {
		t.Fatalf("generated-config mutation failures = %#v, want named archetype and exact config evidence", failures)
	}
}

func discoverRequiredExampleBundles(corpusRoot, configPath string) ([]requiredConformanceBundle, error) {
	examplesRoot := filepath.Join(corpusRoot, "examples")
	var bundles []requiredConformanceBundle
	err := filepath.WalkDir(examplesRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "package.yaml" {
			return nil
		}
		root := filepath.Dir(path)
		relative, err := filepath.Rel(corpusRoot, root)
		if err != nil {
			return err
		}
		bundles = append(bundles, requiredConformanceBundle{
			ID:         filepath.ToSlash(relative),
			Root:       root,
			ConfigPath: configPath,
		})
		return nil
	})
	sort.Slice(bundles, func(i, j int) bool { return bundles[i].ID < bundles[j].ID })
	return bundles, err
}

func materializeRequiredArchetypeBundles(t testing.TB) []requiredConformanceBundle {
	t.Helper()
	ids := make([]string, 0, len(admittedArchetypes))
	for id := range admittedArchetypes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	bundles := make([]requiredConformanceBundle, 0, len(ids))
	for _, id := range ids {
		destination := filepath.Join(t.TempDir(), id)
		if err := scaffoldArchetype(io.Discard, id, destination); err != nil {
			t.Fatalf("materialize required archetype %s: %v", id, err)
		}
		bundles = append(bundles, requiredConformanceBundle{
			ID:         "archetypes/" + id,
			Root:       filepath.Clean(filepath.Join(destination, admittedArchetypes[id].WorkingDir)),
			ConfigPath: filepath.Clean(filepath.Join(destination, admittedArchetypes[id].WorkingDir, "swarm.yaml")),
		})
	}
	return bundles
}

func verifyRequiredPassingBundles(ctx context.Context, repoRoot string, bundles []requiredConformanceBundle) []string {
	var failures []string
	for _, bundle := range bundles {
		opts := defaultVerifyCommandOptions()
		opts.contractsPath = bundle.Root
		opts.configPath = bundle.ConfigPath
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := runVerifyCommandWithOutput(ctx, repoRoot, opts, &stdout, &stderr)
		if code != 0 {
			failures = append(failures, fmt.Sprintf("%s failed supported verify/boot: code=%d stdout=%q stderr=%q", bundle.ID, code, stdout.String(), stderr.String()))
		}
	}
	return failures
}

func catalogWarningsFatal(fixture testcatalog.Fixture) string {
	if fixture.Metadata.Disposition == testcatalog.DispositionVerifyOnly && fixture.Metadata.Verify == testcatalog.VerifyWarning {
		return "false"
	}
	return "true"
}

func assertCatalogVerifyOnlyResult(t *testing.T, repoRoot string, fixture testcatalog.Fixture, code int, stdout, stderr string) {
	t.Helper()
	switch fixture.Metadata.Verify {
	case testcatalog.VerifyPass:
		if code != 0 {
			t.Fatalf("verify-pass fixture failed: code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
	case testcatalog.VerifyWarning:
		if code != 0 {
			t.Fatalf("verify-warning fixture failed: code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
		assertCatalogVerifyDiagnostic(t, repoRoot, fixture, stdout, stderr)
	case testcatalog.VerifyReject:
		if code == 0 {
			t.Fatalf("verify-%s fixture succeeded: stdout=%q stderr=%q", fixture.Metadata.Verify, stdout, stderr)
		}
		assertCatalogVerifyDiagnostic(t, repoRoot, fixture, stdout, stderr)
	default:
		t.Fatalf("unsupported verify-only result %q", fixture.Metadata.Verify)
	}
}

func assertCatalogVerifyDiagnostic(t *testing.T, repoRoot string, fixture testcatalog.Fixture, stdout, stderr string) {
	t.Helper()
	want := fixture.Metadata.Diagnostic
	if want == nil {
		t.Fatal("verify warning/reject fixture has no diagnostic metadata")
	}
	combined := stdout + "\n" + stderr
	if !strings.Contains(combined, want.Contains) {
		t.Fatalf("verify-%s output missing teaching evidence %q: stdout=%q stderr=%q", fixture.Metadata.Verify, want.Contains, stdout, stderr)
	}
	if strings.Contains(combined, want.Category) {
		return
	}
	_, _, err := NewSwarmWorkflowModule(repoRoot, fixture.Root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err); ok && diagnostic.Code == want.Category {
		return
	}
	t.Fatalf("verify-%s path missing canonical diagnostic category %q: stdout=%q stderr=%q loader_error=%v", fixture.Metadata.Verify, want.Category, stdout, stderr, err)
}
