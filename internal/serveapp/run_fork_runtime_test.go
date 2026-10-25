package serveapp

import (
	"bytes"
	"context"

	"fmt"
	"os"
	"path/filepath"

	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/store/storetest"

	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkRuntimeOwnerHarness_ActivateWithBundleHashReachesSelectedActivationGate(t *testing.T) {
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), t.TempDir(), []string{
		"--activate",
		"--bundle-hash", storetest.SemanticFixtureBundleHash,
		"--run", uuid.NewString(),
	}, &buf)
	if code != 1 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d, want runtime failure after parsing; output=%s", code, buf.String())
	}
	if strings.Contains(buf.String(), "invalid --bundle-hash") {
		t.Fatalf("output = %q, want --activate to consume canonical selected activation gate rather than parse-level bundle rejection", buf.String())
	}
}

func TestRunForkRuntimeOwnerHarness_PersistedBundleDoesNotRequireAmbientSource(t *testing.T) {
	dsn := testutil.StartPostgresDSN(t)
	selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
	setPostgresEnvFromDSN(t, dsn)
	repo := repoRootForTest()
	borrowedRoot := t.TempDir()
	copyReleaseFixtureTree(t, filepath.Join(repo, "tests", "tier1-primitives", "test-emits-multiple"), borrowedRoot)
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repo, borrowedRoot)
	if err != nil {
		t.Fatal(err)
	}
	storetest.RequireBundleDataCatalog(t, context.Background(), selected, bundle)
	bundleHash := bundle.SourceArtifact.BundleHash()
	if err := os.RemoveAll(borrowedRoot); err != nil {
		t.Fatalf("remove admitted source root: %v", err)
	}

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repo, []string{
		"--store", "postgres",
		"--bundle-hash", bundleHash,
		"--run", uuid.NewString(),
		"--at", uuid.NewString(),
	}, &buf)
	if code != 1 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d, want missing source-run failure; output=%s", code, buf.String())
	}
	if strings.Contains(buf.String(), "workspace data source") || strings.Contains(buf.String(), "--data") {
		t.Fatalf("output = %q, retired ambient data-source authority survived", buf.String())
	}
	if !strings.Contains(buf.String(), "source run") && !strings.Contains(buf.String(), "run not found") {
		t.Fatalf("output = %q, want execution to pass data projection admission and reach source-run lookup", buf.String())
	}
	if _, err := os.Stat(borrowedRoot); !os.IsNotExist(err) {
		t.Fatalf("admitted source root stat error = %v, want execution independent of removed ambient source", err)
	}
}

func TestRunForkRuntimeOwnerHarness_SelectedContractsExecutesExplicitHostRefusal(t *testing.T) {
	dsn := testutil.StartPostgresDSN(t)
	selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
	setPostgresEnvFromDSN(t, dsn)
	configPath := os.Getenv("SWARM_CONFIG")
	rawConfig, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read run-fork config: %v", err)
	}
	writeRuntimeConfigText(t, configPath, string(rawConfig)+"workspace:\n  backend: host\n")
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), filepath.Join(repoRootForTest(), doctorAgentContractsPath))
	if err != nil {
		t.Fatal(err)
	}
	storetest.RequireBundleDataCatalog(t, context.Background(), selected, bundle)
	bundleHash := bundle.SourceArtifact.BundleHash()
	var out bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repoRootForTest(), []string{
		"--store", "postgres",
		"--config", configPath,
		"--bundle-hash", bundleHash,
		"--run", uuid.NewString(),
		"--at", uuid.NewString(),
	}, &out)
	if code != 1 {
		t.Fatalf("selected-contract run-fork code = %d, want 1\n%s", code, out.String())
	}
	assertClaudeHostRefusal(t, out.String())
}

func TestRunForkRuntimeOwnerHarness_NonDryRunWithoutMaterializeOnlyStaysFailClosed(t *testing.T) {
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), t.TempDir(), []string{
		"--run", uuid.NewString(),
		"--at", uuid.NewString(),
	}, &buf)
	if code != 2 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d, want 2; output=%s", code, buf.String())
	}
	if !strings.Contains(buf.String(), "mutating fork execution requires --bundle-hash") {
		t.Fatalf("output = %q, want fail-closed fork execution message", buf.String())
	}
}

func setPostgresEnvFromDSN(t *testing.T, dsn string) {
	t.Helper()
	connection, err := testpostgres.ParseConnection(dsn)
	if err != nil {
		t.Fatalf("parse canonical test Postgres DSN: %v", err)
	}
	parsed := connection.Parameters()
	t.Setenv("PGPASSWORD", parsed.Password)
	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	t.Setenv("SWARM_CONFIG", configPath)
	writeRuntimeConfigText(t, configPath, fmt.Sprintf(`runtime: {}
store:
  backend: postgres
database:
  host: %q
  port: %d
  name: %q
  user: %q
  password_env: PGPASSWORD
  sslmode: %q
  pool_size: 5
llm:
  backend: claude_cli
  session:
    lock_ttl: 10s
    rotate_after_turns: 40
    rotate_on_parse_failures: 3
  claude_cli:
    command: true
    timeout: 2s
    output_format: json
`, parsed.Host, parsed.Port, parsed.Database, parsed.User, parsed.SSLMode))
}

func writeRuntimeConfigText(t *testing.T, path, configText string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(configText), 0o644); err != nil {
		t.Fatalf("write runtime config %s: %v", path, err)
	}
}
