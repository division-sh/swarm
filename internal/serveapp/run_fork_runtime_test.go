package serveapp

import (
	"bytes"
	"context"
	"database/sql"

	"fmt"
	"os"
	"path/filepath"

	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"

	"github.com/division-sh/swarm/internal/runtime/runfork"

	"github.com/division-sh/swarm/internal/store/storetest"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	runforkrevision "github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
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
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	repo := repoRootForTest()
	borrowedRoot := t.TempDir()
	copyReleaseFixtureTree(t, filepath.Join(repo, "tests", "tier1-primitives", "test-emits-multiple"), borrowedRoot)
	bundleHash, _ := registerRunForkCLIContractCatalog(t, context.Background(), db, borrowedRoot)
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
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	configPath := os.Getenv("SWARM_CONFIG")
	rawConfig, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read run-fork config: %v", err)
	}
	writeRuntimeConfigText(t, configPath, string(rawConfig)+"workspace:\n  backend: host\n")
	bundleHash, _ := registerRunForkCLIContractCatalog(t, context.Background(), db, filepath.Join(repoRootForTest(), doctorAgentContractsPath))
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

func seedRunForkCLIActivationSource(t *testing.T, db *sql.DB, runID, entityID, eventID string, at time.Time) string {
	t.Helper()
	seedRunForkCLIActivationSourceWithoutRevision(t, db, runID, entityID, eventID, at)
	captureRunForkCLIRevision(t, db, runID, runforkrevision.AllFamilies()...)
	return stageRunForkCLIForkPoint(t, db, runID, entityID, at.Add(time.Millisecond))
}

func stageRunForkCLIForkPoint(t *testing.T, db *sql.DB, runID, entityID string, at time.Time) string {
	t.Helper()
	eventID := uuid.NewString()
	envelope := events.EventEnvelope{Scope: events.EventScopeGlobal}
	if entityID != "" {
		envelope = events.EnvelopeForEntityID(events.EventEnvelope{}, entityID)
	}
	storetest.InsertExistingRunRootEventRecord(t, context.Background(), db, authoractivityfixture.DialectPostgres,
		eventID, runID, "fork.cli.checkpoint", eventtest.Producer(events.EventProducerExternal, "test"), []byte(`{}`), envelope, at)
	return eventID
}

func seedRunForkCLIActivationSourceWithoutRevision(t *testing.T, db *sql.DB, runID, entityID, eventID string, at time.Time, contractRoot ...string) {
	t.Helper()
	ctx := context.Background()
	sourceRoot := filepath.Join(repoRootForTest(), "tests", "tier1-primitives", "test-emits-multiple")
	if len(contractRoot) > 1 {
		t.Fatal("activation fixture requires one exact source contract")
	}
	if len(contractRoot) == 1 {
		sourceRoot = contractRoot[0]
	}
	bundleHash, selected := registerRunForkCLIContractCatalog(t, ctx, db, sourceRoot)
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
		Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: at.Add(-time.Minute),
		BundleHash: bundleHash,
	})
	storetest.InsertExistingRunRootEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres, eventID, runID, "fork.cli.activate",
		eventtest.Producer(events.EventProducerExternal, "test"), []byte(`{}`), events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), at)
	seedRunForkCLIConstruction(t, selected, runID, bundleHash, at)
}

func seedRunForkCLISelectedExecutionSource(t *testing.T, db *sql.DB, runID, entityID, eventID, bundleHash string, at time.Time) {
	t.Helper()
	seedRunForkSelectedExecutionSourceEvent(t, db, runID, entityID, eventID, bundleHash, "item.received", "test-node", at)
}

func registerRunForkCLIContractCatalog(t *testing.T, ctx context.Context, db *sql.DB, sourceRoot string) (string, storetest.RunFixtureStore) {
	t.Helper()
	repo := repoRootForTest()
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repo, sourceRoot)
	if err != nil {
		t.Fatalf("load run-fork selected contracts %s: %v", sourceRoot, err)
	}
	catalog, err := runtimecontracts.BuildDurableDataCatalog(bundle)
	if err != nil {
		t.Fatalf("project run-fork selected source data %s: %v", sourceRoot, err)
	}
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	if _, err := selected.EnsureSourceArtifactWithData(ctx, bundle.SourceArtifact, catalog); err != nil {
		t.Fatalf("register run-fork selected source %s: %v", sourceRoot, err)
	}
	return bundle.SourceArtifact.BundleHash(), selected
}

func seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter(t *testing.T, db *sql.DB, runID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	storetest.InsertDiagnosticDirectEventRecordForRun(t, ctx, db, authoractivityfixture.DialectPostgres, eventID, runID, "", "runtime",
		[]byte(`{"level":"info","message":"diagnostic platform row must remain lineage-only"}`), at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance, outcome, reason_code, side_effects, processed_at
		)
		VALUES (
			$1::uuid, 'platform', 'pipeline', NULL, NULL,
			'dead_letter', 'runtime_log_pipeline_dead_letter', '{}'::jsonb, $2
		)
	`, eventID, at); err != nil {
		t.Fatalf("seed diagnostic platform receipt: %v", err)
	}
	captureRunForkCLIRevision(t, db, runID, runforkrevision.FamilyEvents, runforkrevision.FamilyEventReceipts)
}

func runForkPlanHasBlocker(blockers []runfork.RunForkUnsupportedBlocker, code string) bool {
	for _, blocker := range blockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

func runForkPlanHasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func runForkPlanHasBoundary(values []runfork.RunForkSelectedContractExecutionBoundary, concept, disposition string) bool {
	for _, value := range values {
		if value.Concept == concept && value.Disposition == disposition {
			return true
		}
	}
	return false
}

func runForkReadinessFactHas(values []runfork.RunForkSelectedContractReadinessFact, fact string) bool {
	for _, value := range values {
		if value.Fact == fact {
			return true
		}
	}
	return false
}

func runForkReadinessFactHasDisposition(values []runfork.RunForkSelectedContractReadinessFact, fact, disposition string) bool {
	for _, value := range values {
		if value.Fact == fact && value.Disposition == disposition {
			return true
		}
	}
	return false
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
