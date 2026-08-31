package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestRunServeRuntimeDevScratchPersistsExactBundleAndRunSource(t *testing.T) {
	repo, contractsPath, opts := devScratchRuntimeFixture(t)
	process := startServeRuntimeTestProcessAtRepo(t, repo, opts)
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())

	var identity apiv1.RuntimeIdentityResult
	requireServedJSONRPCResult(t, endpoint+"/v1/rpc", "runtime.identity", map[string]any{}, &identity)
	if len(identity.BundleSources) != 1 || identity.BundleSources[0].BundleSource != "persisted" {
		t.Fatalf("runtime identity = %#v, want one persisted source", identity)
	}
	bundleHash := identity.BundleSources[0].BundleHash
	var started struct {
		RunID string `json:"run_id"`
	}
	requireServedJSONRPCResult(t, endpoint+"/v1/rpc", "run.start", map[string]any{
		"bundle_hash": bundleHash,
		"event_name":  "fulfillment.requested",
		"payload":     map[string]any{"order_id": "dev-scratch-source"},
	}, &started)
	if strings.TrimSpace(started.RunID) == "" {
		t.Fatal("run.start returned no run_id")
	}

	db := openDevScratchReadback(t, repo)
	defer db.Close()
	var catalogHash string
	if err := db.QueryRow(`SELECT bundle_hash FROM bundles WHERE bundle_hash = ?`, bundleHash).Scan(&catalogHash); err != nil {
		t.Fatalf("read persisted dev bundle catalog row: %v", err)
	}
	var runHash, runSource string
	if err := db.QueryRow(`SELECT bundle_hash, bundle_source FROM runs WHERE run_id = ?`, started.RunID).Scan(&runHash, &runSource); err != nil {
		t.Fatalf("read persisted dev run source: %v", err)
	}
	if catalogHash != bundleHash || runHash != bundleHash || runSource != "persisted" {
		t.Fatalf("catalog/run source = %q {%q,%q}, want exact persisted %q", catalogHash, runHash, runSource, bundleHash)
	}
	if contractsPath != repo {
		t.Fatalf("fixture contracts %q are not the canonical project root %q", contractsPath, repo)
	}
}

func TestRunServeRuntimeDevScratchRunForkLifecycleSQLite(t *testing.T) {
	repo := canonicalrouting.CopyRootIngressServedFollowUp(t)
	opts := devScratchRuntimeOptions(t, repo)
	process := startServeRuntimeTestProcessAtRepo(t, repo, opts)
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"

	var identity apiv1.RuntimeIdentityResult
	requireServedJSONRPCResult(t, endpoint, "runtime.identity", map[string]any{}, &identity)
	if len(identity.BundleSources) != 1 {
		t.Fatalf("runtime identity = %#v, want one source", identity)
	}
	var started struct {
		RunID string `json:"run_id"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.start", map[string]any{
		"bundle_hash": identity.BundleSources[0].BundleHash,
		"event_name":  "item.received",
		"payload":     map[string]any{"item_id": "dev-scratch-fork"},
	}, &started)
	if strings.TrimSpace(started.RunID) == "" {
		t.Fatal("run.start returned no run_id")
	}
	db := openDevScratchReadback(t, repo)
	defer db.Close()
	waitServedRunDeliveryQuiescence(t, db, "sqlite", started.RunID)
	var published struct {
		EventID string `json:"event_id"`
		RunID   string `json:"run_id"`
	}
	requireServedJSONRPCResult(t, endpoint, "event.publish", map[string]any{
		"bundle_hash":     identity.BundleSources[0].BundleHash,
		"run_id":          started.RunID,
		"event_name":      "item.processed",
		"payload":         map[string]any{"item_id": "review"},
		"idempotency_key": "issue-2361-dev-scratch-fork-point",
	}, &published)
	if published.RunID != started.RunID || strings.TrimSpace(published.EventID) == "" {
		t.Fatalf("event.publish result = %#v", published)
	}
	waitServedRunDeliveryQuiescence(t, db, "sqlite", started.RunID)
	requireServedRunStatus(t, endpoint, started.RunID, "completed")

	var fork apiv1.RunForkExecutionResult
	forkResponse := requestServedJSONRPCWithTimeout(t, endpoint, "run.fork", map[string]any{
		"source_run_id":         started.RunID,
		"fork_event_id":         published.EventID,
		"confirm_source_freeze": true,
		"idempotency_key":       "issue-2361-dev-scratch-run-fork",
	}, 30*time.Second)
	if forkResponse.Error != nil {
		t.Fatalf("run.fork error = %#v\nserve output:\n%s", forkResponse.Error, process.outputString())
	}
	if err := json.Unmarshal(forkResponse.Result, &fork); err != nil {
		t.Fatalf("decode run.fork result: %v\n%s", err, forkResponse.Result)
	}
	if fork.SourceRunID != started.RunID || fork.ForkEventID != published.EventID || fork.ForkRunID == "" || fork.SourceFrozen || fork.SourceRunStatus != "completed" || fork.ExecutedEventCount != 1 {
		t.Fatalf("run.fork result = %#v", fork)
	}
	waitServedRunDeliveryQuiescence(t, db, "sqlite", fork.ForkRunID)
	requireServedRunStatus(t, endpoint, fork.ForkRunID, "completed")

	var runReadback map[string]any
	requireServedJSONRPCResult(t, endpoint, "run.get", map[string]any{"run_id": fork.ForkRunID}, &runReadback)
	for method, params := range map[string]map[string]any{
		"entity.list":       {"run_id": fork.ForkRunID, "limit": 500},
		"event.list":        {"filter": map[string]any{"run_id": fork.ForkRunID}, "limit": 500},
		"conversation.list": {"run_id": fork.ForkRunID, "limit": 500},
	} {
		var readback map[string]any
		requireServedJSONRPCResult(t, endpoint, method, params, &readback)
		if len(readback) == 0 {
			t.Fatalf("%s returned empty result envelope", method)
		}
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("dev scratch run-fork process exit code = %d\n%s", code, process.outputString())
	}
}

func TestRunServeRuntimeDevScratchNextInvocationHasNoPredecessor(t *testing.T) {
	repo, contractsPath, opts := devScratchRuntimeFixture(t)
	promoteDevScratchFixtureToStanding(t, contractsPath)
	durablePath := filepath.Join(repo, ".swarm", "stores", "dev.db")
	if err := os.MkdirAll(filepath.Dir(durablePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(durablePath, []byte("normal-durable-state"), 0o600); err != nil {
		t.Fatal(err)
	}

	first := startServeRuntimeTestProcessAtRepo(t, repo, opts)
	first.waitForReadyLine()
	firstEndpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString())
	var firstIdentity apiv1.RuntimeIdentityResult
	requireServedJSONRPCResult(t, firstEndpoint+"/v1/rpc", "runtime.identity", map[string]any{}, &firstIdentity)
	firstHash := firstIdentity.BundleSources[0].BundleHash
	firstStanding := requireSingleServedStandingRun(t, firstEndpoint+"/v1/rpc", firstHash)
	firstHistory := requireServedStandingHistory(t, firstEndpoint+"/v1/rpc", firstStanding.RunID)
	if code := first.stop(); code != 0 {
		t.Fatalf("first dev scratch process exit code = %d\n%s", code, first.outputString())
	}

	packagePath := filepath.Join(contractsPath, "package.yaml")
	body, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(body), `version: "1.0.0"`, `version: "1.0.1"`, 1)
	if changed == string(body) {
		t.Fatalf("fixture package version was not replaceable:\n%s", body)
	}
	if err := os.WriteFile(packagePath, []byte(changed), 0o644); err != nil {
		t.Fatal(err)
	}

	second := startServeRuntimeTestProcessAtRepo(t, repo, opts)
	second.waitForReadyLine()
	secondEndpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString())
	var secondIdentity apiv1.RuntimeIdentityResult
	requireServedJSONRPCResult(t, secondEndpoint+"/v1/rpc", "runtime.identity", map[string]any{}, &secondIdentity)
	secondHash := secondIdentity.BundleSources[0].BundleHash
	if secondHash == firstHash || secondIdentity.BundleSources[0].BundleSource != "persisted" {
		t.Fatalf("second identity = %#v, want new persisted source replacing %s", secondIdentity, firstHash)
	}
	secondStanding := requireSingleServedStandingRun(t, secondEndpoint+"/v1/rpc", secondHash)
	if secondStanding.RunID != firstStanding.RunID || !secondStanding.Origin.Equal(firstStanding.Origin) {
		t.Fatalf("fresh dev semantic identity = %#v, want deterministic recurrence of %#v", secondStanding, firstStanding)
	}
	if !secondStanding.StartedAt.After(firstStanding.StartedAt) {
		t.Fatalf("fresh dev started_at = %s, want later than predecessor %s", secondStanding.StartedAt, firstStanding.StartedAt)
	}
	secondHistory := requireServedStandingHistory(t, secondEndpoint+"/v1/rpc", secondStanding.RunID)
	assertServedStandingHistoryReplaced(t, firstHistory, secondHistory)

	db := openDevScratchReadback(t, repo)
	defer db.Close()
	var predecessorRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM bundles WHERE bundle_hash = ?`, firstHash).Scan(&predecessorRows); err != nil {
		t.Fatal(err)
	}
	if predecessorRows != 0 {
		t.Fatalf("predecessor bundle rows = %d, want fresh epoch", predecessorRows)
	}
	durable, err := os.ReadFile(durablePath)
	if err != nil || string(durable) != "normal-durable-state" {
		t.Fatalf("normal dev.db changed: body=%q err=%v", durable, err)
	}
}

func promoteDevScratchFixtureToStanding(t *testing.T, contractsPath string) {
	t.Helper()
	packagePath := filepath.Join(contractsPath, "package.yaml")
	body, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := "  - {id: fulfillment, flow: fulfillment, mode: static}"
	standing := "  - id: fulfillment\n    flow: fulfillment\n    mode: singleton\n    activation: standing"
	updated := strings.Replace(string(body), ordinary, standing, 1)
	if updated == string(body) {
		t.Fatalf("dev scratch fixture flow declaration was not replaceable:\n%s", body)
	}
	if err := os.WriteFile(packagePath, []byte(updated), 0o644); err != nil {
		t.Fatal(err)
	}
	schemaPath := filepath.Join(contractsPath, "flows", "fulfillment", "schema.yaml")
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	updatedSchema := strings.Replace(string(schema), "mode: static", "mode: singleton", 1)
	if updatedSchema == string(schema) {
		t.Fatalf("dev scratch fixture flow schema mode was not replaceable:\n%s", schema)
	}
	if err := os.WriteFile(schemaPath, []byte(updatedSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contractsPath, "flows", "fulfillment", "entities.yaml"), []byte("fulfillment: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func requireSingleServedStandingRun(t *testing.T, endpoint, bundleHash string) operatorread.RunHeader {
	t.Helper()
	var result struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"bundle_hash": bundleHash, "limit": 500}, &result)
	var standing []operatorread.RunHeader
	for _, run := range result.Runs {
		if run.Origin.Kind() == runtimerunlifecycle.OriginStandingGeneration {
			standing = append(standing, run)
		}
	}
	if len(standing) != 1 {
		t.Fatalf("run.list for bundle %s returned %d standing runs, want one: %#v", bundleHash, len(standing), result.Runs)
	}
	return standing[0]
}

type servedStandingHistory struct {
	eventIDs    map[string]struct{}
	deliveryIDs map[string]struct{}
	mailboxIDs  map[string]struct{}
}

func requireServedStandingHistory(t *testing.T, endpoint, runID string) servedStandingHistory {
	t.Helper()
	var events operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{
		"filter": map[string]any{"run_id": runID}, "limit": 500,
	}, &events)
	var listed struct {
		Items []mailbox.V1Item `json:"items"`
	}
	requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"run_id": runID, "limit": 200}, &listed)
	history := servedStandingHistory{
		eventIDs:    make(map[string]struct{}, len(events.Events)),
		deliveryIDs: make(map[string]struct{}),
		mailboxIDs:  make(map[string]struct{}, len(listed.Items)),
	}
	for _, event := range events.Events {
		history.eventIDs[event.EventID] = struct{}{}
		for _, delivery := range event.Deliveries {
			history.deliveryIDs[delivery.DeliveryID] = struct{}{}
		}
	}
	for _, item := range listed.Items {
		history.mailboxIDs[item.MailboxID] = struct{}{}
	}
	return history
}

func assertServedStandingHistoryReplaced(t *testing.T, predecessor, fresh servedStandingHistory) {
	t.Helper()
	for label, pair := range map[string][2]map[string]struct{}{
		"event":    {predecessor.eventIDs, fresh.eventIDs},
		"delivery": {predecessor.deliveryIDs, fresh.deliveryIDs},
		"mailbox":  {predecessor.mailboxIDs, fresh.mailboxIDs},
	} {
		for id := range pair[0] {
			if _, exists := pair[1][id]; exists {
				t.Fatalf("fresh dev history retained predecessor %s %s", label, id)
			}
		}
	}
}

func devScratchRuntimeFixture(t *testing.T) (string, string, cliapp.ServeOptions) {
	t.Helper()
	contractsPath := canonicalrouting.WriteNovelDerivedScenarioBundleWithRootInput(t)
	return contractsPath, contractsPath, devScratchRuntimeOptions(t, contractsPath)
}

func devScratchRuntimeOptions(t *testing.T, contractsPath string) cliapp.ServeOptions {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	t.Setenv("PATH", t.TempDir())
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	configPath := writeMockAgentRuntimeConfig(t, storebackend.BackendSQLite.String(), "")
	return cliapp.ServeOptions{
		ConfigPath: configPath, ContractsPath: contractsPath,
		PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
		APIListenAddr:    "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		Dev: true, NoFeed: true, SelfCheck: true, RequireBundleMatch: false, NoRequireBundleMatch: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
}

func openDevScratchReadback(t *testing.T, repo string) *sql.DB {
	t.Helper()
	path := filepath.Join(repo, ".swarm", "stores", "dev-scratch.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("open dev scratch readback %s: %v", path, err)
	}
	return db
}
