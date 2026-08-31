package releasee2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	fullLifecycleAPIToken      = "full-lifecycle-release-e2e-token"
	fullLifecycleSigningSecret = "full-lifecycle-telegram-secret"
	fullLifecycleBotToken      = "full-lifecycle-telegram-bot-token"
	fullLifecycleStartupLimit  = 60 * time.Second
	fullLifecycleRunLimit      = 90 * time.Second
)

type fullLifecycleJourneyKind string

const (
	fullLifecycleGraceful       fullLifecycleJourneyKind = "graceful"
	fullLifecycleCrashIntrinsic fullLifecycleJourneyKind = "crash-intrinsic-recover"
	fullLifecycleDevFresh       fullLifecycleJourneyKind = "dev-fresh-epoch"
)

type fullLifecycleJourney struct {
	name    string
	backend string
	kind    fullLifecycleJourneyKind
}

var fullLifecycleJourneys = []fullLifecycleJourney{
	{name: "J1-sqlite-graceful", backend: "sqlite", kind: fullLifecycleGraceful},
	{name: "J2-postgres-graceful", backend: "postgres", kind: fullLifecycleGraceful},
	{name: "J3-sqlite-crash-intrinsic-recover", backend: "sqlite", kind: fullLifecycleCrashIntrinsic},
	{name: "J4-postgres-crash-intrinsic-recover", backend: "postgres", kind: fullLifecycleCrashIntrinsic},
	{name: "J5-sqlite-dev-fresh", backend: "sqlite", kind: fullLifecycleDevFresh},
}

func TestCompiledProcessFullLifecycleSQLiteSmoke(t *testing.T) {
	if profile, continuous := goldenContinuousProofProfile(t); continuous {
		t.Skipf("complete J1-J5 lifecycle profile supersedes SQLite smoke in %s", profile)
	}
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	assertFullLifecycleSourceAdmission(t, binary, filepath.Join(releaseRoot, "source-admission"))
	journey := fullLifecycleJourneys[0]
	root := filepath.Join(releaseRoot, journey.name)
	runFullLifecycleJourney(t, binary, root, goldenSQLiteStore(filepath.Join(root, "store")), journey)
}

func TestCompiledProcessFullLifecycleJourneysSQLitePostgres(t *testing.T) {
	profile, continuous := goldenContinuousProofProfile(t)
	if !continuous {
		t.Skipf("complete J1-J5 lifecycle profile requires full/nightly, got %q", profile)
	}
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for the complete J1-J5 lifecycle profile", goldenPostgresEnv)
	}
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	assertFullLifecycleSourceAdmission(t, binary, filepath.Join(releaseRoot, "source-admission"))

	limit := make(chan struct{}, 2)
	for _, journey := range fullLifecycleJourneys {
		journey := journey
		t.Run(journey.name, func(t *testing.T) {
			t.Parallel()
			limit <- struct{}{}
			defer func() { <-limit }()
			root := filepath.Join(releaseRoot, journey.name)
			store := goldenSQLiteStore(filepath.Join(root, "store"))
			if journey.backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			runFullLifecycleJourney(t, binary, root, store, journey)
		})
	}
}

func prepareFullLifecycleProject(t *testing.T, binary, root string, store goldenStoreSelection, dev bool) releaseProcessSpec {
	t.Helper()
	ancestor := filepath.Join(root, "hostile-go-checkout")
	projectRoot := filepath.Join(ancestor, "yaml-project")
	writeReleaseFile(t, filepath.Join(ancestor, "go.mod"), "module hostile-full-lifecycle-ancestor\n\ngo 1.23.0\n")
	contracts := filepath.Join(projectRoot, "contracts")
	copyReleaseTree(t, fullLifecycleExecutableSource(t), contracts)
	if _, err := os.Stat(filepath.Join(projectRoot, "go.mod")); !os.IsNotExist(err) {
		t.Fatalf("full lifecycle child project must remain YAML-only: %v", err)
	}
	configPath := filepath.Join(projectRoot, "swarm.yaml")
	writeReleaseFile(t, configPath, fullLifecycleRuntimeConfig(store, dev))
	writeReleaseFile(t, filepath.Join(projectRoot, "api-token"), fullLifecycleAPIToken+"\n")
	env := goldenProcessEnv(t, projectRoot, store.passwordEnv, 0)
	credentialFreeEnv := goldenProcessEnv(t, projectRoot, "", 0)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	verify := runReleaseCommand(t, fullLifecycleStartupLimit, projectRoot, env, "", binary,
		"verify", "--config", "swarm.yaml", "--contracts", "contracts", "--json")
	assertFullLifecycleVerifySuccess(t, verify)
	for key, value := range map[string]string{
		"webhook_signing.telegram": fullLifecycleSigningSecret,
		"telegram_bot_token":       fullLifecycleBotToken,
	} {
		result := runReleaseCommand(t, 30*time.Second, projectRoot, credentialFreeEnv, value+"\n", binary,
			"secrets", "set", key, "--stdin")
		if result.err != nil {
			t.Fatalf("set lifecycle secret %s: %v\n%s", key, result.err, result.output)
		}
	}
	return releaseProcessSpec{
		BinaryPath: binary,
		WorkingDir: projectRoot,
		ConfigPath: "swarm.yaml",
		Contracts:  "contracts",
		Store:      store.name,
		Dev:        dev,
		APIPort:    freeReleaseTCPPort(t),
		MCPPort:    freeReleaseTCPPort(t),
		TokenFile:  "api-token",
		Token:      fullLifecycleAPIToken,
		Env:        env,
	}
}

func fullLifecycleRuntimeConfig(store goldenStoreSelection, dev bool) string {
	runtimeConfig := "runtime:\n  execution_posture: mock_only\n"
	storeConfig := store.configYAML
	if dev {
		storeConfig = "store:\n  backend: sqlite\n"
	}
	return runtimeConfig +
		"llm:\n  backend: claude_cli\n" +
		"workspace:\n  backend: host\n" +
		storeConfig
}

func assertFullLifecycleSourceAdmission(t *testing.T, binary, root string) {
	t.Helper()
	for _, test := range []struct {
		name   string
		mutate bool
	}{
		{name: "positive"},
		{name: "missing-exact-response", mutate: true},
	} {
		t.Run("source-"+test.name, func(t *testing.T) {
			ancestor := filepath.Join(root, test.name, "hostile-go-checkout")
			project := filepath.Join(ancestor, "yaml-project")
			writeReleaseFile(t, filepath.Join(ancestor, "go.mod"), "module hostile-source-admission\n\ngo 1.23.0\n")
			contracts := filepath.Join(project, "contracts")
			copyReleaseTree(t, fullLifecycleExecutableSource(t), contracts)
			if test.mutate {
				mutateFullLifecycleFixtureWithoutExactConnectorResponse(t, contracts)
			}
			store := goldenSQLiteStore(filepath.Join(project, "store"))
			writeReleaseFile(t, filepath.Join(project, "swarm.yaml"), fullLifecycleRuntimeConfig(store, false))
			env := goldenProcessEnv(t, project, "", 0)
			assertGoldenProcessHasNoExternalExecutables(t, env)
			result := runReleaseCommand(t, fullLifecycleStartupLimit, project, env, "", binary,
				"verify", "--config", "swarm.yaml", "--contracts", "contracts", "--json")
			if !test.mutate {
				assertFullLifecycleVerifySuccess(t, result)
				return
			}
			if result.err == nil || !strings.Contains(result.output, "exact mock response") ||
				!strings.Contains(result.output, "telegram.send_message") {
				t.Fatalf("missing-response verify = err:%v output:%s, want exact connector-response refusal", result.err, result.output)
			}
		})
	}
}

func assertFullLifecycleVerifySuccess(t *testing.T, result releaseCommandResult) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("full lifecycle verify failed: %v\n%s", result.err, result.output)
	}
	var payload struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(result.output)), &payload); err != nil || !payload.OK {
		t.Fatalf("full lifecycle verify result = err:%v output:%s", err, result.output)
	}
}

func runFullLifecycleJourney(t *testing.T, binary, root string, store goldenStoreSelection, journey fullLifecycleJourney) {
	t.Helper()
	started := time.Now()
	processSpec := prepareFullLifecycleProject(t, binary, root, store, journey.kind == fullLifecycleDevFresh)
	startReady := func() *releaseServeProcess {
		process := startReleaseServe(t, processSpec)
		ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleStartupLimit)
		err := process.waitReady(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		assertFullLifecycleReadySurface(t, process)
		return process
	}

	process := startReady()
	bundleHash := requireFullLifecycleHealth(t, process.rpc)
	standing := waitForFullLifecycleStandingRun(t, process.rpc, bundleHash, "", 0, "")
	lifecycleCard := waitForFullLifecycleCard(t, process.rpc, standing.RunID, "lifecycle_ready")
	if lifecycleCard.ListedTitle != "Confirm lifecycle readiness" || lifecycleCard.Snapshot.Title != lifecycleCard.ListedTitle {
		t.Fatalf("lifecycle decision card title = %#v, want exact authored title", lifecycleCard)
	}
	assertFullLifecycleTimerCardinality(t, process.rpc, standing.RunID, 1)
	decideFullLifecycleCard(t, process.rpc, lifecycleCard, "lifecycle-gate-"+standing.RunID)
	assertFullLifecycleCardDecided(t, process.rpc, lifecycleCard.CardID)
	waitForFullLifecycleRunStatus(t, process.rpc, standing.RunID, "running")

	switch journey.kind {
	case fullLifecycleGraceful:
		runFullLifecycleGracefulJourney(t, startReady, process, bundleHash, standing)
	case fullLifecycleCrashIntrinsic:
		runFullLifecycleCrashIntrinsicJourney(t, startReady, process, bundleHash, standing)
	case fullLifecycleDevFresh:
		runFullLifecycleDevFreshJourney(t, startReady, process, bundleHash, standing, lifecycleCard.CardID)
	default:
		t.Fatalf("unsupported full lifecycle journey %q", journey.kind)
	}
	t.Logf("compiled lifecycle timing: journey=%s backend=%s elapsed=%s", journey.name, journey.backend, time.Since(started))
}

func runFullLifecycleGracefulJourney(
	t *testing.T,
	startReady func() *releaseServeProcess,
	process *releaseServeProcess,
	bundleHash string,
	standing fullLifecycleRun,
) {
	t.Helper()
	firstReceipt := sendFullLifecycleTelegramUpdate(t, process, 1001, 42)
	approveFullLifecycleEffect(t, process.rpc, standing.RunID, "graceful-first")
	waitForFullLifecycleConvergence(t, process, standing.RunID, 1)
	first := requireFullLifecycleReceiptEvents(t, process.rpc, standing.RunID, 1001, firstReceipt)
	if err := process.stopAndWait(10 * time.Second); err != nil {
		t.Fatalf("graceful lifecycle stop: %v\n%s", err, process.output.String())
	}
	process = startReady()
	if restartedHash := requireFullLifecycleHealth(t, process.rpc); restartedHash != bundleHash {
		t.Fatalf("graceful restart bundle = %s, want %s", restartedHash, bundleHash)
	}
	restored := waitForFullLifecycleStandingRun(t, process.rpc, bundleHash, standing.Origin.ServiceID, standing.Origin.Generation, standing.RunID)
	if restored.RunID != standing.RunID {
		t.Fatalf("graceful restart standing run = %s, want %s", restored.RunID, standing.RunID)
	}
	secondReceipt := sendFullLifecycleTelegramUpdate(t, process, 1002, 42)
	approveFullLifecycleEffect(t, process.rpc, standing.RunID, "graceful-second")
	waitForFullLifecycleConvergence(t, process, standing.RunID, 2)
	second := requireFullLifecycleReceiptEvents(t, process.rpc, standing.RunID, 1002, secondReceipt)
	assertFullLifecycleStandingReceiptContinuity(t, firstReceipt, secondReceipt)
	assertFullLifecycleSameRoute(t, first, second)
	assertFullLifecycleTimerCardinality(t, process.rpc, standing.RunID, 1)
}

func runFullLifecycleCrashIntrinsicJourney(
	t *testing.T,
	startReady func() *releaseServeProcess,
	process *releaseServeProcess,
	bundleHash string,
	standing fullLifecycleRun,
) {
	t.Helper()
	baselineReceipt := sendFullLifecycleTelegramUpdate(t, process, 2000, 42)
	approveFullLifecycleEffect(t, process.rpc, standing.RunID, "recovery-baseline")
	waitForFullLifecycleConvergence(t, process, standing.RunID, 1)
	baseline := requireFullLifecycleReceiptEvents(t, process.rpc, standing.RunID, 2000, baselineReceipt)
	pauseFullLifecycleRun(t, process.rpc, standing.RunID)
	checkpointReceipt := sendFullLifecycleTelegramUpdate(t, process, 2001, 42)
	checkpoint := waitForFullLifecycleCrashCheckpoint(t, process, standing.RunID, 2001, checkpointReceipt)
	if err := process.killAndWait(10 * time.Second); err != nil {
		t.Fatalf("force lifecycle process death: %v\n%s", err, process.output.String())
	}

	process = startReady()
	if recoveredHash := requireFullLifecycleHealth(t, process.rpc); recoveredHash != bundleHash {
		t.Fatalf("intrinsic recovery bundle = %s, want %s", recoveredHash, bundleHash)
	}
	recovered := waitForFullLifecycleStandingRun(t, process.rpc, bundleHash, standing.Origin.ServiceID, standing.Origin.Generation, standing.RunID)
	if recovered.RunID != standing.RunID || recovered.Status != "running" || recovered.ControlReason != "standing_reconcile" ||
		!recovered.StartedAt.Equal(standing.StartedAt) {
		t.Fatalf("recovered standing run = %#v, want same started identity normalized to running by standing_reconcile from %#v", recovered, standing)
	}
	approveFullLifecycleEffect(t, process.rpc, standing.RunID, "recovered-old")
	waitForFullLifecycleConvergence(t, process, standing.RunID, 2)
	assertFullLifecycleDeliveryCompleted(t, process.rpc, standing.RunID, checkpoint)
	old := requireFullLifecycleEventByID(t, process.rpc, standing.RunID, checkpoint.EventID)
	assertFullLifecycleSameRoute(t, baseline, old)
	before := captureFullLifecycleEvidence(t, process.rpc, standing.RunID)

	freshReceipt := sendFullLifecycleTelegramUpdate(t, process, 2002, 42)
	approveFullLifecycleEffect(t, process.rpc, standing.RunID, "recovered-fresh")
	waitForFullLifecycleConvergence(t, process, standing.RunID, 3)
	fresh := requireFullLifecycleReceiptEvents(t, process.rpc, standing.RunID, 2002, freshReceipt)
	assertFullLifecycleStandingReceiptContinuity(t, baselineReceipt, checkpointReceipt, freshReceipt)
	assertFullLifecycleSameRoute(t, old, fresh)
	assertFullLifecycleOldEvidenceUnchanged(t, process.rpc, standing.RunID, checkpoint, before)
	assertFullLifecycleTimerCardinality(t, process.rpc, standing.RunID, 1)
}

func runFullLifecycleDevFreshJourney(
	t *testing.T,
	startReady func() *releaseServeProcess,
	process *releaseServeProcess,
	bundleHash string,
	standing fullLifecycleRun,
	lifecycleCardID string,
) {
	t.Helper()
	baselineReceipt := sendFullLifecycleTelegramUpdate(t, process, 3000, 42)
	approveFullLifecycleEffect(t, process.rpc, standing.RunID, "dev-baseline")
	waitForFullLifecycleConvergence(t, process, standing.RunID, 1)
	requireFullLifecycleReceiptEvents(t, process.rpc, standing.RunID, 3000, baselineReceipt)
	pauseFullLifecycleRun(t, process.rpc, standing.RunID)
	checkpointReceipt := sendFullLifecycleTelegramUpdate(t, process, 3001, 42)
	checkpoint := waitForFullLifecycleCrashCheckpoint(t, process, standing.RunID, 3001, checkpointReceipt)
	predecessorHistory := captureFullLifecycleHistory(t, process.rpc, standing.RunID)
	predecessorHistory.EventIDs[checkpoint.EventID] = true
	predecessorHistory.DeliveryIDs[checkpoint.Delivery.DeliveryID] = true
	predecessorHistory.CardIDs[lifecycleCardID] = true
	if err := process.killAndWait(10 * time.Second); err != nil {
		t.Fatalf("force dev lifecycle process death: %v\n%s", err, process.output.String())
	}
	process = startReady()
	if restartedHash := requireFullLifecycleHealth(t, process.rpc); restartedHash != bundleHash {
		t.Fatalf("fresh dev epoch bundle = %s, want unchanged source %s", restartedHash, bundleHash)
	}
	fresh := waitForFullLifecycleStandingRun(t, process.rpc, bundleHash, standing.Origin.ServiceID, standing.Origin.Generation, standing.RunID)
	if !fresh.StartedAt.After(standing.StartedAt) {
		t.Fatalf("fresh dev standing started_at = %s, want later than predecessor %s (checkpoint=%#v)", fresh.StartedAt, standing.StartedAt, checkpoint)
	}
	card := waitForFullLifecycleCard(t, process.rpc, fresh.RunID, "lifecycle_ready")
	assertFullLifecycleTimerCardinality(t, process.rpc, fresh.RunID, 1)
	assertFullLifecycleHistoryAbsent(t, process.rpc, fresh.RunID, predecessorHistory)
	decideFullLifecycleCard(t, process.rpc, card, "lifecycle-gate-"+fresh.RunID)
	assertFullLifecycleCardDecided(t, process.rpc, card.CardID)
	waitForFullLifecycleRunStatus(t, process.rpc, fresh.RunID, "running")
	freshReceipt := sendFullLifecycleTelegramUpdate(t, process, 3002, 42)
	approveFullLifecycleEffect(t, process.rpc, fresh.RunID, "dev-fresh")
	waitForFullLifecycleConvergence(t, process, fresh.RunID, 1)
	requireFullLifecycleReceiptEvents(t, process.rpc, fresh.RunID, 3002, freshReceipt)
}

func assertFullLifecycleReadySurface(t *testing.T, process *releaseServeProcess) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		output := process.output.String()
		for _, text := range []string{"ready in", "telegram webhook", "webhook_signing.telegram", "bound"} {
			if !strings.Contains(output, text) {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("wait for lifecycle human readiness surface: %v\n%s", err, process.output.String())
	}
	output := process.output.String()
	for _, secret := range []string{fullLifecycleSigningSecret, fullLifecycleBotToken} {
		if strings.Contains(output, secret) {
			t.Fatalf("lifecycle readiness output leaks secret %q:\n%s", secret, output)
		}
	}
	if strings.Contains(output, "\x1b[") {
		t.Fatalf("lifecycle readiness output contains ANSI control:\n%s", output)
	}
}

func requireFullLifecycleHealth(t *testing.T, rpc *releaseRPCClient) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var health struct {
		Alive            bool   `json:"alive"`
		Ready            bool   `json:"ready"`
		DBOK             bool   `json:"db_ok"`
		RuntimeOK        bool   `json:"runtime_ok"`
		ExecutionPosture string `json:"execution_posture"`
		Bundle           struct {
			BundleHash      string `json:"bundle_hash"`
			WorkflowName    string `json:"workflow_name"`
			WorkflowVersion string `json:"workflow_version"`
		} `json:"bundle"`
	}
	if err := rpc.call(ctx, "health.check", map[string]any{}, &health); err != nil {
		t.Fatal(err)
	}
	if !health.Alive || !health.Ready || !health.DBOK || !health.RuntimeOK || health.ExecutionPosture != "mock_only" ||
		health.Bundle.WorkflowName != "full-lifecycle-standing" || health.Bundle.WorkflowVersion != "1.0.0" ||
		strings.TrimSpace(health.Bundle.BundleHash) == "" {
		t.Fatalf("health.check = %#v, want ready full-lifecycle-standing@1.0.0 mock_only runtime", health)
	}
	return health.Bundle.BundleHash
}

type fullLifecycleOrigin struct {
	Kind       string `json:"kind"`
	ServiceID  string `json:"service_id"`
	Generation int64  `json:"generation"`
}

type fullLifecycleRun struct {
	RunID         string              `json:"run_id"`
	Status        string              `json:"status"`
	Origin        fullLifecycleOrigin `json:"origin"`
	StartedAt     time.Time           `json:"started_at"`
	ControlReason string              `json:"control_reason"`
}

func listFullLifecycleRuns(ctx context.Context, rpc *releaseRPCClient, bundleHash string) ([]fullLifecycleRun, error) {
	var result struct {
		Runs []fullLifecycleRun `json:"runs"`
	}
	if err := rpc.call(ctx, "run.list", map[string]any{"bundle_hash": bundleHash, "limit": 500}, &result); err != nil {
		return nil, err
	}
	return result.Runs, nil
}

func waitForFullLifecycleStandingRun(t *testing.T, rpc *releaseRPCClient, bundleHash, serviceID string, generation int64, runID string) fullLifecycleRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleRunLimit)
	defer cancel()
	var match fullLifecycleRun
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		runs, err := listFullLifecycleRuns(ctx, rpc, bundleHash)
		if err != nil {
			return false, err
		}
		matches := make([]fullLifecycleRun, 0, 1)
		for _, run := range runs {
			if run.Origin.Kind != "standing_generation" || (serviceID != "" && run.Origin.ServiceID != serviceID) ||
				(generation != 0 && run.Origin.Generation != generation) || (runID != "" && run.RunID != runID) {
				continue
			}
			matches = append(matches, run)
		}
		if len(matches) == 0 {
			return false, nil
		}
		if len(matches) != 1 {
			return false, fmt.Errorf("run.list returned %d matching standing generations: %#v", len(matches), matches)
		}
		match = matches[0]
		return match.RunID != "" && match.Origin.ServiceID != "" && match.Origin.Generation > 0, nil
	})
	if err != nil {
		t.Fatalf("wait for standing generation service=%q generation=%d run=%q: %v; last=%#v", serviceID, generation, runID, err, match)
	}
	return match
}

func waitForFullLifecycleRunStatus(t *testing.T, rpc *releaseRPCClient, runID, want string) fullLifecycleRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleRunLimit)
	defer cancel()
	var last fullLifecycleRun
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		var result struct {
			Run fullLifecycleRun `json:"run"`
		}
		if err := rpc.call(ctx, "run.get", map[string]any{"run_id": runID}, &result); err != nil {
			return false, err
		}
		last = result.Run
		return last.Status == want, nil
	})
	if err != nil {
		t.Fatalf("wait for run %s status %s: %v; last=%#v", runID, want, err, last)
	}
	return last
}

type fullLifecycleCard struct {
	CardID          string `json:"card_id"`
	RunID           string `json:"run_id"`
	Status          string `json:"status"`
	ExecutionMode   string `json:"execution_mode"`
	Decision        string `json:"decision"`
	Title           string `json:"title"`
	ListedTitle     string `json:"-"`
	CardContentHash string `json:"card_content_hash"`
	Snapshot        struct {
		Decision string `json:"decision"`
		Title    string `json:"title"`
	} `json:"snapshot"`
}

type fullLifecycleCardProjection struct {
	Kind         string            `json:"kind"`
	DecisionCard fullLifecycleCard `json:"decision_card"`
}

func waitForFullLifecycleCard(t *testing.T, rpc *releaseRPCClient, runID, decision string) fullLifecycleCard {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleRunLimit)
	defer cancel()
	var card fullLifecycleCard
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		var result struct {
			Items []fullLifecycleCardProjection `json:"items"`
		}
		if err := rpc.call(ctx, "mailbox.list", map[string]any{"status": "pending", "run_id": runID, "limit": 200}, &result); err != nil {
			return false, err
		}
		matches := make([]fullLifecycleCard, 0, 1)
		for _, item := range result.Items {
			if item.Kind == "decision_card" && item.DecisionCard.Decision == decision {
				matches = append(matches, item.DecisionCard)
			}
		}
		if len(matches) == 0 {
			return false, nil
		}
		if len(matches) != 1 {
			return false, fmt.Errorf("mailbox.list returned %d pending %s cards: %#v", len(matches), decision, matches)
		}
		var detail fullLifecycleCardProjection
		if err := rpc.call(ctx, "mailbox.get", map[string]any{"mailbox_id": matches[0].CardID}, &detail); err != nil {
			return false, err
		}
		card = detail.DecisionCard
		card.ListedTitle = matches[0].Title
		return detail.Kind == "decision_card" && card.CardID != "" && card.CardContentHash != "" &&
			card.Decision == decision && card.Snapshot.Decision == decision, nil
	})
	if err != nil {
		t.Fatalf("wait for pending decision %s run=%s: %v; last=%#v", decision, runID, err, card)
	}
	return card
}

func decideFullLifecycleCard(t *testing.T, rpc *releaseRPCClient, card fullLifecycleCard, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var result struct {
		Status              string `json:"status"`
		IdempotencyReplayed bool   `json:"idempotency_replayed"`
	}
	if err := rpc.call(ctx, "mailbox.decide", map[string]any{
		"card_id": card.CardID, "verdict": "approve", "fields": map[string]any{},
		"observed_content_hash": card.CardContentHash, "idempotency_key": key,
	}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "decided" || result.IdempotencyReplayed {
		t.Fatalf("mailbox.decide %s = %#v, want first decided completion", card.CardID, result)
	}
}

func assertFullLifecycleCardDecided(t *testing.T, rpc *releaseRPCClient, cardID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var detail fullLifecycleCardProjection
	if err := rpc.call(ctx, "mailbox.get", map[string]any{"mailbox_id": cardID}, &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Kind != "decision_card" || detail.DecisionCard.CardID != cardID || detail.DecisionCard.Status != "decided" {
		t.Fatalf("mailbox.get decided card = %#v, want %s decided", detail, cardID)
	}
}

func approveFullLifecycleEffect(t *testing.T, rpc *releaseRPCClient, runID, key string) {
	t.Helper()
	card := waitForFullLifecycleCard(t, rpc, runID, "send_telegram_message")
	if card.ExecutionMode != "mock" {
		t.Fatalf("connector decision card execution mode = %q, want mock", card.ExecutionMode)
	}
	decideFullLifecycleCard(t, rpc, card, "connector-"+key+"-"+card.CardID)
	assertFullLifecycleCardDecided(t, rpc, card.CardID)
}

type fullLifecycleIngressReceipt struct {
	Status        string   `json:"status"`
	EntityID      string   `json:"entity_id"`
	PublicationID string   `json:"publication_id"`
	EventIDs      []string `json:"event_ids"`
	EventNames    []string `json:"event_names"`
}

func assertFullLifecycleStandingReceiptContinuity(t *testing.T, first fullLifecycleIngressReceipt, subsequent ...fullLifecycleIngressReceipt) {
	t.Helper()
	for index, receipt := range subsequent {
		if receipt.EntityID != first.EntityID {
			t.Fatalf("standing ingress receipt entity changed at occurrence %d: first=%#v current=%#v", index+2, first, receipt)
		}
	}
}

func sendFullLifecycleTelegramUpdate(t *testing.T, process *releaseServeProcess, updateID, chatID int) fullLifecycleIngressReceipt {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"from":{"id":%d},"chat":{"id":%d,"type":"private"},"text":"lifecycle %d"}}`, updateID, updateID, chatID, chatID, updateID))
	request, err := http.NewRequest(http.MethodPost, process.apiBase+"/webhooks/chat/telegram", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", fullLifecycleSigningSecret)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("send lifecycle Telegram webhook: %v\n%s", err, process.output.String())
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var receipt fullLifecycleIngressReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatalf("decode lifecycle webhook status=%d body=%q: %v", response.StatusCode, raw, err)
	}
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("lifecycle webhook status=%d receipt=%#v\n%s", response.StatusCode, receipt, process.output.String())
	}
	if receipt.Status != "accepted" || strings.TrimSpace(receipt.EntityID) == "" || strings.TrimSpace(receipt.PublicationID) == "" {
		t.Fatalf("lifecycle webhook receipt omitted accepted identity: %#v", receipt)
	}
	wantNames := []string{"inbound.telegram", "inbound.telegram.text_message"}
	if len(receipt.EventIDs) != len(wantNames) || len(receipt.EventNames) != len(wantNames) {
		t.Fatalf("lifecycle webhook child receipt = ids:%#v names:%#v, want exact ordered two-child receipt", receipt.EventIDs, receipt.EventNames)
	}
	identities := []string{receipt.EntityID, receipt.PublicationID, receipt.EventIDs[0], receipt.EventIDs[1]}
	seen := make(map[string]bool, len(identities))
	for _, identity := range identities {
		identity = strings.TrimSpace(identity)
		if identity == "" || seen[identity] {
			t.Fatalf("lifecycle webhook receipt has empty or duplicate identity: %#v", receipt)
		}
		seen[identity] = true
	}
	for i, want := range wantNames {
		if receipt.EventNames[i] != want {
			t.Fatalf("lifecycle webhook child %d = (%s, %q), want (%s, %q): %#v", i, receipt.EventIDs[i], receipt.EventNames[i], receipt.EventIDs[i], want, receipt)
		}
	}
	return receipt
}

func pauseFullLifecycleRun(t *testing.T, rpc *releaseRPCClient, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var result map[string]any
	if err := rpc.call(ctx, "run.pause", map[string]any{"run_id": runID, "idempotency_key": "lifecycle-pause-" + runID}, &result); err != nil {
		t.Fatal(err)
	}
	waitForFullLifecycleRunStatus(t, rpc, runID, "paused")
}

type fullLifecycleEvent struct {
	EventID                  string                       `json:"event_id"`
	EventName                string                       `json:"event_name"`
	EntityID                 string                       `json:"entity_id"`
	RunID                    string                       `json:"run_id"`
	SourceEventID            string                       `json:"source_event_id"`
	OperatorReferenceEventID string                       `json:"operator_reference_event_id"`
	CreatedAt                time.Time                    `json:"created_at"`
	Source                   string                       `json:"source"`
	ProducerType             string                       `json:"producer_type"`
	ExecutionMode            string                       `json:"execution_mode"`
	Payload                  map[string]any               `json:"payload"`
	Deliveries               []fullLifecycleEventDelivery `json:"deliveries"`
	NoDelivery               *fullLifecycleNoDelivery     `json:"no_delivery"`
	DeadLetters              []map[string]any             `json:"dead_letters"`
}

type fullLifecycleNoDelivery struct {
	Reason string           `json:"reason"`
	Plans  []map[string]any `json:"plans"`
}

type fullLifecycleEventDelivery struct {
	DeliveryID     string                      `json:"delivery_id"`
	SubscriberType string                      `json:"subscriber_type"`
	SubscriberID   string                      `json:"subscriber_id"`
	Target         fullLifecycleDeliveryTarget `json:"target"`
	SessionID      string                      `json:"session_id"`
	Status         string                      `json:"status"`
	ReasonCode     string                      `json:"reason_code"`
	Failure        map[string]any              `json:"failure"`
	RetryCount     int                         `json:"retry_count"`
	RetryScheduled bool                        `json:"retry_scheduled"`
	Terminal       bool                        `json:"terminal"`
	CreatedAt      *time.Time                  `json:"created_at"`
	StartedAt      *time.Time                  `json:"started_at"`
	FinishedAt     *time.Time                  `json:"finished_at"`
	DeadLetters    []map[string]any            `json:"dead_letters"`
}

type fullLifecycleDeliveryTarget struct {
	Kind         string `json:"kind"`
	FlowID       string `json:"flow_id"`
	FlowInstance string `json:"flow_instance"`
	EntityID     string `json:"entity_id"`
}

func listFullLifecycleEvents(ctx context.Context, rpc *releaseRPCClient, runID string) ([]fullLifecycleEvent, error) {
	var all []fullLifecycleEvent
	cursor := ""
	seenCursors := map[string]bool{}
	seenEvents := map[string]bool{}
	for {
		params := map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 200}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Events     []fullLifecycleEvent `json:"events"`
			NextCursor string               `json:"next_cursor"`
		}
		if err := rpc.call(ctx, "event.list", params, &page); err != nil {
			return nil, err
		}
		for _, event := range page.Events {
			if event.EventID == "" || event.RunID != runID || seenEvents[event.EventID] {
				return nil, fmt.Errorf("event.list returned malformed or duplicate event %#v", event)
			}
			seenEvents[event.EventID] = true
			all = append(all, event)
		}
		if page.NextCursor == "" {
			return all, nil
		}
		if seenCursors[page.NextCursor] {
			return nil, fmt.Errorf("event.list repeated cursor %q", page.NextCursor)
		}
		seenCursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
}

type fullLifecycleHistory struct {
	EventIDs    map[string]bool
	DeliveryIDs map[string]bool
	CardIDs     map[string]bool
}

func captureFullLifecycleHistory(t *testing.T, rpc *releaseRPCClient, runID string) fullLifecycleHistory {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := listFullLifecycleEvents(ctx, rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	history := fullLifecycleHistory{
		EventIDs:    make(map[string]bool, len(events)),
		DeliveryIDs: make(map[string]bool),
		CardIDs:     make(map[string]bool),
	}
	for _, event := range events {
		if event.EventID == "" || history.EventIDs[event.EventID] {
			t.Fatalf("public history has duplicate or empty event identity: %#v", event)
		}
		history.EventIDs[event.EventID] = true
		for _, delivery := range event.Deliveries {
			if delivery.DeliveryID == "" || history.DeliveryIDs[delivery.DeliveryID] {
				t.Fatalf("public history has duplicate or empty delivery identity: %#v", delivery)
			}
			history.DeliveryIDs[delivery.DeliveryID] = true
		}
	}
	cardIDs, err := listFullLifecycleCardIDs(ctx, rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, cardID := range cardIDs {
		if history.CardIDs[cardID] {
			t.Fatalf("public history has duplicate decision card identity %s", cardID)
		}
		history.CardIDs[cardID] = true
	}
	return history
}

func listFullLifecycleCardIDs(ctx context.Context, rpc *releaseRPCClient, runID string) ([]string, error) {
	var cardIDs []string
	cursor := ""
	seenCursors := map[string]bool{}
	for {
		params := map[string]any{"run_id": runID, "limit": 200}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Items      []fullLifecycleCardProjection `json:"items"`
			NextCursor string                        `json:"next_cursor"`
		}
		if err := rpc.call(ctx, "mailbox.list", params, &page); err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			if item.Kind != "decision_card" {
				continue
			}
			if item.DecisionCard.CardID == "" {
				return nil, fmt.Errorf("mailbox.list returned decision card without card_id")
			}
			cardIDs = append(cardIDs, item.DecisionCard.CardID)
		}
		if page.NextCursor == "" {
			return cardIDs, nil
		}
		if seenCursors[page.NextCursor] {
			return nil, fmt.Errorf("mailbox.list repeated cursor %q", page.NextCursor)
		}
		seenCursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
}

func assertFullLifecycleHistoryAbsent(t *testing.T, rpc *releaseRPCClient, runID string, predecessor fullLifecycleHistory) {
	t.Helper()
	fresh := captureFullLifecycleHistory(t, rpc, runID)
	for eventID := range fresh.EventIDs {
		if predecessor.EventIDs[eventID] {
			t.Fatalf("fresh dev epoch retained predecessor event %s", eventID)
		}
	}
	for deliveryID := range fresh.DeliveryIDs {
		if predecessor.DeliveryIDs[deliveryID] {
			t.Fatalf("fresh dev epoch retained predecessor delivery %s", deliveryID)
		}
	}
	for cardID := range fresh.CardIDs {
		if predecessor.CardIDs[cardID] {
			t.Fatalf("fresh dev epoch retained predecessor decision card %s", cardID)
		}
	}
}

type fullLifecycleCrashCheckpoint struct {
	EventID  string
	Delivery fullLifecycleEventDelivery
}

func waitForFullLifecycleCrashCheckpoint(t *testing.T, process *releaseServeProcess, runID string, providerMessageReference int, receipt fullLifecycleIngressReceipt) fullLifecycleCrashCheckpoint {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleRunLimit)
	defer cancel()
	var checkpoint fullLifecycleCrashCheckpoint
	var diagnosis goldenDiagnosis
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		events, err := listFullLifecycleEvents(ctx, process.rpc, runID)
		if err != nil {
			return false, err
		}
		_, event, err := fullLifecycleReceiptEvents(events, receipt, providerMessageReference)
		if err != nil {
			return false, err
		}
		if len(event.Deliveries) == 0 {
			return false, nil
		}
		if len(event.Deliveries) != 1 {
			return false, fmt.Errorf("crash checkpoint delivery count = %d, want exactly 1: %#v", len(event.Deliveries), event)
		}
		delivery := event.Deliveries[0]
		if delivery.SubscriberType != "agent" || delivery.Status != "pending" || delivery.Terminal ||
			!strings.Contains(delivery.SubscriberID, "phrase-bot") || delivery.Target.Kind != "existing_entity" ||
			delivery.Target.FlowID != "telegram-chat" || delivery.Target.FlowInstance == "" {
			return false, fmt.Errorf("pending normalized route target is not exact: receipt=%#v event=%#v delivery=%#v", receipt, event, delivery)
		}
		checkpoint = fullLifecycleCrashCheckpoint{EventID: event.EventID, Delivery: delivery}
		if err := process.rpc.call(ctx, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis); err != nil {
			return false, err
		}
		return diagnosis.Run.Status == "paused" && diagnosis.TestQuiescence.ActiveDeliveries == 1, nil
	})
	if err != nil {
		t.Fatalf("wait for public lifecycle crash checkpoint: %v; checkpoint=%#v diagnosis=%#v\n%s", err, checkpoint, diagnosis, process.output.String())
	}
	return checkpoint
}

func assertFullLifecycleDeliveryCompleted(t *testing.T, rpc *releaseRPCClient, runID string, checkpoint fullLifecycleCrashCheckpoint) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := listFullLifecycleEvents(ctx, rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.EventID != checkpoint.EventID {
			continue
		}
		for _, delivery := range event.Deliveries {
			if delivery.DeliveryID == checkpoint.Delivery.DeliveryID {
				if delivery.Status != "delivered" || !delivery.Terminal {
					t.Fatalf("recovered checkpoint delivery = %#v, want terminal delivered", delivery)
				}
				return
			}
		}
	}
	t.Fatalf("event.list omitted recovered checkpoint delivery %#v", checkpoint)
}

func waitForFullLifecycleConvergence(t *testing.T, process *releaseServeProcess, runID string, ingressCount int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleRunLimit)
	defer cancel()
	var diagnosis goldenDiagnosis
	var last []fullLifecycleEvent
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		if err := process.rpc.call(ctx, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis); err != nil {
			return false, err
		}
		events, err := listFullLifecycleEvents(ctx, process.rpc, runID)
		if err != nil {
			return false, err
		}
		last = events
		return diagnosis.TestQuiescence.Ready && goldenActiveWork(diagnosis.TestQuiescence) == 0 &&
			countFullLifecycleEvents(events, "inbound.telegram") == ingressCount &&
			countFullLifecycleEvents(events, "inbound.telegram.text_message") == ingressCount &&
			countFullLifecycleEvents(events, "platform.activity_requested") == ingressCount &&
			countFullLifecycleEventSuffix(events, "/telegram.reply_requested") == ingressCount &&
			countFullLifecycleEventSuffix(events, "/telegram_send_message.succeeded") == ingressCount, nil
	})
	if err != nil {
		t.Fatalf("wait for lifecycle convergence run=%s ingress=%d: %v; diagnosis=%#v events=%#v\n%s", runID, ingressCount, err, diagnosis, last, process.output.String())
	}
	if diagnosis.Run.Status != "running" || len(diagnosis.FailedDeliveries) != 0 {
		t.Fatalf("lifecycle diagnosis = %#v, want running, quiescent, failure-free standing run", diagnosis)
	}
	seenDeliveries := map[string]bool{}
	for _, event := range last {
		if len(event.DeadLetters) != 0 {
			t.Fatalf("lifecycle event %s (%s) dead letters = %s", event.EventName, event.EventID, event.DeadLetters)
		}
		if event.EventName == "inbound.telegram" &&
			(len(event.Deliveries) != 0 || event.NoDelivery == nil || event.NoDelivery.Reason != "no_subscriber_by_design") {
			t.Fatalf("raw Telegram settlement = %#v, want no_subscriber_by_design without delivery", event)
		}
		if event.EventName == "platform.activity_requested" || strings.HasSuffix(event.EventName, "/telegram.reply_requested") ||
			strings.HasSuffix(event.EventName, "/telegram_send_message.succeeded") {
			if event.ExecutionMode != "mock" {
				t.Fatalf("lifecycle event %s execution mode = %q, want mock", event.EventName, event.ExecutionMode)
			}
		}
		for _, delivery := range event.Deliveries {
			if delivery.DeliveryID == "" || seenDeliveries[delivery.DeliveryID] {
				t.Fatalf("duplicate or empty delivery identity in event %s: %#v", event.EventID, delivery)
			}
			seenDeliveries[delivery.DeliveryID] = true
		}
	}
	assertFullLifecycleMockAgentReadback(t, ctx, process.rpc, runID)
}

func countFullLifecycleEvents(events []fullLifecycleEvent, name string) int {
	count := 0
	for _, event := range events {
		if event.EventName == name {
			count++
		}
	}
	return count
}

func countFullLifecycleEventSuffix(events []fullLifecycleEvent, suffix string) int {
	count := 0
	for _, event := range events {
		if strings.HasSuffix(event.EventName, suffix) {
			count++
		}
	}
	return count
}

func assertFullLifecycleTimerCardinality(t *testing.T, rpc *releaseRPCClient, runID string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := listFullLifecycleEvents(ctx, rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countFullLifecycleEvents(events, "platform.stage_timer"); got != want {
		t.Fatalf("platform.stage_timer count = %d, want %d: %#v", got, want, events)
	}
}

func requireFullLifecycleReceiptEvents(t *testing.T, rpc *releaseRPCClient, runID string, providerMessageReference int, receipt fullLifecycleIngressReceipt) fullLifecycleEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := listFullLifecycleEvents(ctx, rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	_, normalized, err := fullLifecycleReceiptEvents(events, receipt, providerMessageReference)
	if err != nil {
		t.Fatalf("join lifecycle webhook receipt to public events: %v; receipt=%#v all=%#v", err, receipt, events)
	}
	return normalized
}

func fullLifecycleReceiptEvents(events []fullLifecycleEvent, receipt fullLifecycleIngressReceipt, providerMessageReference int) (fullLifecycleEvent, fullLifecycleEvent, error) {
	byID := make(map[string]fullLifecycleEvent, len(events))
	var transportMatches []fullLifecycleEvent
	for _, event := range events {
		byID[event.EventID] = event
		if event.EventName == "inbound.telegram.text_message" && fullLifecycleEventHasTransport(event, providerMessageReference, "42") {
			transportMatches = append(transportMatches, event)
		}
	}
	if len(receipt.EventIDs) != 2 || len(receipt.EventNames) != 2 ||
		receipt.EventNames[0] != "inbound.telegram" || receipt.EventNames[1] != "inbound.telegram.text_message" {
		return fullLifecycleEvent{}, fullLifecycleEvent{}, fmt.Errorf("receipt is not the exact ordered raw/normalized pair: %#v", receipt)
	}
	raw, rawOK := byID[receipt.EventIDs[0]]
	normalized, normalizedOK := byID[receipt.EventIDs[1]]
	if !rawOK || !normalizedOK {
		return fullLifecycleEvent{}, fullLifecycleEvent{}, fmt.Errorf("event.list omitted receipt children raw=%t normalized=%t", rawOK, normalizedOK)
	}
	if raw.EventName != receipt.EventNames[0] || raw.EntityID != receipt.EntityID || len(raw.Deliveries) != 0 ||
		raw.NoDelivery == nil || raw.NoDelivery.Reason != "no_subscriber_by_design" || len(raw.DeadLetters) != 0 {
		return fullLifecycleEvent{}, fullLifecycleEvent{}, fmt.Errorf("raw standing event does not match exact receipt settlement: receipt=%#v event=%#v", receipt, raw)
	}
	if normalized.EventName != receipt.EventNames[1] || normalized.EntityID == "" || len(normalized.Deliveries) != 1 ||
		normalized.NoDelivery != nil || len(normalized.DeadLetters) != 0 {
		return fullLifecycleEvent{}, fullLifecycleEvent{}, fmt.Errorf("normalized event does not match exact receipt child: receipt=%#v event=%#v", receipt, normalized)
	}
	delivery := normalized.Deliveries[0]
	if delivery.SubscriberType != "agent" || !strings.Contains(delivery.SubscriberID, "phrase-bot") ||
		delivery.Target.EntityID != normalized.EntityID ||
		(delivery.Target.Kind != "existing_entity" && delivery.Target.Kind != "materializing_entity") ||
		delivery.Target.FlowID != "telegram-chat" || delivery.Target.FlowInstance == "" {
		return fullLifecycleEvent{}, fullLifecycleEvent{}, fmt.Errorf("normalized event/delivery target join is not exact: event=%#v delivery=%#v", normalized, delivery)
	}
	if len(transportMatches) != 1 || transportMatches[0].EventID != receipt.EventIDs[1] {
		return fullLifecycleEvent{}, fullLifecycleEvent{}, fmt.Errorf("transport occurrence matches = %#v, want only normalized receipt child %s", transportMatches, receipt.EventIDs[1])
	}
	return raw, normalized, nil
}

func fullLifecycleEventHasTransport(event fullLifecycleEvent, providerMessageReference int, conversationReference string) bool {
	message, ok := event.Payload["provider_message_reference"].(float64)
	conversation, conversationOK := event.Payload["conversation_reference"].(string)
	return ok && int(message) == providerMessageReference && message == float64(providerMessageReference) &&
		conversationOK && conversation == conversationReference
}

func requireFullLifecycleEventByID(t *testing.T, rpc *releaseRPCClient, runID, eventID string) fullLifecycleEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := listFullLifecycleEvents(ctx, rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.EventID == eventID {
			if event.EventName != "inbound.telegram.text_message" || len(event.Deliveries) != 1 {
				t.Fatalf("checkpoint event %s is not one exact normalized delivery: %#v", eventID, event)
			}
			return event
		}
	}
	t.Fatalf("event.list omitted event %s", eventID)
	return fullLifecycleEvent{}
}

func assertFullLifecycleSameRoute(t *testing.T, first, second fullLifecycleEvent) {
	t.Helper()
	if first.EventID == second.EventID {
		t.Fatalf("distinct transport occurrences reused event %s", first.EventID)
	}
	left, right := first.Deliveries[0], second.Deliveries[0]
	if left.SubscriberType != right.SubscriberType || left.SubscriberID != right.SubscriberID ||
		left.Target.FlowID != right.Target.FlowID || left.Target.FlowInstance != right.Target.FlowInstance ||
		left.Target.EntityID != right.Target.EntityID {
		t.Fatalf("same-conversation route changed: first=%#v second=%#v", first, second)
	}
	if right.Target.Kind != "existing_entity" {
		t.Fatalf("subsequent same-conversation route kind = %q, want existing_entity: %#v", right.Target.Kind, right)
	}
}

type fullLifecycleEvidenceSnapshot struct {
	EventFacts  map[string]string
	EventCounts map[string]int
}

func captureFullLifecycleEvidence(t *testing.T, rpc *releaseRPCClient, runID string) fullLifecycleEvidenceSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := listFullLifecycleEvents(ctx, rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := fullLifecycleEvidenceSnapshot{
		EventFacts:  make(map[string]string, len(events)),
		EventCounts: make(map[string]int),
	}
	for _, event := range events {
		if _, exists := snapshot.EventFacts[event.EventID]; exists {
			t.Fatalf("duplicate event identity in evidence snapshot: %s", event.EventID)
		}
		stable := event
		stable.Deliveries = append([]fullLifecycleEventDelivery(nil), event.Deliveries...)
		sort.Slice(stable.Deliveries, func(i, j int) bool {
			return stable.Deliveries[i].DeliveryID < stable.Deliveries[j].DeliveryID
		})
		encoded, err := json.Marshal(stable)
		if err != nil {
			t.Fatalf("marshal public event evidence %s: %v", event.EventID, err)
		}
		snapshot.EventFacts[event.EventID] = string(encoded)
		snapshot.EventCounts[event.EventName]++
	}
	return snapshot
}

func assertFullLifecycleOldEvidenceUnchanged(t *testing.T, rpc *releaseRPCClient, runID string, checkpoint fullLifecycleCrashCheckpoint, before fullLifecycleEvidenceSnapshot) {
	t.Helper()
	assertFullLifecycleDeliveryCompleted(t, rpc, runID, checkpoint)
	after := captureFullLifecycleEvidence(t, rpc, runID)
	for eventID, want := range before.EventFacts {
		got, exists := after.EventFacts[eventID]
		if !exists {
			t.Fatalf("fresh ingress removed old event %s", eventID)
		}
		if got != want {
			t.Fatalf("fresh ingress mutated old event/delivery facts for %s:\nbefore=%s\nafter=%s", eventID, want, got)
		}
	}
	if after.EventCounts["platform.stage_timer"] != before.EventCounts["platform.stage_timer"] {
		t.Fatalf("old timer cardinality changed after fresh ingress: before=%#v after=%#v", before.EventCounts, after.EventCounts)
	}
	for _, name := range []string{
		"inbound.telegram.text_message",
		"platform.activity_requested",
	} {
		if after.EventCounts[name] != before.EventCounts[name]+1 {
			t.Fatalf("event %s cardinality after fresh ingress = %d, want %d (before=%#v after=%#v)", name, after.EventCounts[name], before.EventCounts[name]+1, before.EventCounts, after.EventCounts)
		}
	}
}

func assertFullLifecycleMockAgentReadback(t *testing.T, ctx context.Context, rpc *releaseRPCClient, runID string) {
	t.Helper()
	var agents struct {
		Agents []goldenAgentSummary `json:"agents"`
	}
	if err := rpc.call(ctx, "agent.list", map[string]any{}, &agents); err != nil {
		t.Fatal(err)
	}
	foundAgent := false
	for _, agent := range agents.Agents {
		if strings.Contains(agent.AgentID, "phrase-bot") {
			foundAgent = true
			if agent.ExecutionMode != "mock" {
				t.Fatalf("agent.list phrase-bot = %#v, want mock execution", agent)
			}
		}
	}
	if !foundAgent {
		t.Fatalf("agent.list omitted phrase-bot mock owner: %#v", agents.Agents)
	}
	conversations := listGoldenConversations(t, ctx, rpc, runID)
	foundConversation := false
	for _, conversation := range conversations {
		if strings.Contains(conversation.AgentID, "phrase-bot") {
			foundConversation = true
			if conversation.ExecutionMode != "mock" || conversation.TurnCount < 1 {
				t.Fatalf("conversation.list phrase-bot = %#v, want mock turn evidence", conversation)
			}
		}
	}
	if !foundConversation {
		t.Fatalf("conversation.list omitted phrase-bot run=%s: %#v", runID, conversations)
	}
}
