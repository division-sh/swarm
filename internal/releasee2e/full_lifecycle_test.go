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
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	fullLifecycleAPIToken      = "full-lifecycle-release-e2e-token"
	fullLifecycleSigningSecret = "full-lifecycle-telegram-secret"
	fullLifecycleStartupLimit  = 60 * time.Second
	fullLifecycleRunLimit      = 90 * time.Second
)

type fullLifecycleOutcome string

const (
	fullLifecycleGracefulRestart fullLifecycleOutcome = "plain-default-graceful"
	fullLifecycleDefaultRefusal  fullLifecycleOutcome = "plain-default-forced-refusal"
	fullLifecycleExplicitRecover fullLifecycleOutcome = "plain-explicit-forced-recovery"
	fullLifecycleDevAbandon      fullLifecycleOutcome = "dev-default-forced-abandon"
)

type fullLifecycleScenario struct {
	Outcome  fullLifecycleOutcome
	Mode     releaseServeMode
	Recovery bool
}

var fullLifecycleMatrix = []fullLifecycleScenario{
	{Outcome: fullLifecycleGracefulRestart, Mode: releaseServePlain},
	{Outcome: fullLifecycleDefaultRefusal, Mode: releaseServePlain},
	{Outcome: fullLifecycleExplicitRecover, Mode: releaseServePlain, Recovery: true},
	{Outcome: fullLifecycleDevAbandon, Mode: releaseServeDev},
}

func TestCompiledProcessFullLifecycleSQLiteSmoke(t *testing.T) {
	if profile, continuous := releaseContinuousProofProfile(t); continuous {
		t.Skipf("complete dual-store lifecycle matrix supersedes SQLite smoke in %s", profile)
	}
	root := releaseRoot(t)
	binary := buildReleaseBinary(t, root)
	assertFullLifecycleSourceAdmission(t, binary, root)
	scenarioRoot := filepath.Join(root, "sqlite-smoke")
	runFullLifecycleScenario(t, binary, scenarioRoot, releaseSQLiteStore(scenarioRoot), fullLifecycleMatrix[0])
}

func TestCompiledProcessFullLifecycleMatrixSQLitePostgres(t *testing.T) {
	profile, continuous := releaseContinuousProofProfile(t)
	if !continuous {
		t.Skipf("complete dual-store lifecycle matrix requires full/nightly profile, got %q", profile)
	}
	dsn := strings.TrimSpace(os.Getenv(releasePostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for the complete lifecycle matrix", releasePostgresEnv)
	}
	root := releaseRoot(t)
	binary := buildReleaseBinary(t, root)
	assertFullLifecycleSourceAdmission(t, binary, root)

	limit := make(chan struct{}, 2)
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		for _, scenario := range fullLifecycleMatrix {
			scenario := scenario
			t.Run(backend+"/"+string(scenario.Outcome), func(t *testing.T) {
				t.Parallel()
				limit <- struct{}{}
				defer func() { <-limit }()
				scenarioRoot := filepath.Join(root, backend+"-"+string(scenario.Outcome))
				store := releaseSQLiteStore(scenarioRoot)
				if backend == "postgres" {
					store = releasePostgresStore(t, dsn)
				}
				runFullLifecycleScenario(t, binary, scenarioRoot, store, scenario)
			})
		}
	}
}

type fullLifecycleProject struct {
	root       string
	contracts  string
	configPath string
	env        []string
	process    releaseProcessSpec
}

func prepareFullLifecycleProject(t *testing.T, binary, root string, store releaseStoreSelection, scenario fullLifecycleScenario) fullLifecycleProject {
	t.Helper()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create lifecycle project: %v", err)
	}
	contracts := filepath.Join(root, "contracts")
	copyReleaseTree(t, fullLifecycleExecutableSource(t), contracts)
	writeReleaseFile(t, filepath.Join(root, "go.mod"), "module full-lifecycle-release-e2e\n\ngo 1.23.0\n")
	dataDir := filepath.Join(root, "data")
	workspaceDir := filepath.Join(root, "workspace")
	for _, dir := range []string{dataDir, workspaceDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create lifecycle directory %s: %v", dir, err)
		}
	}
	configPath := filepath.Join(root, "swarm.yaml")
	writeReleaseFile(t, configPath, fullLifecycleRuntimeConfig(store, workspaceDir, scenario.Recovery))
	tokenFile := filepath.Join(root, "api-token")
	writeReleaseFile(t, tokenFile, fullLifecycleAPIToken+"\n")
	env := releaseIsolatedProcessEnv(t, root, store.passwordEnv, 0)
	assertReleaseProcessHasNoExternalExecutables(t, env)
	verify := runReleaseCommand(t, fullLifecycleStartupLimit, root, env, "", binary, "verify", "--config", configPath, "--contracts", contracts, "--json")
	assertFullLifecycleVerifySuccess(t, verify)
	secret := runReleaseCommand(t, 30*time.Second, root, env, fullLifecycleSigningSecret+"\n", binary, "secrets", "set", "webhook_signing.telegram", "--stdin")
	if secret.err != nil {
		t.Fatalf("set lifecycle signing secret: %v\n%s", secret.err, secret.output)
	}
	return fullLifecycleProject{
		root:       root,
		contracts:  contracts,
		configPath: configPath,
		env:        env,
		process: releaseProcessSpec{
			BinaryPath: binary,
			WorkingDir: root,
			ConfigPath: configPath,
			Contracts:  contracts,
			Data:       dataDir,
			Store:      store.name,
			APIPort:    freeReleaseTCPPort(t),
			MCPPort:    freeReleaseTCPPort(t),
			TokenFile:  tokenFile,
			Token:      fullLifecycleAPIToken,
			Env:        env,
			Mode:       scenario.Mode,
		},
	}
}

func fullLifecycleRuntimeConfig(store releaseStoreSelection, workspaceDir string, recovery bool) string {
	runtime := "runtime:\n  execution_posture: mock_only\n"
	if recovery {
		runtime += "  recovery_on_startup: true\n"
	}
	return runtime +
		"llm:\n" +
		"  backend: claude_cli\n" +
		"workspace:\n" +
		"  backend: host\n" +
		"  data_source: " + strconv.Quote(workspaceDir) + "\n" +
		store.configYAML
}

func assertFullLifecycleSourceAdmission(t *testing.T, binary, root string) {
	t.Helper()
	positiveRoot := filepath.Join(root, "source-positive")
	positiveContracts := filepath.Join(positiveRoot, "contracts")
	copyReleaseTree(t, fullLifecycleExecutableSource(t), positiveContracts)
	positiveStore := releaseSQLiteStore(positiveRoot)
	positiveConfig := filepath.Join(positiveRoot, "swarm.yaml")
	writeReleaseFile(t, positiveConfig, fullLifecycleRuntimeConfig(positiveStore, filepath.Join(positiveRoot, "workspace"), false))
	positiveEnv := releaseIsolatedProcessEnv(t, positiveRoot, "", 0)
	positive := runReleaseCommand(t, fullLifecycleStartupLimit, positiveRoot, positiveEnv, "", binary, "verify", "--config", positiveConfig, "--contracts", positiveContracts, "--json")
	assertFullLifecycleVerifySuccess(t, positive)

	negativeRoot := filepath.Join(root, "source-missing-response")
	negativeContracts := filepath.Join(negativeRoot, "contracts")
	copyReleaseTree(t, fullLifecycleExecutableSource(t), negativeContracts)
	mutateFullLifecycleFixtureWithoutExactConnectorResponse(t, negativeContracts)
	negativeStore := releaseSQLiteStore(negativeRoot)
	negativeConfig := filepath.Join(negativeRoot, "swarm.yaml")
	writeReleaseFile(t, negativeConfig, fullLifecycleRuntimeConfig(negativeStore, filepath.Join(negativeRoot, "workspace"), false))
	negativeEnv := releaseIsolatedProcessEnv(t, negativeRoot, "", 0)
	negative := runReleaseCommand(t, fullLifecycleStartupLimit, negativeRoot, negativeEnv, "", binary, "verify", "--config", negativeConfig, "--contracts", negativeContracts, "--json")
	if negative.err == nil || !strings.Contains(negative.output, "exact mock response") || !strings.Contains(negative.output, "telegram.send_message") {
		t.Fatalf("missing-response verify = err:%v output:%s, want exact provider-response refusal", negative.err, negative.output)
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

func runFullLifecycleScenario(t *testing.T, binary, root string, store releaseStoreSelection, scenario fullLifecycleScenario) {
	t.Helper()
	started := time.Now()
	project := prepareFullLifecycleProject(t, binary, root, store, scenario)
	start := func() *releaseServeProcess {
		process := startReleaseServe(t, project.process)
		ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleStartupLimit)
		defer cancel()
		if err := process.waitReady(ctx); err != nil {
			t.Fatal(err)
		}
		assertFullLifecycleReadySurface(t, process)
		return process
	}

	process := start()
	bundleHash := requireFullLifecycleHealth(t, process.rpc)
	standing := waitForFullLifecycleStandingRun(t, process.rpc, bundleHash, "", 0)
	lifecycleCard := waitForFullLifecycleCard(t, process, standing.RunID, "lifecycle_ready")
	decideFullLifecycleCard(t, process.rpc, lifecycleCard, "lifecycle-gate-"+standing.RunID)
	waitForFullLifecycleRunStatus(t, process.rpc, standing.RunID, "running")

	switch scenario.Outcome {
	case fullLifecycleGracefulRestart:
		entityID := sendFullLifecycleTelegramUpdate(t, process, 1001, 42)
		approveFullLifecycleEffect(t, process, standing.RunID, "graceful-first")
		waitForFullLifecycleConvergence(t, process, standing.RunID, 1)
		if err := process.interruptAndWait(10 * time.Second); err != nil {
			t.Fatalf("graceful lifecycle stop: %v\n%s", err, process.output.String())
		}
		process = start()
		restored := waitForFullLifecycleStandingRun(t, process.rpc, bundleHash, standing.Origin.ServiceID, standing.Origin.Generation)
		if restored.RunID != standing.RunID {
			t.Fatalf("graceful restart standing run = %s, want %s", restored.RunID, standing.RunID)
		}
		if got := sendFullLifecycleTelegramUpdate(t, process, 1002, 42); got != entityID {
			t.Fatalf("graceful restart Telegram entity = %s, want stable %s", got, entityID)
		}
		approveFullLifecycleEffect(t, process, standing.RunID, "graceful-second")
		waitForFullLifecycleConvergence(t, process, standing.RunID, 2)
	case fullLifecycleDefaultRefusal, fullLifecycleExplicitRecover, fullLifecycleDevAbandon:
		pauseFullLifecycleRun(t, process.rpc, standing.RunID)
		entityID := sendFullLifecycleTelegramUpdate(t, process, 2001, 42)
		checkpoint := waitForFullLifecycleCrashCheckpoint(t, process, standing.RunID, entityID)
		if err := process.killAndWait(10 * time.Second); err != nil {
			t.Fatalf("force lifecycle process death: %v\n%s", err, process.output.String())
		}
		switch scenario.Outcome {
		case fullLifecycleDefaultRefusal:
			denied := startReleaseServe(t, project.process)
			if err := denied.waitForExit(fullLifecycleStartupLimit); err == nil {
				t.Fatalf("default restart exited successfully, want fail-closed refusal\n%s", denied.output.String())
			}
			assertFullLifecycleRecoveryRefusal(t, denied, standing.RunID, checkpoint.EventID)
		case fullLifecycleExplicitRecover:
			process = start()
			recovered := waitForFullLifecycleStandingRun(t, process.rpc, bundleHash, standing.Origin.ServiceID, standing.Origin.Generation)
			if recovered.RunID != standing.RunID || recovered.Status != "paused" {
				t.Fatalf("explicit recovery run = %#v, want same paused run %s", recovered, standing.RunID)
			}
			continueFullLifecycleRun(t, process.rpc, standing.RunID)
			approveFullLifecycleEffect(t, process, standing.RunID, "recovered")
			waitForFullLifecycleConvergence(t, process, standing.RunID, 1)
			assertFullLifecycleDeliveryCompleted(t, process.rpc, standing.RunID, checkpoint)
		case fullLifecycleDevAbandon:
			process = start()
			next := waitForFullLifecycleStandingRun(t, process.rpc, bundleHash, standing.Origin.ServiceID, standing.Origin.Generation+1)
			if next.RunID == standing.RunID {
				t.Fatalf("dev restart reused abandoned run %s", standing.RunID)
			}
			assertFullLifecycleAbandonedPredecessor(t, process.rpc, standing.RunID, checkpoint)
			nextLifecycleCard := waitForFullLifecycleCard(t, process, next.RunID, "lifecycle_ready")
			decideFullLifecycleCard(t, process.rpc, nextLifecycleCard, "lifecycle-gate-"+next.RunID)
			sendFullLifecycleTelegramUpdate(t, process, 2002, 84)
			approveFullLifecycleEffect(t, process, next.RunID, "dev-next-generation")
			waitForFullLifecycleConvergence(t, process, next.RunID, 1)
		}
	default:
		t.Fatalf("unsupported full lifecycle outcome %q", scenario.Outcome)
	}
	t.Logf("compiled lifecycle timing: backend=%s outcome=%s elapsed=%s", store.name, scenario.Outcome, time.Since(started))
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
	if strings.Contains(process.output.String(), fullLifecycleSigningSecret) || strings.Contains(process.output.String(), "\x1b[") {
		t.Fatalf("lifecycle readiness output leaks a secret or ANSI control:\n%s", process.output.String())
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
		health.Bundle.WorkflowName != "full-lifecycle-standing" || health.Bundle.WorkflowVersion != "1.0.0" || health.Bundle.BundleHash == "" {
		t.Fatalf("health.check = %#v, want ready full-lifecycle-standing@1.0.0 mock-only runtime", health)
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

func waitForFullLifecycleStandingRun(t *testing.T, rpc *releaseRPCClient, bundleHash, serviceID string, generation int64) fullLifecycleRun {
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
			if run.Origin.Kind != "standing_generation" {
				continue
			}
			if serviceID != "" && run.Origin.ServiceID != serviceID {
				continue
			}
			if generation != 0 && run.Origin.Generation != generation {
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
		t.Fatalf("wait for standing generation service=%q generation=%d: %v; last=%#v", serviceID, generation, err, match)
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

func waitForFullLifecycleCard(t *testing.T, process *releaseServeProcess, runID, decision string) fullLifecycleCard {
	t.Helper()
	rpc := process.rpc
	ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleRunLimit)
	defer cancel()
	var card fullLifecycleCard
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		params := map[string]any{"status": "pending", "limit": 200}
		if runID != "" {
			params["run_id"] = runID
		}
		var result struct {
			Items []fullLifecycleCardProjection `json:"items"`
		}
		if err := rpc.call(ctx, "mailbox.list", params, &result); err != nil {
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
		return detail.Kind == "decision_card" && card.CardID != "" && card.CardContentHash != "", nil
	})
	if err != nil {
		diagnosticCtx, diagnosticCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer diagnosticCancel()
		var diagnosis goldenDiagnosis
		diagnosisErr := rpc.call(diagnosticCtx, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis)
		events, eventsErr := listFullLifecycleEvents(diagnosticCtx, rpc, runID)
		t.Fatalf("wait for pending decision %s run=%s: %v; last=%#v; diagnosis=%#v diagnosis_err=%v; events=%#v events_err=%v\n%s", decision, runID, err, card, diagnosis, diagnosisErr, events, eventsErr, process.output.String())
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
		"card_id":               card.CardID,
		"verdict":               "approve",
		"fields":                map[string]any{},
		"observed_content_hash": card.CardContentHash,
		"idempotency_key":       key,
	}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "decided" || result.IdempotencyReplayed {
		t.Fatalf("mailbox.decide %s = %#v, want first decided completion", card.CardID, result)
	}
}

func approveFullLifecycleEffect(t *testing.T, process *releaseServeProcess, runID, key string) {
	t.Helper()
	card := waitForFullLifecycleCard(t, process, runID, "send_telegram_message")
	if card.ExecutionMode != "mock" {
		t.Fatalf("connector decision card execution mode = %q, want mock", card.ExecutionMode)
	}
	decideFullLifecycleCard(t, process.rpc, card, "connector-"+key+"-"+card.CardID)
}

func sendFullLifecycleTelegramUpdate(t *testing.T, process *releaseServeProcess, updateID, chatID int) string {
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
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode lifecycle webhook status=%d body=%q: %v", response.StatusCode, raw, err)
	}
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("lifecycle webhook status=%d payload=%#v\n%s", response.StatusCode, payload, process.output.String())
	}
	entityID := strings.TrimSpace(fmt.Sprint(payload["entity_id"]))
	if entityID == "" {
		t.Fatalf("lifecycle webhook omitted entity_id: %#v", payload)
	}
	return entityID
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

func continueFullLifecycleRun(t *testing.T, rpc *releaseRPCClient, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var result map[string]any
	if err := rpc.call(ctx, "run.continue", map[string]any{"run_id": runID, "idempotency_key": "lifecycle-continue-" + runID}, &result); err != nil {
		t.Fatal(err)
	}
	waitForFullLifecycleRunStatus(t, rpc, runID, "running")
}

type fullLifecycleEvent struct {
	EventID       string                       `json:"event_id"`
	EventName     string                       `json:"event_name"`
	EntityID      string                       `json:"entity_id"`
	RunID         string                       `json:"run_id"`
	ExecutionMode string                       `json:"execution_mode"`
	Payload       map[string]any               `json:"payload"`
	Deliveries    []fullLifecycleEventDelivery `json:"deliveries"`
	DeadLetters   []json.RawMessage            `json:"dead_letters"`
}

type fullLifecycleEventDelivery struct {
	DeliveryID     string                      `json:"delivery_id"`
	SubscriberType string                      `json:"subscriber_type"`
	SubscriberID   string                      `json:"subscriber_id"`
	Target         fullLifecycleDeliveryTarget `json:"target"`
	Status         string                      `json:"status"`
	Terminal       bool                        `json:"terminal"`
	ReasonCode     string                      `json:"reason_code"`
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
	seen := map[string]bool{}
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
			if event.EventID == "" || event.RunID != runID || seen[event.EventID] {
				return nil, fmt.Errorf("event.list returned malformed or duplicate event %#v", event)
			}
			seen[event.EventID] = true
			all = append(all, event)
		}
		if page.NextCursor == "" {
			return all, nil
		}
		cursor = page.NextCursor
	}
}

type fullLifecycleCrashCheckpoint struct {
	EventID   string
	Delivery  fullLifecycleEventDelivery
	EntityID  string
	EventName string
}

func waitForFullLifecycleCrashCheckpoint(t *testing.T, process *releaseServeProcess, runID, entityID string) fullLifecycleCrashCheckpoint {
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
		for _, event := range events {
			if event.EventName != "inbound.telegram.text_message" || event.EntityID != entityID {
				continue
			}
			for _, delivery := range event.Deliveries {
				if delivery.SubscriberType != "agent" || delivery.Status != "pending" || delivery.Terminal {
					continue
				}
				if !strings.Contains(delivery.SubscriberID, "phrase-bot") || delivery.Target.Kind != "existing_entity" ||
					delivery.Target.FlowID != "telegram-chat" || delivery.Target.FlowInstance == "" || delivery.Target.EntityID != entityID {
					return false, fmt.Errorf("pending normalized-input route target is not exact: event=%#v delivery=%#v", event, delivery)
				}
				checkpoint = fullLifecycleCrashCheckpoint{EventID: event.EventID, Delivery: delivery, EntityID: entityID, EventName: event.EventName}
			}
		}
		if checkpoint.EventID == "" {
			return false, nil
		}
		if err := process.rpc.call(ctx, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis); err != nil {
			return false, err
		}
		return diagnosis.Run.Status == "paused" && diagnosis.TestQuiescence.ActiveDeliveries > 0, nil
	})
	if err != nil {
		t.Fatalf("wait for public lifecycle crash checkpoint: %v; checkpoint=%#v diagnosis=%#v\n%s", err, checkpoint, diagnosis, process.output.String())
	}
	return checkpoint
}

func assertFullLifecycleRecoveryRefusal(t *testing.T, process *releaseServeProcess, runID, eventID string) {
	t.Helper()
	output := process.output.String()
	for _, want := range []string{"runtime.recovery_on_startup=false", "executable delivery obligations"} {
		if !strings.Contains(output, want) {
			t.Fatalf("default recovery refusal omitted %q for run=%s event=%s:\n%s", want, runID, eventID, output)
		}
	}
	if strings.Contains(output, "ready in") {
		t.Fatalf("default recovery refusal crossed readiness for run=%s event=%s:\n%s", runID, eventID, output)
	}
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
	t.Fatalf("recovered event.list omitted checkpoint delivery %#v", checkpoint)
}

func assertFullLifecycleAbandonedPredecessor(t *testing.T, rpc *releaseRPCClient, runID string, checkpoint fullLifecycleCrashCheckpoint) {
	t.Helper()
	run := waitForFullLifecycleRunStatus(t, rpc, runID, "cancelled")
	if run.ControlReason != "server_restart_abandon" {
		t.Fatalf("abandoned predecessor control reason = %q, want server_restart_abandon", run.ControlReason)
	}
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
				if delivery.Status != "dead_letter" || !delivery.Terminal || delivery.ReasonCode != "server_restart_abandon" {
					t.Fatalf("abandoned checkpoint delivery = %#v, want terminal server_restart_abandon dead letter", delivery)
				}
				return
			}
		}
	}
	t.Fatalf("abandoned event.list omitted checkpoint delivery %#v", checkpoint)
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
			countFullLifecycleEvents(events, "inbound.telegram.text_message") == ingressCount &&
			countFullLifecycleEvents(events, "platform.activity_requested") == ingressCount &&
			countFullLifecycleEvents(events, "telegram-chat/telegram_send_message.succeeded") == ingressCount &&
			countFullLifecycleEventSuffix(events, "/telegram.reply_requested") == ingressCount, nil
	})
	if err != nil {
		t.Fatalf("wait for lifecycle convergence run=%s ingress=%d: %v; diagnosis=%#v events=%#v\n%s", runID, ingressCount, err, diagnosis, last, process.output.String())
	}
	if diagnosis.Run.Status != "running" || len(diagnosis.FailedDeliveries) != 0 {
		t.Fatalf("lifecycle diagnosis = %#v, want running, quiescent, failure-free standing run", diagnosis)
	}
	for _, event := range last {
		if len(event.DeadLetters) != 0 {
			t.Fatalf("current lifecycle event %s (%s) dead letters = %s", event.EventName, event.EventID, event.DeadLetters)
		}
		if event.EventName == "platform.activity_requested" || strings.HasSuffix(event.EventName, "/telegram.reply_requested") || strings.HasSuffix(event.EventName, "/telegram_send_message.succeeded") {
			if event.ExecutionMode != "mock" {
				t.Fatalf("lifecycle event %s execution mode = %q, want mock", event.EventName, event.ExecutionMode)
			}
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
