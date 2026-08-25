package releasee2e

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

const (
	goldenAPIToken        = "golden-agent-release-e2e-token"
	goldenPostgresEnv     = "SWARM_TEST_POSTGRES_DSN"
	goldenPostgresPass    = "SWARM_GOLDEN_POSTGRES_PASSWORD"
	goldenProofProfileEnv = "SWARM_TEST_PROOF_PROFILE"
	goldenRunDeadline     = 90 * time.Second
	goldenBurstDeadline   = 150 * time.Second
	goldenStartupTimeout  = 60 * time.Second
	goldenBurstCandidateN = 10
	goldenBurstIterations = 2
	goldenBurstGOMAXPROCS = 2
)

var goldenSmokeCandidateIDs = []string{"alpha", "beta"}

type goldenWorkloadOptions struct {
	candidateIDs      []string
	processGOMAXPROCS int
	runDeadline       time.Duration
}

func TestGoldenAgentWorkloadSQLiteSmoke(t *testing.T) {
	if profile, continuous := goldenContinuousProofProfile(t); continuous {
		t.Skipf("N=%d burst proof supersedes the N=2 smoke in %s", goldenBurstCandidateN, profile)
	}
	releaseRoot := goldenReleaseRoot(t)
	binaryPath := buildReleaseBinary(t, releaseRoot)
	runGoldenAgentWorkload(t, binaryPath, releaseRoot, goldenSQLiteStore(releaseRoot), false, goldenWorkloadOptions{
		candidateIDs: goldenSmokeCandidateIDs,
	})
}

func TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends(t *testing.T) {
	if profile, continuous := goldenContinuousProofProfile(t); profile != "" && !continuous {
		t.Skipf("forced-restart proof runs in full/nightly, not %s", profile)
	}
	releaseRoot := goldenReleaseRoot(t)
	binaryPath := buildReleaseBinary(t, releaseRoot)
	t.Run("sqlite", func(t *testing.T) {
		root := filepath.Join(releaseRoot, "sqlite-restart")
		runGoldenAgentWorkload(t, binaryPath, root, goldenSQLiteStore(root), true, goldenWorkloadOptions{
			candidateIDs: goldenSmokeCandidateIDs,
		})
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
		if dsn == "" {
			t.Skipf("%s is required for the supported host-PostgreSQL proof", goldenPostgresEnv)
		}
		root := filepath.Join(releaseRoot, "postgres-restart")
		runGoldenAgentWorkload(t, binaryPath, root, goldenPostgresStore(t, dsn), true, goldenWorkloadOptions{
			candidateIDs: goldenSmokeCandidateIDs,
		})
	})
}

func TestGoldenAgentWorkloadBurstConcurrencyOnBothBackends(t *testing.T) {
	profile, continuous := goldenContinuousProofProfile(t)
	if !continuous {
		t.Skipf("burst proof requires full/nightly profile, got %q", profile)
	}
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for the supported host-PostgreSQL proof", goldenPostgresEnv)
	}
	releaseRoot := goldenReleaseRoot(t)
	binaryPath := buildRaceReleaseBinary(t, releaseRoot)
	options := goldenWorkloadOptions{
		candidateIDs:      goldenBurstCandidateIDs(),
		processGOMAXPROCS: goldenBurstGOMAXPROCS,
		runDeadline:       goldenBurstDeadline,
	}
	for iteration := 1; iteration <= goldenBurstIterations; iteration++ {
		t.Run(fmt.Sprintf("iteration-%d", iteration), func(t *testing.T) {
			t.Run("sqlite", func(t *testing.T) {
				t.Parallel()
				root := filepath.Join(releaseRoot, fmt.Sprintf("burst-%d-sqlite", iteration))
				runGoldenAgentWorkload(t, binaryPath, root, goldenSQLiteStore(root), false, options)
			})
			t.Run("postgres", func(t *testing.T) {
				t.Parallel()
				root := filepath.Join(releaseRoot, fmt.Sprintf("burst-%d-postgres", iteration))
				runGoldenAgentWorkload(t, binaryPath, root, goldenPostgresStore(t, dsn), false, options)
			})
		})
	}
}

func goldenBurstCandidateIDs() []string {
	ids := make([]string, goldenBurstCandidateN)
	for index := range ids {
		ids[index] = fmt.Sprintf("candidate-%02d", index+1)
	}
	return ids
}

func goldenContinuousProofProfile(t *testing.T) (string, bool) {
	t.Helper()
	profile := strings.TrimSpace(os.Getenv(goldenProofProfileEnv))
	switch profile {
	case "":
		return "", false
	case "pr-common", "pr-escalated":
		return profile, false
	case "full", "nightly":
		return profile, true
	default:
		t.Fatalf("%s has unsupported value %q", goldenProofProfileEnv, profile)
		return profile, false
	}
}

type goldenStoreSelection struct {
	name        string
	configYAML  string
	passwordEnv string
}

func goldenReleaseRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve release root: %v", err)
	}
	return root
}

func goldenSQLiteStore(root string) goldenStoreSelection {
	return goldenStoreSelection{
		name: "sqlite",
		configYAML: "store:\n" +
			"  backend: sqlite\n" +
			"  sqlite:\n" +
			"    path: " + strconv.Quote(filepath.Join(root, "runtime.db")) + "\n",
	}
}

func goldenPostgresStore(t *testing.T, dsn string) goldenStoreSelection {
	t.Helper()
	config, err := pq.NewConfig(strings.TrimSpace(dsn))
	if err != nil {
		t.Fatalf("parse %s: %v", goldenPostgresEnv, err)
	}
	if len(config.Multi) != 0 || strings.TrimSpace(config.Host) == "" || config.Port == 0 ||
		strings.TrimSpace(config.User) == "" || config.Password == "" {
		t.Fatalf("%s must name one TCP host, port, user, and password", goldenPostgresEnv)
	}
	sslMode := strings.TrimSpace(string(config.SSLMode))
	if sslMode == "" {
		sslMode = "require"
	}
	databaseName := goldenDatabaseName(t)
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open host PostgreSQL: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("connect to host PostgreSQL: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+pq.QuoteIdentifier(databaseName)); err != nil {
		_ = admin.Close()
		t.Fatalf("create isolated PostgreSQL database %s: %v", databaseName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+pq.QuoteIdentifier(databaseName)+" WITH (FORCE)")
		_ = admin.Close()
	})
	configYAML := fmt.Sprintf(`store:
  backend: postgres
database:
  host: %s
  port: %d
  name: %s
  user: %s
  password_env: %s
  sslmode: %s
  pool_size: 5
`, strconv.Quote(config.Host), config.Port, strconv.Quote(databaseName), strconv.Quote(config.User), goldenPostgresPass, strconv.Quote(sslMode))
	return goldenStoreSelection{name: "postgres", configYAML: configYAML, passwordEnv: config.Password}
}

func goldenDatabaseName(t *testing.T) string {
	t.Helper()
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("generate PostgreSQL database suffix: %v", err)
	}
	return fmt.Sprintf("swarm_golden_%d_%s", os.Getpid(), hex.EncodeToString(random))
}

func runGoldenAgentWorkload(t *testing.T, binaryPath, root string, store goldenStoreSelection, restart bool, options goldenWorkloadOptions) {
	t.Helper()
	if len(options.candidateIDs) < 2 {
		t.Fatalf("golden workload requires at least two candidates, got %v", options.candidateIDs)
	}
	runDeadline := options.runDeadline
	if runDeadline == 0 {
		runDeadline = goldenRunDeadline
	}
	if runDeadline < 0 {
		t.Fatalf("golden workload run deadline = %s, want a positive duration", runDeadline)
	}
	assertGoldenFixtureHasSingleMockOwner(t)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create release project: %v", err)
	}
	contracts := filepath.Join(root, "contracts")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal", "releasee2e", "testdata", "golden_agent_workload"), contracts)
	writeReleaseFile(t, filepath.Join(root, "go.mod"), "module golden-agent-release-e2e\n\ngo 1.23.0\n")
	dataDir := filepath.Join(root, "data")
	workspaceDir := filepath.Join(root, "workspace")
	for _, dir := range []string{dataDir, workspaceDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create release directory %s: %v", dir, err)
		}
	}
	configPath := filepath.Join(root, "swarm.yaml")
	writeReleaseFile(t, configPath, goldenRuntimeConfig(store, workspaceDir))
	tokenFile := filepath.Join(root, "api-token")
	writeReleaseFile(t, tokenFile, goldenAPIToken+"\n")
	env := goldenProcessEnv(t, root, store.passwordEnv, options.processGOMAXPROCS)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	verify := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binaryPath, "verify", "--config", configPath, "--contracts", contracts, "--json")
	if verify.err != nil {
		t.Fatalf("golden release verify failed: %v\n%s", verify.err, verify.output)
	}
	var verifyResult struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(verify.output)), &verifyResult); err != nil || !verifyResult.OK {
		t.Fatalf("golden release verify result is not canonical success: err=%v output=%s", err, verify.output)
	}
	apiPort := freeReleaseTCPPort(t)
	mcpPort := freeReleaseTCPPort(t)
	start := func() *releaseServeProcess {
		process := startReleaseServe(t, releaseProcessSpec{
			BinaryPath: binaryPath,
			WorkingDir: root,
			ConfigPath: configPath,
			Contracts:  contracts,
			Data:       dataDir,
			Store:      store.name,
			APIPort:    apiPort,
			MCPPort:    mcpPort,
			TokenFile:  tokenFile,
			Token:      goldenAPIToken,
			Env:        env,
		})
		ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
		defer cancel()
		if err := process.waitReady(ctx); err != nil {
			t.Fatal(err)
		}
		return process
	}
	process := start()
	bundleHash := goldenServedBundleHash(t, process.rpc)
	runID := goldenPublishIngress(t, process.rpc, bundleHash, options.candidateIDs)
	if restart {
		waitForGoldenCrashCheckpoint(t, process.rpc, runID, options.candidateIDs, runDeadline)
		if err := process.killAndWait(5 * time.Second); err != nil {
			t.Fatalf("force-kill release serve: %v\n%s", err, process.output.String())
		}
		process = start()
	}
	waitForGoldenTerminalRun(t, process, runID, runDeadline)
	assertGoldenPublicProof(t, process.rpc, runID, restart, options.candidateIDs)
}

func assertGoldenFixtureHasSingleMockOwner(t *testing.T) {
	t.Helper()
	root := filepath.Join(releaseE2ERepoRoot(t), "internal", "releasee2e", "testdata", "golden_agent_workload")
	path := filepath.Join(root, "flows", "candidate", "agents.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden child agent declaration: %v", err)
	}
	var agents map[string]map[string]any
	if err := yaml.Unmarshal(raw, &agents); err != nil {
		t.Fatalf("decode golden child agent declaration: %v", err)
	}
	child, ok := agents["candidate-worker"]
	if len(agents) != 1 || !ok {
		t.Fatalf("golden child agents = %#v, want one canonical candidate-worker map key", agents)
	}
	if _, redundant := child["id"]; redundant {
		t.Fatal("golden child agent must not carry a redundant id; #2169 owns logical-key materialization")
	}
	for _, relativePath := range []string{
		filepath.Join("mocks", "candidate.py"),
		filepath.Join("mocks", "scout.py"),
	} {
		info, err := os.Stat(filepath.Join(root, relativePath))
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("golden package-owned mock %s is not one regular file: info=%v err=%v", relativePath, info, err)
		}
	}
	for _, relativePath := range []string{
		filepath.Join("flows", "candidate", "package.yaml"),
		filepath.Join("flows", "candidate", "mocks", "candidate.py"),
		filepath.Join("flows", "scout", "package.yaml"),
		filepath.Join("flows", "scout", "mocks", "scout.py"),
	} {
		if _, err := os.Stat(filepath.Join(root, relativePath)); err == nil {
			t.Fatalf("obsolete nested golden mock owner returned at %s", relativePath)
		} else if !os.IsNotExist(err) {
			t.Fatalf("inspect obsolete nested golden mock owner %s: %v", relativePath, err)
		}
	}
}

func goldenRuntimeConfig(store goldenStoreSelection, workspaceDir string) string {
	return "runtime:\n" +
		"  execution_posture: mock_only\n" +
		"  recovery_on_startup: true\n" +
		"llm:\n" +
		"  backend: claude_cli\n" +
		"workspace:\n" +
		"  backend: host\n" +
		"  data_source: " + strconv.Quote(workspaceDir) + "\n" +
		store.configYAML
}

func goldenProcessEnv(t *testing.T, root, postgresPassword string, processGOMAXPROCS int) []string {
	t.Helper()
	blockedPrefixes := []string{"SWARM_", "ANTHROPIC_", "CLAUDE_", "OPENAI_", "PG"}
	blockedExact := map[string]bool{"GOMAXPROCS": true, "HOME": true, "PATH": true}
	env := make([]string, 0, len(os.Environ())+8)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if blockedExact[key] {
			continue
		}
		blocked := false
		for _, prefix := range blockedPrefixes {
			blocked = blocked || strings.HasPrefix(key, prefix)
		}
		if !blocked {
			env = append(env, entry)
		}
	}
	home := filepath.Join(root, "home")
	emptyBin := filepath.Join(root, "empty-bin")
	if err := os.MkdirAll(emptyBin, 0o755); err != nil {
		t.Fatalf("create golden empty executable path: %v", err)
	}
	env = append(env,
		"HOME="+home,
		"XDG_CONFIG_HOME="+filepath.Join(home, ".config"),
		"XDG_CACHE_HOME="+filepath.Join(home, ".cache"),
		"XDG_DATA_HOME="+filepath.Join(home, ".local", "share"),
		"PATH="+emptyBin,
		"NO_COLOR=1",
	)
	if postgresPassword != "" {
		env = append(env, goldenPostgresPass+"="+postgresPassword)
	}
	if processGOMAXPROCS > 0 {
		env = append(env, "GOMAXPROCS="+strconv.Itoa(processGOMAXPROCS))
	}
	return env
}

func assertGoldenProcessHasNoExternalExecutables(t *testing.T, env []string) {
	t.Helper()
	path := ""
	for _, entry := range env {
		if key, value, ok := strings.Cut(entry, "="); ok && key == "PATH" {
			path = value
		}
	}
	if path == "" {
		t.Fatal("golden process environment has no executable search path")
	}
	for _, dir := range filepath.SplitList(path) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read golden executable search path %s: %v", dir, err)
		}
		if len(entries) != 0 {
			t.Fatalf("golden executable search path %s is not empty: %v", dir, entries)
		}
		for _, name := range []string{"claude", "claude-code", "docker", "podman"} {
			if info, err := os.Stat(filepath.Join(dir, name)); err == nil && !info.IsDir() {
				t.Fatalf("release PATH unexpectedly exposes %s at %s", name, dir)
			}
		}
	}
}

func goldenServedBundleHash(t *testing.T, rpc *releaseRPCClient) string {
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
		health.Bundle.WorkflowName != "golden-agent-workload" || health.Bundle.WorkflowVersion != "1.0.0" || strings.TrimSpace(health.Bundle.BundleHash) == "" {
		t.Fatalf("health.check = %#v, want ready golden-agent-workload@1.0.0 in mock_only posture", health)
	}
	return health.Bundle.BundleHash
}

func goldenPublishIngress(t *testing.T, rpc *releaseRPCClient, bundleHash string, candidateIDs []string) string {
	t.Helper()
	var result struct {
		EventID       string `json:"event_id"`
		RunID         string `json:"run_id"`
		NewRunCreated bool   `json:"new_run_created"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rpc.call(ctx, "event.publish", map[string]any{
		"bundle_hash":     bundleHash,
		"event_name":      "search.requested",
		"payload":         map[string]any{"query": "golden workload", "candidate_ids": candidateIDs},
		"emitter":         "releasee2e",
		"idempotency_key": "golden-search-ingress",
	}, &result); err != nil {
		t.Fatal(err)
	}
	if result.EventID == "" || result.RunID == "" || !result.NewRunCreated {
		t.Fatalf("event.publish result = %#v, want a new durable run", result)
	}
	return result.RunID
}

type goldenRunHeader struct {
	RunID       string `json:"run_id"`
	Status      string `json:"status"`
	EntityCount int    `json:"entity_count"`
	EventCount  int    `json:"event_count"`
}

type goldenQuiescence struct {
	Ready                   bool `json:"ready"`
	ActiveDeliveries        int  `json:"active_deliveries"`
	UnsettledPipelineEvents int  `json:"unsettled_pipeline_events"`
	DueTimers               int  `json:"due_timers"`
	ActiveSessionLeases     int  `json:"active_session_leases"`
}

type goldenDiagnosis struct {
	Run              goldenRunHeader   `json:"run"`
	OperationalState string            `json:"operational_state"`
	FailedDeliveries []json.RawMessage `json:"failed_deliveries"`
	TestQuiescence   goldenQuiescence  `json:"test_quiescence"`
}

func waitForGoldenCrashCheckpoint(t *testing.T, rpc *releaseRPCClient, runID string, candidateIDs []string, deadline time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	var last goldenDiagnosis
	err := pollReleaseCondition(ctx, 5*time.Millisecond, func() (bool, error) {
		events, err := listGoldenEvents(ctx, rpc, runID)
		if err != nil {
			return false, err
		}
		if countGoldenEvents(events, "candidate.requested") != len(candidateIDs) {
			return false, nil
		}
		if err := rpc.call(ctx, "run.diagnose", map[string]any{"run_id": runID}, &last); err != nil {
			return false, err
		}
		return last.Run.Status == "running" && !last.TestQuiescence.Ready && goldenActiveWork(last.TestQuiescence) > 0, nil
	})
	if err != nil {
		t.Fatalf("wait for public durable forced-kill checkpoint: %v; last diagnosis=%#v", err, last)
	}
}

func goldenActiveWork(quiescence goldenQuiescence) int {
	return quiescence.ActiveDeliveries + quiescence.UnsettledPipelineEvents + quiescence.DueTimers + quiescence.ActiveSessionLeases
}

func waitForGoldenTerminalRun(t *testing.T, process *releaseServeProcess, runID string, deadline time.Duration) {
	t.Helper()
	rpc := process.rpc
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	var last goldenRunHeader
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		var result struct {
			Run goldenRunHeader `json:"run"`
		}
		if err := rpc.call(ctx, "run.get", map[string]any{"run_id": runID}, &result); err != nil {
			return false, err
		}
		last = result.Run
		switch last.Status {
		case "completed":
			return true, nil
		case "failed", "cancelled", "forked", "stopped":
			return false, fmt.Errorf("run reached terminal status %s", last.Status)
		default:
			return false, nil
		}
	})
	if err != nil {
		diagnosticCtx, diagnosticCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer diagnosticCancel()
		var diagnosis goldenDiagnosis
		diagnosisErr := rpc.call(diagnosticCtx, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis)
		events, eventsErr := listGoldenEvents(diagnosticCtx, rpc, runID)
		var logs struct {
			Logs []map[string]any `json:"logs"`
		}
		logsErr := rpc.call(diagnosticCtx, "runtime.logs", map[string]any{"run_id": runID, "limit": 100, "order": "asc"}, &logs)
		t.Fatalf("wait for golden run completion: %v; last run=%#v; diagnosis=%#v diagnosis_err=%v; events=%#v events_err=%v; runtime_logs=%#v logs_err=%v\nserve output:\n%s", err, last, diagnosis, diagnosisErr, events, eventsErr, logs.Logs, logsErr, process.output.String())
	}
}

type goldenEvent struct {
	EventID     string                `json:"event_id"`
	EventName   string                `json:"event_name"`
	EntityID    string                `json:"entity_id"`
	RunID       string                `json:"run_id"`
	Payload     map[string]any        `json:"payload"`
	Deliveries  []goldenEventDelivery `json:"deliveries"`
	NoDelivery  *goldenNoDelivery     `json:"no_delivery"`
	DeadLetters []json.RawMessage     `json:"dead_letters"`
}

type goldenEventDelivery struct {
	DeliveryID     string               `json:"delivery_id"`
	SubscriberType string               `json:"subscriber_type"`
	SubscriberID   string               `json:"subscriber_id"`
	Target         goldenDeliveryTarget `json:"target"`
	SessionID      string               `json:"session_id"`
	Status         string               `json:"status"`
	Terminal       bool                 `json:"terminal"`
	ReasonCode     string               `json:"reason_code"`
}

type goldenDeliveryTarget struct {
	Kind         string `json:"kind"`
	FlowID       string `json:"flow_id"`
	FlowInstance string `json:"flow_instance"`
	EntityID     string `json:"entity_id"`
}

type goldenNoDelivery struct {
	Reason string              `json:"reason"`
	Plans  []goldenConnectPlan `json:"plans"`
}

type goldenConnectPlan struct {
	PlanSHA256 string `json:"plan_sha256"`
	Resolution string `json:"resolution"`
}

func listGoldenEvents(ctx context.Context, rpc *releaseRPCClient, runID string) ([]goldenEvent, error) {
	var all []goldenEvent
	cursor := ""
	seenCursors := map[string]bool{}
	seenEvents := map[string]bool{}
	for {
		params := map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 2}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Events     []goldenEvent `json:"events"`
			NextCursor string        `json:"next_cursor"`
		}
		if err := rpc.call(ctx, "event.list", params, &page); err != nil {
			return nil, err
		}
		for _, event := range page.Events {
			if strings.TrimSpace(event.EventID) == "" || event.RunID != runID || seenEvents[event.EventID] {
				return nil, fmt.Errorf("event.list returned malformed or duplicate row %#v", event)
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

func countGoldenEvents(events []goldenEvent, name string) int {
	count := 0
	for _, event := range events {
		if event.EventName == name {
			count++
		}
	}
	return count
}

type goldenEntitySummary struct {
	EntityID     string `json:"entity_id"`
	RunID        string `json:"run_id"`
	FlowInstance string `json:"flow_instance"`
	EntityType   string `json:"entity_type"`
	CurrentState string `json:"current_state"`
}

type goldenEntitySet struct {
	root       goldenEntitySummary
	scout      goldenEntitySummary
	candidates map[string]goldenEntitySummary
}

type goldenAgentSummary struct {
	AgentID       string `json:"agent_id"`
	Role          string `json:"role"`
	FlowInstance  string `json:"flow_instance"`
	ExecutionMode string `json:"execution_mode"`
	Status        string `json:"status"`
}

type goldenConversation struct {
	SessionID     string `json:"session_id"`
	AgentID       string `json:"agent_id"`
	ExecutionMode string `json:"execution_mode"`
	RunID         string `json:"run_id"`
	TurnCount     int    `json:"turn_count"`
	Status        string `json:"status"`
}

func assertGoldenPublicProof(t *testing.T, rpc *releaseRPCClient, runID string, restarted bool, candidateIDs []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var diagnosis goldenDiagnosis
	if err := rpc.call(ctx, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis); err != nil {
		t.Fatal(err)
	}
	if diagnosis.Run.Status != "completed" || !diagnosis.TestQuiescence.Ready || goldenActiveWork(diagnosis.TestQuiescence) != 0 || len(diagnosis.FailedDeliveries) != 0 {
		t.Fatalf("run.diagnose = %#v, want completed, quiescent, and failure-free (restarted=%t)", diagnosis, restarted)
	}

	entities := listGoldenEntities(t, ctx, rpc, runID)
	entitySet := assertGoldenEntities(t, ctx, rpc, runID, entities, candidateIDs)
	agents := waitForGoldenAgentTeardown(t, ctx, rpc)
	assertGoldenAgentTeardown(t, agents)

	events, err := listGoldenEvents(ctx, rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenTurns(t, ctx, rpc, runID, events, candidateIDs)
	expectedCounts := map[string]int{
		"search.requested":                   1,
		"scout.requested":                    1,
		"scout/scout.work.requested":         1,
		"scout/scout.completed":              1,
		"candidate.requested":                len(candidateIDs),
		"candidate/candidate.work.requested": len(candidateIDs),
	}
	for _, entity := range entitySet.candidates {
		expectedCounts[entity.FlowInstance+"/candidate.analyzed"] = 1
		expectedCounts[entity.FlowInstance+"/candidate.completed"] = 1
	}
	for eventName, want := range expectedCounts {
		if got := countGoldenEvents(events, eventName); got != want {
			t.Errorf("event %s count = %d, want %d (restarted=%t)", eventName, got, want, restarted)
		}
	}
	for _, event := range events {
		if _, known := expectedCounts[event.EventName]; !known {
			switch event.EventName {
			case "platform.agent_started", "platform.runtime_log":
			default:
				t.Errorf("event.list returned unexpected same-run event %s (%s)", event.EventName, event.EventID)
			}
		}
	}
	assertGoldenAgentStartEvents(t, events, entitySet, restarted)
	assertGoldenRoutePayloads(t, events, entitySet, candidateIDs)
	for _, event := range events {
		if len(event.DeadLetters) != 0 {
			t.Errorf("event %s (%s) dead letters = %s", event.EventName, event.EventID, event.DeadLetters)
		}
		if len(event.Deliveries) == 0 {
			assertGoldenNoDelivery(t, event)
			continue
		}
		if event.NoDelivery != nil {
			t.Errorf("event %s (%s) has both deliveries and no_delivery: %#v", event.EventName, event.EventID, event.NoDelivery)
		}
		for _, delivery := range event.Deliveries {
			if !delivery.Terminal || delivery.Status != "delivered" {
				t.Errorf("event %s delivery = %#v, want terminal delivered", event.EventID, delivery)
			}
			assertGoldenDeliveryTarget(t, event, delivery)
		}
	}
}

func assertGoldenNoDelivery(t *testing.T, event goldenEvent) {
	t.Helper()
	if event.NoDelivery == nil {
		t.Errorf("event %s (%s) has neither deliveries nor typed no_delivery", event.EventName, event.EventID)
		return
	}
	switch event.NoDelivery.Reason {
	case "matched_no_recipient", "resolution_blocked", "declared_consumer_no_plan", "no_subscriber_by_design":
	default:
		t.Errorf("event %s (%s) no_delivery reason = %q", event.EventName, event.EventID, event.NoDelivery.Reason)
	}
	for _, plan := range event.NoDelivery.Plans {
		if len(plan.PlanSHA256) != 64 {
			t.Errorf("event %s (%s) no_delivery plan identity = %q", event.EventName, event.EventID, plan.PlanSHA256)
		}
		switch plan.Resolution {
		case "resolved", "runtime_resolution_required", "resolution_blocker", "no_registration":
		default:
			t.Errorf("event %s (%s) no_delivery resolution = %q", event.EventName, event.EventID, plan.Resolution)
		}
	}
}

func assertGoldenDeliveryTarget(t *testing.T, event goldenEvent, delivery goldenEventDelivery) {
	t.Helper()
	target := delivery.Target
	if delivery.SubscriberType != "node" && delivery.SubscriberType != "agent" {
		t.Errorf("event %s delivery %s subscriber_type = %q", event.EventID, delivery.DeliveryID, delivery.SubscriberType)
		return
	}
	if delivery.SubscriberType == "agent" && target == (goldenDeliveryTarget{}) {
		return
	}
	if target.FlowID == "" || target.FlowInstance == "" {
		t.Errorf("event %s node delivery %s has incomplete target %#v", event.EventID, delivery.DeliveryID, target)
	}
	switch target.Kind {
	case "existing_entity", "materializing_entity":
		if target.EntityID == "" {
			t.Errorf("event %s node delivery %s target %#v requires entity_id", event.EventID, delivery.DeliveryID, target)
		}
		if delivery.SubscriberType == "agent" && target.Kind != "existing_entity" {
			t.Errorf("event %s agent delivery %s target %#v must reference its existing entity", event.EventID, delivery.DeliveryID, target)
		}
	case "entityless_receiver":
		if target.EntityID != "" {
			t.Errorf("event %s node delivery %s entityless target carries entity_id: %#v", event.EventID, delivery.DeliveryID, target)
		}
	default:
		t.Errorf("event %s node delivery %s target kind = %q", event.EventID, delivery.DeliveryID, target.Kind)
	}
}

func listGoldenEntities(t *testing.T, ctx context.Context, rpc *releaseRPCClient, runID string) []goldenEntitySummary {
	t.Helper()
	var all []goldenEntitySummary
	cursor := ""
	seenCursors := map[string]bool{}
	seenEntities := map[string]bool{}
	for {
		params := map[string]any{"run_id": runID, "limit": 2}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Entities   []goldenEntitySummary `json:"entities"`
			NextCursor string                `json:"next_cursor"`
		}
		if err := rpc.call(ctx, "entity.list", params, &page); err != nil {
			t.Fatal(err)
		}
		for _, entity := range page.Entities {
			if entity.EntityID == "" || entity.RunID != runID || seenEntities[entity.EntityID] {
				t.Fatalf("entity.list returned malformed or duplicate row %#v", entity)
			}
			seenEntities[entity.EntityID] = true
			all = append(all, entity)
		}
		if page.NextCursor == "" {
			return all
		}
		if seenCursors[page.NextCursor] {
			t.Fatalf("entity.list repeated cursor %q", page.NextCursor)
		}
		seenCursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
}

func assertGoldenEntities(t *testing.T, ctx context.Context, rpc *releaseRPCClient, runID string, entities []goldenEntitySummary, candidateIDs []string) goldenEntitySet {
	t.Helper()
	if len(entities) != len(candidateIDs)+2 {
		t.Fatalf("entity.list count = %d, want root, scout, and %d candidates: %#v", len(entities), len(candidateIDs), entities)
	}
	result := goldenEntitySet{candidates: map[string]goldenEntitySummary{}}
	seenEntityIDs := map[string]bool{}
	seenFlowInstances := map[string]bool{}
	for _, entity := range entities {
		if entity.EntityID == "" || seenEntityIDs[entity.EntityID] {
			t.Errorf("entity.list returned empty or duplicate entity identity %#v", entity)
		}
		seenEntityIDs[entity.EntityID] = true
		switch entity.EntityType {
		case "search_batch":
			if result.root.EntityID != "" || entity.CurrentState != "complete" || entity.FlowInstance != runID {
				t.Errorf("root entity = %#v, want one complete run-owned root", entity)
			}
			result.root = entity
		case "scout_task":
			if result.scout.EntityID != "" || entity.CurrentState != "complete" || entity.FlowInstance != "scout" {
				t.Errorf("scout entity = %#v, want one complete singleton scout", entity)
			}
			result.scout = entity
		case "candidate_analysis":
			var detail struct {
				Fields map[string]any `json:"fields"`
			}
			if err := rpc.call(ctx, "entity.get", map[string]any{"entity_id": entity.EntityID, "run_id": runID}, &detail); err != nil {
				t.Fatal(err)
			}
			candidateID, _ := detail.Fields["candidate_id"].(string)
			if candidateID == "" || entity.CurrentState != "complete" {
				t.Errorf("candidate entity/detail = %#v/%#v, want terminal candidate identity", entity, detail)
			}
			if _, duplicate := result.candidates[candidateID]; duplicate {
				t.Errorf("candidate identity %q materialized more than once", candidateID)
			}
			if entity.FlowInstance == "" || seenFlowInstances[entity.FlowInstance] {
				t.Errorf("candidate %q has empty or duplicate flow instance %#v", candidateID, entity)
			}
			seenFlowInstances[entity.FlowInstance] = true
			result.candidates[candidateID] = entity
		default:
			t.Errorf("entity.list returned unexpected entity type %#v", entity)
		}
	}
	if result.root.EntityID == "" || result.scout.EntityID == "" {
		t.Errorf("entity.list has incomplete root/scout set: %#v", entities)
	}
	for _, candidateID := range candidateIDs {
		entity, ok := result.candidates[candidateID]
		if !ok || entity.FlowInstance == result.root.FlowInstance || entity.FlowInstance == result.scout.FlowInstance {
			t.Errorf("candidate %s entity = %#v, want one distinct opaque child flow instance", candidateID, entity)
		}
	}
	return result
}

func waitForGoldenAgentTeardown(t *testing.T, ctx context.Context, rpc *releaseRPCClient) []goldenAgentSummary {
	t.Helper()
	var agents []goldenAgentSummary
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		var result struct {
			Agents []goldenAgentSummary `json:"agents"`
		}
		if err := rpc.call(ctx, "agent.list", map[string]any{}, &result); err != nil {
			return false, err
		}
		agents = result.Agents
		if len(agents) != 1 {
			return false, nil
		}
		agent := agents[0]
		return agent.AgentID == "scout-worker" && agent.Role == "golden_scout" &&
			agent.FlowInstance == "scout" && agent.ExecutionMode == "mock" && agent.Status == "idle", nil
	})
	if err != nil {
		t.Fatalf("wait for terminal child-agent teardown: %v; last agent.list=%#v", err, agents)
	}
	return agents
}

func assertGoldenAgentTeardown(t *testing.T, agents []goldenAgentSummary) {
	t.Helper()
	if len(agents) != 1 {
		t.Fatalf("agent.list = %#v, want only the idle singleton scout after child teardown", agents)
	}
	agent := agents[0]
	if agent.AgentID != "scout-worker" || agent.Role != "golden_scout" || agent.FlowInstance != "scout" ||
		agent.ExecutionMode != "mock" || agent.Status != "idle" {
		t.Errorf("post-terminal agent.list row = %#v, want idle mock scout and no stale child agents", agent)
	}
}

func assertGoldenTurns(t *testing.T, ctx context.Context, rpc *releaseRPCClient, runID string, events []goldenEvent, candidateIDs []string) {
	t.Helper()
	type turnExpectation struct {
		agentID   string
		eventName string
	}
	wantByTrigger := map[string]turnExpectation{}
	addTrigger := func(event goldenEvent, agentID string) {
		goldenDeliveryFor(t, event, "agent", agentID)
		if event.EventID == "" || wantByTrigger[event.EventID] != (turnExpectation{}) {
			t.Fatalf("agent trigger event has empty or duplicate identity: %#v", event)
		}
		wantByTrigger[event.EventID] = turnExpectation{agentID: agentID, eventName: event.EventName}
	}
	addTrigger(goldenSingleNamedEvent(t, events, "scout/scout.work.requested"), "scout-worker")
	for _, event := range goldenNamedEvents(t, events, "candidate/candidate.work.requested", len(candidateIDs)) {
		addTrigger(event, "candidate-worker")
	}

	conversations := listGoldenConversations(t, ctx, rpc, runID)
	if len(conversations) != len(candidateIDs)+1 {
		t.Fatalf("conversation.list count = %d, want one scout and %d candidate conversations: %#v", len(conversations), len(candidateIDs), conversations)
	}
	seenSessions := map[string]bool{}
	seenTriggers := map[string]int{}
	for _, conversation := range conversations {
		if conversation.SessionID == "" || seenSessions[conversation.SessionID] || conversation.RunID != runID || conversation.TurnCount != 1 || conversation.ExecutionMode != "mock" {
			t.Errorf("conversation = %#v, want unique session, exact run, and one mock turn", conversation)
		}
		seenSessions[conversation.SessionID] = true
		turns := listGoldenTurns(t, ctx, rpc, conversation.SessionID)
		if len(turns) != 1 || !turns[0].ParseOK || turns[0].ExecutionMode != "mock" || turns[0].Ordinal != 1 {
			t.Errorf("conversation %s turns = %#v, want one successful mock turn", conversation.SessionID, turns)
			continue
		}
		expectation, ok := wantByTrigger[turns[0].TriggerEventID]
		if !ok || conversation.AgentID != expectation.agentID || turns[0].TriggerEventType != expectation.eventName {
			t.Errorf("conversation %s turn = %#v agent=%q, want exact routed trigger %#v", conversation.SessionID, turns[0], conversation.AgentID, expectation)
		}
		seenTriggers[turns[0].TriggerEventID]++
	}
	for triggerID, expectation := range wantByTrigger {
		if seenTriggers[triggerID] != 1 {
			t.Errorf("concrete mock agent %s trigger %s (%s) turn count = %d, want 1", expectation.agentID, triggerID, expectation.eventName, seenTriggers[triggerID])
		}
	}
}

func listGoldenConversations(t *testing.T, ctx context.Context, rpc *releaseRPCClient, runID string) []goldenConversation {
	t.Helper()
	var all []goldenConversation
	cursor := ""
	seenCursors := map[string]bool{}
	for {
		params := map[string]any{"run_id": runID, "limit": 2}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Conversations []goldenConversation `json:"conversations"`
			NextCursor    string               `json:"next_cursor"`
		}
		if err := rpc.call(ctx, "conversation.list", params, &page); err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Conversations...)
		if page.NextCursor == "" {
			return all
		}
		if seenCursors[page.NextCursor] {
			t.Fatalf("conversation.list repeated cursor %q", page.NextCursor)
		}
		seenCursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
}

type goldenTurn struct {
	TurnID           string `json:"turn_id"`
	ExecutionMode    string `json:"execution_mode"`
	Ordinal          int    `json:"ordinal"`
	TriggerEventID   string `json:"trigger_event_id"`
	TriggerEventType string `json:"trigger_event_type"`
	ParseOK          bool   `json:"parse_ok"`
}

func listGoldenTurns(t *testing.T, ctx context.Context, rpc *releaseRPCClient, sessionID string) []goldenTurn {
	t.Helper()
	var all []goldenTurn
	cursor := ""
	seenCursors := map[string]bool{}
	seenTurns := map[string]bool{}
	for {
		params := map[string]any{"session_id": sessionID, "limit": 1}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Turns      []goldenTurn `json:"turns"`
			NextCursor string       `json:"next_cursor"`
		}
		if err := rpc.call(ctx, "conversation.list_turns", params, &page); err != nil {
			t.Fatal(err)
		}
		for _, turn := range page.Turns {
			if turn.TurnID == "" || seenTurns[turn.TurnID] {
				t.Fatalf("conversation.list_turns returned malformed or duplicate row %#v", turn)
			}
			seenTurns[turn.TurnID] = true
			all = append(all, turn)
		}
		if page.NextCursor == "" {
			return all
		}
		if seenCursors[page.NextCursor] {
			t.Fatalf("conversation.list_turns repeated cursor %q", page.NextCursor)
		}
		seenCursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
}

func assertGoldenAgentStartEvents(t *testing.T, events []goldenEvent, entities goldenEntitySet, restarted bool) {
	t.Helper()
	want := map[string]bool{
		"scout-worker\x00" + entities.scout.FlowInstance: true,
	}
	for _, entity := range entities.candidates {
		want["candidate-worker\x00"+entity.FlowInstance] = true
	}
	seen := map[string]int{}
	for _, event := range events {
		if event.EventName != "platform.agent_started" {
			continue
		}
		agentID, _ := event.Payload["agent_id"].(string)
		flowInstance, _ := event.Payload["flow_instance"].(string)
		key := agentID + "\x00" + flowInstance
		if !want[key] {
			t.Errorf("platform.agent_started carried unexpected concrete identity %#v", event.Payload)
			continue
		}
		if event.Payload["llm_backend"] != "mock" || event.Payload["resolved_llm_provider"] != "mock" ||
			event.Payload["resolved_llm_transport"] != "in_process" {
			t.Errorf("platform.agent_started for %q = %#v, want in-process mock execution", key, event.Payload)
		}
		seen[key]++
	}
	for key := range want {
		if seen[key] == 0 || (!restarted && seen[key] != 1) {
			t.Errorf("platform.agent_started count for %q = %d, want %s", key, seen[key], map[bool]string{true: "at least 1", false: "1"}[restarted])
		}
	}
}

func assertGoldenRoutePayloads(t *testing.T, events []goldenEvent, entities goldenEntitySet, candidateIDs []string) {
	t.Helper()
	payloadCandidateIDs := goldenPayloadCandidateIDs(candidateIDs)
	search := goldenSingleNamedEvent(t, events, "search.requested")
	assertGoldenExactPayload(t, search, map[string]any{"query": "golden workload", "candidate_ids": payloadCandidateIDs})
	assertGoldenSingleDelivery(t, search, "node", "search-intake", "materializing_entity", "golden-agent-workload", entities.root)

	scoutRequested := goldenSingleNamedEvent(t, events, "scout.requested")
	assertGoldenExactPayload(t, scoutRequested, map[string]any{"query": "golden workload", "candidate_ids": payloadCandidateIDs})
	assertGoldenSingleDelivery(t, scoutRequested, "node", "scout-intake", "materializing_entity", "scout", entities.scout)

	scoutWork := goldenSingleNamedEvent(t, events, "scout/scout.work.requested")
	assertGoldenExactPayload(t, scoutWork, map[string]any{"query": "golden workload", "candidate_ids": payloadCandidateIDs})
	assertGoldenSingleTargetlessAgentDelivery(t, scoutWork, "scout-worker", entities.scout)

	scoutCompleted := goldenSingleNamedEvent(t, events, "scout/scout.completed")
	assertGoldenExactPayload(t, scoutCompleted, map[string]any{
		"batch_id":      "golden-batch",
		"candidate_ids": payloadCandidateIDs,
	})
	if len(scoutCompleted.Deliveries) != 2 || scoutCompleted.EntityID != "" {
		t.Errorf("mixed scout completion = %#v, want two independently targeted deliveries and no singular entity projection", scoutCompleted)
	}
	assertGoldenDeliveryToEntity(t, scoutCompleted, "node", "scout-completion", "existing_entity", "scout", entities.scout)
	assertGoldenDeliveryToEntity(t, scoutCompleted, "node", "scout-collector", "existing_entity", "golden-agent-workload", entities.root)

	candidateRequested := goldenNamedEvents(t, events, "candidate.requested", len(candidateIDs))
	candidateWork := goldenNamedEvents(t, events, "candidate/candidate.work.requested", len(candidateIDs))
	for _, candidateID := range candidateIDs {
		entity := entities.candidates[candidateID]
		requested := goldenCandidateEvent(t, candidateRequested, candidateID)
		assertGoldenCandidatePayload(t, requested, candidateID, false)
		assertGoldenSingleDelivery(t, requested, "node", "candidate-intake", "materializing_entity", "candidate", entity)

		work := goldenCandidateEvent(t, candidateWork, candidateID)
		assertGoldenCandidatePayload(t, work, candidateID, false)
		assertGoldenSingleDelivery(t, work, "agent", "candidate-worker", "existing_entity", "candidate", entity)

		analyzed := goldenSingleNamedEvent(t, events, entity.FlowInstance+"/candidate.analyzed")
		assertGoldenCandidatePayload(t, analyzed, candidateID, true)
		assertGoldenSingleDelivery(t, analyzed, "node", "candidate-completion", "existing_entity", "candidate", entity)

		completed := goldenSingleNamedEvent(t, events, entity.FlowInstance+"/candidate.completed")
		assertGoldenCandidatePayload(t, completed, candidateID, true)
		assertGoldenSingleDelivery(t, completed, "node", "candidate-collector", "existing_entity", "golden-agent-workload", entities.root)
	}
}

func goldenPayloadCandidateIDs(candidateIDs []string) []any {
	values := make([]any, len(candidateIDs))
	for index, candidateID := range candidateIDs {
		values[index] = candidateID
	}
	return values
}

func goldenNamedEvents(t *testing.T, events []goldenEvent, name string, want int) []goldenEvent {
	t.Helper()
	var matches []goldenEvent
	for _, event := range events {
		if event.EventName == name {
			matches = append(matches, event)
		}
	}
	if len(matches) != want {
		t.Fatalf("event %s count = %d, want %d", name, len(matches), want)
	}
	return matches
}

func goldenSingleNamedEvent(t *testing.T, events []goldenEvent, name string) goldenEvent {
	t.Helper()
	return goldenNamedEvents(t, events, name, 1)[0]
}

func goldenCandidateEvent(t *testing.T, events []goldenEvent, candidateID string) goldenEvent {
	t.Helper()
	var matches []goldenEvent
	for _, event := range events {
		if event.Payload["candidate_id"] == candidateID {
			matches = append(matches, event)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("candidate %s event matches = %#v, want exactly one", candidateID, matches)
	}
	return matches[0]
}

func assertGoldenCandidatePayload(t *testing.T, event goldenEvent, candidateID string, requireSummary bool) {
	t.Helper()
	want := map[string]any{
		"batch_id":     "golden-batch",
		"candidate_id": candidateID,
	}
	if requireSummary {
		want["summary"] = "analysis-" + candidateID
	}
	assertGoldenExactPayload(t, event, want)
}

func assertGoldenExactPayload(t *testing.T, event goldenEvent, want map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(event.Payload, want) {
		t.Errorf("event %s payload = %#v, want exact payload %#v", event.EventName, event.Payload, want)
	}
}

func assertGoldenSingleDelivery(t *testing.T, event goldenEvent, subscriberType, subscriberID, kind, flowID string, entity goldenEntitySummary) {
	t.Helper()
	if len(event.Deliveries) != 1 {
		t.Errorf("event %s deliveries = %#v, want one exact %s/%s delivery", event.EventName, event.Deliveries, subscriberType, subscriberID)
	}
	if event.EntityID != entity.EntityID {
		t.Errorf("event %s entity_id = %q, want singular target entity %q", event.EventName, event.EntityID, entity.EntityID)
	}
	assertGoldenDeliveryToEntity(t, event, subscriberType, subscriberID, kind, flowID, entity)
}

func assertGoldenSingleTargetlessAgentDelivery(t *testing.T, event goldenEvent, subscriberID string, entity goldenEntitySummary) {
	t.Helper()
	if len(event.Deliveries) != 1 || event.EntityID != entity.EntityID {
		t.Errorf("event %s = %#v, want one agent delivery in exact entity context %s", event.EventName, event, entity.EntityID)
	}
	delivery := goldenDeliveryFor(t, event, "agent", subscriberID)
	if delivery.Target != (goldenDeliveryTarget{}) {
		t.Errorf("event %s static agent delivery target = %#v, want explicit targetless agent route", event.EventName, delivery.Target)
	}
}

func assertGoldenDeliveryToEntity(t *testing.T, event goldenEvent, subscriberType, subscriberID, kind, flowID string, entity goldenEntitySummary) {
	t.Helper()
	var delivery goldenEventDelivery
	if subscriberType == "node" {
		delivery = goldenNodeDeliveryForTarget(t, event, subscriberID, kind, flowID, entity)
	} else {
		delivery = goldenDeliveryFor(t, event, subscriberType, subscriberID)
	}
	target := delivery.Target
	if target.Kind != kind || target.FlowID != flowID || target.FlowInstance != entity.FlowInstance || target.EntityID != entity.EntityID {
		t.Errorf("event %s %s/%s target = %#v, want %s %s/%s/%s", event.EventName, subscriberType, subscriberID, target, kind, flowID, entity.FlowInstance, entity.EntityID)
	}
}

func goldenDeliveryFor(t *testing.T, event goldenEvent, subscriberType, subscriberID string) goldenEventDelivery {
	t.Helper()
	var matches []goldenEventDelivery
	for _, delivery := range event.Deliveries {
		if delivery.SubscriberType == subscriberType && delivery.SubscriberID == subscriberID {
			matches = append(matches, delivery)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("event %s %s/%s delivery matches = %#v, want exactly one", event.EventName, subscriberType, subscriberID, matches)
	}
	return matches[0]
}

func goldenNodeDeliveryForTarget(t *testing.T, event goldenEvent, subscriberID, kind, flowID string, entity goldenEntitySummary) goldenEventDelivery {
	t.Helper()
	var matches []goldenEventDelivery
	for _, delivery := range event.Deliveries {
		if delivery.SubscriberType != "node" {
			continue
		}
		nodeID, err := goldenCanonicalNodeID(delivery.SubscriberID)
		if err != nil {
			t.Fatalf("event %s returned invalid canonical node subscriber %q: %v", event.EventName, delivery.SubscriberID, err)
		}
		target := delivery.Target
		if nodeID == subscriberID && target.Kind == kind && target.FlowID == flowID &&
			target.FlowInstance == entity.FlowInstance && target.EntityID == entity.EntityID {
			matches = append(matches, delivery)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("event %s node %s target %s/%s/%s delivery matches = %#v, want exactly one", event.EventName, subscriberID, flowID, entity.FlowInstance, entity.EntityID, matches)
	}
	return matches[0]
}

func goldenCanonicalNodeID(key string) (string, error) {
	parts := strings.Split(key, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("canonical node key has %d coordinates, want 3", len(parts))
	}
	decoded := make([]string, len(parts))
	for index, coordinate := range parts {
		value, err := base64.RawURLEncoding.DecodeString(coordinate)
		if err != nil {
			return "", fmt.Errorf("decode coordinate %d: %w", index, err)
		}
		if base64.RawURLEncoding.EncodeToString(value) != coordinate {
			return "", fmt.Errorf("coordinate %d is not canonical base64url", index)
		}
		decoded[index] = string(value)
	}
	if decoded[0] == "" || decoded[2] == "" {
		return "", fmt.Errorf("canonical node key requires package and node identity")
	}
	return decoded[2], nil
}

func TestGoldenAgentWorkloadFixtureHasSingleMockOwner(t *testing.T) {
	assertGoldenFixtureHasSingleMockOwner(t)
}

func TestGoldenProcessEnvironmentDoesNotResolveExternalExecutables(t *testing.T) {
	env := goldenProcessEnv(t, t.TempDir(), "", 0)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	for _, entry := range env {
		if strings.HasPrefix(entry, "CLAUDE_CODE_OAUTH_TOKEN=") || strings.HasPrefix(entry, "ANTHROPIC_API_KEY=") {
			t.Fatalf("golden process environment carries an LLM credential: %s", entry)
		}
	}
}

func TestGoldenContinuousProofProfileSelection(t *testing.T) {
	for _, test := range []struct {
		profile    string
		continuous bool
	}{
		{profile: ""},
		{profile: "pr-common"},
		{profile: "pr-escalated"},
		{profile: "full", continuous: true},
		{profile: "nightly", continuous: true},
	} {
		name := test.profile
		if name == "" {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv(goldenProofProfileEnv, test.profile)
			profile, continuous := goldenContinuousProofProfile(t)
			if profile != test.profile || continuous != test.continuous {
				t.Fatalf("golden profile = %q/%t, want %q/%t", profile, continuous, test.profile, test.continuous)
			}
		})
	}
}
