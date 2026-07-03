package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
)

func TestCLIRuntimeStateAPIConsumersAreExplicitlyAccounted(t *testing.T) {
	sources := readProductionCommandSources(t)
	wantAPIConsumers := map[string]struct{}{
		"agent_directive.go":      {},
		"agent_replay.go":         {},
		"agent_replay_backlog.go": {},
		"agent_restart.go":        {},
		"agents.go":               {},
		"bundle.go":               {},
		"control_mailbox.go":      {},
		"control_nuke.go":         {},
		"control_run.go":          {},
		"conversations.go":        {},
		"diagnostics.go":          {},
		"entities.go":             {},
		"event_publish.go":        {},
		"events.go":               {},
		"fork.go":                 {},
		"forkchat.go":             {},
		"incidents.go":            {},
		"logs.go":                 {},
		"run_command.go":          {},
		"test_command.go":         {},
	}

	gotAPIConsumers := map[string]struct{}{}
	for name, source := range sources {
		if name == "cli_api.go" {
			continue
		}
		if strings.Contains(source, "newCLIAPIClient(") {
			gotAPIConsumers[name] = struct{}{}
		}
	}
	if diff := missingKeys(wantAPIConsumers, gotAPIConsumers); len(diff) > 0 {
		t.Fatalf("API-backed command files missing newCLIAPIClient use: %v", diff)
	}
	if diff := missingKeys(gotAPIConsumers, wantAPIConsumers); len(diff) > 0 {
		t.Fatalf("new API-backed command files must be classified in this guard: %v", diff)
	}

	localBypassNeedles := []string{
		"runForkRuntimeOwnerHarness",
		"loadRunStatusReport",
		"runStatusReportFromStore",
		"printRunStatusReport",
		"buildStores(",
		"LoadRunDebugReport",
		"MaterializeRunFork",
		"PlanRunFork",
		"ActivateSelectedContractRunFork",
		"runtime.NewRuntime",
		"store.NewPostgresStore",
		"SWARM_BUILDER_AUTH_TOKEN",
		"SWARM_OPERATOR_AUTH_TOKEN",
		"BundleHash(",
		"BuildBundleCatalogProjection",
		"UpsertBundleCatalog",
	}
	for name := range wantAPIConsumers {
		source := sources[name]
		for _, needle := range localBypassNeedles {
			if name == "test_command.go" && needle == "BundleHash(" {
				continue
			}
			if strings.Contains(source, needle) {
				t.Fatalf("%s contains local/runtime bypass %q; runtime-state CLI commands must consume newCLIAPIClient", name, needle)
			}
		}
	}
}

func TestBundleDeleteCLIConsumesCanonicalAPIOnly(t *testing.T) {
	source := readProductionCommandSources(t)["bundle.go"]
	for _, needle := range []string{
		"internal/runtime/bundledelete",
		"internal/runtime/destructivereset",
		"internal/store",
		"PlanBundleDelete(",
		"ApplyBundleDeleteFinalMutation(",
		"ApplyBundleForceDeletePreservationCleanup(",
		"ManagedResetContainerInventory(",
		"DELETE FROM",
		"UPDATE runs",
	} {
		if strings.Contains(source, needle) {
			t.Fatalf("bundle.go contains non-authoritative bundle delete bypass %q; swarm bundle delete must consume /v1/rpc bundle.delete only", needle)
		}
	}
	if !strings.Contains(source, "bundleDeleteMethod") || !strings.Contains(source, `"bundle.delete"`) || !strings.Contains(source, "newCLIAPIClient(") {
		t.Fatalf("bundle.go must expose bundle.delete only through the shared CLI API client")
	}
}

func TestCLILocalRuntimeHelpersRemainNonOperatorQuarantined(t *testing.T) {
	sources := readProductionCommandSources(t)
	all := strings.Join(sortedSourceValues(sources), "\n")

	wantCounts := map[string]int{
		"runForkRuntimeOwnerHarness(": 1,
		"loadRunStatusReport(":        1,
		"printRunStatusReport(":       1,
		"runStatusReportFromStore(":   2, // one private helper call plus its definition
	}
	for needle, want := range wantCounts {
		if got := strings.Count(all, needle); got != want {
			t.Fatalf("%q occurrences = %d, want %d; local helper must remain non-operator/quarantined", needle, got, want)
		}
	}

	retiredRoutes := [][]string{
		{"fork", "--dry-run"},
		{"fork", "--materialize-only"},
		{"fork", "--activate"},
		{"fork", "--contracts", "."},
		{"investigate", "runs"},
		{"control", "mailbox", "list"},
	}
	for _, args := range retiredRoutes {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommand(context.Background(), t.TempDir(), args, &stdout, &stderr)
			if code == cliExitOK {
				t.Fatalf("%v code = 0, want fail-closed retired route", args)
			}
			if strings.Contains(stdout.String(), "Fork Run:") || strings.Contains(stderr.String(), "Fork Run:") {
				t.Fatalf("%v reached fork runtime owner output; stdout=%q stderr=%q", args, stdout.String(), stderr.String())
			}
		})
	}
}

func TestCLIRuntimeStateCommandsRequireSharedAPITokenBeforeRequest(t *testing.T) {
	payloadPath := filepath.Join(t.TempDir(), "payload.json")
	if err := os.WriteFile(payloadPath, []byte(`{"topic":"sample"}`), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	envelopePath := filepath.Join(t.TempDir(), "bundle-register.yaml")
	if err := os.WriteFile(envelopePath, []byte("api_version: swarm.bundle.register.v1\nfiles:\n  - path: package.yaml\n    text: \"name: demo\\nversion: \\\"1.0.0\\\"\\nplatform_version: \\\">=0.7.0 <0.8.0\\\"\\nflows: []\\n\"\n"), 0o600); err != nil {
		t.Fatalf("write bundle register envelope: %v", err)
	}
	contractsDir := writeBundleRegisterContractsFixture(t)
	scenarioContractsDir := writeScenarioRunnerFixture(t)

	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "runs", args: []string{"runs"}},
		{name: "status", args: []string{"status", "run-1"}},
		{name: "trace", args: []string{"trace", "run-1"}},
		{name: "trace follow", args: []string{"trace", "run-1", "--follow"}},
		{name: "health", args: []string{"health"}},
		{name: "logs", args: []string{"logs"}},
		{name: "logs follow", args: []string{"logs", "--follow"}},
		{name: "incidents", args: []string{"incidents"}},
		{name: "agents list", args: []string{"agents", "list"}},
		{name: "agent deliveries", args: []string{"agent", "deliveries", "agent-1"}},
		{name: "agent view", args: []string{"agent", "view", "agent-1"}},
		{name: "agent diagnose", args: []string{"agent", "diagnose", "agent-1"}},
		{name: "agent restart", args: []string{"agent", "restart", "agent-1"}},
		{name: "agent replay", args: []string{"agent", "replay", "agent-1", "--event-id", "event-1"}},
		{name: "agent replay backlog", args: []string{"agent", "replay-backlog", "agent-1"}},
		{name: "agent directive", args: []string{"agent", "directive", "agent-1", "continue"}},
		{name: "mailbox list", args: []string{"mailbox", "list"}},
		{name: "mailbox view", args: []string{"mailbox", "view", "mailbox-1"}},
		{name: "mailbox approve", args: []string{"mailbox", "approve", "mailbox-1"}},
		{name: "mailbox reject", args: []string{"mailbox", "reject", "mailbox-1", "--reason", "not approved"}},
		{name: "mailbox defer", args: []string{"mailbox", "defer", "mailbox-1", "--until", "2026-05-23T00:00:00Z"}},
		{name: "control pause", args: []string{"control", "pause", "run-1"}},
		{name: "control continue", args: []string{"control", "continue", "run-1"}},
		{name: "control stop", args: []string{"control", "stop", "run-1"}},
		{name: "control nuke", args: []string{"control", "nuke", "--dry-run"}},
		{name: "events list", args: []string{"events", "list"}},
		{name: "events follow", args: []string{"events", "follow"}},
		{name: "event view", args: []string{"event", "view", "event-1"}},
		{name: "event replay", args: []string{"event", "replay", "event-1"}},
		{name: "event publish", args: []string{"event", "publish", "scan.requested", "--payload-json", `{"topic":"sample"}`}},
		{name: "bundle list", args: []string{"bundle", "list"}},
		{name: "bundle show", args: []string{"bundle", "show", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		{name: "bundle agents", args: []string{"bundle", "agents", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		{name: "bundle register", args: []string{"bundle", "register", envelopePath}},
		{name: "bundle register contracts", args: []string{"bundle", "register", "--contracts", contractsDir}},
		{name: "bundle delete", args: []string{"bundle", "delete", "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		{name: "conversations list", args: []string{"conversations", "list"}},
		{name: "conversation view", args: []string{"conversation", "view", "session-1"}},
		{name: "conversation turn", args: []string{"conversation", "turn", "session-1", "1"}},
		{name: "entities list", args: []string{"entities", "list"}},
		{name: "entity view", args: []string{"entity", "view", "entity-1"}},
		{name: "entity aggregate", args: []string{"entity", "aggregate"}},
		{name: "run connect start", args: []string{"run", "--connect", "http://192.0.2.10:1", "--event", "scan.requested", "--payload", payloadPath, "--no-follow"}},
		{name: "run connect reattach", args: []string{"run", "--connect", "http://192.0.2.10:1", "--reattach", "run-1"}},
		{name: "fork", args: []string{"fork", "11111111-1111-1111-1111-111111111111"}},
		{name: "forkchat new", args: []string{"forkchat", "new", "session-1", "--turn-index", "1"}},
		{name: "forkchat resume", args: []string{"forkchat", "resume", "fork-1", "--message", "continue"}},
		{name: "forkchat list", args: []string{"forkchat", "list"}},
		{name: "forkchat view", args: []string{"forkchat", "view", "fork-1"}},
		{name: "forkchat delete", args: []string{"forkchat", "delete", "fork-1"}},
		{name: "test", args: []string{"test", "--contracts", scenarioContractsDir, "--platform-spec", filepath.Join(repoRoot(), defaultPlatformSpecPath)}},
		{name: "version server", args: []string{"version", "--server"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_BUILDER_AUTH_TOKEN", "builder-token")
			t.Setenv("SWARM_OPERATOR_AUTH_TOKEN", "operator-token")

			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				http.Error(w, "unexpected request", http.StatusInternalServerError)
			}))
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != cliExitAuth {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitAuth, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), "API token source is required") {
				t.Fatalf("stderr = %q, want shared missing-token error", stderr.String())
			}
			if calls.Load() != 0 {
				t.Fatalf("server calls = %d, want 0 before auth", calls.Load())
			}
		})
	}
}

func readProductionCommandSources(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read cmd/swarm: %v", err)
	}
	sources := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		sources[name] = string(raw)
	}
	return sources
}

func sortedSourceValues(sources map[string]string) []string {
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, sources[name])
	}
	return values
}

func missingKeys(want, got map[string]struct{}) []string {
	var missing []string
	for key := range want {
		if _, ok := got[key]; !ok {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	return missing
}
