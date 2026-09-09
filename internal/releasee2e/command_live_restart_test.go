package releasee2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type commandLivePrerequisites struct {
	claudeToken string
	botToken    string
	chatID      int
	image       string
	network     string
	mcpHost     string
	dsn         string
}

func readCommandLivePrerequisites(getenv func(string) string) (commandLivePrerequisites, error) {
	var result commandLivePrerequisites
	for _, item := range []struct {
		key string
		dst *string
	}{
		{"CLAUDE_CODE_OAUTH_TOKEN", &result.claudeToken},
		{"TELEGRAM_BOT_TOKEN", &result.botToken},
		{"SWARM_COMMAND_LIVE_WORKSPACE_IMAGE", &result.image},
		{"SWARM_COMMAND_LIVE_MCP_HOST", &result.mcpHost},
		{goldenPostgresEnv, &result.dsn},
	} {
		value := strings.TrimSpace(getenv(item.key))
		if value == "" || strings.ContainsAny(value, "\r\n") {
			return result, fmt.Errorf("%s must be provisioned as one nonempty line for live proof", item.key)
		}
		*item.dst = value
	}
	chatID, err := strconv.Atoi(strings.TrimSpace(getenv("SWARM_COMMAND_LIVE_TELEGRAM_CHAT_ID")))
	if err != nil || chatID <= 0 {
		return result, fmt.Errorf("SWARM_COMMAND_LIVE_TELEGRAM_CHAT_ID must name an authorized private test chat (positive integer)")
	}
	result.chatID = chatID
	if ip := net.ParseIP(result.mcpHost); ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
		return result, fmt.Errorf("SWARM_COMMAND_LIVE_MCP_HOST must be an explicit container-reachable host interface IP, not loopback or a wildcard")
	}
	result.network = strings.TrimSpace(getenv("SWARM_COMMAND_LIVE_WORKSPACE_NETWORK"))
	return result, nil
}

// Opting in authorizes real provider turns and Telegram messages to the supplied
// dedicated chat. Missing prerequisites are failures, never mock fallbacks.
func TestCommandLiveServeAndRestartParity(t *testing.T) {
	if os.Getenv("SWARM_COMMAND_LIVE_E2E") != "1" {
		t.Skip("provisioned L proof not run; set SWARM_COMMAND_LIVE_E2E=1 with dedicated Claude/Telegram credentials and chat")
	}
	prerequisites, err := readCommandLivePrerequisites(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal("live proof requires Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, docker, "run", "--rm", "--entrypoint", "sh", prerequisites.image,
		"-lc", "command -v claude >/dev/null && claude --version >/dev/null").Run(); err != nil {
		t.Fatalf("provisioned workspace cannot execute Claude: %v", err)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			project := filepath.Join(root, backend)
			store := goldenStoreSelection{name: "sqlite", configYAML: "store:\n  backend: sqlite\n"}
			if backend == "postgres" {
				store = goldenPostgresStore(t, prerequisites.dsn)
			}
			runCommandLiveRestartJourney(t, binary, project, store, prerequisites)
		})
	}
}

func commandLiveRuntimeConfig(store goldenStoreSelection, prerequisites commandLivePrerequisites) string {
	config := "llm:\n  backend: claude_cli\nworkspace:\n  backend: docker\n  image: " + strconv.Quote(prerequisites.image) + "\n"
	if prerequisites.network != "" {
		config += "  network: " + strconv.Quote(prerequisites.network) + "\n"
	}
	return config + store.configYAML
}

func prepareCommandLiveProject(t *testing.T, binary, project string, store goldenStoreSelection, prerequisites commandLivePrerequisites) (releaseProcessSpec, string) {
	t.Helper()
	contracts := filepath.Join(project, "contracts")
	copyReleaseTree(t, fullLifecycleExecutableSource(t), contracts)
	writeReleaseFile(t, filepath.Join(project, "live.yaml"), commandLiveRuntimeConfig(store, prerequisites))
	devStore := goldenStoreSelection{name: "sqlite", configYAML: "store:\n  backend: sqlite\n"}
	writeReleaseFile(t, filepath.Join(project, "dev.yaml"), commandLiveRuntimeConfig(devStore, prerequisites))
	newSecret := func() string {
		var raw [32]byte
		if _, err := rand.Read(raw[:]); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(raw[:])
	}
	apiToken, signingSecret := newSecret(), newSecret()
	writeReleaseFile(t, filepath.Join(project, "api-token"), apiToken+"\n")
	env := goldenProcessEnv(t, project, store.passwordEnv, 0)
	filtered := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if key != "PATH" && key != "TELEGRAM_BOT_TOKEN" {
			filtered = append(filtered, entry)
		}
	}
	env = append(filtered, "PATH="+os.Getenv("PATH"))
	verify := runReleaseCommand(t, fullLifecycleStartupLimit, project, env, "", binary,
		"verify", "contracts", "--config", "live.yaml", "--json")
	assertFullLifecycleVerifySuccess(t, verify)
	secretEnv := slices.DeleteFunc(slices.Clone(env), func(entry string) bool { return strings.HasPrefix(entry, goldenPostgresPass+"=") })
	for key, value := range map[string]string{
		"CLAUDE_CODE_OAUTH_TOKEN":  prerequisites.claudeToken,
		"telegram_bot_token":       prerequisites.botToken,
		"webhook_signing.telegram": signingSecret,
	} {
		result := runReleaseCommand(t, 30*time.Second, project, secretEnv, value+"\n", binary, "secrets", "set", key, "--stdin")
		if strings.Contains(result.output, value) {
			t.Fatalf("public secret setup leaked %s (output withheld)", key)
		}
		if result.err != nil {
			output := &releaseProcessOutput{secrets: []string{prerequisites.claudeToken, prerequisites.botToken, signingSecret, apiToken, store.passwordEnv}}
			_, _ = output.Write([]byte(result.output))
			t.Fatalf("public secret setup failed for %s: %v\n%s", key, result.err, output.String())
		}
	}
	return releaseProcessSpec{
		BinaryPath: binary, WorkingDir: project, Source: "contracts", ConfigPath: "live.yaml",
		Store: store.name, APIPort: freeReleaseTCPPort(t),
		MCPListenHost: prerequisites.mcpHost,
		TokenFile:     "api-token", Token: apiToken, Env: env, WorkspaceBackend: "docker",
		RedactValues: []string{prerequisites.claudeToken, prerequisites.botToken, signingSecret, apiToken},
	}, signingSecret
}

func runCommandLiveRestartJourney(t *testing.T, binary, project string, store goldenStoreSelection, prerequisites commandLivePrerequisites) {
	t.Helper()
	t.Log("proof_surface=L; public serve/dev, real Claude and Telegram, no internal lifecycle entry")
	spec, signingSecret := prepareCommandLiveProject(t, binary, project, store, prerequisites)
	source, err := releaseSourceTree(filepath.Join(project, "contracts"), true)
	if err != nil {
		t.Fatal(err)
	}
	start := func(options releaseProcessSpec) *releaseServeProcess {
		if options.InternalMockLifecycleBinary != "" {
			t.Fatal("L proof must execute the public binary")
		}
		process := startReleaseServe(t, options)
		t.Cleanup(func() {
			if t.Failed() {
				t.Logf("sanitized public serve output:\n%s", process.output.String())
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := process.waitReady(ctx); err != nil {
			t.Fatal(err)
		}
		return process
	}
	stop := func(process *releaseServeProcess) {
		if err := process.stopAndWait(30 * time.Second); err != nil {
			t.Fatalf("live graceful stop: %v\n%s", err, process.output.String())
		}
	}
	openStanding := func(process *releaseServeProcess, previous *fullLifecycleRun) (string, fullLifecycleRun) {
		bundle := requireLifecycleHealthPosture(t, process.rpc, "live")
		if previous != nil {
			run := waitForFullLifecycleStandingRun(t, process.rpc, bundle, previous.Origin.ServiceID, previous.Origin.Generation, previous.RunID)
			if !run.StartedAt.Equal(previous.StartedAt) {
				t.Fatal("retained restart changed standing started_at")
			}
			return bundle, run
		}
		run := waitForFullLifecycleStandingRun(t, process.rpc, bundle, "", 0, "")
		card := waitForFullLifecycleCard(t, process.rpc, run.RunID, "lifecycle_ready")
		decideFullLifecycleCard(t, process.rpc, card, "live-ready-"+card.CardID)
		assertFullLifecycleCardDecided(t, process.rpc, card.CardID)
		waitForFullLifecycleRunStatus(t, process.rpc, run.RunID, "running")
		return bundle, run
	}
	// Each journey owns fresh stores; retain one schema-valid counter across restarts.
	nextID := 1000
	send := func(process *releaseServeProcess) (int, fullLifecycleIngressReceipt) {
		nextID++
		return nextID, sendLifecycleTelegramUpdateWithSecret(t, process, nextID, prerequisites.chatID, signingSecret)
	}
	converge := func(process *releaseServeProcess, run fullLifecycleRun, count, id int, receipt fullLifecycleIngressReceipt) fullLifecycleEvent {
		t.Logf("L ingress %d accepted; awaiting reply approval", id)
		approveLifecycleEffectMode(t, process.rpc, run.RunID, fmt.Sprintf("live-send-%d", id), "live")
		waitForLifecycleConvergenceMode(t, process, run.RunID, count, "live")
		t.Logf("L ingress %d converged through live agent and Telegram connector", id)
		return requireFullLifecycleReceiptEvents(t, process.rpc, run.RunID, id, strconv.Itoa(prerequisites.chatID), receipt)
	}

	process := start(spec)
	bundle, standing := openStanding(process, nil)
	// Capture while the current process/store are still alive, before cleanup
	// callbacks registered by later restarted processes run.
	defer func() {
		if !t.Failed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if store.diagnosticDB != nil {
			rows, err := store.diagnosticDB.QueryContext(ctx, `SELECT jsonb_build_object('attempt_id',attempt_id,'stage',evidence->'stage','operation',evidence->'operation','stderr',evidence->'stderr','stdout',evidence->'stdout')::text FROM runtime_external_effect_attempts WHERE state='outcome_uncertain'`)
			if err != nil {
				t.Logf("uncertain attempt evidence unavailable: %v", err)
			} else {
				defer rows.Close()
				for rows.Next() {
					var evidence string
					if err := rows.Scan(&evidence); err != nil {
						t.Logf("scan uncertain attempt: %v", err)
						break
					}
					output := &releaseProcessOutput{secrets: spec.RedactValues}
					_, _ = output.Write([]byte(evidence))
					t.Logf("sanitized uncertain provider attempt: %s", output.String())
				}
				if err := rows.Err(); err != nil {
					t.Logf("read uncertain attempt: %v", err)
				}
			}
		}
		queries := []struct {
			method string
			params map[string]any
		}{
			{"run.diagnose", map[string]any{"run_id": standing.RunID}},
			{"runtime.logs", map[string]any{"run_id": standing.RunID, "limit": 100, "order": "desc"}},
			{"event.list", map[string]any{"filter": map[string]any{"run_id": standing.RunID}, "limit": 200}},
			{"conversation.list", map[string]any{"run_id": standing.RunID, "limit": 100}},
		}
		for i := 0; i < len(queries); i++ {
			query := queries[i]
			var result json.RawMessage
			if err := process.rpc.call(ctx, query.method, query.params, &result); err != nil {
				t.Logf("failure evidence %s unavailable: %v", query.method, err)
				continue
			}
			output := &releaseProcessOutput{secrets: spec.RedactValues}
			_, _ = output.Write(result)
			t.Logf("sanitized public %s: %s", query.method, output.String())
			if query.method == "conversation.list" {
				var page struct {
					Conversations []goldenConversation `json:"conversations"`
				}
				if err := json.Unmarshal(result, &page); err != nil {
					t.Logf("decode failure conversations: %v", err)
					continue
				}
				for _, conversation := range page.Conversations {
					queries = append(queries, struct {
						method string
						params map[string]any
					}{"conversation.list_turns", map[string]any{"session_id": conversation.SessionID, "limit": 100}})
				}
			}
		}
	}()
	id, firstReceipt := send(process)
	first := converge(process, standing, 1, id, firstReceipt)
	stop(process)
	process = start(spec)
	if restoredBundle, _ := openStanding(process, &standing); restoredBundle != bundle {
		t.Fatal("graceful restart changed source identity")
	}
	id, secondReceipt := send(process)
	second := converge(process, standing, 2, id, secondReceipt)
	assertFullLifecycleStandingReceiptContinuity(t, firstReceipt, secondReceipt)
	assertFullLifecycleSameRoute(t, first, second)

	pauseFullLifecycleRun(t, process.rpc, standing.RunID)
	id, pendingReceipt := send(process)
	checkpoint := waitForFullLifecycleCrashCheckpoint(t, process, standing.RunID, id, strconv.Itoa(prerequisites.chatID), pendingReceipt)
	if err := process.killAndWait(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	process = start(spec)
	recoveredBundle, recovered := openStanding(process, &standing)
	if recoveredBundle != bundle || recovered.Status != "running" || recovered.ControlReason != "standing_reconcile" {
		t.Fatalf("live crash recovery did not converge intrinsically: %#v", recovered)
	}
	old := converge(process, standing, 3, id, pendingReceipt)
	assertFullLifecycleDeliveryCompleted(t, process.rpc, standing.RunID, checkpoint)
	assertFullLifecycleSameRoute(t, second, old)
	before := captureFullLifecycleEvidence(t, process.rpc, standing.RunID)
	id, freshReceipt := send(process)
	fresh := converge(process, standing, 4, id, freshReceipt)
	assertFullLifecycleSameRoute(t, old, fresh)
	assertFullLifecycleStandingReceiptContinuity(t, firstReceipt, pendingReceipt, freshReceipt)
	assertFullLifecycleOldEvidenceUnchanged(t, process.rpc, standing.RunID, checkpoint, before)
	retained := captureFullLifecycleHistory(t, process.rpc, standing.RunID)
	retainedEvidence := captureFullLifecycleEvidence(t, process.rpc, standing.RunID)
	stop(process)

	devSpec := spec
	devSpec.ConfigPath, devSpec.Store, devSpec.Dev = "dev.yaml", "sqlite", true
	devSpec.Env = slices.DeleteFunc(slices.Clone(spec.Env), func(entry string) bool { return strings.HasPrefix(entry, goldenPostgresPass+"=") })
	var priorDev *fullLifecycleRun
	var priorHistory fullLifecycleHistory
	for epoch := 0; epoch < 2; epoch++ {
		process = start(devSpec)
		devBundle, devRun := openStanding(process, nil)
		if devBundle != bundle {
			t.Fatal("dev startup changed source identity")
		}
		if priorDev != nil {
			if !devRun.StartedAt.After(priorDev.StartedAt) {
				t.Fatal("dev restart did not create a later store epoch")
			}
			assertFullLifecycleHistoryAbsent(t, process.rpc, devRun.RunID, priorHistory)
		}
		id, receipt := send(process)
		converge(process, devRun, 1, id, receipt)
		priorHistory = captureFullLifecycleHistory(t, process.rpc, devRun.RunID)
		priorDev = &devRun
		stop(process)
	}
	process = start(spec)
	openStanding(process, &standing)
	actual := captureFullLifecycleHistory(t, process.rpc, standing.RunID)
	if !reflect.DeepEqual(actual.DeliveryIDs, retained.DeliveryIDs) || !reflect.DeepEqual(actual.CardIDs, retained.CardIDs) {
		t.Fatal("dev execution changed retained delivery/card identities")
	}
	actualEvidence := captureFullLifecycleEvidence(t, process.rpc, standing.RunID)
	for id, expected := range retainedEvidence.EventFacts {
		if actualEvidence.EventFacts[id] != expected {
			t.Fatalf("dev execution changed retained event/delivery facts for %s", id)
		}
	}
	for _, name := range []string{"inbound.telegram", "inbound.telegram.text_message", "platform.activity_requested", "platform.stage_timer"} {
		if actualEvidence.EventCounts[name] != retainedEvidence.EventCounts[name] {
			t.Fatalf("dev execution changed retained %s cardinality", name)
		}
	}
	stop(process)
	if actual, err := releaseSourceTree(filepath.Join(project, "contracts"), true); err != nil || !reflect.DeepEqual(actual, source) {
		t.Fatalf("live/restart journey changed authored source, including doubles: %v", err)
	}
}

func TestCommandLiveReceiptUsesExactConfiguredConversation(t *testing.T) {
	for _, chat := range []string{"42", "424242"} {
		t.Run(chat, func(t *testing.T) {
			receipt := fullLifecycleIngressReceipt{EntityID: "standing", EventIDs: []string{"raw", "normalized"}, EventNames: []string{"inbound.telegram", "inbound.telegram.text_message"}}
			events := []fullLifecycleEvent{
				{EventID: "raw", EventName: "inbound.telegram", EntityID: "standing", NoDelivery: &fullLifecycleNoDelivery{Reason: "no_subscriber_by_design"}},
				{EventID: "normalized", EventName: "inbound.telegram.text_message", EntityID: "downstream", Payload: map[string]any{"provider_message_reference": float64(1001), "conversation_reference": chat}, Deliveries: []fullLifecycleEventDelivery{{SubscriberType: "agent", SubscriberID: "phrase-bot", Target: fullLifecycleDeliveryTarget{Kind: "materializing_entity", EntityID: "downstream", FlowID: "telegram-chat", FlowInstance: "telegram-chat/instance"}}}},
			}
			if _, _, err := fullLifecycleReceiptEvents(events, receipt, 1001, chat); err != nil {
				t.Fatal(err)
			}
			if _, _, err := fullLifecycleReceiptEvents(events, receipt, 1001, "wrong-chat"); err == nil {
				t.Fatal("wrong conversation accepted")
			}
			if _, _, err := fullLifecycleReceiptEvents(events, receipt, 1002, chat); err == nil {
				t.Fatal("wrong occurrence accepted")
			}
			slices.Reverse(receipt.EventIDs)
			if _, _, err := fullLifecycleReceiptEvents(events, receipt, 1001, chat); err == nil {
				t.Fatal("unordered receipt accepted")
			}
		})
	}
}

func TestCommandLivePrerequisiteAdmission(t *testing.T) {
	valid := map[string]string{
		"CLAUDE_CODE_OAUTH_TOKEN":             "unit-test-only-not-used-for-execution",
		"TELEGRAM_BOT_TOKEN":                  "unit-test-only-not-used-for-execution",
		"SWARM_COMMAND_LIVE_WORKSPACE_IMAGE":  "dedicated-image",
		"SWARM_COMMAND_LIVE_MCP_HOST":         "172.17.0.1",
		"SWARM_COMMAND_LIVE_TELEGRAM_CHAT_ID": "42",
		goldenPostgresEnv:                     "postgres://configured-test-store",
	}
	for key := range valid {
		t.Run("missing-"+key, func(t *testing.T) {
			_, err := readCommandLivePrerequisites(func(name string) string {
				if name == key {
					return ""
				}
				return valid[name]
			})
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("missing prerequisite %s did not fail explicitly: %v", key, err)
			}
		})
	}
	if _, err := readCommandLivePrerequisites(func(key string) string { return valid[key] }); err != nil {
		t.Fatal(err)
	}
	for _, chat := range []string{"0", "-42", "not-a-chat"} {
		_, err := readCommandLivePrerequisites(func(key string) string {
			if key == "SWARM_COMMAND_LIVE_TELEGRAM_CHAT_ID" {
				return chat
			}
			return valid[key]
		})
		if err == nil {
			t.Fatalf("invalid private chat %q accepted", chat)
		}
	}
	for _, host := range []string{"localhost", "127.0.0.1", "0.0.0.0", "::", "::1"} {
		_, err := readCommandLivePrerequisites(func(key string) string {
			if key == "SWARM_COMMAND_LIVE_MCP_HOST" {
				return host
			}
			return valid[key]
		})
		if err == nil {
			t.Fatalf("unreachable or wildcard MCP host %q accepted", host)
		}
	}
}

func TestLiveProcessOutputRedactsSplitSecretWrites(t *testing.T) {
	output := &releaseProcessOutput{secrets: []string{"private-token", ""}}
	_, _ = output.Write([]byte("message private-"))
	_, _ = output.Write([]byte("token complete"))
	if got := output.String(); got != "message [REDACTED] complete" {
		t.Fatal("process output did not redact a secret spanning writes")
	}
}

func TestLifecycleEffectApprovalUsesExpectedMode(t *testing.T) {
	for _, mode := range []string{"mock", "live"} {
		t.Run(mode, func(t *testing.T) {
			var decisions atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					ID     any            `json:"id"`
					Method string         `json:"method"`
					Params map[string]any `json:"params"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Errorf("decode proof request: %v", err)
					http.Error(w, "invalid request", http.StatusBadRequest)
					return
				}
				card := fullLifecycleCard{
					CardID: "card", RunID: "run", Status: "pending", ExecutionMode: mode,
					Decision: "send_telegram_message", CardContentHash: "content-hash",
				}
				card.Snapshot.Decision = card.Decision
				if decisions.Load() != 0 {
					card.Status = "decided"
				}
				projection := fullLifecycleCardProjection{Kind: "decision_card", DecisionCard: card}
				var result any
				switch request.Method {
				case "mailbox.list":
					result = map[string]any{"items": []fullLifecycleCardProjection{projection}}
				case "mailbox.get":
					result = projection
				case "mailbox.decide":
					if request.Params["card_id"] != card.CardID || request.Params["observed_content_hash"] != card.CardContentHash {
						t.Error("approval lost the exact card/content identity")
					}
					decisions.Add(1)
					result = map[string]any{"status": "decided", "idempotency_replayed": false}
				default:
					t.Errorf("unexpected proof method %s", request.Method)
					http.Error(w, "unexpected method", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
			}))
			defer server.Close()
			rpc := &releaseRPCClient{endpoint: server.URL, client: server.Client()}
			approveLifecycleEffectMode(t, rpc, "run", "proof", mode)
			if got := decisions.Load(); got != 1 {
				t.Fatalf("approval count = %d, want 1", got)
			}
		})
	}
}
