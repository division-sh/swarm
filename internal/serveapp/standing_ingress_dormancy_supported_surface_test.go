package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/testutil"
	"gopkg.in/yaml.v3"
)

func TestDormantIngressTelegramScaffoldJourney(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	cwd := t.TempDir()
	for _, args := range [][]string{
		{"new", "webhook-responder", "--output", "telegram-agent"},
		{"verify", "telegram-agent"},
		{"test", "telegram-agent", "tests/smoke.yaml"},
	} {
		var out, errOut bytes.Buffer
		if code := executeCLIFrom(context.Background(), cwd, args, &out, &errOut, nil); code != 0 {
			t.Fatalf("public command %v exited %d: stdout=%s stderr=%s", args, code, &out, &errOut)
		}
		if args[0] == "test" && !strings.Contains(out.String(), "swarm test ok: scenarios=1") {
			t.Fatalf("actual private scenario did not finish: %s", &out)
		}
	}
	if _, err := os.Stat(credentialPath); !os.IsNotExist(err) {
		t.Fatalf("structural/mock commands acquired credential authority: %v", err)
	}
	for _, old := range []string{"bot", "swarm.yaml", "swarm.live.yaml"} {
		if _, err := os.Stat(filepath.Join(cwd, "telegram-agent", old)); !os.IsNotExist(err) {
			t.Fatalf("scaffold restored legacy %s: %v", old, err)
		}
	}
}

func TestDormantIngressDevPublishesNoAuthority(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	disableChannelOnboardingBusinessConsumers(t, root)
	opts := cliapp.ServeOptions{
		SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
		ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "", "", channelOnboardingHostWorkspaceFields()),
		Dev:        true, SelfCheck: true, Verbose: true, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		WorkspaceBackend: "host", WorkspaceBackendSet: true,
	}
	process := startServeRuntimeTestProcessAtRepo(t, root, opts)
	process.waitForReadyLine()
	if !strings.Contains(process.outputString(), "DORMANT ingress") {
		t.Fatalf("dev hid the missing-credential decision: %s", process.outputString())
	}
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
	var inventory struct {
		Runs []struct {
			Origin struct {
				Kind string `json:"kind"`
			} `json:"origin"`
		} `json:"runs"`
		NextCursor string `json:"next_cursor"`
	}
	requireServedJSONRPCResult(t, endpoint+"/v1/rpc", "run.list", map[string]any{}, &inventory)
	if inventory.Runs == nil || inventory.NextCursor != "" {
		t.Fatalf("incomplete public run inventory: %#v", inventory)
	}
	for _, run := range inventory.Runs {
		if run.Origin.Kind == "" || run.Origin.Kind == "standing_generation" {
			t.Fatalf("fresh dev created a dormant standing generation: %#v", run)
		}
	}
	response, err := http.Post(endpoint+"/webhooks/chat/telegram", "application/json", strings.NewReader(`{"update_id":91,"message":{"message_id":91,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"not executable"}}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("dev published dormant ingress: HTTP %d", response.StatusCode)
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("dev stop=%d", code)
	}
}

func TestDormantIngressProvisionRestartSignedInputBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
			disableChannelOnboardingBusinessConsumers(t, root)
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath,
				APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true,
				WorkspaceBackend: "host", WorkspaceBackendSet: true, StoreMode: backend, StoreModeSet: true}
			var db *sql.DB
			if backend == "sqlite" {
				path := filepath.Join(t.TempDir(), "dormant-ingress.sqlite")
				var err error
				db, err = sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, path, channelOnboardingHostWorkspaceFields())
			} else {
				dsn, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				db = opened
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
			}
			enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
			start := func() (*serveRuntimeTestProcess, string) {
				t.Helper()
				process := startServeRuntimeTestProcess(t, opts)
				process.waitForReadyLine()
				return process, "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
			}
			assertDormant := func(process *serveRuntimeTestProcess, endpoint string) {
				t.Helper()
				output := process.outputString()
				for _, want := range []string{"DORMANT ingress telegram-ingress/telegram", "webhook_signing.telegram", "then restart", "NOT READY"} {
					if !strings.Contains(output, want) {
						t.Fatalf("dormant teaching missing %q:\n%s", want, output)
					}
				}
				resp, err := http.Get(endpoint + "/readyz")
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("otherwise-admissible serve readiness = %d", resp.StatusCode)
				}
				body := `{"update_id":91,"message":{"message_id":91,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"not executable"}}`
				req, err := http.NewRequest(http.MethodPost, endpoint+"/webhooks/chat/telegram", strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
				resp, err = http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("dormant target remained published: %d", resp.StatusCode)
				}
			}
			secretsCLI := func(args []string, input string) {
				t.Helper()
				restore := installChannelOnboardingCLIInput(t, input)
				defer restore()
				var out, errOut bytes.Buffer
				if code := executeCLIFrom(context.Background(), repoRootForTest(), args, &out, &errOut, nil); code != 0 {
					t.Fatalf("secret command %v exit=%d stdout=%s stderr=%s", args, code, out.String(), errOut.String())
				}
			}
			assertCurrent := func(runID string, generation int64, state string) (string, int64) {
				t.Helper()
				var gotRun, gotState string
				var gotGeneration int64
				if err := db.QueryRow(`SELECT current_run_id, current_generation, effective_state FROM standing_services`).Scan(&gotRun, &gotGeneration, &gotState); err != nil {
					t.Fatal(err)
				}
				if gotState != state || gotGeneration != generation || runID != "" && gotRun != runID {
					t.Fatalf("current identity = %s/%d/%s, want %s/%d/%s", gotRun, gotGeneration, gotState, runID, generation, state)
				}
				return gotRun, gotGeneration
			}
			process, endpoint := start()
			assertDormant(process, endpoint)
			for _, table := range []string{"standing_services", "standing_service_generations", "standing_service_journal"} {
				var count int
				if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("fresh dormant %s count=%d err=%v", table, count, err)
				}
			}
			var standingRuns int
			if err := db.QueryRow(`SELECT COUNT(*) FROM runs WHERE origin_kind = 'standing_generation'`).Scan(&standingRuns); err != nil || standingRuns != 0 {
				t.Fatalf("fresh dormant standing runs=%d err=%v", standingRuns, err)
			}
			secretsCLI([]string{"secrets", "set", "webhook_signing.telegram", "--stdin"}, "telegram-secret\n")
			assertDormant(process, endpoint)
			if code := process.stop(); code != 0 {
				t.Fatalf("initial stop=%d", code)
			}
			process, endpoint = start()
			runID, generation := assertCurrent("", 1, "active")
			requireStandingTelegramSignatureRejection(t, endpoint)
			sendStandingTelegramUpdate(t, endpoint, 92, 42, process.outputString)
			duplicateRequest, err := http.NewRequest(http.MethodPost, endpoint+"/webhooks/chat/telegram", strings.NewReader(`{"update_id":92,"message":{"message_id":92,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello 92"}}`))
			if err != nil {
				t.Fatal(err)
			}
			duplicateRequest.Header.Set("Content-Type", "application/json")
			duplicateRequest.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
			duplicate, err := http.DefaultClient.Do(duplicateRequest)
			if err != nil {
				t.Fatal(err)
			}
			var receipt struct {
				Status string `json:"status"`
			}
			decodeErr := json.NewDecoder(duplicate.Body).Decode(&receipt)
			_ = duplicate.Body.Close()
			if decodeErr != nil || duplicate.StatusCode != http.StatusOK || receipt.Status != "duplicate" {
				t.Fatalf("exact duplicate response = %d/%#v/%v", duplicate.StatusCode, receipt, decodeErr)
			}
			var published int
			if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = 'inbound.telegram'`).Scan(&published); err != nil || published != 1 {
				t.Fatalf("signed raw event dedupe = %d, %v", published, err)
			}
			var serviceID string
			var journalCount int
			if err := db.QueryRow(`SELECT service_id FROM standing_services`).Scan(&serviceID); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM standing_service_journal`).Scan(&journalCount); err != nil {
				t.Fatal(err)
			}
			secretsCLI([]string{"secrets", "set", "webhook_signing.telegram", "--stdin"}, "unadmitted-in-process-rotation\n")
			for _, command := range []string{"resume", "reset"} {
				response := requestServedJSONRPC(t, endpoint+"/v1/rpc", "standing."+command, map[string]any{
					"service_id": serviceID, "reason": "credential-currentness proof", "idempotency_key": "stale-standing-" + command,
				})
				if response.Error == nil {
					t.Fatalf("standing %s accepted stale frozen credentials: %#v", command, response)
				}
				assertCurrent(runID, generation, "active")
				var gotJournalCount int
				if err := db.QueryRow(`SELECT COUNT(*) FROM standing_service_journal`).Scan(&gotJournalCount); err != nil || gotJournalCount != journalCount {
					t.Fatalf("refused standing %s mutated history: %d->%d, %v", command, journalCount, gotJournalCount, err)
				}
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("enabled stop=%d", code)
			}
			secretsCLI([]string{"secrets", "remove", "webhook_signing.telegram"}, "")
			process, endpoint = start()
			assertDormant(process, endpoint)
			assertCurrent(runID, generation, "dormant")
			if code := process.stop(); code != 0 {
				t.Fatalf("dormant retained stop=%d", code)
			}
			secretsCLI([]string{"secrets", "set", "webhook_signing.telegram", "--stdin"}, "telegram-secret\n")
			process, endpoint = start()
			assertCurrent(runID, generation, "active")
			sendStandingTelegramUpdate(t, endpoint, 93, 42, process.outputString)
			if code := process.stop(); code != 0 {
				t.Fatalf("restored stop=%d", code)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM standing_service_generations`).Scan(&published); err != nil || published != 1 {
				t.Fatalf("credential restoration minted a successor: %d, %v", published, err)
			}
		})
	}
}

func TestDormantIngressDoesNotWaiveLiveModelReadiness(t *testing.T) {
	for _, dev := range []bool{false, true} {
		name := "serve"
		if dev {
			name = "dev"
		}
		t.Run(name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			t.Setenv("ANTHROPIC_API_KEY", "")
			t.Setenv("TELEGRAM_BOT_TOKEN", "")
			root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
			backend, storePath := "sqlite", filepath.Join(t.TempDir(), "model-readiness.sqlite")
			if dev {
				backend, storePath = "", ""
			}
			var output bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			code := runFrom(ctx, root, cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
				ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, storePath, channelOnboardingHostWorkspaceFields()),
				Dev:        dev, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", WorkspaceBackend: "host", WorkspaceBackendSet: true, Output: &output})
			if code == 0 || ctx.Err() != nil || !strings.Contains(output.String(), "provider_credential_missing component=llm-provider") || strings.Contains(output.String(), "[22/22]") {
				t.Fatalf("dormant ingress waived an independently selected live model: exit=%d context=%v\n%s", code, ctx.Err(), output.String())
			}
		})
	}
}

func TestDormantIngressDoesNotWaiveIndependentOutboundCredential(t *testing.T) {
	for _, signingKey := range []string{"webhook_signing.telegram", "telegram_bot_token"} {
		t.Run(signingKey, func(t *testing.T) {
			testDormantIngressDoesNotWaiveIndependentOutboundCredential(t, signingKey)
		})
	}
}

func testDormantIngressDoesNotWaiveIndependentOutboundCredential(t *testing.T, signingKey string) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	unsetEnvForTest(t, "TELEGRAM_BOT_TOKEN")
	unsetEnvForTest(t, "telegram_bot_token")
	root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	path := filepath.Join(root, "telegram-ingress", "schema.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := yaml.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	ingress := schema["ingress"].(map[string]any)
	providers := ingress["providers"].([]any)
	providers[0].(map[string]any)["signing_secret"] = signingKey
	data, err = yaml.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	writeStandingCandidateFile(t, path, string(data))
	writeStandingCandidateFile(t, filepath.Join(root, "schema.yaml"), `name: telegram-agent
imports:
  provider_trigger_events:
    - provider: telegram
      event: inbound.telegram.text_message
pins:
  inputs: [inbound.telegram.text_message]
  outputs: [inbound.telegram.text_message]
`)
	writeStandingCandidateFile(t, filepath.Join(root, "telegram-chat", "schema.yaml"), `name: telegram-chat
imports:
  connector_packs:
    - provider: telegram
      tool: telegram.send_message
stages: []
`)
	for _, file := range []string{"agents.yaml", "events.yaml"} {
		if err := os.Remove(filepath.Join(root, "telegram-chat", file)); err != nil {
			t.Fatal(err)
		}
	}
	writeStandingCandidateFile(t, filepath.Join(root, "telegram-chat", "nodes.yaml"), `independent-notifier:
  execution_type: system_node
  subscribes_to: [platform.boot]
  event_handlers:
    platform.boot:
      activity:
        id: independent_notice
        tool: telegram.send_message
        input:
          chat_id: "1234"
          text: independent notice
`)
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	code := runFrom(ctx, root, cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
		ConfigPath:    writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", filepath.Join(t.TempDir(), "outbound-readiness.sqlite"), channelOnboardingHostWorkspaceFields()),
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", WorkspaceBackend: "host", WorkspaceBackendSet: true,
		TestLLMRuntime: servedNoopLLMRuntime{}, Output: &output})
	if code == 0 || ctx.Err() != nil || !strings.Contains(output.String(), "credential_key_exists @ telegram_bot_token") || !strings.Contains(output.String(), "required by tool telegram.send_message") || !strings.Contains(output.String(), "independent-notifier") || strings.Contains(output.String(), "[22/22]") {
		t.Fatalf("dormant ingress waived independent outbound demand: exit=%d context=%v\n%s", code, ctx.Err(), output.String())
	}
}
