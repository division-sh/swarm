package releasee2e

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil/connectorapi"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"github.com/division-sh/swarm/internal/testutil/mcpapi"
)

func TestExternalMCPPublicJourneyBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required", goldenPostgresEnv)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			started := time.Now()
			base := filepath.Join(root, backend)
			remote := mcpapi.New(t, "synthetic-mcp-secret")
			model := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "synthetic-llm-secret", externalMCPJourneyReply)
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			source, config := filepath.Join(base, "source"), filepath.Join(base, "swarm.yaml")
			writeReleaseFile(t, config, strings.Replace(goldenRuntimeConfig(store), "  backend: claude_cli\n", fmt.Sprintf("  backend: openai_compatible\n  openai_compatible:\n    base_url: %q\n  models:\n    regular: {openai_compatible: fixture-model}\n", model.URL), 1))
			writeExternalMCPJourneySource(t, source, remote.URL)
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := append(goldenProcessEnv(t, base, store.passwordEnv, 0), "SWARM_CREDENTIALS_FILE="+filepath.Join(base, "credentials.json"))
			secrets := []string{"synthetic-mcp-secret", "synthetic-llm-secret"}
			for key, secret := range map[string]string{"MCP_FIXTURE_KEY": secrets[0], "OPENAI_COMPATIBLE_API_KEY": secrets[1]} {
				result := runReleaseCommand(t, goldenStartupTimeout, base, env, secret+"\n", binary, "--config", config, "secrets", "set", key, "--stdin")
				requireConnectorSecretsAbsent(t, result.output, secrets)
				if result.err != nil {
					t.Fatal("public MCP/LLM credential setup failed")
				}
			}
			process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, ConfigPath: config, Source: source, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env, DefaultExecutionSelection: true, RedactValues: secrets})
			ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("public MCP serve: %v\n%s", err, process.output.String())
			}
			before := remote.Calls()
			if len(before) == 0 {
				t.Fatal("serve did not discover the external MCP server")
			}
			for _, call := range before {
				if call.Method != "initialize" && call.Method != "notifications/initialized" && call.Method != "tools/list" {
					t.Fatalf("discovery invoked execution method %s", call.Method)
				}
			}
			hash := goldenServedBundleHash(t, process.rpc, "live")
			var published struct {
				RunID   string `json:"run_id"`
				EventID string `json:"event_id"`
			}
			if err := process.rpc.call(ctx, "event.publish", map[string]any{"bundle_hash": hash, "event_name": "external.requested", "payload": map[string]any{"text": "synthetic MCP request"}, "idempotency_key": "external-mcp-once"}, &published); err != nil || published.RunID == "" || published.EventID == "" {
				t.Fatalf("public MCP input failed: %v", err)
			}
			completed := waitConnectorPublicResult(t, process, published.RunID, "external.completed", 1, secrets)
			if completed.Payload["text"] != "synthetic MCP request" {
				t.Fatalf("public result lost external MCP value: %#v", completed.Payload)
			}
			calls := 0
			for _, call := range remote.Calls() {
				if call.Auth != "Bearer synthetic-mcp-secret" {
					t.Fatal("MCP discovery or invocation lost the bound credential")
				}
				if call.Method == "tools/call" {
					calls++
					if call.Tool != "ping" || !reflect.DeepEqual(call.Arguments, map[string]any{"text": "synthetic MCP request"}) {
						t.Fatalf("external MCP arguments changed: %s %#v", call.Tool, call.Arguments)
					}
				}
			}
			if calls != 1 || len(model.Requests()) != 2 {
				t.Fatalf("MCP calls/model requests=%d/%d, want 1/2", calls, len(model.Requests()))
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			requireConnectorSecretsAbsent(t, process.output.String(), secrets)
			t.Logf("real binary MCP discovery/agent/external-call/public-result; backend=%s; elapsed=%s (shared build excluded)", backend, time.Since(started))
		})
	}
}

func externalMCPJourneyReply(request llmapi.Request) (llmapi.Reply, error) {
	var body struct {
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
			CallID  string `json:"tool_call_id"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return llmapi.Reply{}, err
	}
	names := map[string]bool{}
	for _, tool := range body.Tools {
		names[tool.Function.Name] = true
	}
	if !names["partner.ping"] || !names["emit_external_completed"] {
		return llmapi.Reply{}, fmt.Errorf("model did not receive the exact external/terminal tools: %v", names)
	}
	reply := llmapi.Reply{ID: fmt.Sprintf("integration-%d", request.Number), Model: "fixture-model", Reasoning: "synthetic integration reasoning", InputTokens: 10, OutputTokens: 5, TotalTokens: 15}
	switch request.Number {
	case 1:
		found := false
		for _, message := range body.Messages {
			found = found || message.Role == "user" && message.Content == `{"text":"synthetic MCP request"}`
		}
		if !found {
			return reply, fmt.Errorf("model input lost the authored public payload")
		}
		reply.Tools = []llmapi.ToolCall{{ID: "mcp-call", Name: "partner.ping", Arguments: json.RawMessage(`{"text":"synthetic MCP request"}`)}}
	case 2:
		found := false
		for _, message := range body.Messages {
			if message.Role != "tool" || message.CallID != "mcp-call" {
				continue
			}
			var value map[string]any
			if err := json.Unmarshal([]byte(message.Content), &value); err != nil || value["ok"] != true || value["echo"] != "synthetic MCP request" {
				return reply, fmt.Errorf("model continuation lost the structured MCP result")
			}
			found = true
		}
		if !found {
			return reply, fmt.Errorf("model continuation lost the MCP call ID")
		}
		reply.Tools = []llmapi.ToolCall{{ID: "emit-call", Name: "emit_external_completed", Arguments: json.RawMessage(`{"text":"synthetic MCP request"}`)}}
	default:
		return reply, fmt.Errorf("unexpected model request %d", request.Number)
	}
	return reply, nil
}

func writeExternalMCPJourneySource(t *testing.T, root, endpoint string) {
	t.Helper()
	for path, body := range map[string]string{
		"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":   "stages: []\npins:\n  inputs: [external.requested, external.completed]\n",
		"events.yaml":   "external.requested: {text: string}\nexternal.completed: {text: string}\n",
		"policy.yaml":   fmt.Sprintf("mcp_servers:\n  configured:\n    transport: http\n    url: %q\n    prefix: partner\n    credentials_key: MCP_FIXTURE_KEY\n", endpoint),
		"agents.yaml":   "worker:\n  role: integration\n  model: regular\n  subscriptions: [external.requested]\n  tools: [partner.ping]\n  emit_events: [external.completed]\n  intent:\n    inline: 'Call partner.ping with the current input text, then emit_external_completed with the exact echo returned by that tool. Do not invent the result or produce prose.'\n",
		"nodes.yaml":    "observer:\n  execution_type: system_node\n  event_handlers:\n    external.completed:\n      guard: {id: observe, check: true}\n",
	} {
		writeReleaseFile(t, filepath.Join(root, path), body)
	}
}

func TestOAuthConnectPublicJourneyBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required", goldenPostgresEnv)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			started := time.Now()
			base := filepath.Join(root, backend)
			slack := connectorapi.NewSlack(t)
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config, source := filepath.Join(base, "swarm.yaml"), filepath.Join(base, "source")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := append(goldenProcessEnv(t, base, store.passwordEnv, 0), "SWARM_CREDENTIALS_FILE="+filepath.Join(base, "credentials.json"), "SWARM_MANAGED_CREDENTIALS_FILE="+filepath.Join(base, "managed.json"))
			secrets := []string{"slack-secret", "fixture-code", "fresh-token", "refresh-secret"}
			command := func(input string, args ...string) releaseCommandResult {
				t.Helper()
				result := runReleaseCommand(t, goldenStartupTimeout, base, env, input, binary, append([]string{"--config", config}, args...)...)
				requireConnectorSecretsAbsent(t, result.output, secrets)
				return result
			}
			writeOAuthJourneySource(t, source, slack.Server.URL, func(args ...string) {
				if result := command("", args...); result.err != nil {
					t.Fatalf("public import failed: %s", result.output)
				}
			})
			begin := command("slack-secret\n", "connections", "connect", "slack.slack_oauth", "--grant", "authorization_code_pkce", "--provider", "slack", "--auth-url", slack.Server.URL+"/authorize", "--token-url", slack.Server.URL+"/oauth.v2.access", "--client-id", "slack-client", "--client-secret-stdin", "--redirect-url", "http://127.0.0.1/callback", "--scope", "chat:write", "--json")
			var begun struct {
				State        string `json:"state"`
				AuthorizeURL string `json:"authorize_url"`
			}
			if begin.err != nil || json.Unmarshal([]byte(begin.output), &begun) != nil || begun.State == "" {
				t.Fatal("public PKCE connect did not establish pending state")
			}
			authorize, err := url.Parse(begun.AuthorizeURL)
			if err != nil || authorize.Query().Get("state") != begun.State || authorize.Query().Get("code_challenge_method") != "S256" || authorize.Query().Get("code_challenge") == "" {
				t.Fatal("public authorize URL lost state/PKCE binding")
			}
			wrong := command("fixture-code\n", "connections", "callback", "slack.slack_oauth", "--state", "wrong-state", "--code-stdin", "--json")
			if wrong.err == nil || !strings.Contains(wrong.output, "state mismatch") || slack.TokenCalls.Load() != 0 {
				t.Fatalf("wrong-state callback admitted/exchanged: err=%v requests=%d", wrong.err, slack.TokenCalls.Load())
			}
			requirePublicOAuthStatus(t, command("", "connections", "status", "slack.slack_oauth", "--json"), "pending_consent")
			completed := command("fixture-code\n", "connections", "callback", "slack.slack_oauth", "--state", begun.State, "--code-stdin", "--json")
			if completed.err != nil || slack.TokenCalls.Load() != 1 {
				t.Fatal("correct-state callback did not exchange exactly once")
			}
			select {
			case form := <-slack.TokenRequests:
				digest := sha256.Sum256([]byte(form.Get("code_verifier")))
				if form.Get("code_verifier") == "" || base64.RawURLEncoding.EncodeToString(digest[:]) != authorize.Query().Get("code_challenge") || form.Get("code") != "fixture-code" || form.Get("client_id") != "slack-client" || form.Get("client_secret") != "slack-secret" || form.Get("redirect_uri") != "http://127.0.0.1/callback" {
					t.Fatal("token exchange lost exact client/code/redirect/PKCE binding (values withheld)")
				}
				secrets = append(secrets, form.Get("code_verifier"))
			default:
				t.Fatal("token endpoint did not observe the correct callback")
			}
			requirePublicOAuthStatus(t, command("", "connections", "status", "slack.slack_oauth", "--json"), "connected")
			requireConnectorSecretsAbsent(t, begin.output+wrong.output+completed.output, secrets)
			process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, Source: source, ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env, RedactValues: secrets})
			ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("serve did not consume publicly connected credential: %v\n%s", err, process.output.String())
			}
			hash := goldenServedBundleHash(t, process.rpc, "live")
			var published struct {
				RunID string `json:"run_id"`
			}
			if err := process.rpc.call(ctx, "event.publish", map[string]any{"bundle_hash": hash, "event_name": "oauth.requested", "payload": map[string]any{"text": "public OAuth journey"}, "idempotency_key": "oauth-once"}, &published); err != nil || published.RunID == "" {
				t.Fatalf("public OAuth input failed: %v", err)
			}
			waitConnectorPublicResult(t, process, published.RunID, "send_slack.succeeded", 1, secrets)
			requireConnectorCall(t, slack.SideEffects, "POST", "/api/chat.postMessage", "Bearer fresh-token", map[string]any{"channel": "C123", "text": "public OAuth journey"})
			if slack.Calls.Load() != 1 || slack.TokenCalls.Load() != 1 {
				t.Fatal("served OAuth journey repeated token exchange or provider effect")
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			requireConnectorSecretsAbsent(t, process.output.String(), secrets)
			t.Logf("real binary PKCE/refusal/callback/status/served-effect; backend=%s; elapsed=%s (shared build excluded)", backend, time.Since(started))
		})
	}
}

func requirePublicOAuthStatus(t *testing.T, result releaseCommandResult, want string) {
	t.Helper()
	var status struct {
		Connections []struct {
			Key     string `json:"key"`
			Status  string `json:"status"`
			Present bool   `json:"present"`
		} `json:"connections"`
	}
	if result.err != nil || json.Unmarshal([]byte(result.output), &status) != nil || len(status.Connections) != 1 || status.Connections[0].Key != "slack.slack_oauth" || !status.Connections[0].Present || status.Connections[0].Status != want {
		t.Fatalf("public connection status did not equal %s", want)
	}
}

func writeOAuthJourneySource(t *testing.T, root, endpoint string, command func(...string)) {
	t.Helper()
	writeReleaseFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeReleaseFile(t, filepath.Join(root, "schema.yaml"), "stages: []\npins:\n  inputs: [oauth.requested]\n")
	writeReleaseFile(t, filepath.Join(root, "events.yaml"), "oauth.requested: {text: string}\n")
	writeReleaseFile(t, filepath.Join(root, "nodes.yaml"), "sender:\n  execution_type: system_node\n  subscribes_to: [oauth.requested]\n  event_handlers:\n    oauth.requested:\n      activity:\n        id: send_slack\n        tool: slack.post_message\n        input:\n          channel: \"C123\"\n          text: payload.text\n")
	command("import", "slack", root, "--json")
	path := filepath.Join(root, "slack", "connector.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "https://slack.com/") {
		t.Fatal("embedded Slack connector lost its exact provider origin")
	}
	writeReleaseFile(t, path, strings.ReplaceAll(string(raw), "https://slack.com", endpoint))
}
