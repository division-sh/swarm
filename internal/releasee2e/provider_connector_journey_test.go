package releasee2e

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil/connectorapi"
	"gopkg.in/yaml.v3"
)

func TestEmbeddedConnectorPublicJourneysBothStores(t *testing.T) {
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
			notion := connectorapi.NewNotion(t)
			graph := connectorapi.NewMicrosoftGraph(t)
			github := connectorapi.NewGitHub(t, filepath.Join(releaseE2ERepoRoot(t), "internal/providerconnectors/catalog/conformance/github/issues-create.yaml"))
			t.Cleanup(slack.Server.Close)
			t.Cleanup(notion.Server.Close)
			t.Cleanup(graph.Server.Close)
			t.Cleanup(github.Server.Close)
			endpoints := map[string]string{"slack": slack.Server.URL, "notion": notion.Server.URL, "microsoft_graph": graph.Server.URL, "github": github.Server.URL}
			source := filepath.Join(base, "connectors")
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config := filepath.Join(base, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := append(goldenProcessEnv(t, base, store.passwordEnv, 0),
				"SWARM_MANAGED_CREDENTIALS_FILE="+filepath.Join(base, "managed-credentials.json"),
				"SWARM_CREDENTIALS_FILE="+filepath.Join(base, "credentials.json"))
			secrets := []string{"slack-secret", "notion-secret", "graph-client-secret", "fresh-token", "after-401-token", "graph-fresh-token", "graph-after-401-token", "refresh-secret", "rotated-refresh", "after-401-refresh", "fixture-github-installation-token", "connector-webhook-secret", github.PrivateKey}
			command := func(input string, args ...string) string {
				t.Helper()
				result := runReleaseCommand(t, goldenStartupTimeout, base, env, input, binary, append([]string{"--config", config}, args...)...)
				requireConnectorSecretsAbsent(t, result.output, secrets)
				if result.err != nil {
					t.Fatalf("public command %s: %v\n%s", args[0], result.err, result.output)
				}
				return result.output
			}
			writeConnectorPublicSource(t, source, endpoints, github.Fixture.Input, command)
			connectPublicConnectorCode(t, command, "slack.slack_oauth", "slack", slack.Server.URL+"/oauth.v2.access", "slack-client", "slack-secret", "--scope", "chat:write")
			connectPublicConnectorCode(t, command, "notion.notion_oauth", "notion", notion.Server.URL+"/v1/oauth/token", "notion-client", "notion-secret", "--grant-model", "workspace_grant", "--token-client-auth", "basic", "--token-body", "json", "--token-header", "Notion-Version=2026-03-11")
			command("graph-client-secret\n", "connections", "connect", "microsoft_graph.microsoft_graph_app", "--grant", "client_credentials", "--provider", "microsoft_graph", "--token-url", graph.Server.URL+"/tenant/oauth2/v2.0/token", "--client-id", "graph-client", "--client-secret-stdin", "--scope", "https://graph.microsoft.com/.default", "--json")
			command(github.PrivateKey, "connections", "connect", "github.github_app", "--grant", "github_app_installation", "--provider", "github", "--api-base-url", github.Server.URL, "--client-id", "github-app-client-id", "--installation-id", "1001", "--private-key-stdin", "--json")
			command("connector-webhook-secret\n", "secrets", "set", "github.webhook_signing_secret", "--stdin")
			for _, key := range []string{"slack.slack_oauth", "notion.notion_oauth", "microsoft_graph.microsoft_graph_app", "github.github_app"} {
				var status struct {
					Connections []struct {
						Key, Status string
						Present     bool
					}
				}
				if err := json.Unmarshal([]byte(command("", "connections", "status", key, "--json")), &status); err != nil || len(status.Connections) != 1 || status.Connections[0].Key != key || status.Connections[0].Status != "connected" || !status.Connections[0].Present {
					t.Fatalf("public connection did not establish %s: %#v %v", key, status, err)
				}
			}
			verify := command("", "verify", source, "--portable", "--json")
			assertFullLifecycleVerifySuccess(t, releaseCommandResult{output: verify})
			publicListen := fmt.Sprintf("127.0.0.1:%d", freeReleaseTCPPort(t))
			publicTarget, err := url.Parse("http://" + publicListen)
			if err != nil {
				t.Fatal(err)
			}
			publicOrigin := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(publicTarget))
			t.Cleanup(publicOrigin.Close)
			certificate := filepath.Join(base, "public-ca.pem")
			writeReleaseFile(t, certificate, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: publicOrigin.Certificate().Raw})))
			env = append(env, "SSL_CERT_FILE="+certificate)
			process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, Source: source, ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env,
				PublicWebhookBaseURL: publicOrigin.URL, PublicWebhookListen: publicListen, RedactValues: secrets})
			ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("public serve: %v\n%s", err, process.output.String())
			}
			hash := goldenServedBundleHash(t, process.rpc, "live")
			runID := ""
			var listed struct {
				Runs []struct {
					RunID      string `json:"run_id"`
					BundleHash string `json:"bundle_hash"`
					FlowPath   string `json:"flow_path"`
					Status     string `json:"status"`
				} `json:"runs"`
			}
			if err := process.rpc.call(ctx, "run.list", map[string]any{"bundle_hash": hash, "limit": 100}, &listed); err != nil {
				t.Fatal(err)
			}
			for _, run := range listed.Runs {
				if run.BundleHash == hash && run.FlowPath == "." && run.Status == "live" {
					if runID != "" {
						t.Fatal("more than one current root holder")
					}
					runID = run.RunID
				}
			}
			if runID == "" {
				t.Fatal("public ingress has no current root holder")
			}
			publish := func(t *testing.T, provider, text, outcome string, count int) goldenEvent {
				t.Helper()
				params := map[string]any{"bundle_hash": hash, "event_name": provider + ".requested", "payload": map[string]any{"text": text}, "idempotency_key": provider + "-" + text}
				if runID != "" {
					params["run_id"] = runID
				}
				var result struct {
					RunID   string `json:"run_id"`
					EventID string `json:"event_id"`
				}
				if err := process.rpc.call(ctx, "event.publish", params, &result); err != nil || result.RunID == "" || result.EventID == "" {
					t.Fatalf("public %s input: %#v %v", provider, result, err)
				}
				if runID != "" && runID != result.RunID {
					t.Fatal("connector publication changed the selected run")
				}
				runID = result.RunID
				return waitConnectorPublicResult(t, process, runID, "send_"+provider+"."+outcome, count, secrets)
			}
			t.Run("slack", func(t *testing.T) {
				publish(t, "slack", "hello from telegram", "succeeded", 1)
				requireConnectorCall(t, slack.SideEffects, "POST", "/api/chat.postMessage", "Bearer fresh-token", map[string]any{"channel": "C123", "text": "hello from telegram"})
				publish(t, "slack", "needs 401 refresh", "succeeded", 2)
				requireConnectorCall(t, slack.SideEffects, "POST", "/api/chat.postMessage", "Bearer after-401-token", map[string]any{"channel": "C123", "text": "needs 401 refresh"})
				publish(t, "slack", "provider ok false", "failed", 1)
				if slack.Calls.Load() != 4 || slack.TokenCalls.Load() != 2 || len(slack.SideEffects) != 0 {
					t.Fatal("Slack lost exact success/401-refresh/provider-error accounting")
				}
			})
			t.Run("notion", func(t *testing.T) {
				publish(t, "notion", "first block", "succeeded", 1)
				want := map[string]any{"children": []any{map[string]any{"object": "block", "type": "paragraph", "paragraph": map[string]any{"rich_text": []any{map[string]any{"type": "text", "text": map[string]any{"content": "hello from swarm"}}}}}}}
				requireConnectorCall(t, notion.SideEffects, "PATCH", "/v1/blocks/block-123/children", "Bearer fresh-token", want)
				publish(t, "notion", "needs 401 refresh", "succeeded", 2)
				requireConnectorCall(t, notion.SideEffects, "PATCH", "/v1/blocks/block-123/children", "Bearer after-401-token", want)
				publish(t, "notion", "provider 429 fixture", "failed", 1)
				if notion.Calls.Load() != 4 || notion.TokenCalls.Load() != 2 || len(notion.SideEffects) != 0 {
					t.Fatal("Notion lost exact success/401-refresh/provider-error accounting")
				}
			})
			t.Run("microsoft_graph", func(t *testing.T) {
				for i, text := range []string{"send first mail", "needs 401 refresh"} {
					publish(t, "microsoft_graph", text, "succeeded", i+1)
					auth := "Bearer graph-fresh-token"
					if i == 1 {
						auth = "Bearer graph-after-401-token"
					}
					requireConnectorCall(t, graph.SideEffects, "POST", "/v1.0/users/user@example.com/sendMail", auth, map[string]any{"message": map[string]any{"subject": text, "body": map[string]any{"contentType": "Text", "content": text}, "toRecipients": []any{map[string]any{"emailAddress": map[string]any{"address": "recipient@example.com"}}}}})
				}
				publish(t, "microsoft_graph", "provider 429 fixture", "failed", 1)
				if graph.Calls.Load() != 4 || graph.TokenCalls.Load() != 2 || len(graph.RefreshTokens) != 0 || len(graph.SideEffects) != 0 {
					t.Fatal("Graph lost exact client-credentials/401/provider-error accounting")
				}
			})
			t.Run("github", func(t *testing.T) {
				publish(t, "github", "direct catalog issue", "succeeded", 1)
				requireConnectorCall(t, github.SideEffects, github.Fixture.Expected.Method, "/repos/octocat/hello-world/issues", github.Fixture.Expected.Headers["Authorization"], github.Fixture.Expected.Body)
				postConnectorGitHubWebhook(t, process, publicOrigin.URL, publicOrigin.Client(), runID, github)
				if github.Calls.Load() != 2 || github.TokenCalls.Load() != 1 || len(github.SideEffects) != 0 {
					t.Fatal("GitHub duplicate or hostile delivery changed provider effects")
				}
			})
			var diagnosis goldenDiagnosis
			if err := process.rpc.call(ctx, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(diagnosis)
			if err != nil {
				t.Fatal(err)
			}
			requireConnectorSecretsAbsent(t, string(raw), secrets)
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			requireConnectorSecretsAbsent(t, process.output.String(), secrets)
			t.Logf("real binary serve/CLI/HTTP; backend=%s; four providers; one serve; elapsed=%s (binary build excluded)", backend, time.Since(started))
		})
	}
}

func connectPublicConnectorCode(t *testing.T, command func(string, ...string) string, key, provider, tokenURL, clientID, secret string, extra ...string) {
	t.Helper()
	args := []string{"connections", "connect", key, "--grant", "authorization_code", "--provider", provider, "--auth-url", strings.TrimSuffix(tokenURL, "/token") + "/authorize", "--token-url", tokenURL, "--client-id", clientID, "--client-secret-stdin", "--redirect-url", "http://127.0.0.1/callback", "--json"}
	var begun struct{ State string }
	if err := json.Unmarshal([]byte(command(secret+"\n", append(args, extra...)...)), &begun); err != nil || begun.State == "" {
		t.Fatalf("public auth-code begin: %v", err)
	}
	command("fixture-code\n", "connections", "callback", key, "--state", begun.State, "--code-stdin", "--json")
}

func requireConnectorSecretsAbsent(t testing.TB, text string, secrets []string) {
	t.Helper()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(text, secret) {
			t.Fatal("public output exposed a fixture credential (output withheld)")
		}
	}
}

func requireConnectorCall(t *testing.T, calls <-chan connectorapi.Call, method, path, auth string, body map[string]any) {
	t.Helper()
	select {
	case got := <-calls:
		if got.Method != method || got.Path != path || got.Auth != auth || !reflect.DeepEqual(got.Body, body) {
			t.Fatalf("provider request mismatch: method=%s path=%s body=%s; credential withheld", got.Method, got.Path, got.Raw)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("successful public result has no provider HTTP effect")
	}
}

func waitConnectorPublicResult(t *testing.T, process *releaseServeProcess, runID, name string, count int, secrets []string) goldenEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var found goldenEvent
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		events, err := listGoldenEvents(ctx, process.rpc, runID)
		if err != nil {
			return false, err
		}
		seen := 0
		for _, event := range events {
			if event.EventName == name {
				seen++
				found = event
			}
		}
		if seen > count {
			return false, fmt.Errorf("public %s has %d receipts, want %d", name, seen, count)
		}
		return seen == count, nil
	})
	if err != nil {
		t.Fatalf("wait for public terminal activity %s: %v\n%s", name, err, process.output.String())
	}
	if found.EventID == "" || found.ExecutionMode != "live" {
		t.Fatalf("activity did not execute through live dispatch: %#v", found)
	}
	raw, err := json.Marshal(found)
	if err != nil {
		t.Fatal(err)
	}
	requireConnectorSecretsAbsent(t, string(raw), secrets)
	return found
}

func postConnectorGitHubWebhook(t *testing.T, process *releaseServeProcess, origin string, client *http.Client, runID string, github *connectorapi.GitHub) {
	t.Helper()
	client.Timeout = 5 * time.Second
	body := []byte(`{"action":"opened","installation":{"id":1001},"repository":{"owner":{"login":"octocat"},"name":"hello-world"},"issue":{"number":7,"title":"Generated issue"}}`)
	post := func(delivery, secret string) (int, string) {
		t.Helper()
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(body)
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, origin+"/webhooks/connectors/github", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-GitHub-Delivery", delivery)
		request.Header.Set("X-GitHub-Event", "issues")
		request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, string(raw)
	}
	before, err := listGoldenEvents(t.Context(), process.rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := post("hostile-delivery", "wrong-signing-key"); status != http.StatusUnauthorized {
		t.Fatalf("hostile GitHub signature HTTP=%d", status)
	}
	after, err := listGoldenEvents(t.Context(), process.rpc, runID)
	if err != nil || !reflect.DeepEqual(connectorEventIDs(before), connectorEventIDs(after)) || github.Calls.Load() != 1 {
		t.Fatal("hostile GitHub delivery published or caused an external effect")
	}
	if status, response := post("catalog-delivery", "connector-webhook-secret"); status != http.StatusAccepted {
		var logs struct {
			Logs []goldenRuntimeLog `json:"logs"`
		}
		if err := process.rpc.call(t.Context(), "runtime.logs", map[string]any{"component": "inbound-gateway", "limit": 20, "order": "desc"}, &logs); err == nil {
			raw, err := json.Marshal(logs)
			if err == nil {
				requireConnectorSecretsAbsent(t, string(raw), []string{"connector-webhook-secret", "fixture-github-installation-token"})
				t.Logf("public inbound failure readback: %s", raw)
			}
		}
		t.Logf("served failure output: %s", process.output.String())
		t.Fatalf("signed GitHub delivery HTTP=%d: %s", status, response)
	}
	waitConnectorPublicResult(t, process, runID, "send_github_webhook.succeeded", 1, []string{"connector-webhook-secret", "fixture-github-installation-token"})
	requireConnectorCall(t, github.SideEffects, github.Fixture.Expected.Method, "/repos/octocat/hello-world/issues", github.Fixture.Expected.Headers["Authorization"], github.Fixture.Expected.Body)
	before, err = listGoldenEvents(t.Context(), process.rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	if status, response := post("catalog-delivery", "connector-webhook-secret"); status != http.StatusOK || !strings.Contains(response, "duplicate") {
		t.Fatalf("duplicate GitHub delivery HTTP=%d: %s", status, response)
	}
	after, err = listGoldenEvents(t.Context(), process.rpc, runID)
	if err != nil || !reflect.DeepEqual(connectorEventIDs(before), connectorEventIDs(after)) || github.Calls.Load() != 2 {
		t.Fatal("duplicate GitHub delivery repeated publication or provider effect")
	}
}

func connectorEventIDs(events []goldenEvent) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.EventID)
	}
	slices.Sort(ids)
	return ids
}

func writeConnectorPublicSource(t *testing.T, root string, endpoints map[string]string, githubInput map[string]any, command func(string, ...string) string) {
	t.Helper()
	writeReleaseFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeReleaseFile(t, filepath.Join(root, "schema.yaml"), "stages: []\npins:\n  inputs: [slack.requested, notion.requested, microsoft_graph.requested, github.requested, inbound.github.issue_opened]\n")
	inputs := map[string]any{}
	nodes := map[string]any{}
	literal := func(value any) *yaml.Node {
		node := &yaml.Node{}
		if err := node.Encode(value); err != nil {
			t.Fatal(err)
		}
		if node.Kind == yaml.ScalarNode && node.Tag == "!!str" {
			node.Style = yaml.DoubleQuotedStyle
		}
		return node
	}
	githubValues := map[string]any{}
	for key, value := range githubInput {
		githubValues[key] = literal(value)
	}
	activities := map[string]struct {
		tool  string
		input map[string]any
	}{
		"slack":           {"post_message", map[string]any{"channel": literal("C123"), "text": "payload.text"}},
		"notion":          {"append_block_children", map[string]any{"block_id": literal("block-123"), "children": []any{map[string]any{"object": literal("block"), "type": literal("paragraph"), "paragraph": map[string]any{"rich_text": []any{map[string]any{"type": literal("text"), "text": map[string]any{"content": literal("hello from swarm")}}}}}}}},
		"microsoft_graph": {"send_mail", map[string]any{"user_id": literal("user@example.com"), "to_recipients": []any{map[string]any{"emailAddress": map[string]any{"address": literal("recipient@example.com")}}}, "subject": "payload.text", "content": "payload.text"}},
		"github":          {"create_issue", githubValues},
	}
	origins := map[string]string{"slack": "https://slack.com", "notion": "https://api.notion.com", "microsoft_graph": "https://graph.microsoft.com", "github": "https://api.github.com"}
	for provider, activity := range activities {
		inputs[provider+".requested"] = map[string]any{"text": "text"}
		nodes[provider+"-worker"] = map[string]any{"execution_type": "system_node", "subscribes_to": []string{provider + ".requested"}, "event_handlers": map[string]any{provider + ".requested": map[string]any{"activity": map[string]any{"id": "send_" + provider, "tool": provider + "." + activity.tool, "input": activity.input}}}}
		command("", "import", provider, root, "--json")
		path := filepath.Join(root, provider, "connector.yaml")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		if err := yaml.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		for _, entry := range body["tools"].(map[string]any) {
			httpBody := entry.(map[string]any)["http"].(map[string]any)
			url := httpBody["url"].(string)
			if !strings.HasPrefix(url, origins[provider]+"/") {
				t.Fatal("embedded connector changed its exact provider origin")
			}
			httpBody["url"] = endpoints[provider] + strings.TrimPrefix(url, origins[provider])
		}
		raw, err = yaml.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		writeReleaseFile(t, path, string(raw))
	}
	nodes["github-webhook-worker"] = map[string]any{"execution_type": "system_node", "subscribes_to": []string{"inbound.github.issue_opened"}, "event_handlers": map[string]any{"inbound.github.issue_opened": map[string]any{"activity": map[string]any{"id": "send_github_webhook", "tool": "github.create_issue", "input": githubValues}}}}
	for path, document := range map[string]any{"events.yaml": inputs, "nodes.yaml": nodes} {
		raw, err := yaml.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		writeReleaseFile(t, filepath.Join(root, path), string(raw))
	}
}
