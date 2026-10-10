//go:build linux || darwin

package serveapp

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/tools"
)

func TestWhatsAppAuthoredReplyFixtureUsesInstalledPrivateConnector(t *testing.T) {
	repo := repoRootForTest()
	module, _, err := cliapp.NewSwarmWorkflowModule(repo, filepath.Join(repo, "internal/serveapp/testdata/whatsapp-session-reply"), filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	tool, found := module.SemanticSource().ToolEntries()["whatsapp.send_text"]
	native, installed := tool.InProcess()
	if !found || !installed || native != contracts.ToolInProcessWhatsAppSendText || tool.AgentExposable() || tool.Effect() != contracts.ActivityEffectClassNonIdempotentWrite {
		t.Fatal("authored fixture did not consume its exact private connector")
	}
	if warnings, err := tools.ValidateToolImplementations(module.SemanticSource()); err != nil || len(warnings) != 0 {
		t.Fatal("declaration-first native activity admission failed", warnings, err)
	}
	readAccount := contracts.MustToolSchemaEntry(contracts.WithToolHandler(contracts.ToolHandlerInProcess),
		contracts.WithToolCategory(contracts.ToolCategoryProviderRegistration.String()), contracts.WithToolEffect(contracts.ActivityEffectClassReadOnly),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppReadAccount))
	if _, err := tools.ValidateToolImplementations(semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{"whatsapp.read_account": readAccount}})); err == nil {
		t.Fatal("session-write qualification admitted read-account execution")
	}
}

func requireServedNativeAuthoredCustomerReply(t *testing.T, endpoint string, peer *serveNativeProtocolPeer, uncertain bool) servedNativeMessage {
	t.Helper()
	const text = "A genuine customer message for the authored activity"
	if uncertain {
		peer.mu.Lock()
		peer.malformedAck = text
		peer.mu.Unlock()
	}
	customer, customerLID := peer.customer(text, "SERVED_AUTHORED_CUSTOMER")
	deadline := time.After(5 * time.Second)
	for {
		select {
		case sent := <-peer.sent:
			if sent.To.ToNonAD() != customer && sent.To.ToNonAD() != customerLID {
				if sent.Body.GetConversation() == text {
					t.Fatal("authored business reply was sent to the operator mailbox")
				}
				continue
			}
			if sent.Body.GetConversation() != text || sent.ID == "" {
				t.Fatal("authored activity changed the admitted customer input", sent.ID, sent.Body)
			}
			requireServedNativeActivityResult(t, endpoint, sent.ID, customer.String(), text, uncertain)
			return sent
		case <-deadline:
			logServedNativeActivityFailure(t, endpoint)
			t.Fatal("enabled authored native activity did not produce a genuine encrypted customer reply")
		}
	}
}

func requireServedNativeActivityResult(t *testing.T, endpoint, messageID, destination, text string, uncertain bool) {
	t.Helper()
	code := ""
	if uncertain {
		code = "native_activity_acknowledgment_invalid"
	}
	requireServedNativeActivityOutcome(t, endpoint, messageID, destination, text, code)
}

func requireServedNativeActivityOutcome(t *testing.T, endpoint, messageID, destination, text, failureCode string) {
	t.Helper()
	var runs struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"limit": 10}, &runs)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, run := range runs.Runs {
			var requests operatorread.OperatorEventListResult
			requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{
				"filter": map[string]any{"run_id": run.RunID, "event_name": "platform.activity_requested"}, "limit": 10}, &requests)
			for _, request := range requests.Events {
				if request.Payload["tool"] != "whatsapp.send_text" {
					continue
				}
				target, _ := request.Payload["native_session_target"].(string)
				input, _ := request.Payload["input"].(map[string]any)
				if !strings.HasPrefix(target, contracts.PrivateChannelActivityPrefix) || request.Payload["plan_generation"] == nil ||
					request.Payload["channel_activation_generation"] == nil || request.Payload["bundle_hash"] == "" ||
					request.Payload["workflow_version"] == "" || input["destination"] != destination || input["text"] != text ||
					strings.ReplaceAll(request.EventID, "-", "") != messageID || len(requests.Events) != 1 {
					t.Fatal("served native request lost its immutable source/target/input", request.Payload)
				}
				outcome := request.Payload["success_event"]
				if failureCode != "" {
					outcome = request.Payload["failure_event"]
				}
				var results operatorread.OperatorEventListResult
				requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{
					"filter": map[string]any{"run_id": run.RunID, "event_name": outcome}, "limit": 10}, &results)
				for _, result := range results.Events {
					if result.Payload["activity_id"] != request.Payload["activity_id"] {
						continue
					}
					if result.SourceEventID != request.Payload["source_event_id"] || len(results.Events) != 1 || result.Payload["tool"] != "whatsapp.send_text" {
						t.Fatal("native activity outcome lost its original request", result)
					}
					if failureCode != "" {
						failure, _ := result.Payload["failure"].(map[string]any)
						detail, _ := failure["detail"].(map[string]any)
						if failure["class"] != "platform.outcome_uncertain" || detail["code"] != failureCode {
							t.Fatal("unconfirmed native effect became success or lost its exact uncertainty", result.Payload)
						}
					} else {
						value, _ := result.Payload["result"].(map[string]any)
						if len(value) != 1 || value["id"] != messageID {
							t.Fatal("authored result adopted channel receipt semantics or changed the observed acknowledgment", result.Payload)
						}
					}
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	logServedNativeActivityFailure(t, endpoint)
	t.Fatal("genuine native reply did not settle its original activity request/result")
}

func logServedNativeActivityFailure(t *testing.T, endpoint string) {
	t.Helper()
	var runs struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"limit": 10}, &runs)
	for _, run := range runs.Runs {
		for _, method := range []string{"event.list", "runtime.logs"} {
			params := map[string]any{"filter": map[string]any{"run_id": run.RunID}, "limit": 50}
			if method == "runtime.logs" {
				params = map[string]any{"run_id": run.RunID, "level": "error", "limit": 10}
			}
			response := requestServedJSONRPC(t, endpoint, method, params)
			t.Logf("authored native %s run=%s error=%v result=%s", method, run.RunID, response.Error, response.Result)
		}
	}
}
