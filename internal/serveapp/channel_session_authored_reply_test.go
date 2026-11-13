//go:build linux || darwin

package serveapp

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"go.mau.fi/whatsmeow/types"
)

func TestWhatsAppAuthoredReplyFixtureUsesInstalledPrivateConnector(t *testing.T) {
	repo := repoRootForTest()
	module, _, err := cliapp.NewSwarmWorkflowModule(repo, filepath.Join(repo, "internal/serveapp/testdata/whatsapp-session-reply"))
	if err != nil {
		t.Fatal(err)
	}
	tool, found := module.SemanticSource().ToolEntries()["whatsapp.send_text"]
	native, installed := tool.InProcess()
	if !found || !installed || native != contracts.ToolInProcessWhatsAppSendText || tool.AgentExposable() {
		t.Fatal("authored fixture did not consume its exact private connector")
	}
	if warnings, err := tools.ValidateToolImplementations(module.SemanticSource()); err != nil || len(warnings) != 0 {
		t.Fatal("declaration-first native activity admission failed", warnings, err)
	}
}

func requireServedNativeAuthoredCustomerReply(t *testing.T, endpoint string, peer *serveNativeProtocolPeer, uncertain, earlierActivities bool, messageIDs ...string) servedNativeMessage {
	t.Helper()
	const text = "A genuine customer message for the authored activity"
	if uncertain {
		peer.mu.Lock()
		peer.malformedAck = text
		peer.mu.Unlock()
	}
	var customer, customerLID types.JID
	var cut []servedNativeActivityCut
	if len(messageIDs) == 0 {
		if earlierActivities {
			cut = append(cut, servedNativeActivityCut{servedNativeActivityLedger(t, endpoint), "SERVED_AUTHORED_CUSTOMER"})
		}
		customer, customerLID = peer.customer(text, "SERVED_AUTHORED_CUSTOMER")
	} else {
		if len(messageIDs) != 1 || messageIDs[0] == "" {
			t.Fatal("one exact new proof input is required")
		}
		customer, customerLID = types.NewJID("15551234573", types.DefaultUserServer), types.NewJID("100000000005", types.HiddenUserServer)
		if customer == peer.account.ToNonAD() || customer == peer.from.ToNonAD() || customerLID == peer.lid.ToNonAD() || customerLID == peer.fromLID.ToNonAD() {
			t.Fatal("post-boot customer aliases a bound role")
		}
		sender, _, _ := peer.senderFrom(customer, customerLID)
		cut = append(cut, servedNativeActivityCut{servedNativeActivityLedger(t, endpoint), messageIDs[0]})
		peer.sendEncrypted(sender.EncryptText(t, text, messageIDs[0]), messageIDs[0])
	}
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
			requireServedNativeActivityResult(t, endpoint, sent.ID, customer.String(), text, uncertain, cut...)
			return sent
		case <-deadline:
			logServedNativeActivityFailure(t, endpoint)
			t.Fatal("enabled authored native activity did not produce a genuine encrypted customer reply")
		}
	}
}

func requireServedNativeActivityResult(t *testing.T, endpoint, messageID, destination, text string, uncertain bool, cut ...servedNativeActivityCut) {
	t.Helper()
	code := ""
	if uncertain {
		code = "native_activity_acknowledgment_invalid"
	}
	requireServedNativeActivityOutcome(t, endpoint, messageID, destination, text, code, cut...)
}

func requireServedNativeActivityOutcome(t *testing.T, endpoint, messageID, destination, text, failureCode string, cut ...servedNativeActivityCut) {
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
				if len(cut) != 0 && strings.ReplaceAll(request.EventID, "-", "") != messageID {
					continue
				}
				target, _ := request.Payload["native_session_target"].(string)
				input, _ := request.Payload["input"].(map[string]any)
				if !strings.HasPrefix(target, contracts.PrivateChannelActivityPrefix) || request.Payload["plan_generation"] == nil ||
					request.Payload["channel_activation_generation"] == nil || request.Payload["bundle_hash"] == "" ||
					request.Payload["workflow_version"] == "" || input["destination"] != destination || input["text"] != text ||
					strings.ReplaceAll(request.EventID, "-", "") != messageID || (len(cut) == 0 && len(requests.Events) != 1) {
					t.Fatal("served native request lost its immutable source/target/input", messageID, len(requests.Events), request)
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
					if len(cut) != 0 && result.SourceEventID != request.Payload["source_event_id"] {
						continue
					}
					if result.SourceEventID != request.Payload["source_event_id"] || (len(cut) == 0 && len(results.Events) != 1) || result.Payload["tool"] != "whatsapp.send_text" {
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
					if len(cut) != 0 {
						requireServedNativeActivityDelta(t, endpoint, cut[0], request, result, messageID)
					}
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	logServedNativeActivityFailure(t, endpoint)
	t.Fatal("genuine native reply did not settle its original activity request/result", messageID, destination, cut)
}

type servedNativeActivityCut struct {
	before  map[string]operatorread.OperatorEventFull
	inputID string
}

func servedNativeActivityLedger(t *testing.T, endpoint string) map[string]operatorread.OperatorEventFull {
	t.Helper()
	var runs struct {
		Runs       []operatorread.RunHeader `json:"runs"`
		NextCursor string                   `json:"next_cursor"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"limit": 10}, &runs)
	if runs.NextCursor != "" {
		t.Fatal("activity ledger run list truncated", runs)
	}
	ledger := make(map[string]operatorread.OperatorEventFull)
	for _, run := range runs.Runs {
		for _, name := range []string{"inbound.whatsapp.message", "platform.activity_requested", "customer_reply.succeeded", "customer_reply.failed"} {
			var events operatorread.OperatorEventListResult
			requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": run.RunID, "event_name": name}, "limit": 50}, &events)
			if events.NextCursor != "" {
				t.Fatal("activity ledger event list truncated", run.RunID, name, events)
			}
			for _, event := range events.Events {
				if _, duplicate := ledger[event.EventID]; event.EventID == "" || duplicate || event.RunID != run.RunID || event.EventName != name {
					t.Fatal("activity ledger identity mismatch/duplicate", run.RunID, name, event, ledger)
				}
				event.Deliveries, event.NoDelivery, event.DeadLetters = nil, nil, nil
				ledger[event.EventID] = event
			}
		}
	}
	return ledger
}

func requireServedNativeActivityDelta(t *testing.T, endpoint string, cut servedNativeActivityCut, request, result operatorread.OperatorEventFull, messageID string) {
	t.Helper()
	after := servedNativeActivityLedger(t, endpoint)
	delta := make(map[string]operatorread.OperatorEventFull)
	for id, before := range cut.before {
		if current, found := after[id]; !found || !reflect.DeepEqual(before, current) {
			t.Fatal("activity ledger lost/changed baseline", id, before, current, cut.before, after)
		}
	}
	for id, event := range after {
		if _, existed := cut.before[id]; !existed {
			delta[id] = event
		}
	}
	sourceID, _ := request.Payload["source_event_id"].(string)
	source := delta[sourceID]
	if len(delta) != 3 || delta[request.EventID].EventID == "" || delta[result.EventID].EventID == "" ||
		source.EventName != "inbound.whatsapp.message" || source.Payload["provider_message_reference"] != cut.inputID ||
		source.RunID != request.RunID || result.RunID != request.RunID || result.SourceEventID != source.EventID ||
		strings.ReplaceAll(request.EventID, "-", "") != messageID {
		t.Fatal("activity ledger did not add exactly its input/request/result", messageID, cut.inputID, request, result, cut.before, after, delta)
	}
	t.Logf("closed recovery activity ledger input=%s source=%s request=%s result=%s outbound=%s baseline=%d delta=%d", cut.inputID, source.EventID, request.EventID, result.EventID, messageID, len(cut.before), len(delta))
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
