package serveapp

import (
	"database/sql"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"gopkg.in/yaml.v3"
)

func textReplyChannelPackBodies(t *testing.T) map[string][]byte {
	t.Helper()
	bodies := objectChannelPackBodies(t)
	var channel, connector, trigger map[string]any
	for kind, target := range map[string]*map[string]any{
		packmodel.TypeChannel: &channel, packmodel.TypeConnector: &connector, packmodel.TypeTrigger: &trigger,
	} {
		if err := yaml.Unmarshal(bodies[kind], target); err != nil {
			t.Fatal(err)
		}
	}
	capabilities := channel["capabilities"].(map[string]any)
	for _, optional := range []string{"actions_as_buttons", "edit", "acknowledgment"} {
		capabilities[optional] = false
	}
	delete(channel, "native_inbox")
	operations, tools := channel["operations"].(map[string]any), connector["tools"].(map[string]any)
	for name, operation := range operations {
		if name != "deliver" {
			delete(tools, operation.(map[string]any)["tool"].(string))
			delete(operations, name)
		}
	}
	delete(channel["events"].(map[string]any), "action")
	for _, slot := range []string{"interaction_reference", "delivery_receipt"} {
		delete(channel["opaque_types"].(map[string]any), slot)
	}
	delete(operations["deliver"].(map[string]any)["input"].(map[string]any), "controls")
	deliver := tools["mock.deliver"].(map[string]any)
	input := deliver["input_schema"].(map[string]any)
	delete(input["properties"].(map[string]any), "controls")
	input["required"] = []any{"queue", "body"}
	delete(deliver["http"].(map[string]any)["body"].(map[string]any), "controls")
	trigger["normalized_events"] = trigger["normalized_events"].([]any)[:1]
	for kind, value := range map[string]any{
		packmodel.TypeChannel: channel, packmodel.TypeConnector: connector, packmodel.TypeTrigger: trigger,
	} {
		raw, err := yaml.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		bodies[kind] = raw
	}
	return bodies
}

func TestChannelTextReplyBaselinePublicJourneyBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, scope := range []string{"direct", "shared"} {
			t.Run(string(backend)+"/"+scope, func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				provider := &objectChannelProvider{textOnly: true, commands: map[string][]any{}, calls: map[string][]map[string]any{}}
				server := httptest.NewServer(provider)
				t.Cleanup(server.Close)
				redirectExternalHosts(t, map[string]string{"mock.example.test": server.URL})
				h.opts.SourceRoot = writeObjectChannelPacksWithBodies(t, h.opts.ConfigPath,
					canonicalrouting.CopyChannelLearnedObjectOptionalInputJourney(t, true), textReplyChannelPackBodies(t))
				h.opts.AbandonActiveRuns = false
				h.start(t)
				t.Cleanup(func() { h.stop(t) })
				driver := "sqlite"
				if backend == servedparity.BackendExplicitPostgres {
					driver = "postgres"
				}
				db, err := sql.Open(driver, h.storeDSN)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				var begun channelonboarding.Result
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
					"provider": "mock", "verb": "connect", "provider_credential": "object-provider-secret",
					"save_proof": true,
				}, &begun)
				if begun.IdentityOperation == nil {
					t.Fatal("text/reply connect omitted the real human claim")
				}
				provider.mu.Lock()
				callback, signing := provider.callback, provider.signing
				provider.mu.Unlock()
				if callback == "" || signing == "" {
					t.Fatal("text/reply connect did not register authenticated ingress")
				}
				objectChannelIngress(t, callback, signing, "baseline-claim", map[string]any{
					"text": begun.IdentityOperation.Challenge, "principal": "operator-a", "room": "queue-a",
					"scope": scope, "message": map[string]any{"id": "claim"},
				})
				claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
				if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
					t.Fatalf("claim was not admitted: %+v", claimed)
				}
				var confirmed map[string]any
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
					"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
				}, &confirmed)
				ready := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
				if ready.Readiness == nil || !ready.Readiness.Ready || ready.Readiness.NativeInboxRequired || ready.Readiness.NativeInbox != nil {
					t.Fatalf("baseline readiness fabricated a native menu obligation: %+v", ready)
				}
				postText := func(id, account, text, quote string, inbox bool) {
					t.Helper()
					provider.mu.Lock()
					callback, signing = provider.callback, provider.signing
					provider.mu.Unlock()
					fact := map[string]any{"text": text, "principal": account, "room": "queue-a", "scope": scope,
						"message": map[string]any{"id": id}}
					if quote != "" {
						fact["reply"] = map[string]any{"id": quote}
					}
					if inbox {
						fact["entry"] = map[string]any{"reference": "inbox"}
					}
					objectChannelIngress(t, callback, signing, id, fact)
				}
				postText("customer-inbox", "customer-b", "/inbox", "", true)
				waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "customer-inbox", "entry_rejected")
				postText("operator-inbox", "operator-a", "/inbox", "", true)
				waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "operator-inbox", "entry")
				waitTextReplyMessage(t, provider, "Inbox")
				var identity apiv1.RuntimeIdentityResult
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": identity.SourceArtifacts[0].BundleHash,
					"payload": map[string]any{"detail": "text/reply baseline"}, "idempotency_key": "baseline-card",
				})
				card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
				message := waitObjectCardReceipt(t, db, card)
				waitTextReplyMessage(t, provider, "Action: reject")
				postText("customer-action", "customer-b", "reject", message, false)
				waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "customer-action", "rejected")
				postText("no-quote", "operator-a", "reject", "", false)
				postText("input-begin", "operator-a", "reject", message, false)
				postText("input-begin", "operator-a", "reject", message, false)
				waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "input-begin", "input_started")
				prompt := waitTextReplyMessage(t, provider, "Input: reason (text)")
				postText("input-skip", "operator-a", "Skip field", prompt, false)
				waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "input-skip", "applied")
				finalPrompt := waitTextReplyMessage(t, provider, "Input: later (text) required")
				h.stop(t)
				h.start(t)
				postText("obsolete-prompt", "operator-a", "old answer", prompt, false)
				waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "obsolete-prompt", "teaching")
				postText("input-final", "operator-a", "private-answer", finalPrompt, false)
				waitUncertainCopyDecision(t, db, card)
				postText("old-action", "operator-a", "reject", message, false)
				waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "old-action", "stale")
				provider.mu.Lock()
				defer provider.mu.Unlock()
				for path, calls := range provider.calls {
					if path != "/v2/deliver" && path != "/v2/register" && path != "/v2/registration" && path != "/v2/identify" && len(calls) != 0 {
						t.Fatalf("unsupported provider operation launched: %s", path)
					}
				}
				for _, delivery := range provider.deliveries {
					if !objectChannelPresentationValid(delivery, true) || strings.Contains(fmt.Sprint(delivery["body"]), "private-answer") {
						t.Fatalf("text/reply output violates bounded private presentation: %#v", delivery)
					}
				}
			})
		}
	}
}

func waitTextReplyMessage(t *testing.T, provider *objectChannelProvider, contains string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		provider.mu.Lock()
		for index, delivery := range provider.deliveries {
			if strings.Contains(fmt.Sprint(delivery["body"]), contains) {
				provider.mu.Unlock()
				return fmt.Sprintf("delivery-%d", index+1)
			}
		}
		provider.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no bounded text/reply message containing %q", contains)
	return ""
}
