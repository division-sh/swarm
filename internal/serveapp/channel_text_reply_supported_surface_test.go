package serveapp

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packmodel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
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
					canonicalrouting.CopyChannelLearnedObjectControlPagesJourney(t), textReplyChannelPackBodies(t))
				h.opts.AbandonActiveRuns = false
				h.start(t)
				t.Cleanup(func() { h.stop(t) })
				driver := "sqlite"
				if backend == servedparity.BackendExplicitPostgres {
					driver = "postgres"
				}
				reader, err := storetest.OpenChannelObservation(driver, h.storeDSN)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := reader.Close(); err != nil {
						t.Error(err)
					}
				})
				var begun channelonboarding.Result
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
					"provider": "mock", "verb": "connect", "provider_credential": "object-provider-secret",
					"save_proof": true,
				}, &begun)
				if begun.IdentityOperation == nil {
					t.Fatal("text/reply connect omitted the real human claim")
				}
				settled := func(kind render.IntentKind, eventID, disposition string) {
					t.Helper()
					waitTextReplyIntent(t, reader, render.IntentObservationQuery{Kind: kind, Provider: "mock",
						ProviderEventID: eventID, InterfaceKey: begun.IdentityOperation.Interface.Key()}, disposition)
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
				settled(render.IntentText, "customer-inbox", "entry_rejected")
				postText("operator-inbox", "operator-a", "/inbox", "", true)
				settled(render.IntentText, "operator-inbox", "entry")
				waitTextReplyMessage(t, provider, "Inbox")
				var identity apiv1.RuntimeIdentityResult
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
				seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": identity.SourceArtifacts[0].BundleHash,
					"payload": map[string]any{"detail": "text/reply baseline"}, "idempotency_key": "baseline-card",
				})
				card := waitTextReplyPublicCard(t, h, seed.RunID)
				plans := waitTextReplyCardCopies(t, reader, card, 1)
				receipt, found, err := reader.GetCurrentChannelSentReceipt(context.Background(), plans[0].DeliveryID, plans[0].CurrentReceiptID)
				if err != nil || !found {
					t.Fatalf("first card has no exact receipt: %v", err)
				}
				message := receipt.DeliveryReference.(map[string]any)["id"].(string)
				firstCopy := message
				waitTextReplyMessage(t, provider, "Action: More choices")
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				h.stop(t)
				h.start(t)
				reader, err = storetest.OpenChannelObservation(driver, h.storeDSN)
				if err != nil {
					t.Fatal(err)
				}
				postText("control-page", "operator-a", "More choices", message, false)
				postText("control-page", "operator-a", "More choices", message, false)
				settled(render.IntentAction, "control-page", "navigation")
				message = waitTextReplyMessage(t, provider, "Action: reject")
				if message == firstCopy {
					t.Fatal("no-edit navigation modified or borrowed the original physical copy")
				}
				waitTextReplyCardCopies(t, reader, card, 2)
				postText("customer-action", "customer-b", "reject", message, false)
				settled(render.IntentText, "customer-action", "rejected")
				postText("no-quote", "operator-a", "reject", "", false)
				postText("old-page-request", "operator-a", "More choices", firstCopy, false)
				settled(render.IntentAction, "old-page-request", "stale")
				postText("input-begin", "operator-a", "reject", message, false)
				postText("input-begin", "operator-a", "reject", message, false)
				settled(render.IntentAction, "input-begin", "input_started")
				prompt := waitTextReplyMessage(t, provider, "Input: reason (text)")
				postText("input-skip", "operator-a", "Skip field", prompt, false)
				settled(render.IntentAction, "input-skip", "applied")
				finalPrompt := waitTextReplyMessage(t, provider, "Input: later (text) required")
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				h.stop(t)
				h.start(t)
				reader, err = storetest.OpenChannelObservation(driver, h.storeDSN)
				if err != nil {
					t.Fatal(err)
				}
				postText("obsolete-prompt", "operator-a", "old answer", prompt, false)
				settled(render.IntentText, "obsolete-prompt", "teaching")
				postText("input-final", "operator-a", "private-answer", finalPrompt, false)
				waitTextReplyPublicDecision(t, h, card)
				postText("old-action", "operator-a", "reject", message, false)
				settled(render.IntentAction, "old-action", "stale")
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

func waitTextReplyIntent(t *testing.T, reader render.Observer, demand render.IntentObservationQuery, disposition string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last render.IntentObservation
	var seen bool
	for time.Now().Before(deadline) {
		result, found, err := reader.ObserveChannelIntent(context.Background(), demand)
		if err != nil {
			t.Fatal(err)
		}
		last, seen = result, found
		if found && result.State == "settled" && result.Disposition == disposition {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if demand.Kind == render.IntentAction {
		textDemand := demand
		textDemand.Kind = render.IntentText
		text, found, err := reader.ObserveChannelIntent(context.Background(), textDemand)
		t.Logf("original text: found=%t state=%q disposition=%q err=%v", found, text.State, text.Disposition, err)
	}
	t.Fatalf("intent %s/%s did not settle as %s: found=%t state=%q disposition=%q", demand.Kind, demand.ProviderEventID, disposition, seen, last.State, last.Disposition)
}

func textReplyCardCopies(t *testing.T, reader render.Observer, cardID string) []render.Candidate {
	t.Helper()
	var result []render.Candidate
	cursor := ""
	for {
		page, err := reader.ListCurrentChannelDeliveryPlans(context.Background(), cursor, 200)
		if err != nil {
			t.Fatal(err)
		}
		for _, plan := range page {
			if plan.SourceKind == "card" && plan.SourceID == cardID {
				result = append(result, plan)
			}
		}
		if len(page) < 200 {
			return result
		}
		next := page[len(page)-1].DeliveryID
		if next <= cursor {
			t.Fatal("channel observation did not advance")
		}
		cursor = next
	}
}

func waitTextReplyCardCopies(t *testing.T, reader render.Observer, cardID string, count int) []render.Candidate {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		plans := textReplyCardCopies(t, reader, cardID)
		if len(plans) > count {
			t.Fatal("duplicate request created an extra card copy")
		}
		sent := 0
		for _, plan := range plans {
			if plan.State == "sent" && plan.CurrentReceiptID != "" {
				sent++
			}
		}
		if sent == count {
			return plans
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("card %s did not retain %d accepted copies", cardID, count)
	return nil
}

func waitTextReplyPublicCard(t *testing.T, h *channelOnboardingE2EHarness, runID string) string {
	t.Helper()
	return waitChannelPublicCard(t, h, runID, decisioncard.AnchorKindStageGate, "reviews")
}

func waitChannelPublicCard(t *testing.T, h *channelOnboardingE2EHarness, runID string, kind decisioncard.AnchorKind, flowInstance string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var matches []string
		cursor := ""
		for {
			var page struct {
				Items []struct {
					Kind string                `json:"kind"`
					Card decisioncard.ListItem `json:"decision_card"`
				} `json:"items"`
				Next string `json:"next_cursor"`
			}
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.list", map[string]any{
				"run_id": runID, "anchor_kind": string(kind), "status": "pending", "cursor": cursor, "limit": 200}, &page)
			for _, item := range page.Items {
				if item.Kind == "decision_card" && item.Card.RunID == runID && item.Card.Scope.FlowInstance == flowInstance {
					matches = append(matches, item.Card.CardID)
				}
			}
			if page.Next == "" {
				break
			}
			if page.Next == cursor {
				t.Fatal("public mailbox cursor did not advance")
			}
			cursor = page.Next
		}
		if len(matches) > 1 {
			t.Fatal("public baseline has ambiguous pending cards")
		}
		if len(matches) == 1 {
			return matches[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("public baseline card did not appear")
	return ""
}

func waitTextReplyPublicDecision(t *testing.T, h *channelOnboardingE2EHarness, cardID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var result struct {
			Card struct {
				CardID string `json:"card_id"`
				Status string `json:"status"`
			} `json:"decision_card"`
		}
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": cardID}, &result)
		if result.Card.CardID != cardID {
			t.Fatal("public card readback changed identity")
		}
		if result.Card.Status == decisioncard.StatusDecided {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("public baseline card did not complete")
}

func waitTextReplyMessage(t *testing.T, provider *objectChannelProvider, contains string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		provider.mu.Lock()
		for index, delivery := range provider.deliveries {
			body := fmt.Sprint(delivery["body"])
			if strings.HasPrefix(contains, "Action: ") {
				if reference := strings.LastIndex(body, "\nReference: "); reference >= 0 {
					body = body[reference:]
				} else {
					continue
				}
			}
			if strings.Contains(body, contains) {
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
