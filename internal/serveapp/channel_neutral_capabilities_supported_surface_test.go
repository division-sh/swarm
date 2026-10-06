package serveapp

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/packmodel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"gopkg.in/yaml.v3"
)

func optionalChannelPackBodies(t *testing.T, buttons, edit, ack bool) map[string][]byte {
	t.Helper()
	bodies := objectChannelPackBodies(t)
	var channel, connector, trigger map[string]any
	for kind, target := range map[string]*map[string]any{packmodel.TypeChannel: &channel, packmodel.TypeConnector: &connector, packmodel.TypeTrigger: &trigger} {
		if err := yaml.Unmarshal(bodies[kind], target); err != nil {
			t.Fatal(err)
		}
	}
	capabilities := channel["capabilities"].(map[string]any)
	capabilities["actions_as_buttons"], capabilities["edit"], capabilities["acknowledgment"] = buttons, edit, ack
	delete(channel, "native_inbox")
	operations, tools := channel["operations"].(map[string]any), connector["tools"].(map[string]any)
	for name, operation := range operations {
		if name != "deliver" && !(name == "edit" && edit) && !(name == "acknowledge_interaction" && ack) {
			delete(tools, operation.(map[string]any)["tool"].(string))
			delete(operations, name)
		}
	}
	if !edit {
		delete(channel["opaque_types"].(map[string]any), "delivery_receipt")
	}
	if !buttons && !ack {
		delete(channel["events"].(map[string]any), "action")
		delete(channel["opaque_types"].(map[string]any), "interaction_reference")
		trigger["normalized_events"] = trigger["normalized_events"].([]any)[:1]
	}
	if !buttons {
		for _, name := range []string{"deliver", "edit"} {
			operation, exists := operations[name].(map[string]any)
			if !exists {
				continue
			}
			delete(operation["input"].(map[string]any), "controls")
			tool := tools[operation["tool"].(string)].(map[string]any)
			input := tool["input_schema"].(map[string]any)
			delete(input["properties"].(map[string]any), "controls")
			input["required"] = []any{"queue", "body"}
			if name == "edit" {
				input["required"] = []any{"queue", "body", "reference"}
			}
			delete(tool["http"].(map[string]any)["body"].(map[string]any), "controls")
		}
	}
	for kind, value := range map[string]any{packmodel.TypeChannel: channel, packmodel.TypeConnector: connector, packmodel.TypeTrigger: trigger} {
		raw, err := yaml.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		bodies[kind] = raw
	}
	return bodies
}

type neutralChannelJourney struct {
	h                   *channelOnboardingE2EHarness
	provider            *objectChannelProvider
	reader              storetest.ChannelObservation
	workerLog           *lockedBuffer
	interfaceKey, scope string
}

func startNeutralChannelJourney(t *testing.T, backend servedparity.Backend, scope, source string, buttons, edit, ack bool) *neutralChannelJourney {
	t.Helper()
	workerLog := &lockedBuffer{}
	previousLog := log.Writer()
	log.SetOutput(io.MultiWriter(previousLog, workerLog))
	t.Cleanup(func() { log.SetOutput(previousLog) })
	h := newChannelOnboardingE2EHarness(t, backend, true)
	p := &objectChannelProvider{textOnly: !buttons, commands: map[string][]any{}, calls: map[string][]map[string]any{},
		supportedPaths: map[string]bool{"/v2/deliver": true, "/v2/identify": true, "/v2/register": true, "/v2/registration": true, "/v2/edit": edit, "/v2/ack": ack}}
	server := httptest.NewServer(p)
	t.Cleanup(server.Close)
	redirectExternalHosts(t, map[string]string{"mock.example.test": server.URL})
	h.opts.SourceRoot = writeObjectChannelPacksWithBodies(t, h.opts.ConfigPath, source, optionalChannelPackBodies(t, buttons, edit, ack))
	h.opts.AbandonActiveRuns = false
	h.opts.TestLLMRuntime = channelAnchorLLMRuntime{}
	h.start(t)
	t.Cleanup(func() { h.stop(t) })
	j := &neutralChannelJourney{h: h, provider: p, scope: scope, workerLog: workerLog}
	j.openReader(t)
	t.Cleanup(func() {
		if err := j.reader.Close(); err != nil {
			t.Error(err)
		}
	})
	var begun channelonboarding.Result
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
		"provider": "mock", "verb": "connect", "provider_credential": "object-provider-secret", "save_proof": true,
	}, &begun)
	if begun.IdentityOperation == nil {
		t.Fatal("connect omitted the human claim")
	}
	j.interfaceKey = begun.IdentityOperation.Interface.Key()
	j.postText(t, "claim", "operator-a", begun.IdentityOperation.Challenge, "")
	claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
	if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
		t.Fatalf("real human claim was not admitted: %+v", claimed)
	}
	var confirmed map[string]any
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
		"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
	}, &confirmed)
	ready := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
	if ready.Readiness == nil || !ready.Readiness.Ready || ready.Readiness.NativeInboxRequired {
		t.Fatalf("neutral connect cannot reach readiness: %+v", ready)
	}
	return j
}

func (j *neutralChannelJourney) openReader(t *testing.T) {
	t.Helper()
	driver := "sqlite"
	if j.h.backend == servedparity.BackendExplicitPostgres {
		driver = "postgres"
	}
	var err error
	j.reader, err = storetest.OpenChannelObservation(driver, j.h.storeDSN)
	if err != nil {
		t.Fatal(err)
	}
}

func (j *neutralChannelJourney) postText(t *testing.T, id, account, text, quote string) {
	t.Helper()
	j.provider.mu.Lock()
	callback, signing := j.provider.callback, j.provider.signing
	j.provider.mu.Unlock()
	fact := map[string]any{"text": text, "principal": account, "room": "queue-a", "scope": j.scope, "message": map[string]any{"id": id}}
	if quote != "" {
		fact["reply"] = map[string]any{"id": quote}
	}
	objectChannelIngress(t, callback, signing, id, fact)
}

func (j *neutralChannelJourney) waitIntent(t *testing.T, kind render.IntentKind, id, disposition string) {
	t.Helper()
	waitTextReplyIntent(t, j.reader, render.IntentObservationQuery{Kind: kind, Provider: "mock", ProviderEventID: id,
		InterfaceKey: j.interfaceKey}, disposition)
}

func (j *neutralChannelJourney) assertNoUnsupportedEffects(t *testing.T) {
	t.Helper()
	j.provider.mu.Lock()
	defer j.provider.mu.Unlock()
	for path, calls := range j.provider.calls {
		if len(calls) != 0 && !j.provider.supportedPaths[path] {
			t.Fatalf("unsupported effect launched: %s", path)
		}
	}
	if strings.Contains(j.workerLog.String(), "channel action reconciliation:") {
		t.Fatalf("valid action produced a reconciliation error: %s", j.workerLog.String())
	}
}

func TestChannelOptionalCapabilitiesPublicJourneyBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for mask := 0; mask < 8; mask++ {
			buttons, edit, ack := mask&1 != 0, mask&2 != 0, mask&4 != 0
			t.Run(fmt.Sprintf("%s/buttons_%t/edit_%t/ack_%t", backend, buttons, edit, ack), func(t *testing.T) {
				j := startNeutralChannelJourney(t, backend, "direct", canonicalrouting.CopyChannelLearnedObjectControlPagesJourney(t), buttons, edit, ack)
				var identity apiv1.RuntimeIdentityResult
				requireServedJSONRPCResult(t, j.h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
				seed := requireServedEventPublishRPCResult(t, j.h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": identity.SourceArtifacts[0].BundleHash,
					"payload": map[string]any{"detail": "optional capability matrix"}, "idempotency_key": "card",
				})
				card := waitTextReplyPublicCard(t, j.h, seed.RunID)
				plans := waitTextReplyCardCopies(t, j.reader, card, 1)
				receipt, found, err := j.reader.GetCurrentChannelSentReceipt(context.Background(), plans[0].DeliveryID, plans[0].CurrentReceiptID)
				if err != nil || !found {
					t.Fatalf("card has no exact receipt: %v", err)
				}
				message := receipt.DeliveryReference.(map[string]any)["id"].(string)
				if buttons {
					token := waitObjectMessageControl(t, j.provider, message, "More choices")
					j.provider.mu.Lock()
					callback, signing := j.provider.callback, j.provider.signing
					j.provider.mu.Unlock()
					fact := map[string]any{"token": token, "cursor": "callback", "principal": "operator-a", "room": "queue-a", "scope": "direct", "message": map[string]any{"id": message}}
					objectChannelIngress(t, callback, signing, "callback", fact)
					objectChannelIngress(t, callback, signing, "callback", fact)
					j.waitIntent(t, render.IntentAction, "callback", "navigation")
					if ack {
						waitObjectChannelEffect(t, j.provider, "/v2/ack", func(input map[string]any) bool { return input["cursor"] == "callback" })
					}
				} else {
					j.postText(t, "reply", "operator-a", "More choices", message)
					j.postText(t, "reply", "operator-a", "More choices", message)
					j.waitIntent(t, render.IntentAction, "reply", "navigation")
				}
				if edit {
					waitObjectChannelEffect(t, j.provider, "/v2/edit", func(input map[string]any) bool { return input["reference"].(map[string]any)["id"] == message })
				} else {
					waitTextReplyCardCopies(t, j.reader, card, 2)
				}
				j.assertNoUnsupportedEffects(t)
				j.provider.mu.Lock()
				ackCalls := len(j.provider.calls["/v2/ack"])
				j.provider.mu.Unlock()
				want := 0
				if buttons && ack {
					want = 1
				}
				if ackCalls != want {
					t.Fatalf("native ACK count=%d, want=%d; buttons do not imply ACK and replies never ACK", ackCalls, want)
				}
			})
		}
	}
}

func TestChannelTextReplyNoticeAcknowledgmentPublicJourneyBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, scope := range []string{"direct", "shared"} {
			t.Run(string(backend)+"/"+scope, func(t *testing.T) {
				j := startNeutralChannelJourney(t, backend, scope, canonicalrouting.CopyChannelNoticeJourney(t), false, false, false)
				var identity apiv1.RuntimeIdentityResult
				requireServedJSONRPCResult(t, j.h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
				requireServedEventPublishRPCResult(t, j.h.rpcEndpoint(), map[string]any{
					"event_name": "notice.requested", "bundle_hash": identity.SourceArtifacts[0].BundleHash,
					"payload": map[string]any{"seed": true}, "idempotency_key": "notice",
				})
				message := waitTextReplyMessage(t, j.provider, "Notice: Observed notice")
				assertNeutralNoticeReadback(t, j, 1)
				for _, negative := range []struct{ id, account, quote, disposition string }{
					{"foreign-principal", "customer-b", message, "rejected"}, {"foreign-receipt", "operator-a", "foreign-message", "teaching"},
				} {
					j.postText(t, negative.id, negative.account, "Acknowledge", negative.quote)
					j.waitIntent(t, render.IntentText, negative.id, negative.disposition)
					assertNeutralNoticeReadback(t, j, 1)
				}
				j.postText(t, "notice-ack", "operator-a", "Acknowledge", message)
				j.postText(t, "notice-ack", "operator-a", "Acknowledge", message)
				j.waitIntent(t, render.IntentAction, "notice-ack", "applied")
				assertNeutralNoticeReadback(t, j, 0)
				if err := j.reader.Close(); err != nil {
					t.Fatal(err)
				}
				j.h.stop(t)
				j.h.start(t)
				j.openReader(t)
				j.postText(t, "notice-ack", "operator-a", "Acknowledge", message)
				j.waitIntent(t, render.IntentAction, "notice-ack", "applied")
				assertNeutralNoticeReadback(t, j, 0)
				j.assertNoUnsupportedEffects(t)
			})
		}
	}
}

func TestChannelCollidingCaptionReceiptExecutionPublicJourneyBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, buttons := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/buttons_%t", backend, buttons), func(t *testing.T) {
				j := startNeutralChannelJourney(t, backend, "direct", canonicalrouting.CopyChannelCaptionJourney(t), buttons, true, buttons)
				var identity apiv1.RuntimeIdentityResult
				requireServedJSONRPCResult(t, j.h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
				seed := requireServedEventPublishRPCResult(t, j.h.rpcEndpoint(), map[string]any{
					"event_name": "work.requested", "bundle_hash": identity.SourceArtifacts[0].BundleHash,
					"payload": map[string]any{"detail": "caption collision"}, "idempotency_key": "card",
				})
				card := waitTextReplyPublicCard(t, j.h, seed.RunID)
				plans := waitTextReplyCardCopies(t, j.reader, card, 1)
				receipt, found, err := j.reader.GetCurrentChannelSentReceipt(context.Background(), plans[0].DeliveryID, plans[0].CurrentReceiptID)
				if err != nil || !found {
					t.Fatalf("colliding-caption card has no exact receipt: %v", err)
				}
				message := receipt.DeliveryReference.(map[string]any)["id"].(string)
				j.provider.mu.Lock()
				var text string
				for index, delivery := range j.provider.deliveries {
					if fmt.Sprintf("delivery-%d", index+1) == message {
						text, _ = delivery["body"].(string)
					}
				}
				j.provider.mu.Unlock()
				source, err := channelPageSourceText(text, []string{"1 Accept", "2 accept"})
				if err != nil || !strings.Contains(source, "Action: Accept") || !strings.Contains(source, "Action: accept") {
					t.Fatalf("visible words lost canonical captions: %q: %v", text, err)
				}
				j.postText(t, "old-caption", "operator-a", "Accept", message)
				j.waitIntent(t, render.IntentText, "old-caption", "teaching")
				j.postText(t, "foreign", "customer-b", "1 Accept", message)
				j.waitIntent(t, render.IntentText, "foreign", "rejected")
				if buttons {
					token := waitObjectMessageControl(t, j.provider, message, "1 Accept")
					j.provider.mu.Lock()
					callback, signing := j.provider.callback, j.provider.signing
					j.provider.mu.Unlock()
					fact := map[string]any{"token": token, "cursor": "approve", "principal": "operator-a", "room": "queue-a", "scope": "direct", "message": map[string]any{"id": message}}
					objectChannelIngress(t, callback, signing, "approve", fact)
					objectChannelIngress(t, callback, signing, "approve", fact)
				} else {
					j.postText(t, "approve", "operator-a", " 1 ACCEPT ", message)
					j.postText(t, "approve", "operator-a", " 1 ACCEPT ", message)
				}
				j.waitIntent(t, render.IntentAction, "approve", "applied")
				waitTextReplyPublicDecision(t, j.h, card)
				var result struct {
					Card struct {
						Verdict string `json:"verdict"`
					} `json:"decision_card"`
				}
				requireServedJSONRPCResult(t, j.h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": card}, &result)
				if result.Card.Verdict != "approve" {
					t.Fatalf("visible selector selected a different semantic verdict: %+v", result)
				}
				j.assertNoUnsupportedEffects(t)
			})
		}
	}
}

func assertNeutralNoticeReadback(t *testing.T, j *neutralChannelJourney, unread int) {
	t.Helper()
	var listed struct {
		Unread int `json:"unread_informational_notices"`
		Items  []struct {
			Kind   string         `json:"kind"`
			Notice mailbox.V1Item `json:"notice"`
		} `json:"items"`
	}
	requireServedJSONRPCResult(t, j.h.rpcEndpoint(), "mailbox.list", map[string]any{"type": "operator_notice", "limit": 200}, &listed)
	var notices []mailbox.V1Item
	for _, item := range listed.Items {
		if item.Kind == "notice" {
			notices = append(notices, item.Notice)
		}
	}
	if listed.Unread != unread || len(notices) != 1 || notices[0].Status != "pending" {
		t.Fatalf("principal notice acknowledgment/readback diverged: %+v, want unread=%d", listed, unread)
	}
	var got struct {
		Kind   string               `json:"kind"`
		Notice mailbox.V1ItemDetail `json:"notice"`
	}
	requireServedJSONRPCResult(t, j.h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": notices[0].MailboxID}, &got)
	if got.Kind != "notice" || got.Notice.Item.Status != "pending" {
		t.Fatalf("notice detail disagrees with canonical list: %+v", got)
	}
	if len(got.Notice.History) != 1 || got.Notice.History[0].Action != "created" {
		t.Fatalf("notice acknowledgment fabricated decision history: %+v", got.Notice.History)
	}
}
