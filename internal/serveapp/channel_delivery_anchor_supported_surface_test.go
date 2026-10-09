package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type channelAnchorLLMRuntime struct{ telegramPhraseBotLLMRuntime }

func (r channelAnchorLLMRuntime) ContinueManagedSession(ctx context.Context, session *runtimellm.Session, call runtimellm.ManagedCall) (*runtimellm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("anchor proof lacks admitted managed capabilities")
	}
	observed, err := runtimellm.ObserveAPIRequestCapabilitySurface(surface, session.Tools)
	if err != nil {
		return nil, err
	}
	response := &runtimellm.Response{
		Message: runtimellm.Message{Role: "assistant", Content: "Observed."}, SessionID: session.ID, CapabilitySurface: &observed,
	}
	event := call.Frame().Turn.Event.Type
	if message.Role != "tool" && (strings.HasSuffix(event, "observer.requested") || strings.HasSuffix(event, "notice.requested")) {
		tool := runtimellm.ToolCall{ID: "human-" + session.ID, Name: "ask_human", Arguments: map[string]any{
			"scope": "flow", "category": "review", "description": "Review the observed work.",
		}}
		if strings.HasSuffix(event, "notice.requested") {
			tool.Name = "notify_human"
			tool.Arguments = map[string]any{"summary": "Observed notice", "context": map[string]any{"proof": "mailbox-completion"}}
		}
		var payload struct {
			DeadlineAt string `json:"deadline_at"`
		}
		if err := json.Unmarshal(call.Frame().Turn.Event.Payload, &payload); err != nil {
			return nil, err
		}
		if payload.DeadlineAt != "" {
			tool.Arguments.(map[string]any)["deadline_at"] = payload.DeadlineAt
		}
		response.ToolCalls = []runtimellm.ToolCall{tool}
		response.Message.ToolCalls = response.ToolCalls
		response.ToolOutputAuthority = &runtimellm.ToolOutputAuthority{
			ProviderOperationID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("channel-anchor:"+call.Frame().FrameID)).String(),
			SettledAt:           time.Now().UTC(),
		}
	}
	return response, nil
}

func TestChannelDeliveryRealAnchorProducersPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			observation := &channelReconcileObservation{}
			h, bundleHash := startChannelAnchorPublicJourney(t, backend, "anchor-token", false, 0, func(opts *cliapp.ServeOptions) {
				observation.configure(opts)
				opts.TestChannelReconcileCadence = render.ReconcileCadence{Ordinary: time.Hour, Native: time.Hour}
			})
			reader := openChannelAnchorObservation(t, h)
			var channels struct {
				Channels []channelonboarding.ConnectedChannelReadback `json:"channels"`
			}
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.list", map[string]any{}, &channels)
			if len(channels.Channels) != 1 {
				t.Fatalf("anchor journey requires one exact channel identity: %+v", channels)
			}
			interfaceKey := channels.Channels[0].Identity.Interface.Key()
			for _, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect} {
				t.Run(string(kind), func(t *testing.T) {
					flowInstance := "reviews"
					if kind == decisioncard.AnchorKindHumanTask {
						flowInstance = "observers"
					}
					seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
						"event_name": "work.requested", "bundle_hash": bundleHash,
						"payload": map[string]any{"seed": true}, "idempotency_key": "channel-anchor-" + string(kind),
					})
					gateID := waitChannelPublicCard(t, h, seed.RunID, decisioncard.AnchorKindStageGate, "reviews")
					waitChannelAnchorObservedReceipt(t, reader, gateID)
					if kind != decisioncard.AnchorKindStageGate {
						event := "observer.requested"
						if kind == decisioncard.AnchorKindProposedEffect {
							event = "effect.requested"
						}
						requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
							"event_name": event, "run_id": seed.RunID, "source_event_id": seed.EventID,
							"payload": map[string]any{"seed": true}, "idempotency_key": "producer-" + string(kind),
						})
					}
					cardID := waitChannelPublicCard(t, h, seed.RunID, kind, flowInstance)
					messageID := waitChannelAnchorObservedReceipt(t, reader, cardID)
					message := h.provider.Delivery(messageID - 1)
					label := "Approve"
					if kind == decisioncard.AnchorKindStageGate {
						label = "approve"
					}
					token, found := telegramCallbackToken(message, label)
					if !found {
						t.Fatalf("real %s card lacks approve action: %v", kind, message)
					}
					var arrived <-chan struct{}
					var release func()
					var dispatchCut render.ReconcileMark
					if kind == decisioncard.AnchorKindProposedEffect {
						arrived, release = h.provider.PauseNextDeliveryResponse()
						t.Cleanup(release)
					}
					post := func(update int, occurrence string) {
						t.Helper()
						callback, signing, _ := h.provider.Registration()
						if names := postChannelTelegramUpdate(t, callback, signing, map[string]any{
							"update_id": update,
							"callback_query": map[string]any{
								"id": occurrence, "from": map[string]any{"id": 7000},
								"message": map[string]any{"message_id": messageID, "chat": map[string]any{"id": 1001, "type": "private"}},
								"data":    token,
							},
						}); len(names) != 0 {
							t.Fatalf("operator anchor action leaked business events: %v", names)
						}
					}
					post(837000+messageID, "anchor-"+cardID)
					waitChannelAnchorPublicApproval(t, h, cardID)
					if arrived != nil {
						select {
						case <-arrived:
						case <-time.After(15 * time.Second):
							t.Fatal("approved effect did not reach the real provider request")
						}
						var readback struct {
							Card struct {
								Verdict string `json:"verdict"`
							} `json:"decision_card"`
							Effect decisioncard.ProposedEffectReadback `json:"effect"`
						}
						requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": cardID}, &readback)
						if readback.Card.Verdict != "approve" || readback.Effect.DispatchState == "" || readback.Effect.DispatchState == "succeeded" {
							t.Fatalf("approval inferred completed dispatch while provider response is blocked: %+v", readback)
						}
						// This action is running inside the worker's current pass;
						// post-commit demand must survive until that pass returns.
						observation.mu.Lock()
						mark := observation.mark
						observation.mu.Unlock()
						var active bool
						dispatchCut, active = mark()
						if !active {
							t.Fatal("activity cut lost its exact channel Process")
						}
						release()
						deadline := time.Now().Add(15 * time.Second)
						for {
							requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": cardID}, &readback)
							if readback.Effect.DispatchState == "succeeded" {
								break
							}
							if time.Now().After(deadline) {
								t.Fatalf("real approved effect did not settle: %+v", readback)
							}
							time.Sleep(20 * time.Millisecond)
						}
						count := 0
						for index := 0; ; index++ {
							delivery := h.provider.Delivery(index)
							if delivery == nil {
								break
							}
							if delivery["text"] == "review" && fmt.Sprint(delivery["chat_id"]) == "42" {
								count++
							}
						}
						if count != 1 {
							t.Fatalf("actual approved provider effect count=%d, want one", count)
						}
					}
					waitChannelAnchorTerminalEdit(t, h, messageID)
					if kind == decisioncard.AnchorKindProposedEffect {
						waitChannelAnchorDispatchEdit(t, h, messageID, "succeeded")
						observation.oneAfter(t, dispatchCut, render.ReconcileOrdinary)
					}
					post(838000+messageID, "double-tap-"+cardID)
					waitChannelAnchorRejectedIntent(t, reader, interfaceKey, fmt.Sprint(838000+messageID))
					var events struct {
						Events []struct {
							RunID     string `json:"run_id"`
							EventName string `json:"event_name"`
						} `json:"events"`
						NextCursor string `json:"next_cursor"`
					}
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.list", map[string]any{
						"filter": map[string]any{"run_id": seed.RunID, "event_name": "mailbox.card_decided"}, "limit": 2,
					}, &events)
					if len(events.Events) != 1 || events.NextCursor != "" || events.Events[0].RunID != seed.RunID || events.Events[0].EventName != "mailbox.card_decided" {
						t.Fatalf("double tap changed exact completion cardinality: %+v", events)
					}
				})
			}
		})
	}
}

func TestChannelDeliveryReconciliationActivityDispatchPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			observation := &channelReconcileObservation{}
			h, hash := startChannelAnchorPublicJourney(t, backend, "activity-wake-token", false, 0, func(opts *cliapp.ServeOptions) {
				observation.configure(opts)
				opts.TestChannelReconcileCadence = render.ReconcileCadence{Ordinary: time.Hour, Native: time.Hour}
			})
			reader := openChannelAnchorObservation(t, h)
			seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
				"event_name": "work.requested", "bundle_hash": hash,
				"payload": map[string]any{"seed": true}, "idempotency_key": "isolated-activity-wake",
			})
			requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
				"event_name": "effect.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"seed": true}, "idempotency_key": "isolated-proposal-wake",
			})
			cardID := waitChannelPublicCard(t, h, seed.RunID, decisioncard.AnchorKindProposedEffect, "reviews")
			messageID := waitChannelAnchorObservedReceipt(t, reader, cardID)
			var item struct {
				Card struct {
					CardContentHash string `json:"card_content_hash"`
				} `json:"decision_card"`
			}
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": cardID}, &item)
			arrived, release := h.provider.PauseDeliveryResponseMatching(func(message map[string]any) bool {
				return message["text"] == "review" && fmt.Sprint(message["chat_id"]) == "42"
			})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				response, status, err := mailboxTransportRequest(ctx, h.rpcEndpoint(), "http", apiv1.DefaultLoopbackAPIToken, "mailbox.decide", map[string]any{
					"card_id": cardID, "verdict": "approve", "observed_content_hash": item.Card.CardContentHash,
					"idempotency_key": "isolated-activity-decision",
				})
				if err == nil && (status != 200 || response.Error != nil) {
					err = fmt.Errorf("public decision status=%d error=%+v", status, response.Error)
				}
				done <- err
			}()
			defer cancel()
			defer func() {
				release()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			select {
			case <-arrived:
			case <-time.After(15 * time.Second):
				t.Fatal("approved activity did not reach the exact held provider call")
			}
			waitChannelAnchorDispatchEdit(t, h, messageID, "started")
			// Earlier decision/start hints have all been consumed. With no repair
			// tick, only the subsequent completion can release this card edit.
			_, cut := observation.completedCurrentOrdinary(t)
			release()
			waitChannelAnchorDispatchEdit(t, h, messageID, "succeeded")
			observation.oneAfter(t, cut, render.ReconcileOrdinary)
		})
	}
}

func waitChannelAnchorDispatchEdit(t *testing.T, h *channelOnboardingE2EHarness, messageID int, dispatch string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		for _, edit := range h.provider.Edits() {
			if fmt.Sprint(edit["message_id"]) == fmt.Sprint(messageID) && strings.Contains(fmt.Sprint(edit["text"]), "Dispatch: "+dispatch) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual receipt did not reflect dispatch %s: %v", dispatch, h.provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func startChannelAnchorJourney(t *testing.T, backend servedparity.Backend, token string, withSummary bool) (*channelOnboardingE2EHarness, *sql.DB, string) {
	t.Helper()
	return startChannelAnchorJourneyWithDraftTTL(t, backend, token, withSummary, 0)
}

func startChannelAnchorJourneyWithDraftTTL(t *testing.T, backend servedparity.Backend, token string, withSummary bool, draftTTL time.Duration) (*channelOnboardingE2EHarness, *sql.DB, string) {
	t.Helper()
	h, hash := startChannelAnchorPublicJourney(t, backend, token, withSummary, draftTTL, nil)
	driver := "sqlite"
	if backend == servedparity.BackendExplicitPostgres {
		driver = "postgres"
	}
	db, err := sql.Open(driver, h.storeDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return h, db, hash
}

func startChannelAnchorPublicJourney(t *testing.T, backend servedparity.Backend, token string, withSummary bool, draftTTL time.Duration, configure func(*cliapp.ServeOptions)) (*channelOnboardingE2EHarness, string) {
	t.Helper()
	h := newChannelOnboardingE2EHarness(t, backend, true)
	h.opts.AbandonActiveRuns = false
	writeChannelAnchorJourneySource(t, h.opts.SourceRoot, withSummary)
	h.opts.TestLLMRuntime = channelAnchorLLMRuntime{}
	if configure != nil {
		configure(&h.opts)
	}
	if draftTTL > 0 {
		body, err := os.ReadFile(h.opts.ConfigPath)
		if err != nil {
			t.Fatal(err)
		}
		var config map[string]any
		if err := yaml.Unmarshal(body, &config); err != nil {
			t.Fatal(err)
		}
		runtimeConfig, ok := config["runtime"].(map[string]any)
		if !ok {
			t.Fatal("channel journey config lacks its runtime section")
		}
		runtimeConfig["decision_card_input_draft_ttl"] = draftTTL.String()
		body, err = yaml.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(h.opts.ConfigPath, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := credentials.Set(context.Background(), "telegram_bot_token", token); err != nil {
		t.Fatal(err)
	}
	h.start(t)
	t.Cleanup(func() { h.stop(t) })
	var identity apiv1.RuntimeIdentityResult
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
	if len(identity.SourceArtifacts) != 1 {
		t.Fatalf("anchor journey requires one admitted source: %+v", identity)
	}
	if withSummary {
		requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
			"event_name": "notice.requested", "bundle_hash": identity.SourceArtifacts[0].BundleHash,
			"payload": map[string]any{"seed": true}, "idempotency_key": "uncertainty-summary-notice",
		})
		onlyPublicChannelNoticeID(t, h.rpcEndpoint(), "")
	}
	runChannelOnboardingCLIJourney(t, h.opts.ConfigPath, h.endpoint, h.provider, "connect", token, 1001, "private", 0)
	return h, identity.SourceArtifacts[0].BundleHash
}

func openChannelAnchorObservation(t *testing.T, h *channelOnboardingE2EHarness) storetest.ChannelObservation {
	t.Helper()
	driver := "sqlite"
	if h.backend == servedparity.BackendExplicitPostgres {
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
	return reader
}

func waitChannelAnchorObservedReceipt(t *testing.T, reader render.Observer, cardID string) int {
	t.Helper()
	plans := waitTextReplyCardCopies(t, reader, cardID, 1)
	receipt, found, err := reader.GetCurrentChannelSentReceipt(context.Background(), plans[0].DeliveryID, plans[0].CurrentReceiptID)
	if err != nil || !found || receipt.DeliveryID != plans[0].DeliveryID || receipt.OperationID != plans[0].CurrentReceiptID || receipt.RenderID != plans[0].CurrentRenderID {
		t.Fatalf("card %s has no exact current sent receipt: %+v found=%t err=%v", cardID, receipt, found, err)
	}
	raw, err := json.Marshal(receipt.DeliveryReference)
	if err != nil {
		t.Fatal(err)
	}
	var reference struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(raw, &reference); err != nil || reference.ID < 1 {
		t.Fatalf("receipt has no real provider message: %s %v", raw, err)
	}
	return reference.ID
}

func waitChannelAnchorPublicApproval(t *testing.T, h *channelOnboardingE2EHarness, cardID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var result struct {
			Card struct {
				CardID  string `json:"card_id"`
				Status  string `json:"status"`
				Verdict string `json:"verdict"`
			} `json:"decision_card"`
		}
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": cardID}, &result)
		if result.Card.CardID != cardID {
			t.Fatal("public card readback changed identity")
		}
		if result.Card.Status == decisioncard.StatusDecided && result.Card.Verdict == "approve" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("authenticated channel action did not decide actual card: %+v", result)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitChannelAnchorRejectedIntent(t *testing.T, reader render.Observer, interfaceKey, providerEventID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		result, found, err := reader.ObserveChannelIntent(context.Background(), render.IntentObservationQuery{
			Kind: render.IntentAction, Provider: "telegram", ProviderEventID: providerEventID, InterfaceKey: interfaceKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		if found && result.State == "settled" && (result.Disposition == "stale" || result.Disposition == "rejected") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("exact stale callback has no durable non-mutation disposition: %+v found=%t", result, found)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func writeChannelAnchorJourneySource(t *testing.T, root string, withNotice bool) {
	t.Helper()
	fixture := canonicalrouting.CopyMailboxCompletionMatrix(t)
	if withNotice {
		fixture = canonicalrouting.CopyMailboxNoticeCompletion(t)
	}
	if err := filepath.WalkDir(fixture, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(fixture, path)
		if err != nil {
			return err
		}
		target := filepath.Join(root, "reviews", relative)
		if relative == "observers" || strings.HasPrefix(relative, "observers/") {
			target = filepath.Join(root, relative)
		}
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if relative == "events.yaml" {
			var events map[string]any
			if err := yaml.Unmarshal(body, &events); err != nil {
				return err
			}
			for _, event := range []string{"work.requested", "observer.requested", "effect.requested", "notice.requested"} {
				delete(events, event)
			}
			body, err = yaml.Marshal(events)
			if err != nil {
				return err
			}
		}
		if relative == "schema.yaml" {
			pins := "pins:\n  inputs:\n    - work.requested\n    - effect.requested\n    - observer.requested\n"
			outputs := "  outputs: [observer.requested]\nconnect:\n  - {event: observer.requested, from: ., to: observers}\n"
			if withNotice {
				pins += "    - notice.requested\n"
				outputs = "  outputs: [observer.requested, notice.requested]\nconnect:\n  - {event: observer.requested, from: ., to: observers}\n  - {event: notice.requested, from: ., to: observers}\n"
			}
			// Keep the executable gate's scalar styles intact while relocating its pins.
			old := pins + outputs
			if strings.Count(string(body), old) != 1 {
				return fmt.Errorf("channel anchor fixture changed its exact pins/connect declaration")
			}
			body = []byte(strings.Replace(string(body), old, "pins:\n  inputs:\n    - work.requested\n    - effect.requested\n", 1))
		}
		return os.WriteFile(target, body, 0o600)
	}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "schema.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "name: telegram-agent\n" {
		t.Fatalf("channel anchor fixture requires the isolated root declaration, got:\n%s", body)
	}
	var pins, connects strings.Builder
	pins.WriteString("pins:\n  inputs:\n")
	events := []string{"work.requested", "observer.requested", "effect.requested"}
	if withNotice {
		events = append(events, "notice.requested")
	}
	for _, event := range events {
		fmt.Fprintf(&pins, "    - %s\n", event)
	}
	pins.WriteString("  outputs:\n")
	connects.WriteString("connect:\n")
	for _, event := range events {
		fmt.Fprintf(&pins, "    - %s\n", event)
		target := "reviews"
		if event == "observer.requested" || event == "notice.requested" {
			target = "observers"
		}
		fmt.Fprintf(&connects, "  - {event: %s, from: ., to: %s}\n", event, target)
	}
	body = append(body, []byte(pins.String()+connects.String())...)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	eventDocument := "work.requested:\n  seed: boolean\nobserver.requested:\n  seed: boolean\n  deadline_at: text?\neffect.requested:\n  seed: boolean\n"
	if withNotice {
		eventDocument += "notice.requested:\n  seed: boolean\n"
	}
	if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte(eventDocument), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestScalar2556ChannelAnchorSourcePreservesGateText(t *testing.T) {
	for _, withNotice := range []bool{false, true} {
		t.Run(fmt.Sprintf("notice_%t", withNotice), func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
			disableChannelOnboardingBusinessConsumers(t, root)
			writeChannelAnchorJourneySource(t, root, withNotice)
			raw, err := os.ReadFile(filepath.Join(root, "reviews", "schema.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			for _, text := range []string{`fields: {result: "approved"}`, `fields: {result: "rejected"}`} {
				if strings.Count(string(raw), text) != 1 {
					t.Fatalf("relocation lost exact quoted gate text %s:\n%s", text, raw)
				}
			}
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			hash, err := contracts.BundleHash(bundle)
			if err != nil {
				t.Fatal(err)
			}
			fact, err := correlation.NewSourceArtifactFact(hash)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := runtimepkg.AdmitEffectiveSourceProjection(runtimepkg.EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
			if err != nil {
				t.Fatal(err)
			}
			if findings := bootverify.Run(context.Background(), projection.Source(), bootverify.Options{Purpose: bootverify.StructuralValidation}).HardInvalidities(); len(findings) != 0 {
				t.Fatalf("composed channel anchor source is invalid: %#v", findings)
			}
		})
	}
}

func waitChannelAnchorCard(t *testing.T, db *sql.DB, runID string, kind decisioncard.AnchorKind, flowInstance string) string {
	t.Helper()
	return waitChannelAnchorCardInState(t, db, runID, kind, flowInstance, "")
}

func waitChannelAnchorCardInState(t *testing.T, db *sql.DB, runID string, kind decisioncard.AnchorKind, flowInstance, status string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		rows, err := db.Query(`SELECT card_id,CAST(anchor AS TEXT) FROM decision_cards WHERE run_id=$1 AND anchor_kind=$2 AND ($3='' OR status=$3)`, runID, string(kind), status)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		id := ""
		for rows.Next() {
			var candidate, raw string
			if err := rows.Scan(&candidate, &raw); err != nil {
				t.Fatal(err)
			}
			anchor, err := decisioncard.DecodeAnchor(string(kind), []byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			scope, err := anchor.Scope()
			if err != nil {
				t.Fatal(err)
			}
			if scope.FlowInstance == flowInstance {
				if id != "" {
					t.Fatalf("multiple %s cards for exact %s/%s", kind, runID, flowInstance)
				}
				id = candidate
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if id != "" {
			return id
		}
		if time.Now().After(deadline) {
			t.Fatalf("real %s producer did not commit its exact %s/%s card", kind, runID, flowInstance)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitChannelAnchorReceipt(t *testing.T, db *sql.DB, cardID string) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var raw string
		err := db.QueryRow(`SELECT CAST(r.provider_reference AS TEXT) FROM channel_delivery_receipts r
			JOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id
			WHERE p.source_id=$1 AND r.state='sent' ORDER BY r.settled_at LIMIT 1`, cardID).Scan(&raw)
		if err == nil {
			var reference struct {
				Delivery struct {
					ID int `json:"id"`
				} `json:"delivery_reference"`
			}
			if err := json.Unmarshal([]byte(raw), &reference); err != nil || reference.Delivery.ID < 1 {
				t.Fatalf("receipt has no real provider message: %s %v", raw, err)
			}
			return reference.Delivery.ID
		}
		if err != sql.ErrNoRows || time.Now().After(deadline) {
			t.Fatalf("real card was not delivered: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitChannelAnchorDecision(t *testing.T, db *sql.DB, cardID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var status, verdict string
		if err := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, cardID).Scan(&status, &verdict); err != nil {
			t.Fatal(err)
		}
		if status == "decided" && verdict == "approve" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("authenticated channel action did not decide actual card: %s/%s", status, verdict)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitChannelRejectedCallback(t *testing.T, db *sql.DB, token string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var count int
		query := `SELECT COUNT(*) FROM operator_channel_action_intents
			WHERE state='settled' AND disposition IN ('stale','rejected') AND CAST(fact AS TEXT) LIKE $1`
		if err := db.QueryRow(query, "%"+token+"%").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("stale callback has no durable non-mutation disposition")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitChannelAnchorTerminalEdit(t *testing.T, h *channelOnboardingE2EHarness, messageID int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, edit := range h.provider.Edits() {
			if fmt.Sprint(edit["message_id"]) != fmt.Sprint(messageID) || !strings.Contains(fmt.Sprint(edit["text"]), "Decision: approve") {
				continue
			}
			markup, _ := edit["reply_markup"].(map[string]any)
			rows, _ := markup["inline_keyboard"].([]any)
			if len(rows) != 0 {
				t.Fatalf("terminal card retains mutation controls: %v", edit)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual card receipt was not terminally edited: %v", h.provider.Edits())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
