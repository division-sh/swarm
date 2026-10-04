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
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
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
			h, db, bundleHash := startChannelAnchorJourney(t, backend, "anchor-token", false)
			for _, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect} {
				t.Run(string(kind), func(t *testing.T) {
					seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
						"event_name": "work.requested", "bundle_hash": bundleHash,
						"payload": map[string]any{"seed": true}, "idempotency_key": "channel-anchor-" + string(kind),
					})
					gateID := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate)
					waitChannelAnchorReceipt(t, db, gateID)
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
					cardID := waitChannelAnchorCard(t, db, seed.RunID, kind)
					messageID := waitChannelAnchorReceipt(t, db, cardID)
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
					waitChannelAnchorDecision(t, db, cardID)
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
						deadline := time.Now().Add(15 * time.Second)
						for {
							found := false
							for _, edit := range h.provider.Edits() {
								found = found || (fmt.Sprint(edit["message_id"]) == fmt.Sprint(messageID) && strings.Contains(fmt.Sprint(edit["text"]), "Dispatch: succeeded"))
							}
							if found {
								break
							}
							if time.Now().After(deadline) {
								t.Fatalf("actual receipt did not reflect completed dispatch: %v", h.provider.Edits())
							}
							time.Sleep(20 * time.Millisecond)
						}
					}
					post(838000+messageID, "double-tap-"+cardID)
					waitChannelRejectedCallback(t, db, token)
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&count); err != nil || count != 1 {
						t.Fatalf("double tap changed completion cardinality: count=%d err=%v", count, err)
					}
				})
			}
		})
	}
}

func startChannelAnchorJourney(t *testing.T, backend servedparity.Backend, token string, withSummary bool) (*channelOnboardingE2EHarness, *sql.DB, string) {
	t.Helper()
	return startChannelAnchorJourneyWithDraftTTL(t, backend, token, withSummary, 0)
}

func startChannelAnchorJourneyWithDraftTTL(t *testing.T, backend servedparity.Backend, token string, withSummary bool, draftTTL time.Duration) (*channelOnboardingE2EHarness, *sql.DB, string) {
	t.Helper()
	h := newChannelOnboardingE2EHarness(t, backend, true)
	h.opts.AbandonActiveRuns = false
	writeChannelAnchorJourneySource(t, h.opts.SourceRoot, withSummary)
	h.opts.TestLLMRuntime = channelAnchorLLMRuntime{}
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
	driver := "sqlite"
	if backend == servedparity.BackendExplicitPostgres {
		driver = "postgres"
	}
	db, err := sql.Open(driver, h.storeDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
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
	return h, db, identity.SourceArtifacts[0].BundleHash
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
			var schema map[string]any
			if err := yaml.Unmarshal(body, &schema); err != nil {
				return err
			}
			schema["pins"] = map[string]any{"inputs": []string{"work.requested", "effect.requested"}}
			delete(schema, "connect")
			body, err = yaml.Marshal(schema)
			if err != nil {
				return err
			}
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
	var schema map[string]any
	if err := yaml.Unmarshal(body, &schema); err != nil {
		t.Fatal(err)
	}
	inputs, outputs := []any{}, []any{}
	connects := []any{}
	events := []string{"work.requested", "observer.requested", "effect.requested"}
	if withNotice {
		events = append(events, "notice.requested")
	}
	for _, event := range events {
		inputs = append(inputs, event)
		outputs = append(outputs, event)
		target := "reviews"
		if event == "observer.requested" || event == "notice.requested" {
			target = "observers"
		}
		connects = append(connects, map[string]any{"event": event, "from": ".", "to": target})
	}
	schema["pins"] = map[string]any{"inputs": inputs, "outputs": outputs}
	schema["connect"] = connects
	body, err = yaml.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
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

func waitChannelAnchorCard(t *testing.T, db *sql.DB, runID string, kind decisioncard.AnchorKind) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var id string
		err := db.QueryRow(`SELECT card_id FROM decision_cards WHERE run_id=$1 AND anchor_kind=$2`, runID, string(kind)).Scan(&id)
		if err == nil {
			return id
		}
		if err != sql.ErrNoRows || time.Now().After(deadline) {
			t.Fatalf("real %s producer did not commit a card: %v", kind, err)
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
