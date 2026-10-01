package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelLearnedObjectInputPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, schedule := range []string{"chooser_current", "chooser_restart", "chooser_edit_ack_loss", "lost_prompt_edit"} {
			t.Run(string(backend)+"/"+schedule, func(t *testing.T) {
				h, db, provider, command, hash := startObjectChannelInputJourney(t, backend, false)
				if schedule != "lost_prompt_edit" {
					proveObjectChannelChooserRestart(t, h, db, provider, hash, schedule)
				} else {
					proveObjectChannelLostPrompt(t, h, db, provider, hash, command)
				}
				provider.mu.Lock()
				defer provider.mu.Unlock()
				for _, operation := range []string{"/v2/deliver", "/v2/edit"} {
					for _, input := range provider.calls[operation] {
						if !objectChannelPresentationValid(input) || strings.Contains(fmt.Sprint(input["body"]), "object-private-") {
							t.Fatalf("%s violated tighter bounds or exposed private input: %#v", operation, input)
						}
					}
				}
			})
		}
	}
}

func TestChannelLearnedObjectInvalidChosenAnswerPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h, db, p, _, hash := startObjectChannelInputJourney(t, backend, true)
			for index := 0; index < 2; index++ {
				card, receipt, reject := beginObjectInputCard(t, h, db, p, hash, index)
				postObjectInputAction(t, p, fmt.Sprintf("typed-reject-%d", index), receipt, reject)
				waitChannelDraftPromptSettlement(t, db, card)
			}
			postObjectChannelFact(t, p, "typed-invalid-answer", map[string]any{"text": "not-an-integer"})
			waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "typed-invalid-answer", "chooser")
			_, chooser := waitObjectRequestedResponse(t, db, "typed-invalid-answer")
			choice := firstObjectDraftChoice(t, p, chooser)
			postObjectInputAction(t, p, "typed-invalid-select", chooser, choice)
			waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "typed-invalid-select", "navigation")
			_, teaching := waitObjectIntentResponse(t, db, "operator_channel_action_intents", "typed-invalid-select")
			waitObjectMessageText(t, p, teaching, "That answer does not match the requested field.")
			p.mu.Lock()
			input := p.message(teaching)
			valid := objectChannelPresentationValid(input)
			p.mu.Unlock()
			if !valid {
				t.Fatalf("chosen-answer teaching exceeded the compiled provider bounds: %#v", input)
			}
			var decided int
			if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE status='decided'`).Scan(&decided); err != nil || decided != 0 {
				t.Fatalf("invalid chosen answer completed a card: %d %v", decided, err)
			}
			postObjectChannelFact(t, p, "typed-valid-answer", map[string]any{"text": "27"})
			waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "typed-valid-answer", "chooser")
			_, nextChooser := waitObjectRequestedResponse(t, db, "typed-valid-answer")
			postObjectInputAction(t, p, "typed-valid-select", nextChooser, firstObjectDraftChoice(t, p, nextChooser))
			waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "typed-valid-select", "applied")
			if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE status='decided'`).Scan(&decided); err != nil || decided != 1 {
				t.Fatalf("valid retry did not complete exactly one card: %d %v", decided, err)
			}
		})
	}
}

func firstObjectDraftChoice(t *testing.T, p *objectChannelProvider, receipt string) string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	input := p.message(receipt)
	controls, _ := input["controls"].([]any)
	for _, control := range controls {
		row, _ := control.(map[string]any)
		if row["name"] != "More choices" {
			if token, ok := row["value"].(string); ok && token != "" {
				return token
			}
		}
	}
	t.Fatalf("delivered chooser has no draft choice: %#v", input)
	return ""
}

func startObjectChannelInputJourney(t *testing.T, backend servedparity.Backend, integerInput bool) (*channelOnboardingE2EHarness, *sql.DB, *objectChannelProvider, string, string) {
	t.Helper()
	root := canonicalrouting.CopyChannelLearnedObjectJourney(t)
	if integerInput {
		root = canonicalrouting.CopyChannelLearnedObjectIntegerInputJourney(t)
	}
	return startObjectChannelJourney(t, backend, root, telegramPhraseBotLLMRuntime{}, "")
}

func startObjectChannelJourney(t *testing.T, backend servedparity.Backend, root string, llm runtimellm.Runtime, activityCredential string) (*channelOnboardingE2EHarness, *sql.DB, *objectChannelProvider, string, string) {
	t.Helper()
	h := newChannelOnboardingE2EHarness(t, backend, true)
	p := &objectChannelProvider{commands: map[string][]any{}, calls: map[string][]map[string]any{}}
	server := httptest.NewServer(p)
	t.Cleanup(server.Close)
	redirectExternalHosts(t, map[string]string{"mock.example.test": server.URL})
	h.opts.SourceRoot = writeObjectChannelPacks(t, h.opts.ConfigPath, root)
	h.opts.TestLLMRuntime = llm
	h.opts.AbandonActiveRuns = false
	if activityCredential != "" {
		credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := credentials.Set(context.Background(), "mock_api_key", activityCredential); err != nil {
			t.Fatal(err)
		}
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
	var begun channelonboarding.Result
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
		"provider": "mock", "verb": "connect", "provider_credential": "object-provider-secret", "save_proof": true, "client_language": "fr",
	}, &begun)
	if begun.IdentityOperation == nil {
		t.Fatal("learned input journey lacks the public identity ceremony")
	}
	postObjectChannelFact(t, p, "input-claim", map[string]any{"text": begun.IdentityOperation.Challenge})
	claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
	if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
		t.Fatalf("public object claim not admitted: %+v", claimed)
	}
	var confirmation map[string]any
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
		"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
	}, &confirmation)
	ready := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
	if ready.Readiness == nil || !ready.Readiness.Ready {
		t.Fatalf("public learned input journey not READY: %+v", ready)
	}
	waitObjectNativeQualification(t, h, begun.Operation.OperationID)
	p.mu.Lock()
	installed := p.calls["/v2/install"]
	var command string
	if len(installed) == 1 {
		commands, _ := installed[0]["commands"].([]any)
		if len(commands) == 1 {
			row, _ := commands[0].(map[string]any)
			command, _ = row["command"].(string)
		}
	}
	p.mu.Unlock()
	if command == "" {
		t.Fatal("public learned journey has no actual native launcher")
	}
	var identity apiv1.RuntimeIdentityResult
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
	if len(identity.SourceArtifacts) != 1 {
		t.Fatalf("unexpected public source identity: %+v", identity)
	}
	return h, db, p, command, identity.SourceArtifacts[0].BundleHash
}

func postObjectChannelFact(t *testing.T, p *objectChannelProvider, id string, fact map[string]any) {
	t.Helper()
	p.mu.Lock()
	callback, signing := p.callback, p.signing
	p.mu.Unlock()
	fact["principal"], fact["room"], fact["scope"] = "operator-a", "queue-a", "direct"
	if fact["message"] == nil {
		fact["message"] = map[string]any{"id": id}
	}
	objectChannelIngress(t, callback, signing, id, fact)
}

func postObjectInputAction(t *testing.T, p *objectChannelProvider, id, receipt, token string) {
	t.Helper()
	fact := map[string]any{"token": token, "cursor": id, "message": map[string]any{"id": receipt}}
	postObjectChannelFact(t, p, id, fact)
	postObjectChannelFact(t, p, id, fact)
	waitObjectChannelEffect(t, p, "/v2/ack", func(input map[string]any) bool { return input["cursor"] == id })
}

func beginObjectInputCard(t *testing.T, h *channelOnboardingE2EHarness, db *sql.DB, p *objectChannelProvider, hash string, index int) (string, string, string) {
	t.Helper()
	seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
		"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"detail": strings.Repeat("input-owned detail;", 60)},
		"idempotency_key": fmt.Sprintf("input-card-%d", index),
	})
	card := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate)
	receipt := waitObjectCardReceipt(t, db, card)
	for page := 0; page < 2; page++ {
		more := waitObjectMessageControl(t, p, receipt, "More choices")
		postObjectInputAction(t, p, fmt.Sprintf("input-page-%d-%d", index, page), receipt, more)
		label := "approve"
		if page == 1 {
			label = "reject"
		}
		waitObjectMessageControl(t, p, receipt, label)
	}
	return card, receipt, waitObjectMessageControl(t, p, receipt, "reject")
}

func proveObjectChannelChooserRestart(t *testing.T, h *channelOnboardingE2EHarness, db *sql.DB, p *objectChannelProvider, hash, schedule string) {
	t.Helper()
	cards := make([]string, 3)
	for index := range cards {
		card, receipt, reject := beginObjectInputCard(t, h, db, p, hash, index)
		cards[index] = card
		postObjectInputAction(t, p, fmt.Sprintf("input-reject-%d", index), receipt, reject)
		waitChannelDraftPromptSettlement(t, db, card)
	}
	postObjectChannelFact(t, p, "ambiguous-answer", map[string]any{"text": "object-private-ambiguous-answer"})
	waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "ambiguous-answer", "chooser")
	deliveryID, receipt := waitObjectRequestedResponse(t, db, "ambiguous-answer")
	if schedule == "chooser_restart" {
		h.stop(t)
		h.start(t)
	}
	if schedule == "chooser_edit_ack_loss" {
		proveObjectResponseEditLoss(t, h, db, p, deliveryID, receipt)
		postObjectChannelFact(t, p, "fresh-chooser-answer", map[string]any{"text": "object-private-new-answer"})
		waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "fresh-chooser-answer", "chooser")
		_, fresh := waitObjectRequestedResponse(t, db, "fresh-chooser-answer")
		if fresh == receipt {
			t.Fatal("new explicit answer reused uncertain chooser")
		}
		receipt = fresh
	}
	seen := map[string]bool{}
	var selected string
	for page := 0; page < 3; page++ {
		p.mu.Lock()
		input := p.message(receipt)
		controls, _ := input["controls"].([]any)
		var next string
		for _, control := range controls {
			row, _ := control.(map[string]any)
			label, _ := row["name"].(string)
			token, _ := row["value"].(string)
			if label == "More choices" {
				next = token
			} else {
				seen[token] = true
				selected = token
			}
		}
		p.mu.Unlock()
		if next == "" || selected == "" {
			t.Fatalf("two-action chooser cannot reach all three drafts: %#v", input)
		}
		if page < 2 {
			postObjectInputAction(t, p, fmt.Sprintf("chooser-page-%d", page), receipt, next)
			waitObjectEditAfterAction(t, p, receipt, next)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("chooser dropped or duplicated drafts across its pages: %#v", seen)
	}
	postObjectInputAction(t, p, "chooser-select", receipt, selected)
	deadline := time.Now().Add(15 * time.Second)
	for {
		var decided int
		if err := db.QueryRow(`SELECT COUNT(*) FROM decision_cards WHERE card_id IN ($1,$2,$3) AND status='decided'`, cards[0], cards[1], cards[2]).Scan(&decided); err != nil {
			t.Fatal(err)
		}
		if decided == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained chooser completed %d cards, want exactly one", decided)
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.stop(t)
	h.start(t)
	postObjectChannelFact(t, p, "obsolete-choice", map[string]any{"token": selected, "cursor": "obsolete-choice", "message": map[string]any{"id": receipt}})
	waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "obsolete-choice", "stale", "rejected")
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate/restart changed chooser completion cardinality: %d %v", count, err)
	}
}

func waitObjectDeliveryContaining(t *testing.T, p *objectChannelProvider, text string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		p.mu.Lock()
		for index := len(p.deliveries) - 1; index >= 0; index-- {
			if strings.Contains(fmt.Sprint(p.deliveries[index]["body"]), text) {
				p.mu.Unlock()
				return fmt.Sprintf("delivery-%d", index+1)
			}
		}
		p.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("provider has no actual delivery containing %q", text)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitObjectEditAfterAction(t *testing.T, p *objectChannelProvider, receipt, oldToken string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		p.mu.Lock()
		input := p.message(receipt)
		controls, _ := input["controls"].([]any)
		old := false
		for _, control := range controls {
			row, _ := control.(map[string]any)
			old = old || row["value"] == oldToken
		}
		p.mu.Unlock()
		if !old {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("chooser navigation has not published the successor receipt controls")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveObjectChannelLostPrompt(t *testing.T, h *channelOnboardingE2EHarness, db *sql.DB, p *objectChannelProvider, hash, command string) {
	t.Helper()
	card, oldReceipt, reject := beginObjectInputCard(t, h, db, p, hash, 0)
	p.mu.Lock()
	p.loseNextPrompt = true
	p.mu.Unlock()
	postObjectInputAction(t, p, "lost-prompt-reject", oldReceipt, reject)
	waitUncertainCopyPlan(t, db, card)
	waitObjectMessageText(t, p, oldReceipt, "Input: reason (text) required")
	for _, restart := range []bool{false, true} {
		if restart {
			h.stop(t)
			h.start(t)
		}
		id := fmt.Sprintf("old-prompt-%t", restart)
		postObjectChannelFact(t, p, id, map[string]any{"token": reject, "cursor": id, "message": map[string]any{"id": oldReceipt}})
		waitObjectChannelDisposition(t, db, "operator_channel_action_intents", id, "stale", "rejected")
		postObjectChannelFact(t, p, id+"-text", map[string]any{"text": "object-private-obsolete", "reply": map[string]any{"id": oldReceipt}})
		waitObjectChannelDisposition(t, db, "operator_channel_text_intents", id+"-text", "teaching")
	}
	postObjectChannelFact(t, p, "prompt-recovery", map[string]any{"text": "/" + command, "entry": map[string]any{"reference": command, "address": "mock_bot"}})
	inbox := waitObjectDeliveryContaining(t, p, "Older copies may be outdated")
	p.mu.Lock()
	input := p.message(inbox)
	controls, _ := input["controls"].([]any)
	var resend string
	for _, control := range controls {
		row, _ := control.(map[string]any)
		if strings.HasPrefix(fmt.Sprint(row["name"]), "Resend card") {
			resend, _ = row["value"].(string)
		}
	}
	p.mu.Unlock()
	if resend == "" {
		t.Fatalf("uncertain prompt lacks explicit bounded fresh-copy recovery: %#v", input)
	}
	postObjectInputAction(t, p, "prompt-resend", inbox, resend)
	fresh := waitObjectCardReceipt(t, db, card)
	if fresh == oldReceipt {
		t.Fatal("prompt recovery reused its uncertain predecessor receipt")
	}
	waitObjectMessageText(t, p, fresh, "Input: reason (text) required")
	postObjectChannelFact(t, p, "fresh-prompt-answer", map[string]any{"text": "object-private-fresh-answer", "reply": map[string]any{"id": fresh}})
	waitUncertainCopyDecision(t, db, card)
	waitObjectMessageText(t, p, fresh, "Decision: reject")
	var copies, uncertain, decisions int
	if err := db.QueryRow(`SELECT COUNT(*),SUM(CASE WHEN state='uncertain' THEN 1 ELSE 0 END) FROM channel_delivery_plans WHERE source_kind='card' AND source_id=$1`, card).Scan(&copies, &uncertain); err != nil || copies != 2 || uncertain != 1 {
		t.Fatalf("prompt recovery changed predecessor truth: %d/%d %v", copies, uncertain, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'`).Scan(&decisions); err != nil || decisions != 1 {
		t.Fatalf("uncertain prompt or fresh answer repeated completion: %d %v", decisions, err)
	}
}
