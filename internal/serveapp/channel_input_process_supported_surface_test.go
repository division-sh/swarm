package serveapp

import (
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelInputAuthorityRestartPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, change := range []string{"rebind", "unbind"} {
			t.Run(string(backend)+"/"+change, func(t *testing.T) {
				proveChannelInputAuthorityRestart(t, backend, change)
			})
		}
	}
}

func proveChannelInputAuthorityRestart(t *testing.T, backend servedparity.Backend, change string) {
	t.Helper()
	h, db, p, command, hash := startObjectChannelInputJourney(t, backend, false)
	h.stop(t)
	t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
	first := startChannelOnboardingCrashServeProcess(t, h.opts, objectChannelProviderURL(t, p))
	h.endpoint = first.endpoint(t)
	var cards [2]string
	var before [2][7]string
	for index := range cards {
		card, receipt, reject := beginObjectInputCard(t, h, db, p, hash, index)
		postObjectInputAction(t, p, fmt.Sprintf("authority-begin-%d", index), receipt, reject)
		waitChannelDraftPromptSettlement(t, db, card)
		cards[index], before[index] = card, selectedChannelDraftState(t, db, card)
	}
	for _, occurrence := range []string{"authority-answer", "authority-unrelated"} {
		postObjectChannelFact(t, p, occurrence, map[string]any{"text": occurrence + "-private"})
		waitObjectChannelDisposition(t, db, "operator_channel_text_intents", occurrence, "chooser")
		waitObjectRequestedResponse(t, db, occurrence)
	}
	_, chooser := waitObjectRequestedResponse(t, db, "authority-answer")
	token := firstObjectDraftChoice(t, p, chooser)
	var operationID string
	if err := db.QueryRow(`SELECT operation_id FROM connected_channel_activations WHERE status='current'`).Scan(&operationID); err != nil {
		t.Fatal(err)
	}
	old := getChannelOnboardingRPC(t, h, operationID)
	var result map[string]any
	if change == "rebind" {
		var begun channelonboarding.Result
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
			"provider": "mock", "verb": "rebind", "provider_credential": "object-provider-secret", "save_proof": true, "client_language": "fr",
		}, &begun)
		if begun.IdentityOperation == nil {
			t.Fatal("public authority change lacks identity ceremony")
		}
		p.mu.Lock()
		callback, signing := p.callback, p.signing
		p.mu.Unlock()
		objectChannelIngress(t, callback, signing, "authority-claim", map[string]any{
			"text": begun.IdentityOperation.Challenge, "principal": "operator-a", "room": "queue-b", "scope": "direct", "message": map[string]any{"id": "authority-claim"},
		})
		claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
		if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
			t.Fatal("public authority change was not confirmed by the verified claimant")
		}
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
			"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
		}, &result)
		ready := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
		if ready.Readiness == nil || !ready.Readiness.Ready {
			t.Fatal("public rebind did not become READY")
		}
		waitObjectNativeQualification(t, h, begun.Operation.OperationID)
	} else {
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.unbind", map[string]any{
			"interface": old.Operation.Interface.Selector, "expected_revision": old.Binding.Revision, "idempotency_key": "authority-unbind",
		}, &result)
	}
	waitChannelDeliverySendsSettled(t, db)
	beforeACK, beforeEdit := objectChannelCallCounts(p)
	if err := first.kill(); err != nil {
		t.Fatal(err)
	}
	if first.waitError() == nil {
		t.Fatal("authority interruption reported graceful shutdown")
	}
	second := startChannelOnboardingCrashServeProcess(t, h.opts, objectChannelProviderURL(t, p))
	h.endpoint = second.endpoint(t)
	waitChannelDeliverySendsSettled(t, db)
	if ack, edit := objectChannelCallCounts(p); ack != beforeACK || edit != beforeEdit {
		t.Fatalf("authority restart replayed old effects: ACK %d/%d edit %d/%d", beforeACK, ack, beforeEdit, edit)
	}
	if change == "rebind" {
		for attempt := 0; attempt < 2; attempt++ {
			postObjectChannelFact(t, p, "authority-old-choice", map[string]any{"token": token, "cursor": "authority-old-choice", "message": map[string]any{"id": chooser}})
		}
		waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "authority-old-choice", "stale", "rejected")
		postObjectChannelFact(t, p, "authority-old-text", map[string]any{
			"text": "/" + command, "entry": map[string]any{"reference": command, "address": "mock_bot"},
		})
		waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "authority-old-text", "entry_rejected")
	} else {
		p.mu.Lock()
		callback, signing := p.callback, p.signing
		p.mu.Unlock()
		for _, fact := range []map[string]any{
			{"token": token, "cursor": "withdrawn-choice", "message": map[string]any{"id": chooser}},
			{"text": "must-not-progress", "message": map[string]any{"id": "withdrawn-text"}},
		} {
			fact["principal"], fact["room"], fact["scope"] = "operator-a", "queue-a", "direct"
			if status := objectChannelIngressStatus(t, callback, signing, "withdrawn-occurrence", fact); status != http.StatusNotFound {
				t.Fatalf("retired ingress remained available: %d", status)
			}
		}
	}
	for index, card := range cards {
		if after := selectedChannelDraftState(t, db, card); after != before[index] {
			t.Fatalf("retained chooser gained successor authority: %v / %v", before[index], after)
		}
	}
	if ack, edit := objectChannelCallCounts(p); ack != beforeACK || edit != beforeEdit {
		t.Fatalf("obsolete control dispatched effects: ACK %d/%d edit %d/%d", beforeACK, ack, beforeEdit, edit)
	}
	for _, occurrence := range []string{"authority-answer", "authority-unrelated"} {
		waitObjectChannelDisposition(t, db, "operator_channel_text_intents", occurrence, "chooser")
	}
	var decisions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'`).Scan(&decisions); err != nil || decisions != 0 {
		t.Fatalf("obsolete authority decided a card: %d %v", decisions, err)
	}
	if err := second.stop(); err != nil {
		t.Fatalf("authority recovery did not stop: %v", err)
	}
}

func TestChannelInputAbruptProcessDeathPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, mode := range []string{"plain", "chosen", "skip"} {
			for _, partial := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/partial=%t", backend, mode, partial), func(t *testing.T) {
					proveChannelInputCommitDeath(t, backend, mode, partial)
				})
			}
		}
	}
}

func proveChannelInputCommitDeath(t *testing.T, backend servedparity.Backend, mode string, partial bool) {
	t.Helper()
	root := canonicalrouting.CopyChannelLearnedObjectOptionalInputJourney(t, partial)
	h, db, p, _, hash := startObjectChannelJourney(t, backend, root, telegramPhraseBotLLMRuntime{}, "")
	h.stop(t)
	t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
	first := startChannelOnboardingCrashServeProcess(t, h.opts, objectChannelProviderURL(t, p))
	h.endpoint = first.endpoint(t)
	card, receipt, reject := beginObjectInputCard(t, h, db, p, hash, 0)
	postObjectInputAction(t, p, "crash-input-begin", receipt, reject)
	waitChannelDraftPromptSettlement(t, db, card)
	var otherCard, action, actionReceipt string
	var unrelated [7]string
	if mode == "chosen" {
		otherCard, actionReceipt, reject = beginObjectInputCard(t, h, db, p, hash, 1)
		postObjectInputAction(t, p, "crash-other-begin", actionReceipt, reject)
		waitChannelDraftPromptSettlement(t, db, otherCard)
		postObjectChannelFact(t, p, "crash-answer", map[string]any{"text": "private-crash-answer"})
		waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "crash-answer", "chooser")
		_, actionReceipt = waitObjectRequestedResponse(t, db, "crash-answer")
		action = firstObjectDraftChoice(t, p, actionReceipt)
		selected := objectChannelChoiceCard(t, db, action)
		if selected == otherCard {
			card, otherCard = otherCard, card
			receipt = waitObjectCardReceipt(t, db, card)
		}
		unrelated = selectedChannelDraftState(t, db, otherCard)
		postObjectChannelFact(t, p, "crash-unrelated-answer", map[string]any{"text": "unrelated-private-answer"})
		waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "crash-unrelated-answer", "chooser")
	}
	if mode == "skip" {
		actionReceipt = receipt
		action = objectAnchorChoice(t, p, receipt, "Skip field", 3)
	}
	arrived, release := p.pauseInputEditResponse(t, receipt)
	if mode == "plain" {
		postObjectChannelFact(t, p, "crash-answer", map[string]any{"text": "private-crash-answer"})
	} else {
		postObjectInputAction(t, p, "crash-input-action", actionReceipt, action)
	}
	select {
	case <-arrived:
	case <-time.After(20 * time.Second):
		t.Fatal("committed input never reached the real provider edit-response barrier")
	}
	assertChannelInputCommit(t, db, card, mode, partial)
	committed := selectedChannelDraftState(t, db, card)
	beforeACK, beforeEdit := objectChannelCallCounts(p)
	if err := first.kill(); err != nil {
		t.Fatal(err)
	}
	if first.waitError() == nil {
		t.Fatal("input process death reported graceful shutdown")
	}
	release()
	second := startChannelOnboardingCrashServeProcess(t, h.opts, objectChannelProviderURL(t, p))
	h.endpoint = second.endpoint(t)
	assertChannelInputCommit(t, db, card, mode, partial)
	if after := selectedChannelDraftState(t, db, card); after != committed {
		t.Fatalf("restart reinterpreted committed input: %v / %v", committed, after)
	}
	if mode == "plain" {
		postObjectChannelFact(t, p, "crash-answer", map[string]any{"text": "private-crash-answer"})
	} else {
		postObjectChannelFact(t, p, "crash-input-action", map[string]any{"token": action, "cursor": "crash-input-action", "message": map[string]any{"id": actionReceipt}})
	}
	assertChannelInputCommit(t, db, card, mode, partial)
	if mode == "chosen" {
		if after := selectedChannelDraftState(t, db, otherCard); after != unrelated {
			t.Fatal("chosen input consumed the unrelated draft")
		}
		waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "crash-unrelated-answer", "chooser")
	}
	afterACK, afterEdit := objectChannelCallCounts(p)
	if beforeACK != afterACK || beforeEdit != afterEdit {
		t.Fatalf("restart/duplicate replayed provider effects: ACK %d/%d edit %d/%d", beforeACK, afterACK, beforeEdit, afterEdit)
	}
	if after := selectedChannelDraftState(t, db, card); after != committed {
		t.Fatal("duplicate occurrence repeated committed draft progress")
	}
	if err := second.stop(); err != nil {
		t.Fatalf("recovered input process did not stop: %v", err)
	}
}

func assertChannelInputCommit(t *testing.T, db *sql.DB, card, mode string, partial bool) {
	t.Helper()
	if mode != "skip" {
		disposition := "input_complete"
		if partial {
			disposition = "input_progressed"
		}
		if mode == "chosen" {
			disposition = "chooser"
		}
		waitObjectChannelDisposition(t, db, "operator_channel_text_intents", "crash-answer", disposition)
	}
	if mode != "plain" {
		waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "crash-input-action", "applied")
	}
	var index, decisions int
	var draftStatus, cardStatus, draftFields, cardFields string
	if err := db.QueryRow(`SELECT d.next_field_index,d.status,c.status,CAST(d.input_fields AS TEXT),CAST(c.fields AS TEXT),
		(SELECT COUNT(*) FROM events e WHERE e.run_id=c.run_id AND e.event_name='mailbox.card_decided')
		FROM decision_card_input_drafts d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.card_id=$1`, card).
		Scan(&index, &draftStatus, &cardStatus, &draftFields, &cardFields, &decisions); err != nil {
		t.Fatal(err)
	}
	wantDraft, wantCard, wantIndex, wantDecisions := "consumed", "decided", 0, 1
	fields := cardFields
	if partial {
		wantDraft, wantCard, wantIndex, wantDecisions = "active", "pending", 1, 0
		fields = draftFields
	}
	if index != wantIndex || draftStatus != wantDraft || cardStatus != wantCard || decisions != wantDecisions {
		t.Fatalf("non-atomic input commit: index=%d draft=%s card=%s events=%d", index, draftStatus, cardStatus, decisions)
	}
	value, err := canonicaljson.Decode([]byte(fields))
	if err != nil {
		t.Fatal(err)
	}
	reason, found := value.Lookup("reason")
	if mode == "skip" {
		if found {
			t.Fatal("skip fabricated a field value")
		}
	} else if answer, ok := reason.String(); !found || !ok || answer != "private-crash-answer" {
		t.Fatal("committed input lost its exact selected answer")
	}
}

func objectChannelChoiceCard(t *testing.T, db *sql.DB, token string) string {
	t.Helper()
	var raw []byte
	var hash string
	var position int
	if err := db.QueryRow(`SELECT r.render_input,r.render_hash,a.action_position
		FROM channel_delivery_actions a JOIN channel_delivery_renders r ON r.render_id=a.render_id
		WHERE a.action_token=$1`, token).Scan(&raw, &hash, &position); err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.Canonicalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := channeldelivery.Decode(raw, hash)
	if err != nil || position < 1 || position > len(frozen.DraftChoices) {
		t.Fatalf("chosen input has no exact frozen target: %v", err)
	}
	return frozen.DraftChoices[position-1].CardID
}

func (p *objectChannelProvider) pauseInputEditResponse(t *testing.T, reference string) (<-chan struct{}, func()) {
	t.Helper()
	pause := &objectChannelResponsePause{arrived: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	if p.editResponse != nil {
		p.mu.Unlock()
		t.Fatal("input edit already paused")
	}
	p.editResponse, p.editReference = pause, reference
	p.mu.Unlock()
	var once sync.Once
	release := func() { once.Do(func() { close(pause.release) }) }
	t.Cleanup(release)
	return pause.arrived, release
}

func objectChannelCallCounts(p *objectChannelProvider) (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls["/v2/ack"]), len(p.calls["/v2/edit"])
}

func objectChannelProviderURL(t *testing.T, p *objectChannelProvider) string {
	t.Helper()
	p.mu.Lock()
	baseURL := p.baseURL
	p.mu.Unlock()
	if baseURL != "" {
		return baseURL
	}
	t.Fatal("independent provider fixture has no exact HTTP redirect")
	return ""
}
