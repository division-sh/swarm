package serveapp

import (
	"context"
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
	"github.com/division-sh/swarm/internal/store/storetest"
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
	h, _, p, command, hash := startObjectChannelInputJourney(t, backend, false)
	reader := channelProcessStorageObservation(t, h)
	closeObjectIntentObservation(t, p)
	h.stop(t)
	t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
	first := startChannelOnboardingCrashServeProcess(t, h.opts, objectChannelProviderURL(t, p))
	h.endpoint = first.endpoint(t)
	p.intentReader = openChannelAnchorObservation(t, h)
	var cards [2]string
	var before [2][7]string
	for index := range cards {
		card, receipt, reject := beginObjectInputCard(t, h, p, hash, index)
		postObjectInputAction(t, p, fmt.Sprintf("authority-begin-%d", index), receipt, reject)
		waitChannelDraftPromptSettlement(t, channelSettlementStorageObservation(t, h), card)
		cards[index], before[index] = card, selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), card)
	}
	for _, occurrence := range []string{"authority-answer", "authority-unrelated"} {
		postObjectChannelFact(t, p, occurrence, map[string]any{"text": occurrence + "-private"})
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, occurrence), "chooser")
		waitObjectRequestedResponse(t, channelSettlementStorageObservation(t, h), occurrence)
	}
	_, chooser := waitObjectRequestedResponse(t, channelSettlementStorageObservation(t, h), "authority-answer")
	token := firstObjectDraftChoice(t, p, chooser)
	operationID, err := reader.ReadChannelCurrentActivationOperationStorage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	old := getChannelOnboardingRPC(t, h, operationID)
	successorIntent := p.intentIdentity
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
		successorIntent = channeldelivery.IntentObservationQuery{Provider: ready.Operation.Provider, InterfaceKey: ready.Operation.Interface.Key()}
	} else {
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.unbind", map[string]any{
			"interface": old.Operation.Interface.Selector, "expected_revision": old.Binding.Revision, "idempotency_key": "authority-unbind",
		}, &result)
	}
	waitSelectedChannelDeliverySendsSettled(t, channelSettlementStorageObservation(t, h))
	beforeACK, beforeEdit := objectChannelCallCounts(p)
	closeObjectIntentObservation(t, p)
	if err := first.kill(); err != nil {
		t.Fatal(err)
	}
	if first.waitError() == nil {
		t.Fatal("authority interruption reported graceful shutdown")
	}
	second := startChannelOnboardingCrashServeProcess(t, h.opts, objectChannelProviderURL(t, p))
	h.endpoint = second.endpoint(t)
	p.intentReader = openChannelAnchorObservation(t, h)
	waitSelectedChannelDeliverySendsSettled(t, channelSettlementStorageObservation(t, h))
	if ack, edit := objectChannelCallCounts(p); ack != beforeACK || edit != beforeEdit {
		t.Fatalf("authority restart replayed old effects: ACK %d/%d edit %d/%d", beforeACK, ack, beforeEdit, edit)
	}
	if change == "rebind" {
		for attempt := 0; attempt < 2; attempt++ {
			postObjectChannelFact(t, p, "authority-old-choice", map[string]any{"token": token, "cursor": "authority-old-choice", "message": map[string]any{"id": chooser}})
		}
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(successorIntent, channeldelivery.IntentAction, "authority-old-choice"), "stale", "rejected")
		postObjectChannelFact(t, p, "authority-old-text", map[string]any{
			"text": "/" + command, "entry": map[string]any{"reference": command, "address": "mock_bot"},
		})
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(successorIntent, channeldelivery.IntentText, "authority-old-text"), "entry_rejected")
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
		if after := selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), card); after != before[index] {
			t.Fatalf("retained chooser gained successor authority: %v / %v", before[index], after)
		}
	}
	if ack, edit := objectChannelCallCounts(p); ack != beforeACK || edit != beforeEdit {
		t.Fatalf("obsolete control dispatched effects: ACK %d/%d edit %d/%d", beforeACK, ack, beforeEdit, edit)
	}
	for _, occurrence := range []string{"authority-answer", "authority-unrelated"} {
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, occurrence), "chooser")
	}
	var decisions int
	if err := func() error {
		var err error
		decisions, err = channelAnchorStorageObservation(t, h).CountEventNameStorage(context.Background(), "mailbox.card_decided")
		return err
	}(); err != nil || decisions != 0 {
		t.Fatalf("obsolete authority decided a card: %d %v", decisions, err)
	}
	closeObjectIntentObservation(t, p)
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
	h, _, p, _, hash := startObjectChannelJourney(t, backend, root, telegramPhraseBotLLMRuntime{}, "")
	reader := channelProcessStorageObservation(t, h)
	closeObjectIntentObservation(t, p)
	h.stop(t)
	t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
	first := startChannelOnboardingCrashServeProcess(t, h.opts, objectChannelProviderURL(t, p))
	h.endpoint = first.endpoint(t)
	p.intentReader = openChannelAnchorObservation(t, h)
	card, receipt, reject := beginObjectInputCard(t, h, p, hash, 0)
	postObjectInputAction(t, p, "crash-input-begin", receipt, reject)
	waitChannelDraftPromptSettlement(t, channelSettlementStorageObservation(t, h), card)
	var otherCard, action, actionReceipt string
	var unrelated [7]string
	if mode == "chosen" {
		otherCard, actionReceipt, reject = beginObjectInputCard(t, h, p, hash, 1)
		postObjectInputAction(t, p, "crash-other-begin", actionReceipt, reject)
		waitChannelDraftPromptSettlement(t, channelSettlementStorageObservation(t, h), otherCard)
		postObjectChannelFact(t, p, "crash-answer", map[string]any{"text": "private-crash-answer"})
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, "crash-answer"), "chooser")
		_, actionReceipt = waitObjectRequestedResponse(t, channelSettlementStorageObservation(t, h), "crash-answer")
		action = firstObjectDraftChoice(t, p, actionReceipt)
		selected := objectChannelChoiceCard(t, channelProcessStorageObservation(t, h), action)
		if selected == otherCard {
			card, otherCard = otherCard, card
			receipt = waitObjectCardReceipt(t, channelSettlementStorageObservation(t, h), card)
		}
		unrelated = selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), otherCard)
		postObjectChannelFact(t, p, "crash-unrelated-answer", map[string]any{"text": "unrelated-private-answer"})
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, "crash-unrelated-answer"), "chooser")
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
	waitChannelInputCommitDispositions(t, p, mode, partial)
	assertChannelInputCommit(t, reader, card, mode, partial)
	committed := selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), card)
	beforeACK, beforeEdit := objectChannelCallCounts(p)
	closeObjectIntentObservation(t, p)
	if err := first.kill(); err != nil {
		t.Fatal(err)
	}
	if first.waitError() == nil {
		t.Fatal("input process death reported graceful shutdown")
	}
	release()
	second := startChannelOnboardingCrashServeProcess(t, h.opts, objectChannelProviderURL(t, p))
	h.endpoint = second.endpoint(t)
	p.intentReader = openChannelAnchorObservation(t, h)
	waitChannelInputCommitDispositions(t, p, mode, partial)
	assertChannelInputCommit(t, reader, card, mode, partial)
	if after := selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), card); after != committed {
		t.Fatalf("restart reinterpreted committed input: %v / %v", committed, after)
	}
	if mode == "plain" {
		postObjectChannelFact(t, p, "crash-answer", map[string]any{"text": "private-crash-answer"})
	} else {
		postObjectChannelFact(t, p, "crash-input-action", map[string]any{"token": action, "cursor": "crash-input-action", "message": map[string]any{"id": actionReceipt}})
	}
	waitChannelInputCommitDispositions(t, p, mode, partial)
	assertChannelInputCommit(t, reader, card, mode, partial)
	if mode == "chosen" {
		if after := selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), otherCard); after != unrelated {
			t.Fatal("chosen input consumed the unrelated draft")
		}
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, "crash-unrelated-answer"), "chooser")
	}
	afterACK, afterEdit := objectChannelCallCounts(p)
	if beforeACK != afterACK || beforeEdit != afterEdit {
		t.Fatalf("restart/duplicate replayed provider effects: ACK %d/%d edit %d/%d", beforeACK, afterACK, beforeEdit, afterEdit)
	}
	if after := selectedChannelDraftState(t, channelSettlementStorageObservation(t, h), card); after != committed {
		t.Fatal("duplicate occurrence repeated committed draft progress")
	}
	closeObjectIntentObservation(t, p)
	if err := second.stop(); err != nil {
		t.Fatalf("recovered input process did not stop: %v", err)
	}
}

func waitChannelInputCommitDispositions(t *testing.T, p *objectChannelProvider, mode string, partial bool) {
	t.Helper()
	if mode != "skip" {
		disposition := "input_complete"
		if partial {
			disposition = "input_progressed"
		}
		if mode == "chosen" {
			disposition = "chooser"
		}
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, "crash-answer"), disposition)
	}
	if mode != "plain" {
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentAction, "crash-input-action"), "applied")
	}
}

func assertChannelInputCommit(t *testing.T, reader storetest.ChannelProcessObservation, card, mode string, partial bool) {
	t.Helper()
	observed, err := reader.ReadChannelInputCommitStorage(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	wantDraft, wantCard, wantIndex, wantDecisions := "consumed", "decided", 0, 1
	fields := observed.CardFields
	if partial {
		wantDraft, wantCard, wantIndex, wantDecisions = "active", "pending", 1, 0
		fields = observed.DraftFields
	}
	if observed.Index != wantIndex || observed.DraftStatus != wantDraft || observed.CardStatus != wantCard || observed.Decisions != wantDecisions {
		t.Fatalf("non-atomic input commit: index=%d draft=%s card=%s events=%d", observed.Index, observed.DraftStatus, observed.CardStatus, observed.Decisions)
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

func objectChannelChoiceCard(t *testing.T, reader storetest.ChannelProcessObservation, token string) string {
	t.Helper()
	observed, err := reader.ReadChannelActionChoiceStorage(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.Canonicalize(observed.Input)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := channeldelivery.Decode(raw, observed.Hash)
	if err != nil || observed.Position < 1 || observed.Position > len(frozen.DraftChoices) {
		t.Fatalf("chosen input has no exact frozen target: %v", err)
	}
	return frozen.DraftChoices[observed.Position-1].CardID
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
