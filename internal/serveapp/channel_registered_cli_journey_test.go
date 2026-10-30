package serveapp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

func registeredChannelMailboxCLI(t *testing.T, h *channelOnboardingE2EHarness, wantSuccess bool, args ...string) string {
	t.Helper()
	out, errOut := &lockedBuffer{}, &lockedBuffer{}
	command := append([]string{"--config", h.opts.ConfigPath, "mailbox"}, args...)
	command = append(command, "--api-server", h.endpoint)
	code := executeCLI(t.Context(), command, out, errOut, nil)
	if (code == 0) != wantSuccess {
		t.Fatalf("public mailbox %v exit=%d out=%s err=%s", args, code, out.String(), errOut.String())
	}
	return out.String() + errOut.String()
}

type registeredChannelCard struct {
	CardID          string          `json:"card_id"`
	RunID           string          `json:"run_id"`
	Status          string          `json:"status"`
	Verdict         string          `json:"verdict"`
	Snapshot        json.RawMessage `json:"snapshot"`
	Fields          map[string]any  `json:"fields"`
	DecisionEventID string          `json:"decision_event_id"`
}

func readRegisteredChannelCard(t *testing.T, h *channelOnboardingE2EHarness, id string) registeredChannelCard {
	t.Helper()
	var result struct {
		Card registeredChannelCard `json:"decision_card"`
	}
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": id}, &result)
	if result.Card.CardID != id || result.Card.RunID == "" || len(result.Card.Snapshot) == 0 {
		t.Fatalf("public card lost its canonical identity: %+v", result.Card)
	}
	return result.Card
}

func decideRegisteredAnchorCLI(t *testing.T, h *channelOnboardingE2EHarness, cardID string, effect bool) {
	t.Helper()
	before := readRegisteredChannelCard(t, h, cardID)
	registeredChannelMailboxCLI(t, h, true, "view", cardID)
	args := []string{"decide", cardID, "approve", "--idempotency-key", "cli-" + cardID}
	registeredChannelMailboxCLI(t, h, true, args...)
	decided := readRegisteredChannelCard(t, h, cardID)
	if decided.Status != decisioncard.StatusDecided || decided.Verdict != "approve" || decided.DecisionEventID == "" || string(decided.Snapshot) != string(before.Snapshot) {
		t.Fatalf("CLI changed the frozen anchor or failed to decide: %+v", decided)
	}
	registeredChannelMailboxCLI(t, h, true, args...)
	if replay := readRegisteredChannelCard(t, h, cardID); replay.DecisionEventID != decided.DecisionEventID {
		t.Fatal("replayed CLI decision emitted a different completion")
	}
	if !effect {
		return
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		var readback struct {
			Effect decisioncard.ProposedEffectReadback `json:"effect"`
		}
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": cardID}, &readback)
		if readback.Effect.DispatchState == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("CLI-approved effect did not settle: %+v", readback)
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
		t.Fatalf("CLI-approved provider effect count=%d, want one", count)
	}
}

func runRegisteredCLIProcessRestart(t *testing.T, h *channelOnboardingE2EHarness, hash, cut string) {
	t.Helper()
	h.stop(t)
	t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
	first := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
	h.endpoint = first.endpoint(t)
	seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
		"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true}, "idempotency_key": "cli-crash-seed",
	})
	card := waitChannelPublicCard(t, h, seed.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
	before := readRegisteredChannelCard(t, h, card)
	if cut == "settled" {
		decideRegisteredAnchorCLI(t, h, card, false)
	}
	committed := readRegisteredChannelCard(t, h, card)
	if err := first.kill(); err != nil {
		t.Fatal(err)
	}
	if first.waitError() == nil {
		t.Fatal("forced process death reported a graceful exit")
	}
	second := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
	h.endpoint = second.endpoint(t)
	after := readRegisteredChannelCard(t, h, card)
	if after.Status != committed.Status || after.DecisionEventID != committed.DecisionEventID || string(after.Snapshot) != string(before.Snapshot) || after.RunID != seed.RunID {
		t.Fatal("process death changed pending/committed CLI card identity")
	}
	decideRegisteredAnchorCLI(t, h, card, false)
	if cut == "settled" && readRegisteredChannelCard(t, h, card).DecisionEventID != committed.DecisionEventID {
		t.Fatal("crash replay duplicated committed decision")
	}
	if err := second.stop(); err != nil {
		t.Fatal(err)
	}
	// Native-send/ACK in-flight barriers remain in the original roots, deferred
	// by 6526882297; this path proves only canonical CLI crash/replay behavior.
}

func runRegisteredChannelCLIInbox(t *testing.T, h *channelOnboardingE2EHarness, scenario string, retained bool) {
	t.Helper()
	var cards []decisioncard.ListItem
	deadline := time.Now().Add(15 * time.Second)
	for {
		var page struct {
			Items []struct {
				Kind string                `json:"kind"`
				Card decisioncard.ListItem `json:"decision_card"`
			} `json:"items"`
		}
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.list", map[string]any{"status": "pending", "limit": 200}, &page)
		cards = nil
		for _, item := range page.Items {
			if item.Kind == "decision_card" {
				cards = append(cards, item.Card)
			}
		}
		want := 1
		if scenario == "draft_chooser" || scenario == "quoted_two" {
			want = 2
		}
		if len(cards) == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("canonical CLI cards=%v, want %d", cards, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
	listing := registeredChannelMailboxCLI(t, h, true, "list", "--limit", "200")
	for _, card := range cards {
		if !strings.Contains(listing, card.CardID) {
			t.Fatalf("CLI omitted card %s: %s", card.CardID, listing)
		}
		registeredChannelMailboxCLI(t, h, true, "view", card.CardID)
	}
	card := readRegisteredChannelCard(t, h, cards[0].CardID)
	if retained || strings.Contains(scenario, "restart") {
		h.stop(t)
		h.start(t)
		after := readRegisteredChannelCard(t, h, card.CardID)
		if after.Status != card.Status || string(after.Snapshot) != string(card.Snapshot) || after.RunID != card.RunID {
			t.Fatal("restart changed a retained undecided card")
		}
	}
	callback, signing, _ := h.provider.Registration()
	// The deployment operator's conversation is never customer business input.
	operator := `{"update_id":88001,"message":{"message_id":88001,"from":{"id":7000},"chat":{"id":1001,"type":"private"},"text":"operator-only text"}}`
	status, body := postProviderAliasUpdate(t, callback, "", signing, operator)
	if status != http.StatusOK || len(body) != 0 {
		t.Fatalf("operator conversation leaked into business ingress: status=%d body=%s", status, body)
	}
	customer := `{"update_id":88002,"message":{"message_id":88002,"from":{"id":9001},"chat":{"id":9001,"type":"private"},"text":"ordinary business text"}}`
	status, body = postProviderAliasUpdate(t, callback, "", signing, customer)
	var admitted standingTelegramPublication
	if status != http.StatusAccepted || json.Unmarshal(body, &admitted) != nil || len(admitted.EventIDs) != 2 || admitted.RunID == "" {
		t.Fatalf("held-account customer ingress: status=%d body=%s", status, body)
	}
	status, body = postProviderAliasUpdate(t, callback, "", signing, customer)
	var replay standingTelegramPublication
	if status != http.StatusOK || json.Unmarshal(body, &replay) != nil || fmt.Sprint(replay.EventIDs) != fmt.Sprint(admitted.EventIDs) || replay.RunID != admitted.RunID {
		t.Fatalf("customer replay changed publication: status=%d body=%s", status, body)
	}
	inputs := []string{}
	switch scenario {
	case "required_input", "cancel_input", "bare_input", "draft_chooser", "quoted_two", "edit_loss_resend":
		registeredChannelMailboxCLI(t, h, false, "decide", card.CardID, "retire")
		inputs = []string{"--input", "reason=reviewed through CLI"}
	case "invalid_input":
		registeredChannelMailboxCLI(t, h, false, "decide", card.CardID, "retire", "--input", "reason=not-an-integer")
		inputs = []string{"--input", "reason=42"}
	case "ordered_input":
		registeredChannelMailboxCLI(t, h, false, "decide", card.CardID, "retire", "--input", "zeta=42")
		inputs = []string{"--input", "zeta=42", "--input", "alpha=true"}
	}
	before := readRegisteredChannelCard(t, h, card.CardID)
	if before.Status != card.Status || before.DecisionEventID != "" || string(before.Snapshot) != string(card.Snapshot) {
		t.Fatal("readback, customer traffic, or refused CLI input mutated the decision")
	}
	args := append([]string{"decide", card.CardID, "retire", "--idempotency-key", "registered-cli-decision"}, inputs...)
	registeredChannelMailboxCLI(t, h, true, args...)
	decided := readRegisteredChannelCard(t, h, card.CardID)
	if decided.Status != decisioncard.StatusDecided || decided.Verdict != "retire" || decided.DecisionEventID == "" || string(decided.Snapshot) != string(card.Snapshot) {
		t.Fatalf("CLI did not decide the exact frozen card: %+v", decided)
	}
	registeredChannelMailboxCLI(t, h, true, args...)
	replayed := readRegisteredChannelCard(t, h, card.CardID)
	if replayed.DecisionEventID != decided.DecisionEventID || fmt.Sprint(replayed.Fields) != fmt.Sprint(decided.Fields) {
		t.Fatal("CLI decision replay changed the committed outcome")
	}
	for _, sibling := range cards[1:] {
		if got := readRegisteredChannelCard(t, h, sibling.CardID); got.Status != decisioncard.StatusPending || got.DecisionEventID != "" {
			t.Fatalf("decision escaped to sibling card: %+v", got)
		}
	}
	if retained || strings.Contains(scenario, "restart") {
		h.stop(t)
		h.start(t)
		if got := readRegisteredChannelCard(t, h, card.CardID); got.DecisionEventID != decided.DecisionEventID || got.Verdict != decided.Verdict {
			t.Fatal("restart changed the committed CLI decision")
		}
	}
	if writes := h.provider.CommandWrites(); len(writes) != 0 {
		t.Fatal("CLI-only registration installed channel-card commands", writes)
	}
}
