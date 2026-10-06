package serveapp

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

type objectChannelResponsePause struct {
	arrived chan struct{}
	release chan struct{}
}

func (p *objectChannelProvider) pauseBusinessResponse(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	pause := &objectChannelResponsePause{arrived: make(chan struct{}), release: make(chan struct{})}
	p.mu.Lock()
	if p.businessResponse != nil {
		p.mu.Unlock()
		t.Fatal("business response is already paused")
	}
	p.businessResponse = pause
	p.mu.Unlock()
	var once sync.Once
	release := func() { once.Do(func() { close(pause.release) }) }
	t.Cleanup(release)
	return pause.arrived, release
}

func TestChannelLearnedObjectRealAnchorProducersPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h, db, p, _, hash := startObjectChannelJourney(t, backend, canonicalrouting.CopyChannelLearnedObjectAnchorJourney(t), channelAnchorLLMRuntime{}, "object-provider-secret")
			for index, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect} {
				t.Run(string(kind), func(t *testing.T) {
					flowInstance := "reviews"
					if kind == decisioncard.AnchorKindHumanTask {
						flowInstance = "observers"
					}
					seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
						"event_name": "work.requested", "bundle_hash": hash, "payload": map[string]any{"seed": true},
						"idempotency_key": "object-anchor-" + string(kind),
					})
					waitObjectCardReceipt(t, db, waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate, "reviews"))
					if kind != decisioncard.AnchorKindStageGate {
						event := "observer.requested"
						if kind == decisioncard.AnchorKindProposedEffect {
							event = "effect.requested"
						}
						requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
							"event_name": event, "run_id": seed.RunID, "source_event_id": seed.EventID,
							"payload": map[string]any{"seed": true}, "idempotency_key": "object-producer-" + string(kind),
						})
					}
					card := waitChannelAnchorCard(t, db, seed.RunID, kind, flowInstance)
					receipt := waitObjectCardReceipt(t, db, card)
					label := "Approve"
					if kind == decisioncard.AnchorKindStageGate {
						label = "approve"
					}
					approve := objectAnchorChoice(t, p, receipt, label, index)
					var arrived <-chan struct{}
					var release func()
					if kind == decisioncard.AnchorKindProposedEffect {
						arrived, release = p.pauseBusinessResponse(t)
					}
					occurrence := fmt.Sprintf("anchor-approve-%d", index)
					postObjectInputAction(t, p, occurrence, receipt, approve)
					waitObjectChannelDisposition(t, db, "operator_channel_action_intents", occurrence, "applied")
					waitChannelAnchorDecision(t, db, card)
					if arrived != nil {
						select {
						case <-arrived:
						case <-time.After(15 * time.Second):
							t.Fatal("approved effect never reached the independent mock provider")
						}
						waitObjectAnchorDispatch(t, h, card, false)
						release()
						waitObjectAnchorDispatch(t, h, card, true)
						waitObjectMessageText(t, p, receipt, "Dispatch: succeeded")
					}
					waitObjectTerminalReceipt(t, db, p, card, receipt, "Decision: approve")
					proveObjectRejectedControl(t, db, p, receipt, approve, fmt.Sprintf("anchor-stale-%d", index))
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, seed.RunID).Scan(&count); err != nil || count != 1 {
						t.Fatalf("duplicate/stale %s completion count=%d: %v", kind, count, err)
					}
				})
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			business := 0
			for _, operation := range []string{"/v2/deliver", "/v2/edit"} {
				for _, input := range p.calls[operation] {
					if !objectChannelPresentationValid(input, false) {
						t.Fatalf("actual %s exceeded the independent compiled bounds: %#v", operation, input)
					}
					if operation == "/v2/deliver" && input["queue"] == "queue-b" && input["body"] == "approved-effect" {
						business++
					}
				}
			}
			if business != 1 {
				t.Fatalf("business effect dispatched %d times, want exactly once", business)
			}
		})
	}
}

func proveObjectRejectedControl(t *testing.T, db *sql.DB, p *objectChannelProvider, receipt, token, occurrence string) {
	t.Helper()
	p.mu.Lock()
	acknowledgments := len(p.calls["/v2/ack"])
	p.mu.Unlock()
	fact := map[string]any{"token": token, "cursor": occurrence, "message": map[string]any{"id": receipt}}
	postObjectChannelFact(t, p, occurrence, fact)
	postObjectChannelFact(t, p, occurrence, fact)
	waitObjectChannelDisposition(t, db, "operator_channel_action_intents", occurrence, "rejected")
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.calls["/v2/ack"]) != acknowledgments {
		t.Fatal("rejected historical control acquired provider acknowledgment authority")
	}
}

func objectAnchorChoice(t *testing.T, p *objectChannelProvider, receipt, label string, index int) string {
	t.Helper()
	for page := 0; page < 8; page++ {
		p.mu.Lock()
		input := p.message(receipt)
		controls, _ := input["controls"].([]any)
		var choice, more string
		for _, control := range controls {
			row, _ := control.(map[string]any)
			if row["name"] == label {
				choice, _ = row["value"].(string)
			}
			if row["name"] == "More choices" {
				more, _ = row["value"].(string)
			}
		}
		p.mu.Unlock()
		if choice != "" {
			return choice
		}
		if more == "" {
			t.Fatalf("actual anchor message lacks %s and navigation: %#v", label, input)
		}
		postObjectInputAction(t, p, fmt.Sprintf("anchor-nav-%d-%d", index, page), receipt, more)
		deadline := time.Now().Add(15 * time.Second)
		for {
			p.mu.Lock()
			current, _ := p.message(receipt)["controls"].([]any)
			unchanged := false
			for _, control := range current {
				row, _ := control.(map[string]any)
				unchanged = unchanged || row["value"] == more
			}
			p.mu.Unlock()
			if !unchanged {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("anchor navigation did not advance its exact message")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Fatalf("%s is not reachable through the finite anchor pages", label)
	return ""
}

func waitObjectAnchorDispatch(t *testing.T, h *channelOnboardingE2EHarness, card string, succeeded bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var result struct {
			Card struct {
				Verdict string `json:"verdict"`
			} `json:"decision_card"`
			Effect decisioncard.ProposedEffectReadback `json:"effect"`
		}
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.get", map[string]any{"mailbox_id": card}, &result)
		if result.Card.Verdict != "approve" {
			t.Fatalf("real proposal lacks committed approval: %+v", result)
		}
		if !succeeded {
			if result.Effect.DispatchState == "" || result.Effect.DispatchState == "succeeded" {
				t.Fatalf("approval falsely implies successful dispatch while response is held: %+v", result)
			}
			return
		}
		if result.Effect.DispatchState == "succeeded" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("approved mock operation did not settle: %+v", result)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitObjectTerminalReceipt(t *testing.T, db *sql.DB, p *objectChannelProvider, card, receipt, fragment string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		p.mu.Lock()
		input := p.message(receipt)
		controls, _ := input["controls"].([]any)
		terminal := len(controls) == 0 && strings.Contains(fmt.Sprint(input["body"]), fragment)
		p.mu.Unlock()
		var settled int
		err := db.QueryRow(`SELECT COUNT(*) FROM channel_delivery_plans p JOIN channel_delivery_receipts r ON r.effect_operation_id=p.current_receipt_operation_id
			WHERE p.source_id=$1 AND p.state='sent' AND r.state='sent' AND r.render_id=p.current_render_id`, card).Scan(&settled)
		if err != nil {
			t.Fatal(err)
		}
		if terminal && settled == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("terminal object receipt was not durably reconciled: settled=%d %#v", settled, input)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
