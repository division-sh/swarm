package channeldelivery

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestChannelRenderFreezesFullOrderedCardWithoutPrivateAnswerEcho(t *testing.T) {
	principalID := uuid.NewString()
	audience := Audience{
		PrincipalID: principalID, InterfaceKey: "mock-channel", DeliveryEpoch: 1,
		ExternalAccountRef: "account", ConversationRef: "group", ConversationScope: operatorchannel.ConversationScopeShared,
	}
	entityID := uuid.NewString()
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		Route: runtimeflowidentity.RouteForInstancePath("root"), FlowID: ".", EntityID: entityID,
		Stage: "awaiting_review", StageActivationID: uuid.NewString(), Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("launch_review", "Launch review", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"accept": {Verdict: "accept"},
		"revise": {Verdict: "revise", Input: map[string]runtimecontracts.WorkflowGateInputField{
			"zeta": {Type: "text", Required: true}, "alpha": {Type: "text", Required: true},
		}, InputOrder: []string{"zeta", "alpha"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: uuid.NewString(), Anchor: anchor, Snapshot: snapshot,
		ExecutionMode: "live", BundleHash: "sha256:example", WorkflowVersion: "1", CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := FreezeCard(card, 17, "", audience, DraftPrompt{})
	if err != nil {
		t.Fatal(err)
	}
	if frozen.Hash != canonicaljson.HashBytes(frozen.Input) || frozen.Revision != 17 ||
		!strings.Contains(frozen.FullText, "Gate: . / awaiting_review") ||
		strings.Index(frozen.FullText, "zeta: text") > strings.Index(frozen.FullText, "alpha: text") ||
		len(frozen.Choices) != 2 || frozen.Choices[1].Verdict != "revise" ||
		frozen.Choices[1].Fields[0].Name != "zeta" || frozen.Choices[1].Fields[1].Name != "alpha" {
		t.Fatalf("frozen card = %#v", frozen)
	}
	repeated, err := FreezeCard(card, 17, "", audience, DraftPrompt{})
	if err != nil || !bytes.Equal(frozen.Input, repeated.Input) || frozen.Hash != repeated.Hash {
		t.Fatalf("repeated freeze changed: %#v, %v", repeated, err)
	}
	prompt := DraftPrompt{DraftID: uuid.NewString(), Verdict: "revise", NextFieldIndex: 1, ExpiresAt: now.Add(15 * time.Minute)}
	prompted, err := FreezeCard(card, 18, "", audience, prompt)
	if err != nil || !strings.Contains(prompted.FullText, "Input: alpha (text) required") ||
		strings.Contains(prompted.FullText, "private-answer") || prompted.Hash == frozen.Hash {
		t.Fatalf("private-answer-free ordered prompt = %#v, %v", prompted, err)
	}
	identity := operatorchannel.InterfaceIdentity{InterfaceRef: operatorchannel.InterfaceHITLChannelV2,
		ChannelPackID: "provider.mock.hitl_channel", ChannelPackVersion: "1", ChannelManifestHash: "sha256:mock",
		SemanticGeneration: "mock-generation"}.Normalized()
	action := operatorchannel.InboundAction{ActionFact: operatorchannel.ActionFact{
		Interface: identity, ExternalAccountRef: "account", ConversationRef: "group",
		ConversationScope: operatorchannel.ConversationScopeShared, MessageReference: `{"id":91}`,
		InteractionRef: "callback-91", Token: uuid.NewString(),
	}, Provider: "mock", ProviderEventID: "event-91", PublicationID: uuid.NewString(), ProviderAuthorization: "verified"}
	pending := PendingAction{PublicationID: action.PublicationID, Fact: action, ReceivedAt: now.Add(time.Second)}
	resolved := ResolvedAction{Action: Action{Token: action.Token, Kind: "verdict", Verdict: "accept"},
		SourceKind: "card", SourceID: card.CardID, PrincipalID: principalID, ReceiptOperationID: uuid.NewString(),
		RenderHash: frozen.Hash, CurrentRender: true}
	prepared, err := PrepareCardAction(pending, resolved, card)
	if err != nil || prepared.Request.Method != "mailbox.decide" || prepared.Mutation.Kind() != runtimepipeline.DecisionCardMutationDecide ||
		prepared.Request.IdempotencyKey != action.PublicationID {
		t.Fatalf("direct verdict action = %#v, err=%v", prepared, err)
	}
	repeatedAction, err := PrepareCardAction(pending, resolved, card)
	if err != nil || prepared.Request.RequestHash != repeatedAction.Request.RequestHash ||
		!prepared.Mutation.SameRequest(repeatedAction.Mutation) {
		t.Fatalf("callback retry changed request: %#v, err=%v", repeatedAction, err)
	}
	resolved.Action.Verdict = "revise"
	prepared, err = PrepareCardAction(pending, resolved, card)
	if err != nil || prepared.Request.Method != "mailbox.begin_input" || prepared.Mutation.Kind() != runtimepipeline.DecisionCardMutationBeginInput {
		t.Fatalf("required-input action = %#v, err=%v", prepared, err)
	}
	stale := resolved
	stale.CurrentRender = false
	if _, err := PrepareCardAction(pending, stale, card); err == nil {
		t.Fatal("stale render prepared a card mutation")
	}
	foreign := resolved
	foreign.SourceID = uuid.NewString()
	if _, err := PrepareCardAction(pending, foreign, card); err == nil {
		t.Fatal("foreign card prepared a mutation")
	}
	fields, err := canonicaljson.FromGo(map[string]any{"zeta": "private-answer-one", "alpha": "private-answer-two"})
	if err != nil {
		t.Fatal(err)
	}
	card.Status, card.Verdict, card.Fields = decisioncard.StatusDecided, "revise", fields
	card.DecidedBy, card.DecidedAt, card.DecisionEventID = principalID, now.Add(time.Minute), uuid.NewString()
	if _, err := PrepareCardAction(pending, resolved, card); err == nil {
		t.Fatal("terminal card prepared a new mutation")
	}
	decided, err := FreezeCard(card, 18, "", audience, DraftPrompt{})
	if err != nil {
		t.Fatal(err)
	}
	if len(decided.Choices) != 0 || !strings.Contains(decided.FullText, "Decision: revise") ||
		!strings.Contains(decided.FullText, "Actor: "+principalID) ||
		strings.Contains(decided.FullText, "private-answer") || decided.Hash == frozen.Hash {
		t.Fatalf("decided render = %#v", decided)
	}
	if _, err := FreezeCard(card, 18, "sent", audience, DraftPrompt{}); err == nil {
		t.Fatal("non-effect card accepted dispatch state")
	}
	if _, err := FreezeCard(card, 18, "", audience, prompt); err == nil {
		t.Fatal("terminal card accepted active input prompt")
	}
}

func TestChannelRenderPreservesEachCanonicalCardAnchor(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "mock-channel", DeliveryEpoch: 1,
		ExternalAccountRef: "account", ConversationRef: "direct", ConversationScope: operatorchannel.ConversationScopeDirect}
	runID, entityID := uuid.NewString(), uuid.NewString()
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	operationID, err := decisioncard.NewHumanTaskOperationID(runID, "request-help")
	if err != nil {
		t.Fatal(err)
	}
	stage, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		Route: runtimeflowidentity.RouteForInstancePath("root"), FlowID: ".", EntityID: entityID,
		Stage: "awaiting_review", StageActivationID: uuid.NewString(), Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	human, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "assistant", OperationID: operationID, Category: "approval",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeGlobal}, Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	effect, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: uuid.NewString(), ActivityID: "provision", Decision: "approve",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeGlobal}, Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("review", "Review request",
		map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"accept": {Verdict: "accept"},
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, label, dispatch, effectHash string
		anchor                            decisioncard.Anchor
	}{
		{name: "stage_gate", label: "Gate: . / awaiting_review", anchor: stage},
		{name: "human_task", label: "Task: assistant / approval", anchor: human},
		{name: "proposed_effect", label: "Effect: provision", anchor: effect, dispatch: "pending", effectHash: "sha256:effect"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			card, err := decisioncard.New(decisioncard.Card{
				CardID: uuid.NewString(), RunID: runID, Anchor: tc.anchor, Snapshot: snapshot,
				ExecutionMode: "live", BundleHash: "sha256:example", WorkflowVersion: "1",
				EffectContentHash: tc.effectHash, CreatedAt: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC),
			})
			if err != nil {
				t.Fatal(err)
			}
			frozen, err := FreezeCard(card, 1, tc.dispatch, audience, DraftPrompt{})
			if err != nil || !strings.Contains(frozen.FullText, tc.label) ||
				len(frozen.Choices) != 1 || frozen.Choices[0].Verdict != "accept" {
				t.Fatalf("frozen %s card = %#v, %v", tc.name, frozen, err)
			}
			decoded, err := Decode(frozen.Input, frozen.Hash)
			if err != nil || decoded.FullText != frozen.FullText || decoded.Audience != audience {
				t.Fatalf("readback %s card = %#v, %v", tc.name, decoded, err)
			}
		})
	}
}

func TestChannelResponseFreezesRequestedAudienceAndContent(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "mock-channel", DeliveryEpoch: 3,
		ExternalAccountRef: "account", ConversationRef: "shared", ConversationScope: operatorchannel.ConversationScopeShared}
	publicationID := uuid.NewString()
	frozen, err := FreezeResponse(publicationID, "Inbox\nUnread notices: 2", audience)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(frozen.Input, frozen.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SourceKind != "response" || decoded.SourceID != publicationID || decoded.Audience != audience ||
		decoded.FullText != "Inbox\nUnread notices: 2" || len(decoded.Choices) != 0 {
		t.Fatalf("frozen response changed on readback: %#v", decoded)
	}
	if _, err := FreezeResponse(publicationID, " ", audience); err == nil {
		t.Fatal("empty channel response was admitted")
	}
}

func TestChannelRecoveryInboxPagesRetainExactChoices(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "mock-channel", DeliveryEpoch: 3,
		ExternalAccountRef: "account", ConversationRef: "direct", ConversationScope: operatorchannel.ConversationScopeDirect}
	choices := make([]RecoveryChoice, 16)
	for index := range choices {
		choices[index] = RecoveryChoice{DeliveryID: uuid.NewString(), Label: fmt.Sprintf("Resend card %d", index)}
	}
	for pageIndex, wantCount := range []int{7, 7, 2} {
		frozen, err := FreezeRecoveryInbox(uuid.NewString(), "Inbox", choices, pageIndex, audience)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(frozen.Input, frozen.Hash)
		if err != nil || decoded.Recovery == nil || decoded.Recovery.PageIndex != pageIndex ||
			len(decoded.RecoveryChoices) != len(choices) || decoded.Audience != audience ||
			!strings.Contains(decoded.FullText, "The first message may have arrived") {
			t.Fatalf("recovery page %d changed on readback: %#v, %v", pageIndex, decoded, err)
		}
		if got := strings.Count(decoded.FullText, "- Resend card "); got != wantCount {
			t.Fatalf("recovery page %d has %d visible choices, want %d", pageIndex, got, wantCount)
		}
	}
	if _, err := FreezeRecoveryInbox(uuid.NewString(), "Inbox", choices, 3, audience); err == nil {
		t.Fatal("out-of-range recovery page was admitted")
	}
}

func TestChannelViewFullPagesRetainExactFrozenUnicode(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "mock-channel", DeliveryEpoch: 3,
		ExternalAccountRef: "account", ConversationRef: "shared", ConversationScope: operatorchannel.ConversationScopeShared}
	fullText := strings.Repeat("α", 7100)
	source, err := FreezeResponse(uuid.NewString(), fullText, audience)
	if err != nil {
		t.Fatal(err)
	}
	sourceRenderID := uuid.NewString()
	var reconstructed strings.Builder
	for index := 0; ; index++ {
		page, err := FreezeResponsePage(uuid.NewString(), source, sourceRenderID, index, audience)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(page.Input, page.Hash)
		if err != nil || decoded.Page == nil || decoded.Page.Index != index || decoded.Page.SourceRenderID != sourceRenderID ||
			decoded.Page.SourceRenderHash != source.Hash {
			t.Fatalf("frozen page %d changed on readback: %#v, %v", index, decoded, err)
		}
		presentation, truncated, err := PresentationText(decoded)
		if err != nil || truncated || len([]rune(presentation)) > ChannelExcerptRunes {
			t.Fatalf("page %d exceeds provider presentation bound: %d, truncated=%t, err=%v", index, len([]rune(presentation)), truncated, err)
		}
		parts := strings.SplitN(page.FullText, "\n", 2)
		if len(parts) != 2 {
			t.Fatalf("page %d lacks its heading", index)
		}
		reconstructed.WriteString(parts[1])
		if index+1 == page.Page.Count {
			break
		}
	}
	if reconstructed.String() != fullText {
		t.Fatal("page concatenation changed the complete frozen text")
	}
	if _, err := FreezeResponsePage(uuid.NewString(), source, sourceRenderID, 100, audience); err == nil {
		t.Fatal("out-of-range page was admitted")
	}
}

func TestChannelRenderNoticeIsRunlessAndHasNoDecisionActions(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "mock-channel", DeliveryEpoch: 1,
		ExternalAccountRef: "account", ConversationRef: "direct", ConversationScope: operatorchannel.ConversationScopeDirect}
	notice := Notice{ID: uuid.NewString(), Type: "notify_human", Summary: "Budget is low", Priority: "critical", Context: []byte(`{"remaining":2}`)}
	frozen, err := FreezeNotice(notice, audience)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(frozen.FullText, "Scope: global") || strings.Contains(frozen.FullText, "Run:") ||
		len(frozen.Choices) != 0 || frozen.Hash != canonicaljson.HashBytes(frozen.Input) {
		t.Fatalf("global notice render = %#v", frozen)
	}
	summary, err := FreezeSummary(uuid.NewString(), 2, audience)
	if err != nil || !strings.Contains(summary.FullText, "2 earlier notices") ||
		!strings.Contains(summary.FullText, "Open inbox from the chat menu") ||
		strings.Contains(summary.FullText, "swarm mailbox") || len(summary.Choices) != 0 {
		t.Fatalf("phone backlog summary = %#v, %v", summary, err)
	}
}

func TestChannelRenderExcerptPreservesFullHashAndTail(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "mock-channel", DeliveryEpoch: 1,
		ExternalAccountRef: "account", ConversationRef: "direct", ConversationScope: operatorchannel.ConversationScopeDirect}
	frozen, err := FreezeNotice(Notice{ID: uuid.NewString(), Type: "notify_human", Summary: "Long notice",
		Priority: "normal", Context: []byte(`{"detail":"` + strings.Repeat("a", 4000) + `TAIL"}`)}, audience)
	if err != nil {
		t.Fatal(err)
	}
	excerpt, truncated, err := PresentationText(frozen)
	if err != nil || !truncated || len([]rune(excerpt)) > ChannelExcerptRunes ||
		!strings.Contains(excerpt, "TAIL") || strings.Contains(excerpt, strings.Repeat("a", 4000)) ||
		frozen.Hash != canonicaljson.HashBytes(frozen.Input) {
		t.Fatalf("excerpt = %q, truncated=%t err=%v", excerpt, truncated, err)
	}
}
