package channeldelivery

import (
	"bytes"
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
	frozen, err := FreezeCard(card, 17, "", audience)
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
	repeated, err := FreezeCard(card, 17, "", audience)
	if err != nil || !bytes.Equal(frozen.Input, repeated.Input) || frozen.Hash != repeated.Hash {
		t.Fatalf("repeated freeze changed: %#v, %v", repeated, err)
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
	decided, err := FreezeCard(card, 18, "", audience)
	if err != nil {
		t.Fatal(err)
	}
	if len(decided.Choices) != 0 || !strings.Contains(decided.FullText, "Decision: revise") ||
		!strings.Contains(decided.FullText, "Actor: "+principalID) ||
		strings.Contains(decided.FullText, "private-answer") || decided.Hash == frozen.Hash {
		t.Fatalf("decided render = %#v", decided)
	}
	if _, err := FreezeCard(card, 18, "sent", audience); err == nil {
		t.Fatal("non-effect card accepted dispatch state")
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
