package channeldelivery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

const ProjectionVersion = "channel-render-v3"

type ActionPage struct {
	Index    int `json:"index"`
	Capacity int `json:"capacity"`
}

type Audience struct {
	PrincipalID        string
	InterfaceKey       string
	DeliveryEpoch      int64
	ExternalAccountRef string
	ConversationRef    string
	ConversationScope  operatorchannel.ConversationScope
}

func (a Audience) Validate() error {
	if uuid.Validate(a.PrincipalID) != nil || a.InterfaceKey == "" || a.DeliveryEpoch < 1 ||
		a.ExternalAccountRef == "" || a.ConversationRef == "" || !a.ConversationScope.Valid() {
		return fmt.Errorf("channel render audience is incomplete")
	}
	return nil
}

type Choice struct {
	Verdict string
	Label   string
	Fields  []Field
}

type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type Frozen struct {
	SourceKind         string
	SourceID           string
	Revision           int64
	NoticeAcknowledged bool
	Audience           Audience
	Input              json.RawMessage
	Hash               string
	FullText           string
	Choices            []Choice
	ActionPage         *ActionPage
	Bounds             packs.PresentationBounds
	Prompt             *DraftPrompt
	DraftChoices       []DraftChoice
	DraftChooser       *DraftChooser
	Recovery           *RecoveryPage
	RecoveryChoices    []RecoveryChoice
	Page               *ResponsePage
	InputCard          *InputCardPrompt
}

type InputCardPrompt struct {
	CardID                   string `json:"card_id"`
	CardContentHash          string `json:"card_content_hash"`
	ParentReceiptOperationID string `json:"parent_receipt_operation_id"`
}

type RecoveryChoice struct {
	DeliveryID string `json:"delivery_id"`
	Label      string `json:"label"`
}

type RecoveryPage struct {
	BaseText string `json:"base_text"`
}

type DraftChooser struct {
	TextPublicationID string `json:"text_publication_id"`
}

type DraftChoice struct {
	DraftID string `json:"draft_id"`
	CardID  string `json:"card_id"`
	Label   string `json:"label"`
}

type ResponsePage struct {
	SourceRenderID   string `json:"source_render_id"`
	SourceRenderHash string `json:"source_render_hash"`
	Index            int    `json:"index"`
	Count            int    `json:"count"`
}

func (p ResponsePage) Valid() bool {
	return uuid.Validate(p.SourceRenderID) == nil && p.SourceRenderHash != "" &&
		p.Index >= 0 && p.Count > 0 && p.Count <= 1000 && p.Index < p.Count
}

func Decode(raw []byte, hash string) (Frozen, error) {
	var wire struct {
		SourceKind      string           `json:"source_kind"`
		SourceID        string           `json:"source_id"`
		SourceRevision  int64            `json:"source_revision"`
		Acknowledged    bool             `json:"acknowledged"`
		FullText        string           `json:"full_text"`
		Page            *ResponsePage    `json:"page"`
		DraftPrompt     *DraftPrompt     `json:"draft_prompt"`
		InputCard       *InputCardPrompt `json:"input_card_prompt"`
		DraftChoices    []DraftChoice    `json:"draft_choices"`
		DraftChooser    *DraftChooser    `json:"draft_chooser"`
		Recovery        *RecoveryPage    `json:"recovery_page"`
		RecoveryChoices []RecoveryChoice `json:"recovery_choices"`
		Choices         []struct {
			Verdict string  `json:"verdict"`
			Label   string  `json:"label"`
			Fields  []Field `json:"fields"`
		} `json:"choices"`
		ActionPage *ActionPage              `json:"action_page"`
		Bounds     packs.PresentationBounds `json:"presentation_bounds"`
		Audience   struct {
			PrincipalID        string                            `json:"principal_id"`
			InterfaceKey       string                            `json:"interface_key"`
			DeliveryEpoch      int64                             `json:"delivery_epoch"`
			ExternalAccountRef string                            `json:"external_account_reference"`
			ConversationRef    string                            `json:"conversation_reference"`
			ConversationScope  operatorchannel.ConversationScope `json:"conversation_scope"`
		} `json:"audience"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return Frozen{}, err
	}
	choices := make([]Choice, 0, len(wire.Choices))
	for _, choice := range wire.Choices {
		choices = append(choices, Choice{Verdict: choice.Verdict, Label: choice.Label, Fields: choice.Fields})
	}
	frozen := Frozen{
		SourceKind: wire.SourceKind, SourceID: wire.SourceID, Revision: wire.SourceRevision,
		NoticeAcknowledged: wire.Acknowledged,
		Audience: Audience{PrincipalID: wire.Audience.PrincipalID, InterfaceKey: wire.Audience.InterfaceKey,
			DeliveryEpoch: wire.Audience.DeliveryEpoch, ExternalAccountRef: wire.Audience.ExternalAccountRef,
			ConversationRef: wire.Audience.ConversationRef, ConversationScope: wire.Audience.ConversationScope},
		Input: append(json.RawMessage(nil), raw...), Hash: hash, FullText: wire.FullText, Choices: choices,
		Prompt: wire.DraftPrompt, DraftChoices: wire.DraftChoices, DraftChooser: wire.DraftChooser,
		InputCard: wire.InputCard,
		Recovery:  wire.Recovery, RecoveryChoices: wire.RecoveryChoices, Page: wire.Page, ActionPage: wire.ActionPage, Bounds: wire.Bounds,
	}
	if err := frozen.Validate(); err != nil {
		return Frozen{}, err
	}
	return frozen, nil
}

type frozenProjectionIndex struct {
	ProjectionVersion string `json:"projection_version"`
	SourceKind        string `json:"source_kind"`
	SourceID          string `json:"source_id"`
	SourceRevision    int64  `json:"source_revision"`
	Acknowledged      bool   `json:"acknowledged"`
	Audience          struct {
		PrincipalID        string `json:"principal_id"`
		InterfaceKey       string `json:"interface_key"`
		DeliveryEpoch      int64  `json:"delivery_epoch"`
		ExternalAccountRef string `json:"external_account_reference"`
		ConversationRef    string `json:"conversation_reference"`
		ConversationScope  string `json:"conversation_scope"`
	} `json:"audience"`
	FullText        string                   `json:"full_text"`
	Page            *ResponsePage            `json:"page"`
	Prompt          *DraftPrompt             `json:"draft_prompt"`
	InputCard       *InputCardPrompt         `json:"input_card_prompt"`
	Drafts          []DraftChoice            `json:"draft_choices"`
	Chooser         *DraftChooser            `json:"draft_chooser"`
	Recovery        *RecoveryPage            `json:"recovery_page"`
	RecoveryChoices []RecoveryChoice         `json:"recovery_choices"`
	ActionPage      *ActionPage              `json:"action_page"`
	Bounds          packs.PresentationBounds `json:"presentation_bounds"`
	Choices         []Choice                 `json:"choices"`
}

func (f Frozen) Validate() error {
	if err := f.Audience.Validate(); err != nil {
		return err
	}
	if uuid.Validate(f.SourceID) != nil || f.Revision < 1 || f.FullText == "" ||
		(f.SourceKind != "notice" && f.SourceKind != "card" && f.SourceKind != "summary" && f.SourceKind != "response") {
		return fmt.Errorf("channel render source is incomplete")
	}
	canonical, err := canonicaljson.Canonicalize(f.Input)
	if err != nil || !bytes.Equal(canonical, f.Input) || f.Hash != canonicaljson.HashBytes(f.Input) {
		return fmt.Errorf("channel render input/hash is not canonical")
	}
	var index frozenProjectionIndex
	if err := json.Unmarshal(f.Input, &index); err != nil {
		return err
	}
	for _, validate := range []func(frozenProjectionIndex) error{
		f.validateFrozenIdentity, f.validateFrozenPages, f.validateFrozenPrompt,
		f.validateFrozenChooser, f.validateFrozenRecovery,
	} {
		if err := validate(index); err != nil {
			return err
		}
	}
	return nil
}

func (f Frozen) validateFrozenIdentity(index frozenProjectionIndex) error {
	if index.ProjectionVersion != ProjectionVersion || index.SourceKind != f.SourceKind ||
		index.SourceID != f.SourceID || index.SourceRevision != f.Revision || index.FullText != f.FullText ||
		index.Acknowledged != f.NoticeAcknowledged || (f.NoticeAcknowledged && f.SourceKind != "notice") ||
		index.Audience.PrincipalID != f.Audience.PrincipalID || index.Audience.InterfaceKey != f.Audience.InterfaceKey ||
		index.Audience.DeliveryEpoch != f.Audience.DeliveryEpoch || index.Audience.ExternalAccountRef != f.Audience.ExternalAccountRef ||
		index.Audience.ConversationRef != f.Audience.ConversationRef ||
		index.Audience.ConversationScope != string(f.Audience.ConversationScope) {
		return fmt.Errorf("channel render projection index contradicts frozen input")
	}
	if len(f.Choices) != len(index.Choices) {
		return fmt.Errorf("channel controls contradict frozen input")
	}
	for i, choice := range f.Choices {
		stored := index.Choices[i]
		if choice.Verdict != stored.Verdict || choice.Label != stored.Label || len(choice.Fields) != len(stored.Fields) {
			return fmt.Errorf("channel control contradicts frozen input")
		}
		for j, field := range choice.Fields {
			if field != stored.Fields[j] {
				return fmt.Errorf("channel control field contradicts frozen input")
			}
		}
	}
	return nil
}

func (f Frozen) validateFrozenPages(index frozenProjectionIndex) error {
	if (f.Page == nil) != (index.Page == nil) {
		return fmt.Errorf("channel response page contradicts frozen input")
	}
	if (f.ActionPage == nil) != (index.ActionPage == nil) {
		return fmt.Errorf("channel action page contradicts frozen input")
	}
	if f.Bounds != index.Bounds || (f.ActionPage == nil && f.Bounds != (packs.PresentationBounds{})) {
		return fmt.Errorf("channel presentation bounds contradict frozen input")
	}
	if f.ActionPage != nil {
		page := f.ActionPage
		if *page != *index.ActionPage || page.Capacity != f.Bounds.Actions || page.Index < 0 {
			return fmt.Errorf("channel action page is invalid")
		}
		if f.Bounds != index.Bounds || f.Bounds.Validate() != nil {
			return fmt.Errorf("channel presentation bounds contradict frozen input")
		}
		_, err := actionPageCount(f, page.Capacity)
		if err != nil {
			return err
		}
	}
	if f.Page != nil && (f.SourceKind != "response" || !f.Page.Valid() || *f.Page != *index.Page) {
		return fmt.Errorf("channel response page is invalid")
	}
	return nil
}

func (f Frozen) validateFrozenPrompt(index frozenProjectionIndex) error {
	if (f.InputCard == nil) != (index.InputCard == nil) || f.InputCard != nil &&
		(f.SourceKind != "response" || *f.InputCard != *index.InputCard || f.Prompt == nil ||
			f.Page != nil || f.DraftChooser != nil || f.Recovery != nil || len(f.Choices) != 0 ||
			uuid.Validate(f.InputCard.CardID) != nil || f.InputCard.CardContentHash == "" ||
			uuid.Validate(f.InputCard.ParentReceiptOperationID) != nil) {
		return fmt.Errorf("channel input card prompt contradicts frozen input")
	}
	if (f.Prompt == nil) != (index.Prompt == nil) || f.Prompt != nil &&
		((f.SourceKind != "card" && f.InputCard == nil) || *f.Prompt != *index.Prompt || uuid.Validate(f.Prompt.DraftID) != nil ||
			f.Prompt.Verdict == "" || f.Prompt.NextFieldIndex < 0 || f.Prompt.ExpiresAt.IsZero()) {
		return fmt.Errorf("channel draft prompt contradicts frozen input")
	}
	return nil
}

func FreezeInputCardPrompt(publicationID string, card decisioncard.Card, prompt DraftPrompt, parentReceiptID string, audience Audience) (Frozen, error) {
	if err := card.Validate(); err != nil {
		return Frozen{}, err
	}
	if uuid.Validate(publicationID) != nil || uuid.Validate(prompt.DraftID) != nil ||
		card.Status != decisioncard.StatusPending || uuid.Validate(parentReceiptID) != nil {
		return Frozen{}, fmt.Errorf("input prompt requires an exact active card draft and parent receipt")
	}
	projection, line, err := cardDraftPrompt(card, prompt)
	if err != nil || projection == nil || line == "" {
		return Frozen{}, fmt.Errorf("input prompt requires a current requested field: %w", err)
	}
	text := "Input for " + card.Snapshot.Title + "\n" + line
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "response", "source_id": publicationID,
		"source_revision": int64(1), "audience": audienceProjection(audience), "full_text": text,
		"draft_prompt": projection, "input_card_prompt": InputCardPrompt{
			CardID: card.CardID, CardContentHash: card.CardContentHash, ParentReceiptOperationID: parentReceiptID},
	}
	return freeze(input, "response", publicationID, 1, audience, text)
}

func (f Frozen) validateFrozenChooser(index frozenProjectionIndex) error {
	if (f.DraftChooser == nil) != (index.Chooser == nil) ||
		len(f.DraftChoices) != len(index.Drafts) ||
		(f.DraftChooser == nil && len(f.DraftChoices) != 0) ||
		(f.DraftChooser != nil && (f.SourceKind != "response" || f.Page != nil || f.Prompt != nil ||
			uuid.Validate(f.DraftChooser.TextPublicationID) != nil || len(f.DraftChoices) < 2 ||
			*f.DraftChooser != *index.Chooser)) {
		return fmt.Errorf("channel draft chooser contradicts frozen input")
	}
	seenDrafts := make(map[string]bool, len(f.DraftChoices))
	for i, choice := range f.DraftChoices {
		if choice != index.Drafts[i] || uuid.Validate(choice.DraftID) != nil || uuid.Validate(choice.CardID) != nil ||
			strings.TrimSpace(choice.Label) == "" || seenDrafts[choice.DraftID] {
			return fmt.Errorf("channel draft chooser has invalid or duplicate choice")
		}
		seenDrafts[choice.DraftID] = true
	}
	return nil
}

func (f Frozen) validateFrozenRecovery(index frozenProjectionIndex) error {
	if (f.Recovery == nil) != (index.Recovery == nil) || len(f.RecoveryChoices) != len(index.RecoveryChoices) ||
		(f.Recovery == nil && len(f.RecoveryChoices) != 0) ||
		(f.Recovery != nil && (f.SourceKind != "response" || f.Page != nil || f.Prompt != nil || f.DraftChooser != nil ||
			len(f.RecoveryChoices) == 0 || strings.TrimSpace(f.Recovery.BaseText) == "" ||
			*f.Recovery != *index.Recovery)) {
		return fmt.Errorf("channel recovery page contradicts frozen input")
	}
	seenRecovery := make(map[string]bool, len(f.RecoveryChoices))
	for i, choice := range f.RecoveryChoices {
		if choice != index.RecoveryChoices[i] || uuid.Validate(choice.DeliveryID) != nil ||
			strings.TrimSpace(choice.Label) == "" || seenRecovery[choice.DeliveryID] {
			return fmt.Errorf("channel recovery page has invalid or duplicate choice")
		}
		seenRecovery[choice.DeliveryID] = true
	}
	return nil
}

func ActionPageCount(f Frozen) (int, error) {
	if err := f.Validate(); err != nil {
		return 0, err
	}
	if f.ActionPage == nil {
		return 0, fmt.Errorf("channel render has no selected action page")
	}
	return actionPageCount(f, f.ActionPage.Capacity)
}

func actionPageCount(f Frozen, capacity int) (int, error) {
	if capacity != f.Bounds.Actions {
		return 0, fmt.Errorf("channel control page capacity contradicts pinned bounds")
	}
	pages, err := projectedControlPages(f)
	return len(pages), err
}

func WithPresentation(f Frozen, bounds packs.PresentationBounds, index int) (Frozen, error) {
	if err := f.Validate(); err != nil {
		return Frozen{}, err
	}
	if err := bounds.Validate(); err != nil {
		return Frozen{}, err
	}
	if index < 0 || f.ActionPage != nil && f.Bounds != bounds {
		return Frozen{}, fmt.Errorf("presentation requires exact pinned bounds and page")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(f.Input, &raw); err != nil {
		return Frozen{}, err
	}
	projection := make(map[string]any, len(raw)+1)
	for key, value := range raw {
		projection[key] = value
	}
	projection["action_page"] = ActionPage{Index: index, Capacity: bounds.Actions}
	projection["presentation_bounds"] = bounds
	return freeze(projection, f.SourceKind, f.SourceID, f.Revision, f.Audience, f.FullText)
}

// Notice is the committed notice presentation projection. Mailbox persistence
// remains the notice owner; this value contains no acknowledgment authority.
type Notice struct {
	ID           string
	Acknowledged bool
	Type         string
	Summary      string
	Context      json.RawMessage
	Priority     string
	FromAgent    string
	EntityID     string
	FlowInstance string
}

type DraftPrompt struct {
	DraftID        string    `json:"draft_id"`
	Verdict        string    `json:"verdict"`
	NextFieldIndex int       `json:"next_field_index"`
	ExpiresAt      time.Time `json:"expires_at"`
	Optional       bool      `json:"optional"`
}

// FreezeCard accepts the exact canonical change sequence. Dispatch is an
// independent axis for proposed-effect cards, never inferred from a verdict.
func orderedCardChoices(card decisioncard.Card) ([]Choice, error) {
	choices := make([]Choice, 0, len(card.Snapshot.Outcomes))
	if card.Status == decisioncard.StatusPending {
		keys := make([]string, 0, len(card.Snapshot.Outcomes))
		for key := range card.Snapshot.Outcomes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			outcome := card.Snapshot.Outcomes[key]
			choice := Choice{Verdict: key, Label: outcome.Label}
			if strings.TrimSpace(choice.Label) == "" {
				choice.Label = key
			}
			for _, name := range outcome.InputOrder {
				field, ok := outcome.Input[name]
				if !ok {
					return nil, fmt.Errorf("card outcome %s has incomplete input order", key)
				}
				choice.Fields = append(choice.Fields, Field{Name: name, Type: field.Type, Required: field.Required})
			}
			if len(choice.Fields) != len(outcome.Input) {
				return nil, fmt.Errorf("card outcome %s has incomplete input order", key)
			}
			choices = append(choices, choice)
		}
	}
	return choices, nil
}

func cardDraftPrompt(card decisioncard.Card, prompt DraftPrompt) (any, string, error) {
	var promptInput any
	line := ""
	if prompt.DraftID != "" {
		if card.Status != decisioncard.StatusPending || uuid.Validate(prompt.DraftID) != nil || prompt.ExpiresAt.IsZero() {
			return nil, "", fmt.Errorf("channel draft prompt lacks current card identity")
		}
		outcome, found := card.Snapshot.Outcomes[prompt.Verdict]
		if !found || prompt.NextFieldIndex < 0 || prompt.NextFieldIndex > len(outcome.InputOrder) || len(outcome.InputOrder) == 0 {
			return nil, "", fmt.Errorf("channel draft prompt contradicts frozen outcome")
		}
		promptInput = map[string]any{
			"draft_id": prompt.DraftID, "verdict": prompt.Verdict,
			"next_field_index": prompt.NextFieldIndex, "expires_at": prompt.ExpiresAt.UTC(),
			"optional": prompt.Optional,
		}
		if prompt.NextFieldIndex == len(outcome.InputOrder) {
			line = "Input complete; decision pending"
		} else {
			name := outcome.InputOrder[prompt.NextFieldIndex]
			field, found := outcome.Input[name]
			if !found {
				return nil, "", fmt.Errorf("channel draft prompt field is absent")
			}
			label := strings.TrimSpace(field.Label)
			if label == "" {
				label = name
			}
			line = "Input: " + label + " (" + field.Type + ")"
			if field.Required {
				line += " required"
			} else {
				promptInput.(map[string]any)["optional"] = true
			}
		}
	}
	return promptInput, line, nil
}

func FreezeCard(card decisioncard.Card, revision int64, dispatchState string, audience Audience, prompt DraftPrompt) (Frozen, error) {
	if err := audience.Validate(); err != nil {
		return Frozen{}, err
	}
	if err := card.Validate(); err != nil {
		return Frozen{}, err
	}
	if revision < 1 {
		return Frozen{}, fmt.Errorf("channel card render requires canonical change sequence")
	}
	if card.Anchor.Kind() == decisioncard.AnchorKindProposedEffect {
		if strings.TrimSpace(dispatchState) == "" {
			return Frozen{}, fmt.Errorf("proposed-effect render requires independent dispatch state")
		}
	} else if dispatchState != "" {
		return Frozen{}, fmt.Errorf("non-effect card cannot carry dispatch state")
	}
	scope, err := card.Anchor.Scope()
	if err != nil {
		return Frozen{}, err
	}
	title := strings.TrimSpace(card.Snapshot.Title)
	if title == "" {
		title = strings.TrimSpace(card.Snapshot.Decision)
	}
	if title == "" {
		title = string(card.Anchor.Kind())
	}
	subtitle, err := cardSubtitle(card)
	if err != nil {
		return Frozen{}, err
	}
	contextJSON, err := canonicaljson.Encode(card.Snapshot.Context)
	if err != nil {
		return Frozen{}, err
	}
	choices, err := orderedCardChoices(card)
	if err != nil {
		return Frozen{}, err
	}
	lines := []string{title, subtitle, "Run: " + shortRunID(card.RunID), "Scope: " + string(scope.Kind)}
	if scope.FlowInstance != "" {
		lines = append(lines, "Flow instance: "+scope.FlowInstance)
	}
	if scope.EntityID != "" {
		lines = append(lines, "Entity: "+scope.EntityID)
	}
	lines = append(lines, "Context: "+string(contextJSON))
	lines = append(lines, cardDecisionLines(card)...)
	if dispatchState != "" {
		lines = append(lines, "Dispatch: "+dispatchState)
	}
	promptInput, promptLine, err := cardDraftPrompt(card, prompt)
	if err != nil {
		return Frozen{}, err
	}
	if promptLine != "" {
		lines = append(lines, promptLine)
	}
	for _, choice := range choices {
		line := "Action: " + choice.Label
		for _, field := range choice.Fields {
			line += " | " + field.Name + ": " + field.Type
			if field.Required {
				line += " (required)"
			}
		}
		lines = append(lines, line)
	}
	fullText := strings.Join(lines, "\n")
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "card", "source_id": card.CardID,
		"source_revision": revision, "audience": audienceProjection(audience),
		"card_content_hash": card.CardContentHash, "decision_schema_hash": card.DecisionSchemaHash,
		"status": card.Status, "anchor_kind": card.Anchor.Kind(), "scope": scope,
		"dispatch_state": dispatchState, "full_text": fullText, "choices": choicesProjection(choices),
	}
	if promptInput != nil {
		input["draft_prompt"] = promptInput
	}
	return freeze(input, "card", card.CardID, revision, audience, fullText)
}

// CardDecisionReadback uses the same canonical outcome projection as the full
// card, without copying private draft/answer values or granting controls.
func CardDecisionReadback(card decisioncard.Card) (string, error) {
	if err := card.Validate(); err != nil {
		return "", err
	}
	return strings.Join(cardDecisionLines(card), "; "), nil
}

func cardDecisionLines(card decisioncard.Card) []string {
	switch card.Status {
	case decisioncard.StatusPending:
		lines := []string{"Decision: pending"}
		if !card.DeferredUntil.IsZero() {
			lines = append(lines, "Deferred until: "+card.DeferredUntil.UTC().Format(time.RFC3339Nano))
		}
		return lines
	case decisioncard.StatusDecided:
		return []string{"Decision: " + card.Verdict, "Actor: " + card.DecidedBy,
			"At: " + card.DecidedAt.UTC().Format(time.RFC3339Nano)}
	case decisioncard.StatusSuperseded:
		return []string{"The flow moved on. No action was taken."}
	case decisioncard.StatusExpired:
		return []string{"The decision expired. No action was taken."}
	default:
		return nil
	}
}

func FreezeNotice(notice Notice, audience Audience) (Frozen, error) {
	if err := audience.Validate(); err != nil {
		return Frozen{}, err
	}
	if uuid.Validate(notice.ID) != nil || strings.TrimSpace(notice.Type) == "" || strings.TrimSpace(notice.Summary) == "" ||
		(notice.Priority != "normal" && notice.Priority != "urgent" && notice.Priority != "critical") {
		return Frozen{}, fmt.Errorf("channel notice render requires committed notice identity and summary")
	}
	context := notice.Context
	if len(context) == 0 {
		context = []byte("{}")
	}
	canonicalContext, err := canonicaljson.Canonicalize(context)
	if err != nil {
		return Frozen{}, fmt.Errorf("channel notice context: %w", err)
	}
	scope := "global"
	if notice.EntityID != "" {
		scope = "entity"
	} else if notice.FlowInstance != "" {
		scope = "flow"
	}
	lines := []string{"Notice: " + notice.Summary, "Scope: " + scope}
	if notice.FlowInstance != "" {
		lines = append(lines, "Flow instance: "+notice.FlowInstance)
	}
	if notice.EntityID != "" {
		lines = append(lines, "Entity: "+notice.EntityID)
	}
	if notice.FromAgent != "" {
		lines = append(lines, "From: "+notice.FromAgent)
	}
	lines = append(lines, "Priority: "+notice.Priority, "Context: "+string(canonicalContext))
	if notice.Acknowledged {
		lines = append(lines, "Acknowledged")
	}
	fullText := strings.Join(lines, "\n")
	revision := int64(1)
	if notice.Acknowledged {
		revision = 2
	}
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "notice", "source_id": notice.ID,
		"source_revision": revision, "audience": audienceProjection(audience), "item_type": notice.Type,
		"scope": scope, "acknowledged": notice.Acknowledged, "full_text": fullText,
	}
	return freeze(input, "notice", notice.ID, revision, audience, fullText)
}

func FreezeSummary(firstOperationID string, count int64, audience Audience) (Frozen, error) {
	if err := audience.Validate(); err != nil {
		return Frozen{}, err
	}
	if uuid.Validate(firstOperationID) != nil || count < 1 {
		return Frozen{}, fmt.Errorf("channel backlog summary requires first connection and positive count")
	}
	noun := "notices"
	if count == 1 {
		noun = "notice"
	}
	fullText := fmt.Sprintf("%d earlier %s waiting in your inbox. Send /inbox or reply with Open inbox to review them.", count, noun)
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "summary", "source_id": firstOperationID,
		"source_revision": 1, "audience": audienceProjection(audience), "summary_count": count,
		"full_text": fullText,
	}
	return freeze(input, "summary", firstOperationID, 1, audience, fullText)
}

// FreezeResponse records a requested readback, not a notice or card mutation.
// Its source identity is the verified inbound publication that requested it.
func FreezeResponse(publicationID, fullText string, audience Audience) (Frozen, error) {
	if err := audience.Validate(); err != nil {
		return Frozen{}, err
	}
	if uuid.Validate(publicationID) != nil || strings.TrimSpace(fullText) == "" {
		return Frozen{}, fmt.Errorf("channel response requires an inbound publication and content")
	}
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "response", "source_id": publicationID,
		"source_revision": 1, "audience": audienceProjection(audience), "full_text": fullText,
	}
	return freeze(input, "response", publicationID, 1, audience, fullText)
}

func FreezeDraftChooser(publicationID, textPublicationID string, choices []DraftChoice, audience Audience) (Frozen, error) {
	if err := audience.Validate(); err != nil {
		return Frozen{}, err
	}
	if uuid.Validate(publicationID) != nil || uuid.Validate(textPublicationID) != nil ||
		len(choices) < 2 {
		return Frozen{}, fmt.Errorf("channel draft chooser requires exact publications and a bounded page")
	}
	lines := []string{"Choose the card for your reply:"}
	seen := make(map[string]bool, len(choices))
	for _, choice := range choices {
		if uuid.Validate(choice.DraftID) != nil || uuid.Validate(choice.CardID) != nil ||
			strings.TrimSpace(choice.Label) == "" || seen[choice.DraftID] {
			return Frozen{}, fmt.Errorf("channel draft chooser has invalid or duplicate choices")
		}
		seen[choice.DraftID] = true
		lines = append(lines, "- "+choice.Label)
	}
	fullText := strings.Join(lines, "\n")
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "response", "source_id": publicationID,
		"source_revision": 1, "audience": audienceProjection(audience), "full_text": fullText,
		"draft_choices": choices, "draft_chooser": DraftChooser{TextPublicationID: textPublicationID},
	}
	return freeze(input, "response", publicationID, 1, audience, fullText)
}

func DraftChoiceLabel(title, cardID string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		title = "Card"
	}
	return title + " [" + shortRunID(cardID) + "]"
}

func FreezeRecoveryInbox(publicationID, baseText string, choices []RecoveryChoice, audience Audience) (Frozen, error) {
	if err := audience.Validate(); err != nil {
		return Frozen{}, err
	}
	if uuid.Validate(publicationID) != nil || strings.TrimSpace(baseText) == "" || len(choices) == 0 {
		return Frozen{}, fmt.Errorf("channel recovery page requires exact entry and bounded choices")
	}
	seen := make(map[string]bool, len(choices))
	lines := []string{baseText, "Uncertain deliveries - check the chat before resending:",
		"The first message may have arrived. Resend creates another message."}
	for _, choice := range choices {
		if uuid.Validate(choice.DeliveryID) != nil || strings.TrimSpace(choice.Label) == "" ||
			seen[choice.DeliveryID] {
			return Frozen{}, fmt.Errorf("channel recovery page has invalid or duplicate choice")
		}
		seen[choice.DeliveryID] = true
		lines = append(lines, "- "+choice.Label)
	}
	fullText := strings.Join(lines, "\n")
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "response", "source_id": publicationID,
		"source_revision": 1, "audience": audienceProjection(audience), "full_text": fullText,
		"recovery_page": RecoveryPage{BaseText: baseText}, "recovery_choices": choices,
	}
	return freeze(input, "response", publicationID, 1, audience, fullText)
}

// FreezeResponsePage projects one deterministic chunk of an immutable render.
// A callback requests each successor page; a page is never inferred from live
// card or notice state.
func FreezeResponsePage(publicationID string, source Frozen, sourceRenderID string, index int, audience Audience) (Frozen, error) {
	if err := source.Validate(); err != nil {
		return Frozen{}, err
	}
	if uuid.Validate(publicationID) != nil || uuid.Validate(sourceRenderID) != nil || source.Page != nil {
		return Frozen{}, fmt.Errorf("view-full response requires an immutable original render")
	}
	pageText, count, err := FullTextPage(source.FullText, index, source.Bounds)
	if err != nil {
		return Frozen{}, err
	}
	page := &ResponsePage{SourceRenderID: sourceRenderID, SourceRenderHash: source.Hash, Index: index, Count: count}
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "response", "source_id": publicationID,
		"source_revision": 1, "audience": audienceProjection(audience), "full_text": pageText, "page": page,
	}
	raw, err := canonicaljson.Bytes(input)
	if err != nil {
		return Frozen{}, err
	}
	frozen := Frozen{SourceKind: "response", SourceID: publicationID, Revision: 1, Audience: audience,
		Input: raw, Hash: canonicaljson.HashBytes(raw), FullText: pageText, Page: page}
	return WithPresentation(frozen, source.Bounds, 0)
}

func FullTextPage(fullText string, index int, bounds packs.PresentationBounds) (string, int, error) {
	if err := bounds.Validate(); err != nil {
		return "", 0, err
	}
	runes := []rune(fullText)
	if len(runes) == 0 {
		return "", 0, fmt.Errorf("view-full source is empty")
	}
	continuation, err := visibleControlWords([]Action{{Kind: "next_page", Label: "Next page"}}, bounds)
	if err != nil {
		return "", 0, err
	}
	pageRunes := bounds.TextRunes - len("Page 1000/1000\n") - textReplySuffixRunes("00000000-0000-0000-0000-000000000000", continuation)
	if pageRunes < 1 {
		return "", 0, fmt.Errorf("view-full page cannot retain its exact reference and continuation")
	}
	count := (len(runes) + pageRunes - 1) / pageRunes
	if count > 1000 || index < 0 || index >= count {
		return "", 0, fmt.Errorf("view-full page is outside the bounded immutable source")
	}
	start := index * pageRunes
	end := start + pageRunes
	if end > len(runes) {
		end = len(runes)
	}
	return fmt.Sprintf("Page %d/%d\n%s", index+1, count, string(runes[start:end])), count, nil
}

// InboxText presents only canonical list metadata; draft answers and card
// context are deliberately absent from this requested readback.
type InboxEntry struct {
	Kind   string
	ID     string
	Label  string
	Status string
}

func InboxNoticeEntry(notice mailbox.V1Item) (InboxEntry, error) {
	if uuid.Validate(notice.MailboxID) != nil || notice.Status != "pending" || strings.TrimSpace(notice.Type) == "" {
		return InboxEntry{}, fmt.Errorf("inbox notice projection is invalid")
	}
	return InboxEntry{Kind: "notice", ID: notice.MailboxID, Label: notice.Type, Status: notice.Status}, nil
}

func InboxCardEntry(card decisioncard.ListItem) (InboxEntry, error) {
	if uuid.Validate(card.CardID) != nil || card.Status != decisioncard.StatusPending {
		return InboxEntry{}, fmt.Errorf("inbox card projection is invalid")
	}
	title := strings.TrimSpace(card.Title)
	if title == "" {
		title = string(card.Anchor.Kind())
	}
	return InboxEntry{Kind: "card", ID: card.CardID, Label: title, Status: card.Status}, nil
}

func InboxText(unread int, entries []InboxEntry, more bool) (string, error) {
	if unread < 0 {
		return "", fmt.Errorf("inbox unread count is invalid")
	}
	lines := []string{"Inbox", fmt.Sprintf("Unread notices: %d", unread)}
	if len(entries) == 0 {
		lines = append(lines, "No open items")
	} else {
		lines = append(lines, "Open items:")
		for _, entry := range entries {
			if uuid.Validate(entry.ID) != nil || entry.Status != "pending" ||
				(entry.Kind != "notice" && entry.Kind != "card") || strings.TrimSpace(entry.Label) == "" {
				return "", fmt.Errorf("inbox entry is not a pending canonical projection")
			}
			lines = append(lines, "- "+entry.Label)
		}
		if more {
			lines = append(lines, "More open items are available")
		}
	}
	return strings.Join(lines, "\n"), nil
}

func freeze(input map[string]any, kind, id string, revision int64, audience Audience, fullText string) (Frozen, error) {
	raw, err := canonicaljson.Bytes(input)
	if err != nil {
		return Frozen{}, err
	}
	frozen, err := Decode(raw, canonicaljson.HashBytes(raw))
	if err != nil {
		return Frozen{}, err
	}
	if frozen.SourceKind != kind || frozen.SourceID != id || frozen.Revision != revision ||
		frozen.Audience != audience || frozen.FullText != fullText {
		return Frozen{}, fmt.Errorf("channel source projection contradicted its frozen input")
	}
	return frozen, nil
}

func audienceProjection(a Audience) map[string]any {
	return map[string]any{
		"principal_id": a.PrincipalID, "interface_key": a.InterfaceKey, "delivery_epoch": a.DeliveryEpoch,
		"external_account_reference": a.ExternalAccountRef, "conversation_reference": a.ConversationRef,
		"conversation_scope": string(a.ConversationScope),
	}
}

func choicesProjection(choices []Choice) []any {
	out := make([]any, 0, len(choices))
	for _, choice := range choices {
		fields := make([]any, 0, len(choice.Fields))
		for _, field := range choice.Fields {
			fields = append(fields, map[string]any{"name": field.Name, "type": field.Type, "required": field.Required})
		}
		out = append(out, map[string]any{"verdict": choice.Verdict, "label": choice.Label, "fields": fields})
	}
	return out
}

func cardSubtitle(card decisioncard.Card) (string, error) {
	switch card.Anchor.Kind() {
	case decisioncard.AnchorKindStageGate:
		anchor, err := card.Anchor.StageGate()
		if err != nil {
			return "", err
		}
		return "Gate: " + anchor.FlowID + " / " + anchor.Stage, nil
	case decisioncard.AnchorKindHumanTask:
		anchor, err := card.Anchor.HumanTask()
		if err != nil {
			return "", err
		}
		return "Task: " + anchor.RequesterAgentID + " / " + anchor.Category, nil
	case decisioncard.AnchorKindProposedEffect:
		anchor, err := card.Anchor.ProposedEffect()
		if err != nil {
			return "", err
		}
		return "Effect: " + anchor.ActivityID, nil
	default:
		return "", fmt.Errorf("unsupported decision card anchor")
	}
}

func shortRunID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}
