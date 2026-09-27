package channeldelivery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

const ProjectionVersion = "channel-render-v1"

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
	SourceKind string
	SourceID   string
	Revision   int64
	Audience   Audience
	Input      json.RawMessage
	Hash       string
	FullText   string
	Choices    []Choice
}

func Decode(raw []byte, hash string) (Frozen, error) {
	var wire struct {
		SourceKind     string `json:"source_kind"`
		SourceID       string `json:"source_id"`
		SourceRevision int64  `json:"source_revision"`
		FullText       string `json:"full_text"`
		Choices        []struct {
			Verdict string  `json:"verdict"`
			Label   string  `json:"label"`
			Fields  []Field `json:"fields"`
		} `json:"choices"`
		Audience struct {
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
		Audience: Audience{PrincipalID: wire.Audience.PrincipalID, InterfaceKey: wire.Audience.InterfaceKey,
			DeliveryEpoch: wire.Audience.DeliveryEpoch, ExternalAccountRef: wire.Audience.ExternalAccountRef,
			ConversationRef: wire.Audience.ConversationRef, ConversationScope: wire.Audience.ConversationScope},
		Input: append(json.RawMessage(nil), raw...), Hash: hash, FullText: wire.FullText, Choices: choices,
	}
	if err := frozen.Validate(); err != nil {
		return Frozen{}, err
	}
	return frozen, nil
}

func (f Frozen) Validate() error {
	if err := f.Audience.Validate(); err != nil {
		return err
	}
	if uuid.Validate(f.SourceID) != nil || f.Revision < 1 || f.FullText == "" ||
		(f.SourceKind != "notice" && f.SourceKind != "card" && f.SourceKind != "summary") {
		return fmt.Errorf("channel render source is incomplete")
	}
	canonical, err := canonicaljson.Canonicalize(f.Input)
	if err != nil || !bytes.Equal(canonical, f.Input) || f.Hash != canonicaljson.HashBytes(f.Input) {
		return fmt.Errorf("channel render input/hash is not canonical")
	}
	var index struct {
		ProjectionVersion string `json:"projection_version"`
		SourceKind        string `json:"source_kind"`
		SourceID          string `json:"source_id"`
		SourceRevision    int64  `json:"source_revision"`
		Audience          struct {
			PrincipalID        string `json:"principal_id"`
			InterfaceKey       string `json:"interface_key"`
			DeliveryEpoch      int64  `json:"delivery_epoch"`
			ExternalAccountRef string `json:"external_account_reference"`
			ConversationRef    string `json:"conversation_reference"`
			ConversationScope  string `json:"conversation_scope"`
		} `json:"audience"`
		FullText string `json:"full_text"`
	}
	if err := json.Unmarshal(f.Input, &index); err != nil {
		return err
	}
	if index.ProjectionVersion != ProjectionVersion || index.SourceKind != f.SourceKind ||
		index.SourceID != f.SourceID || index.SourceRevision != f.Revision || index.FullText != f.FullText ||
		index.Audience.PrincipalID != f.Audience.PrincipalID || index.Audience.InterfaceKey != f.Audience.InterfaceKey ||
		index.Audience.DeliveryEpoch != f.Audience.DeliveryEpoch || index.Audience.ExternalAccountRef != f.Audience.ExternalAccountRef ||
		index.Audience.ConversationRef != f.Audience.ConversationRef ||
		index.Audience.ConversationScope != string(f.Audience.ConversationScope) {
		return fmt.Errorf("channel render projection index contradicts frozen input")
	}
	return nil
}

// Notice is the committed notice presentation projection. Mailbox persistence
// remains the notice owner; this value contains no acknowledgment authority.
type Notice struct {
	ID           string
	Type         string
	Summary      string
	Context      json.RawMessage
	Priority     string
	FromAgent    string
	EntityID     string
	FlowInstance string
}

// FreezeCard accepts the exact canonical change sequence. Dispatch is an
// independent axis for proposed-effect cards, never inferred from a verdict.
func FreezeCard(card decisioncard.Card, revision int64, dispatchState string, audience Audience) (Frozen, error) {
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
					return Frozen{}, fmt.Errorf("card outcome %s has incomplete input order", key)
				}
				choice.Fields = append(choice.Fields, Field{Name: name, Type: field.Type, Required: field.Required})
			}
			if len(choice.Fields) != len(outcome.Input) {
				return Frozen{}, fmt.Errorf("card outcome %s has incomplete input order", key)
			}
			choices = append(choices, choice)
		}
	}
	lines := []string{title, subtitle, "Run: " + shortRunID(card.RunID), "Scope: " + string(scope.Kind)}
	if scope.FlowInstance != "" {
		lines = append(lines, "Flow instance: "+scope.FlowInstance)
	}
	if scope.EntityID != "" {
		lines = append(lines, "Entity: "+scope.EntityID)
	}
	lines = append(lines, "Context: "+string(contextJSON))
	switch card.Status {
	case decisioncard.StatusPending:
		lines = append(lines, "Decision: pending")
	case decisioncard.StatusDecided:
		lines = append(lines, "Decision: "+card.Verdict, "Actor: "+card.DecidedBy,
			"At: "+card.DecidedAt.UTC().Format(time.RFC3339Nano))
	case decisioncard.StatusSuperseded:
		lines = append(lines, "The flow moved on. No action was taken.")
	case decisioncard.StatusExpired:
		lines = append(lines, "The decision expired. No action was taken.")
	}
	if dispatchState != "" {
		lines = append(lines, "Dispatch: "+dispatchState)
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
	return freeze(input, "card", card.CardID, revision, audience, fullText, choices)
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
	fullText := strings.Join(lines, "\n")
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "notice", "source_id": notice.ID,
		"source_revision": 1, "audience": audienceProjection(audience), "item_type": notice.Type,
		"scope": scope, "full_text": fullText,
	}
	return freeze(input, "notice", notice.ID, 1, audience, fullText, nil)
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
	fullText := fmt.Sprintf("%d earlier %s waiting in your inbox. Open inbox from the chat menu to review them.", count, noun)
	input := map[string]any{
		"projection_version": ProjectionVersion, "source_kind": "summary", "source_id": firstOperationID,
		"source_revision": 1, "audience": audienceProjection(audience), "summary_count": count,
		"full_text": fullText,
	}
	return freeze(input, "summary", firstOperationID, 1, audience, fullText, nil)
}

func freeze(input map[string]any, kind, id string, revision int64, audience Audience, fullText string, choices []Choice) (Frozen, error) {
	raw, err := canonicaljson.Bytes(input)
	if err != nil {
		return Frozen{}, err
	}
	frozen := Frozen{SourceKind: kind, SourceID: id, Revision: revision, Audience: audience,
		Input: raw, Hash: canonicaljson.HashBytes(raw), FullText: fullText, Choices: choices}
	if err := frozen.Validate(); err != nil {
		return Frozen{}, err
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
