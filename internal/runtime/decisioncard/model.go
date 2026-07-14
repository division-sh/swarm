package decisioncard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
	"github.com/google/uuid"
)

const (
	KindNotice       = "notice"
	KindDecisionCard = "decision_card"

	StatusPending    = "pending"
	StatusDecided    = "decided"
	StatusSuperseded = "superseded"
	StatusExpired    = "expired"

	DraftStatusActive    = "active"
	DraftStatusCancelled = "cancelled"
	DraftStatusConsumed  = "consumed"
	DraftStatusExpired   = "expired"

	ChangeCreated        = "created"
	ChangeDecided        = "decided"
	ChangeDeferred       = "deferred"
	ChangeExpired        = "expired"
	ChangeSuperseded     = "superseded"
	ChangeDraftStarted   = "input_draft_started"
	ChangeDraftCancelled = "input_draft_cancelled"
	ChangeDraftExpired   = "input_draft_expired"
	ChangeDraftConsumed  = "input_draft_consumed"

	DefaultInputDraftTTL    = 15 * time.Minute
	DefaultFirstReminder    = 4 * time.Hour
	DefaultUrgency          = 24 * time.Hour
	DefaultReminderInterval = 24 * time.Hour
)

var (
	ErrNotFound          = errors.New("decision card not found")
	ErrInvalidCursor     = errors.New("invalid decision card cursor")
	ErrAlreadyTerminal   = errors.New("decision card is already terminal")
	ErrStaleContent      = errors.New("decision card content hash does not match")
	ErrInvalidVerdict    = errors.New("decision card verdict is not authored")
	ErrInvalidFields     = errors.New("decision card fields are invalid")
	ErrInvalidDeferUntil = errors.New("decision card defer until must be in the future")
	ErrDraftNotFound     = errors.New("decision card input draft not found")
	ErrDraftNotAuthority = errors.New("decision card input draft is not authoritative")
)

// CanonicalTimestamp matches the exact precision shared by the selected stores.
func CanonicalTimestamp(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Round(time.Microsecond)
}

var reservedNoticeFields = map[string]struct{}{
	"card_id": {}, "decision_id": {}, "stage_activation_id": {}, "card_content_hash": {},
	"decision_schema_hash": {}, "decision_event_id": {}, "verdict": {}, "outcomes": {},
}

func ValidateNoticeShape(itemType string, payload map[string]any) error {
	itemType = strings.ToLower(strings.TrimSpace(itemType))
	itemType = strings.NewReplacer("-", "_", ".", "_").Replace(itemType)
	if itemType == KindDecisionCard || itemType == string(AnchorKindHumanTask) {
		return fmt.Errorf("mailbox item_type %s is reserved for the typed decision-card owner", itemType)
	}
	return validateNoticePayloadFields(payload, "")
}

func validateNoticePayloadFields(payload map[string]any, prefix string) error {
	for field, value := range payload {
		field = strings.TrimSpace(field)
		path := field
		if prefix != "" {
			path = prefix + "." + field
		}
		leaf := field
		if index := strings.LastIndex(leaf, "."); index >= 0 {
			leaf = leaf[index+1:]
		}
		if _, reserved := reservedNoticeFields[strings.ToLower(strings.TrimSpace(leaf))]; reserved {
			return fmt.Errorf("mailbox notice payload field %s is reserved for the typed decision-card owner", path)
		}
		if nested, ok := value.(map[string]any); ok {
			if err := validateNoticePayloadFields(nested, path); err != nil {
				return err
			}
		}
	}
	return nil
}

type Store interface {
	CreateDecisionCard(context.Context, Card) error
	ListDecisionCards(context.Context, ListOptions) ([]ListItem, string, error)
	GetDecisionCard(context.Context, string) (Card, error)
	DecideDecisionCard(context.Context, DecideRequest) (DecisionOutcome, error)
	DeferDecisionCard(context.Context, DeferRequest) (DecisionOutcome, error)
	BeginDecisionCardInput(context.Context, BeginInputRequest) (InputDraft, error)
	CancelDecisionCardInput(context.Context, CancelInputRequest) (InputDraft, error)
	ListDecisionCardChanges(context.Context, SubscriptionOptions) ([]Change, error)
	SupersedeDecisionCardsForStage(context.Context, string, string, string, string, time.Time) error
	SupersedeDecisionCardsForRun(context.Context, string, string, time.Time) error
}

type Cadence struct {
	FirstReminderAt  time.Time `json:"first_reminder_at"`
	UrgencyAt        time.Time `json:"urgency_at"`
	InputDraftTTL    string    `json:"input_draft_ttl"`
	ReminderInterval string    `json:"reminder_interval"`
}

type CadencePolicy struct {
	FirstReminderDelay time.Duration
	UrgencyDelay       time.Duration
	InputDraftTTL      time.Duration
	ReminderInterval   time.Duration
}

func (p CadencePolicy) Normalize() CadencePolicy {
	if p.FirstReminderDelay <= 0 {
		p.FirstReminderDelay = DefaultFirstReminder
	}
	if p.UrgencyDelay <= 0 {
		p.UrgencyDelay = DefaultUrgency
	}
	if p.InputDraftTTL <= 0 {
		p.InputDraftTTL = DefaultInputDraftTTL
	}
	if p.ReminderInterval <= 0 {
		p.ReminderInterval = DefaultReminderInterval
	}
	return p
}

func (p CadencePolicy) Stamp(createdAt time.Time) Cadence {
	p = p.Normalize()
	createdAt = createdAt.UTC()
	return Cadence{
		FirstReminderAt: createdAt.Add(p.FirstReminderDelay), UrgencyAt: createdAt.Add(p.UrgencyDelay),
		InputDraftTTL: p.InputDraftTTL.String(), ReminderInterval: p.ReminderInterval.String(),
	}
}

type Snapshot struct {
	Decision string
	Title    string
	Context  semanticvalue.Value
	Outcomes map[string]FrozenOutcome
}

func (s Snapshot) MarshalJSON() ([]byte, error) {
	value, err := s.SemanticValue()
	if err != nil {
		return nil, err
	}
	return canonicaljson.Encode(value)
}

type decisionSchemaProjection struct {
	Version  string                                     `json:"version"`
	Outcomes map[string]decisionOutcomeSchemaProjection `json:"outcomes"`
}

type decisionOutcomeSchemaProjection struct {
	Input map[string]decisionInputSchemaProjection `json:"input"`
	Emit  *decisionEmitSchemaProjection            `json:"emit,omitempty"`
}

type decisionInputSchemaProjection struct {
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type decisionEmitSchemaProjection struct {
	Fields map[string]map[string]any `json:"fields"`
	Schema map[string]any            `json:"schema"`
}

type Card struct {
	CardID             string              `json:"card_id"`
	RunID              string              `json:"run_id"`
	Anchor             Anchor              `json:"-"`
	Status             string              `json:"status"`
	Snapshot           Snapshot            `json:"snapshot"`
	CardContentHash    string              `json:"card_content_hash"`
	DecisionSchemaHash string              `json:"decision_schema_hash"`
	BundleHash         string              `json:"bundle_hash"`
	WorkflowVersion    string              `json:"workflow_version"`
	EffectiveCadence   Cadence             `json:"effective_cadence"`
	Provenance         semanticvalue.Value `json:"-"`
	Verdict            string              `json:"verdict,omitempty"`
	Fields             semanticvalue.Value `json:"-"`
	DecidedBy          string              `json:"decided_by,omitempty"`
	DecidedAt          time.Time           `json:"decided_at,omitempty"`
	DeferredUntil      time.Time           `json:"deferred_until,omitempty"`
	DecisionEventID    string              `json:"decision_event_id,omitempty"`
	DeliveryReceiptID  string              `json:"delivery_receipt_id,omitempty"`
	DeliveryRenderHash string              `json:"delivery_render_hash,omitempty"`
	SupersededReason   string              `json:"superseded_reason,omitempty"`
	CreatedAt          time.Time           `json:"created_at"`
	UpdatedAt          time.Time           `json:"updated_at"`
}

func (c Card) MarshalJSON() ([]byte, error) {
	snapshot, err := SnapshotJSON(c)
	if err != nil {
		return nil, err
	}
	scope, err := c.Anchor.Scope()
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"card_id": c.CardID, "run_id": c.RunID, "anchor_kind": c.Anchor.Kind(),
		"anchor": c.Anchor.SemanticValue().Interface(), "scope": scope,
		"status": c.Status, "snapshot": json.RawMessage(snapshot),
		"card_content_hash": c.CardContentHash, "decision_schema_hash": c.DecisionSchemaHash,
		"bundle_hash": c.BundleHash, "workflow_version": c.WorkflowVersion,
		"effective_cadence": c.EffectiveCadence, "provenance": c.Provenance.Interface(),
		"created_at": c.CreatedAt.UTC(), "updated_at": c.UpdatedAt.UTC(),
	}
	switch c.Anchor.Kind() {
	case AnchorKindStageGate:
		out["decision"] = c.Snapshot.Decision
	case AnchorKindHumanTask:
		if anchor, anchorErr := c.Anchor.HumanTask(); anchorErr == nil {
			out["category"] = anchor.Category
		}
	}
	for name, value := range map[string]string{
		"verdict": c.Verdict, "decided_by": c.DecidedBy, "decision_event_id": c.DecisionEventID,
		"delivery_receipt_id": c.DeliveryReceiptID, "delivery_render_hash": c.DeliveryRenderHash,
		"superseded_reason": c.SupersededReason,
	} {
		if value != "" {
			out[name] = value
		}
	}
	if c.Status == StatusDecided {
		out["fields"] = c.Fields.Interface()
	}
	if !c.DecidedAt.IsZero() {
		out["decided_at"] = c.DecidedAt.UTC()
	}
	if !c.DeferredUntil.IsZero() {
		out["deferred_until"] = c.DeferredUntil.UTC()
	}
	return canonicaljson.Bytes(out)
}

type InputDraft struct {
	InputDraftID      string    `json:"input_draft_id"`
	RunID             string    `json:"run_id"`
	CardID            string    `json:"card_id"`
	ActorTokenID      string    `json:"actor_token_id"`
	Verdict           string    `json:"verdict"`
	DeliveryReceiptID string    `json:"delivery_receipt_id,omitempty"`
	Status            string    `json:"status"`
	ExpiresAt         time.Time `json:"expires_at"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type CreateRequest struct {
	Card Card
}

type ListOptions struct {
	Status     string
	RunID      string
	EntityID   string
	AnchorKind string
	Kind       string
	Limit      int
	Cursor     string
}

type ListItem struct {
	Kind          string    `json:"kind"`
	CardID        string    `json:"card_id"`
	RunID         string    `json:"run_id"`
	Anchor        Anchor    `json:"-"`
	Scope         Scope     `json:"scope"`
	Title         string    `json:"title"`
	Decision      string    `json:"decision,omitempty"`
	Category      string    `json:"category,omitempty"`
	Status        string    `json:"status"`
	DeferredUntil time.Time `json:"deferred_until,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (i ListItem) MarshalJSON() ([]byte, error) {
	type listItemAlias ListItem
	var deferredUntil *time.Time
	if !i.DeferredUntil.IsZero() {
		value := i.DeferredUntil.UTC()
		deferredUntil = &value
	}
	base := map[string]any{
		"kind": i.Kind, "card_id": i.CardID, "run_id": i.RunID,
		"anchor_kind": i.Anchor.Kind(), "anchor": i.Anchor.SemanticValue().Interface(),
		"scope": i.Scope, "title": i.Title, "status": i.Status,
		"created_at": i.CreatedAt, "updated_at": i.UpdatedAt,
	}
	if deferredUntil != nil {
		base["deferred_until"] = deferredUntil
	}
	if i.Decision != "" {
		base["decision"] = i.Decision
	}
	if i.Category != "" {
		base["category"] = i.Category
	}
	return canonicaljson.Bytes(base)
}

type DecideRequest struct {
	CardID              string
	Verdict             string
	Fields              semanticvalue.Value
	ActorTokenID        string
	ObservedContentHash string
	DeliveryReceiptID   string
	DeliveryRenderHash  string
	InputDraftID        string
	DecisionEventID     string
	Now                 time.Time
}

type DeferRequest struct {
	CardID       string
	ActorTokenID string
	Until        time.Time
	Now          time.Time
}

type BeginInputRequest struct {
	CardID            string
	Verdict           string
	ActorTokenID      string
	DeliveryReceiptID string
	Now               time.Time
	TTL               time.Duration
}

type CancelInputRequest struct {
	CardID       string
	InputDraftID string
	ActorTokenID string
	Now          time.Time
}

type DecisionOutcome struct {
	Card           Card  `json:"card"`
	ChangeID       int64 `json:"change_id"`
	Replayed       bool  `json:"idempotency_replayed"`
	ForcedDeferred bool  `json:"forced_deferred,omitempty"`
}

type SubscriptionOptions struct {
	After int64
	Limit int
}

type Change struct {
	Sequence   int64               `json:"sequence"`
	CardID     string              `json:"card_id"`
	RunID      string              `json:"run_id"`
	ChangeType string              `json:"change_type"`
	Payload    semanticvalue.Value `json:"-"`
	CreatedAt  time.Time           `json:"created_at"`
}

func (c Change) MarshalJSON() ([]byte, error) {
	return canonicaljson.Bytes(map[string]any{
		"sequence": c.Sequence, "card_id": c.CardID, "run_id": c.RunID,
		"change_type": c.ChangeType, "payload": c.Payload.Interface(), "created_at": c.CreatedAt.UTC(),
	})
}

func (c Card) Validate() error {
	for field, value := range map[string]string{
		"card_id": c.CardID, "run_id": c.RunID,
		"card_content_hash": c.CardContentHash, "decision_schema_hash": c.DecisionSchemaHash, "bundle_hash": c.BundleHash,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("decision card %s is required", field)
		}
	}
	if _, err := uuid.Parse(c.CardID); err != nil {
		return fmt.Errorf("decision card id is invalid: %w", err)
	}
	switch c.Status {
	case StatusPending, StatusDecided, StatusSuperseded, StatusExpired:
	default:
		return fmt.Errorf("decision card status %q is invalid", c.Status)
	}
	if c.Status == StatusDecided && (strings.TrimSpace(c.Verdict) == "" || c.DecidedAt.IsZero() || strings.TrimSpace(c.DecisionEventID) == "") {
		return fmt.Errorf("decided card is missing verdict evidence")
	}
	if c.Status == StatusSuperseded && strings.TrimSpace(c.SupersededReason) == "" {
		return fmt.Errorf("superseded card is missing reason")
	}
	if c.Status == StatusExpired && (strings.TrimSpace(c.Verdict) != "" || c.DecidedAt.IsZero()) {
		return fmt.Errorf("expired card is missing terminal evidence")
	}
	if err := c.Anchor.Validate(); err != nil {
		return err
	}
	if c.Provenance.Kind() != semanticvalue.KindObject {
		return fmt.Errorf("decision card provenance must be an object")
	}
	if c.Fields.Kind() != semanticvalue.KindObject {
		return fmt.Errorf("decision card fields must be an object")
	}
	if len(c.Snapshot.Outcomes) == 0 {
		return fmt.Errorf("decision card has no authored outcomes")
	}
	if err := validateSnapshotContract(c.Snapshot); err != nil {
		return err
	}
	contentHash, schemaHash, err := snapshotHashes(c.Snapshot)
	if err != nil {
		return err
	}
	if c.CardContentHash != contentHash {
		return fmt.Errorf("decision card content hash does not match its snapshot")
	}
	if c.DecisionSchemaHash != schemaHash {
		return fmt.Errorf("decision card schema hash does not match its semantic input contract")
	}
	if c.Status == StatusDecided {
		if err := ValidateDecision(c, c.Verdict, c.Fields); err != nil {
			return fmt.Errorf("decided card outcome evidence is invalid: %w", err)
		}
	}
	draftTTL, err := time.ParseDuration(strings.TrimSpace(c.EffectiveCadence.InputDraftTTL))
	if err != nil || draftTTL <= 0 {
		return fmt.Errorf("decision card input draft TTL is invalid")
	}
	reminderInterval, err := time.ParseDuration(strings.TrimSpace(c.EffectiveCadence.ReminderInterval))
	if err != nil || reminderInterval <= 0 {
		return fmt.Errorf("decision card reminder interval is invalid")
	}
	if draftTTL > reminderInterval {
		return fmt.Errorf("decision card input draft TTL exceeds reminder interval")
	}
	if c.EffectiveCadence.FirstReminderAt.IsZero() || c.EffectiveCadence.UrgencyAt.IsZero() {
		return fmt.Errorf("decision card reminder and urgency deadlines are required")
	}
	if c.EffectiveCadence.FirstReminderAt.Before(c.CreatedAt) || c.EffectiveCadence.UrgencyAt.Before(c.EffectiveCadence.FirstReminderAt) {
		return fmt.Errorf("decision card cadence deadlines are invalid")
	}
	return nil
}

func validateSnapshotContract(snapshot Snapshot) error {
	contextValues, ok := snapshot.Context.ObjectMap()
	if !ok {
		return fmt.Errorf("decision card context must be an object")
	}
	if err := validateCanonicalDecisionMapIdentity("context field", contextValues); err != nil {
		return err
	}
	if snapshot.Decision == "" || snapshot.Decision != strings.TrimSpace(snapshot.Decision) {
		return fmt.Errorf("decision card decision identity %q is not canonical", snapshot.Decision)
	}
	if err := validateCanonicalDecisionMapIdentity("verdict", snapshot.Outcomes); err != nil {
		return err
	}
	for verdict, outcome := range snapshot.Outcomes {
		if outcome.Verdict != "" && outcome.Verdict != verdict {
			return fmt.Errorf("outcome %s carries mismatched verdict identity %q", verdict, outcome.Verdict)
		}
		if err := validateCanonicalDecisionMapIdentity("outcome "+verdict+" input field", outcome.Input); err != nil {
			return err
		}
		if err := validateCanonicalDecisionMapIdentity("outcome "+verdict+" emit field", outcome.Emit.Fields); err != nil {
			return err
		}
		for name, declaration := range outcome.Input {
			if _, err := runtimecontracts.ValidateCanonicalWorkflowGateInputType(declaration.Type); err != nil {
				return fmt.Errorf("decision card outcome %s input %s: %w", strings.TrimSpace(verdict), strings.TrimSpace(name), err)
			}
		}
		if err := validateFrozenOutcomeContract(verdict, outcome); err != nil {
			return err
		}
	}
	return nil
}

func validateCanonicalDecisionMapIdentity[T any](label string, values map[string]T) error {
	seen := map[string]string{}
	for raw := range values {
		canonical := strings.TrimSpace(raw)
		if canonical == "" {
			return fmt.Errorf("decision card %s is empty", label)
		}
		if previous, exists := seen[canonical]; exists {
			return fmt.Errorf("decision card %s contains duplicate normalized key %q (from %q and %q)", label, canonical, previous, raw)
		}
		seen[canonical] = raw
		if raw != canonical {
			return fmt.Errorf("decision card %s key %q is not canonical; use %q", label, raw, canonical)
		}
	}
	return nil
}

func validateFrozenOutcomeContract(verdict string, outcome FrozenOutcome) error {
	if outcome.Emit.Empty() {
		if outcome.EmitSchema.Kind() != semanticvalue.KindObject || outcome.EmitSchema.Len() != 0 {
			return fmt.Errorf("decision card outcome %s carries an event schema without an emit", verdict)
		}
		return nil
	}
	if outcome.EmitSchema.Kind() != semanticvalue.KindObject || outcome.EmitSchema.Len() == 0 {
		return fmt.Errorf("decision card outcome %s emit is missing its frozen resolved event schema", verdict)
	}
	schema, err := semanticObjectProjection(outcome.EmitSchema, "decision card outcome schema")
	if err != nil {
		return err
	}
	properties := runtimesharedjson.SchemaProperties(schema["properties"])
	literalPayload := make(map[string]any, len(outcome.Emit.Fields))
	allLiteral := true
	for field, expression := range outcome.Emit.Fields {
		fieldSchema, ok := properties[field]
		if !ok {
			return fmt.Errorf("decision card outcome %s emit field %s is absent from its frozen event schema", verdict, field)
		}
		if expression.HasLiteralValue() {
			literalPayload[field] = expression.Literal.Interface()
			if err := runtimeeventschema.ValidateValueAgainstSchema(fieldSchema, expression.Literal.Interface()); err != nil {
				return fmt.Errorf("decision card outcome %s literal emit field %s: %w", verdict, field, err)
			}
			continue
		}
		allLiteral = false
		inputName, err := outcomeDecisionField(expression)
		if err != nil {
			return fmt.Errorf("decision card outcome %s emit field %s: %w", verdict, field, err)
		}
		input, ok := outcome.Input[inputName]
		if !ok {
			return fmt.Errorf("decision card outcome %s emit field %s reads undeclared decision.%s", verdict, field, inputName)
		}
		if !input.Required {
			return fmt.Errorf("decision card outcome %s emit field %s reads optional decision.%s", verdict, field, inputName)
		}
		if !runtimecontracts.WorkflowGateInputTypeCompatibleWithResolvedSchema(input.Type, fieldSchema) {
			return fmt.Errorf("decision card outcome %s decision.%s type %s is incompatible with emit field %s frozen schema", verdict, inputName, input.Type, field)
		}
	}
	for _, required := range runtimesharedjson.RequiredList(schema["required"]) {
		if _, ok := outcome.Emit.Fields[required]; !ok {
			return fmt.Errorf("decision card outcome %s emit is missing required field %s from its frozen event schema", verdict, required)
		}
	}
	if allLiteral {
		if err := runtimeeventschema.ValidatePayloadAgainstSchema(schema, literalPayload); err != nil {
			return fmt.Errorf("decision card outcome %s assembled literal payload: %w", verdict, err)
		}
	}
	return nil
}

func outcomeDecisionField(expression FrozenExpression) (string, error) {
	raw := strings.TrimSpace(expression.Ref)
	if raw == "" {
		raw = strings.TrimSpace(expression.CEL)
	}
	if !strings.HasPrefix(raw, "decision.") || strings.Count(raw, ".") != 1 {
		return "", fmt.Errorf("only exact decision.<field> references are supported")
	}
	field := strings.TrimPrefix(raw, "decision.")
	if field == "" || field != strings.TrimSpace(field) {
		return "", fmt.Errorf("decision field reference %q is not canonical", raw)
	}
	return field, nil
}

func New(card Card) (Card, error) {
	decisionID := strings.TrimSpace(card.Snapshot.Decision)
	if card.Snapshot.Decision != decisionID {
		return Card{}, fmt.Errorf("decision card decision identity %q is not canonical; use %q", card.Snapshot.Decision, decisionID)
	}
	now := CanonicalTimestamp(card.CreatedAt)
	if now.IsZero() {
		now = CanonicalTimestamp(time.Now())
	}
	card.CreatedAt = now
	card.UpdatedAt = now
	card.Status = StatusPending
	card.Snapshot.Decision = decisionID
	if strings.TrimSpace(card.Snapshot.Title) == "" {
		card.Snapshot.Title = humanize(decisionID)
	}
	if card.Snapshot.Context.Kind() == semanticvalue.KindNull {
		card.Snapshot.Context = semanticvalue.EmptyObject()
	}
	if card.Provenance.Kind() == semanticvalue.KindNull {
		card.Provenance = semanticvalue.EmptyObject()
	}
	if card.Fields.Kind() == semanticvalue.KindNull {
		card.Fields = semanticvalue.EmptyObject()
	}
	if strings.TrimSpace(card.EffectiveCadence.InputDraftTTL) == "" {
		card.EffectiveCadence.InputDraftTTL = DefaultInputDraftTTL.String()
	}
	if strings.TrimSpace(card.EffectiveCadence.ReminderInterval) == "" {
		card.EffectiveCadence.ReminderInterval = DefaultReminderInterval.String()
	}
	if card.EffectiveCadence.FirstReminderAt.IsZero() {
		card.EffectiveCadence.FirstReminderAt = now.Add(DefaultFirstReminder)
	}
	if card.EffectiveCadence.UrgencyAt.IsZero() {
		card.EffectiveCadence.UrgencyAt = now.Add(DefaultUrgency)
	}
	if err := validateSnapshotContract(card.Snapshot); err != nil {
		return Card{}, err
	}
	contentHash, schemaHash, err := snapshotHashes(card.Snapshot)
	if err != nil {
		return Card{}, err
	}
	card.CardContentHash = contentHash
	card.DecisionSchemaHash = schemaHash
	if err := card.Validate(); err != nil {
		return Card{}, err
	}
	return card, nil
}

func snapshotHashes(snapshot Snapshot) (string, string, error) {
	schema, err := projectDecisionSchema(snapshot)
	if err != nil {
		return "", "", err
	}
	contentValue, err := snapshot.SemanticValue()
	if err != nil {
		return "", "", fmt.Errorf("encode decision card content: %w", err)
	}
	contentHash, err := canonicaljson.HashValue(contentValue)
	if err != nil {
		return "", "", fmt.Errorf("hash decision card content: %w", err)
	}
	schemaHash, err := canonicaljson.HashValue(schema)
	if err != nil {
		return "", "", fmt.Errorf("hash decision card schema: %w", err)
	}
	return contentHash, schemaHash, nil
}

func projectDecisionSchema(snapshot Snapshot) (semanticvalue.Value, error) {
	projection := decisionSchemaProjection{
		Version:  "swarm.decision-schema/v1",
		Outcomes: make(map[string]decisionOutcomeSchemaProjection, len(snapshot.Outcomes)),
	}
	for verdict, outcome := range snapshot.Outcomes {
		projected := decisionOutcomeSchemaProjection{
			Input: make(map[string]decisionInputSchemaProjection, len(outcome.Input)),
		}
		for name, input := range outcome.Input {
			projected.Input[name] = decisionInputSchemaProjection{Type: input.Type, Required: input.Required}
		}
		if !outcome.Emit.Empty() {
			schema, err := semanticObjectProjection(outcome.EmitSchema, "decision card outcome schema")
			if err != nil {
				return semanticvalue.Value{}, err
			}
			emit := &decisionEmitSchemaProjection{
				Fields: make(map[string]map[string]any, len(outcome.Emit.Fields)),
				Schema: runtimeeventschema.CanonicalAcceptanceSchema(schema),
			}
			for field, expression := range outcome.Emit.Fields {
				if expression.HasLiteralValue() {
					emit.Fields[field] = map[string]any{"kind": "literal", "value": expression.Literal.Interface()}
					continue
				}
				inputName, err := outcomeDecisionField(expression)
				if err != nil {
					return semanticvalue.Value{}, fmt.Errorf("project decision schema outcome %s field %s: %w", verdict, field, err)
				}
				emit.Fields[field] = map[string]any{"kind": "decision", "field": inputName}
			}
			projected.Emit = emit
		}
		projection.Outcomes[verdict] = projected
	}
	value, err := canonicaljson.FromGo(projection)
	if err != nil {
		return semanticvalue.Value{}, fmt.Errorf("admit decision schema projection: %w", err)
	}
	return value, nil
}

func ValidateDecision(card Card, verdict string, fields semanticvalue.Value) error {
	rawVerdict := verdict
	verdict = strings.TrimSpace(verdict)
	if rawVerdict != verdict {
		return fmt.Errorf("%w: verdict %q is not canonical; use %q", ErrInvalidVerdict, rawVerdict, verdict)
	}
	outcome, ok := card.Snapshot.Outcomes[verdict]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidVerdict, verdict)
	}
	if fields.Kind() == semanticvalue.KindNull {
		fields = semanticvalue.EmptyObject()
	}
	fieldValues, ok := fields.ObjectMap()
	if !ok {
		return fmt.Errorf("%w: fields must be an object", ErrInvalidFields)
	}
	seenFields := map[string]string{}
	for name := range fieldValues {
		canonical := strings.TrimSpace(name)
		if canonical == "" || canonical != name {
			return fmt.Errorf("%w: field %q is not canonical", ErrInvalidFields, name)
		}
		if previous, exists := seenFields[canonical]; exists {
			return fmt.Errorf("%w: fields %q and %q have the same normalized identity", ErrInvalidFields, previous, name)
		}
		seenFields[canonical] = name
		if _, declared := outcome.Input[name]; !declared {
			return fmt.Errorf("%w: undeclared field %s", ErrInvalidFields, name)
		}
	}
	for name, declaration := range outcome.Input {
		value, present := fieldValues[name]
		if declaration.Required && (!present || value.Kind() == semanticvalue.KindNull || (value.Kind() == semanticvalue.KindString && strings.TrimSpace(value.Interface().(string)) == "")) {
			return fmt.Errorf("%w: required field %s is missing", ErrInvalidFields, name)
		}
		if !present || value.Kind() == semanticvalue.KindNull {
			continue
		}
		if !runtimecontracts.WorkflowGateInputValueMatches(declaration.Type, value.Interface()) {
			return fmt.Errorf("%w: field %s must be %s", ErrInvalidFields, name, declaration.Type)
		}
	}
	if !outcome.Emit.Empty() {
		payload, err := BuildOutcomePayload(outcome, fields)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidFields, err)
		}
		schema, schemaErr := semanticObjectProjection(outcome.EmitSchema, "decision card outcome schema")
		if schemaErr != nil {
			return fmt.Errorf("%w: %v", ErrInvalidFields, schemaErr)
		}
		payloadMap, payloadErr := semanticObjectProjection(payload, "decision card outcome payload")
		if payloadErr != nil {
			return fmt.Errorf("%w: %v", ErrInvalidFields, payloadErr)
		}
		if err := runtimeeventschema.ValidatePayloadAgainstSchema(schema, payloadMap); err != nil {
			return fmt.Errorf("%w: emitted payload does not satisfy the frozen event schema: %v", ErrInvalidFields, err)
		}
	}
	return nil
}

// BuildOutcomePayload resolves the frozen gate outcome using only authored
// literals and exact decision fields. Settlement and routing share this owner.
func BuildOutcomePayload(outcome FrozenOutcome, fields semanticvalue.Value) (semanticvalue.Value, error) {
	fieldValues, ok := fields.ObjectMap()
	if !ok {
		return semanticvalue.Value{}, fmt.Errorf("decision fields must be an object")
	}
	payload := make(map[string]semanticvalue.Value, len(outcome.Emit.Fields))
	for field, expression := range outcome.Emit.Fields {
		if expression.HasLiteralValue() {
			payload[field] = expression.Literal
			continue
		}
		inputName, err := outcomeDecisionField(expression)
		if err != nil {
			return semanticvalue.Value{}, fmt.Errorf("gate outcome field %s: %w", field, err)
		}
		value, ok := fieldValues[inputName]
		if !ok || value.Kind() == semanticvalue.KindNull {
			return semanticvalue.Value{}, fmt.Errorf("gate outcome field %s: decision field %s is absent", field, inputName)
		}
		payload[field] = value
	}
	return semanticvalue.ObjectFromMap(payload)
}

func SnapshotJSON(card Card) ([]byte, error) {
	value, err := card.Snapshot.SemanticValue()
	if err != nil {
		return nil, err
	}
	return canonicaljson.Encode(value)
}

func DecodeSnapshot(raw []byte) (Snapshot, error) {
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return Snapshot{}, err
	}
	return snapshotFromSemanticValue(value)
}

func humanize(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "_", " "))
	if value == "" {
		return "Decision"
	}
	return strings.ToUpper(value[:1]) + value[1:]
}
