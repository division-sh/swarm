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
	"github.com/google/uuid"
)

const (
	KindNotice       = "notice"
	KindDecisionCard = "decision_card"

	StatusPending    = "pending"
	StatusDecided    = "decided"
	StatusSuperseded = "superseded"

	DraftStatusActive    = "active"
	DraftStatusCancelled = "cancelled"
	DraftStatusConsumed  = "consumed"
	DraftStatusExpired   = "expired"

	ChangeCreated        = "created"
	ChangeDecided        = "decided"
	ChangeDeferred       = "deferred"
	ChangeSuperseded     = "superseded"
	ChangeDraftStarted   = "input_draft_started"
	ChangeDraftCancelled = "input_draft_cancelled"
	ChangeDraftExpired   = "input_draft_expired"

	DefaultInputDraftTTL    = 15 * time.Minute
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

type Snapshot struct {
	Decision string                                              `json:"decision"`
	Title    string                                              `json:"title"`
	Context  map[string]any                                      `json:"context"`
	Outcomes map[string]runtimecontracts.WorkflowGateOutcomePlan `json:"outcomes"`
}

type Card struct {
	CardID             string         `json:"card_id"`
	RunID              string         `json:"run_id"`
	FlowInstance       string         `json:"flow_instance"`
	FlowID             string         `json:"flow_id,omitempty"`
	EntityID           string         `json:"entity_id"`
	Stage              string         `json:"stage"`
	StageActivationID  string         `json:"stage_activation_id"`
	DecisionID         string         `json:"decision_id"`
	Status             string         `json:"status"`
	Snapshot           Snapshot       `json:"snapshot"`
	CardContentHash    string         `json:"card_content_hash"`
	DecisionSchemaHash string         `json:"decision_schema_hash"`
	BundleHash         string         `json:"bundle_hash"`
	WorkflowVersion    string         `json:"workflow_version"`
	EffectiveCadence   Cadence        `json:"effective_cadence"`
	Provenance         map[string]any `json:"provenance"`
	Verdict            string         `json:"verdict,omitempty"`
	Fields             map[string]any `json:"fields,omitempty"`
	DecidedBy          string         `json:"decided_by,omitempty"`
	DecidedAt          time.Time      `json:"decided_at,omitempty"`
	DeferredUntil      time.Time      `json:"deferred_until,omitempty"`
	DecisionEventID    string         `json:"decision_event_id,omitempty"`
	DeliveryReceiptID  string         `json:"delivery_receipt_id,omitempty"`
	DeliveryRenderHash string         `json:"delivery_render_hash,omitempty"`
	SupersededReason   string         `json:"superseded_reason,omitempty"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
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
	Status   string
	RunID    string
	EntityID string
	Kind     string
	Limit    int
	Cursor   string
}

type ListItem struct {
	Kind          string    `json:"kind"`
	CardID        string    `json:"card_id"`
	RunID         string    `json:"run_id"`
	FlowInstance  string    `json:"flow_instance"`
	EntityID      string    `json:"entity_id"`
	Stage         string    `json:"stage"`
	DecisionID    string    `json:"decision_id"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	DeferredUntil time.Time `json:"deferred_until,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type DecideRequest struct {
	CardID              string
	Verdict             string
	Fields              map[string]any
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
	Card     Card  `json:"card"`
	ChangeID int64 `json:"change_id"`
	Replayed bool  `json:"idempotency_replayed"`
}

type SubscriptionOptions struct {
	After int64
	Limit int
}

type Change struct {
	Sequence   int64          `json:"sequence"`
	CardID     string         `json:"card_id"`
	RunID      string         `json:"run_id"`
	ChangeType string         `json:"change_type"`
	Payload    map[string]any `json:"payload"`
	CreatedAt  time.Time      `json:"created_at"`
}

func (c Card) Validate() error {
	for field, value := range map[string]string{
		"card_id": c.CardID, "run_id": c.RunID, "entity_id": c.EntityID, "stage": c.Stage,
		"stage_activation_id": c.StageActivationID, "decision_id": c.DecisionID,
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
	case StatusPending, StatusDecided, StatusSuperseded:
	default:
		return fmt.Errorf("decision card status %q is invalid", c.Status)
	}
	if c.Status == StatusDecided && (strings.TrimSpace(c.Verdict) == "" || c.DecidedAt.IsZero() || strings.TrimSpace(c.DecisionEventID) == "") {
		return fmt.Errorf("decided card is missing verdict evidence")
	}
	if c.Status == StatusSuperseded && strings.TrimSpace(c.SupersededReason) == "" {
		return fmt.Errorf("superseded card is missing reason")
	}
	if strings.TrimSpace(c.Snapshot.Decision) != strings.TrimSpace(c.DecisionID) {
		return fmt.Errorf("decision card snapshot identity does not match decision_id")
	}
	if len(c.Snapshot.Outcomes) == 0 {
		return fmt.Errorf("decision card has no authored outcomes")
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
	return nil
}

func New(card Card) (Card, error) {
	now := card.CreatedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	card.CreatedAt = now
	card.UpdatedAt = now
	card.Status = StatusPending
	card.Snapshot.Decision = strings.TrimSpace(card.DecisionID)
	if strings.TrimSpace(card.Snapshot.Title) == "" {
		card.Snapshot.Title = humanize(card.DecisionID)
	}
	if card.Snapshot.Context == nil {
		card.Snapshot.Context = map[string]any{}
	}
	if card.Provenance == nil {
		card.Provenance = map[string]any{}
	}
	if strings.TrimSpace(card.EffectiveCadence.InputDraftTTL) == "" {
		card.EffectiveCadence.InputDraftTTL = DefaultInputDraftTTL.String()
	}
	if strings.TrimSpace(card.EffectiveCadence.ReminderInterval) == "" {
		card.EffectiveCadence.ReminderInterval = DefaultReminderInterval.String()
	}
	schema := map[string]any{}
	for verdict, outcome := range card.Snapshot.Outcomes {
		schema[verdict] = outcome.Input
	}
	var err error
	card.CardContentHash, err = canonicaljson.Hash(card.Snapshot)
	if err != nil {
		return Card{}, fmt.Errorf("hash decision card content: %w", err)
	}
	card.DecisionSchemaHash, err = canonicaljson.Hash(schema)
	if err != nil {
		return Card{}, fmt.Errorf("hash decision card schema: %w", err)
	}
	if err := card.Validate(); err != nil {
		return Card{}, err
	}
	return card, nil
}

func ValidateDecision(card Card, verdict string, fields map[string]any) error {
	verdict = strings.TrimSpace(verdict)
	outcome, ok := card.Snapshot.Outcomes[verdict]
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidVerdict, verdict)
	}
	if fields == nil {
		fields = map[string]any{}
	}
	for name := range fields {
		if _, declared := outcome.Input[name]; !declared {
			return fmt.Errorf("%w: undeclared field %s", ErrInvalidFields, name)
		}
	}
	for name, declaration := range outcome.Input {
		value, present := fields[name]
		if declaration.Required && (!present || value == nil || strings.TrimSpace(fmt.Sprint(value)) == "") {
			return fmt.Errorf("%w: required field %s is missing", ErrInvalidFields, name)
		}
		if !present || value == nil {
			continue
		}
		if !matchesType(value, declaration.Type) {
			return fmt.Errorf("%w: field %s must be %s", ErrInvalidFields, name, declaration.Type)
		}
	}
	return nil
}

func SnapshotJSON(card Card) ([]byte, error) {
	return canonicaljson.Bytes(card.Snapshot)
}

func DecodeSnapshot(raw []byte) (Snapshot, error) {
	var snapshot Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func matchesType(value any, kind string) bool {
	switch strings.TrimSpace(strings.ToLower(kind)) {
	case "text":
		_, ok := value.(string)
		return ok
	case "boolean", "bool":
		_, ok := value.(bool)
		return ok
	case "integer":
		switch typed := value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		case float64:
			return typed == float64(int64(typed))
		case json.Number:
			_, err := typed.Int64()
			return err == nil
		}
	case "number":
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
			return true
		}
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "list":
		_, ok := value.([]any)
		return ok
	}
	return false
}

func humanize(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "_", " "))
	if value == "" {
		return "Decision"
	}
	return strings.ToUpper(value[:1]) + value[1:]
}
