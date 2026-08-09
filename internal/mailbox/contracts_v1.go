package mailbox

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
)

var (
	ErrV1NotFound      = errors.New("mailbox item not found")
	ErrV1InvalidCursor = errors.New("invalid mailbox cursor")
)

type V1ListOptions struct {
	Status, RunID, EntityID, Type, Priority, AnchorKind string
	Limit                                               int
	Cursor                                              string
}

type V1Item struct {
	MailboxID      string         `json:"mailbox_id"`
	Type           string         `json:"type"`
	Status         string         `json:"status"`
	Priority       string         `json:"priority"`
	SourceEventID  string         `json:"source_event_id"`
	ExecutionMode  string         `json:"execution_mode,omitempty"`
	SourceRunID    string         `json:"-"`
	SourceFlow     string         `json:"source_flow"`
	SourceEntityID string         `json:"source_entity_id,omitempty"`
	Payload        map[string]any `json:"payload"`
	CreatedAt      string         `json:"created_at"`
	DecidedAt      string         `json:"decided_at,omitempty"`
	Decision       string         `json:"decision,omitempty"`
	DeferredUntil  string         `json:"deferred_until,omitempty"`
}

type V1HistoryEntry struct {
	Action          string         `json:"action"`
	ActorTokenID    string         `json:"actor_token_id"`
	TS              string         `json:"ts"`
	DecisionPayload map[string]any `json:"decision_payload,omitempty"`
	Reason          string         `json:"reason,omitempty"`
}

type V1ItemDetail struct {
	Item          V1Item           `json:"item"`
	Payload       map[string]any   `json:"payload"`
	History       []V1HistoryEntry `json:"history"`
	DecisionSheet *V1DecisionSheet `json:"decision_sheet,omitempty"`
}

type V1DecisionSheet struct {
	EntityContext     V1EntityContext     `json:"entity_context"`
	DownstreamPreview V1DownstreamPreview `json:"downstream_preview"`
}

type V1EntityContext struct {
	Available bool                             `json:"available"`
	Reason    string                           `json:"reason,omitempty"`
	Entity    *operatorread.OperatorEntityFull `json:"entity,omitempty"`
}

type V1DownstreamPreview struct {
	Available        bool     `json:"available"`
	Reason           string   `json:"reason,omitempty"`
	EventName        string   `json:"event_name,omitempty"`
	Subscribers      []string `json:"subscribers"`
	SubscriberSource string   `json:"subscriber_source"`
}

type V1Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	MailboxID string    `json:"mailbox_id"`
}

func EncodeV1Cursor(createdAt time.Time, mailboxID string) string {
	raw, _ := json.Marshal(V1Cursor{CreatedAt: createdAt.UTC(), MailboxID: strings.TrimSpace(mailboxID)})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func DecodeV1Cursor(raw string) (V1Cursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return V1Cursor{}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return V1Cursor{}, ErrV1InvalidCursor
	}
	var cursor V1Cursor
	if err := json.Unmarshal(decoded, &cursor); err != nil || cursor.CreatedAt.IsZero() || strings.TrimSpace(cursor.MailboxID) == "" {
		return V1Cursor{}, ErrV1InvalidCursor
	}
	return cursor, nil
}
