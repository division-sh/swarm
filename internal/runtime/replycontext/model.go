package replycontext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

type State string

const (
	StateOpen     State = "open"
	StateTerminal State = "terminal"
)

type Record struct {
	ID                   string
	RunID                string
	RequestEventID       string
	RequesterFlowID      string
	RequestOutputPin     string
	ReplyInputPin        string
	ProviderFlowID       string
	ProviderInputPin     string
	ProviderOutputPin    string
	Origin               events.RouteIdentity
	RequestCorrelationID string
	CorrelationKey       string
	State                State
	AcceptedReplyEventID string
	CreatedAt            time.Time
	UpdatedAt            time.Time
	TerminalAt           *time.Time
}

func (r Record) Normalized() Record {
	r.ID = strings.TrimSpace(r.ID)
	r.RunID = strings.TrimSpace(r.RunID)
	r.RequestEventID = strings.TrimSpace(r.RequestEventID)
	r.RequesterFlowID = strings.TrimSpace(r.RequesterFlowID)
	r.RequestOutputPin = strings.TrimSpace(r.RequestOutputPin)
	r.ReplyInputPin = strings.TrimSpace(r.ReplyInputPin)
	r.ProviderFlowID = strings.TrimSpace(r.ProviderFlowID)
	r.ProviderInputPin = strings.TrimSpace(r.ProviderInputPin)
	r.ProviderOutputPin = strings.TrimSpace(r.ProviderOutputPin)
	r.Origin = r.Origin.Normalized()
	r.RequestCorrelationID = strings.TrimSpace(r.RequestCorrelationID)
	r.CorrelationKey = strings.TrimSpace(r.CorrelationKey)
	r.AcceptedReplyEventID = strings.TrimSpace(r.AcceptedReplyEventID)
	if r.State == "" {
		r.State = StateOpen
	}
	if !r.CreatedAt.IsZero() {
		r.CreatedAt = r.CreatedAt.UTC()
	}
	if !r.UpdatedAt.IsZero() {
		r.UpdatedAt = r.UpdatedAt.UTC()
	}
	if r.TerminalAt != nil {
		terminalAt := r.TerminalAt.UTC()
		r.TerminalAt = &terminalAt
	}
	return r
}

func (r Record) Validate() error {
	r = r.Normalized()
	if r.ID == "" || r.RequestEventID == "" || r.RequesterFlowID == "" || r.RequestOutputPin == "" || r.ReplyInputPin == "" || r.ProviderFlowID == "" || r.ProviderInputPin == "" || r.ProviderOutputPin == "" || r.RequestCorrelationID == "" {
		return fmt.Errorf("reply context requires stable id, request identity, paired topology, and correlation")
	}
	if r.Origin.Empty() {
		return fmt.Errorf("reply context origin route is required")
	}
	switch r.State {
	case StateOpen:
		if r.AcceptedReplyEventID != "" || r.TerminalAt != nil {
			return fmt.Errorf("open reply context cannot have terminal reply state")
		}
	case StateTerminal:
		if r.AcceptedReplyEventID == "" || r.TerminalAt == nil {
			return fmt.Errorf("terminal reply context requires accepted reply event and terminal time")
		}
	default:
		return fmt.Errorf("unsupported reply context state %q", r.State)
	}
	return nil
}

// SameIdentity reports whether two records describe the same immutable
// request/reply authority. Lifecycle fields and timestamps may change after
// creation and therefore are intentionally excluded.
func (r Record) SameIdentity(other Record) bool {
	r = r.Normalized()
	other = other.Normalized()
	return r.ID == other.ID &&
		r.RunID == other.RunID &&
		r.RequestEventID == other.RequestEventID &&
		r.RequesterFlowID == other.RequesterFlowID &&
		r.RequestOutputPin == other.RequestOutputPin &&
		r.ReplyInputPin == other.ReplyInputPin &&
		r.ProviderFlowID == other.ProviderFlowID &&
		r.ProviderInputPin == other.ProviderInputPin &&
		r.ProviderOutputPin == other.ProviderOutputPin &&
		r.Origin == other.Origin &&
		r.RequestCorrelationID == other.RequestCorrelationID &&
		r.CorrelationKey == other.CorrelationKey
}

func DeterministicID(requestEventID, requesterFlowID, requestOutputPin, replyInputPin, providerFlowID string, origin events.RouteIdentity) string {
	origin = origin.Normalized()
	material := strings.Join([]string{
		strings.TrimSpace(requestEventID),
		strings.TrimSpace(requesterFlowID),
		strings.TrimSpace(requestOutputPin),
		strings.TrimSpace(replyInputPin),
		strings.TrimSpace(providerFlowID),
		origin.FlowID,
		origin.FlowInstance,
		origin.EntityID,
	}, "\x00")
	sum := sha256.Sum256([]byte(material))
	return "reply-v1:" + hex.EncodeToString(sum[:])
}

type ClaimOutcome string

const (
	ClaimAccepted   ClaimOutcome = "accepted"
	ClaimIdempotent ClaimOutcome = "idempotent"
	ClaimTerminal   ClaimOutcome = "already_terminal"
)

type Store interface {
	CreateReplyContext(context.Context, Record) error
	LoadReplyContext(context.Context, string) (Record, error)
	ClaimReplyContext(context.Context, string, string) (Record, ClaimOutcome, error)
}

var ErrNotFound = fmt.Errorf("reply context not found")
