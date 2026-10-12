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
	ID                      string
	RunID                   string
	RequestEventID          string
	SourceRunID             string
	SourceRequestEventID    string
	EnclosingReplyContextID string
	RequesterFlowID         string
	RequestOutputPin        string
	ReplyInputPin           string
	ProviderFlowID          string
	ProviderInputPin        string
	ProviderOutputPin       string
	Origin                  events.RouteIdentity
	ReturnJoins             []events.JoinAdmissionReceipt
	RequestCorrelationID    string
	CorrelationKey          string
	State                   State
	AcceptedReplyEventID    string
	CreatedAt               time.Time
	UpdatedAt               time.Time
	TerminalAt              *time.Time
}

func (r Record) Normalized() Record {
	r.ID = strings.TrimSpace(r.ID)
	r.RunID = strings.TrimSpace(r.RunID)
	r.RequestEventID = strings.TrimSpace(r.RequestEventID)
	r.SourceRunID = strings.TrimSpace(r.SourceRunID)
	r.SourceRequestEventID = strings.TrimSpace(r.SourceRequestEventID)
	r.EnclosingReplyContextID = strings.TrimSpace(r.EnclosingReplyContextID)
	r.RequesterFlowID = strings.TrimSpace(r.RequesterFlowID)
	r.RequestOutputPin = strings.TrimSpace(r.RequestOutputPin)
	r.ReplyInputPin = strings.TrimSpace(r.ReplyInputPin)
	r.ProviderFlowID = strings.TrimSpace(r.ProviderFlowID)
	r.ProviderInputPin = strings.TrimSpace(r.ProviderInputPin)
	r.ProviderOutputPin = strings.TrimSpace(r.ProviderOutputPin)
	r.Origin = r.Origin.Normalized()
	r.ReturnJoins = (events.DeliveryContext{Joins: r.ReturnJoins}).Normalized().Joins
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
	if r.ID == "" || r.RequestEventID == "" || r.SourceRequestEventID == "" || r.RequesterFlowID == "" || r.RequestOutputPin == "" || r.ReplyInputPin == "" || r.ProviderFlowID == "" || r.ProviderInputPin == "" || r.ProviderOutputPin == "" || r.RequestCorrelationID == "" {
		return fmt.Errorf("reply context requires stable id, request identity, paired topology, and correlation")
	}
	if r.EnclosingReplyContextID == r.ID {
		return fmt.Errorf("reply context cannot enclose itself")
	}
	if r.Origin.Empty() {
		return fmt.Errorf("reply context origin route is required")
	}
	if err := (events.DeliveryContext{Joins: r.ReturnJoins}).Validate(); err != nil {
		return fmt.Errorf("reply context return admission: %w", err)
	}
	for _, receipt := range r.ReturnJoins {
		if receipt.Disposition == events.JoinAdmissionBound {
			entry := receipt.Ref.StageEntry()
			if entry.RunID != r.RunID || entry.InstancePath != r.Origin.FlowInstance || entry.EntityID != r.Origin.EntityID {
				return fmt.Errorf("reply return admission contradicts its retained origin")
			}
		}
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
		r.SourceRunID == other.SourceRunID &&
		r.SourceRequestEventID == other.SourceRequestEventID &&
		r.EnclosingReplyContextID == other.EnclosingReplyContextID &&
		r.RequesterFlowID == other.RequesterFlowID &&
		r.RequestOutputPin == other.RequestOutputPin &&
		r.ReplyInputPin == other.ReplyInputPin &&
		r.ProviderFlowID == other.ProviderFlowID &&
		r.ProviderInputPin == other.ProviderInputPin &&
		r.ProviderOutputPin == other.ProviderOutputPin &&
		r.Origin == other.Origin &&
		r.sameReturnJoins(other) &&
		r.RequestCorrelationID == other.RequestCorrelationID &&
		r.CorrelationKey == other.CorrelationKey
}

// Continuation may settle an open obligation once; it cannot replace its
// immutable binding, rewrite a previous settlement or manufacture earlier work.
func (r Record) ValidateContinuationFrom(previous Record, admittedAt time.Time) error {
	r, previous = r.Normalized(), previous.Normalized()
	if err := previous.Validate(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if !r.SameIdentity(previous) || !r.CreatedAt.Equal(previous.CreatedAt) ||
		admittedAt.IsZero() || r.CreatedAt.IsZero() || previous.UpdatedAt.IsZero() || previous.UpdatedAt.Before(previous.CreatedAt) || r.UpdatedAt.Before(previous.UpdatedAt) {
		return fmt.Errorf("reply continuation changed immutable or admitted lifecycle evidence")
	}
	if previous.State == StateTerminal && (previous.TerminalAt.Before(previous.CreatedAt) || !previous.TerminalAt.Equal(previous.UpdatedAt)) {
		return fmt.Errorf("retained reply settlement contradicts its lifecycle evidence")
	}
	if previous.State == StateTerminal || r.State == StateOpen {
		if r.State != previous.State || r.AcceptedReplyEventID != previous.AcceptedReplyEventID ||
			!r.UpdatedAt.Equal(previous.UpdatedAt) || !sameReplyTerminalTime(r.TerminalAt, previous.TerminalAt) {
			return fmt.Errorf("reply continuation rewrote retained lifecycle evidence")
		}
		return nil
	}
	if r.TerminalAt.Before(previous.UpdatedAt) || r.TerminalAt.Before(admittedAt) || !r.TerminalAt.Equal(r.UpdatedAt) {
		return fmt.Errorf("reply settlement precedes its admitted obligation")
	}
	return nil
}

// A continued request keeps its row-owned logical occurrence, evidence and
// return arm. Its new physical publication must match the same paired request.
func (r Record) ValidateInheritedRequestPublication(draft Record) error {
	r, draft = r.Normalized(), draft.Normalized()
	if err := r.Validate(); err != nil {
		return err
	}
	if err := draft.Validate(); err != nil {
		return err
	}
	if r.State != StateOpen || draft.State != StateOpen ||
		(draft.EnclosingReplyContextID != "" && draft.EnclosingReplyContextID != r.EnclosingReplyContextID) {
		return fmt.Errorf("inherited request changed its open enclosing obligation")
	}
	draft.ID, draft.RequestEventID = r.ID, r.RequestEventID
	draft.SourceRunID, draft.SourceRequestEventID = r.SourceRunID, r.SourceRequestEventID
	draft.EnclosingReplyContextID, draft.ReturnJoins = r.EnclosingReplyContextID, r.ReturnJoins
	if r.CorrelationKey == "" {
		draft.RequestCorrelationID = r.RequestCorrelationID
	}
	if !r.SameIdentity(draft) {
		return fmt.Errorf("inherited request publication changed its retained binding")
	}
	return nil
}

func sameReplyTerminalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

// BindEnclosing retains the inbound obligation of this request's publishing
// turn. The provider is serving that enclosing request, not a global last caller.
func (r *Record) BindEnclosing(enclosing Record) error {
	if r == nil {
		return fmt.Errorf("reply request is required")
	}
	if err := r.ValidateEnclosing(enclosing); err != nil {
		return err
	}
	enclosing = enclosing.Normalized()
	if enclosing.State != StateOpen {
		return fmt.Errorf("reply request requires its publishing turn's open enclosing obligation")
	}
	r.EnclosingReplyContextID = enclosing.ID
	return nil
}

func (r Record) ValidateEnclosing(enclosing Record) error {
	r, enclosing = r.Normalized(), enclosing.Normalized()
	if err := enclosing.Validate(); err != nil {
		return err
	}
	if enclosing.ID == r.ID || enclosing.RunID != r.RunID || enclosing.ProviderFlowID != r.RequesterFlowID {
		return fmt.Errorf("enclosing reply context contradicts the publishing requester")
	}
	if r.EnclosingReplyContextID != "" && r.EnclosingReplyContextID != enclosing.ID {
		return fmt.Errorf("reply request changed its enclosing obligation")
	}
	return nil
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

// ClaimCommand is the complete semantic input for claiming one reply context
// in the same selected-store operation that persists the reply event. The
// expected record makes stale planner reads fail closed.
type ClaimCommand struct {
	Expected     Record
	ReplyEventID string
}

func (c ClaimCommand) Normalized() ClaimCommand {
	c.Expected = c.Expected.Normalized()
	c.ReplyEventID = strings.TrimSpace(c.ReplyEventID)
	return c
}

func (c ClaimCommand) Validate() error {
	c = c.Normalized()
	if err := c.Expected.Validate(); err != nil {
		return fmt.Errorf("reply claim expected context: %w", err)
	}
	if c.ReplyEventID == "" {
		return fmt.Errorf("reply claim requires reply event id")
	}
	return nil
}

type Store interface {
	CreateReplyContext(context.Context, Record) error
	LoadReplyContext(context.Context, string) (Record, error)
	ClaimReplyContext(context.Context, string, string) (Record, ClaimOutcome, error)
}

var ErrNotFound = fmt.Errorf("reply context not found")
