package deliverylifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

var (
	ErrConflict   = errors.New("delivery lifecycle conflict")
	ErrNotFound   = errors.New("delivery obligation not found")
	ErrIneligible = errors.New("delivery obligation is not eligible")
)

const (
	AgentMaxRetries = 1
	NodeMaxRetries  = 3
	DefaultLeaseTTL = 5 * time.Minute
)

type SubscriberClass string

const (
	SubscriberAgent SubscriberClass = "agent"
	SubscriberNode  SubscriberClass = "node"
)

func ParseSubscriberClass(raw string) (SubscriberClass, error) {
	class := SubscriberClass(strings.TrimSpace(raw))
	switch class {
	case SubscriberAgent, SubscriberNode:
		return class, nil
	default:
		return "", fmt.Errorf("delivery subscriber class %q is invalid", raw)
	}
}

func (c SubscriberClass) MaxRetries() int {
	switch c {
	case SubscriberAgent:
		return AgentMaxRetries
	case SubscriberNode:
		return NodeMaxRetries
	default:
		return -1
	}
}

type Status string

const (
	StatusPending    Status = "pending"
	StatusInProgress Status = "in_progress"
	StatusDelivered  Status = "delivered"
	StatusFailed     Status = "failed"
	StatusDeadLetter Status = "dead_letter"
)

func ParseStatus(raw string) (Status, error) {
	status := Status(strings.TrimSpace(raw))
	switch status {
	case StatusPending, StatusInProgress, StatusDelivered, StatusFailed, StatusDeadLetter:
		return status, nil
	default:
		return "", fmt.Errorf("delivery status %q is invalid", raw)
	}
}

func (s Status) Terminal() bool { return s == StatusDelivered || s == StatusDeadLetter }

type State string

const (
	StateQueued    State = "queued"
	StateLaunching State = "launching"
	StateActive    State = "active"
	StateRetrying  State = "retrying"
	StateDelivered State = "delivered"
	StateExhausted State = "exhausted"
)

type Transition struct {
	DeliveryID     string
	EventID        string
	SubscriberType SubscriberClass
	SubscriberID   string
	EntityID       string
	State          State
	PreviousState  State
	Reason         string
	Failure        *runtimefailures.Envelope
	RetryCount     int
}

func StateFromStatus(status Status, activeSessionID string) State {
	switch status {
	case StatusPending:
		return StateQueued
	case StatusInProgress:
		if strings.TrimSpace(activeSessionID) != "" {
			return StateActive
		}
		return StateLaunching
	case StatusFailed:
		return StateRetrying
	case StatusDelivered:
		return StateDelivered
	case StatusDeadLetter:
		return StateExhausted
	default:
		return ""
	}
}

// StateFromDelivery is retained as a presentation decoder while callers move
// to Snapshot. It rejects unknown persisted states rather than guessing.
func StateFromDelivery(status, activeSessionID string) (State, bool) {
	parsed, err := ParseStatus(status)
	if err != nil {
		return "", false
	}
	return StateFromStatus(parsed, activeSessionID), true
}

var obligationNamespace = uuid.MustParse("8f9a1200-f087-5adb-93d2-fd41bb3b6d9a")

// Obligation is the only valid construction input for an executable delivery
// row. Its identity is deterministic over the admitted event and exact route.
type Obligation struct {
	deliveryID    string
	eventID       string
	runID         string
	routeIdentity events.DeliveryRouteIdentity
	route         events.DeliveryRoute
	class         SubscriberClass
	maxRetries    int
}

func NewObligation(eventID, runID string, route events.DeliveryRoute) (Obligation, error) {
	eventID = strings.TrimSpace(eventID)
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(eventID); err != nil {
		return Obligation{}, fmt.Errorf("delivery obligation event id: %w", err)
	}
	if _, err := uuid.Parse(runID); err != nil {
		return Obligation{}, fmt.Errorf("delivery obligation run id: %w", err)
	}
	route = route.Normalized()
	class, err := ParseSubscriberClass(route.SubscriberType)
	if err != nil {
		return Obligation{}, err
	}
	if route.SubscriberID == "" {
		return Obligation{}, fmt.Errorf("delivery obligation subscriber id is required")
	}
	identity, err := route.Identity()
	if err != nil {
		return Obligation{}, err
	}
	deliveryID := uuid.NewSHA1(obligationNamespace, []byte(eventID+"\x00"+identity.String())).String()
	return Obligation{
		deliveryID: deliveryID, eventID: eventID, runID: runID,
		routeIdentity: identity, route: route, class: class, maxRetries: class.MaxRetries(),
	}, nil
}

func (o Obligation) DeliveryID() string                          { return o.deliveryID }
func (o Obligation) EventID() string                             { return o.eventID }
func (o Obligation) RunID() string                               { return o.runID }
func (o Obligation) RouteIdentity() events.DeliveryRouteIdentity { return o.routeIdentity }
func (o Obligation) Route() events.DeliveryRoute                 { return o.route.Normalized() }
func (o Obligation) SubscriberClass() SubscriberClass            { return o.class }
func (o Obligation) SubscriberID() string                        { return o.route.SubscriberID }
func (o Obligation) MaxRetries() int                             { return o.maxRetries }

type Snapshot struct {
	DeliveryID       string
	EventID          string
	RunID            string
	RouteIdentity    events.DeliveryRouteIdentity
	Route            events.DeliveryRoute
	SubscriberClass  SubscriberClass
	SubscriberID     string
	Status           Status
	RetryCount       int
	MaxRetries       int
	NextEligibleAt   time.Time
	ClaimVersion     int64
	ClaimExpiresAt   time.Time
	ActiveSessionID  string
	ReasonCode       string
	Failure          *runtimefailures.Envelope
	StartedAt        time.Time
	SettledAt        time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	RetryEligible    bool
	ClaimReclaimable bool
}

func (s Snapshot) Terminal() bool { return s.Status.Terminal() }
func (s Snapshot) State() State   { return StateFromStatus(s.Status, s.ActiveSessionID) }

// Claim is a fenced durable capability. It has no public constructor and its
// identity cannot be recovered from a Snapshot.
type Claim struct {
	deliveryID    string
	runID         string
	routeIdentity string
	token         string
	version       int64
	class         SubscriberClass
	subscriberID  string
}

func (c Claim) DeliveryID() string               { return c.deliveryID }
func (c Claim) RunID() string                    { return c.runID }
func (c Claim) Version() int64                   { return c.version }
func (c Claim) SubscriberClass() SubscriberClass { return c.class }
func (c Claim) SubscriberID() string             { return c.subscriberID }

func (c Claim) valid() bool {
	return strings.TrimSpace(c.deliveryID) != "" && strings.TrimSpace(c.runID) != "" && strings.TrimSpace(c.routeIdentity) != "" &&
		strings.TrimSpace(c.token) != "" && c.version > 0 && c.class.MaxRetries() >= 0 && strings.TrimSpace(c.subscriberID) != ""
}

type claimContextKey struct{}

func WithClaim(ctx context.Context, claim Claim) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, claimContextKey{}, claim)
}

// WithoutClaim starts a distinct delivery execution boundary. A claim is an
// exact capability for one obligation and must not flow into child delivery
// dispatch created by the current handler.
func WithoutClaim(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, claimContextKey{}, Claim{})
}

func ClaimFromContext(ctx context.Context) (Claim, bool) {
	if ctx == nil {
		return Claim{}, false
	}
	claim, ok := ctx.Value(claimContextKey{}).(Claim)
	return claim, ok && claim.valid()
}

type routeContextKey struct{}

func WithRoute(ctx context.Context, route events.DeliveryRoute) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, routeContextKey{}, route.Normalized())
}

func RouteFromContext(ctx context.Context) (events.DeliveryRoute, bool) {
	if ctx == nil {
		return events.DeliveryRoute{}, false
	}
	route, ok := ctx.Value(routeContextKey{}).(events.DeliveryRoute)
	if !ok {
		return events.DeliveryRoute{}, false
	}
	_, err := route.Identity()
	return route.Normalized(), err == nil
}

type ClaimedObligation struct {
	Snapshot Snapshot
	Claim    Claim
}

// DurableHandoffProof proves that one exact route-level obligation exists in
// the selected store. It deliberately carries no lifecycle mutation power.
type DurableHandoffProof struct {
	deliveryID    string
	eventID       string
	routeIdentity string
}

func (p DurableHandoffProof) DeliveryID() string { return p.deliveryID }

func (p DurableHandoffProof) valid() bool {
	return p.deliveryID != "" && p.eventID != "" && p.routeIdentity != ""
}

type FailureDisposition string

const (
	FailureRetry      FailureDisposition = "retry"
	FailureDeadLetter FailureDisposition = "dead_letter"
)

type Settlement struct {
	Disposition FailureDisposition
	ReasonCode  string
	Failure     *runtimefailures.Envelope
	SideEffects []string
	Duration    time.Duration
	RetryBase   time.Duration
}

type Outcome struct {
	DeliveryID   string
	ClaimVersion int64
	Outcome      string
	ReasonCode   string
	Failure      *runtimefailures.Envelope
	SideEffects  []string
	Duration     time.Duration
	SettledAt    time.Time
}

type RunSummary struct {
	RunID             string
	Total             int
	Pending           int
	InProgress        int
	RetryScheduled    int
	Delivered         int
	DeadLetter        int
	NextEligibleAt    time.Time
	ActiveDeliveryIDs []string
}

type Terminalization struct {
	Previous Snapshot
	Current  Snapshot
}

func (s RunSummary) Settled() bool {
	return s.Pending == 0 && s.InProgress == 0 && s.RetryScheduled == 0
}

type AgentExecution struct {
	Event    events.Event
	Snapshot Snapshot
	Claim    Claim
}

type NodeExecution = AgentExecution

// Store is the narrow selected-store semantic port consumed by runtime code.
// Raw rows, status strings, SQL transactions, and caller-selected retry limits
// do not cross this boundary.
type Store interface {
	ClaimAgentDelivery(context.Context, events.Event, events.DeliveryRoute) (ClaimedObligation, error)
	ClaimAgentBacklog(context.Context, string, int) ([]AgentExecution, error)
	ClaimNodeDelivery(context.Context, events.Event, events.DeliveryRoute) (ClaimedObligation, error)
	ClaimNodeBacklog(context.Context, string, int) ([]NodeExecution, error)
	RenewClaim(context.Context, Claim) (Snapshot, error)
	BindAgentSession(context.Context, Claim, string) (Snapshot, error)
	SettleSuccess(context.Context, Claim, []string, time.Duration) (Snapshot, error)
	SettleFailure(context.Context, Claim, Settlement) (Snapshot, error)
	Snapshot(context.Context, string) (Snapshot, error)
	Outcomes(context.Context, string) ([]Outcome, error)
	ProveHandoff(context.Context, string, events.DeliveryRoute) (DurableHandoffProof, error)
	SummarizeRun(context.Context, string) (RunSummary, error)
	TerminalizeRun(context.Context, string, string) ([]Terminalization, error)
}
