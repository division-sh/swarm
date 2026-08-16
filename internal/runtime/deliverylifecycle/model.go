package deliverylifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

var (
	ErrConflict = errors.New("delivery lifecycle conflict")
	ErrNotFound = errors.New("delivery obligation not found")
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

func deliveryRecipientForClass(class SubscriberClass, id string) (events.DeliveryRecipient, error) {
	switch class {
	case SubscriberNode:
		node, err := runtimeidentity.ParseExecutableNodeKey(strings.TrimSpace(id))
		if err != nil {
			return events.DeliveryRecipient{}, fmt.Errorf("delivery node subscriber identity: %w", err)
		}
		return events.NewNodeDeliveryRecipient(node)
	case SubscriberAgent:
		return events.NewAgentDeliveryRecipient(id)
	default:
		return events.DeliveryRecipient{}, fmt.Errorf("delivery subscriber class %q is invalid", class)
	}
}

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

func DeliveryID(eventID string, route events.DeliveryRoute) (string, error) {
	eventID = strings.TrimSpace(eventID)
	if _, err := uuid.Parse(eventID); err != nil {
		return "", fmt.Errorf("delivery obligation event id: %w", err)
	}
	identity, err := route.Normalized().Identity()
	if err != nil {
		return "", err
	}
	return uuid.NewSHA1(obligationNamespace, []byte(eventID+"\x00"+events.EncodeDeliveryRouteIdentity(identity))).String(), nil
}

type ExecutionAuthorityKind string

const (
	ExecutionAuthorityNormalRuntime        ExecutionAuthorityKind = "normal_runtime"
	ExecutionAuthoritySelectedContractFork ExecutionAuthorityKind = "selected_contract_fork"
)

// ExecutionAuthority is the immutable, closed authority under which an
// executable delivery may continue. It is deliberately distinct from routing:
// a subscriber and bundle identify work, but do not authorize its execution.
type ExecutionAuthority struct {
	kind         ExecutionAuthorityKind
	bundleSource runtimecorrelation.BundleSourceFact
	executionID  string
	forkRunID    string
	generation   uint64
}

func NewExecutionAuthority(source runtimecorrelation.BundleSourceFact, admission managedexecution.Admission) (ExecutionAuthority, error) {
	if err := source.Validate(); err != nil {
		return ExecutionAuthority{}, fmt.Errorf("delivery execution authority source: %w", err)
	}
	if err := admission.Validate(); err != nil {
		return ExecutionAuthority{}, fmt.Errorf("delivery execution admission: %w", err)
	}
	bundleHash, _ := source.StorageValues()
	if admission.BundleHash != bundleHash {
		return ExecutionAuthority{}, fmt.Errorf("delivery execution admission bundle does not match source")
	}
	authority := ExecutionAuthority{
		bundleSource: source,
		executionID:  strings.TrimSpace(admission.ExecutionAuthorityID),
		generation:   admission.Generation,
	}
	switch admission.Kind {
	case managedexecution.KindNormalRuntime:
		authority.kind = ExecutionAuthorityNormalRuntime
	case managedexecution.KindSelectedContractFork:
		authority.kind = ExecutionAuthoritySelectedContractFork
		authority.forkRunID = strings.TrimSpace(admission.RunID)
	default:
		return ExecutionAuthority{}, fmt.Errorf("delivery execution admission kind %q is invalid", admission.Kind)
	}
	return authority, authority.Validate()
}

func NewNormalExecutionAuthority(source runtimecorrelation.BundleSourceFact, executionID string, generation uint64) (ExecutionAuthority, error) {
	authority := ExecutionAuthority{
		kind: ExecutionAuthorityNormalRuntime, bundleSource: source,
		executionID: strings.TrimSpace(executionID), generation: generation,
	}
	return authority, authority.Validate()
}

func NewSelectedExecutionAuthority(source runtimecorrelation.BundleSourceFact, executionID, forkRunID string, generation uint64) (ExecutionAuthority, error) {
	authority := ExecutionAuthority{
		kind: ExecutionAuthoritySelectedContractFork, bundleSource: source,
		executionID: strings.TrimSpace(executionID), forkRunID: strings.TrimSpace(forkRunID), generation: generation,
	}
	return authority, authority.Validate()
}

func DecodeExecutionAuthority(kind ExecutionAuthorityKind, bundleHash, bundleSource, executionID, forkRunID string, generation uint64) (ExecutionAuthority, error) {
	source, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return ExecutionAuthority{}, err
	}
	authority := ExecutionAuthority{
		kind: kind, bundleSource: source, executionID: strings.TrimSpace(executionID),
		forkRunID: strings.TrimSpace(forkRunID), generation: generation,
	}
	return authority, authority.Validate()
}

func (a ExecutionAuthority) Validate() error {
	if err := a.bundleSource.Validate(); err != nil {
		return fmt.Errorf("delivery execution authority source: %w", err)
	}
	if strings.TrimSpace(a.executionID) == "" || a.generation == 0 {
		return fmt.Errorf("delivery execution authority identity is incomplete")
	}
	switch a.kind {
	case ExecutionAuthorityNormalRuntime:
		if a.forkRunID != "" {
			return fmt.Errorf("normal delivery execution authority cannot carry fork run identity")
		}
	case ExecutionAuthoritySelectedContractFork:
		if _, err := uuid.Parse(a.executionID); err != nil {
			return fmt.Errorf("selected delivery execution id: %w", err)
		}
		if _, err := uuid.Parse(a.forkRunID); err != nil {
			return fmt.Errorf("selected delivery fork run id: %w", err)
		}
	default:
		return fmt.Errorf("delivery execution authority kind %q is invalid", a.kind)
	}
	return nil
}

func (a ExecutionAuthority) Kind() ExecutionAuthorityKind { return a.kind }
func (a ExecutionAuthority) ExecutionID() string          { return a.executionID }
func (a ExecutionAuthority) ForkRunID() string            { return a.forkRunID }
func (a ExecutionAuthority) Generation() uint64           { return a.generation }
func (a ExecutionAuthority) BundleSource() runtimecorrelation.BundleSourceFact {
	return a.bundleSource
}

func (a ExecutionAuthority) Equal(other ExecutionAuthority) bool {
	return a.Validate() == nil && other.Validate() == nil &&
		a.kind == other.kind &&
		a.executionID == other.executionID &&
		a.forkRunID == other.forkRunID &&
		a.generation == other.generation &&
		a.bundleSource.Matches(other.bundleSource)
}

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
	authority     ExecutionAuthority
}

func NewObligation(eventID, runID string, route events.DeliveryRoute, authority ExecutionAuthority) (Obligation, error) {
	eventID = strings.TrimSpace(eventID)
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(eventID); err != nil {
		return Obligation{}, fmt.Errorf("delivery obligation event id: %w", err)
	}
	if _, err := uuid.Parse(runID); err != nil {
		return Obligation{}, fmt.Errorf("delivery obligation run id: %w", err)
	}
	if err := authority.Validate(); err != nil {
		return Obligation{}, err
	}
	if authority.Kind() == ExecutionAuthoritySelectedContractFork && authority.ForkRunID() != runID {
		return Obligation{}, fmt.Errorf("selected delivery authority fork run does not match obligation run")
	}
	route = route.Normalized()
	class := SubscriberNode
	if route.Recipient.IsAgent() {
		class = SubscriberAgent
	} else if !route.Recipient.IsNode() {
		return Obligation{}, fmt.Errorf("delivery obligation subscriber class is invalid")
	}
	if route.Recipient.ID() == "" {
		return Obligation{}, fmt.Errorf("delivery obligation subscriber id is required")
	}
	identity, err := route.Identity()
	if err != nil {
		return Obligation{}, err
	}
	deliveryID, err := DeliveryID(eventID, route)
	if err != nil {
		return Obligation{}, err
	}
	return Obligation{
		deliveryID: deliveryID, eventID: eventID, runID: runID,
		routeIdentity: identity, route: route, class: class, maxRetries: class.MaxRetries(), authority: authority,
	}, nil
}

func (o Obligation) DeliveryID() string                          { return o.deliveryID }
func (o Obligation) EventID() string                             { return o.eventID }
func (o Obligation) RunID() string                               { return o.runID }
func (o Obligation) RouteIdentity() events.DeliveryRouteIdentity { return o.routeIdentity }
func (o Obligation) Route() events.DeliveryRoute                 { return o.route.Normalized() }
func (o Obligation) SubscriberClass() SubscriberClass            { return o.class }
func (o Obligation) SubscriberID() string                        { return o.route.Recipient.ID() }
func (o Obligation) MaxRetries() int                             { return o.maxRetries }
func (o Obligation) Authority() ExecutionAuthority               { return o.authority }

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
	RetryScheduled   bool
	ClaimReclaimable bool
	Authority        ExecutionAuthority
}

func (s Snapshot) Terminal() bool { return s.Status.Terminal() }
func (s Snapshot) State() State   { return StateFromStatus(s.Status, s.ActiveSessionID) }

type SnapshotPage struct {
	Snapshots []Snapshot
	HasMore   bool
}

type AgentLifecyclePageQuery struct {
	AgentIdentity    agentidentity.Identity
	RunID            string
	Statuses         []Status
	BeforeCreatedAt  time.Time
	BeforeDeliveryID string
	Limit            int
}

type AgentDiagnosticPageQuery struct {
	AgentIdentity    agentidentity.Identity
	Status           Status
	BeforeOccurredAt time.Time
	BeforeDeliveryID string
	Limit            int
}

type PendingRunEventQuery struct {
	RunID              string
	Since              time.Time
	Limit              int
	ExcludedEventNames []string
}

type AgentPendingAggregate struct {
	AgentIdentity agentidentity.Identity
	Count         int
	OldestEventAt time.Time
}

type AgentPendingPageQuery struct {
	AgentIdentity agentidentity.Identity
	Since         time.Time
	Limit         int
	After         *AgentPendingPosition
}

type AgentPendingPosition struct {
	EventCreatedAt time.Time
	EventID        string
	DeliveryID     string
}

type AgentPendingReference struct {
	Snapshot       Snapshot
	EventCreatedAt time.Time
}

type AgentPendingReferencePage struct {
	References []AgentPendingReference
	HasMore    bool
}

// RunTracePageQuery selects the exact row identities needed by one bounded
// run-debug page. Snapshot hydration remains owned by Adapter.
type RunTracePageQuery struct {
	RunID              string
	Limit              int
	After              *RunTracePosition
	Since              *time.Time
	Until              *time.Time
	EventNames         []string
	EntityIDs          []string
	DeliveryStatuses   []Status
	SubscriberIDs      []string
	SubscriberClasses  []SubscriberClass
	ExcludeRuntimeLogs bool
}

type RunTracePosition struct {
	EventCreatedAt    time.Time
	EventID           string
	DeliveryCreatedAt time.Time
	DeliveryID        string
	TurnCreatedAt     time.Time
	TurnID            string
}

type RunTraceReference struct {
	EventID  string
	Delivery *Snapshot
	TurnID   string
}

type RunTraceReferencePage struct {
	References []RunTraceReference
	HasMore    bool
}

type AgentDiagnosticCounts struct {
	Failures    int
	DeadLetters int
}

// RunDiagnosticCount is an exact count of persisted delivery obligations for
// one subscriber and lifecycle status within a run.
type RunDiagnosticCount struct {
	SubscriberID string
	Status       Status
	Count        int
}

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
func (c Claim) RouteIdentity() string            { return c.routeIdentity }
func (c Claim) Version() int64                   { return c.version }
func (c Claim) SubscriberClass() SubscriberClass { return c.class }
func (c Claim) SubscriberID() string             { return c.subscriberID }

// AdmitPersistedClaim reconstructs the opaque claim capability from one exact
// selected-store compare-and-set result. Production use is confined by the
// persistence boundary guard to the private delivery adapter.
func AdmitPersistedClaim(deliveryID, runID, routeIdentity, token string, version int64, class SubscriberClass, subscriberID string) (Claim, error) {
	claim := Claim{
		deliveryID:    strings.TrimSpace(deliveryID),
		runID:         strings.TrimSpace(runID),
		routeIdentity: strings.TrimSpace(routeIdentity),
		token:         strings.TrimSpace(token),
		version:       version,
		class:         class,
		subscriberID:  strings.TrimSpace(subscriberID),
	}
	if !claim.valid() {
		return Claim{}, fmt.Errorf("persisted delivery claim is incomplete")
	}
	return claim, nil
}

// PersistenceToken returns the fencing token required by the private adapter.
// Runtime consumers deliberately receive no interface that exposes it.
func (c Claim) PersistenceToken() string { return c.token }

func (c Claim) Validate() error {
	if !c.valid() {
		return fmt.Errorf("delivery claim is incomplete")
	}
	return nil
}

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

type ClaimDisposition string

const (
	ClaimAcquired         ClaimDisposition = "acquired"
	ClaimDeferred         ClaimDisposition = "deferred"
	ClaimBusy             ClaimDisposition = "busy"
	ClaimReclaimable      ClaimDisposition = "reclaimable"
	ClaimTerminal         ClaimDisposition = "terminal"
	ClaimWrongAuthority   ClaimDisposition = "wrong_authority"
	ClaimAbsent           ClaimDisposition = "absent"
	ClaimInvariantInvalid ClaimDisposition = "invariant_invalid"
)

type ClaimResult struct {
	Disposition ClaimDisposition
	Previous    ClaimDisposition
	Snapshot    Snapshot
	Claimed     ClaimedObligation
	Invariant   error
}

func (r ClaimResult) Acquired() (ClaimedObligation, bool) {
	return r.Claimed, r.Disposition == ClaimAcquired && r.Claimed.Claim.valid()
}

type ContinuationCursor struct {
	authorityID string
	createdAt   time.Time
	deliveryID  string
	started     bool
}

// AdmitContinuationCursor records the exact monotonic selected-store position.
func AdmitContinuationCursor(authorityID string, createdAt time.Time, deliveryID string) (ContinuationCursor, error) {
	authorityID = strings.TrimSpace(authorityID)
	deliveryID = strings.TrimSpace(deliveryID)
	createdAt = createdAt.UTC()
	if authorityID == "" || deliveryID == "" || createdAt.IsZero() {
		return ContinuationCursor{}, fmt.Errorf("delivery continuation cursor is incomplete")
	}
	return ContinuationCursor{authorityID: authorityID, createdAt: createdAt, deliveryID: deliveryID, started: true}, nil
}

// Position exposes the immutable cursor facts to the private selected-store
// adapter without making them independently mutable.
func (c ContinuationCursor) Position() (authorityID string, createdAt time.Time, deliveryID string, started bool) {
	return c.authorityID, c.createdAt, c.deliveryID, c.started
}

type ContinuationItem struct {
	DeliveryID  string
	Event       events.Event
	Snapshot    Snapshot
	Disposition ClaimDisposition
	Wake        ContinuationWake
	Invariant   error
}

type ContinuationPage struct {
	Items     []ContinuationItem
	Next      ContinuationCursor
	Exhausted bool
}

// ContinuationWake is an opaque relative wake issued from one selected-store
// observation. Process clocks may arm this delay but cannot reconstruct
// eligibility from persisted timestamps.
type ContinuationWake struct {
	after   time.Duration
	present bool
}

func newContinuationWake(after time.Duration) ContinuationWake {
	if after < 0 {
		after = 0
	}
	return ContinuationWake{after: after, present: true}
}

// AdmitContinuationWake converts one selected-store eligibility observation
// into the opaque relative wake consumed by runtime scheduling.
func AdmitContinuationWake(after time.Duration) ContinuationWake {
	return newContinuationWake(after)
}

func (w ContinuationWake) After() (time.Duration, bool) {
	return w.after, w.present
}

type ContinuationObservation struct {
	DeliveryID  string
	Disposition ClaimDisposition
	Wake        ContinuationWake
	Invariant   error
}

// RecoveryInventory is the exact normal-authority bundle scope inspected
// before startup may activate or rewrite durable delivery authority.
type RecoveryInventory struct {
	Pending    int
	Failed     int
	InProgress int
}

func (i RecoveryInventory) Total() int {
	return i.Pending + i.Failed + i.InProgress
}

func (i RecoveryInventory) HasWork() bool {
	return i.Total() > 0
}

// DurableHandoffProof proves that one exact route-level obligation exists in
// the selected store. It deliberately carries no lifecycle mutation power.
type DurableHandoffProof struct {
	deliveryID    string
	eventID       string
	routeIdentity string
	authority     ExecutionAuthority
}

// AdmitDurableHandoffProof constructs proof only from exact committed storage
// facts. The architecture guard confines production calls to the private
// delivery adapter.
func AdmitDurableHandoffProof(deliveryID, eventID, routeIdentity string, authority ExecutionAuthority) (DurableHandoffProof, error) {
	proof := DurableHandoffProof{
		deliveryID:    strings.TrimSpace(deliveryID),
		eventID:       strings.TrimSpace(eventID),
		routeIdentity: strings.TrimSpace(routeIdentity),
		authority:     authority,
	}
	if err := proof.Validate(); err != nil {
		return DurableHandoffProof{}, err
	}
	return proof, nil
}

func (p DurableHandoffProof) DeliveryID() string            { return p.deliveryID }
func (p DurableHandoffProof) EventID() string               { return p.eventID }
func (p DurableHandoffProof) Authority() ExecutionAuthority { return p.authority }

func (p DurableHandoffProof) Validate() error {
	if p.deliveryID == "" || p.eventID == "" || p.routeIdentity == "" {
		return fmt.Errorf("durable delivery handoff identity is incomplete")
	}
	if err := p.authority.Validate(); err != nil {
		return err
	}
	return nil
}

func (p DurableHandoffProof) valid() bool { return p.Validate() == nil }

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

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return fmt.Errorf("delivery run summary requires run_id")
	}
	for _, count := range []int{s.Total, s.Pending, s.InProgress, s.RetryScheduled, s.Delivered, s.DeadLetter} {
		if count < 0 {
			return fmt.Errorf("delivery run summary counts cannot be negative")
		}
	}
	if s.Pending+s.InProgress+s.RetryScheduled+s.Delivered+s.DeadLetter != s.Total {
		return fmt.Errorf("delivery run summary counts do not cover total")
	}
	return nil
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
	ActivateDeliveryAuthority(context.Context, ExecutionAuthority) error
	InspectDeliveryRecovery(context.Context, runtimecorrelation.BundleSourceFact) (RecoveryInventory, error)
	ClaimDelivery(context.Context, ExecutionAuthority, events.Event, events.DeliveryRoute) (ClaimResult, error)
	ScanDeliveryContinuations(context.Context, ExecutionAuthority, ContinuationCursor, int) (ContinuationPage, error)
	ObserveDeliveryContinuation(context.Context, ExecutionAuthority, string) (ContinuationObservation, error)
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
