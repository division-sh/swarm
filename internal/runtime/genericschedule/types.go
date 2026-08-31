package genericschedule

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

const occurrenceProducerID = "runtime.generic_schedule"

type OwnerKind string

const (
	OwnerAgent  OwnerKind = "agent"
	OwnerSystem OwnerKind = "system"
)

type DueBasisKind string

const (
	DueAbsolute DueBasisKind = "absolute"
	DueDelay    DueBasisKind = "delay"
	DueCron     DueBasisKind = "cron"
	DueEvery    DueBasisKind = "every"
)

// DueBasis is immutable author intent. Relative values are resolved only by
// selected-store admission after exact-key lookup has ruled out replay.
type DueBasis struct {
	Kind     DueBasisKind
	Absolute time.Time
	Delay    time.Duration
	Cron     string
	Every    time.Duration
}

func AbsoluteDue(at time.Time) DueBasis {
	return DueBasis{Kind: DueAbsolute, Absolute: canonicalTime(at)}
}

func DelayDue(delay time.Duration) DueBasis {
	return DueBasis{Kind: DueDelay, Delay: delay}
}

func CronDue(spec string) DueBasis {
	return DueBasis{Kind: DueCron, Cron: strings.TrimSpace(spec)}
}

func EveryDue(interval time.Duration) DueBasis {
	return DueBasis{Kind: DueEvery, Every: interval}
}

func (b DueBasis) Canonical() DueBasis {
	b.Kind = DueBasisKind(strings.TrimSpace(string(b.Kind)))
	b.Absolute = canonicalTime(b.Absolute)
	b.Cron = strings.TrimSpace(b.Cron)
	return b
}

func (b DueBasis) Validate() error {
	b = b.Canonical()
	switch b.Kind {
	case DueAbsolute:
		if b.Absolute.IsZero() || b.Delay != 0 || b.Cron != "" || b.Every != 0 {
			return errors.New("absolute schedule due basis requires only an exact timestamp")
		}
	case DueDelay:
		if !b.Absolute.IsZero() || b.Delay <= 0 || b.Cron != "" || b.Every != 0 {
			return errors.New("delay schedule due basis requires only a positive duration")
		}
	case DueCron:
		if !b.Absolute.IsZero() || b.Delay != 0 || b.Cron == "" || b.Every != 0 {
			return errors.New("cron schedule due basis requires only a canonical UTC cron expression")
		}
		if strings.HasPrefix(b.Cron, "@every") {
			return errors.New("@every recurrence must use the every due-basis kind")
		}
		if _, err := cron.ParseStandard(b.Cron); err != nil {
			return fmt.Errorf("invalid UTC cron expression: %w", err)
		}
	case DueEvery:
		if !b.Absolute.IsZero() || b.Delay != 0 || b.Cron != "" || b.Every <= 0 {
			return errors.New("every schedule due basis requires only a positive exact duration")
		}
	default:
		return fmt.Errorf("generic schedule due basis kind %q is invalid", b.Kind)
	}
	return nil
}

func (b DueBasis) Recurring() bool {
	b = b.Canonical()
	return b.Kind == DueCron || b.Kind == DueEvery
}

// FirstDue resolves a new activation only. Callers must perform immutable-key
// replay lookup before invoking it for delay, cron, or every bases.
func (b DueBasis) FirstDue(admittedAt time.Time) (time.Time, error) {
	b = b.Canonical()
	admittedAt = canonicalTime(admittedAt)
	if err := b.Validate(); err != nil {
		return time.Time{}, err
	}
	if admittedAt.IsZero() {
		return time.Time{}, errors.New("generic schedule admission time is required")
	}
	switch b.Kind {
	case DueAbsolute:
		return b.Absolute, nil
	case DueDelay:
		return canonicalTime(admittedAt.Add(b.Delay)), nil
	case DueEvery:
		return canonicalTime(admittedAt.Add(b.Every)), nil
	case DueCron:
		parsed, err := cron.ParseStandard(b.Cron)
		if err != nil {
			return time.Time{}, err
		}
		return canonicalTime(parsed.Next(admittedAt)), nil
	default:
		return time.Time{}, errors.New("generic schedule due basis is invalid")
	}
}

func (b DueBasis) Next(previous time.Time) (time.Time, error) {
	b = b.Canonical()
	previous = canonicalTime(previous)
	if previous.IsZero() {
		return time.Time{}, errors.New("generic schedule recurrence requires the prior persisted coordinate")
	}
	switch b.Kind {
	case DueEvery:
		return canonicalTime(previous.Add(b.Every)), nil
	case DueCron:
		parsed, err := cron.ParseStandard(b.Cron)
		if err != nil {
			return time.Time{}, err
		}
		return canonicalTime(parsed.Next(previous)), nil
	default:
		return time.Time{}, errors.New("one-shot schedule has no next occurrence")
	}
}

type AdmissionCommand struct {
	ScheduleKey   string
	RunID         string
	EntityID      string
	FlowInstance  string
	OwnerKind     OwnerKind
	OwnerID       string
	AgentIdentity agentidentity.Identity
	EventType     string
	Payload       semanticvalue.Value
	RoutingSource events.RoutingSource
	ExecutionMode executionmode.Mode
	ReplyContext  string
	Due           DueBasis
	TaskID        string
}

func (c AdmissionCommand) Canonical() AdmissionCommand {
	c.ScheduleKey = strings.TrimSpace(c.ScheduleKey)
	c.RunID = strings.TrimSpace(c.RunID)
	c.EntityID = strings.TrimSpace(c.EntityID)
	c.FlowInstance = strings.Trim(strings.TrimSpace(c.FlowInstance), "/")
	c.OwnerKind = OwnerKind(strings.TrimSpace(string(c.OwnerKind)))
	c.OwnerID = strings.TrimSpace(c.OwnerID)
	c.AgentIdentity = c.AgentIdentity.Normalize()
	c.EventType = strings.TrimSpace(c.EventType)
	c.ExecutionMode = executionmode.Mode(strings.TrimSpace(string(c.ExecutionMode)))
	c.ReplyContext = strings.TrimSpace(c.ReplyContext)
	c.Due = c.Due.Canonical()
	c.TaskID = strings.TrimSpace(c.TaskID)
	return c
}

func (c AdmissionCommand) Validate() error {
	c = c.Canonical()
	if c.ScheduleKey == "" {
		return errors.New("schedule_key is required")
	}
	if c.OwnerID == "" || c.EventType == "" {
		return errors.New("generic schedule owner_id and event_type are required")
	}
	if c.Payload.Kind() != semanticvalue.KindObject {
		return errors.New("generic schedule payload must be a semantic object")
	}
	if !c.ExecutionMode.Valid() {
		return fmt.Errorf("generic schedule execution_mode %q is invalid", c.ExecutionMode)
	}
	if err := c.Due.Validate(); err != nil {
		return err
	}
	if c.Due.Recurring() && c.ReplyContext != "" {
		return errors.New("recurring generic schedules cannot carry reply context")
	}
	switch c.OwnerKind {
	case OwnerAgent:
		if err := c.AgentIdentity.Validate(); err != nil {
			return fmt.Errorf("agent-owned generic schedule requires concrete identity: %w", err)
		}
		if c.AgentIdentity.RunID != c.RunID ||
			c.AgentIdentity.AgentID() != c.OwnerID ||
			c.AgentIdentity.FlowInstance() != c.FlowInstance {
			return errors.New("generic schedule owner does not match concrete agent identity")
		}
	case OwnerSystem:
		if !c.AgentIdentity.IsZero() {
			return errors.New("system-owned generic schedule cannot carry agent identity")
		}
	default:
		return fmt.Errorf("generic schedule owner kind %q is invalid", c.OwnerKind)
	}
	if err := validateRoutingScope(c); err != nil {
		return err
	}
	if err := validateSystemJoinSchedule(c); err != nil {
		return err
	}
	if _, err := events.AdmitRuntimeControlEventType(events.EventType(c.EventType), c.RoutingSource); err != nil {
		return err
	}
	return nil
}

func validateSystemJoinSchedule(c AdmissionCommand) error {
	hasJoinEvent := c.EventType == "platform.join_timeout" || c.EventType == "platform.join_complete"
	payload, ok := c.Payload.Interface().(map[string]any)
	if !ok {
		if hasJoinEvent {
			return errors.New("join generic schedule requires a typed handle payload")
		}
		return nil
	}
	handle, ref, hasJoinHandle := timeridentity.ParseJoinHandle(payload)
	if !hasJoinHandle && !hasJoinEvent {
		return nil
	}
	if !hasJoinHandle || !hasJoinEvent || handle.EventType() != c.EventType {
		return errors.New("join generic schedule event does not match its typed handle")
	}
	if c.OwnerKind != OwnerSystem || c.OwnerID != "workflow-runtime" {
		return errors.New("join generic schedule requires the workflow-runtime system owner")
	}
	if c.ScheduleKey != handle.TaskID() || c.TaskID != handle.TaskID() {
		return errors.New("join generic schedule task does not match its typed handle")
	}
	route := c.RoutingSource.Route().Normalized()
	if ref.FlowID() == "" {
		if c.RoutingSource.Kind() != events.RoutingSourceRoot || route.FlowID != "" || c.FlowInstance != "" {
			return errors.New("root join generic schedule source contradicts its explicit root declaration")
		}
		return nil
	}
	if c.RoutingSource.Kind() != events.RoutingSourceFlowOwnedControl || route.FlowID != ref.FlowID() || route.FlowInstance != c.FlowInstance {
		return errors.New("flow-owned join generic schedule source contradicts its declaration")
	}
	return nil
}

func validateRoutingScope(c AdmissionCommand) error {
	switch c.RoutingSource.Kind() {
	case events.RoutingSourceRoot:
		route := c.RoutingSource.Route()
		if route.EntityID != c.EntityID || c.FlowInstance != "" || c.RunID == "" {
			return errors.New("root generic schedule source does not match run/entity scope")
		}
	case events.RoutingSourceFlowOwnedControl:
		route := c.RoutingSource.Route()
		if route.EntityID != c.EntityID || route.FlowInstance != c.FlowInstance || c.RunID == "" {
			return errors.New("flow-owned generic schedule source does not match run/entity/flow scope")
		}
	case events.RoutingSourcePlatformControl:
		if c.OwnerKind != OwnerSystem || c.RunID != "" || c.EntityID != "" || c.FlowInstance != "" {
			return errors.New("platform-control generic schedule must be global and system-owned")
		}
	default:
		return errors.New("generic schedule requires root, flow-owned, or platform-control routing source")
	}
	return nil
}

func (c AdmissionCommand) ScopeKey() (string, error) {
	c = c.Canonical()
	if err := c.Validate(); err != nil {
		return "", err
	}
	identity := agentidentity.StorageFields{}
	if !c.AgentIdentity.IsZero() {
		var err error
		identity, err = c.AgentIdentity.StorageFields()
		if err != nil {
			return "", err
		}
	}
	parts := []string{
		c.RunID, c.EntityID, c.FlowInstance, string(c.OwnerKind), c.OwnerID,
		identity.NameOwner, identity.NameSource, identity.RoutePresence,
		identity.FlowScopeKey, identity.FlowInstanceID, identity.FlowInstancePath,
	}
	return strings.Join(parts, "\x1f"), nil
}

func (c AdmissionCommand) ImmutableHash() (string, error) {
	c = c.Canonical()
	if err := c.Validate(); err != nil {
		return "", err
	}
	payload, err := canonicaljson.Encode(c.Payload)
	if err != nil {
		return "", err
	}
	routing, err := json.Marshal(c.RoutingSource)
	if err != nil {
		return "", err
	}
	agent, err := json.Marshal(c.AgentIdentity)
	if err != nil {
		return "", err
	}
	due := c.Due.Canonical()
	projection := struct {
		ScheduleKey   string             `json:"schedule_key"`
		RunID         string             `json:"run_id"`
		EntityID      string             `json:"entity_id"`
		FlowInstance  string             `json:"flow_instance"`
		OwnerKind     OwnerKind          `json:"owner_kind"`
		OwnerID       string             `json:"owner_id"`
		Agent         json.RawMessage    `json:"agent_identity"`
		EventType     string             `json:"event_type"`
		Payload       json.RawMessage    `json:"payload"`
		Routing       json.RawMessage    `json:"routing_source"`
		ExecutionMode executionmode.Mode `json:"execution_mode"`
		ReplyContext  string             `json:"reply_context_id"`
		DueKind       DueBasisKind       `json:"due_kind"`
		DueAbsolute   string             `json:"due_absolute,omitempty"`
		DueDelay      string             `json:"due_delay,omitempty"`
		DueCron       string             `json:"due_cron,omitempty"`
		DueEvery      string             `json:"due_every,omitempty"`
		TaskID        string             `json:"task_id"`
	}{
		ScheduleKey: c.ScheduleKey, RunID: c.RunID, EntityID: c.EntityID,
		FlowInstance: c.FlowInstance, OwnerKind: c.OwnerKind, OwnerID: c.OwnerID,
		Agent: agent, EventType: c.EventType, Payload: payload, Routing: routing, ExecutionMode: c.ExecutionMode,
		ReplyContext: c.ReplyContext, DueKind: due.Kind, DueAbsolute: formatTime(due.Absolute),
		DueDelay: due.Delay.String(), DueCron: due.Cron, DueEvery: due.Every.String(), TaskID: c.TaskID,
	}
	encoded, err := canonicaljson.Bytes(projection)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil
}

type Status string

const (
	StatusActive    Status = "active"
	StatusFired     Status = "fired"
	StatusCancelled Status = "cancelled"
	StatusFailed    Status = "failed"
)

type Failure struct {
	Code    string
	Message string
}

type Activation struct {
	ID                     string
	Command                AdmissionCommand
	ImmutableHash          string
	AdmittedAt             time.Time
	InitialDueAt           time.Time
	CurrentDueAt           time.Time
	CurrentEventID         string
	CurrentEventAdmittedAt time.Time
	Status                 Status
	CancelCause            string
	CancelledAt            time.Time
	FiredAt                time.Time
	AcceptedAt             time.Time
	FailedAt               time.Time
	Failure                Failure
}

func (a Activation) Canonical() Activation {
	a.ID = strings.TrimSpace(a.ID)
	a.Command = a.Command.Canonical()
	a.ImmutableHash = strings.TrimSpace(a.ImmutableHash)
	a.AdmittedAt = canonicalTime(a.AdmittedAt)
	a.InitialDueAt = canonicalTime(a.InitialDueAt)
	a.CurrentDueAt = canonicalTime(a.CurrentDueAt)
	a.CurrentEventID = strings.TrimSpace(a.CurrentEventID)
	a.CurrentEventAdmittedAt = canonicalTime(a.CurrentEventAdmittedAt)
	a.Status = Status(strings.TrimSpace(string(a.Status)))
	a.CancelCause = strings.TrimSpace(a.CancelCause)
	a.CancelledAt = canonicalTime(a.CancelledAt)
	a.FiredAt = canonicalTime(a.FiredAt)
	a.AcceptedAt = canonicalTime(a.AcceptedAt)
	a.FailedAt = canonicalTime(a.FailedAt)
	a.Failure.Code = strings.TrimSpace(a.Failure.Code)
	a.Failure.Message = strings.TrimSpace(a.Failure.Message)
	return a
}

func (a Activation) Validate() error {
	a = a.Canonical()
	if _, err := uuid.Parse(a.ID); err != nil {
		return errors.New("generic schedule activation_id must be a UUID")
	}
	if err := a.Command.Validate(); err != nil {
		return err
	}
	wantHash, err := a.Command.ImmutableHash()
	if err != nil {
		return err
	}
	if a.ImmutableHash != wantHash {
		return errors.New("generic schedule immutable hash does not match admitted facts")
	}
	if a.AdmittedAt.IsZero() || a.InitialDueAt.IsZero() || a.CurrentDueAt.IsZero() {
		return errors.New("generic schedule activation requires admitted, initial, and current due coordinates")
	}
	derived, err := a.Command.Due.FirstDue(a.AdmittedAt)
	if err != nil || !derived.Equal(a.InitialDueAt) {
		return errors.New("generic schedule initial due coordinate does not match persisted due basis")
	}
	if a.CurrentDueAt.Before(a.InitialDueAt) {
		return errors.New("generic schedule current due coordinate precedes its initial coordinate")
	}
	if !a.Command.Due.Recurring() && !a.CurrentDueAt.Equal(a.InitialDueAt) {
		return errors.New("one-shot generic schedule current due coordinate changed")
	}
	if a.Command.Due.Kind == DueEvery && a.CurrentDueAt.Sub(a.InitialDueAt)%a.Command.Due.Every != 0 {
		return errors.New("every generic schedule current due coordinate is off cadence")
	}
	hasOccurrenceID := a.CurrentEventID != ""
	hasOccurrenceAdmission := !a.CurrentEventAdmittedAt.IsZero()
	if hasOccurrenceID != hasOccurrenceAdmission {
		return errors.New("generic schedule occurrence identity and admission time must be stamped together")
	}
	hasFired := !a.FiredAt.IsZero()
	hasAccepted := !a.AcceptedAt.IsZero()
	if hasFired != hasAccepted {
		return errors.New("generic schedule fired and accepted history must be stamped together")
	}
	if hasFired && a.AcceptedAt.Before(a.FiredAt) {
		return errors.New("generic schedule fired and accepted history is not chronological")
	}
	if !a.Command.Due.Recurring() && a.Status != StatusFired && hasFired {
		return errors.New("one-shot generic schedule cannot retain accepted history before terminal firing")
	}
	hasCancelCause := a.CancelCause != ""
	hasCancelledAt := !a.CancelledAt.IsZero()
	if hasCancelCause != hasCancelledAt {
		return errors.New("generic schedule cancellation facts must be stamped together")
	}
	hasFailureCode := a.Failure.Code != ""
	hasFailedAt := !a.FailedAt.IsZero()
	if hasFailureCode != hasFailedAt || (a.Failure.Message != "" && !hasFailureCode) {
		return errors.New("generic schedule failure facts must be stamped together")
	}
	if hasCancelCause && hasFailureCode {
		return errors.New("generic schedule cancellation and failure facts are mutually exclusive")
	}
	switch a.Status {
	case StatusActive:
		if hasCancelCause || hasFailureCode {
			return errors.New("active generic schedule carries terminal facts")
		}
	case StatusFired:
		if a.Command.Due.Recurring() || !hasFired {
			return errors.New("fired generic schedule requires accepted one-shot occurrence")
		}
		if hasCancelCause || hasFailureCode {
			return errors.New("fired generic schedule carries another terminal family's facts")
		}
		if hasOccurrenceAdmission && a.AcceptedAt.Before(a.CurrentEventAdmittedAt) {
			return errors.New("fired generic schedule acceptance precedes occurrence admission")
		}
	case StatusCancelled:
		if !hasCancelCause || hasFailureCode {
			return errors.New("cancelled generic schedule requires typed cause and time")
		}
	case StatusFailed:
		if !hasFailureCode || hasCancelCause {
			return errors.New("failed generic schedule requires typed failure and time")
		}
	default:
		return fmt.Errorf("generic schedule status %q is invalid", a.Status)
	}
	if a.CurrentEventID != "" && a.CurrentEventID != OccurrenceEventID(a.ID, a.CurrentDueAt) {
		return errors.New("generic schedule occurrence event identity is not deterministic")
	}
	return nil
}

func (a Activation) Wakeup() (Wakeup, error) {
	a = a.Canonical()
	if err := a.Validate(); err != nil {
		return Wakeup{}, err
	}
	if a.Status != StatusActive {
		return Wakeup{}, errors.New("only an active generic schedule has a wakeup")
	}
	return NewWakeup(a.ID, a.CurrentDueAt)
}

type AdmissionOutcome string

const (
	AdmissionCreated     AdmissionOutcome = "created"
	AdmissionExactReplay AdmissionOutcome = "exact_replay"
)

type AdmissionResult struct {
	Outcome    AdmissionOutcome
	Activation Activation
}

func (r AdmissionResult) Validate() error {
	if r.Outcome != AdmissionCreated && r.Outcome != AdmissionExactReplay {
		return fmt.Errorf("generic schedule admission outcome %q is invalid", r.Outcome)
	}
	return r.Activation.Validate()
}

type ConflictError struct {
	ScopeKey    string
	ScheduleKey string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("generic schedule %q conflicts with immutable activation in scope %q", e.ScheduleKey, e.ScopeKey)
}

func IsConflict(err error) bool {
	var conflict *ConflictError
	return errors.As(err, &conflict)
}

type Wakeup struct {
	activationID string
	dueAt        time.Time
}

func NewWakeup(activationID string, dueAt time.Time) (Wakeup, error) {
	wakeup := Wakeup{activationID: strings.TrimSpace(activationID), dueAt: canonicalTime(dueAt)}
	if err := wakeup.Validate(); err != nil {
		return Wakeup{}, err
	}
	return wakeup, nil
}

func (w Wakeup) ActivationID() string { return strings.TrimSpace(w.activationID) }
func (w Wakeup) DueAt() time.Time     { return canonicalTime(w.dueAt) }

func (w Wakeup) Validate() error {
	if _, err := uuid.Parse(w.ActivationID()); err != nil || w.DueAt().IsZero() {
		return errors.New("generic schedule wakeup requires activation UUID and exact due coordinate")
	}
	return nil
}

type Occurrence struct {
	ActivationID string
	DueAt        time.Time
	EventID      string
	AdmittedAt   time.Time
}

func (o Occurrence) Canonical() Occurrence {
	o.ActivationID = strings.TrimSpace(o.ActivationID)
	o.DueAt = canonicalTime(o.DueAt)
	o.EventID = strings.TrimSpace(o.EventID)
	o.AdmittedAt = canonicalTime(o.AdmittedAt)
	return o
}

func (o Occurrence) Validate() error {
	o = o.Canonical()
	if _, err := uuid.Parse(o.ActivationID); err != nil || o.DueAt.IsZero() || o.AdmittedAt.IsZero() {
		return errors.New("generic schedule occurrence requires activation UUID, due coordinate, and admission time")
	}
	if o.EventID != OccurrenceEventID(o.ActivationID, o.DueAt) {
		return errors.New("generic schedule occurrence event identity is invalid")
	}
	return nil
}

func OccurrenceEventID(activationID string, dueAt time.Time) string {
	name := strings.TrimSpace(activationID) + "\x1f" + formatTime(dueAt)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:generic-schedule-occurrence:"+name)).String()
}

func OccurrenceProducerID() string { return occurrenceProducerID }

type PrepareOutcome string

const (
	PrepareReady          PrepareOutcome = "ready"
	PrepareTerminal       PrepareOutcome = "terminal"
	PrepareStaleCancelled PrepareOutcome = "stale_cancelled"
)

type PreparedOccurrence struct {
	Outcome    PrepareOutcome
	Activation Activation
	Occurrence Occurrence
}

func (r PreparedOccurrence) Validate() error {
	switch r.Outcome {
	case PrepareTerminal, PrepareStaleCancelled:
		if r.Occurrence != (Occurrence{}) {
			return errors.New("terminal occurrence preparation cannot carry occurrence evidence")
		}
		return nil
	case PrepareReady:
		if err := r.Activation.Validate(); err != nil {
			return err
		}
		if r.Activation.Status != StatusActive {
			return errors.New("ready occurrence requires active activation")
		}
		if err := r.Occurrence.Validate(); err != nil {
			return err
		}
		if r.Occurrence.ActivationID != r.Activation.ID || !r.Occurrence.DueAt.Equal(r.Activation.CurrentDueAt) {
			return errors.New("prepared occurrence does not match activation coordinate")
		}
		return nil
	default:
		return fmt.Errorf("generic schedule prepare outcome %q is invalid", r.Outcome)
	}
}

type CancelCommand struct {
	ActivationID string
	Cause        string
	CancelledAt  time.Time
}

func (c CancelCommand) Canonical() CancelCommand {
	c.ActivationID = strings.TrimSpace(c.ActivationID)
	c.Cause = strings.TrimSpace(c.Cause)
	c.CancelledAt = canonicalTime(c.CancelledAt)
	return c
}

func (c CancelCommand) Validate() error {
	c = c.Canonical()
	if _, err := uuid.Parse(c.ActivationID); err != nil || c.Cause == "" || c.CancelledAt.IsZero() {
		return errors.New("generic schedule cancellation requires activation UUID, typed cause, and time")
	}
	return nil
}

type CancelOutcome string

const (
	CancelChanged  CancelOutcome = "cancelled"
	CancelTerminal CancelOutcome = "terminal"
	CancelMissing  CancelOutcome = "missing"
)

type CancelResult struct {
	Outcome    CancelOutcome
	Activation Activation
}

type CommitOutcome string

const (
	CommitCommitted      CommitOutcome = "committed"
	CommitRetry          CommitOutcome = "retry"
	CommitTerminal       CommitOutcome = "terminal"
	CommitStaleCancelled CommitOutcome = "stale_cancelled"
)

func canonicalTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Truncate(time.Microsecond)
}

func formatTime(value time.Time) string {
	value = canonicalTime(value)
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}
