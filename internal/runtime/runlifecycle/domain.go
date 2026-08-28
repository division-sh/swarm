package runlifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type State string

const (
	StateRunning   State = "running"
	StatePaused    State = "paused"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
	StateForked    State = "forked"
)

const (
	BundleSourcePersisted = "persisted"
	BundleSourceEphemeral = "ephemeral"
	BundleSourceDeleted   = "deleted"
)

type OriginKind string

const (
	OriginEvent               OriginKind = "event"
	OriginScenarioSetup       OriginKind = "scenario_setup"
	OriginStandingGeneration  OriginKind = "standing_generation"
	OriginForkMaterialization OriginKind = "fork_materialization"
)

const (
	ScenarioSetupOriginType       = "test.setup_entities"
	StandingGenerationOriginType  = "standing.generation"
	ForkMaterializationOriginType = "run.fork"
)

type RunOrigin struct {
	kind          OriginKind
	eventID       string
	eventType     string
	serviceID     string
	generation    int64
	sourceRunID   string
	sourceEventID string
}

func EventRunOrigin(eventID, eventType string) (RunOrigin, error) {
	origin := RunOrigin{
		kind:      OriginEvent,
		eventID:   strings.TrimSpace(eventID),
		eventType: strings.TrimSpace(eventType),
	}
	if err := origin.Validate(); err != nil {
		return RunOrigin{}, err
	}
	return origin, nil
}

func ScenarioSetupRunOrigin() RunOrigin {
	return RunOrigin{kind: OriginScenarioSetup}
}

func StandingGenerationRunOrigin(serviceID string, generation int64) (RunOrigin, error) {
	origin := RunOrigin{
		kind:       OriginStandingGeneration,
		serviceID:  strings.TrimSpace(serviceID),
		generation: generation,
	}
	if err := origin.Validate(); err != nil {
		return RunOrigin{}, err
	}
	return origin, nil
}

func ForkMaterializationRunOrigin(sourceRunID, sourceEventID string) (RunOrigin, error) {
	origin := RunOrigin{
		kind:          OriginForkMaterialization,
		sourceRunID:   strings.TrimSpace(sourceRunID),
		sourceEventID: strings.TrimSpace(sourceEventID),
	}
	if err := origin.Validate(); err != nil {
		return RunOrigin{}, err
	}
	return origin, nil
}

func DecodeRunOrigin(
	kind, eventID, eventType, serviceID string,
	generation int64,
	sourceRunID, sourceEventID string,
) (RunOrigin, error) {
	origin := RunOrigin{
		kind:          OriginKind(strings.TrimSpace(kind)),
		eventID:       strings.TrimSpace(eventID),
		eventType:     strings.TrimSpace(eventType),
		serviceID:     strings.TrimSpace(serviceID),
		generation:    generation,
		sourceRunID:   strings.TrimSpace(sourceRunID),
		sourceEventID: strings.TrimSpace(sourceEventID),
	}
	if err := origin.Validate(); err != nil {
		return RunOrigin{}, err
	}
	return origin, nil
}

func (o RunOrigin) Validate() error {
	switch o.kind {
	case OriginEvent:
		if o.eventID == "" || o.eventType == "" {
			return errors.New("event run origin requires event_id and event_type")
		}
		if o.serviceID != "" || o.generation != 0 || o.sourceRunID != "" || o.sourceEventID != "" {
			return errors.New("event run origin forbids standing and fork identity")
		}
	case OriginScenarioSetup:
		if o.eventID != "" || o.eventType != "" || o.serviceID != "" || o.generation != 0 || o.sourceRunID != "" || o.sourceEventID != "" {
			return errors.New("scenario setup run origin forbids event, standing, and fork identity")
		}
	case OriginStandingGeneration:
		if o.serviceID == "" || o.generation <= 0 {
			return errors.New("standing generation run origin requires service_id and positive generation")
		}
		if o.eventID != "" || o.eventType != "" || o.sourceRunID != "" || o.sourceEventID != "" {
			return errors.New("standing generation run origin forbids event and fork identity")
		}
	case OriginForkMaterialization:
		if o.sourceRunID == "" || o.sourceEventID == "" {
			return errors.New("fork materialization run origin requires source_run_id and source_event_id")
		}
		if o.eventID != "" || o.eventType != "" || o.serviceID != "" || o.generation != 0 {
			return errors.New("fork materialization run origin forbids event and standing identity")
		}
	default:
		return fmt.Errorf("invalid run origin kind %q", o.kind)
	}
	return nil
}

func (o RunOrigin) Kind() OriginKind           { return o.kind }
func (o RunOrigin) EventID() string            { return o.eventID }
func (o RunOrigin) EventType() string          { return o.eventType }
func (o RunOrigin) ServiceID() string          { return o.serviceID }
func (o RunOrigin) Generation() int64          { return o.generation }
func (o RunOrigin) SourceRunID() string        { return o.sourceRunID }
func (o RunOrigin) SourceEventID() string      { return o.sourceEventID }
func (o RunOrigin) Equal(other RunOrigin) bool { return o == other }

func (o RunOrigin) ActivityTriggerType() string {
	switch o.kind {
	case OriginEvent:
		return o.eventType
	case OriginScenarioSetup:
		return ScenarioSetupOriginType
	case OriginStandingGeneration:
		return StandingGenerationOriginType
	case OriginForkMaterialization:
		return ForkMaterializationOriginType
	default:
		return ""
	}
}

func (o RunOrigin) MarshalJSON() ([]byte, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	type encodedOrigin struct {
		Kind          OriginKind `json:"kind"`
		EventID       string     `json:"event_id,omitempty"`
		EventType     string     `json:"event_type,omitempty"`
		ServiceID     string     `json:"service_id,omitempty"`
		Generation    int64      `json:"generation,omitempty"`
		SourceRunID   string     `json:"source_run_id,omitempty"`
		SourceEventID string     `json:"source_event_id,omitempty"`
	}
	return json.Marshal(encodedOrigin{
		Kind: o.kind, EventID: o.eventID, EventType: o.eventType,
		ServiceID: o.serviceID, Generation: o.generation,
		SourceRunID: o.sourceRunID, SourceEventID: o.sourceEventID,
	})
}

func (o *RunOrigin) UnmarshalJSON(raw []byte) error {
	if o == nil {
		return errors.New("run origin target is nil")
	}
	var encoded struct {
		Kind          OriginKind `json:"kind"`
		EventID       string     `json:"event_id"`
		EventType     string     `json:"event_type"`
		ServiceID     string     `json:"service_id"`
		Generation    int64      `json:"generation"`
		SourceRunID   string     `json:"source_run_id"`
		SourceEventID string     `json:"source_event_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&encoded); err != nil {
		return fmt.Errorf("decode run origin: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("decode run origin: trailing JSON value")
		}
		return fmt.Errorf("decode run origin: %w", err)
	}
	origin, err := DecodeRunOrigin(
		string(encoded.Kind),
		encoded.EventID,
		encoded.EventType,
		encoded.ServiceID,
		encoded.Generation,
		encoded.SourceRunID,
		encoded.SourceEventID,
	)
	if err != nil {
		return err
	}
	*o = origin
	return nil
}

func ParseState(raw string) (State, error) {
	state := State(strings.ToLower(strings.TrimSpace(raw)))
	switch state {
	case StateRunning, StatePaused, StateCompleted, StateFailed, StateCancelled, StateForked:
		return state, nil
	default:
		return "", fmt.Errorf("invalid run lifecycle state %q", raw)
	}
}

func (s State) Terminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateCancelled, StateForked:
		return true
	default:
		return false
	}
}

func (s State) Active() bool {
	return s == StateRunning || s == StatePaused
}

func ActiveStates() [2]State {
	return [2]State{StateRunning, StatePaused}
}

func ValidateStateFailure(state State, failure *runtimefailures.Envelope) error {
	if _, err := ParseState(string(state)); err != nil {
		return err
	}
	if state == StateFailed {
		if failure == nil {
			return errors.New("failed run requires canonical failure")
		}
		if err := runtimefailures.ValidateEnvelope(*failure); err != nil {
			return fmt.Errorf("failed run failure is invalid: %w", err)
		}
		return nil
	}
	if failure != nil {
		return fmt.Errorf("run lifecycle state %s forbids failure", state)
	}
	return nil
}

func ValidatePersistedState(raw string, failure *runtimefailures.Envelope) (State, error) {
	state, err := ParseState(raw)
	if err != nil {
		return "", err
	}
	if err := ValidateStateFailure(state, failure); err != nil {
		return "", err
	}
	return state, nil
}

func ParseTerminalState(raw string) (State, error) {
	state, err := ParseState(raw)
	if err != nil {
		return "", err
	}
	if !state.Terminal() {
		return "", fmt.Errorf("run lifecycle state %s is not terminal", state)
	}
	return state, nil
}

var (
	ErrRunNotFound                = errors.New("run not found")
	ErrRunNotActive               = errors.New("run is not active")
	ErrPersistedBundleUnavailable = errors.New("persisted bundle source unavailable")
	ErrForkSourceUnsupported      = errors.New("run lifecycle fork source transition is unsupported")
)

type RunNotFoundError struct {
	RunID string
}

func (e *RunNotFoundError) Error() string {
	if e == nil {
		return ErrRunNotFound.Error()
	}
	return fmt.Sprintf("%s: run_id=%s", ErrRunNotFound, strings.TrimSpace(e.RunID))
}

func (e *RunNotFoundError) Unwrap() error {
	return ErrRunNotFound
}

type RunNotActiveError struct {
	RunID string
	State State
}

func (e *RunNotActiveError) Error() string {
	if e == nil {
		return ErrRunNotActive.Error()
	}
	return fmt.Sprintf("%s: run_id=%s state=%s", ErrRunNotActive, strings.TrimSpace(e.RunID), e.State)
}

func (e *RunNotActiveError) Unwrap() error {
	return ErrRunNotActive
}

type PersistedBundleUnavailableError struct {
	BundleHash   string
	BundleSource string
	Cause        string
}

func (e *PersistedBundleUnavailableError) Error() string {
	if e == nil {
		return ErrPersistedBundleUnavailable.Error()
	}
	parts := []string{ErrPersistedBundleUnavailable.Error()}
	if value := strings.TrimSpace(e.BundleHash); value != "" {
		parts = append(parts, "bundle_hash="+value)
	}
	if value := strings.TrimSpace(e.BundleSource); value != "" {
		parts = append(parts, "bundle_source="+value)
	}
	if value := strings.TrimSpace(e.Cause); value != "" {
		parts = append(parts, "cause="+value)
	}
	return strings.Join(parts, " ")
}

func (e *PersistedBundleUnavailableError) Unwrap() error {
	return ErrPersistedBundleUnavailable
}

type MutationDisposition string

const (
	MutationApplied   MutationDisposition = "applied"
	MutationExactNoop MutationDisposition = "exact_noop"
)

// CanonicalTimestamp admits lifecycle time to the precision shared by both
// selected stores before validation, persistence, and exact replay comparison.
func CanonicalTimestamp(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Round(time.Microsecond)
}

type TerminalRequest struct {
	RunID            string
	State            State
	Failure          *runtimefailures.Envelope
	ContinuedAsRunID string
	EndedAt          time.Time
}

func (r TerminalRequest) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return errors.New("terminal run lifecycle mutation requires run_id")
	}
	if r.State != StateFailed && r.State != StateCancelled {
		return fmt.Errorf("explicit terminal run lifecycle mutation requires failed or cancelled state, got %s", r.State)
	}
	if err := ValidateStateFailure(r.State, r.Failure); err != nil {
		return err
	}
	if continuedAsRunID := strings.TrimSpace(r.ContinuedAsRunID); continuedAsRunID != "" {
		return fmt.Errorf("run lifecycle state %s forbids continued_as_run_id", r.State)
	}
	if r.EndedAt.IsZero() {
		return errors.New("terminal run lifecycle mutation requires ended_at")
	}
	return nil
}

type ForkSourceRequest struct {
	RunID            string
	ContinuedAsRunID string
	EndedAt          time.Time
}

func (r ForkSourceRequest) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return errors.New("fork source lifecycle mutation requires run_id")
	}
	if strings.TrimSpace(r.ContinuedAsRunID) == "" {
		return errors.New("fork source lifecycle mutation requires continued_as_run_id")
	}
	if strings.TrimSpace(r.RunID) == strings.TrimSpace(r.ContinuedAsRunID) {
		return errors.New("fork source lifecycle continuation must reference a different run")
	}
	if r.EndedAt.IsZero() {
		return errors.New("fork source lifecycle mutation requires ended_at")
	}
	return nil
}

type ActiveTransitionRequest struct {
	RunID string
	State State
}

func (r ActiveTransitionRequest) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return errors.New("active run lifecycle transition requires run_id")
	}
	if r.State != StateRunning && r.State != StatePaused {
		return fmt.Errorf("active run lifecycle transition requires running or paused state, got %s", r.State)
	}
	return nil
}

type CreateRequest struct {
	RunID     string
	Origin    RunOrigin
	Source    runtimecorrelation.BundleSourceFact
	StartedAt time.Time
}

func (r CreateRequest) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return errors.New("run creation requires run_id")
	}
	if err := r.Source.Validate(); err != nil {
		return fmt.Errorf("run creation source: %w", err)
	}
	if err := r.Origin.Validate(); err != nil {
		return fmt.Errorf("run creation origin: %w", err)
	}
	if r.StartedAt.IsZero() {
		return errors.New("run creation requires started_at")
	}
	return nil
}

type SourceRevisionRequest struct {
	RunID  string
	Source runtimecorrelation.BundleSourceFact
}

func (r SourceRevisionRequest) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return errors.New("run source revision requires run_id")
	}
	if err := r.Source.Validate(); err != nil {
		return fmt.Errorf("run source revision: %w", err)
	}
	return nil
}

type Snapshot struct {
	RunID            string
	State            State
	Origin           RunOrigin
	BundleHash       string
	BundleSource     string
	EventCount       int
	EntityCount      int
	Failure          *runtimefailures.Envelope
	ContinuedAsRunID string
	StartedAt        time.Time
	EndedAt          *time.Time
}

func (s Snapshot) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return errors.New("run lifecycle snapshot requires run_id")
	}
	if _, err := ParseState(string(s.State)); err != nil {
		return err
	}
	if err := s.Origin.Validate(); err != nil {
		return fmt.Errorf("run lifecycle snapshot origin: %w", err)
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(strings.TrimSpace(s.BundleHash)); err != nil {
		return fmt.Errorf("run lifecycle snapshot bundle_hash: %w", err)
	}
	switch strings.TrimSpace(s.BundleSource) {
	case BundleSourcePersisted, BundleSourceEphemeral, BundleSourceDeleted:
	default:
		return fmt.Errorf("run lifecycle snapshot has invalid bundle_source %q", s.BundleSource)
	}
	if s.EventCount < 0 || s.EntityCount < 0 {
		return errors.New("run lifecycle snapshot counters must be non-negative")
	}
	if s.StartedAt.IsZero() {
		return errors.New("run lifecycle snapshot requires started_at")
	}
	if s.State == StateFailed {
		if s.Failure == nil {
			return errors.New("failed run requires canonical failure")
		}
		if err := runtimefailures.ValidateEnvelope(*s.Failure); err != nil {
			return fmt.Errorf("failed run failure is invalid: %w", err)
		}
	} else if s.Failure != nil {
		return fmt.Errorf("run lifecycle state %s forbids failure", s.State)
	}
	if s.State == StateForked && strings.TrimSpace(s.ContinuedAsRunID) == "" {
		return errors.New("forked run lifecycle snapshot requires continued_as_run_id")
	}
	if strings.TrimSpace(s.RunID) == strings.TrimSpace(s.ContinuedAsRunID) {
		return errors.New("run lifecycle snapshot continuation must reference a different run")
	}
	if s.State != StateForked && strings.TrimSpace(s.ContinuedAsRunID) != "" {
		return fmt.Errorf("run lifecycle state %s forbids continued_as_run_id", s.State)
	}
	if s.State.Terminal() && s.EndedAt == nil {
		return fmt.Errorf("terminal run lifecycle state %s requires ended_at", s.State)
	}
	if !s.State.Terminal() && s.EndedAt != nil {
		return fmt.Errorf("active run lifecycle state %s forbids ended_at", s.State)
	}
	if s.EndedAt != nil && s.EndedAt.Before(s.StartedAt) {
		return fmt.Errorf(
			"run lifecycle snapshot run_id=%s ended_at %s precedes started_at %s",
			s.RunID,
			s.EndedAt.Format(time.RFC3339Nano),
			s.StartedAt.Format(time.RFC3339Nano),
		)
	}
	return nil
}

type Candidate struct {
	RunID      string
	BundleHash string
	Revision   int64
	DueAt      time.Time
}

type CandidateIdentity struct {
	RunID          string
	BundleHash     string
	Revision       int64
	DueAtUnixMicro int64
}

func (c Candidate) Identity() CandidateIdentity {
	return CandidateIdentity{
		RunID:          c.RunID,
		BundleHash:     c.BundleHash,
		Revision:       c.Revision,
		DueAtUnixMicro: c.DueAt.UnixMicro(),
	}
}

func (c Candidate) SameIdentity(other Candidate) bool {
	return c.Identity() == other.Identity()
}

func (c Candidate) Validate() error {
	if strings.TrimSpace(c.RunID) == "" {
		return errors.New("completion candidate requires run_id")
	}
	if strings.TrimSpace(c.BundleHash) == "" {
		return errors.New("completion candidate requires bundle_hash")
	}
	if c.Revision <= 0 {
		return errors.New("completion candidate requires positive revision")
	}
	if c.DueAt.IsZero() {
		return errors.New("completion candidate requires selected-store due_at")
	}
	if !c.DueAt.Equal(CanonicalTimestamp(c.DueAt)) {
		return errors.New("completion candidate due_at must use canonical microsecond precision")
	}
	return nil
}

type CandidateScope struct {
	BundleHash string
}

func (s CandidateScope) Validate() error {
	if strings.TrimSpace(s.BundleHash) == "" {
		return errors.New("completion candidate scope requires bundle_hash")
	}
	return nil
}

type CandidateCursor struct {
	RunID string
}

type CandidatePage struct {
	Candidates []Candidate
	Next       CandidateCursor
	Exhausted  bool
}

type CandidateRequestDisposition string

const (
	CandidateRequested        CandidateRequestDisposition = "requested"
	CandidateAlreadyCurrent   CandidateRequestDisposition = "already_current"
	CandidateDeferredPaused   CandidateRequestDisposition = "deferred_paused"
	CandidateAbsorbedTerminal CandidateRequestDisposition = "absorbed_terminal"
)

type CandidateTiming string

const (
	CandidateImmediate CandidateTiming = "immediate"
	CandidateAt        CandidateTiming = "at"
)

type CandidateRequest struct {
	RunID  string
	Timing CandidateTiming
	DueAt  time.Time
}

func ImmediateCandidate(runID string) CandidateRequest {
	return CandidateRequest{RunID: strings.TrimSpace(runID), Timing: CandidateImmediate}
}

func CandidateAtTime(runID string, dueAt time.Time) CandidateRequest {
	return CandidateRequest{
		RunID:  strings.TrimSpace(runID),
		Timing: CandidateAt,
		DueAt:  CanonicalTimestamp(dueAt),
	}
}

func (r CandidateRequest) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return errors.New("completion candidate request requires run_id")
	}
	switch r.Timing {
	case CandidateImmediate:
		if !r.DueAt.IsZero() {
			return errors.New("immediate completion candidate request forbids due_at")
		}
	case CandidateAt:
		if r.DueAt.IsZero() {
			return errors.New("scheduled completion candidate request requires due_at")
		}
		if !r.DueAt.Equal(CanonicalTimestamp(r.DueAt)) {
			return errors.New("scheduled completion candidate due_at must use canonical microsecond precision")
		}
	default:
		return fmt.Errorf("invalid completion candidate timing %q", r.Timing)
	}
	return nil
}

type CandidateRequestResult struct {
	Disposition CandidateRequestDisposition
	Candidate   Candidate
}

func (r CandidateRequestResult) Validate() error {
	switch r.Disposition {
	case CandidateRequested, CandidateAlreadyCurrent:
		return r.Candidate.Validate()
	case CandidateDeferredPaused, CandidateAbsorbedTerminal:
		if r.Candidate.Revision != 0 || !r.Candidate.DueAt.IsZero() {
			return fmt.Errorf("%s candidate result must not carry candidate identity", r.Disposition)
		}
		return nil
	default:
		return fmt.Errorf("invalid completion candidate request disposition %q", r.Disposition)
	}
}

func (r CandidateRequestResult) RequiresRepresentation() bool {
	return r.Disposition == CandidateRequested || r.Disposition == CandidateAlreadyCurrent
}

type CompletionOutcome string

const (
	OutcomeTerminallyEligible CompletionOutcome = "terminally_eligible"
	OutcomeRearmAt            CompletionOutcome = "rearm_at"
	OutcomeAwaitMutation      CompletionOutcome = "await_mutation"
	OutcomeRetryCurrent       CompletionOutcome = "retry_current"
	OutcomeExactNoop          CompletionOutcome = "exact_noop"
)

type CompletionResult struct {
	Outcome                    CompletionOutcome
	Candidate                  Candidate
	Retryable                  error
	GenericScheduleActivations []CommittedGenericScheduleActivation
}

// CommittedGenericScheduleActivation is post-commit projection evidence. The
// generic-schedule lifecycle remains the sole interpreter of the activation.
type CommittedGenericScheduleActivation struct {
	id string
}

func NewCommittedGenericScheduleActivation(id string) (CommittedGenericScheduleActivation, error) {
	activation := CommittedGenericScheduleActivation{id: strings.TrimSpace(id)}
	if err := activation.Validate(); err != nil {
		return CommittedGenericScheduleActivation{}, err
	}
	return activation, nil
}

func (a CommittedGenericScheduleActivation) ID() string { return strings.TrimSpace(a.id) }

func (a CommittedGenericScheduleActivation) Validate() error {
	if _, err := uuid.Parse(a.ID()); err != nil {
		return fmt.Errorf("committed generic schedule activation requires a UUID identity: %w", err)
	}
	return nil
}

type SelectedStoreBeforeRunStartError struct {
	RunID      string
	SelectedAt time.Time
	StartedAt  time.Time
}

func (e *SelectedStoreBeforeRunStartError) Error() string {
	if e == nil {
		return "selected-store time precedes run start"
	}
	return fmt.Sprintf(
		"selected-store time %s precedes run %s start %s",
		e.SelectedAt.UTC().Format(time.RFC3339Nano),
		strings.TrimSpace(e.RunID),
		e.StartedAt.UTC().Format(time.RFC3339Nano),
	)
}

func (r CompletionResult) Validate() error {
	seenActivations := make(map[string]struct{}, len(r.GenericScheduleActivations))
	for _, activation := range r.GenericScheduleActivations {
		if err := activation.Validate(); err != nil {
			return fmt.Errorf("completion result generic schedule activation: %w", err)
		}
		if _, exists := seenActivations[activation.ID()]; exists {
			return fmt.Errorf("completion result repeats generic schedule activation %s", activation.ID())
		}
		seenActivations[activation.ID()] = struct{}{}
	}
	switch r.Outcome {
	case OutcomeTerminallyEligible, OutcomeAwaitMutation, OutcomeExactNoop:
		if r.Retryable != nil {
			return fmt.Errorf("%s completion outcome forbids retry error", r.Outcome)
		}
		return nil
	case OutcomeRearmAt:
		if r.Retryable != nil {
			return errors.New("rearm_at completion outcome forbids retry error")
		}
		return r.Candidate.Validate()
	case OutcomeRetryCurrent:
		if r.Retryable == nil {
			return errors.New("retry_current completion outcome requires retry error")
		}
		if len(r.GenericScheduleActivations) != 0 {
			return errors.New("retry_current completion outcome forbids committed generic schedule activations")
		}
		return nil
	default:
		return fmt.Errorf("invalid completion outcome %q", r.Outcome)
	}
}

type TerminalCatalog struct {
	Workflow []string
	Flows    map[string][]string
}

func NewTerminalCatalog(workflow []string, flows map[string][]string) TerminalCatalog {
	out := TerminalCatalog{
		Workflow: normalizeStates(workflow),
		Flows:    make(map[string][]string, len(flows)),
	}
	for key, states := range flows {
		key = strings.Trim(strings.TrimSpace(key), "/")
		states = normalizeStates(states)
		if key != "" && len(states) > 0 {
			out.Flows[key] = states
		}
	}
	if len(out.Flows) == 0 {
		out.Flows = nil
	}
	return out
}

func (c TerminalCatalog) Empty() bool {
	return len(c.Workflow) == 0 && len(c.Flows) == 0
}

func (c TerminalCatalog) Terminal(flowTemplate, flowInstance, state string) (bool, bool) {
	state = strings.ToLower(strings.TrimSpace(state))
	if state == "" {
		return false, false
	}
	for _, raw := range []string{flowTemplate, flowInstance} {
		key := strings.Trim(strings.TrimSpace(raw), "/")
		if key == "" {
			continue
		}
		if states, ok := c.Flows[key]; ok {
			i := sort.SearchStrings(states, state)
			return i < len(states) && states[i] == state, true
		}
	}
	if strings.TrimSpace(flowInstance) != "" || len(c.Workflow) == 0 {
		return false, false
	}
	i := sort.SearchStrings(c.Workflow, state)
	return i < len(c.Workflow) && c.Workflow[i] == state, true
}

func normalizeStates(states []string) []string {
	seen := make(map[string]struct{}, len(states))
	for _, state := range states {
		state = strings.ToLower(strings.TrimSpace(state))
		if state != "" {
			seen[state] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for state := range seen {
		out = append(out, state)
	}
	sort.Strings(out)
	return out
}

type CandidateStore interface {
	ListCompletionCandidates(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error)
	ExecuteCompletionCandidate(context.Context, Candidate, TerminalCatalog) (CompletionResult, error)
}

type CandidateSink interface {
	ReserveCompletionCandidate(context.Context) (CandidateAdmission, error)
}

type CandidateAdmission interface {
	Submit(Candidate) error
	Cancel() error
}

type CandidateRegistration interface {
	Release()
}

type CandidateRegistrar interface {
	RegisterCompletionCandidateSink(context.Context, CandidateScope, CandidateSink) (CandidateRegistration, error)
}

type CandidateOwner interface {
	CandidateStore
	CandidateRegistrar
}

// OperationOwner owns standalone lifecycle reads and named lifecycle mutations.
// SQL, candidate revisions, and post-commit handoff remain private to the
// selected-store adapter.
type OperationOwner interface {
	RequirePresentRun(context.Context, string) error
	RequireActiveRun(context.Context, string) error
	RequirePresentRunSource(context.Context, string) (runtimecorrelation.BundleSourceFact, error)
	RequireActiveRunSource(context.Context, string) (runtimecorrelation.BundleSourceFact, error)
	CreateRun(context.Context, CreateRequest) (MutationDisposition, error)
	RequestCompletionCandidate(context.Context, CandidateRequest) (CandidateRequestDisposition, error)
	TransitionActiveRun(context.Context, ActiveTransitionRequest) (MutationDisposition, error)
	MarkTerminalRun(context.Context, TerminalRequest) (Snapshot, MutationDisposition, error)
	ForkRunSource(context.Context, ForkSourceRequest) (Snapshot, MutationDisposition, error)
	ReviseRunSource(context.Context, SourceRevisionRequest) (MutationDisposition, error)
	SyncRunCounters(context.Context, string) error
}
