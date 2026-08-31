package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

var ErrStandingServiceNotFound = errors.New("standing service not found")

type StandingServiceError struct {
	ServiceID string
	Err       error
	Detail    string
}

func (e *StandingServiceError) Error() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.Detail) == "" {
		return fmt.Sprintf("%s: %s", e.Err, e.ServiceID)
	}
	return fmt.Sprintf("%s: %s: %s", e.Err, e.ServiceID, e.Detail)
}

func (e *StandingServiceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type StandingServiceCandidate struct {
	ServiceID  string
	PackageKey string
	FlowID     string
	InstanceID string
	EntityID   string
	Source     runtimecorrelation.BundleSourceFact
}

func (c StandingServiceCandidate) Normalized() StandingServiceCandidate {
	c.ServiceID = strings.TrimSpace(c.ServiceID)
	c.PackageKey = strings.TrimSpace(c.PackageKey)
	c.FlowID = strings.TrimSpace(c.FlowID)
	c.InstanceID = strings.TrimSpace(c.InstanceID)
	c.EntityID = strings.TrimSpace(c.EntityID)
	return c
}

func (c StandingServiceCandidate) Validate() error {
	c = c.Normalized()
	for field, value := range map[string]string{
		"service_id": c.ServiceID, "package_key": c.PackageKey, "flow_id": c.FlowID,
		"instance_id": c.InstanceID, "entity_id": c.EntityID,
	} {
		if value == "" {
			return fmt.Errorf("standing service %s is required", field)
		}
	}
	for field, value := range map[string]string{"service_id": c.ServiceID, "entity_id": c.EntityID} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("standing service %s must be a UUID: %w", field, err)
		}
	}
	wantServiceID := runtimeflowidentity.StandingServiceID(c.PackageKey, c.FlowID)
	if c.ServiceID != wantServiceID {
		return fmt.Errorf("standing service_id %s does not match package_key/flow_id owner %s", c.ServiceID, wantServiceID)
	}
	if err := c.Source.Validate(); err != nil {
		return fmt.Errorf("standing service bundle source: %w", err)
	}
	return nil
}

type StandingServiceReconciliation struct {
	ServiceID                    string
	PackageKey                   string
	FlowID                       string
	InstanceID                   string
	EntityID                     string
	RunID                        string
	Generation                   int64
	PublicationSequence          int64
	Transition                   string
	EffectiveState               string
	BundleHash                   string
	BundleSource                 string
	Reason                       string
	RestartDisposition           StandingRestartDisposition
	DeliveryContinuationRequired bool
	TimerCancellations           []runtimetimercancellation.Ref
}

type StandingRestartDispositionKind string

const (
	StandingRestartOrdinary         StandingRestartDispositionKind = "ordinary"
	StandingRestartActiveIntrinsic  StandingRestartDispositionKind = "active_intrinsic"
	StandingRestartSuspended        StandingRestartDispositionKind = "suspended"
	StandingRestartOrphaned         StandingRestartDispositionKind = "orphaned"
	StandingRestartTerminalDeclared StandingRestartDispositionKind = "terminal_declared"
	StandingRestartTerminalOrphaned StandingRestartDispositionKind = "terminal_orphaned"
	StandingRestartInvalidCurrent   StandingRestartDispositionKind = "invalid_current"
)

type StandingRestartRemediation string

const (
	StandingRestartNoRemediation      StandingRestartRemediation = "none"
	StandingRestartResumeOrReset      StandingRestartRemediation = "resume_or_reset"
	StandingRestartRestoreDeclaration StandingRestartRemediation = "restore_declaration"
	StandingRestartReset              StandingRestartRemediation = "reset"
	StandingRestartRestoreThenReset   StandingRestartRemediation = "restore_then_reset"
)

// StandingRestartFact is the complete durable state product required to
// classify one exact current standing generation. ExactCurrent=false is the
// only ordinary result and intentionally carries no partial standing facts.
type StandingRestartFact struct {
	ExactCurrent       bool
	ServiceID          string
	RunID              string
	Generation         int64
	DeclarationPresent bool
	EffectiveState     string
	OperatorOverride   string
	RunState           string
}

type StandingRestartDisposition struct {
	Kind               StandingRestartDispositionKind
	ServiceID          string
	RunID              string
	Generation         int64
	DeclarationPresent bool
	EffectiveState     string
	OperatorOverride   string
	RunState           string
	Remediation        StandingRestartRemediation
}

type StandingRestartDispositionReader interface {
	StandingRunRestartDisposition(context.Context, string) (StandingRestartDisposition, error)
}

func (d StandingRestartDisposition) ExactCurrent() bool {
	return d.Kind != "" && d.Kind != StandingRestartOrdinary
}

func (d StandingRestartDisposition) Executable() bool {
	return d.Kind == StandingRestartActiveIntrinsic
}

func (d StandingRestartDisposition) UsesGenericRecovery() bool {
	return d.Kind == StandingRestartOrdinary
}

func (d StandingRestartDisposition) RunControlGuidance() string {
	if d.Kind == StandingRestartActiveIntrinsic {
		return fmt.Sprintf("use `swarm standing suspend %s` or `swarm standing reset %s`", d.ServiceID, d.ServiceID)
	}
	switch d.Remediation {
	case StandingRestartNoRemediation:
		return "use standing service controls"
	case StandingRestartResumeOrReset:
		return fmt.Sprintf("use `swarm standing resume %s` or `swarm standing reset %s`", d.ServiceID, d.ServiceID)
	case StandingRestartRestoreDeclaration:
		return "restore the standing declaration"
	case StandingRestartReset:
		return fmt.Sprintf("use `swarm standing reset %s`", d.ServiceID)
	case StandingRestartRestoreThenReset:
		return fmt.Sprintf("restore the standing declaration, then use `swarm standing reset %s`", d.ServiceID)
	default:
		return "repair the standing service disposition"
	}
}

func (d StandingRestartDisposition) Validate() error {
	switch d.Kind {
	case StandingRestartOrdinary:
		if d.ServiceID != "" || d.RunID != "" || d.Generation != 0 {
			return errors.New("ordinary standing restart disposition cannot carry a current owner")
		}
		return nil
	case StandingRestartActiveIntrinsic, StandingRestartSuspended, StandingRestartOrphaned,
		StandingRestartTerminalDeclared, StandingRestartTerminalOrphaned, StandingRestartInvalidCurrent:
	default:
		return fmt.Errorf("invalid standing restart disposition %q", d.Kind)
	}
	if _, err := uuid.Parse(strings.TrimSpace(d.ServiceID)); err != nil {
		return fmt.Errorf("standing restart service_id: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(d.RunID)); err != nil {
		return fmt.Errorf("standing restart run_id: %w", err)
	}
	if d.Generation <= 0 {
		return errors.New("standing restart generation must be positive")
	}
	if _, err := runtimerunlifecycle.ParseState(d.RunState); err != nil {
		return err
	}
	if d.Remediation == "" {
		return errors.New("standing restart remediation is required")
	}
	return nil
}

// ClassifyStandingRestart is the sole decision table for exact-current
// standing restart semantics. Selected-store readers prove the fact product;
// consumers receive only this typed result.
func ClassifyStandingRestart(fact StandingRestartFact) (StandingRestartDisposition, error) {
	if !fact.ExactCurrent {
		result := StandingRestartDisposition{Kind: StandingRestartOrdinary, Remediation: StandingRestartNoRemediation}
		return result, result.Validate()
	}
	fact.ServiceID = strings.TrimSpace(fact.ServiceID)
	fact.RunID = strings.TrimSpace(fact.RunID)
	fact.EffectiveState = strings.TrimSpace(fact.EffectiveState)
	fact.OperatorOverride = strings.TrimSpace(fact.OperatorOverride)
	fact.RunState = strings.TrimSpace(fact.RunState)
	if _, err := uuid.Parse(fact.ServiceID); err != nil {
		return StandingRestartDisposition{}, fmt.Errorf("standing restart service_id: %w", err)
	}
	if _, err := uuid.Parse(fact.RunID); err != nil {
		return StandingRestartDisposition{}, fmt.Errorf("standing restart run_id: %w", err)
	}
	if fact.Generation <= 0 {
		return StandingRestartDisposition{}, errors.New("standing restart generation must be positive")
	}
	state, err := runtimerunlifecycle.ParseState(fact.RunState)
	if err != nil {
		return StandingRestartDisposition{}, err
	}
	if fact.EffectiveState != "active" && fact.EffectiveState != "suspended" && fact.EffectiveState != "orphaned" {
		return StandingRestartDisposition{}, fmt.Errorf("invalid standing restart effective_state %q", fact.EffectiveState)
	}
	if fact.OperatorOverride != "none" && fact.OperatorOverride != "suspended" {
		return StandingRestartDisposition{}, fmt.Errorf("invalid standing restart operator_override %q", fact.OperatorOverride)
	}
	desiredValid := (!fact.DeclarationPresent && fact.EffectiveState == "orphaned" &&
		(fact.OperatorOverride == "none" || fact.OperatorOverride == "suspended")) ||
		(fact.DeclarationPresent && fact.EffectiveState == "active" && fact.OperatorOverride == "none") ||
		(fact.DeclarationPresent && fact.EffectiveState == "suspended" && fact.OperatorOverride == "suspended")
	result := StandingRestartDisposition{
		ServiceID: fact.ServiceID, RunID: fact.RunID, Generation: fact.Generation,
		DeclarationPresent: fact.DeclarationPresent, EffectiveState: fact.EffectiveState,
		OperatorOverride: fact.OperatorOverride, RunState: string(state),
	}
	if !desiredValid {
		return StandingRestartDisposition{}, fmt.Errorf(
			"standing restart desired-state product is inconsistent: declaration_present=%t effective_state=%s operator_override=%s",
			fact.DeclarationPresent,
			fact.EffectiveState,
			fact.OperatorOverride,
		)
	}
	switch {
	case state.Terminal() && fact.DeclarationPresent:
		result.Kind, result.Remediation = StandingRestartTerminalDeclared, StandingRestartReset
	case state.Terminal():
		result.Kind, result.Remediation = StandingRestartTerminalOrphaned, StandingRestartRestoreThenReset
	case !fact.DeclarationPresent && fact.EffectiveState == "orphaned" &&
		(fact.OperatorOverride == "none" || fact.OperatorOverride == "suspended") &&
		state == runtimerunlifecycle.StatePaused:
		result.Kind, result.Remediation = StandingRestartOrphaned, StandingRestartRestoreDeclaration
	case fact.DeclarationPresent && fact.EffectiveState == "suspended" &&
		fact.OperatorOverride == "suspended" && state == runtimerunlifecycle.StatePaused:
		result.Kind, result.Remediation = StandingRestartSuspended, StandingRestartResumeOrReset
	case fact.DeclarationPresent && fact.EffectiveState == "active" &&
		fact.OperatorOverride == "none" && state.Active():
		result.Kind, result.Remediation = StandingRestartActiveIntrinsic, StandingRestartNoRemediation
	case fact.DeclarationPresent:
		result.Kind, result.Remediation = StandingRestartInvalidCurrent, StandingRestartReset
	default:
		result.Kind, result.Remediation = StandingRestartInvalidCurrent, StandingRestartRestoreThenReset
	}
	return result, result.Validate()
}

type StandingServiceOperation struct {
	ServiceID        string
	Actor            string
	Reason           string
	ExecutionPosture executionposture.Posture
}

type StandingServiceStatus struct {
	StandingServiceReconciliation
	DeclarationPresent bool
	OperatorOverride   string
	PublicationState   string
	OverrideActor      string
	OverrideReason     string
	OverrideAt         time.Time
}

func (o StandingServiceOperation) Normalized() StandingServiceOperation {
	o.ServiceID = strings.TrimSpace(o.ServiceID)
	o.Actor = strings.TrimSpace(o.Actor)
	o.Reason = strings.TrimSpace(o.Reason)
	if o.Actor == "" {
		o.Actor = "operator"
	}
	return o
}

// StandingServicePersistence owns standing-service reads and complete atomic
// mutations. DeliveryContinuationRequired is commit evidence consumed only
// after a successful selected-store commit.
type StandingServicePersistence interface {
	ReconcileStandingService(context.Context, StandingServiceCandidate) (StandingServiceReconciliation, error)
	LoadReconciledStandingService(context.Context, StandingServiceCandidate) (StandingServiceReconciliation, bool, error)
	ReconcileStandingServiceSet(context.Context, []StandingServiceCandidate) ([]StandingServiceReconciliation, error)
	SuspendStandingService(context.Context, StandingServiceOperation) (StandingServiceReconciliation, error)
	ResumeStandingService(context.Context, StandingServiceOperation) (StandingServiceReconciliation, error)
	ResetStandingService(context.Context, StandingServiceOperation) (StandingServiceReconciliation, error)
	AdmitStandingServiceRun(context.Context, string, executionposture.Posture) error
	PublishStandingService(context.Context, string, string, int64) (int64, error)
	StandingRunRestartDisposition(context.Context, string) (StandingRestartDisposition, error)
	ListStandingServiceStatuses(context.Context) ([]StandingServiceStatus, error)
}

func (s *workflowInstanceStore) ReconcileStandingService(ctx context.Context, candidate StandingServiceCandidate) (StandingServiceReconciliation, error) {
	if s == nil || s.standingServices == nil {
		return StandingServiceReconciliation{}, errors.New("standing service persistence owner is required")
	}
	result, err := s.standingServices.ReconcileStandingService(ctx, candidate)
	s.consumeStandingServiceCommit(result, err)
	return result, err
}

func (s *workflowInstanceStore) LoadReconciledStandingService(ctx context.Context, candidate StandingServiceCandidate) (StandingServiceReconciliation, bool, error) {
	if s == nil || s.standingServices == nil {
		return StandingServiceReconciliation{}, false, errors.New("standing service persistence owner is required")
	}
	return s.standingServices.LoadReconciledStandingService(ctx, candidate)
}

func (s *workflowInstanceStore) ReconcileStandingServiceSet(ctx context.Context, candidates []StandingServiceCandidate) ([]StandingServiceReconciliation, error) {
	if s == nil || s.standingServices == nil {
		return nil, errors.New("standing service persistence owner is required")
	}
	results, err := s.standingServices.ReconcileStandingServiceSet(ctx, candidates)
	s.consumeStandingServiceCommits(results, err)
	return results, err
}

func (s *workflowInstanceStore) SuspendStandingService(ctx context.Context, operation StandingServiceOperation) (StandingServiceReconciliation, error) {
	if s == nil || s.standingServices == nil {
		return StandingServiceReconciliation{}, errors.New("standing service persistence owner is required")
	}
	result, err := s.standingServices.SuspendStandingService(ctx, operation)
	s.consumeStandingServiceCommit(result, err)
	return result, err
}

func (s *workflowInstanceStore) ResumeStandingService(ctx context.Context, operation StandingServiceOperation) (StandingServiceReconciliation, error) {
	if s == nil || s.standingServices == nil {
		return StandingServiceReconciliation{}, errors.New("standing service persistence owner is required")
	}
	result, err := s.standingServices.ResumeStandingService(ctx, operation)
	s.consumeStandingServiceCommit(result, err)
	return result, err
}

func (s *workflowInstanceStore) ResetStandingService(ctx context.Context, operation StandingServiceOperation) (StandingServiceReconciliation, error) {
	if s == nil || s.standingServices == nil {
		return StandingServiceReconciliation{}, errors.New("standing service persistence owner is required")
	}
	result, err := s.standingServices.ResetStandingService(ctx, operation)
	s.consumeStandingServiceCommit(result, err)
	return result, err
}

func (s *workflowInstanceStore) AdmitStandingServiceRun(ctx context.Context, runID string, posture executionposture.Posture) error {
	if s == nil || s.standingServices == nil {
		return errors.New("standing service persistence owner is required")
	}
	return s.standingServices.AdmitStandingServiceRun(ctx, runID, posture)
}

func (s *workflowInstanceStore) PublishStandingService(ctx context.Context, serviceID, runID string, generation int64) (int64, error) {
	if s == nil || s.standingServices == nil {
		return 0, errors.New("standing service persistence owner is required")
	}
	return s.standingServices.PublishStandingService(ctx, serviceID, runID, generation)
}

func (s *workflowInstanceStore) StandingRunRestartDisposition(ctx context.Context, runID string) (StandingRestartDisposition, error) {
	if s == nil || s.standingServices == nil {
		return StandingRestartDisposition{}, errors.New("standing service persistence owner is required")
	}
	return s.standingServices.StandingRunRestartDisposition(ctx, runID)
}

func (s *workflowInstanceStore) ListStandingServiceStatuses(ctx context.Context) ([]StandingServiceStatus, error) {
	if s == nil || s.standingServices == nil {
		return nil, errors.New("standing service persistence owner is required")
	}
	return s.standingServices.ListStandingServiceStatuses(ctx)
}

func (s *workflowInstanceStore) consumeStandingServiceCommit(result StandingServiceReconciliation, err error) {
	if err == nil && result.DeliveryContinuationRequired {
		s.signalDeliveryContinuations()
	}
}

func (s *workflowInstanceStore) consumeStandingServiceCommits(results []StandingServiceReconciliation, err error) {
	if err != nil {
		return
	}
	for _, result := range results {
		if result.DeliveryContinuationRequired {
			s.signalDeliveryContinuations()
			return
		}
	}
}
