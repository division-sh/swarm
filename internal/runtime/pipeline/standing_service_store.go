package pipeline

import (
	"context"
	"errors"
	"fmt"
	runtimestanding "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"strings"
	"time"

	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
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
	BindingEnabled     bool
	BindingBlockReason runtimestanding.StandingBindingBlockReason
	ServiceID          string
	FlowPath           string
	Source             runtimecorrelation.SourceArtifactFact
}

func (c StandingServiceCandidate) Normalized() StandingServiceCandidate {
	c.ServiceID = strings.TrimSpace(c.ServiceID)
	c.FlowPath = strings.TrimSpace(c.FlowPath)
	return c
}

func (c StandingServiceCandidate) Validate() error {
	c = c.Normalized()
	if err := c.BindingBlockReason.Validate(c.BindingEnabled); err != nil {
		return err
	}
	for field, value := range map[string]string{
		"service_id": c.ServiceID, "flow_path": c.FlowPath,
	} {
		if value == "" {
			return fmt.Errorf("standing service %s is required", field)
		}
	}
	for field, value := range map[string]string{"service_id": c.ServiceID} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("standing service %s must be a UUID: %w", field, err)
		}
	}
	wantServiceID := runtimeflowidentity.StandingServiceID(c.FlowPath)
	if c.ServiceID != wantServiceID {
		return fmt.Errorf("standing service_id %s does not match flow_path owner %s", c.ServiceID, wantServiceID)
	}
	if err := c.Source.Validate(); err != nil {
		return fmt.Errorf("standing service bundle source: %w", err)
	}
	return nil
}

type StandingServiceReconciliation struct {
	BindingEnabled               bool
	ServiceID                    string
	FlowPath                     string
	RunID                        string
	Generation                   int64
	PublicationSequence          int64
	Transition                   string
	EffectiveState               string
	BundleHash                   string
	Reason                       string
	RestartDisposition           runtimestanding.StandingRestartDisposition
	CommittedMutation            runtimestanding.MutationDisposition
	DeliveryContinuationRequired bool
	TimerCancellations           []runtimetimercancellation.Ref
}

type StandingServiceOperation struct {
	ServiceID        string
	Actor            string
	Reason           string
	ExecutionPosture executionposture.Posture
	Expected         *StandingServiceReconciliation
}

func (r StandingServiceReconciliation) SameAuthority(other StandingServiceReconciliation) bool {
	return r.ServiceID != "" && r.ServiceID == other.ServiceID && r.RunID == other.RunID && r.Generation == other.Generation &&
		r.FlowPath == other.FlowPath &&
		r.BundleHash == other.BundleHash && r.PublicationSequence == other.PublicationSequence && r.RestartDisposition == other.RestartDisposition
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
// mutations. DeliveryContinuationRequired is selected-store commit evidence,
// including when post-commit cleanup returns an error.
type StandingServicePersistence interface {
	ReconcileStandingService(context.Context, StandingServiceCandidate) (StandingServiceReconciliation, error)
	LoadReconciledStandingService(context.Context, StandingServiceCandidate) (StandingServiceReconciliation, bool, error)
	ReconcileStandingServiceSet(context.Context, []StandingServiceCandidate) ([]StandingServiceReconciliation, error)
	SuspendStandingService(context.Context, StandingServiceOperation) (StandingServiceReconciliation, error)
	ResumeStandingService(context.Context, StandingServiceOperation) (StandingServiceReconciliation, error)
	ResetStandingService(context.Context, StandingServiceOperation) (StandingServiceReconciliation, error)
	AdmitStandingServiceRun(context.Context, string, executionposture.Posture) error
	PublishStandingService(context.Context, string, string, int64) (int64, error)
	StandingRunRestartDisposition(context.Context, string) (runtimestanding.StandingRestartDisposition, error)
	ListStandingServiceStatuses(context.Context) ([]StandingServiceStatus, error)
}

func (s *workflowInstanceStore) ReconcileStandingService(ctx context.Context, candidate StandingServiceCandidate) (StandingServiceReconciliation, error) {
	if s == nil || s.standingServices == nil {
		return StandingServiceReconciliation{}, errors.New("standing service persistence owner is required")
	}
	result, err := s.standingServices.ReconcileStandingService(ctx, candidate)
	s.consumeStandingServiceCommit(result)
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
	s.consumeStandingServiceCommits(results)
	return results, err
}

func (s *workflowInstanceStore) SuspendStandingService(ctx context.Context, operation StandingServiceOperation) (StandingServiceReconciliation, error) {
	if s == nil || s.standingServices == nil {
		return StandingServiceReconciliation{}, errors.New("standing service persistence owner is required")
	}
	result, err := s.standingServices.SuspendStandingService(ctx, operation)
	s.consumeStandingServiceCommit(result)
	return result, err
}

func (s *workflowInstanceStore) ResumeStandingService(ctx context.Context, operation StandingServiceOperation) (StandingServiceReconciliation, error) {
	if s == nil || s.standingServices == nil {
		return StandingServiceReconciliation{}, errors.New("standing service persistence owner is required")
	}
	result, err := s.standingServices.ResumeStandingService(ctx, operation)
	s.consumeStandingServiceCommit(result)
	return result, err
}

func (s *workflowInstanceStore) ResetStandingService(ctx context.Context, operation StandingServiceOperation) (StandingServiceReconciliation, error) {
	if s == nil || s.standingServices == nil {
		return StandingServiceReconciliation{}, errors.New("standing service persistence owner is required")
	}
	result, err := s.standingServices.ResetStandingService(ctx, operation)
	s.consumeStandingServiceCommit(result)
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

func (s *workflowInstanceStore) StandingRunRestartDisposition(ctx context.Context, runID string) (runtimestanding.StandingRestartDisposition, error) {
	if s == nil || s.standingServices == nil {
		return runtimestanding.StandingRestartDisposition{}, errors.New("standing service persistence owner is required")
	}
	return s.standingServices.StandingRunRestartDisposition(ctx, runID)
}

func (s *workflowInstanceStore) ListStandingServiceStatuses(ctx context.Context) ([]StandingServiceStatus, error) {
	if s == nil || s.standingServices == nil {
		return nil, errors.New("standing service persistence owner is required")
	}
	return s.standingServices.ListStandingServiceStatuses(ctx)
}

func (s *workflowInstanceStore) consumeStandingServiceCommit(result StandingServiceReconciliation) {
	if result.DeliveryContinuationRequired {
		s.signalDeliveryContinuations()
	}
}

func (s *workflowInstanceStore) consumeStandingServiceCommits(results []StandingServiceReconciliation) {
	for _, result := range results {
		if result.DeliveryContinuationRequired {
			s.signalDeliveryContinuations()
			return
		}
	}
}
