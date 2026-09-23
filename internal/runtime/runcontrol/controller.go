package runcontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
)

const (
	StatusRunning   = "running"
	StatusPaused    = "paused"
	StatusCancelled = "cancelled"
	StatusStopped   = "stopped"
)

var (
	ErrRunNotFound     = errors.New("run not found")
	ErrAlreadyTerminal = errors.New("run already terminal")
	ErrAlreadyPaused   = errors.New("run already paused")
	ErrNotPaused       = errors.New("run not paused")
	ErrDispatchBlocked = errors.New("run dispatch is blocked")
)

type StateError struct {
	Err           error
	RunID         string
	CurrentStatus string
}

func (e *StateError) Error() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.CurrentStatus) == "" {
		return fmt.Sprintf("%s: %s", e.Err, strings.TrimSpace(e.RunID))
	}
	return fmt.Sprintf("%s: %s status=%s", e.Err, strings.TrimSpace(e.RunID), strings.TrimSpace(e.CurrentStatus))
}

func (e *StateError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type State struct {
	RunID               string
	Status              string
	BundleHash          string
	ControlStatus       string
	Reason              string
	ControlledBy        string
	UpdatedAt           time.Time
	AbandonedDeliveries int
	TimerCancellations  []runtimetimercancellation.Ref
}

type TransitionRequest struct {
	RunID        string
	Reason       string
	ControlledBy string
	Now          time.Time
}

type TransitionResult struct {
	RunID               string
	Status              string
	AbandonedDeliveries int
	Recovery            PostCommitRecovery
}

type RecoveryDisposition string

const (
	RecoveryComplete      RecoveryDisposition = "complete"
	RecoveryNotConfigured RecoveryDisposition = "not_configured"
	RecoveryExhausted     RecoveryDisposition = "exhausted"
	RecoveryBlocked       RecoveryDisposition = "blocked"
	RecoveryFailed        RecoveryDisposition = "failed"
	RecoveryPending       RecoveryDisposition = "pending"
	RecoveryCancelled     RecoveryDisposition = "cancelled"
)

// PostCommitRecovery reports recovery work after a committed run transition.
// Its error is diagnostic: the transition is already committed and must not be
// replayed.
type PostCommitRecovery struct {
	Disposition RecoveryDisposition
	Sweep       runtimepipelineobligation.SweepResult
	Err         error
}

type Store interface {
	StopRunControlOutcome(context.Context, TransitionRequest) (StoreTransition, error)
	PauseRunControlOutcome(context.Context, TransitionRequest) (StoreTransition, error)
	ContinueRunControlOutcome(context.Context, TransitionRequest) (StoreTransition, error)
	RunDispatchBlocked(context.Context, string) (bool, error)
}

type StoreTransition struct {
	State        State
	Acknowledged bool
}

type QueueReleaser interface {
	PreflightRunQueue(context.Context, string) error
	ReleaseRunQueue(context.Context, string, int) (runtimepipelineobligation.SweepResult, error)
	BeginRunStop(context.Context, string) (StopTransition, error)
}

// StopTransition retains the existing pipeline parent exclusion until the
// selected stop transaction settles. It does not revoke foreground claims.
type StopTransition = runtimepipelineobligation.ParentTransition

type TimerCancellationReconciler interface {
	Reconcile(context.Context, []runtimetimercancellation.Ref) error
}

type Options struct {
	Now                func() time.Time
	ReleaseLimit       int
	TimerCancellations TimerCancellationReconciler
}

type Controller struct {
	store              Store
	queue              QueueReleaser
	now                func() time.Time
	releaseLimit       int
	timerCancellations TimerCancellationReconciler
}

func NewController(store Store, queue QueueReleaser, opts Options) *Controller {
	if store == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	limit := opts.ReleaseLimit
	if limit <= 0 {
		limit = 200
	}
	return &Controller{
		store:              store,
		queue:              queue,
		now:                now,
		releaseLimit:       limit,
		timerCancellations: opts.TimerCancellations,
	}
}

func (c *Controller) Stop(ctx context.Context, req TransitionRequest) (TransitionResult, error) {
	if c == nil || c.store == nil {
		return TransitionResult{}, fmt.Errorf("run control owner is not configured")
	}
	req = c.normalize(req)
	if c.queue != nil {
		transition, err := c.queue.BeginRunStop(ctx, req.RunID)
		if err != nil {
			return TransitionResult{}, StopFailure("pipeline_drain", err)
		}
		if transition == nil {
			return TransitionResult{}, StopFailure("pipeline_drain", errors.New("run stop transition is required"))
		}
		defer transition.Done()
	}
	committed, err := c.store.StopRunControlOutcome(ctx, req)
	if !committed.Acknowledged {
		if err == nil {
			err = errors.New("run stop was not acknowledged")
		}
		return TransitionResult{}, err
	}
	state := committed.State
	result := TransitionResult{
		RunID:               state.RunID,
		Status:              StatusCancelled,
		AbandonedDeliveries: state.AbandonedDeliveries,
		Recovery:            PostCommitRecovery{Disposition: RecoveryComplete},
	}
	if err != nil {
		result.Recovery = PostCommitRecovery{Disposition: RecoveryFailed, Err: err}
	}
	if len(state.TimerCancellations) == 0 {
		return result, err
	}
	if c.timerCancellations == nil {
		result.Recovery = PostCommitRecovery{
			Disposition: RecoveryFailed,
			Err:         errors.Join(err, errors.New("timer cancellation reconciler is required after committed run stop")),
		}
		return result, err
	}
	if reconcileErr := c.timerCancellations.Reconcile(context.WithoutCancel(ctx), state.TimerCancellations); reconcileErr != nil {
		disposition := RecoveryFailed
		var reconciliation *runtimetimercancellation.ReconciliationError
		if errors.As(reconcileErr, &reconciliation) && reconciliation.RecoveryPendingOnly() {
			disposition = RecoveryPending
		}
		result.Recovery = PostCommitRecovery{Disposition: disposition, Err: errors.Join(err, reconcileErr)}
	}
	return result, err
}

func (c *Controller) Pause(ctx context.Context, req TransitionRequest) (TransitionResult, error) {
	if c == nil || c.store == nil {
		return TransitionResult{}, fmt.Errorf("run control owner is not configured")
	}
	req = c.normalize(req)
	committed, err := c.store.PauseRunControlOutcome(ctx, req)
	if !committed.Acknowledged {
		if err == nil {
			err = errors.New("run pause was not acknowledged")
		}
		return TransitionResult{}, err
	}
	return TransitionResult{RunID: committed.State.RunID, Status: StatusPaused}, err
}

func (c *Controller) Continue(ctx context.Context, req TransitionRequest) (TransitionResult, error) {
	if c == nil || c.store == nil {
		return TransitionResult{}, fmt.Errorf("run control owner is not configured")
	}
	req = c.normalize(req)
	if c.queue != nil {
		if err := c.queue.PreflightRunQueue(ctx, req.RunID); err != nil {
			return TransitionResult{}, err
		}
	}
	committed, err := c.store.ContinueRunControlOutcome(ctx, req)
	if !committed.Acknowledged {
		if err == nil {
			err = errors.New("run continue was not acknowledged")
		}
		return TransitionResult{}, err
	}
	state := committed.State
	result := TransitionResult{
		RunID:  state.RunID,
		Status: StatusRunning,
		Recovery: PostCommitRecovery{
			Disposition: RecoveryNotConfigured,
		},
	}
	if c.queue != nil {
		result.Recovery = c.releaseQueuedAfterContinue(ctx, state.RunID)
	}
	if err != nil {
		result.Recovery.Err = errors.Join(err, result.Recovery.Err)
		if result.Recovery.Disposition != RecoveryCancelled {
			result.Recovery.Disposition = RecoveryFailed
		}
	}
	return result, err
}

func (c *Controller) releaseQueuedAfterContinue(ctx context.Context, runID string) PostCommitRecovery {
	if c == nil || c.queue == nil {
		return PostCommitRecovery{Disposition: RecoveryNotConfigured}
	}
	recovery := PostCommitRecovery{}
	firstBatch := true
	for {
		releaseCtx := ctx
		if firstBatch {
			// An acknowledged transition must initiate recovery even if its caller canceled.
			releaseCtx = context.WithoutCancel(ctx)
			firstBatch = false
		}
		result, err := c.queue.ReleaseRunQueue(releaseCtx, runID, c.releaseLimit)
		recovery.Sweep.Settled += result.Settled
		recovery.Sweep.Examined += result.Examined
		recovery.Sweep.Exhausted = recovery.Sweep.Exhausted || result.Exhausted
		recovery.Sweep.Blocked = recovery.Sweep.Blocked || result.Blocked
		if err != nil {
			recovery.Err = err
			recovery.Disposition = RecoveryFailed
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				recovery.Disposition = RecoveryCancelled
			}
			return recovery
		}
		if result.Blocked {
			recovery.Disposition = RecoveryBlocked
			return recovery
		}
		if result.Exhausted {
			recovery.Disposition = RecoveryExhausted
			return recovery
		}
	}
}

func (c *Controller) QueueableRunDispatchBlocked(ctx context.Context, runID string) (bool, error) {
	if c == nil || c.store == nil {
		return false, nil
	}
	return c.store.RunDispatchBlocked(ctx, runID)
}

func (c *Controller) normalize(req TransitionRequest) TransitionRequest {
	req.RunID = strings.TrimSpace(req.RunID)
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Reason == "" {
		req.Reason = "operator_request"
	}
	req.ControlledBy = strings.TrimSpace(req.ControlledBy)
	if req.ControlledBy == "" {
		req.ControlledBy = "api.v1"
	}
	if req.Now.IsZero() {
		req.Now = c.now().UTC()
	} else {
		req.Now = req.Now.UTC()
	}
	return req
}
