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
	RecoveryNotConfigured RecoveryDisposition = "not_configured"
	RecoveryExhausted     RecoveryDisposition = "exhausted"
	RecoveryBlocked       RecoveryDisposition = "blocked"
	RecoveryFailed        RecoveryDisposition = "failed"
	RecoveryCancelled     RecoveryDisposition = "cancelled"
)

// PostCommitRecovery reports the immediate durable-recovery attempt after a
// successful continue. Its error is diagnostic: the run transition is already
// committed and must not be replayed.
type PostCommitRecovery struct {
	Disposition RecoveryDisposition
	Sweep       runtimepipelineobligation.SweepResult
	Err         error
}

type Store interface {
	StopRunControl(context.Context, TransitionRequest) (State, error)
	PauseRunControl(context.Context, TransitionRequest) (State, error)
	ContinueRunControl(context.Context, TransitionRequest) (State, error)
	RunDispatchBlocked(context.Context, string) (bool, error)
}

type QueueReleaser interface {
	ReleaseRunQueue(context.Context, string, int) (runtimepipelineobligation.SweepResult, error)
}

type Options struct {
	Now          func() time.Time
	ReleaseLimit int
}

type Controller struct {
	store        Store
	queue        QueueReleaser
	now          func() time.Time
	releaseLimit int
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
		store:        store,
		queue:        queue,
		now:          now,
		releaseLimit: limit,
	}
}

func (c *Controller) Stop(ctx context.Context, req TransitionRequest) (TransitionResult, error) {
	if c == nil || c.store == nil {
		return TransitionResult{}, fmt.Errorf("run control owner is not configured")
	}
	req = c.normalize(req)
	state, err := c.store.StopRunControl(ctx, req)
	if err != nil {
		return TransitionResult{}, err
	}
	return TransitionResult{
		RunID:               state.RunID,
		Status:              StatusCancelled,
		AbandonedDeliveries: state.AbandonedDeliveries,
	}, nil
}

func (c *Controller) Pause(ctx context.Context, req TransitionRequest) (TransitionResult, error) {
	if c == nil || c.store == nil {
		return TransitionResult{}, fmt.Errorf("run control owner is not configured")
	}
	req = c.normalize(req)
	state, err := c.store.PauseRunControl(ctx, req)
	if err != nil {
		return TransitionResult{}, err
	}
	return TransitionResult{RunID: state.RunID, Status: StatusPaused}, nil
}

func (c *Controller) Continue(ctx context.Context, req TransitionRequest) (TransitionResult, error) {
	if c == nil || c.store == nil {
		return TransitionResult{}, fmt.Errorf("run control owner is not configured")
	}
	req = c.normalize(req)
	state, err := c.store.ContinueRunControl(ctx, req)
	if err != nil {
		return TransitionResult{}, err
	}
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
	return result, nil
}

func (c *Controller) releaseQueuedAfterContinue(ctx context.Context, runID string) PostCommitRecovery {
	if c == nil || c.queue == nil {
		return PostCommitRecovery{Disposition: RecoveryNotConfigured}
	}
	recovery := PostCommitRecovery{}
	for {
		result, err := c.queue.ReleaseRunQueue(ctx, runID, c.releaseLimit)
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
