package runcontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
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
	ControlStatus       string
	Reason              string
	ControlledBy        string
	UpdatedAt           time.Time
	AbandonedDeliveries int
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
	ReleasedDeliveries  int
}

type Store interface {
	StopRunControl(context.Context, TransitionRequest) (State, error)
	PauseRunControl(context.Context, TransitionRequest) (State, error)
	ContinueRunControl(context.Context, TransitionRequest) (State, error)
	RunDispatchBlocked(context.Context, string) (bool, error)
}

type QueueReleaser interface {
	ReleaseRunQueue(context.Context, string, time.Duration, int) (int, error)
}

type Options struct {
	Now             func() time.Time
	ReleaseLookback time.Duration
	ReleaseLimit    int
}

type Controller struct {
	store           Store
	queue           QueueReleaser
	now             func() time.Time
	releaseLookback time.Duration
	releaseLimit    int
}

func NewController(store Store, queue QueueReleaser, opts Options) *Controller {
	if store == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	lookback := opts.ReleaseLookback
	if lookback <= 0 {
		lookback = 24 * time.Hour
	}
	limit := opts.ReleaseLimit
	if limit <= 0 {
		limit = 200
	}
	return &Controller{
		store:           store,
		queue:           queue,
		now:             now,
		releaseLookback: lookback,
		releaseLimit:    limit,
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
	result := TransitionResult{RunID: state.RunID, Status: StatusRunning}
	if c.queue != nil {
		result.ReleasedDeliveries = c.releaseQueuedAfterContinue(ctx, state.RunID)
	}
	return result, nil
}

func (c *Controller) releaseQueuedAfterContinue(ctx context.Context, runID string) int {
	if c == nil || c.queue == nil {
		return 0
	}
	const attempts = 3
	const retryDelay = 25 * time.Millisecond
	releasedTotal := 0
	for attempt := 0; attempt < attempts; attempt++ {
		released, err := c.queue.ReleaseRunQueue(ctx, runID, c.releaseLookback, c.releaseLimit)
		if err == nil {
			releasedTotal += released
			if released > 0 {
				return releasedTotal
			}
		}
		if attempt == attempts-1 {
			return releasedTotal
		}
		select {
		case <-ctx.Done():
			return releasedTotal
		case <-time.After(retryDelay):
		}
	}
	return releasedTotal
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
