package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
)

const (
	StatusRunning Status = "running"
	StatusPaused  Status = "paused"
)

var (
	ErrAlreadyPaused       = errors.New("runtime ingress already paused")
	ErrNotPaused           = errors.New("runtime ingress is not paused")
	ErrStateNotInitialized = errors.New("runtime ingress state is not initialized")
)

type Status string

type State struct {
	Status            Status
	Reason            string
	ControlledBy      string
	TransitionEventID string
	UpdatedAt         time.Time
}

type TransitionRequest struct {
	Reason       string
	ControlledBy string
	Now          time.Time
}

type TransitionResult struct {
	Status        Status
	TransitionID  string
	ReleasedCount int
}

type Store interface {
	EnsureRuntimeIngressState(context.Context, time.Time) (State, error)
	LoadRuntimeIngressState(context.Context) (State, error)
	TransitionRuntimeIngressState(context.Context, Status, string, string, time.Time) (State, bool, error)
	SetRuntimeIngressTransitionEvent(context.Context, Status, string, time.Time) (bool, error)
}

type EventPublisher interface {
	Publish(context.Context, events.Event) error
	ReleaseRuntimeIngressQueue(context.Context, time.Duration, int) (int, error)
}

type Options struct {
	Now             func() time.Time
	ReleaseLookback time.Duration
	ReleaseLimit    int
}

type Controller struct {
	mu              sync.Mutex
	store           Store
	publisher       EventPublisher
	now             func() time.Time
	releaseLookback time.Duration
	releaseLimit    int
	memory          State
	memoryInit      bool
}

func NewController(store Store, publisher EventPublisher, opts Options) *Controller {
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
		publisher:       publisher,
		now:             now,
		releaseLookback: lookback,
		releaseLimit:    limit,
	}
}

func (c *Controller) SyncState(ctx context.Context) error {
	state, err := c.ensureState(ctx, c.currentTime())
	if err != nil {
		return err
	}
	syncRuntimeBus(state.Status)
	return nil
}

func (c *Controller) Pause(ctx context.Context, req TransitionRequest) (TransitionResult, error) {
	return c.transition(ctx, StatusPaused, req, ErrAlreadyPaused)
}

func (c *Controller) Resume(ctx context.Context, req TransitionRequest) (TransitionResult, error) {
	return c.transition(ctx, StatusRunning, req, ErrNotPaused)
}

// SafetyPause is used by internal safety triggers such as auth breakers. It
// consumes the same owner but intentionally does not expose public API
// idempotency/resource-state semantics.
func (c *Controller) SafetyPause(ctx context.Context, req TransitionRequest) (TransitionResult, error) {
	return c.transition(ctx, StatusPaused, req, nil)
}

func (c *Controller) QueueableIngressPaused(ctx context.Context) (bool, error) {
	state, err := c.ensureState(ctx, c.currentTime())
	if err != nil {
		return false, err
	}
	syncRuntimeBus(state.Status)
	return state.Status == StatusPaused, nil
}

func (c *Controller) RequestResponseIngressPaused(ctx context.Context) (bool, error) {
	return c.QueueableIngressPaused(ctx)
}

func (c *Controller) AdmitQueueableIngress(ctx context.Context, _ string) error {
	_, err := c.ensureState(ctx, c.currentTime())
	return err
}

func (c *Controller) transition(ctx context.Context, target Status, req TransitionRequest, alreadyErr error) (TransitionResult, error) {
	if c == nil {
		return TransitionResult{}, fmt.Errorf("runtime ingress controller is required")
	}
	now := req.Now
	if now.IsZero() {
		now = c.currentTime()
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "operator_request"
	}
	controlledBy := strings.TrimSpace(req.ControlledBy)
	if controlledBy == "" {
		controlledBy = "api.v1"
	}
	state, changed, err := c.transitionState(ctx, target, reason, controlledBy, now)
	if err != nil {
		return TransitionResult{}, err
	}
	syncRuntimeBus(state.Status)
	result := TransitionResult{Status: state.Status, TransitionID: state.TransitionEventID}
	if !changed {
		if alreadyErr != nil {
			return result, alreadyErr
		}
		return result, nil
	}
	// Once the owner state has committed, callers must see the transition as
	// successful so API idempotency can record and replay the completed write.
	eventID, err := c.publishTransitionEvent(ctx, target, reason, controlledBy, now)
	if err != nil {
		return result, nil
	}
	result.TransitionID = eventID
	if eventID != "" {
		_, _ = c.setTransitionEvent(ctx, target, eventID, state.UpdatedAt)
	}
	if target == StatusRunning {
		released, err := c.releaseQueued(ctx)
		if err != nil {
			result.ReleasedCount = released
			return result, nil
		}
		result.ReleasedCount = released
	}
	return result, nil
}

func (c *Controller) transitionState(ctx context.Context, target Status, reason, controlledBy string, now time.Time) (State, bool, error) {
	if c.store != nil {
		return c.store.TransitionRuntimeIngressState(ctx, target, reason, controlledBy, now)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.memoryInit {
		c.memory = State{Status: StatusRunning, ControlledBy: "runtime", UpdatedAt: now}
		c.memoryInit = true
	}
	if c.memory.Status == target {
		return c.memory, false, nil
	}
	c.memory.Status = target
	c.memory.Reason = reason
	c.memory.ControlledBy = controlledBy
	c.memory.TransitionEventID = ""
	c.memory.UpdatedAt = now
	return c.memory, true, nil
}

func (c *Controller) ensureState(ctx context.Context, now time.Time) (State, error) {
	if c == nil {
		return State{Status: StatusRunning}, nil
	}
	if c.store != nil {
		state, err := c.store.LoadRuntimeIngressState(ctx)
		if err == nil {
			return state, nil
		}
		if !errors.Is(err, ErrStateNotInitialized) {
			return State{}, err
		}
		return c.store.EnsureRuntimeIngressState(ctx, now)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.memoryInit {
		status := StatusRunning
		if runtimebus.RuntimeIngressPaused() {
			status = StatusPaused
		}
		c.memory = State{Status: status, ControlledBy: "runtime", UpdatedAt: now}
		c.memoryInit = true
	}
	return c.memory, nil
}

func (c *Controller) setTransitionEvent(ctx context.Context, target Status, eventID string, now time.Time) (bool, error) {
	if strings.TrimSpace(eventID) == "" {
		return false, nil
	}
	if c.store != nil {
		return c.store.SetRuntimeIngressTransitionEvent(ctx, target, eventID, now)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.memory.Status != target || !c.memory.UpdatedAt.Equal(now) {
		return false, nil
	}
	c.memory.TransitionEventID = strings.TrimSpace(eventID)
	c.memory.UpdatedAt = now
	return true, nil
}

func (c *Controller) publishTransitionEvent(ctx context.Context, target Status, reason, controlledBy string, now time.Time) (string, error) {
	if c.publisher == nil {
		return "", nil
	}
	eventType := events.EventType("platform.paused")
	payload := map[string]any{
		"reason":    reason,
		"paused_by": controlledBy,
		"timestamp": now.Format(time.RFC3339Nano),
	}
	if target == StatusRunning {
		eventType = events.EventType("platform.resumed")
		payload = map[string]any{
			"reason":     reason,
			"resumed_by": controlledBy,
			"timestamp":  now.Format(time.RFC3339Nano),
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	eventID := uuid.NewString()
	if err := c.publisher.Publish(ctx, events.Event{
		ID:          eventID,
		Type:        eventType,
		SourceAgent: "runtime",
		Payload:     raw,
		CreatedAt:   now,
	}); err != nil {
		return "", err
	}
	return eventID, nil
}

func (c *Controller) releaseQueued(ctx context.Context) (int, error) {
	if c == nil || c.publisher == nil {
		return 0, nil
	}
	total := 0
	for {
		n, err := c.publisher.ReleaseRuntimeIngressQueue(ctx, c.releaseLookback, c.releaseLimit)
		total += n
		if err != nil {
			return total, err
		}
		if n < c.releaseLimit {
			return total, nil
		}
	}
}

func (c *Controller) currentTime() time.Time {
	if c == nil || c.now == nil {
		return time.Now().UTC()
	}
	return c.now().UTC()
}

func syncRuntimeBus(status Status) {
	switch status {
	case StatusPaused:
		runtimebus.PauseRuntimeIngress()
	default:
		runtimebus.ResumeRuntimeIngress()
	}
}
