package deliverycontinuation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const (
	scanPageSize = 200
)

var errCoordinatorRetired = errors.New("delivery continuation coordinator is retired")

// Dispatcher re-enters the existing exact EventBus route. The selected store,
// not the coordinator, still decides whether the delivery can be claimed.
type Dispatcher interface {
	DispatchDeliveryContinuation(context.Context, events.Event, events.DeliveryRoute) DispatchResult
}

type ErrorReporter func(context.Context, error)

type ownershipState uint8

const (
	ownershipCoordinator ownershipState = iota + 1
	ownershipCarrier
	ownershipAttempt
	ownershipTerminalCarrier
	ownershipTerminal
)

type entry struct {
	state   ownershipState
	carrier *capability
}

type synchronizationRequest struct {
	result chan error
}

// Coordinator is the one normal-runtime-generation owner for executable
// delivery continuations. It is a bounded selected-store projection, not a
// durable queue or a second eligibility clock.
type Coordinator struct {
	store      runtimedelivery.Store
	restarts   runtimepipeline.StandingRestartDispositionReader
	authority  runtimedelivery.ExecutionAuthority
	workOwner  worklifetime.Occurrence
	dispatcher Dispatcher
	report     ErrorReporter

	mu      sync.Mutex
	entries map[string]entry
	started bool
	retired bool
	wake    chan struct{}
	sync    chan synchronizationRequest
	done    chan struct{}
	cancel  context.CancelFunc
	failure error
}

func New(
	store runtimedelivery.Store,
	restarts runtimepipeline.StandingRestartDispositionReader,
	authority runtimedelivery.ExecutionAuthority,
	workOwner worklifetime.Occurrence,
	dispatcher Dispatcher,
	report ErrorReporter,
) (*Coordinator, error) {
	if store == nil {
		return nil, errors.New("delivery continuation selected store is required")
	}
	if restarts == nil {
		return nil, errors.New("delivery continuation standing restart reader is required")
	}
	if authority.Kind() != runtimedelivery.ExecutionAuthorityNormalRuntime {
		return nil, errors.New("normal delivery continuation coordinator requires normal execution authority")
	}
	if err := authority.Validate(); err != nil {
		return nil, err
	}
	if workOwner == nil {
		return nil, errors.New("delivery continuation work owner is required")
	}
	if dispatcher == nil {
		return nil, errors.New("delivery continuation dispatcher is required")
	}
	return &Coordinator{
		store: store, restarts: restarts, authority: authority, workOwner: workOwner, dispatcher: dispatcher, report: report,
		entries: make(map[string]entry), wake: make(chan struct{}, 1), sync: make(chan synchronizationRequest), done: make(chan struct{}),
	}, nil
}

func (c *Coordinator) Authority() runtimedelivery.ExecutionAuthority {
	if c == nil {
		return runtimedelivery.ExecutionAuthority{}
	}
	return c.authority
}

// Start completes the bounded selected-store enumeration before returning.
// Runtime readiness therefore cannot precede representation of existing work.
func (c *Coordinator) Start(ctx context.Context) error {
	if c == nil {
		return errors.New("delivery continuation coordinator is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.started || c.retired {
		c.mu.Unlock()
		return errors.New("delivery continuation coordinator cannot be started")
	}
	// Retirement owns cancellation even while the standing lease is being acquired.
	startCtx, cancel := context.WithCancel(ctx)
	c.started, c.cancel = true, cancel
	c.mu.Unlock()

	lease, err := c.workOwner.BeginStanding(startCtx)
	if err != nil {
		return c.finish(startCtx, cancel, nil, fmt.Errorf("admit delivery continuation coordinator: %w", err), false)
	}
	runCtx := lease.Context()
	c.mu.Lock()
	retired := c.retired
	c.mu.Unlock()
	if retired {
		return c.finish(runCtx, cancel, lease, errCoordinatorRetired, false)
	}
	if err := runCtx.Err(); err != nil {
		return c.finish(runCtx, cancel, lease, err, false)
	}
	next, wake, err := c.scan(runCtx)
	if err != nil {
		return c.finish(runCtx, cancel, lease, fmt.Errorf("enumerate delivery continuations before readiness: %w", err), false)
	}
	c.mu.Lock()
	retired = c.retired
	if !retired {
		err = runCtx.Err()
	} else {
		err = errCoordinatorRetired
	}
	c.mu.Unlock()
	if err != nil {
		return c.finish(runCtx, cancel, lease, err, false)
	}
	go func() {
		var runErr error
		defer func() { c.finish(runCtx, cancel, lease, runErr, true) }()
		runErr = c.run(runCtx, next, wake)
	}()
	c.Signal()
	return nil
}

// Synchronize completes one scan requested after a startup-owned lifecycle
// transition, such as publishing restored agent routes. It reports the exact
// scan result and does not wait for or manufacture durable eligibility.
func (c *Coordinator) Synchronize(ctx context.Context) error {
	if c == nil {
		return errors.New("delivery continuation coordinator is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	started := c.started
	retired := c.retired
	failure := c.failure
	c.mu.Unlock()
	if !started {
		return errors.New("delivery continuation coordinator is not started")
	}
	if failure != nil {
		return failure
	}
	if retired {
		return errCoordinatorRetired
	}
	request := synchronizationRequest{result: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.stoppedError()
	case c.sync <- request:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		select {
		case err := <-request.result:
			return err
		default:
			return c.stoppedError()
		}
	case err := <-request.result:
		return err
	}
}

func (c *Coordinator) Retire(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	c.retired = true
	cancel := c.cancel
	started := c.started
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if !started {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.failure
	}
}

func (c *Coordinator) Signal() {
	if c == nil {
		return
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// AcceptCommitted performs publication-owner to coordinator transfer using
// only the exact handoffs returned by the committing transaction.
func (c *Coordinator) AcceptCommitted(proofs []runtimedelivery.DurableHandoffProof) error {
	if c == nil {
		return errors.New("delivery continuation coordinator is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired {
		return errCoordinatorRetired
	}
	seen := make(map[string]struct{}, len(proofs))
	for _, proof := range proofs {
		if err := proof.Validate(); err != nil {
			return err
		}
		if !proof.Authority().Equal(c.authority) {
			return fmt.Errorf("delivery %s belongs to a different execution authority", proof.DeliveryID())
		}
		if _, duplicate := seen[proof.DeliveryID()]; duplicate {
			return fmt.Errorf("delivery %s is duplicated in committed handoff batch", proof.DeliveryID())
		}
		seen[proof.DeliveryID()] = struct{}{}
		if current, exists := c.entries[proof.DeliveryID()]; exists {
			switch current.state {
			case ownershipCoordinator, ownershipCarrier, ownershipAttempt, ownershipTerminalCarrier, ownershipTerminal:
			default:
				return fmt.Errorf("delivery %s has unknown continuation ownership", proof.DeliveryID())
			}
		}
	}
	for _, proof := range proofs {
		if _, exists := c.entries[proof.DeliveryID()]; !exists {
			c.entries[proof.DeliveryID()] = entry{state: ownershipCoordinator}
		}
	}
	c.Signal()
	return nil
}

// Retain transfers a retry-scheduled attempt back to this generation before
// the attempt may report completion.
func (c *Coordinator) Retain(snapshot runtimedelivery.Snapshot) error {
	if c == nil {
		return errors.New("delivery continuation coordinator is required")
	}
	if snapshot.DeliveryID == "" || snapshot.Status != runtimedelivery.StatusFailed || !snapshot.Authority.Equal(c.authority) {
		return errors.New("retry continuation snapshot is invalid")
	}
	c.mu.Lock()
	if c.retired {
		c.mu.Unlock()
		return errCoordinatorRetired
	}
	current, exists := c.entries[snapshot.DeliveryID]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("delivery %s has no process-local continuation owner", snapshot.DeliveryID)
	}
	switch current.state {
	case ownershipCoordinator, ownershipCarrier, ownershipAttempt, ownershipTerminalCarrier, ownershipTerminal:
		// A scan may already have reclaimed this committed retry and transferred
		// it again. Retain signals durable evidence; it never overwrites an owner.
	default:
		c.mu.Unlock()
		return fmt.Errorf("delivery %s has unknown continuation ownership", snapshot.DeliveryID)
	}
	c.mu.Unlock()
	c.Signal()
	return nil
}

// Release settles the exact process-local continuation after the selected
// store has committed a terminal delivery transition, or fences a live carrier
// until that exact capability resolves.
func (c *Coordinator) Release(deliveryID string) error {
	if c == nil || deliveryID == "" {
		return errors.New("exact delivery continuation identity is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.releaseTerminalLocked(deliveryID)
}

func (c *Coordinator) releaseTerminalLocked(deliveryID string) error {
	current, exists := c.entries[deliveryID]
	if !exists {
		c.entries[deliveryID] = entry{state: ownershipTerminal}
		return nil
	}
	switch current.state {
	case ownershipCoordinator, ownershipAttempt:
		c.entries[deliveryID] = entry{state: ownershipTerminal}
		return nil
	case ownershipCarrier:
		current.state = ownershipTerminalCarrier
		c.entries[deliveryID] = current
		return nil
	case ownershipTerminalCarrier, ownershipTerminal:
		// Keep terminal evidence until this generation is released; a delayed
		// committed handoff or scan must not revive the delivery.
		return nil
	default:
		return fmt.Errorf("delivery %s has unknown continuation ownership", deliveryID)
	}
}

func (*Coordinator) OwnsPersistedRecovery() bool { return true }

// Acquire transfers one exact coordinator-held continuation to a carrier.
// Callers must attach the returned capability before enqueueing the carrier.
func (c *Coordinator) Acquire(deliveryID string) (worklifetime.DeliveryAcquisition, error) {
	if c == nil || deliveryID == "" {
		return worklifetime.DeliveryAcquisition{}, errors.New("exact delivery continuation identity is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired {
		return worklifetime.DeliveryAcquisition{}, errCoordinatorRetired
	}
	current, exists := c.entries[deliveryID]
	if !exists {
		return worklifetime.DeliveryAcquisition{}, fmt.Errorf("delivery %s has no continuation evidence", deliveryID)
	}
	switch current.state {
	case ownershipCarrier, ownershipAttempt:
		return worklifetime.AlreadyOwnedDelivery(deliveryID), nil
	case ownershipTerminalCarrier, ownershipTerminal:
		return worklifetime.TerminallyFencedDelivery(deliveryID), nil
	case ownershipCoordinator:
		cap := &capability{coordinator: c, deliveryID: deliveryID}
		c.entries[deliveryID] = entry{state: ownershipCarrier, carrier: cap}
		return worklifetime.AcquiredDelivery(cap), nil
	default:
		return worklifetime.DeliveryAcquisition{}, fmt.Errorf("delivery %s has unknown continuation ownership", deliveryID)
	}
}

func (c *Coordinator) run(ctx context.Context, next time.Duration, wake bool) error {
	var timer *time.Timer
	var err error
	if wake {
		timer = time.NewTimer(next)
	}
	for {
		var timerC <-chan time.Time
		if timer != nil {
			timerC = timer.C
		}
		var synchronized chan error
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return ctx.Err()
		case <-c.wake:
		case <-timerC:
		case request := <-c.sync:
			synchronized = request.result
		}
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		next, wake, err = c.scan(ctx)
		if err == nil {
			c.mu.Lock()
			if c.retired {
				err = errCoordinatorRetired
			} else {
				err = ctx.Err()
			}
			c.mu.Unlock()
		}
		if synchronized != nil {
			synchronized <- err
			close(synchronized)
		}
		if err != nil {
			return err
		}
		if wake {
			timer = time.NewTimer(next)
		}
	}
}

// finish is called exactly once by Start (abort) or the worker (successful start).
// Classify before cancellation, and settle the lease before publishing completion.
func (c *Coordinator) finish(ctx context.Context, cancel context.CancelFunc, lease *worklifetime.Lease, err error, report bool) error {
	c.mu.Lock()
	retired := c.retired
	c.retired = true
	c.mu.Unlock()
	failure := err
	if ordinaryCoordinatorStop(ctx, err, retired) {
		failure = nil
	}
	cancel()
	var cleanupErr error
	if lease != nil {
		cleanupErr = lease.Done()
		if cleanupErr != nil {
			failure = errors.Join(failure, cleanupErr)
		}
	}
	c.mu.Lock()
	c.failure = failure
	c.mu.Unlock()
	if report && failure != nil && c.report != nil {
		c.report(ctx, failure)
	}
	close(c.done)
	if cleanupErr != nil {
		return errors.Join(err, cleanupErr)
	}
	return err
}

func ordinaryCoordinatorStop(ctx context.Context, err error, retired bool) bool {
	if err == nil {
		return true
	}
	// Every branch must be owned cancellation. A joined independent failure is
	// still fatal even when it also contains context.Canceled or retirement.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if !ordinaryCoordinatorStop(ctx, cause, retired) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if cause := wrapped.Unwrap(); cause != nil {
			return ordinaryCoordinatorStop(ctx, cause, retired)
		}
	}
	return (retired && err == errCoordinatorRetired) || (ctx.Err() != nil && (err == ctx.Err() || err == context.Cause(ctx)))
}

func (c *Coordinator) stoppedError() error {
	if c == nil {
		return errors.New("delivery continuation coordinator is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	if c.retired {
		return errCoordinatorRetired
	}
	return errors.New("delivery continuation coordinator stopped without a result")
}

func (c *Coordinator) scan(ctx context.Context) (time.Duration, bool, error) {
	var cursor runtimedelivery.ContinuationCursor
	var next time.Duration
	var wake bool
	for {
		beforeScan := c.heldEntries()
		page, err := c.store.ScanDeliveryContinuations(ctx, c.authority, cursor, scanPageSize)
		if err != nil {
			return 0, false, err
		}
		for _, item := range page.Items {
			if item.Snapshot.RunID != "" && item.Disposition != runtimedelivery.ClaimAbsent && item.Disposition != runtimedelivery.ClaimInvariantInvalid {
				disposition, err := c.restarts.StandingRunRestartDisposition(ctx, item.Snapshot.RunID)
				if err != nil {
					return 0, false, fmt.Errorf("classify delivery continuation %s standing disposition: %w", item.DeliveryID, err)
				}
				if disposition.ExactCurrent() && !disposition.Executable() {
					continue
				}
			}
			if after, ok := item.Wake.After(); ok {
				next, wake = earlierWake(next, wake, after)
			}
			switch item.Disposition {
			case runtimedelivery.ClaimAcquired, runtimedelivery.ClaimReclaimable:
				if err := c.observe(item.DeliveryID); err != nil {
					return 0, false, err
				}
				c.reclaimAttempt(item.DeliveryID, beforeScan[item.DeliveryID])
				{
					result := c.dispatcher.DispatchDeliveryContinuation(ctx, item.Event, item.Snapshot.Route)
					if err := result.Validate(); err != nil {
						return 0, false, fmt.Errorf("dispatch delivery continuation %s returned invalid result: %w", item.DeliveryID, err)
					}
					switch result.Disposition() {
					case DispatchTransferred, DispatchAlreadyOwned:
					case DispatchTerminal:
						if err := c.releaseTerminal(item.DeliveryID); err != nil {
							return 0, false, err
						}
					case DispatchDeferred:
						// The named owner signals this coordinator after its
						// transition commits. A deferral never invents a timer.
					case DispatchFatal:
						return 0, false, fmt.Errorf("dispatch delivery continuation %s: %w", item.DeliveryID, result.Failure())
					default:
						return 0, false, fmt.Errorf("dispatch delivery continuation %s returned unknown disposition", item.DeliveryID)
					}
				}
			case runtimedelivery.ClaimDeferred:
				if err := c.observe(item.DeliveryID); err != nil {
					return 0, false, err
				}
			case runtimedelivery.ClaimBusy:
				if err := c.observe(item.DeliveryID); err != nil {
					return 0, false, err
				}
			case runtimedelivery.ClaimTerminal:
				if err := c.releaseTerminal(item.DeliveryID); err != nil {
					return 0, false, err
				}
			case runtimedelivery.ClaimWrongAuthority:
				return 0, false, fmt.Errorf("continuation %s crossed execution authority", item.DeliveryID)
			case runtimedelivery.ClaimAbsent:
				return 0, false, fmt.Errorf("continuation %s has no durable delivery", item.DeliveryID)
			case runtimedelivery.ClaimInvariantInvalid:
				return 0, false, fmt.Errorf("continuation %s violates durable invariant: %w", item.DeliveryID, item.Invariant)
			default:
				return 0, false, fmt.Errorf("continuation %s has unknown disposition %q", item.DeliveryID, item.Disposition)
			}
		}
		if page.Exhausted {
			break
		}
		cursor = page.Next
	}
	reconcileNext, reconcileWake, err := c.reconcileHeld(ctx)
	if err != nil {
		return 0, false, err
	}
	if reconcileWake {
		next, wake = earlierWake(next, wake, reconcileNext)
	}
	return next, wake, nil
}

func (c *Coordinator) observe(deliveryID string) error {
	if deliveryID == "" {
		return errors.New("delivery continuation scan returned empty identity")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired {
		return errCoordinatorRetired
	}
	if current, exists := c.entries[deliveryID]; !exists {
		c.entries[deliveryID] = entry{state: ownershipCoordinator}
	} else {
		switch current.state {
		case ownershipCoordinator, ownershipCarrier, ownershipAttempt, ownershipTerminalCarrier, ownershipTerminal:
		default:
			return fmt.Errorf("delivery %s has unknown continuation ownership", deliveryID)
		}
	}
	return nil
}

func (c *Coordinator) reclaimAttempt(deliveryID string, observed entry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, exists := c.entries[deliveryID]; exists && current == observed && observed.state == ownershipAttempt {
		c.entries[deliveryID] = entry{state: ownershipCoordinator}
		return true
	}
	return false
}

func (c *Coordinator) heldEntries() map[string]entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	held := make(map[string]entry, len(c.entries))
	for deliveryID, current := range c.entries {
		if current.state != ownershipTerminal && current.state != ownershipTerminalCarrier {
			held[deliveryID] = current
		}
	}
	return held
}

func (c *Coordinator) reconcileHeld(ctx context.Context) (time.Duration, bool, error) {
	var next time.Duration
	var wake bool
	for deliveryID, observed := range c.heldEntries() {
		observation, err := c.store.ObserveDeliveryContinuation(ctx, c.authority, deliveryID)
		if err != nil {
			return 0, false, err
		}
		if after, ok := observation.Wake.After(); ok {
			next, wake = earlierWake(next, wake, after)
		}
		switch observation.Disposition {
		case runtimedelivery.ClaimTerminal:
			if err := c.releaseTerminal(deliveryID); err != nil {
				return 0, false, err
			}
		case runtimedelivery.ClaimAcquired, runtimedelivery.ClaimReclaimable:
			if c.reclaimAttempt(deliveryID, observed) {
				next, wake = earlierWake(next, wake, 0)
			}
		case runtimedelivery.ClaimDeferred, runtimedelivery.ClaimBusy:
		case runtimedelivery.ClaimWrongAuthority:
			return 0, false, fmt.Errorf("continuation %s crossed execution authority", deliveryID)
		case runtimedelivery.ClaimAbsent:
			return 0, false, fmt.Errorf("continuation %s has no durable delivery", deliveryID)
		case runtimedelivery.ClaimInvariantInvalid:
			return 0, false, fmt.Errorf("continuation %s violates durable invariant: %w", deliveryID, observation.Invariant)
		default:
			return 0, false, fmt.Errorf("continuation %s has unknown disposition %q", deliveryID, observation.Disposition)
		}
	}
	return next, wake, nil
}

func (c *Coordinator) releaseTerminal(deliveryID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.releaseTerminalLocked(deliveryID)
}

func earlierWake(current time.Duration, present bool, candidate time.Duration) (time.Duration, bool) {
	if candidate < 0 {
		candidate = 0
	}
	if present && current <= candidate {
		return current, true
	}
	return candidate, true
}

type capability struct {
	mu          sync.Mutex
	coordinator *Coordinator
	deliveryID  string
	settled     bool
}

func (c *capability) DeliveryID() string {
	if c == nil {
		return ""
	}
	return c.deliveryID
}

func (c *capability) Resolve(_ context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	if c == nil || c.coordinator == nil {
		return 0, errors.New("delivery continuation capability is required")
	}
	if intent != worklifetime.DeliveryContinuationReturn && intent != worklifetime.DeliveryContinuationReturnUnqueued && intent != worklifetime.DeliveryContinuationConsume {
		return 0, errors.New("delivery continuation resolution intent is invalid")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settled {
		return 0, errors.New("delivery continuation capability is already settled")
	}
	c.coordinator.mu.Lock()
	current, exists := c.coordinator.entries[c.deliveryID]
	if !exists || current.carrier != c || (current.state != ownershipCarrier && current.state != ownershipTerminalCarrier) {
		c.coordinator.mu.Unlock()
		return 0, fmt.Errorf("delivery %s is not carrier-owned", c.deliveryID)
	}
	if current.state == ownershipTerminalCarrier {
		c.coordinator.entries[c.deliveryID] = entry{state: ownershipTerminal}
		c.coordinator.mu.Unlock()
		c.settled = true
		return worklifetime.DeliveryContinuationTerminal, nil
	}
	if c.coordinator.retired && intent == worklifetime.DeliveryContinuationConsume {
		c.coordinator.mu.Unlock()
		return 0, errors.New("retired delivery continuation coordinator cannot admit an attempt")
	}
	if intent != worklifetime.DeliveryContinuationConsume {
		c.coordinator.entries[c.deliveryID] = entry{state: ownershipCoordinator, carrier: c}
	} else {
		c.coordinator.entries[c.deliveryID] = entry{state: ownershipAttempt, carrier: c}
	}
	c.coordinator.mu.Unlock()
	c.settled = true
	if intent != worklifetime.DeliveryContinuationReturnUnqueued {
		c.coordinator.Signal()
	}
	if intent != worklifetime.DeliveryContinuationConsume {
		return worklifetime.DeliveryContinuationReturned, nil
	}
	return worklifetime.DeliveryContinuationConsumed, nil
}
