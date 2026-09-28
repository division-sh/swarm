package deliverycontinuation

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const (
	scanPageSize     = 32
	dispatchWorkers  = 8
	dispatchCapacity = 32
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

type dispatchJob struct {
	deliveryID string
	event      events.Event
	route      events.DeliveryRoute
	lease      *worklifetime.Lease
}

// Coordinator is the one execution-generation owner for executable
// delivery continuations. It is a bounded selected-store projection, not a
// durable queue or a second eligibility clock.
type Coordinator struct {
	store      runtimedelivery.Store
	restarts   runtimepipeline.StandingRestartDispositionReader
	authority  runtimedelivery.ExecutionAuthority
	workOwner  worklifetime.Occurrence
	dispatcher Dispatcher
	report     ErrorReporter

	mu            sync.Mutex
	entries       map[string]entry
	reserved      map[string]struct{}
	started       bool
	retired       bool
	wake          chan struct{}
	sync          chan synchronizationRequest
	done          chan struct{}
	cancel        context.CancelFunc
	failure       error
	workerFailure error
	workerLimit   int
	jobs          chan dispatchJob
	workers       sync.WaitGroup
	rescanNeeded  bool
	scanCursor    runtimedelivery.ContinuationCursor
	scanSeen      map[string]struct{}
	scanSkipped   bool
}

func New(
	store runtimedelivery.Store,
	restarts runtimepipeline.StandingRestartDispositionReader,
	authority runtimedelivery.ExecutionAuthority,
	workOwner worklifetime.Occurrence,
	dispatcher Dispatcher,
	report ErrorReporter,
) (*Coordinator, error) {
	if authority.Kind() != runtimedelivery.ExecutionAuthorityNormalRuntime {
		return nil, errors.New("normal delivery continuation coordinator requires normal execution authority")
	}
	return newCoordinator(store, restarts, authority, workOwner, dispatcher, report)
}

// NewSelected reuses the durable continuation owner for one selected fork
// generation. Selected work has no normal standing-restart classification.
func NewSelected(
	store runtimedelivery.Store,
	authority runtimedelivery.ExecutionAuthority,
	workOwner worklifetime.Occurrence,
	dispatcher Dispatcher,
	report ErrorReporter,
) (*Coordinator, error) {
	if authority.Kind() != runtimedelivery.ExecutionAuthoritySelectedContractFork {
		return nil, errors.New("selected delivery continuation coordinator requires selected execution authority")
	}
	return newCoordinator(store, nil, authority, workOwner, dispatcher, report)
}

func newCoordinator(
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
	if authority.Kind() == runtimedelivery.ExecutionAuthorityNormalRuntime && restarts == nil {
		return nil, errors.New("delivery continuation standing restart reader is required")
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
	workerLimit := dispatchWorkers
	if bounded, ok := store.(interface{ DeliveryContinuationWorkerLimit() int }); ok {
		workerLimit = bounded.DeliveryContinuationWorkerLimit()
		if workerLimit < 1 || workerLimit > dispatchWorkers {
			return nil, fmt.Errorf("delivery continuation worker limit must be between 1 and %d", dispatchWorkers)
		}
	}
	return &Coordinator{
		store: store, restarts: restarts, authority: authority, workOwner: workOwner, dispatcher: dispatcher, report: report,
		workerLimit: workerLimit,
		entries:     make(map[string]entry), reserved: make(map[string]struct{}),
		wake: make(chan struct{}, 1), sync: make(chan synchronizationRequest), done: make(chan struct{}),
		jobs: make(chan dispatchJob, dispatchCapacity),
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
	c.workers.Add(c.workerLimit)
	for range c.workerLimit {
		go c.dispatch(runCtx)
	}
	next, wake, err := c.scan(runCtx, true)
	if err != nil {
		return c.finish(runCtx, cancel, lease, fmt.Errorf("enumerate delivery continuations before readiness: %w", err), false)
	}
	c.mu.Lock()
	retired = c.retired
	if !retired {
		err = c.workerFailure
		if err == nil {
			err = runCtx.Err()
		}
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
		next, wake, err = c.scan(ctx, synchronized != nil)
		if err == nil {
			c.mu.Lock()
			if c.retired {
				err = errCoordinatorRetired
			} else {
				err = c.workerFailure
				if err == nil {
					err = ctx.Err()
				}
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
	cancel()
	c.workers.Wait()
	for {
		select {
		case job := <-c.jobs:
			if job.lease != nil {
				if doneErr := job.lease.Done(); doneErr != nil {
					err = errors.Join(err, doneErr)
				}
			}
		default:
			goto drained
		}
	}
drained:
	canceledAsOrdinary := ordinaryCoordinatorStop(ctx, err, retired)
	c.mu.Lock()
	workerFailure := c.workerFailure
	c.mu.Unlock()
	failure := errors.Join(err, workerFailure)
	if canceledAsOrdinary && workerFailure == nil {
		failure = nil
	}
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
	return errors.Join(err, workerFailure, cleanupErr)
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

// The existing worker lease joins admitted SQL reads. The mutex check is their
// admission point against Retire; no lock is held during I/O or dispatch.
func (c *Coordinator) scanStopError(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired {
		return errCoordinatorRetired
	}
	if c.workerFailure != nil {
		return c.workerFailure
	}
	return ctx.Err()
}

func (c *Coordinator) dispatch(ctx context.Context) {
	defer c.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-c.jobs:
			if ctx.Err() != nil {
				c.completeDispatch(job.deliveryID, job.lease.Done())
				return
			}
			result := c.dispatcher.DispatchDeliveryContinuation(job.lease.Context(), job.event, job.route)
			var err error
			if validationErr := result.Validate(); validationErr != nil {
				err = fmt.Errorf("dispatch delivery continuation %s returned invalid result: %w", job.deliveryID, validationErr)
			} else {
				switch result.Disposition() {
				case DispatchTransferred, DispatchAlreadyOwned, DispatchDeferred:
				case DispatchTerminal:
					err = c.releaseTerminal(job.deliveryID)
				case DispatchFatal:
					err = fmt.Errorf("dispatch delivery continuation %s: %w", job.deliveryID, result.Failure())
				default:
					err = fmt.Errorf("dispatch delivery continuation %s returned unknown disposition", job.deliveryID)
				}
			}
			if job.lease.Context().Err() != nil && ordinaryCoordinatorStop(job.lease.Context(), err, true) {
				err = nil
			}
			err = errors.Join(err, job.lease.Done())
			c.completeDispatch(job.deliveryID, err)
			if err != nil {
				return
			}
		}
	}
}

func (c *Coordinator) completeDispatch(deliveryID string, err error) {
	c.mu.Lock()
	delete(c.reserved, deliveryID)
	if err != nil {
		c.workerFailure = errors.Join(c.workerFailure, err)
	}
	wake := err != nil || (c.rescanNeeded && len(c.reserved) <= dispatchCapacity/2)
	if wake {
		c.rescanNeeded = false
	}
	c.mu.Unlock()
	if wake {
		c.Signal()
	}
}

func (c *Coordinator) schedule(ctx context.Context, item runtimedelivery.ContinuationItem) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired || ctx.Err() != nil {
		return errCoordinatorRetired
	}
	if c.workerFailure != nil {
		return c.workerFailure
	}
	if _, exists := c.reserved[item.DeliveryID]; exists {
		return nil
	}
	if len(c.reserved) == dispatchCapacity {
		c.rescanNeeded = true
		return nil
	}
	lease, err := c.workOwner.Begin(ctx)
	if err != nil {
		return fmt.Errorf("admit delivery continuation dispatch %s: %w", item.DeliveryID, err)
	}
	c.reserved[item.DeliveryID] = struct{}{}
	c.jobs <- dispatchJob{deliveryID: item.DeliveryID, event: item.Event, route: item.Snapshot.Route, lease: lease}
	return nil
}

func (c *Coordinator) scan(ctx context.Context, exhaustive bool) (time.Duration, bool, error) {
	var cursor runtimedelivery.ContinuationCursor
	var next time.Duration
	var wake bool
	seen := make(map[string]struct{})
	if !exhaustive {
		cursor = c.scanCursor
		for deliveryID := range c.scanSeen {
			seen[deliveryID] = struct{}{}
		}
	}
	for {
		if err := c.scanStopError(ctx); err != nil {
			return 0, false, err
		}
		beforeScan := c.heldEntries()
		page, err := c.store.ScanDeliveryContinuations(context.WithoutCancel(ctx), c.authority, cursor, scanPageSize)
		if err != nil {
			return 0, false, err
		}
		if err := c.scanStopError(ctx); err != nil {
			return 0, false, err
		}
		for _, item := range page.Items {
			seen[item.DeliveryID] = struct{}{}
			if err := c.scanStopError(ctx); err != nil {
				return 0, false, err
			}
			if c.restarts != nil && item.Snapshot.RunID != "" && item.Disposition != runtimedelivery.ClaimAbsent && item.Disposition != runtimedelivery.ClaimInvariantInvalid {
				disposition, err := c.restarts.StandingRunRestartDisposition(context.WithoutCancel(ctx), item.Snapshot.RunID)
				if err != nil {
					return 0, false, fmt.Errorf("classify delivery continuation %s standing disposition: %w", item.DeliveryID, err)
				}
				if err := c.scanStopError(ctx); err != nil {
					return 0, false, err
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
				if err := c.schedule(ctx, item); err != nil {
					return 0, false, err
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
		if !exhaustive && c.deferRemainderAtCapacity() {
			c.scanCursor = page.Next
			c.scanSeen = seen
			c.scanSkipped = true
			return next, wake, nil
		}
		cursor = page.Next
	}
	if !exhaustive && c.scanSkipped {
		c.mu.Lock()
		if len(c.reserved) <= dispatchCapacity/2 {
			next, wake = earlierWake(next, wake, 0)
		} else {
			c.rescanNeeded = true
		}
		c.mu.Unlock()
	}
	c.scanCursor = runtimedelivery.ContinuationCursor{}
	c.scanSeen = nil
	c.scanSkipped = false
	reconcileNext, reconcileWake, err := c.reconcileHeld(ctx, seen)
	if err != nil {
		return 0, false, err
	}
	if reconcileWake {
		next, wake = earlierWake(next, wake, reconcileNext)
	}
	return next, wake, nil
}

func (c *Coordinator) deferRemainderAtCapacity() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reserved) < dispatchCapacity {
		return false
	}
	c.rescanNeeded = true
	return true
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
	if _, reserved := c.reserved[deliveryID]; reserved {
		return false
	}
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

func (c *Coordinator) reconcileHeld(ctx context.Context, seen map[string]struct{}) (time.Duration, bool, error) {
	var next time.Duration
	var wake bool
	held := c.heldEntries()
	missing := make([]string, 0, len(held))
	for deliveryID := range held {
		if _, current := seen[deliveryID]; current {
			continue
		}
		missing = append(missing, deliveryID)
	}
	slices.Sort(missing)
	for start := 0; start < len(missing); start += runtimedelivery.MaxContinuationObservationBatch {
		end := min(start+runtimedelivery.MaxContinuationObservationBatch, len(missing))
		batch := missing[start:end]
		if err := c.scanStopError(ctx); err != nil {
			return 0, false, err
		}
		observations, err := c.store.ObserveDeliveryContinuations(context.WithoutCancel(ctx), c.authority, batch)
		if err != nil {
			return 0, false, err
		}
		if err := c.scanStopError(ctx); err != nil {
			return 0, false, err
		}
		if len(observations) != len(batch) {
			return 0, false, fmt.Errorf("delivery continuation observation batch returned %d entries for %d identities", len(observations), len(batch))
		}
		for i, observation := range observations {
			deliveryID := batch[i]
			if observation.DeliveryID != deliveryID {
				return 0, false, fmt.Errorf("delivery continuation observation %s returned identity %s", deliveryID, observation.DeliveryID)
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
				if c.reclaimAttempt(deliveryID, held[deliveryID]) {
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
