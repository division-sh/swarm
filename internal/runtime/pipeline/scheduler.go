package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/robfig/cron/v3"
)

type ScheduleOwnerKind string

const (
	ScheduleOwnerAgent  ScheduleOwnerKind = "agent"
	ScheduleOwnerSystem ScheduleOwnerKind = "system"
)

type Schedule struct {
	Context       events.DeliveryContext
	RunID         string
	AgentID       string
	OwnerKind     ScheduleOwnerKind
	AgentIdentity agentidentity.Identity
	EventType     string
	Mode          string // once | cron
	Cron          string // supports "@every <duration>" and plain duration string
	At            time.Time
	EntityID      string
	FlowInstance  string
	TaskID        string
	Payload       []byte
	RoutingSource events.RoutingSource
}

type scheduledProjectionKind uint8

const (
	scheduledProjectionUnknown scheduledProjectionKind = iota
	scheduledProjectionGeneric
	scheduledProjectionWorkflowTimer
)

type scheduledProjection struct {
	kind     scheduledProjectionKind
	generic  Schedule
	workflow WorkflowTimerWakeup
}

func newGenericScheduledProjection(sc Schedule) (scheduledProjection, cronSpec, error) {
	normalized, spec, err := validateSchedule(sc)
	if err != nil {
		return scheduledProjection{}, cronSpec{}, err
	}
	return scheduledProjection{kind: scheduledProjectionGeneric, generic: normalized}, spec, nil
}

func newWorkflowTimerScheduledProjection(wakeup WorkflowTimerWakeup) (scheduledProjection, cronSpec, error) {
	if err := wakeup.validate(); err != nil {
		return scheduledProjection{}, cronSpec{}, err
	}
	return scheduledProjection{
		kind:     scheduledProjectionWorkflowTimer,
		workflow: wakeup,
	}, cronSpec{}, nil
}

func (p scheduledProjection) key() string {
	switch p.kind {
	case scheduledProjectionGeneric:
		return scheduleKey(p.generic)
	case scheduledProjectionWorkflowTimer:
		return workflowTimerTaskFamily + "|" + p.workflow.Occurrence().Activation.ActivationID
	default:
		return ""
	}
}

func (p scheduledProjection) mode() string {
	if p.kind == scheduledProjectionGeneric {
		return p.generic.Mode
	}
	if p.kind == scheduledProjectionWorkflowTimer {
		return "once"
	}
	return ""
}

func (p scheduledProjection) dueAt() time.Time {
	if p.kind == scheduledProjectionGeneric {
		return p.generic.At
	}
	if p.kind == scheduledProjectionWorkflowTimer {
		return p.workflow.DueAt()
	}
	return time.Time{}
}

func (p scheduledProjection) clone() scheduledProjection {
	if p.kind == scheduledProjectionGeneric {
		p.generic = cloneSchedule(p.generic)
	}
	return p
}

func (p scheduledProjection) fire(
	ctx context.Context,
	genericFire func(context.Context, Schedule),
	workflowTimerFire func(context.Context, WorkflowTimerWakeup),
) {
	switch p.kind {
	case scheduledProjectionGeneric:
		if genericFire != nil {
			genericFire(ctx, p.generic)
		}
	case scheduledProjectionWorkflowTimer:
		if workflowTimerFire == nil {
			panic("workflow timer scheduler binding became unavailable")
		}
		workflowTimerFire(ctx, p.workflow)
	}
}

func (s Schedule) EffectiveRunID() string {
	return strings.TrimSpace(s.RunID)
}

func (s *Schedule) NormalizeRunID() {
	if s == nil {
		return
	}
	s.RunID = s.EffectiveRunID()
}

func (s *Schedule) NormalizeDeliveryContext() {
	if s == nil {
		return
	}
	s.Context = s.Context.Normalized()
}

func (s Schedule) EffectiveEntityID() string {
	return strings.TrimSpace(s.EntityID)
}

func (s *Schedule) NormalizeEntityID() {
	if s == nil {
		return
	}
	entityID := s.EffectiveEntityID()
	s.EntityID = entityID
}

func (s Schedule) EffectiveFlowInstance() string {
	return strings.Trim(strings.TrimSpace(s.FlowInstance), "/")
}

func (s *Schedule) NormalizeFlowInstance() {
	if s == nil {
		return
	}
	s.FlowInstance = s.EffectiveFlowInstance()
}

func (s *Schedule) NormalizeOwner() error {
	if s == nil {
		return errors.New("schedule is required")
	}
	s.OwnerKind = ScheduleOwnerKind(strings.TrimSpace(string(s.OwnerKind)))
	s.AgentID = strings.TrimSpace(s.AgentID)
	s.AgentIdentity = s.AgentIdentity.Normalize()
	switch s.OwnerKind {
	case ScheduleOwnerAgent:
		if err := s.AgentIdentity.Validate(); err != nil {
			return fmt.Errorf("agent-owned schedule requires concrete agent identity: %w", err)
		}
		if s.AgentIdentity.AgentID() != s.AgentID {
			return errors.New("agent-owned schedule owner does not match concrete agent identity")
		}
		if s.AgentIdentity.FlowInstance() != s.EffectiveFlowInstance() {
			return errors.New("agent-owned schedule flow_instance does not match concrete agent identity")
		}
	case ScheduleOwnerSystem:
		if !s.AgentIdentity.IsZero() {
			return errors.New("system-owned schedule cannot carry agent identity")
		}
	default:
		return fmt.Errorf("schedule owner_kind %q is invalid", s.OwnerKind)
	}
	return nil
}

type Scheduler struct {
	mu              sync.Mutex
	onFire          func(context.Context, Schedule)
	onWorkflowTimer func(context.Context, WorkflowTimerWakeup)
	tasks           map[string]*scheduledTask
	draining        map[*scheduledTask]struct{}
	reservations    map[string]*PreparedParkedSetRebind
	transitions     map[*PreparedParkedSetRebind]struct{}
	stopped         bool
	owner           worklifetime.Occurrence
}

type scheduledTask struct {
	stop          chan struct{}
	done          chan struct{}
	prior         <-chan struct{}
	lease         *worklifetime.Lease
	owner         worklifetime.Occurrence
	standingOwner *worklifetime.StandingOccurrence
	projection    scheduledProjection
	state         scheduledTaskState
	retiring      bool
}

type scheduledTaskState uint8

const (
	scheduledTaskArmed scheduledTaskState = iota
	scheduledTaskFiring
	scheduledTaskSettled
)

type parkedProjection struct {
	task          *scheduledTask
	projection    scheduledProjection
	originalOwner worklifetime.Occurrence
	observedState scheduledTaskState
	restorable    bool
}

type parkedOccurrenceState uint8

const (
	parkedOccurrencePending parkedOccurrenceState = iota
	parkedOccurrencePrepared
	parkedOccurrenceRestored
	parkedOccurrenceRebound
)

// ParkedOccurrence is the scheduler-owned authority for one standing
// transition. It deliberately keeps original execution owners and timer state
// private so callers cannot weaken rollback to bare schedule facts.
type ParkedOccurrence struct {
	mu            sync.Mutex
	scheduler     *Scheduler
	standingOwner *worklifetime.StandingOccurrence
	projections   []parkedProjection
	state         parkedOccurrenceState
}

// ParkedRebind binds one exact parked source authority to one fresh standing
// occurrence. The target scheduler consumes the complete set atomically.
type ParkedRebind struct {
	Parked *ParkedOccurrence
	Owner  *worklifetime.StandingOccurrence
}

type preparedParkedTask struct {
	key  string
	task *scheduledTask
	spec cronSpec
}

type preparedParkedSource struct {
	parked *ParkedOccurrence
	state  parkedOccurrenceState
}

type preparedParkedSetState uint8

const (
	preparedParkedSetPending preparedParkedSetState = iota
	preparedParkedSetCommitted
	preparedParkedSetActive
	preparedParkedSetAborted
)

// PreparedParkedSetRebind is the scheduler-owned replacement transaction for
// a complete parked service set. Target keys remain reserved until activation,
// so runtime publication can commit dormant tasks before making them runnable.
type PreparedParkedSetRebind struct {
	mu        sync.Mutex
	scheduler *Scheduler
	sources   []preparedParkedSource
	tasks     []preparedParkedTask
	keys      []string
	done      chan struct{}
	state     preparedParkedSetState
}

type cronSpec struct {
	every    time.Duration
	schedule cron.Schedule
}

func NewSchedulerWithWorkOwner(owner worklifetime.Occurrence, callbacks ...func(context.Context, Schedule)) *Scheduler {
	var cb func(context.Context, Schedule)
	if len(callbacks) > 0 {
		cb = callbacks[0]
	}
	return &Scheduler{
		onFire:       cb,
		tasks:        make(map[string]*scheduledTask),
		draining:     make(map[*scheduledTask]struct{}),
		reservations: make(map[string]*PreparedParkedSetRebind),
		transitions:  make(map[*PreparedParkedSetRebind]struct{}),
		owner:        owner,
	}
}

func (s *Scheduler) bindWorkflowTimerLifecycle(callback func(context.Context, WorkflowTimerWakeup)) error {
	if s == nil {
		return errors.New("workflow timer lifecycle requires a scheduler")
	}
	if callback == nil {
		return errors.New("workflow timer lifecycle callback is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errors.New("cannot bind workflow timer lifecycle to a stopped scheduler")
	}
	if s.onWorkflowTimer != nil {
		return errors.New("workflow timer lifecycle is already bound")
	}
	s.onWorkflowTimer = callback
	return nil
}

func (s *Scheduler) Register(ctx context.Context, sc Schedule) error {
	projection, spec, err := newGenericScheduledProjection(sc)
	if err != nil {
		return err
	}
	return s.registerProjection(ctx, projection, spec)
}

func (s *Scheduler) registerWorkflowTimerWakeup(ctx context.Context, wakeup WorkflowTimerWakeup) error {
	projection, spec, err := newWorkflowTimerScheduledProjection(wakeup)
	if err != nil {
		return err
	}
	return s.registerProjection(ctx, projection, spec)
}

func (s *Scheduler) registerProjection(ctx context.Context, projection scheduledProjection, spec cronSpec) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return errors.New("scheduler stopped")
	}
	if s.owner == nil {
		s.mu.Unlock()
		return errors.New("scheduler requires a runtime work occurrence")
	}
	if projection.kind == scheduledProjectionWorkflowTimer && s.onWorkflowTimer == nil {
		s.mu.Unlock()
		return errors.New("workflow timer lifecycle is not bound to scheduler")
	}
	key := projection.key()
	if key == "" {
		s.mu.Unlock()
		return errors.New("scheduled projection identity is required")
	}
	if _, reserved := s.reservations[key]; reserved {
		s.mu.Unlock()
		return errors.New("schedule key is reserved by a standing replacement transition")
	}
	owner := s.owner
	if contextual, ok := worklifetime.OccurrenceFromContext(ctx); ok {
		owner = contextual
	}
	lease, err := owner.Begin(context.WithoutCancel(ctx))
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("admit scheduled task: %w", err)
	}
	if existing, ok := s.tasks[key]; ok {
		taskPrior := existing.done
		s.retireTaskLocked(key, existing)
		standingOwner, _ := worklifetime.StandingProjection(owner)
		task := &scheduledTask{
			stop: make(chan struct{}), done: make(chan struct{}), prior: taskPrior, lease: lease,
			owner: owner, standingOwner: standingOwner, projection: projection.clone(),
		}
		s.tasks[key] = task
		s.mu.Unlock()
		s.startTask(key, task, spec)
		return nil
	}
	standingOwner, _ := worklifetime.StandingProjection(owner)
	task := &scheduledTask{
		stop: make(chan struct{}), done: make(chan struct{}), lease: lease,
		owner: owner, standingOwner: standingOwner, projection: projection.clone(),
	}
	s.tasks[key] = task
	s.mu.Unlock()
	s.startTask(key, task, spec)
	return nil
}

func (s *Scheduler) Wait(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	done := make([]<-chan struct{}, 0, len(s.draining)+len(s.tasks)+len(s.transitions))
	for task := range s.draining {
		done = append(done, task.done)
	}
	for _, task := range s.tasks {
		done = append(done, task.done)
	}
	for transition := range s.transitions {
		done = append(done, transition.done)
	}
	s.mu.Unlock()
	for _, taskDone := range done {
		select {
		case <-taskDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *Scheduler) CancelExact(sc Schedule) error {
	normalized, _, err := validateSchedule(sc)
	if err != nil {
		return err
	}
	key := scheduleKey(normalized)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, reserved := s.reservations[key]; reserved {
		return errors.New("schedule key is reserved by a standing replacement transition")
	}
	task, ok := s.tasks[key]
	if !ok {
		return nil
	}
	s.retireTaskLocked(key, task)
	return nil
}

func (s *Scheduler) cancelWorkflowTimerWakeup(ref timeridentity.WorkflowTimerActivationRef) error {
	ref = ref.Normalize()
	if ref.ActivationID == "" {
		return errors.New("workflow timer activation identity is required")
	}
	key := workflowTimerTaskFamily + "|" + ref.ActivationID
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, reserved := s.reservations[key]; reserved {
		return errors.New("workflow timer key is reserved by a standing replacement transition")
	}
	task := s.tasks[key]
	if task == nil {
		return nil
	}
	if task.projection.kind != scheduledProjectionWorkflowTimer {
		return errors.New("workflow timer key is owned by a different scheduled projection")
	}
	s.retireTaskLocked(key, task)
	return nil
}

func (s *Scheduler) retireWorkflowTimerWakeups(
	ctx context.Context,
	refs []timeridentity.WorkflowTimerActivationRef,
) error {
	if s == nil {
		return errWorkflowTimerSchedulerRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	keys := make([]string, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		ref = ref.Normalize()
		if !ref.Valid() || ref.ActivationID == "" {
			return errors.New("workflow timer retirement requires exact activation identity")
		}
		key := workflowTimerTaskFamily + "|" + ref.ActivationID
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}

	s.mu.Lock()
	for _, key := range keys {
		if _, reserved := s.reservations[key]; reserved {
			s.mu.Unlock()
			return errors.New("workflow timer key is reserved by a standing replacement transition")
		}
		if task := s.tasks[key]; task != nil && task.projection.kind != scheduledProjectionWorkflowTimer {
			s.mu.Unlock()
			return errors.New("workflow timer key is owned by a different scheduled projection")
		}
	}
	draining := make([]*scheduledTask, 0, len(keys))
	for task := range s.draining {
		if task == nil {
			continue
		}
		if _, selected := seen[task.projection.key()]; !selected {
			continue
		}
		if task.projection.kind != scheduledProjectionWorkflowTimer {
			s.mu.Unlock()
			return errors.New("workflow timer key is draining a different scheduled projection")
		}
		draining = append(draining, task)
	}
	done := make([]<-chan struct{}, 0, len(keys))
	joined := make(map[*scheduledTask]struct{}, len(keys))
	for _, key := range keys {
		task := s.tasks[key]
		if task == nil {
			continue
		}
		s.retireTaskLocked(key, task)
		done = append(done, task.done)
		joined[task] = struct{}{}
	}
	for _, task := range draining {
		if _, alreadyJoined := joined[task]; alreadyJoined {
			continue
		}
		done = append(done, task.done)
	}
	s.mu.Unlock()

	for _, taskDone := range done {
		select {
		case <-taskDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *Scheduler) stopWorkflowTimerWakeups(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	done := make([]<-chan struct{}, 0)
	for key, task := range s.tasks {
		if task.projection.kind != scheduledProjectionWorkflowTimer {
			continue
		}
		s.retireTaskLocked(key, task)
		done = append(done, task.done)
	}
	for task := range s.draining {
		if task.projection.kind == scheduledProjectionWorkflowTimer {
			done = append(done, task.done)
		}
	}
	s.mu.Unlock()
	for _, taskDone := range done {
		select {
		case <-taskDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// ParkOccurrence withdraws every local timer projection owned by one exact
// occurrence. Firing and parking linearize under the scheduler lock: an armed
// projection is restorable, a firing one-shot is consumed, and a firing cron
// becomes restorable only after its callback settles.
func (s *Scheduler) ParkOccurrence(ctx context.Context, owner *worklifetime.StandingOccurrence) (*ParkedOccurrence, error) {
	if s == nil || owner == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	parked := &ParkedOccurrence{scheduler: s, standingOwner: owner}
	s.mu.Lock()
	for transition := range s.transitions {
		if transition.targetsStandingOwner(owner) {
			s.mu.Unlock()
			return nil, errors.New("standing occurrence is reserved by a scheduler replacement transition")
		}
	}
	for key, task := range s.tasks {
		if task == nil || task.standingOwner != owner {
			continue
		}
		projection := parkedProjection{
			task:          task,
			projection:    task.projection.clone(),
			originalOwner: task.owner,
			observedState: task.state,
		}
		projection.restorable = projection.observedState == scheduledTaskArmed ||
			(projection.observedState == scheduledTaskFiring && task.projection.mode() == "cron")
		parked.projections = append(parked.projections, projection)
		s.retireTaskLocked(key, task)
	}
	s.mu.Unlock()
	if err := parked.wait(ctx); err != nil {
		return parked, fmt.Errorf("park occurrence schedules: %w", err)
	}
	return parked, nil
}

// Count returns the number of projections eligible for rollback or fresh
// generation rebinding after predecessor settlement.
func (p *ParkedOccurrence) Count() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, projection := range p.projections {
		if projection.restorable {
			count++
		}
	}
	return count
}

// RestoreOriginal rolls an aborted transition back under every task's exact
// original execution owner. It waits for all predecessor callbacks first and
// never substitutes the standing projection for a composed Manager owner.
func (p *ParkedOccurrence) RestoreOriginal(ctx context.Context) error {
	if p == nil {
		return nil
	}
	prepared, err := prepareParkedSet(ctx, p.scheduler, []parkedSetBinding{{parked: p, restoreOriginal: true}})
	if err != nil {
		return err
	}
	return prepared.Publish()
}

// Rebind projects eligible schedule facts under one distinct fresh standing
// generation. Predecessor Manager ownership is intentionally not retained.
func (p *ParkedOccurrence) Rebind(ctx context.Context, scheduler *Scheduler, owner *worklifetime.StandingOccurrence) error {
	if p == nil {
		return nil
	}
	prepared, err := PrepareParkedSetRebind(ctx, scheduler, []ParkedRebind{{Parked: p, Owner: owner}})
	if err != nil {
		return err
	}
	return prepared.Publish()
}

func (p *ParkedOccurrence) wait(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for _, projection := range p.projections {
		if projection.task == nil {
			continue
		}
		select {
		case <-projection.task.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type parkedSetBinding struct {
	parked          *ParkedOccurrence
	owner           *worklifetime.StandingOccurrence
	restoreOriginal bool
}

// PrepareParkedSetRebind validates and pre-admits the complete successor set,
// reserves every exact target key, and retires and joins every exact target
// incumbent. Source authorities remain pending until CommitDormant succeeds.
func PrepareParkedSetRebind(ctx context.Context, scheduler *Scheduler, bindings []ParkedRebind) (*PreparedParkedSetRebind, error) {
	internal := make([]parkedSetBinding, 0, len(bindings))
	for _, binding := range bindings {
		internal = append(internal, parkedSetBinding{parked: binding.Parked, owner: binding.Owner})
	}
	return prepareParkedSet(ctx, scheduler, internal)
}

func prepareParkedSet(ctx context.Context, scheduler *Scheduler, bindings []parkedSetBinding) (*PreparedParkedSetRebind, error) {
	if scheduler == nil {
		return nil, errors.New("parked schedule set requires a target scheduler")
	}
	if len(bindings) == 0 {
		return nil, errors.New("parked schedule set requires at least one source authority")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	transition := &PreparedParkedSetRebind{scheduler: scheduler, done: make(chan struct{})}
	seenSources := make(map[*ParkedOccurrence]struct{}, len(bindings))
	resetSources := func() {
		for _, source := range transition.sources {
			source.parked.mu.Lock()
			if source.parked.state == parkedOccurrencePrepared {
				source.parked.state = parkedOccurrencePending
			}
			source.parked.mu.Unlock()
		}
	}
	releaseTasks := func() {
		for _, prepared := range transition.tasks {
			_ = prepared.task.lease.Done()
		}
	}
	fail := func(err error) (*PreparedParkedSetRebind, error) {
		releaseTasks()
		resetSources()
		return nil, err
	}

	for _, binding := range bindings {
		parked := binding.parked
		if parked == nil {
			return fail(errors.New("parked schedule set contains a nil source authority"))
		}
		if _, duplicate := seenSources[parked]; duplicate {
			return fail(errors.New("parked schedule set contains a duplicate source authority"))
		}
		seenSources[parked] = struct{}{}
		parked.mu.Lock()
		if parked.state != parkedOccurrencePending {
			parked.mu.Unlock()
			return fail(errors.New("parked occurrence projection is already settled or reserved"))
		}
		if !binding.restoreOriginal && (binding.owner == nil || binding.owner == parked.standingOwner) {
			parked.mu.Unlock()
			return fail(errors.New("rebind parked schedules requires a distinct fresh standing owner"))
		}
		parked.state = parkedOccurrencePrepared
		resultState := parkedOccurrenceRebound
		if binding.restoreOriginal {
			resultState = parkedOccurrenceRestored
		}
		transition.sources = append(transition.sources, preparedParkedSource{parked: parked, state: resultState})
		parked.mu.Unlock()
		if err := parked.wait(ctx); err != nil {
			return fail(fmt.Errorf("join parked predecessor schedules: %w", err))
		}

		parked.mu.Lock()
		projections := append([]parkedProjection(nil), parked.projections...)
		parked.mu.Unlock()
		for _, projection := range projections {
			if !projection.restorable {
				continue
			}
			executionOwner := worklifetime.Occurrence(binding.owner)
			if binding.restoreOriginal {
				executionOwner = projection.originalOwner
				standing, ok := worklifetime.StandingProjection(executionOwner)
				if !ok || standing != parked.standingOwner {
					return fail(errors.New("parked schedule original owner no longer projects the exact standing occurrence"))
				}
			}
			if executionOwner == nil {
				return fail(errors.New("parked schedule set requires an exact execution owner"))
			}
			normalized, spec, err := validateScheduledProjection(projection.projection.clone())
			if err != nil {
				return fail(fmt.Errorf("validate parked scheduled projection: %w", err))
			}
			lease, err := executionOwner.Begin(context.WithoutCancel(ctx))
			if err != nil {
				return fail(fmt.Errorf("admit parked schedule successor: %w", err))
			}
			standingOwner, _ := worklifetime.StandingProjection(executionOwner)
			task := &scheduledTask{
				stop: make(chan struct{}), done: make(chan struct{}), lease: lease,
				owner: executionOwner, standingOwner: standingOwner, projection: normalized,
			}
			transition.tasks = append(transition.tasks, preparedParkedTask{key: normalized.key(), task: task, spec: spec})
		}
	}

	seenKeys := make(map[string]struct{}, len(transition.tasks))
	for _, prepared := range transition.tasks {
		if _, duplicate := seenKeys[prepared.key]; duplicate {
			return fail(fmt.Errorf("parked schedule set contains duplicate target key %q", prepared.key))
		}
		seenKeys[prepared.key] = struct{}{}
		transition.keys = append(transition.keys, prepared.key)
	}

	scheduler.mu.Lock()
	if scheduler.stopped {
		scheduler.mu.Unlock()
		return fail(errors.New("target scheduler stopped"))
	}
	for _, prepared := range transition.tasks {
		if prepared.task.projection.kind == scheduledProjectionWorkflowTimer && scheduler.onWorkflowTimer == nil {
			scheduler.mu.Unlock()
			return fail(errors.New("target scheduler has no workflow timer lifecycle binding"))
		}
	}
	for _, key := range transition.keys {
		if _, reserved := scheduler.reservations[key]; reserved {
			scheduler.mu.Unlock()
			return fail(fmt.Errorf("target schedule key %q is already reserved", key))
		}
	}
	incumbents := make([]*scheduledTask, 0, len(transition.keys))
	scheduler.transitions[transition] = struct{}{}
	for _, key := range transition.keys {
		scheduler.reservations[key] = transition
		if incumbent := scheduler.tasks[key]; incumbent != nil {
			incumbents = append(incumbents, incumbent)
			scheduler.retireTaskLocked(key, incumbent)
		}
	}
	scheduler.mu.Unlock()

	for _, incumbent := range incumbents {
		select {
		case <-incumbent.done:
		case <-ctx.Done():
			_ = transition.Abort()
			return nil, fmt.Errorf("join target schedule incumbent: %w", ctx.Err())
		}
	}
	return transition, nil
}

// CommitDormant installs the complete rebound set while reservations still
// reject target-key mutation. No callback is started by this operation.
func (t *PreparedParkedSetRebind) CommitDormant() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != preparedParkedSetPending {
		return errors.New("parked schedule set is not pending")
	}
	s := t.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errors.New("target scheduler stopped before parked schedule commit")
	}
	for _, prepared := range t.tasks {
		if s.reservations[prepared.key] != t {
			return fmt.Errorf("target schedule key %q lost its exact reservation", prepared.key)
		}
		if s.tasks[prepared.key] != nil {
			return fmt.Errorf("target schedule key %q gained an unreserved incumbent", prepared.key)
		}
	}
	for _, prepared := range t.tasks {
		s.tasks[prepared.key] = prepared.task
	}
	t.state = preparedParkedSetCommitted
	return nil
}

// Activate settles every source authority, releases every target reservation,
// and starts the complete committed task set at one scheduler linearization.
func (t *PreparedParkedSetRebind) Activate() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if t.state != preparedParkedSetCommitted {
		t.mu.Unlock()
		return errors.New("parked schedule set is not committed")
	}
	s := t.scheduler
	s.mu.Lock()
	for _, prepared := range t.tasks {
		if s.reservations[prepared.key] != t || s.tasks[prepared.key] != prepared.task {
			s.mu.Unlock()
			t.mu.Unlock()
			return fmt.Errorf("target schedule key %q changed before activation", prepared.key)
		}
	}
	for _, source := range t.sources {
		source.parked.mu.Lock()
		if source.parked.state != parkedOccurrencePrepared {
			source.parked.mu.Unlock()
			s.mu.Unlock()
			t.mu.Unlock()
			return errors.New("parked source authority changed before activation")
		}
		source.parked.state = source.state
		source.parked.mu.Unlock()
	}
	for _, key := range t.keys {
		delete(s.reservations, key)
	}
	delete(s.transitions, t)
	t.state = preparedParkedSetActive
	close(t.done)
	s.mu.Unlock()
	t.mu.Unlock()
	for _, prepared := range t.tasks {
		s.startTask(prepared.key, prepared.task, prepared.spec)
	}
	return nil
}

// Abort releases successor leases and reservations without settling any
// source authority. Retired target incumbents remain retired; callers must
// discard that unavailable candidate and retry with a fresh candidate.
func (t *PreparedParkedSetRebind) Abort() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == preparedParkedSetAborted {
		return nil
	}
	if t.state != preparedParkedSetPending {
		return errors.New("committed parked schedule set cannot be aborted")
	}
	s := t.scheduler
	s.mu.Lock()
	for _, key := range t.keys {
		if s.reservations[key] == t {
			delete(s.reservations, key)
		}
	}
	delete(s.transitions, t)
	t.state = preparedParkedSetAborted
	close(t.done)
	s.mu.Unlock()
	for _, prepared := range t.tasks {
		_ = prepared.task.lease.Done()
	}
	for _, source := range t.sources {
		source.parked.mu.Lock()
		if source.parked.state == parkedOccurrencePrepared {
			source.parked.state = parkedOccurrencePending
		}
		source.parked.mu.Unlock()
	}
	return nil
}

// Publish is the one-step form used when no surrounding runtime publication
// needs the dormant commit boundary.
func (t *PreparedParkedSetRebind) Publish() error {
	if err := t.CommitDormant(); err != nil {
		_ = t.Abort()
		return err
	}
	return t.Activate()
}

func (t *PreparedParkedSetRebind) targetsStandingOwner(owner *worklifetime.StandingOccurrence) bool {
	if t == nil || owner == nil {
		return false
	}
	for _, prepared := range t.tasks {
		if prepared.task.standingOwner == owner {
			return true
		}
	}
	return false
}

func (s *Scheduler) Stop() {
	for {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return
		}
		if len(s.transitions) > 0 {
			pending := make([]<-chan struct{}, 0, len(s.transitions))
			for transition := range s.transitions {
				pending = append(pending, transition.done)
			}
			s.mu.Unlock()
			for _, done := range pending {
				<-done
			}
			continue
		}
		s.stopped = true
		for key, task := range s.tasks {
			s.retireTaskLocked(key, task)
		}
		s.mu.Unlock()
		return
	}
}

func (s *Scheduler) startTask(key string, task *scheduledTask, spec cronSpec) {
	switch task.projection.mode() {
	case "once":
		go func() {
			defer s.finishTask(key, task)
			if !task.waitForPrior() {
				return
			}
			s.runOnce(task, key, task.projection)
		}()
	case "cron":
		go func() {
			defer s.finishTask(key, task)
			if !task.waitForPrior() {
				return
			}
			s.runCron(task, key, task.projection, spec)
		}()
	default:
		panic("validated schedule mode became unreachable")
	}
}

func (task *scheduledTask) waitForPrior() bool {
	if task == nil || task.prior == nil {
		return true
	}
	select {
	case <-task.prior:
		return true
	case <-task.stop:
		return false
	case <-task.lease.Context().Done():
		return false
	}
}

func (s *Scheduler) runOnce(task *scheduledTask, key string, projection scheduledProjection) {
	delay := time.Until(projection.dueAt())
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-task.stop:
		return
	case <-task.lease.Context().Done():
		return
	case <-timer.C:
		if !s.beginTaskFire(key, task) {
			return
		}
		projection.fire(worklifetime.WithOccurrence(task.lease.Context(), task.owner), s.onFire, s.onWorkflowTimer)
		s.endTaskFire(task, false)
	}
}

func (s *Scheduler) runCron(task *scheduledTask, key string, projection scheduledProjection, spec cronSpec) {
	if spec.every > 0 {
		ticker := time.NewTicker(spec.every)
		defer ticker.Stop()
		for {
			select {
			case <-task.stop:
				return
			case <-task.lease.Context().Done():
				return
			case <-ticker.C:
				if !s.beginTaskFire(key, task) {
					return
				}
				projection.fire(worklifetime.WithOccurrence(task.lease.Context(), task.owner), s.onFire, s.onWorkflowTimer)
				if !s.endTaskFire(task, true) {
					return
				}
			}
		}
	}
	for {
		next := spec.schedule.Next(time.Now())
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-task.stop:
			timer.Stop()
			return
		case <-task.lease.Context().Done():
			timer.Stop()
			return
		case <-timer.C:
			if !s.beginTaskFire(key, task) {
				return
			}
			projection.fire(worklifetime.WithOccurrence(task.lease.Context(), task.owner), s.onFire, s.onWorkflowTimer)
			if !s.endTaskFire(task, true) {
				return
			}
		}
	}
}

func (s *Scheduler) beginTaskFire(key string, task *scheduledTask) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task == nil || s.tasks[key] != task || task.retiring || task.state != scheduledTaskArmed {
		return false
	}
	task.state = scheduledTaskFiring
	return true
}

func (s *Scheduler) endTaskFire(task *scheduledTask, recurring bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task == nil || task.state != scheduledTaskFiring {
		return false
	}
	if task.retiring || !recurring {
		task.state = scheduledTaskSettled
		return false
	}
	task.state = scheduledTaskArmed
	return true
}

func (s *Scheduler) retireTaskLocked(key string, task *scheduledTask) {
	if task == nil {
		return
	}
	if current := s.tasks[key]; current == task {
		delete(s.tasks, key)
	}
	if s.draining == nil {
		s.draining = make(map[*scheduledTask]struct{})
	}
	s.draining[task] = struct{}{}
	task.retiring = true
	select {
	case <-task.stop:
	default:
		close(task.stop)
	}
}

func (s *Scheduler) finishTask(key string, task *scheduledTask) {
	_ = task.lease.Done()
	s.mu.Lock()
	task.state = scheduledTaskSettled
	if current := s.tasks[key]; current == task {
		delete(s.tasks, key)
	}
	delete(s.draining, task)
	close(task.done)
	s.mu.Unlock()
}

func scheduleKey(sc Schedule) string {
	fields := agentidentity.StorageFields{}
	if !sc.AgentIdentity.IsZero() {
		fields, _ = sc.AgentIdentity.StorageFields()
	}
	return strings.Join([]string{
		strings.TrimSpace(sc.EffectiveRunID()),
		strings.TrimSpace(sc.AgentID),
		strings.TrimSpace(string(sc.OwnerKind)),
		fields.NameOwner,
		fields.NameSource,
		fields.RoutePresence,
		fields.FlowScopeKey,
		fields.FlowInstanceID,
		fields.FlowInstancePath,
		strings.TrimSpace(sc.EventType),
		strings.TrimSpace(sc.EffectiveEntityID()),
		strings.TrimSpace(sc.EffectiveFlowInstance()),
		strings.TrimSpace(sc.TaskID),
	}, "|")
}

func cloneSchedule(sc Schedule) Schedule {
	sc.Payload = append([]byte(nil), sc.Payload...)
	return sc
}

func validateSchedule(sc Schedule) (Schedule, cronSpec, error) {
	if sc.AgentID == "" || sc.EventType == "" {
		return Schedule{}, cronSpec{}, errors.New("agent_id and event_type are required")
	}
	sc.NormalizeRunID()
	sc.NormalizeDeliveryContext()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := sc.NormalizeOwner(); err != nil {
		return Schedule{}, cronSpec{}, err
	}
	var err error
	sc, err = sc.AdmitEventIdentity()
	if err != nil {
		return Schedule{}, cronSpec{}, err
	}
	if sc.Mode == "" {
		sc.Mode = "once"
	}
	var spec cronSpec
	switch sc.Mode {
	case "once":
		if sc.At.IsZero() {
			return Schedule{}, cronSpec{}, errors.New("schedule.at is required for mode=once")
		}
	case "cron":
		var err error
		spec, err = parseCronSpec(sc.Cron)
		if err != nil {
			return Schedule{}, cronSpec{}, err
		}
	default:
		return Schedule{}, cronSpec{}, fmt.Errorf("unsupported schedule mode: %s", sc.Mode)
	}
	return sc, spec, nil
}

func (sc Schedule) ValidateRoutingSource() error {
	switch sc.RoutingSource.Kind() {
	case events.RoutingSourceRoot:
		route := sc.RoutingSource.Route()
		if route.EntityID != sc.EntityID || sc.FlowInstance != "" {
			return errors.New("root-owned schedule routing source does not match its persisted entity scope")
		}
		if sc.OwnerKind == ScheduleOwnerSystem {
			return sc.validateSystemJoinDeclaration()
		}
	case events.RoutingSourceFlowOwnedControl:
		route := sc.RoutingSource.Route()
		if route.FlowInstance != sc.FlowInstance || route.EntityID != sc.EntityID {
			return errors.New("flow-owned schedule routing source does not match its persisted scope")
		}
		if sc.OwnerKind == ScheduleOwnerAgent {
			flowScopeKey, _, _, present := sc.AgentIdentity.Route.Fields()
			if !present || route.FlowID != flowScopeKey {
				return errors.New("flow-owned schedule routing source does not match its concrete agent flow scope")
			}
		} else if sc.OwnerKind == ScheduleOwnerSystem {
			return sc.validateSystemJoinDeclaration()
		}
	case events.RoutingSourcePlatformControl:
		if sc.OwnerKind != ScheduleOwnerSystem || sc.EntityID != "" || sc.FlowInstance != "" {
			return errors.New("platform-control schedule must be a closed system schedule without flow or entity scope")
		}
	default:
		return errors.New("schedule requires an exact flow-owned or platform-control routing source")
	}
	return nil
}

func (sc Schedule) validateSystemJoinDeclaration() error {
	var payload map[string]any
	if err := json.Unmarshal(sc.Payload, &payload); err != nil || payload == nil {
		return errors.New("flow- or root-owned system schedule requires a join timer handle")
	}
	ref, kind, ok := timeridentity.ParseJoinRef(payload)
	if !ok {
		return errors.New("flow- or root-owned system schedule requires a valid join timer handle")
	}
	handle := timeridentity.JoinTimeoutHandle(ref)
	expectedEvent := joinTimeoutEvent
	if kind == timeridentity.TimerHandleJoinComplete {
		handle = timeridentity.JoinCompleteHandle(ref)
		expectedEvent = joinCompleteEvent
	}
	if strings.TrimSpace(sc.TaskID) != handle.TaskID() {
		return errors.New("system join schedule task identity does not match its timer handle")
	}
	if strings.TrimSpace(sc.AgentID) != runtimeWorkflowID {
		return errors.New("system join schedule owner does not match the workflow runtime")
	}
	if strings.TrimSpace(sc.EventType) != expectedEvent {
		return errors.New("system join schedule event does not match its timer handle")
	}
	if sc.RoutingSource.Route().FlowID != ref.FlowID {
		return errors.New("system join schedule source flow does not match its timer handle")
	}
	return nil
}

func (sc Schedule) AdmitEventIdentity() (Schedule, error) {
	if err := sc.ValidateRoutingSource(); err != nil {
		return Schedule{}, err
	}
	admitted, err := events.AdmitRuntimeControlEventType(events.EventType(sc.EventType), sc.RoutingSource)
	if err != nil {
		return Schedule{}, err
	}
	sc.EventType = string(admitted)
	return sc, nil
}

func validateScheduledProjection(projection scheduledProjection) (scheduledProjection, cronSpec, error) {
	switch projection.kind {
	case scheduledProjectionGeneric:
		normalized, spec, err := newGenericScheduledProjection(projection.generic)
		if err != nil {
			return scheduledProjection{}, cronSpec{}, err
		}
		return normalized, spec, nil
	case scheduledProjectionWorkflowTimer:
		return newWorkflowTimerScheduledProjection(projection.workflow)
	default:
		return scheduledProjection{}, cronSpec{}, errors.New("scheduled projection kind is invalid")
	}
}

func parseCronSpec(expr string) (cronSpec, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return cronSpec{}, errors.New("schedule.cron is required for mode=cron")
	}
	if strings.HasPrefix(expr, "@every ") {
		d, err := time.ParseDuration(strings.TrimSpace(strings.TrimPrefix(expr, "@every ")))
		if err != nil {
			return cronSpec{}, fmt.Errorf("invalid @every duration: %w", err)
		}
		if d <= 0 {
			return cronSpec{}, errors.New("interval must be > 0")
		}
		return cronSpec{every: d}, nil
	}
	parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s, err := parser.Parse(expr)
	if err == nil {
		return cronSpec{schedule: s}, nil
	}
	return cronSpec{}, fmt.Errorf("invalid cron expression %q", expr)
}
