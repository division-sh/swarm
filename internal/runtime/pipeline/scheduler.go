package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

type scheduledProjectionKind uint8

const (
	scheduledProjectionUnknown scheduledProjectionKind = iota
	scheduledProjectionWorkflowTimer
	scheduledProjectionGenericActivation
)

type scheduledProjection struct {
	kind       scheduledProjectionKind
	workflow   WorkflowTimerWakeup
	activation runtimegenericschedule.Wakeup
}

func newWorkflowTimerScheduledProjection(wakeup WorkflowTimerWakeup) (scheduledProjection, error) {
	if err := wakeup.validate(); err != nil {
		return scheduledProjection{}, err
	}
	return scheduledProjection{
		kind:     scheduledProjectionWorkflowTimer,
		workflow: wakeup,
	}, nil
}

func newGenericActivationScheduledProjection(wakeup runtimegenericschedule.Wakeup) (scheduledProjection, error) {
	if err := wakeup.Validate(); err != nil {
		return scheduledProjection{}, err
	}
	return scheduledProjection{kind: scheduledProjectionGenericActivation, activation: wakeup}, nil
}

func (p scheduledProjection) key() string {
	switch p.kind {
	case scheduledProjectionWorkflowTimer:
		return workflowTimerTaskFamily + "|" + p.workflow.Occurrence().Activation.ActivationID
	case scheduledProjectionGenericActivation:
		return "generic_schedule|" + p.activation.ActivationID()
	default:
		return ""
	}
}

func (p scheduledProjection) dueAt() time.Time {
	if p.kind == scheduledProjectionWorkflowTimer {
		return p.workflow.DueAt()
	}
	if p.kind == scheduledProjectionGenericActivation {
		return p.activation.DueAt()
	}
	return time.Time{}
}

func (p scheduledProjection) clone() scheduledProjection {
	return p
}

func (p scheduledProjection) fire(
	ctx context.Context,
	workflowTimerFire func(context.Context, WorkflowTimerWakeup),
	genericActivationFire func(context.Context, runtimegenericschedule.Wakeup),
) {
	switch p.kind {
	case scheduledProjectionWorkflowTimer:
		if workflowTimerFire == nil {
			panic("workflow timer scheduler binding became unavailable")
		}
		workflowTimerFire(ctx, p.workflow)
	case scheduledProjectionGenericActivation:
		if genericActivationFire == nil {
			panic("generic schedule lifecycle binding became unavailable")
		}
		genericActivationFire(ctx, p.activation)
	}
}

type Scheduler struct {
	mu                  sync.Mutex
	onWorkflowTimer     func(context.Context, WorkflowTimerWakeup)
	onGenericActivation func(context.Context, runtimegenericschedule.Wakeup)
	tasks               map[string]*scheduledTask
	draining            map[*scheduledTask]struct{}
	stopped             bool
	owner               worklifetime.Occurrence
	startupRelease      chan struct{}
}

type scheduledTask struct {
	stop       chan struct{}
	done       chan struct{}
	prior      <-chan struct{}
	lease      *worklifetime.Lease
	owner      worklifetime.Occurrence
	projection scheduledProjection
	state      scheduledTaskState
	retiring   bool
}

type scheduledTaskState uint8

const (
	scheduledTaskArmed scheduledTaskState = iota
	scheduledTaskFiring
	scheduledTaskSettled
)

func NewSchedulerWithWorkOwner(owner worklifetime.Occurrence) *Scheduler {
	return &Scheduler{
		tasks:    make(map[string]*scheduledTask),
		draining: make(map[*scheduledTask]struct{}),
		owner:    owner,
	}
}

// PrepareStartup keeps exact task registration available to topology completion
// while withholding callbacks until recovery hands off to delivery continuations.
func (s *Scheduler) PrepareStartup() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || s.startupRelease != nil || len(s.tasks) != 0 || len(s.draining) != 0 {
		return errors.New("scheduler startup requires an unused live scheduler")
	}
	s.startupRelease = make(chan struct{})
	return nil
}

func (s *Scheduler) ReleaseStartup(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.stopped || s.startupRelease == nil {
		return errors.New("scheduler startup is not prepared or is stopped")
	}
	select {
	case <-s.startupRelease:
		return errors.New("scheduler startup already released")
	default:
		close(s.startupRelease)
		return nil
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

func (s *Scheduler) BindGenericScheduleLifecycle(callback func(context.Context, runtimegenericschedule.Wakeup)) error {
	if s == nil {
		return errors.New("generic schedule lifecycle requires a scheduler")
	}
	if callback == nil {
		return errors.New("generic schedule lifecycle callback is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errors.New("cannot bind generic schedule lifecycle to a stopped scheduler")
	}
	if s.onGenericActivation != nil {
		return errors.New("generic schedule lifecycle is already bound")
	}
	s.onGenericActivation = callback
	return nil
}

func (s *Scheduler) registerWorkflowTimerWakeup(ctx context.Context, wakeup WorkflowTimerWakeup) error {
	projection, err := newWorkflowTimerScheduledProjection(wakeup)
	if err != nil {
		return err
	}
	return s.registerProjection(ctx, projection)
}

func (s *Scheduler) RegisterGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) error {
	projection, err := newGenericActivationScheduledProjection(wakeup)
	if err != nil {
		return err
	}
	return s.registerProjection(ctx, projection)
}

func (s *Scheduler) registerProjection(ctx context.Context, projection scheduledProjection) error {
	if ctx == nil {
		ctx = context.Background()
	}

	key := projection.key()
	var task *scheduledTask
	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.stopped {
			return errors.New("scheduler stopped")
		}
		if s.owner == nil {
			return errors.New("scheduler requires a runtime work occurrence")
		}
		if projection.kind == scheduledProjectionWorkflowTimer && s.onWorkflowTimer == nil {
			return errors.New("workflow timer lifecycle is not bound to scheduler")
		}
		if projection.kind == scheduledProjectionGenericActivation && s.onGenericActivation == nil {
			return errors.New("generic schedule lifecycle is not bound to scheduler")
		}
		if key == "" {
			return errors.New("scheduled projection identity is required")
		}
		owner := s.owner
		if contextual, ok := worklifetime.OccurrenceFromContext(ctx); ok {
			owner = contextual
		}
		lease, err := owner.Begin(context.WithoutCancel(ctx))
		if err != nil {
			return fmt.Errorf("admit scheduled task: %w", err)
		}
		var prior <-chan struct{}
		if existing, ok := s.tasks[key]; ok {
			prior = existing.done
			s.retireTaskLocked(key, existing)
		}
		task = &scheduledTask{
			stop: make(chan struct{}), done: make(chan struct{}), prior: prior, lease: lease,
			owner: owner, projection: projection.clone(),
		}
		s.tasks[key] = task
		return nil
	}()
	if err != nil {
		return err
	}
	s.startTask(key, task)
	return nil
}

func (s *Scheduler) Wait(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	done := make([]<-chan struct{}, 0, len(s.draining)+len(s.tasks))
	for task := range s.draining {
		done = append(done, task.done)
	}
	for _, task := range s.tasks {
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

func (s *Scheduler) cancelWorkflowTimerWakeup(ref timeridentity.WorkflowTimerActivationRef) error {
	ref = ref.Normalize()
	if ref.ActivationID == "" {
		return errors.New("workflow timer activation identity is required")
	}
	key := workflowTimerTaskFamily + "|" + ref.ActivationID
	s.mu.Lock()
	defer s.mu.Unlock()
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

func (s *Scheduler) RetireGenericScheduleWakeup(wakeup runtimegenericschedule.Wakeup) error {
	if err := wakeup.Validate(); err != nil {
		return err
	}
	key := "generic_schedule|" + wakeup.ActivationID()
	s.mu.Lock()
	defer s.mu.Unlock()
	task := s.tasks[key]
	if task == nil {
		return nil
	}
	if task.projection.kind != scheduledProjectionGenericActivation {
		return errors.New("generic schedule key is owned by a different scheduled projection")
	}
	s.retireTaskLocked(key, task)
	return nil
}

func (s *Scheduler) StopGenericScheduleWakeups(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	done := make([]<-chan struct{}, 0)
	for key, task := range s.tasks {
		if task.projection.kind != scheduledProjectionGenericActivation {
			continue
		}
		s.retireTaskLocked(key, task)
		done = append(done, task.done)
	}
	for task := range s.draining {
		if task.projection.kind == scheduledProjectionGenericActivation {
			done = append(done, task.done)
		}
	}
	s.mu.Unlock()
	for _, taskDone := range done {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-taskDone:
		}
	}
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

func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	for key, task := range s.tasks {
		s.retireTaskLocked(key, task)
	}
}

func (s *Scheduler) startTask(key string, task *scheduledTask) {
	go func() {
		defer s.finishTask(key, task)
		if !task.waitForPrior() {
			return
		}
		s.runOnce(task, key, task.projection)
	}()
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
	s.mu.Lock()
	startupRelease := s.startupRelease
	s.mu.Unlock()
	if startupRelease != nil {
		select {
		case <-startupRelease:
		case <-task.stop:
			return
		case <-task.lease.Context().Done():
			return
		}
	}
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
		projection.fire(worklifetime.WithOccurrence(task.lease.Context(), task.owner), s.onWorkflowTimer, s.onGenericActivation)
		s.endTaskFire(task, false)
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

func validateScheduledProjection(projection scheduledProjection) (scheduledProjection, error) {
	switch projection.kind {
	case scheduledProjectionWorkflowTimer:
		return newWorkflowTimerScheduledProjection(projection.workflow)
	case scheduledProjectionGenericActivation:
		return newGenericActivationScheduledProjection(projection.activation)
	default:
		return scheduledProjection{}, errors.New("scheduled projection kind is invalid")
	}
}
