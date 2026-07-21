package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/robfig/cron/v3"
)

type Schedule struct {
	Context      events.DeliveryContext
	RunID        string
	AgentID      string
	EventType    string
	Mode         string // once | cron
	Cron         string // supports "@every <duration>" and plain duration string
	At           time.Time
	EntityID     string
	FlowInstance string
	TaskID       string
	// TimerID is populated only for canonical workflow activations. Generic
	// schedule persistence must never manufacture or reinterpret it.
	TimerID string
	Payload []byte
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

func (s Schedule) EffectiveTimerID() string {
	return strings.TrimSpace(s.TimerID)
}

func (s *Schedule) NormalizeTimerID() {
	if s == nil {
		return
	}
	s.TimerID = s.EffectiveTimerID()
}

func (s Schedule) CanonicalWorkflowTimer() bool {
	ref, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(s.TaskID)
	return ok && s.EffectiveTimerID() != "" && ref.Activation.ActivationID == s.EffectiveTimerID()
}

type Scheduler struct {
	mu       sync.Mutex
	onFire   func(context.Context, Schedule)
	tasks    map[string]*scheduledTask
	draining map[*scheduledTask]struct{}
	stopped  bool
	owner    worklifetime.Occurrence
}

type scheduledTask struct {
	stop          chan struct{}
	done          chan struct{}
	lease         *worklifetime.Lease
	owner         worklifetime.Occurrence
	standingOwner *worklifetime.StandingOccurrence
	schedule      Schedule
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
	schedule      Schedule
	originalOwner worklifetime.Occurrence
	observedState scheduledTaskState
	restorable    bool
}

type parkedOccurrenceState uint8

const (
	parkedOccurrencePending parkedOccurrenceState = iota
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
		onFire:   cb,
		tasks:    make(map[string]*scheduledTask),
		draining: make(map[*scheduledTask]struct{}),
		owner:    owner,
	}
}

func (s *Scheduler) Register(ctx context.Context, sc Schedule) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if sc.AgentID == "" || sc.EventType == "" {
		return errors.New("agent_id and event_type are required")
	}
	sc.NormalizeRunID()
	sc.NormalizeDeliveryContext()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	sc.NormalizeTimerID()
	if sc.Mode == "" {
		sc.Mode = "once"
	}
	var spec cronSpec
	switch sc.Mode {
	case "once":
		if sc.At.IsZero() {
			return errors.New("schedule.at is required for mode=once")
		}
	case "cron":
		var err error
		spec, err = parseCronSpec(sc.Cron)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported schedule mode: %s", sc.Mode)
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
	owner := s.owner
	if contextual, ok := worklifetime.OccurrenceFromContext(ctx); ok {
		owner = contextual
	}
	lease, err := owner.Begin(ownerActionAdmissionContext(ctx))
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("admit scheduled task: %w", err)
	}
	key := scheduleKey(sc)
	if existing, ok := s.tasks[key]; ok {
		s.retireTaskLocked(key, existing)
	}
	standingOwner, _ := worklifetime.StandingProjection(owner)
	task := &scheduledTask{
		stop: make(chan struct{}), done: make(chan struct{}), lease: lease,
		owner: owner, standingOwner: standingOwner, schedule: cloneSchedule(sc),
	}
	s.tasks[key] = task
	s.mu.Unlock()

	switch sc.Mode {
	case "once":
		go func() {
			defer s.finishTask(key, task)
			s.runOnce(task, key, sc)
		}()
		return nil
	case "cron":
		go func() {
			defer s.finishTask(key, task)
			s.runCron(task, key, sc, spec)
		}()
		return nil
	}
	panic("validated schedule mode became unreachable")
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

func (s *Scheduler) Cancel(agentID string, eventType string) error {
	if agentID == "" || eventType == "" {
		return errors.New("agent_id and event_type are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, task := range s.tasks {
		if !scheduleKeyMatchesAgentEvent(key, agentID, eventType) {
			continue
		}
		s.retireTaskLocked(key, task)
	}
	return nil
}

func (s *Scheduler) CancelExact(sc Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return errors.New("agent_id and event_type are required")
	}
	key := scheduleKey(sc)
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[key]
	if !ok {
		return nil
	}
	s.retireTaskLocked(key, task)
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
	for key, task := range s.tasks {
		if task == nil || task.standingOwner != owner {
			continue
		}
		projection := parkedProjection{
			task:          task,
			schedule:      cloneSchedule(task.schedule),
			originalOwner: task.owner,
			observedState: task.state,
		}
		projection.restorable = projection.observedState == scheduledTaskArmed ||
			(projection.observedState == scheduledTaskFiring && task.schedule.Mode == "cron")
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != parkedOccurrencePending {
		return errors.New("parked occurrence projection is already settled")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.wait(ctx); err != nil {
		return fmt.Errorf("join parked predecessor schedules: %w", err)
	}
	for _, projection := range p.projections {
		if !projection.restorable {
			continue
		}
		standing, ok := worklifetime.StandingProjection(projection.originalOwner)
		if !ok || standing != p.standingOwner {
			return errors.New("parked schedule original owner no longer projects the exact standing occurrence")
		}
	}
	if err := p.restore(ctx, p.scheduler, nil); err != nil {
		return err
	}
	p.state = parkedOccurrenceRestored
	return nil
}

// Rebind projects eligible schedule facts under one distinct fresh standing
// generation. Predecessor Manager ownership is intentionally not retained.
func (p *ParkedOccurrence) Rebind(ctx context.Context, scheduler *Scheduler, owner *worklifetime.StandingOccurrence) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != parkedOccurrencePending {
		return errors.New("parked occurrence projection is already settled")
	}
	if scheduler == nil || owner == nil {
		return errors.New("rebind parked schedules requires scheduler and fresh standing owner")
	}
	if owner == p.standingOwner {
		return errors.New("rebind parked schedules requires a distinct fresh standing owner")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.wait(ctx); err != nil {
		return fmt.Errorf("join parked predecessor schedules: %w", err)
	}
	if err := p.restore(ctx, scheduler, owner); err != nil {
		return err
	}
	p.state = parkedOccurrenceRebound
	return nil
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

func (p *ParkedOccurrence) restore(ctx context.Context, scheduler *Scheduler, freshOwner *worklifetime.StandingOccurrence) error {
	if scheduler == nil {
		return errors.New("restore parked schedules requires scheduler")
	}
	restored := make([]Schedule, 0, len(p.projections))
	for _, projection := range p.projections {
		if !projection.restorable {
			continue
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, scheduler.cancelAndWait(context.Background(), restored))
		}
		executionOwner := projection.originalOwner
		if freshOwner != nil {
			executionOwner = freshOwner
		}
		if executionOwner == nil {
			return errors.Join(errors.New("restore parked schedule requires exact execution owner"), scheduler.cancelAndWait(context.Background(), restored))
		}
		registerCtx := worklifetime.WithOccurrence(ctx, executionOwner)
		if err := scheduler.Register(registerCtx, cloneSchedule(projection.schedule)); err != nil {
			return errors.Join(fmt.Errorf("restore parked schedule: %w", err), scheduler.cancelAndWait(context.Background(), restored))
		}
		restored = append(restored, projection.schedule)
	}
	return nil
}

func (s *Scheduler) cancelAndWait(ctx context.Context, schedules []Schedule) error {
	if s == nil || len(schedules) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	tasks := make([]*scheduledTask, 0, len(schedules))
	for _, schedule := range schedules {
		key := scheduleKey(schedule)
		task := s.tasks[key]
		if task == nil {
			continue
		}
		tasks = append(tasks, task)
		s.retireTaskLocked(key, task)
	}
	s.mu.Unlock()
	for _, task := range tasks {
		select {
		case <-task.done:
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

func (s *Scheduler) runOnce(task *scheduledTask, key string, sc Schedule) {
	delay := time.Until(sc.At)
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
		s.fire(worklifetime.WithOccurrence(task.lease.Context(), task.owner), sc)
		s.endTaskFire(task, false)
	}
}

func (s *Scheduler) runCron(task *scheduledTask, key string, sc Schedule, spec cronSpec) {
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
				s.fire(worklifetime.WithOccurrence(task.lease.Context(), task.owner), sc)
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
			s.fire(worklifetime.WithOccurrence(task.lease.Context(), task.owner), sc)
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

func (s *Scheduler) fire(ctx context.Context, sc Schedule) {
	if s.onFire != nil {
		s.onFire(ctx, sc)
	}
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
	return strings.Join([]string{
		strings.TrimSpace(sc.EffectiveRunID()),
		strings.TrimSpace(sc.AgentID),
		strings.TrimSpace(sc.EventType),
		strings.TrimSpace(sc.EffectiveEntityID()),
		strings.TrimSpace(sc.EffectiveFlowInstance()),
		strings.TrimSpace(sc.TaskID),
		strings.TrimSpace(sc.EffectiveTimerID()),
	}, "|")
}

func cloneSchedule(sc Schedule) Schedule {
	sc.Payload = append([]byte(nil), sc.Payload...)
	return sc
}

func scheduleKeyMatchesAgentEvent(key, agentID, eventType string) bool {
	parts := strings.Split(key, "|")
	return len(parts) >= 3 &&
		strings.TrimSpace(parts[1]) == strings.TrimSpace(agentID) &&
		strings.TrimSpace(parts[2]) == strings.TrimSpace(eventType)
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
