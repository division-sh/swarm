package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type Schedule struct {
	AgentID    string
	EventType  string
	Mode       string // once | cron
	Cron       string // supports "@every <duration>" and plain duration string
	At         time.Time
	EntityID   string
	TaskID     string
	Payload    []byte
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

type Scheduler struct {
	mu      sync.Mutex
	onFire  func(Schedule)
	tasks   map[string]*scheduledTask
	stopped bool
}

type scheduledTask struct {
	stop chan struct{}
}

type cronSpec struct {
	every    time.Duration
	schedule cron.Schedule
}

func NewScheduler(callbacks ...func(Schedule)) *Scheduler {
	var cb func(Schedule)
	if len(callbacks) > 0 {
		cb = callbacks[0]
	}
	return &Scheduler{
		onFire: cb,
		tasks:  make(map[string]*scheduledTask),
	}
}

func (s *Scheduler) Register(sc Schedule) error {
	if sc.AgentID == "" || sc.EventType == "" {
		return errors.New("agent_id and event_type are required")
	}
	sc.NormalizeEntityID()
	if sc.Mode == "" {
		sc.Mode = "once"
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return errors.New("scheduler stopped")
	}
	key := scheduleKey(sc)
	if existing, ok := s.tasks[key]; ok {
		close(existing.stop)
		delete(s.tasks, key)
	}
	task := &scheduledTask{stop: make(chan struct{})}
	s.tasks[key] = task
	s.mu.Unlock()

	switch sc.Mode {
	case "once":
		if sc.At.IsZero() {
			return errors.New("schedule.at is required for mode=once")
		}
		go s.runOnce(task, key, sc)
		return nil
	case "cron":
		spec, err := parseCronSpec(sc.Cron)
		if err != nil {
			return err
		}
		go s.runCron(task, key, sc, spec)
		return nil
	default:
		return fmt.Errorf("unsupported schedule mode: %s", sc.Mode)
	}
}

func (s *Scheduler) Cancel(agentID string, eventType string) error {
	if agentID == "" || eventType == "" {
		return errors.New("agent_id and event_type are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := schedulePrefix(agentID, eventType)
	for key, task := range s.tasks {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		close(task.stop)
		delete(s.tasks, key)
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
	close(task.stop)
	delete(s.tasks, key)
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
		close(task.stop)
		delete(s.tasks, key)
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
	case <-timer.C:
		s.fire(sc)
		s.unregisterTask(key, task)
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
			case <-ticker.C:
				s.fire(sc)
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
		case <-timer.C:
			s.fire(sc)
		}
	}
}

func (s *Scheduler) fire(sc Schedule) {
	if s.onFire != nil {
		s.onFire(sc)
	}
}

func (s *Scheduler) unregisterTask(key string, task *scheduledTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.tasks[key]
	if !ok || current != task {
		return
	}
	delete(s.tasks, key)
}

func scheduleKey(sc Schedule) string {
	return strings.Join([]string{
		strings.TrimSpace(sc.AgentID),
		strings.TrimSpace(sc.EventType),
		strings.TrimSpace(sc.EffectiveEntityID()),
		strings.TrimSpace(sc.TaskID),
	}, "|")
}

func schedulePrefix(agentID, eventType string) string {
	return strings.TrimSpace(agentID) + "|" + strings.TrimSpace(eventType) + "|"
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
