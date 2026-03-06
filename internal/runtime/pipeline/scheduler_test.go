package pipeline

import (
	"testing"
	"time"
)

func TestSchedulerOnce(t *testing.T) {
	fired := make(chan Schedule, 1)
	s := NewScheduler(func(sc Schedule) {
		fired <- sc
	})
	defer s.Stop()

	err := s.Register(Schedule{
		AgentID:   "a1",
		EventType: "heartbeat",
		Mode:      "once",
		At:        time.Now().Add(30 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("register once: %v", err)
	}

	select {
	case sc := <-fired:
		if sc.AgentID != "a1" {
			t.Fatalf("unexpected agent id: %s", sc.AgentID)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("once schedule did not fire")
	}
}

func TestSchedulerCancel(t *testing.T) {
	fired := make(chan Schedule, 1)
	s := NewScheduler(func(sc Schedule) {
		fired <- sc
	})
	defer s.Stop()

	err := s.Register(Schedule{
		AgentID:   "a1",
		EventType: "heartbeat",
		Mode:      "once",
		At:        time.Now().Add(40 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("register once: %v", err)
	}
	if err := s.Cancel("a1", "heartbeat"); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	select {
	case <-fired:
		t.Fatal("schedule fired after cancel")
	case <-time.After(180 * time.Millisecond):
	}
}

func TestSchedulerEvery(t *testing.T) {
	fired := make(chan struct{}, 4)
	s := NewScheduler(func(sc Schedule) {
		if sc.EventType == "pulse" {
			fired <- struct{}{}
		}
	})
	defer s.Stop()

	err := s.Register(Schedule{
		AgentID:   "a2",
		EventType: "pulse",
		Mode:      "cron",
		Cron:      "@every 20ms",
	})
	if err != nil {
		t.Fatalf("register every: %v", err)
	}

	timeout := time.After(300 * time.Millisecond)
	count := 0
	for count < 2 {
		select {
		case <-fired:
			count++
		case <-timeout:
			t.Fatalf("expected at least 2 ticks, got %d", count)
		}
	}
}

func TestSchedulerCronExpression(t *testing.T) {
	fired := make(chan struct{}, 2)
	s := NewScheduler(func(sc Schedule) {
		if sc.EventType == "cron.tick" {
			fired <- struct{}{}
		}
	})
	defer s.Stop()

	err := s.Register(Schedule{
		AgentID:   "a3",
		EventType: "cron.tick",
		Mode:      "cron",
		Cron:      "*/1 * * * * *",
	})
	if err != nil {
		t.Fatalf("register cron expression: %v", err)
	}

	select {
	case <-fired:
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("cron expression schedule did not fire")
	}
}
