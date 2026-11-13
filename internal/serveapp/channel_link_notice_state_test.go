package serveapp

import (
	"testing"
	"time"
)

// Invariant: creation-only quotas, burst reporting and restart baselines; pure policy, not native eligibility.
func TestChannelNoticeLinkMemoryCapsAndBurst(t *testing.T) {
	now := time.Unix(1000000, 0)
	var s linkNoticeState
	s.observe(now, true, false, "")
	if s.next(now) != nil {
		t.Fatal("healthy startup fabricated a back notice")
	}
	s.observe(now, false, false, "connection reset")
	event := s.next(now)
	if event == nil || event.kind != "down" || event.extra != 0 {
		t.Fatal("newly observed down was silent", event)
	}
	s.admitted(*event, now)
	s.observe(now.Add(time.Minute), false, false, "connection reset")
	if s.next(now.Add(time.Minute)) != nil || s.drops != 1 {
		t.Fatal("poll/retry counted another drop")
	}
	s.observe(now.Add(2*time.Minute), true, false, "")
	event = s.next(now.Add(2 * time.Minute))
	if event == nil || event.kind != "back" || event.upAt.Sub(event.since) != 2*time.Minute {
		t.Fatal("genuine recovery lost its observed duration", event)
	}
	s.admitted(*event, now.Add(2*time.Minute))
	for i := 3; i <= 5; i++ {
		s.observe(now.Add(time.Duration(i)*time.Minute), i == 4, false, "connection reset")
		if s.next(now.Add(time.Duration(i)*time.Minute)) != nil {
			t.Fatal("creation quota was reset by recovery", i)
		}
	}
	if s.next(now.Add(10*time.Minute-time.Nanosecond)) != nil {
		t.Fatal("down admitted before its exact ten-minute boundary")
	}
	event = s.next(now.Add(10 * time.Minute))
	if event == nil || event.kind != "down" || event.extra != 2 {
		t.Fatal("next down lost the intervening-drop count", event)
	}
	s.admitted(*event, now.Add(10*time.Minute))
	s.observe(now.Add(11*time.Minute), true, false, "")
	if s.next(now.Add(12*time.Minute-time.Nanosecond)) != nil {
		t.Fatal("back borrowed the down quota")
	}
	event = s.next(now.Add(12 * time.Minute))
	if event == nil || event.kind != "back" || event.extra != 0 {
		t.Fatal("back repeated an already reported count", event)
	}
	var restart linkNoticeState
	restart.observe(now.Add(13*time.Minute), true, false, "")
	if restart.next(now.Add(13*time.Minute)) != nil {
		t.Fatal("restart reconstructed a past clear")
	}
	restart.observe(now.Add(14*time.Minute), false, false, "cause unknown")
	if got := restart.next(now.Add(14 * time.Minute)); got == nil || got.kind != "down" || got.drops != 1 {
		t.Fatal("restart during outage did not announce a fresh observation", got)
	}
}

// Invariant: attention is immediate per cause, independent of down/back caps; only genuine clear resets it.
func TestChannelNoticeLinkAttentionHasIndependentCauseSuppression(t *testing.T) {
	now := time.Unix(1000000, 0)
	var s linkNoticeState
	s.observe(now, false, false, "connection reset")
	s.admitted(*s.next(now), now)
	for _, cause := range []string{"logged out", "account temporarily banned"} {
		s.observe(now.Add(time.Second), false, true, cause)
		event := s.next(now.Add(time.Second))
		if event == nil || event.kind != "attention" || event.cause != cause {
			t.Fatal("attention waited for the down quota", event)
		}
		s.admitted(*event, now.Add(time.Second))
		if s.next(now.Add(2*time.Second)) != nil || s.lastDown != now || !s.lastBack.IsZero() {
			t.Fatal("attention repeated or consumed a creation quota")
		}
	}
	s.observe(now.Add(3*time.Second), false, true, "logged out")
	if s.next(now.Add(3*time.Second)) != nil {
		t.Fatal("previously seen cause repeated without recovery")
	}
	old := linkNoticeEvent{kind: "attention", cause: "logged out", drops: s.drops}
	s.observe(now.Add(4*time.Second), true, false, "")
	s.admitted(old, now.Add(5*time.Second))
	s.observe(now.Add(6*time.Second), false, true, "logged out")
	if event := s.next(now.Add(6 * time.Second)); event == nil || event.kind != "attention" {
		t.Fatal("old pending admission suppressed a new terminal episode", event)
	}
}
