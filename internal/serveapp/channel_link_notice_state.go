package serveapp

import "time"

type linkNoticeState struct {
	down                                  bool
	since, upAt, lastDown, lastBack       time.Time
	drops, reported, downDrops, backDrops int
	cause, terminal                       string
	attention                             map[string]int
}

type linkNoticeEvent struct {
	kind, cause  string
	since, upAt  time.Time
	drops, extra int
}

func (s *linkNoticeState) observe(now time.Time, connected, terminal bool, cause string) {
	if connected {
		if s.down {
			s.upAt = now
		}
		s.down, s.terminal = false, ""
		return
	}
	if !s.down {
		if s.drops == s.backDrops {
			s.since = now
		}
		s.drops++
	}
	s.down, s.cause = true, cause
	if terminal {
		s.terminal = cause
	}
}

func (s *linkNoticeState) next(now time.Time) *linkNoticeEvent {
	kind := ""
	switch {
	case s.terminal != "":
		if s.attention[s.terminal] == s.drops {
			return nil
		}
		kind = "attention"
	case s.down && s.drops > s.downDrops && (s.lastDown.IsZero() || !now.Before(s.lastDown.Add(10*time.Minute))):
		kind = "down"
	case !s.down && s.drops > s.backDrops && (s.lastBack.IsZero() || !now.Before(s.lastBack.Add(10*time.Minute))):
		kind = "back"
	default:
		return nil
	}
	extra := s.drops - s.reported
	if s.reported == 0 {
		extra--
	}
	return &linkNoticeEvent{kind: kind, cause: s.cause, since: s.since, upAt: s.upAt,
		drops: s.drops, extra: max(0, extra)}
}

func (s *linkNoticeState) admitted(event linkNoticeEvent, now time.Time) {
	s.reported = max(s.reported, event.drops)
	switch event.kind {
	case "down":
		s.lastDown, s.downDrops = now, max(s.downDrops, event.drops)
	case "back":
		s.lastBack, s.backDrops = now, max(s.backDrops, event.drops)
	case "attention":
		if s.attention == nil {
			s.attention = map[string]int{}
		}
		s.attention[event.cause] = max(s.attention[event.cause], event.drops)
	}
}
