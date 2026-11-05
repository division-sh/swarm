package pipelinepersistence

import (
	"context"
	"log/slog"
	"time"
)

const (
	fanOutTimingKeys = iota
	fanOutTimingJoined
	fanOutTimingEvents
	fanOutTimingDeliveries
)

type fanOutFoldComponent struct {
	calls, completed, partial, rows, requested, returned int
	completeTime, partialTime, minimum, maximum          time.Duration
}

type fanOutFoldTiming struct {
	components                            [4]fanOutFoldComponent
	clockReads, intentsStarted, intentsOK int
}

type fanOutFoldSpan struct {
	owner   *fanOutFoldTiming
	phase   int
	started time.Time
	done    bool
}

func newFanOutFoldTiming(ctx context.Context) *fanOutFoldTiming {
	if !slog.Default().Enabled(ctx, slog.LevelDebug) {
		return nil
	}
	return &fanOutFoldTiming{}
}

func fanOutFoldTimingOption(options []*fanOutFoldTiming) *fanOutFoldTiming {
	if len(options) != 0 {
		return options[0]
	}
	return nil
}

func (t *fanOutFoldTiming) begin(phase, requested int) fanOutFoldSpan {
	if t == nil {
		return fanOutFoldSpan{}
	}
	t.components[phase].calls++
	t.components[phase].requested += requested
	t.clockReads++
	return fanOutFoldSpan{owner: t, phase: phase, started: time.Now()}
}

func (s *fanOutFoldSpan) finish(complete bool, returned int) {
	if s.owner == nil || s.done {
		return
	}
	elapsed := time.Since(s.started)
	s.done = true
	s.owner.clockReads++
	c := &s.owner.components[s.phase]
	c.returned += returned
	if !complete {
		c.partial++
		c.partialTime += elapsed
		return
	}
	c.completed++
	c.completeTime += elapsed
	if c.completed == 1 {
		c.minimum = elapsed
	} else {
		c.minimum = min(c.minimum, elapsed)
	}
	c.maximum = max(c.maximum, elapsed)
}

func (s *fanOutFoldSpan) row() {
	if s.owner != nil {
		s.owner.components[s.phase].rows++
	}
}

func (t *fanOutFoldTiming) intentStarted() {
	if t != nil {
		t.intentsStarted++
	}
}

func (t *fanOutFoldTiming) intentReturned() {
	if t != nil {
		t.intentsOK++
	}
}

func (t *fanOutFoldTiming) attributes(total time.Duration) []any {
	if t == nil {
		return []any{"components_measured", false}
	}
	values := []any{"components_measured", true, "component_clock_reads", t.clockReads,
		"intent_calls", t.intentsStarted, "intent_returns", t.intentsOK}
	var measured time.Duration
	valid := t.intentsOK <= t.intentsStarted
	for phase, name := range []string{"keys", "joined", "event_admission", "delivery_snapshots"} {
		c := t.components[phase]
		measured += c.completeTime + c.partialTime
		valid = valid && c.calls == c.completed+c.partial && c.minimum <= c.maximum && c.completeTime >= 0 && c.partialTime >= 0
		values = append(values, slog.Group(name, "calls", c.calls, "completed", c.completed, "partial", c.partial,
			"complete_elapsed", c.completeTime, "partial_elapsed", c.partialTime,
			"min_complete", c.minimum, "max_complete", c.maximum,
			"rows", c.rows, "requested", c.requested, "returned", c.returned))
	}
	residual := total - measured
	return append(values, "component_elapsed", measured, "unassigned_elapsed", residual,
		"components_valid", valid && residual >= 0)
}
