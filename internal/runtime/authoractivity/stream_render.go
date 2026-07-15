package authoractivity

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

type HumanRenderer struct {
	Options RenderOptions
	pending *repeatGroup
}

type repeatGroup struct {
	key     string
	last    Occurrence
	firstAt time.Time
	pending int
	total   int
}

func NewHumanRenderer(opts RenderOptions) HumanRenderer {
	if opts.Mode == "" {
		opts.Mode = RenderPlain
	}
	return HumanRenderer{Options: opts}
}

func (r HumanRenderer) PreparePage(occurrences []Occurrence) ([]byte, HumanRenderer, error) {
	r = r.clone()
	var out bytes.Buffer
	for _, occurrence := range occurrences {
		if err := r.renderOne(&out, occurrence); err != nil {
			return nil, r, err
		}
	}
	return out.Bytes(), r, nil
}

func (r HumanRenderer) PrepareFlush() ([]byte, HumanRenderer, error) {
	r = r.clone()
	var out bytes.Buffer
	if err := r.flushPending(&out); err != nil {
		return nil, r, err
	}
	return out.Bytes(), r, nil
}

// PrepareWindowClose emits any aggregate whose repetition window has elapsed.
// The returned renderer is adopted only after the caller accepts the bytes.
func (r HumanRenderer) PrepareWindowClose(now time.Time) ([]byte, HumanRenderer, error) {
	r = r.clone()
	if r.pending == nil || now.Sub(r.pending.firstAt) < 3*time.Minute {
		return nil, r, nil
	}
	var out bytes.Buffer
	if err := r.flushPending(&out); err != nil {
		return nil, r, err
	}
	return out.Bytes(), r, nil
}

func (r HumanRenderer) clone() HumanRenderer {
	if r.pending != nil {
		copy := *r.pending
		r.pending = &copy
	}
	return r
}

func (r *HumanRenderer) renderOne(out *bytes.Buffer, occurrence Occurrence) error {
	if !HumanVisible(occurrence.Kind, occurrence.Transition) && occurrence.Failure == nil {
		if err := validateOccurrenceForRender(occurrence); err != nil {
			return err
		}
		return r.flushPending(out)
	}
	if occurrence.Failure == nil {
		if err := r.flushPending(out); err != nil {
			return err
		}
		return writeHumanOccurrence(out, occurrence, r.Options, "")
	}
	key, err := repetitionKey(occurrence)
	if err != nil {
		return err
	}
	if r.pending == nil || r.pending.key != key || occurrence.OccurredAt.Sub(r.pending.firstAt) > 3*time.Minute {
		if err := r.flushPending(out); err != nil {
			return err
		}
		r.pending = &repeatGroup{key: key, last: occurrence, firstAt: occurrence.OccurredAt, total: 1}
		return writeHumanOccurrence(out, occurrence, r.Options, "")
	}
	r.pending.last = occurrence
	r.pending.total++
	if r.pending.total == 2 {
		return writeHumanOccurrence(out, occurrence, r.Options, " (2nd time)")
	}
	r.pending.pending++
	if r.pending.pending == 5 {
		if err := writeHumanOccurrence(out, occurrence, r.Options, repeatSuffix(r.pending.pending, r.pending.firstAt, occurrence.OccurredAt)); err != nil {
			return err
		}
		r.pending.pending = 0
		r.pending.firstAt = occurrence.OccurredAt
	}
	return nil
}

func (r *HumanRenderer) flushPending(out *bytes.Buffer) error {
	if r.pending == nil {
		return nil
	}
	if r.pending.pending > 0 {
		if err := writeHumanOccurrence(out, r.pending.last, r.Options, repeatSuffix(r.pending.pending, r.pending.firstAt, r.pending.last.OccurredAt)); err != nil {
			return err
		}
	}
	r.pending = nil
	return nil
}

func repetitionKey(occurrence Occurrence) (string, error) {
	subject, err := activitySubject(occurrence)
	if err != nil {
		return "", err
	}
	failureClass, failureCode := "", ""
	if occurrence.Failure != nil {
		failureClass = string(occurrence.Failure.Class)
		failureCode = occurrence.Failure.Detail.Code
	}
	return strings.Join([]string{
		string(occurrence.Kind), occurrence.Transition, subject, activityAction(occurrence), failureClass, failureCode,
		diagnosticRoute(occurrence), occurrence.AuthorSafeSummary,
	}, "\x00"), nil
}

func repeatSuffix(count int, first, last time.Time) string {
	duration := last.Sub(first).Round(time.Second)
	if duration < 0 {
		duration = 0
	}
	return fmt.Sprintf(" (×%d in %s)", count, duration)
}

func writeHumanOccurrence(out *bytes.Buffer, occurrence Occurrence, opts RenderOptions, suffix string) error {
	line, continuation, err := humanLine(occurrence, opts)
	if err != nil {
		return err
	}
	width := opts.Width
	if width <= 0 || width > 120 {
		width = 120
	}
	line = truncateLine(line+suffix, width)
	line = colorHumanLine(line, occurrence, opts)
	fmt.Fprintln(out, line)
	if continuation != "" && suffix == "" {
		continuation = "          └ " + continuation
		continuation = truncateLine(continuation, width)
		if opts.Mode == RenderTTY {
			continuation = apply(opts.Palette.Time, continuation)
		}
		fmt.Fprintln(out, continuation)
	}
	return nil
}

func diagnosticRoute(occurrence Occurrence) string {
	if !diagnosticRouteRequired(occurrence) {
		return ""
	}
	if occurrence.RunID != "" {
		return "swarm logs --run " + occurrence.RunID + " --level error"
	}
	return "swarm logs --level error"
}
