package authoractivity

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

type RenderMode string

const (
	RenderTTY    RenderMode = "tty"
	RenderPlain  RenderMode = "plain"
	RenderNDJSON RenderMode = "ndjson"
)

type Palette struct {
	Time            func(string) string
	Subject         func(string) string
	Identity        func(string) string
	SubjectIdentity func(string) string
	Success         func(string) string
	Warning         func(string) string
	Failure         func(string) string
}

type RenderOptions struct {
	Mode    RenderMode
	Width   int
	Palette Palette
}

func Render(w io.Writer, occurrences []Occurrence, opts RenderOptions) error {
	if w == nil {
		return fmt.Errorf("author activity writer is required")
	}
	if opts.Mode == "" {
		opts.Mode = RenderPlain
	}
	if opts.Mode == RenderNDJSON {
		encoder := json.NewEncoder(w)
		encoder.SetEscapeHTML(false)
		for _, occurrence := range occurrences {
			if err := validateOccurrenceForRender(occurrence); err != nil {
				return err
			}
			if err := encoder.Encode(occurrence); err != nil {
				return fmt.Errorf("render author activity NDJSON: %w", err)
			}
		}
		return nil
	}
	if opts.Mode != RenderPlain && opts.Mode != RenderTTY {
		return fmt.Errorf("author activity render mode %q is not supported", opts.Mode)
	}
	renderer := NewHumanRenderer(opts)
	rendered, next, err := renderer.PreparePage(occurrences)
	if err != nil {
		return err
	}
	flushed, _, err := next.PrepareFlush()
	if err != nil {
		return err
	}
	if _, err := w.Write(append(rendered, flushed...)); err != nil {
		return err
	}
	return nil
}

func humanLine(occurrence Occurrence, opts RenderOptions) (string, string, error) {
	if err := validateOccurrenceForRender(occurrence); err != nil {
		return "", "", err
	}
	timestamp := occurrence.OccurredAt.Format("15:04:05")
	subject, err := activitySubject(occurrence)
	if err != nil {
		return "", "", err
	}
	action := activityAction(occurrence)
	failed := occurrence.Failure != nil || strings.Contains(action, "failed")
	if failed && !strings.Contains(action, "✗") {
		action = "✗ " + action
	}
	if occurrence.AuthorSafeSummary != "" {
		action += " " + fmt.Sprintf("%q", occurrence.AuthorSafeSummary)
	}
	if occurrence.Projection.DurationMS != nil && *occurrence.Projection.DurationMS >= 0 {
		action += " (" + formatDurationMS(*occurrence.Projection.DurationMS) + ")"
	}
	if occurrence.Failure != nil {
		action += " — internal error"
	}
	line := timestamp + "  " + subject + " " + action
	if !diagnosticRouteRequired(occurrence) {
		return line, "", nil
	}
	return line, diagnosticRoute(occurrence), nil
}

func diagnosticRouteRequired(occurrence Occurrence) bool {
	if occurrence.Failure != nil {
		return true
	}
	return occurrence.Kind == KindAgentLifecycle && occurrence.Transition == "failed"
}

func activitySubject(occurrence Occurrence) (string, error) {
	contract, ok := kindContracts[occurrence.Kind]
	if !ok {
		return "", fmt.Errorf("author activity kind %q is not registered", occurrence.Kind)
	}
	if contract.SubjectRenderer != nil {
		return contract.SubjectRenderer(occurrence), nil
	}
	projection := occurrence.Projection
	switch contract.SubjectStrategy {
	case subjectTypedIdentity:
		if projection.AuthorSubjectType != "" && projection.AuthorSubjectID != "" {
			return strings.TrimSpace(projection.AuthorSubjectType) + " " + strings.TrimSpace(projection.AuthorSubjectID), nil
		}
		if projection.SubjectType == "agent" || projection.SubjectType == "node" {
			return strings.TrimSpace(projection.SubjectID), nil
		}
		return strings.TrimSpace(projection.SubjectType) + " " + strings.TrimSpace(projection.SubjectID), nil
	case subjectProducer:
		return strings.TrimSpace(projection.ProducerID), nil
	case subjectAdapter:
		return strings.TrimSpace(projection.Adapter), nil
	default:
		return "", fmt.Errorf("author activity %s subject strategy is not registered", occurrence.Kind)
	}
}

func validateOccurrenceForRender(occurrence Occurrence) error {
	if err := ValidateDraft(Draft{
		OccurrenceID: occurrence.OccurrenceID, Kind: occurrence.Kind, Version: occurrence.Version,
		Transition: occurrence.Transition, SourceOwner: occurrence.SourceOwner, SourceIdentity: occurrence.SourceIdentity,
		DedupKey: occurrence.DedupKey, OccurredAt: occurrence.OccurredAt, RunID: occurrence.RunID,
		EntityID: occurrence.EntityID, AgentID: occurrence.AgentID, FlowID: occurrence.FlowID,
		Scope: occurrence.Scope, AuthorSafeSummary: occurrence.AuthorSafeSummary, Projection: occurrence.Projection, Failure: occurrence.Failure,
	}); err != nil {
		return fmt.Errorf("render author activity occurrence: %w", err)
	}
	return nil
}

func activityAction(occurrence Occurrence) string {
	contract, ok := kindContracts[occurrence.Kind]
	if !ok {
		return occurrence.Transition
	}
	action := strings.TrimSpace(contract.Actions[occurrence.Transition])
	if contract.ActionRenderer != nil {
		return contract.ActionRenderer(occurrence, action)
	}
	return action
}

func renderInboundSubject(occurrence Occurrence) string {
	return strings.TrimSpace(occurrence.Projection.Provider)
}

func renderInboundAction(occurrence Occurrence, action string) string {
	p := occurrence.Projection
	if p.AuthorSubjectType == "" || p.AuthorSubjectID == "" {
		return "→ " + action
	}
	return "→ " + action + " (" + strings.TrimSpace(p.AuthorSubjectType) + " " + strings.TrimSpace(p.AuthorSubjectID) + ")"
}

func renderEventAction(occurrence Occurrence, _ string) string {
	return "→ " + strings.TrimSpace(occurrence.Projection.EventType)
}

func renderEntitySubject(occurrence Occurrence) string {
	p := occurrence.Projection
	if strings.TrimSpace(p.AuthorSubjectType) != "" && strings.TrimSpace(p.AuthorSubjectID) != "" {
		return strings.TrimSpace(p.AuthorSubjectType) + " " + strings.TrimSpace(p.AuthorSubjectID)
	}
	return "entity"
}

func renderEntityAction(occurrence Occurrence, action string) string {
	p := occurrence.Projection
	if occurrence.Transition == "stage_changed" {
		return strings.TrimSpace("moved " + p.OldState + " → " + p.NewState)
	}
	if strings.TrimSpace(p.NewState) == "" {
		return action
	}
	return action + " · stage " + strings.TrimSpace(p.NewState)
}

func renderDeadLetterSubject(occurrence Occurrence) string {
	if eventType := strings.TrimSpace(occurrence.Projection.EventType); eventType != "" {
		return eventType
	}
	return "event"
}

func renderActivitySubject(occurrence Occurrence) string {
	if activity := strings.TrimSpace(occurrence.Projection.Activity); activity != "" {
		return activity
	}
	if tool := strings.TrimSpace(occurrence.Projection.Tool); tool != "" {
		return tool
	}
	return "activity"
}

func colorHumanLine(line string, occurrence Occurrence, opts RenderOptions) string {
	if opts.Mode != RenderTTY {
		return line
	}
	timestamp := occurrence.OccurredAt.Format("15:04:05")
	subject, err := activitySubject(occurrence)
	if err != nil {
		return line
	}
	prefix := timestamp + "  " + subject + " "
	if !strings.HasPrefix(line, prefix) {
		if strings.HasPrefix(line, timestamp) {
			return apply(opts.Palette.Time, timestamp) + strings.TrimPrefix(line, timestamp)
		}
		return line
	}
	action := strings.TrimPrefix(line, prefix)
	failed := occurrence.Failure != nil || strings.Contains(action, "failed")
	if failed {
		return apply(opts.Palette.Failure, line)
	}
	action = colorActionIdentities(action, occurrence, opts.Palette)
	if strings.Contains(action, "uncertain") || strings.Contains(action, "warning") || strings.Contains(action, "stalled") {
		action = apply(opts.Palette.Warning, action)
	} else {
		action = apply(opts.Palette.Success, action)
	}
	styledSubject := apply(opts.Palette.Subject, subject)
	if subjectCarriesIdentity(occurrence) {
		if opts.Palette.SubjectIdentity != nil {
			styledSubject = apply(opts.Palette.SubjectIdentity, subject)
		} else {
			styledSubject = apply(opts.Palette.Identity, styledSubject)
		}
	}
	return apply(opts.Palette.Time, timestamp) + "  " + styledSubject + " " + action
}

func colorActionIdentities(action string, occurrence Occurrence, palette Palette) string {
	if occurrence.Kind != KindInboundReceived {
		return action
	}
	p := occurrence.Projection
	identity := strings.TrimSpace(p.AuthorSubjectType) + " " + strings.TrimSpace(p.AuthorSubjectID)
	identity = strings.TrimSpace(identity)
	if identity == "" || p.AuthorSubjectType == "" || p.AuthorSubjectID == "" {
		return action
	}
	return strings.Replace(action, identity, apply(palette.Identity, identity), 1)
}

func subjectCarriesIdentity(occurrence Occurrence) bool {
	p := occurrence.Projection
	switch occurrence.Kind {
	case KindInboundReceived, KindDeadLetterRecorded, KindActivityLifecycle:
		return false
	case KindEntityLifecycle:
		return strings.TrimSpace(p.AuthorSubjectType) != "" && strings.TrimSpace(p.AuthorSubjectID) != ""
	case KindEventEmitted, KindEffectLifecycle:
		return true
	case KindPlatformSignal:
		return p.SubjectType == "agent" || p.SubjectType == "run"
	default:
		return p.SubjectType == "agent" || p.SubjectType == "node" || p.SubjectType == "run" || p.SubjectType == "card"
	}
}

func renderPlatformSignalSubject(occurrence Occurrence) string {
	p := occurrence.Projection
	switch strings.TrimSpace(p.SubjectType) {
	case "agent", "node":
		return strings.TrimSpace(p.SubjectID)
	case "run":
		return strings.TrimSpace("run " + p.SubjectID)
	case "event":
		if eventType := strings.TrimSpace(p.EventType); eventType != "" {
			return eventType
		}
		return "event"
	case "entity":
		return "entity"
	default:
		return "platform"
	}
}

func renderTurnToolAction(occurrence Occurrence, action string) string {
	return suffix(action, occurrence.Projection.ToolName)
}

func truncateLine(line string, width int) string {
	if utf8.RuneCountInString(line) <= width {
		return line
	}
	if width <= 1 {
		return "…"
	}
	runes := []rune(line)
	return string(runes[:width-1]) + "…"
}

func suffix(prefix, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return prefix
	}
	return prefix + " " + value
}

func apply(fn func(string) string, value string) string {
	if fn == nil {
		return value
	}
	return fn(value)
}

func formatDurationMS(value int) string {
	return (time.Duration(value) * time.Millisecond).String()
}
