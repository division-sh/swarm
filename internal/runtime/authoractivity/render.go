package authoractivity

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

type RenderMode string

const (
	RenderTTY    RenderMode = "tty"
	RenderPlain  RenderMode = "plain"
	RenderNDJSON RenderMode = "ndjson"
)

type Palette struct {
	Time    func(string) string
	Subject func(string) string
	Success func(string) string
	Warning func(string) string
	Failure func(string) string
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
	width := opts.Width
	if width <= 0 || width > 120 {
		width = 120
	}
	writer := bufio.NewWriter(w)
	defer writer.Flush()
	for _, occurrence := range occurrences {
		line, continuation, err := humanLine(occurrence, opts)
		if err != nil {
			return err
		}
		line = truncateLine(line, width)
		if _, err := fmt.Fprintln(writer, line); err != nil {
			return err
		}
		if continuation != "" {
			continuation = truncateLine("          "+continuation, width)
			if _, err := fmt.Fprintln(writer, continuation); err != nil {
				return err
			}
		}
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
	if opts.Mode == RenderTTY {
		timestamp = apply(opts.Palette.Time, timestamp)
		subject = apply(opts.Palette.Subject, subject)
		switch {
		case occurrence.Failure != nil || strings.Contains(action, "failed"):
			action = apply(opts.Palette.Failure, action)
		case strings.Contains(action, "uncertain") || strings.Contains(action, "warning") || strings.Contains(action, "stalled"):
			action = apply(opts.Palette.Warning, action)
		default:
			action = apply(opts.Palette.Success, action)
		}
	}
	line := strings.Join([]string{timestamp, subject, action}, "  ")
	if !diagnosticRouteRequired(occurrence) {
		return line, "", nil
	}
	route := "swarm logs --level error"
	if occurrence.RunID != "" {
		route = "swarm logs --run " + occurrence.RunID + " --level error"
	}
	return line, route, nil
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
	projection := occurrence.Projection
	switch contract.SubjectStrategy {
	case subjectTypedIdentity:
		return strings.TrimSpace(projection.SubjectType) + "[" + strings.TrimSpace(projection.SubjectID) + "]", nil
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
		Projection: occurrence.Projection, Failure: occurrence.Failure,
	}); err != nil {
		return fmt.Errorf("render author activity occurrence: %w", err)
	}
	return nil
}

func activityAction(occurrence Occurrence) string {
	p := occurrence.Projection
	switch occurrence.Kind {
	case KindInboundReceived:
		return "message received"
	case KindEventEmitted:
		return suffix("emitted", p.EventType)
	case KindEntityLifecycle:
		if occurrence.Transition == "stage_changed" {
			return strings.TrimSpace("moved " + p.OldState + " -> " + p.NewState)
		}
		return suffix("created", "stage "+p.NewState)
	case KindDeliveryLifecycle:
		switch occurrence.Transition {
		case "in_progress":
			return "in flight"
		case "delivered":
			return "sent"
		case "failed":
			return "failed, retrying"
		default:
			return "failed"
		}
	case KindDeadLetterRecorded:
		return "event failed"
	case KindActivityLifecycle:
		return mapAction(occurrence.Transition, map[string]string{"started": "in flight", "succeeded": "completed", "failed": "failed", "uncertain": "outcome uncertain"})
	case KindEffectLifecycle:
		return mapAction(occurrence.Transition, map[string]string{"launched": "in flight", "terminal_failure": "failed", "outcome_uncertain": "outcome uncertain"})
	case KindTurnLifecycle:
		return "turn " + occurrence.Transition
	case KindTurnToolCompleted:
		return suffix("tool completed", p.ToolName)
	case KindCardLifecycle:
		return occurrence.Transition
	case KindAgentLifecycle:
		return occurrence.Transition
	case KindDirectiveLifecycle:
		return "directive " + strings.ReplaceAll(occurrence.Transition, "_", " ")
	case KindRunLifecycle:
		return strings.ReplaceAll(occurrence.Transition, "_", " ")
	case KindPlatformSignal:
		return strings.ReplaceAll(occurrence.Transition, "_", " ")
	default:
		return occurrence.Transition
	}
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

func mapAction(value string, mapping map[string]string) string {
	if mapped := mapping[value]; mapped != "" {
		return mapped
	}
	return strings.ReplaceAll(value, "_", " ")
}

func apply(fn func(string) string, value string) string {
	if fn == nil {
		return value
	}
	return fn(value)
}
