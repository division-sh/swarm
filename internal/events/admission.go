package events

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type AdmissionOptions struct {
	Class                    EventAdmissionClass
	Now                      time.Time
	RunIDCandidate           string
	ParentEventIDCandidate   string
	SelectedForkLineageOwner string
	SourceAgentDefault       string
}

func AdmitForPublish(evt Event, opts AdmissionOptions) (Event, error) {
	return admitPersistableEvent(evt, opts, true)
}

func AdmitForPersistence(evt Event, opts AdmissionOptions) (Event, error) {
	return admitPersistableEvent(evt, opts, false)
}

func admitPersistableEvent(evt Event, opts AdmissionOptions, allowProjectionDefaults bool) (Event, error) {
	class := normalizedAdmissionClass(opts.Class)
	if class == EventAdmissionUnknown {
		class = normalizedAdmissionClass(evt.AdmissionClass())
	}
	if class == EventAdmissionUnknown {
		return Event{}, fmt.Errorf("event admission class is required for persistence")
	}
	if class == EventAdmissionRouteProbe {
		return Event{}, fmt.Errorf("route probe events are not persistable")
	}
	eventType := EventType(strings.TrimSpace(string(evt.Type())))
	if eventType == "" {
		return Event{}, fmt.Errorf("event type is required for persistence admission")
	}
	if !evt.ExecutionMode().Valid() {
		return Event{}, fmt.Errorf("event execution_mode must be live or mock for persistence admission")
	}
	if class == EventAdmissionProjection && !allowProjectionDefaults {
		return admitAuthoritativeProjectionForPersistence(evt, opts, eventType)
	}
	id := strings.TrimSpace(evt.ID())
	if id == "" {
		id = uuid.NewString()
	}
	createdAt := evt.CreatedAt()
	if createdAt.IsZero() {
		createdAt = opts.Now
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	createdAt = createdAt.UTC()

	runID := firstNonEmpty(evt.RunID(), opts.RunIDCandidate)
	parentEventID := firstNonEmpty(evt.ParentEventID(), opts.ParentEventIDCandidate)
	sourceAgent := firstNonEmpty(evt.SourceAgent(), opts.SourceAgentDefault)

	switch class {
	case EventAdmissionRootIngress:
		if runID == "" {
			runID = uuid.NewString()
		}
	case EventAdmissionChild:
		if runID == "" {
			return Event{}, fmt.Errorf("%s event %s requires admitted run_id", class, eventType)
		}
		if parentEventID == "" {
			return Event{}, fmt.Errorf("%s event %s requires admitted parent_event_id", class, eventType)
		}
	case EventAdmissionReplay:
		if runID == "" {
			return Event{}, fmt.Errorf("%s event %s requires admitted run_id", class, eventType)
		}
		if parentEventID == "" && strings.TrimSpace(opts.SelectedForkLineageOwner) == "" {
			return Event{}, fmt.Errorf("%s event %s requires admitted parent_event_id or selected_fork_lineage_owner", class, eventType)
		}
	case EventAdmissionRuntimeControl, EventAdmissionRuntimeDiagnostic:
		if isStandaloneRuntimePlatformEvent(eventType, sourceAgent) && runID == "" {
			runID = uuid.NewString()
		}
	case EventAdmissionDiagnosticDirect, EventAdmissionProjection:
		// These classes may be global/no-run records. If a run/parent is present
		// it must come from the constructor or a bounded context candidate.
	default:
		return Event{}, fmt.Errorf("unsupported event admission class %q", class)
	}

	return newEvent(
		class,
		id,
		eventType,
		sourceAgent,
		evt.TaskID(),
		evt.Payload(),
		evt.ChainDepth(),
		runID,
		parentEventID,
		evt.NormalizedEnvelope(),
		createdAt,
	).WithProducerType(evt.ProducerType()).WithExecutionMode(evt.ExecutionMode()).WithDeliveryContext(evt.DeliveryContext()), nil
}

func admitAuthoritativeProjectionForPersistence(evt Event, opts AdmissionOptions, eventType EventType) (Event, error) {
	id := strings.TrimSpace(evt.ID())
	if id == "" {
		return Event{}, fmt.Errorf("%s event %s requires authoritative event_id for persistence admission", EventAdmissionProjection, eventType)
	}
	createdAt := evt.CreatedAt()
	if createdAt.IsZero() {
		return Event{}, fmt.Errorf("%s event %s requires authoritative created_at for persistence admission", EventAdmissionProjection, eventType)
	}
	runID := strings.TrimSpace(evt.RunID())
	if runID == "" && strings.TrimSpace(opts.RunIDCandidate) != "" {
		return Event{}, fmt.Errorf("%s event %s requires authoritative run_id for persistence admission", EventAdmissionProjection, eventType)
	}
	parentEventID := strings.TrimSpace(evt.ParentEventID())
	if parentEventID == "" && strings.TrimSpace(opts.ParentEventIDCandidate) != "" {
		return Event{}, fmt.Errorf("%s event %s requires authoritative parent_event_id for persistence admission", EventAdmissionProjection, eventType)
	}
	return newEvent(
		EventAdmissionProjection,
		id,
		eventType,
		evt.SourceAgent(),
		evt.TaskID(),
		evt.Payload(),
		evt.ChainDepth(),
		runID,
		parentEventID,
		evt.NormalizedEnvelope(),
		createdAt.UTC(),
	).WithProducerType(evt.ProducerType()).WithExecutionMode(evt.ExecutionMode()).WithDeliveryContext(evt.DeliveryContext()), nil
}

func normalizedAdmissionClass(class EventAdmissionClass) EventAdmissionClass {
	return EventAdmissionClass(strings.TrimSpace(string(class)))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func isStandaloneRuntimePlatformEvent(eventType EventType, sourceAgent string) bool {
	if strings.TrimSpace(sourceAgent) != "runtime" {
		return false
	}
	switch strings.TrimSpace(string(eventType)) {
	case "platform.boot",
		"platform.recovery_failed",
		"platform.event_quarantined",
		"platform.agent_panic",
		"platform.agent_failed",
		"platform.auth_required",
		"platform.paused",
		"platform.resumed",
		"platform.dead_letter_escalation",
		"platform.run_stalled",
		"platform.budget_threshold_crossed":
		return true
	default:
		return false
	}
}
