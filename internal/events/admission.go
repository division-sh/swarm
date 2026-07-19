package events

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type AdmissionOptions struct {
	Now                           time.Time
	RequirePersistentUUIDIdentity bool
}

func AdmitForPublish(event Event, options AdmissionOptions) (AdmittedEvent, error) {
	if event.AdmissionClass() == EventAdmissionDiagnosticDirect {
		return AdmittedEvent{}, fmt.Errorf("diagnostic-direct event requires its closed named persistence operation")
	}
	return admitPersistableEvent(event, options)
}

func AdmitForPersistence(event Event, options AdmissionOptions) (AdmittedEvent, error) {
	return admitPersistableEvent(event, options)
}

func admitPersistableEvent(event Event, options AdmissionOptions) (AdmittedEvent, error) {
	class := event.AdmissionClass()
	switch class {
	case EventAdmissionRootIngress,
		EventAdmissionOperatorInjected,
		EventAdmissionChild,
		EventAdmissionReplay,
		EventAdmissionSelectedForkReplay,
		EventAdmissionRuntimeControl,
		EventAdmissionRuntimeDiagnostic,
		EventAdmissionDiagnosticDirect:
	default:
		return AdmittedEvent{}, fmt.Errorf("event class %q is not persistable", class)
	}
	if err := event.Producer().Validate(); err != nil {
		return AdmittedEvent{}, fmt.Errorf("event producer identity: %w", err)
	}
	if strings.TrimSpace(string(event.Type())) == "" {
		return AdmittedEvent{}, fmt.Errorf("event type is required")
	}
	if !event.ExecutionMode().Valid() {
		return AdmittedEvent{}, fmt.Errorf("event execution_mode must be live or mock")
	}
	if event.ChainDepth() < 0 {
		return AdmittedEvent{}, fmt.Errorf("event chain_depth must be nonnegative")
	}
	if err := validateEnvelopeClaim(event.envelopeClaimForAdmission(), options.RequirePersistentUUIDIdentity); err != nil {
		return AdmittedEvent{}, fmt.Errorf("event envelope: %w", err)
	}

	admitted := event.Clone()
	if admitted.ID() == "" {
		admitted.id = uuid.NewString()
	}
	if admitted.CreatedAt().IsZero() {
		admitted.createdAt = options.Now
	}
	if admitted.CreatedAt().IsZero() {
		admitted.createdAt = time.Now().UTC()
	}
	admitted.createdAt = admitted.createdAt.UTC().Truncate(time.Microsecond)

	switch class {
	case EventAdmissionRootIngress:
		if admitted.RunID() == "" {
			admitted.runID = uuid.NewString()
		}
	case EventAdmissionOperatorInjected:
		if admitted.RunID() == "" {
			return AdmittedEvent{}, fmt.Errorf("operator-injected event requires run_id")
		}
	case EventAdmissionChild, EventAdmissionReplay:
		if admitted.RunID() == "" || admitted.ParentEventID() == "" {
			return AdmittedEvent{}, fmt.Errorf("%s event requires run_id and parent_event_id", class)
		}
	case EventAdmissionSelectedForkReplay:
		if admitted.RunID() == "" || admitted.selectedFork == nil {
			return AdmittedEvent{}, fmt.Errorf("selected-fork replay event requires typed lineage")
		}
	case EventAdmissionRuntimeControl, EventAdmissionRuntimeDiagnostic:
		if isStandaloneRuntimePlatformEvent(admitted.Type(), admitted.SourceAgent()) && admitted.RunID() == "" {
			admitted.runID = uuid.NewString()
		}
	case EventAdmissionDiagnosticDirect:
		if admitted.ParentEventID() != "" && admitted.RunID() == "" {
			return AdmittedEvent{}, fmt.Errorf("diagnostic-direct event with causal parent requires run_id")
		}
	}
	if err := validateAdmittedIdentity(admitted.ID(), admitted.RunID(), admitted.ParentEventID(), options.RequirePersistentUUIDIdentity); err != nil {
		return AdmittedEvent{}, err
	}
	return newAdmittedEvent(admitted), nil
}

func validateAdmittedIdentity(eventID, runID, parentEventID string, requirePersistentUUIDIdentity bool) error {
	if !requirePersistentUUIDIdentity {
		return nil
	}
	if err := validateOptionalUUID("event_id", eventID); err != nil {
		return err
	}
	if err := validateOptionalUUID("run_id", runID); err != nil {
		return err
	}
	if err := validateOptionalUUID("parent_event_id", parentEventID); err != nil {
		return err
	}
	return nil
}

func isStandaloneRuntimePlatformEvent(eventType EventType, producerID string) bool {
	if strings.TrimSpace(producerID) != "runtime" {
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
