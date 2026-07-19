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
	if err := ValidateGenericPublishEvent(event); err != nil {
		return AdmittedEvent{}, err
	}
	return admitPersistableEvent(event, options)
}

func AdmitForPersistence(event Event, options AdmissionOptions) (AdmittedEvent, error) {
	return admitPersistableEvent(event, options)
}

// RevalidatePersistedEvent restores the opaque admitted carrier at a recovery
// boundary without allocating or normalizing any durable fact.
func RevalidatePersistedEvent(event Event) (AdmittedEvent, error) {
	if event.AdmissionClass() == EventAdmissionRootIngress {
		event.rootIntent = rootIngressExistingRun
	}
	if event.CreatedAt().IsZero() {
		return AdmittedEvent{}, fmt.Errorf("persisted event created_at is required")
	}
	if err := ValidatePersistentEvent(event); err != nil {
		return AdmittedEvent{}, err
	}
	return newAdmittedEvent(event.Clone(), restoredRunDisposition(event)), nil
}

func admitPersistableEvent(event Event, options AdmissionOptions) (AdmittedEvent, error) {
	if err := ValidateEventContract(event); err != nil {
		return AdmittedEvent{}, err
	}
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
		switch admitted.rootIntent {
		case rootIngressRunCreating:
			if admitted.RunID() == "" {
				admitted.runID = uuid.NewString()
			}
		case rootIngressExistingRun:
			if admitted.RunID() == "" {
				return AdmittedEvent{}, fmt.Errorf("existing-run root ingress requires run_id")
			}
		default:
			return AdmittedEvent{}, fmt.Errorf("root ingress requires explicit run intent")
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
		if err := validateRuntimeLineageIntent(admitted); err != nil {
			return AdmittedEvent{}, err
		}
		if admitted.runtimeIntent == runtimeLineageStandalone && admitted.RunID() == "" {
			admitted.runID = uuid.NewString()
		}
	case EventAdmissionDiagnosticDirect:
		if err := validateRuntimeLineageIntent(admitted); err != nil {
			return AdmittedEvent{}, err
		}
	}
	if err := validateAdmittedIdentity(admitted.ID(), admitted.RunID(), admitted.ParentEventID(), options.RequirePersistentUUIDIdentity); err != nil {
		return AdmittedEvent{}, err
	}
	if options.RequirePersistentUUIDIdentity {
		if err := ValidatePersistentEvent(admitted); err != nil {
			return AdmittedEvent{}, err
		}
	}
	disposition, err := freshRunDisposition(admitted)
	if err != nil {
		return AdmittedEvent{}, err
	}
	return newAdmittedEvent(admitted, disposition), nil
}

func freshRunDisposition(event Event) (AdmittedRunDisposition, error) {
	switch event.AdmissionClass() {
	case EventAdmissionRootIngress:
		switch event.rootIntent {
		case rootIngressRunCreating:
			return AdmittedRunCreateAuthorized, nil
		case rootIngressExistingRun:
			return AdmittedRunRequireActive, nil
		}
	case EventAdmissionOperatorInjected, EventAdmissionChild, EventAdmissionReplay, EventAdmissionSelectedForkReplay:
		return AdmittedRunRequireActive, nil
	case EventAdmissionRuntimeControl, EventAdmissionRuntimeDiagnostic:
		switch event.runtimeIntent {
		case runtimeLineageStandalone:
			return AdmittedRunCreateAuthorized, nil
		case runtimeLineageCausal, runtimeLineageRunScoped:
			return AdmittedRunRequireActive, nil
		}
	case EventAdmissionDiagnosticDirect:
		if event.Type() == EventTypePlatformRuntimeLog {
			if strings.TrimSpace(event.RunID()) == "" {
				return AdmittedRunless, nil
			}
			return AdmittedRunRequirePresent, nil
		}
		switch event.runtimeIntent {
		case runtimeLineageRunCreating:
			if event.Type() != EventTypePlatformAgentDirective || strings.TrimSpace(event.RunID()) == "" {
				return "", fmt.Errorf("diagnostic-direct event %q is not authorized to create a run", event.Type())
			}
			return AdmittedRunCreateAuthorized, nil
		case runtimeLineageStandalone:
			if strings.TrimSpace(event.RunID()) == "" {
				return AdmittedRunless, nil
			}
		case runtimeLineageCausal, runtimeLineageRunScoped:
			return AdmittedRunRequireActive, nil
		}
	}
	return "", fmt.Errorf("event class %q has no admitted run disposition", event.AdmissionClass())
}

func validateRuntimeLineageIntent(event Event) error {
	switch event.runtimeIntent {
	case runtimeLineageCausal:
		if event.RunID() == "" || event.ParentEventID() == "" {
			return fmt.Errorf("%s causal event requires run_id and parent_event_id", event.AdmissionClass())
		}
	case runtimeLineageRunScoped:
		if event.RunID() == "" || event.ParentEventID() != "" {
			return fmt.Errorf("%s run-scoped event requires run_id without parent_event_id", event.AdmissionClass())
		}
	case runtimeLineageRunCreating:
		if event.AdmissionClass() != EventAdmissionDiagnosticDirect || event.Type() != EventTypePlatformAgentDirective || event.RunID() == "" || event.ParentEventID() != "" {
			return fmt.Errorf("%s run-creating event requires authorized subtype, run_id, and no parent_event_id", event.AdmissionClass())
		}
	case runtimeLineageStandalone:
		if event.ParentEventID() != "" {
			return fmt.Errorf("%s standalone event cannot carry parent_event_id", event.AdmissionClass())
		}
	default:
		return fmt.Errorf("%s event requires explicit causal, run-scoped, or standalone lineage intent", event.AdmissionClass())
	}
	return nil
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
