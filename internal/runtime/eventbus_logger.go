package runtime

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeventpayload "github.com/division-sh/swarm/internal/runtime/eventpayload"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func newRuntimeEventBus(store runtimebus.EventStore, durable runtimebus.DurableDependencies, pipelineObligations runtimepipelineobligation.Store, logger *RuntimeLogger, source semanticview.Source, posture executionposture.Posture, bundleSourceFact runtimecorrelation.BundleSourceFact, runtimeInstanceID string, workOwner *worklifetime.RuntimeOccurrence, interceptorProvider func() []runtimebus.EventInterceptor, payloadAdmitter runtimebus.PayloadAdmitter, templateInstanceActivator runtimepipeline.FlowInstanceActivator, templateInstancePlanner runtimepipeline.FlowInstanceActivationPlanner, flowActivationFinalizer runtimepipeline.CommittedFlowInstanceActivationFinalizer, providerOutputVerifier runtimebus.ProviderOutputAuthorizationVerifier, testLifecycleProbe runtimelifecycleprobe.Observer) (*runtimebus.EventBus, error) {
	var hook runtimebus.LoggerHook
	if logger != nil {
		hook = runtimeLoggerHook{logger: logger}
	}
	opts := runtimebus.EventBusOptions{
		ExecutionPosture:          posture,
		Logger:                    hook,
		InterceptorProvider:       interceptorProvider,
		ContractBundle:            source,
		TemplateInstanceActivator: templateInstanceActivator,
		TemplateInstancePlanner:   templateInstancePlanner,
		FlowActivationFinalizer:   flowActivationFinalizer,
		PayloadAdmitter:           payloadAdmitter,
		BundleSourceFact:          bundleSourceFact,
		RuntimeInstanceID:         strings.TrimSpace(runtimeInstanceID),
		WorkOwner:                 workOwner,
		ReceiverExecution:         eventreceiver.NormalExecution(),
		TestLifecycleProbe:        testLifecycleProbe,
		ProviderOutputVerifier:    providerOutputVerifier,
		PipelineObligations:       pipelineObligations,
		Durable:                   durable,
	}
	if pipelineObligations == nil {
		return runtimebus.NewEphemeralEventBusWithOptions(store, opts)
	}
	return runtimebus.NewEventBusWithOptions(store, opts)
}

// NewRuntimePayloadAdmitter is the single event payload admission owner. It
// resolves only against the runtime's pinned semantic source and returns the
// normalized bytes together with immutable schema provenance.
func NewRuntimePayloadAdmitter(logger *RuntimeLogger, source semanticview.Source, bundleFact runtimecorrelation.BundleSourceFact) runtimebus.PayloadAdmitter {
	bundleHash, bundleSource := bundleFact.StorageValues()
	return func(ctx context.Context, event events.Event, flowID string) (events.PayloadAdmission, error) {
		eventType := strings.TrimSpace(string(event.Type()))
		if eventType == "" {
			return events.PayloadAdmission{}, fmt.Errorf("event type is required for payload admission")
		}
		payload := event.Payload()
		if len(payload) == 0 {
			if event.AdmissionClass() == events.EventAdmissionSelectedForkReplay {
				return events.PayloadAdmission{}, fmt.Errorf("selected-fork replay payload bytes are required")
			}
			payload = []byte("{}")
		}
		decoded := map[string]any{}
		if err := canonicaljson.DecodeInto(payload, &decoded); err != nil {
			if logger != nil {
				handleRuntimeLogPersistenceError("event-bus", "payload_validation_json_invalid", logger.Warn(ctx, "event-bus", "payload_validation_json_invalid", map[string]any{
					"event_type": eventType,
				}, err))
			}
			return events.PayloadAdmission{}, err
		}

		resolution := semanticview.ResolveEventSchema(source, strings.TrimSpace(flowID), eventType)
		schema := map[string]any(nil)
		bindingClass := events.PayloadSchemaSchemaLess
		eventKey := eventType
		schemaDigest := canonicaljson.HashBytes([]byte("{}"))
		if resolution.HasSchema {
			if err := resolution.UnresolvedTypeError(); err != nil {
				return events.PayloadAdmission{}, err
			}
			schema = resolution.Schema.Schema
			if key := strings.TrimSpace(resolution.EventKey); key != "" {
				eventKey = key
			}
			acceptanceBytes, err := canonicaljson.Bytes(runtimeeventschema.CanonicalAcceptanceSchema(schema))
			if err != nil {
				return events.PayloadAdmission{}, fmt.Errorf("canonical event acceptance schema: %w", err)
			}
			schemaDigest = canonicaljson.HashBytes(acceptanceBytes)
			if !resolution.HasClassification {
				return events.PayloadAdmission{}, fmt.Errorf("event %s payload schema has no canonical source classification", eventType)
			}
			if resolution.HasCompiled {
				schemaDigest = resolution.CompiledSchema.AcceptanceSchemaDigest()
			}
			switch resolution.Classification {
			case runtimecontracts.CompiledEventSchemaAuthored:
				bindingClass = events.PayloadSchemaAuthored
			case runtimecontracts.CompiledEventSchemaImported:
				bindingClass = events.PayloadSchemaImported
			case runtimecontracts.CompiledEventSchemaGenerated:
				bindingClass = events.PayloadSchemaGenerated
			case runtimecontracts.CompiledEventSchemaPattern:
				bindingClass = events.PayloadSchemaPattern
			case runtimecontracts.CompiledEventSchemaPlatform:
				bindingClass = events.PayloadSchemaPlatform
			default:
				return events.PayloadAdmission{}, fmt.Errorf("event %s payload schema has unsupported source classification %q", eventType, resolution.Classification)
			}
		}

		preservePayloadBytes := event.AdmissionClass() == events.EventAdmissionSelectedForkReplay
		if schema != nil && !preservePayloadBytes {
			normalized, err := runtimeeventschema.NormalizeOptionalFieldNulls(schema, decoded)
			if err != nil {
				return events.PayloadAdmission{}, err
			}
			decoded = normalized
		}
		if err := runtimetools.ValidatePayloadAgainstSchema(schema, payloadForCanonicalEventValidation(decoded, schema)); err != nil {
			if logger != nil {
				handleRuntimeLogPersistenceError("event-bus", "payload_validation_rejected", logger.Warn(ctx, "event-bus", "payload_validation_rejected", map[string]any{
					"event_type": eventType,
				}, err))
			}
			return events.PayloadAdmission{}, err
		}
		normalizedBytes := payload
		if !preservePayloadBytes {
			var err error
			normalizedBytes, err = canonicaljson.Bytes(decoded)
			if err != nil {
				return events.PayloadAdmission{}, fmt.Errorf("canonical event payload: %w", err)
			}
		}
		binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
			BundleHash: bundleHash, BundleSource: bundleSource, FlowID: strings.TrimSpace(flowID),
			EventKey: eventKey, SchemaDigest: schemaDigest, SchemaClass: bindingClass,
		})
		if err != nil {
			return events.PayloadAdmission{}, err
		}
		admission, err := events.NewPayloadAdmission(normalizedBytes, binding)
		if err != nil {
			return events.PayloadAdmission{}, err
		}
		return admission, nil
	}
}

// payloadForCanonicalEventValidation validates only the event payload contract.
// Runtime-owned canonical context is envelope/admission metadata unless the
// target event schema explicitly declares the same field as payload.
func payloadForCanonicalEventValidation(payload map[string]any, schema map[string]any) map[string]any {
	if len(payload) == 0 || schema == nil {
		return payload
	}
	return runtimeeventpayload.StripUndeclaredRuntimeOwnedCanonicalContext(payload, schemaPropertyNames(schema))
}

func schemaPropertyNames(schema map[string]any) map[string]struct{} {
	props := runtimesharedjson.SchemaProperties(schema["properties"])
	out := make(map[string]struct{}, len(props))
	for key := range props {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = struct{}{}
	}
	return out
}

type runtimeLoggerHook struct {
	logger *RuntimeLogger
}

func (h runtimeLoggerHook) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	if h.logger == nil {
		return nil
	}
	return h.logger.Log(ctx, RuntimeLogEntry{
		Level:       level,
		Message:     message,
		Component:   component,
		Action:      action,
		EventID:     eventID,
		EventType:   eventType,
		AgentID:     agentID,
		EntityID:    strings.TrimSpace(entityID),
		SessionID:   sessionID,
		Correlation: correlation,
		Detail:      detail,
		Failure:     runtimefailures.CloneEnvelope(failure),
		DurationUS:  durationUS,
	})
}
