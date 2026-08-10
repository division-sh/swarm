package activityjournal

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type EventDescriptorResolver interface {
	ResolveAuthorActivityEventDescriptor(runtimeauthoractivity.Scope, string) (runtimeauthoractivity.EventDescriptor, bool)
}

type EventCatalogLeaseResolver interface {
	AuthorActivityEventCatalogRegistered(runtimeauthoractivity.Scope) bool
}

func RecordPersistedEvent(ctx context.Context, story runtimeauthoractivity.Mutation, resolver EventDescriptorResolver, admitted events.AdmittedEvent, producedBy, producedByType string) error {
	if story == nil {
		return fmt.Errorf("persisted event author activity mutation is required")
	}
	evt := admitted.Event()
	if PlatformEventRegistered(strings.TrimSpace(string(evt.Type()))) {
		return RecordPlatformSignal(ctx, story, evt)
	}
	draft, ok, err := PersistedEventDraft(ctx, resolver, admitted, producedBy, producedByType)
	if err != nil || !ok {
		return err
	}
	return story.Record(ctx, draft)
}

func RecordNoDeliveryWarning(ctx context.Context, story runtimeauthoractivity.Mutation, admitted events.AdmittedEvent, settlement events.RouteSettlement) error {
	if story == nil {
		return fmt.Errorf("no-delivery author activity mutation is required")
	}
	if !settlement.NoDelivery() || settlement.Reason() == events.NoDeliveryNoSubscriberByDesign {
		return nil
	}
	evt := admitted.Event()
	sourceRoute := evt.RoutingSource().Route().Normalized()
	plans := settlement.Ledger().Plans()
	planIDs := make([]string, 0, len(plans))
	for _, plan := range plans {
		planIDs = append(planIDs, plan.PlanIdentity().String())
	}
	return story.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindPlatformSignal, Transition: "event_no_delivery",
		SourceOwner: "events", SourceIdentity: evt.ID(), DedupKey: "no-delivery:" + evt.ID(),
		OccurredAt: evt.CreatedAt(), RunID: evt.RunID(), EntityID: evt.EntityID(), FlowID: sourceRoute.FlowID,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "event", SubjectID: evt.ID(), EventType: string(evt.Type()),
			ReasonCode: settlement.Reason().Code(), InstancePath: evt.FlowInstance(), PlanSHA256: planIDs,
		},
	})
}

func PersistedEventDraft(ctx context.Context, resolver EventDescriptorResolver, admitted events.AdmittedEvent, producedBy, producedByType string) (runtimeauthoractivity.Draft, bool, error) {
	evt := admitted.Event()
	name := strings.TrimSpace(string(evt.Type()))
	if name == "platform.inbound_recorded" || PlatformEventHandledElsewhere(name) || PlatformEventDifferentConcept(name) {
		return runtimeauthoractivity.Draft{}, false, nil
	}
	if PlatformEventRegistered(name) {
		return runtimeauthoractivity.Draft{}, false, fmt.Errorf("platform signal event %q requires its named story operation", name)
	}
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || scope.Kind != runtimeauthoractivity.ScopeBundle {
		return runtimeauthoractivity.Draft{}, false, fmt.Errorf("persist event %q author activity requires exact bundle scope", name)
	}
	if resolver == nil {
		return runtimeauthoractivity.Draft{}, false, fmt.Errorf("persist event %q author activity descriptor registry is required", name)
	}
	descriptor, registered := resolver.ResolveAuthorActivityEventDescriptor(scope, name)
	resolved, hasResolved, err := runtimeauthoractivity.ResolvedEventDescriptorFromContext(ctx, scope, name)
	if err != nil {
		return runtimeauthoractivity.Draft{}, false, fmt.Errorf("persist event %q author activity descriptor: %w", name, err)
	}
	if registered && hasResolved && descriptor != resolved {
		return runtimeauthoractivity.Draft{}, false, fmt.Errorf("persist event %q author activity descriptor conflicts with registered bundle descriptor", name)
	}
	if !registered && hasResolved {
		leaseResolver, ok := resolver.(EventCatalogLeaseResolver)
		if !ok || !leaseResolver.AuthorActivityEventCatalogRegistered(scope) {
			return runtimeauthoractivity.Draft{}, false, fmt.Errorf("persist event %q author activity descriptor has no live registry lease for runtime %q bundle %q", name, scope.RuntimeInstanceID, scope.BundleHash)
		}
		descriptor = resolved
		registered = true
	}
	if !registered {
		return runtimeauthoractivity.Draft{}, false, fmt.Errorf("persist event %q has no author activity descriptor for runtime %q bundle %q", name, scope.RuntimeInstanceID, scope.BundleHash)
	}
	if descriptor.Disposition == runtimeauthoractivity.StoryDifferent {
		return runtimeauthoractivity.Draft{}, false, nil
	}
	summary, err := AuthorSafeEventSummary(evt.Payload(), descriptor.AuthorSummaryField)
	if err != nil {
		return runtimeauthoractivity.Draft{}, false, fmt.Errorf("persist event %q author summary: %w", name, err)
	}
	return runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindEventEmitted, Transition: "emitted",
		SourceOwner: "events", SourceIdentity: evt.ID(), DedupKey: "emit:" + evt.ID(),
		OccurredAt: evt.CreatedAt(), RunID: evt.RunID(), EntityID: evt.EntityID(), FlowID: evt.FlowInstance(),
		AuthorSafeSummary: summary,
		Projection: runtimeauthoractivity.Projection{
			EventType: name, ProducerType: strings.TrimSpace(producedByType), ProducerID: strings.TrimSpace(producedBy),
		},
	}, true, nil
}

func AuthorSafeEventSummary(payload []byte, field string) (string, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return "", nil
	}
	var object map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return "", fmt.Errorf("decode declared summary field %q: %w", field, err)
	}
	value, ok := object[field]
	if !ok || value == nil {
		return "", nil
	}
	switch typed := value.(type) {
	case string:
		return runtimeauthoractivity.NormalizeAuthorSafeSummary(typed)
	case json.Number:
		return runtimeauthoractivity.NormalizeAuthorSafeSummary(typed.String())
	case bool:
		return runtimeauthoractivity.NormalizeAuthorSafeSummary(strconv.FormatBool(typed))
	default:
		return "", fmt.Errorf("declared summary field %q must be scalar", field)
	}
}

func RecordInbound(ctx context.Context, story runtimeauthoractivity.Mutation, evt events.Event, provider string, projection runtimeauthoractivity.InboundProjection) error {
	if story == nil {
		return fmt.Errorf("inbound author activity mutation is required")
	}
	draft := runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindInboundReceived, Transition: "received",
		SourceOwner: "events", SourceIdentity: evt.ID(), DedupKey: "inbound:" + evt.ID(),
		OccurredAt: evt.CreatedAt(), RunID: evt.RunID(), EntityID: evt.EntityID(), FlowID: evt.FlowInstance(),
		AuthorSafeSummary: projection.Summary,
		Projection:        runtimeauthoractivity.Projection{SubjectType: "entity", SubjectID: evt.EntityID(), Provider: strings.TrimSpace(provider), AuthorSubjectType: projection.SubjectType, AuthorSubjectID: projection.SubjectID},
	}
	return story.Record(ctx, draft)
}

const (
	PlatformDispositionRegistered = "registered"
	PlatformDispositionHandled    = "handled"
	PlatformDispositionDifferent  = "different"
)

var PlatformEventDisposition = map[string]string{
	"platform.agent_panic":              PlatformDispositionRegistered,
	"platform.agent_failed":             PlatformDispositionRegistered,
	"platform.event_quarantined":        PlatformDispositionRegistered,
	"platform.dead_letter_escalation":   PlatformDispositionRegistered,
	"platform.run_stalled":              PlatformDispositionRegistered,
	"platform.reset":                    PlatformDispositionRegistered,
	"platform.auth_required":            PlatformDispositionRegistered,
	"platform.budget_threshold_crossed": PlatformDispositionRegistered,
	"platform.paused":                   PlatformDispositionRegistered,
	"platform.resumed":                  PlatformDispositionRegistered,
	"platform.recovery_failed":          PlatformDispositionRegistered,
	"platform.inbound_recorded":         PlatformDispositionHandled,
	"platform.activity_requested":       PlatformDispositionHandled,
	"platform.dead_letter":              PlatformDispositionHandled,
	"platform.agent_directive":          PlatformDispositionHandled,
	"platform.agent_started":            PlatformDispositionHandled,
	"mailbox.card_decided":              PlatformDispositionHandled,
	"mailbox.card_deferred":             PlatformDispositionHandled,
	"mailbox.card_expired":              PlatformDispositionHandled,
	"mailbox.card_superseded":           PlatformDispositionHandled,
	"human_task.approved":               PlatformDispositionHandled,
	"human_task.deferred":               PlatformDispositionHandled,
	"human_task.expired":                PlatformDispositionHandled,
	"human_task.rejected":               PlatformDispositionHandled,
	"platform.runtime_log":              PlatformDispositionDifferent,
	"platform.boot":                     PlatformDispositionDifferent,
	"event.replayed":                    PlatformDispositionDifferent,
}

func PlatformEventRegistered(name string) bool {
	return PlatformEventDisposition[name] == PlatformDispositionRegistered
}
func PlatformEventHandledElsewhere(name string) bool {
	return PlatformEventDisposition[name] == PlatformDispositionHandled
}
func PlatformEventDifferentConcept(name string) bool {
	return PlatformEventDisposition[name] == PlatformDispositionDifferent
}

type platformSignalPayload struct {
	AgentID          string                    `json:"agent_id"`
	EntityID         string                    `json:"entity_id"`
	FlowInstance     string                    `json:"flow_instance"`
	RetryCount       int                       `json:"retry_count"`
	LastEventType    string                    `json:"last_event_type"`
	EventName        string                    `json:"event_name"`
	ReasonCode       string                    `json:"reason_code"`
	RunID            string                    `json:"run_id"`
	OperationalState string                    `json:"operational_state"`
	BlockingLayer    string                    `json:"blocking_layer"`
	Reason           string                    `json:"reason"`
	Source           string                    `json:"source"`
	Tool             string                    `json:"tool"`
	Action           string                    `json:"action"`
	Level            string                    `json:"level"`
	Spend            json.RawMessage           `json:"spend"`
	Cap              json.RawMessage           `json:"cap"`
	Percentage       json.RawMessage           `json:"percentage"`
	Period           string                    `json:"period"`
	FailedEventID    string                    `json:"failed_event_id"`
	Failure          *runtimefailures.Envelope `json:"failure"`
	LastFailure      *runtimefailures.Envelope `json:"last_failure"`
}

func RecordPlatformSignal(ctx context.Context, story runtimeauthoractivity.Mutation, evt events.Event) error {
	if story == nil {
		return fmt.Errorf("platform signal author activity mutation is required")
	}
	var payload platformSignalPayload
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		return fmt.Errorf("decode registered author activity platform event %s: %w", evt.Type(), err)
	}
	name := strings.TrimSpace(string(evt.Type()))
	transition := ""
	failure := payload.Failure
	if failure == nil {
		failure = payload.LastFailure
	}
	switch name {
	case "platform.agent_panic":
		transition = "agent_failed_retrying"
	case "platform.agent_failed":
		transition = "agent_failed"
	case "platform.event_quarantined":
		transition = "event_quarantined"
	case "platform.dead_letter_escalation":
		transition = "dead_letters_escalated"
	case "platform.run_stalled":
		transition = "run_stalled"
	case "platform.reset":
		transition = "runtime_reset"
	case "platform.auth_required":
		transition = "authorization_required"
	case "platform.budget_threshold_crossed":
		var err error
		transition, err = budgetTransition(payload.Level)
		if err != nil {
			return err
		}
	case "platform.paused":
		transition = "runtime_paused"
	case "platform.resumed":
		transition = "runtime_resumed"
	case "platform.recovery_failed":
		transition = "recovery_failed"
	default:
		return fmt.Errorf("registered author activity platform event %q has no typed adapter", name)
	}
	runID := firstString(evt.RunID(), payload.RunID)
	entityID := firstString(evt.EntityID(), payload.EntityID)
	flowID := firstString(evt.FlowInstance(), payload.FlowInstance)
	subjectType, subjectID := platformSignalSubject(name, payload, runID, entityID)
	projection := runtimeauthoractivity.Projection{
		SubjectType: subjectType, SubjectID: subjectID, EventType: firstString(payload.EventName, payload.LastEventType),
		RetryCount: intPointerIfNonZero(payload.RetryCount), ReasonCode: firstString(payload.ReasonCode, payload.Reason),
		Source: payload.Source, Level: payload.Level, Spend: rawNumberString(payload.Spend), Cap: rawNumberString(payload.Cap),
		Percentage: rawNumberString(payload.Percentage), Period: payload.Period, OperationalState: payload.OperationalState,
		BlockingLayer: payload.BlockingLayer, Tool: payload.Tool,
	}
	draft := runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindPlatformSignal, Transition: transition,
		SourceOwner: "events", SourceIdentity: evt.ID(), DedupKey: "platform-signal:" + evt.ID(),
		OccurredAt: evt.CreatedAt(), RunID: runID, EntityID: entityID, AgentID: payload.AgentID, FlowID: flowID,
		Projection: projection, Failure: failure,
	}
	return story.Record(ctx, draft)
}

func budgetTransition(level string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(level)) {
	case "warning":
		return "budget_warning", nil
	case "throttle":
		return "budget_throttle", nil
	case "emergency":
		return "budget_emergency", nil
	case "ok":
		return "budget_ok", nil
	default:
		return "", fmt.Errorf("registered budget threshold level %q is not supported", level)
	}
}

func platformSignalSubject(name string, payload platformSignalPayload, runID, entityID string) (string, string) {
	if payload.AgentID != "" {
		return "agent", payload.AgentID
	}
	if entityID != "" {
		return "entity", entityID
	}
	if runID != "" {
		return "run", runID
	}
	if payload.FailedEventID != "" {
		return "event", payload.FailedEventID
	}
	return "platform", name
}

func intPointerIfNonZero(value int) *int {
	if value == 0 {
		return nil
	}
	return &value
}

func rawNumberString(raw json.RawMessage) string {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return ""
	}
	if value, err := strconv.Unquote(text); err == nil {
		return value
	}
	return text
}

func firstString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func EventOccurredAt(evt events.Event) time.Time {
	if at := evt.CreatedAt(); !at.IsZero() {
		return at.UTC()
	}
	return time.Now().UTC()
}
