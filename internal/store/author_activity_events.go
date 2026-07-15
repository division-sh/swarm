package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func (s *PostgresStore) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return ensureAuthorActivityCatalog(&s.authorActivityCatalogMu, &s.authorActivityCatalog).Register(scope, descriptors)
}

func (s *SQLiteRuntimeStore) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return ensureAuthorActivityCatalog(&s.authorActivityCatalogMu, &s.authorActivityCatalog).Register(scope, descriptors)
}

func ensureAuthorActivityCatalog(mu *sync.Mutex, registry **runtimeauthoractivity.EventCatalogRegistry) *runtimeauthoractivity.EventCatalogRegistry {
	mu.Lock()
	defer mu.Unlock()
	if *registry == nil {
		*registry = runtimeauthoractivity.NewEventCatalogRegistry()
	}
	return *registry
}

func (s *PostgresStore) authorActivityEventDescriptor(scope runtimeauthoractivity.Scope, name string) (runtimeauthoractivity.EventDescriptor, bool) {
	return ensureAuthorActivityCatalog(&s.authorActivityCatalogMu, &s.authorActivityCatalog).Resolve(scope, name)
}

func (s *SQLiteRuntimeStore) authorActivityEventDescriptor(scope runtimeauthoractivity.Scope, name string) (runtimeauthoractivity.EventDescriptor, bool) {
	return ensureAuthorActivityCatalog(&s.authorActivityCatalogMu, &s.authorActivityCatalog).Resolve(scope, name)
}

func (s *PostgresStore) authorActivityEventCatalogRegistered(scope runtimeauthoractivity.Scope) bool {
	return ensureAuthorActivityCatalog(&s.authorActivityCatalogMu, &s.authorActivityCatalog).HasScope(scope)
}

func (s *SQLiteRuntimeStore) authorActivityEventCatalogRegistered(scope runtimeauthoractivity.Scope) bool {
	return ensureAuthorActivityCatalog(&s.authorActivityCatalogMu, &s.authorActivityCatalog).HasScope(scope)
}

type authoredEventDescriptorResolver interface {
	authorActivityEventDescriptor(runtimeauthoractivity.Scope, string) (runtimeauthoractivity.EventDescriptor, bool)
}

type authoredEventCatalogLeaseResolver interface {
	authorActivityEventCatalogRegistered(runtimeauthoractivity.Scope) bool
}

func recordPersistedEventAuthorActivity(ctx context.Context, resolver authoredEventDescriptorResolver, evt events.Event, producedBy, producedByType string) error {
	name := strings.TrimSpace(string(evt.Type()))
	if name == "platform.inbound_recorded" || platformEventHandledElsewhere(name) || platformEventDifferentConcept(name) {
		return nil
	}
	if platformEventRegistered(name) {
		return recordPlatformSignalAuthorActivity(ctx, evt)
	}
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || scope.Kind != runtimeauthoractivity.ScopeBundle {
		return fmt.Errorf("persist event %q author activity requires exact bundle scope", name)
	}
	if resolver == nil {
		return fmt.Errorf("persist event %q author activity descriptor registry is required", name)
	}
	descriptor, registered := resolver.authorActivityEventDescriptor(scope, name)
	resolved, hasResolved, err := runtimeauthoractivity.ResolvedEventDescriptorFromContext(ctx, scope, name)
	if err != nil {
		return fmt.Errorf("persist event %q author activity descriptor: %w", name, err)
	}
	if registered && hasResolved && descriptor != resolved {
		return fmt.Errorf("persist event %q author activity descriptor conflicts with registered bundle descriptor", name)
	}
	if !registered && hasResolved {
		leaseResolver, ok := resolver.(authoredEventCatalogLeaseResolver)
		if !ok || !leaseResolver.authorActivityEventCatalogRegistered(scope) {
			return fmt.Errorf("persist event %q author activity descriptor has no live registry lease for runtime %q bundle %q", name, scope.RuntimeInstanceID, scope.BundleHash)
		}
		descriptor = resolved
		registered = true
	}
	if !registered {
		return fmt.Errorf("persist event %q has no author activity descriptor for runtime %q bundle %q", name, scope.RuntimeInstanceID, scope.BundleHash)
	}
	if descriptor.Disposition == runtimeauthoractivity.StoryDifferent {
		return nil
	}
	summary, err := authoredEventSummary(evt.Payload(), descriptor.AuthorSummaryField)
	if err != nil {
		return fmt.Errorf("persist event %q author summary: %w", name, err)
	}
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindEventEmitted, Transition: "emitted",
		SourceOwner: "events", SourceIdentity: evt.ID(), DedupKey: "emit:" + evt.ID(),
		OccurredAt: evt.CreatedAt(), RunID: evt.RunID(), EntityID: evt.EntityID(), FlowID: evt.FlowInstance(),
		AuthorSafeSummary: summary,
		Projection: runtimeauthoractivity.Projection{
			EventType: name, ProducerType: strings.TrimSpace(producedByType), ProducerID: strings.TrimSpace(producedBy),
		},
	})
}

func authoredEventSummary(payload []byte, field string) (string, error) {
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

func recordInboundAuthorActivity(ctx context.Context, evt events.Event, provider string) error {
	projection, _ := runtimeauthoractivity.InboundProjectionFromContext(ctx)
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindInboundReceived, Transition: "received",
		SourceOwner: "events", SourceIdentity: evt.ID(), DedupKey: "inbound:" + evt.ID(),
		OccurredAt: evt.CreatedAt(), RunID: evt.RunID(), EntityID: evt.EntityID(), FlowID: evt.FlowInstance(),
		AuthorSafeSummary: projection.Summary,
		Projection:        runtimeauthoractivity.Projection{SubjectType: "entity", SubjectID: evt.EntityID(), Provider: strings.TrimSpace(provider), AuthorSubjectType: projection.SubjectType, AuthorSubjectID: projection.SubjectID},
	})
}

const (
	platformDispositionRegistered = "registered"
	platformDispositionHandled    = "handled"
	platformDispositionDifferent  = "different"
)

var platformEventDisposition = map[string]string{
	"platform.agent_panic":              platformDispositionRegistered,
	"platform.agent_failed":             platformDispositionRegistered,
	"platform.event_quarantined":        platformDispositionRegistered,
	"platform.dead_letter_escalation":   platformDispositionRegistered,
	"platform.run_stalled":              platformDispositionRegistered,
	"platform.reset":                    platformDispositionRegistered,
	"platform.auth_required":            platformDispositionRegistered,
	"platform.budget_threshold_crossed": platformDispositionRegistered,
	"platform.paused":                   platformDispositionRegistered,
	"platform.resumed":                  platformDispositionRegistered,
	"platform.recovery_failed":          platformDispositionRegistered,
	"platform.inbound_recorded":         platformDispositionHandled,
	"platform.activity_requested":       platformDispositionHandled,
	"platform.dead_letter":              platformDispositionHandled,
	"platform.agent_directive":          platformDispositionHandled,
	"platform.agent_started":            platformDispositionHandled,
	"mailbox.card_decided":              platformDispositionHandled,
	"mailbox.card_deferred":             platformDispositionHandled,
	"mailbox.card_expired":              platformDispositionHandled,
	"mailbox.card_superseded":           platformDispositionHandled,
	"human_task.approved":               platformDispositionHandled,
	"human_task.deferred":               platformDispositionHandled,
	"human_task.expired":                platformDispositionHandled,
	"human_task.rejected":               platformDispositionHandled,
	"platform.runtime_log":              platformDispositionDifferent,
	"platform.boot":                     platformDispositionDifferent,
	"event.replayed":                    platformDispositionDifferent,
}

func platformEventRegistered(name string) bool {
	return platformEventDisposition[name] == platformDispositionRegistered
}
func platformEventHandledElsewhere(name string) bool {
	return platformEventDisposition[name] == platformDispositionHandled
}
func platformEventDifferentConcept(name string) bool {
	return platformEventDisposition[name] == platformDispositionDifferent
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

func recordPlatformSignalAuthorActivity(ctx context.Context, evt events.Event) error {
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
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindPlatformSignal, Transition: transition,
		SourceOwner: "events", SourceIdentity: evt.ID(), DedupKey: "platform-signal:" + evt.ID(),
		OccurredAt: evt.CreatedAt(), RunID: runID, EntityID: entityID, AgentID: payload.AgentID, FlowID: flowID,
		Projection: projection, Failure: failure,
	})
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

func authorActivityEventOccurredAt(evt events.Event) time.Time {
	if at := evt.CreatedAt(); !at.IsZero() {
		return at.UTC()
	}
	return time.Now().UTC()
}
