package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type publicInputAdmissionContextKey struct{}
type apiEventPublicationAdmissionContextKey struct{}

type apiEventPublicationEndpointKind uint8

const (
	apiEventPublicationEndpointOrdinaryFlow apiEventPublicationEndpointKind = iota + 1
	apiEventPublicationEndpointTemplate
)

// APIEventPublicationEndpoint is an opaque, source-validated create-new
// endpoint. Callers cannot manufacture flow ownership from event strings.
type APIEventPublicationEndpoint struct {
	kind        apiEventPublicationEndpointKind
	flowID      string
	eventType   events.EventType
	publicInput semanticview.AuthoredEventEndpoint
}

type APIEventPublicationEndpointReadback struct {
	Kind        string
	FlowID      string
	EventType   string
	PublicInput semanticview.AuthoredEventEndpoint
}

func (e APIEventPublicationEndpoint) Readback() APIEventPublicationEndpointReadback {
	readback := APIEventPublicationEndpointReadback{
		FlowID: strings.TrimSpace(e.flowID), EventType: strings.TrimSpace(string(e.eventType)),
		PublicInput: e.publicInput,
	}
	switch e.kind {
	case apiEventPublicationEndpointOrdinaryFlow:
		readback.Kind = "ordinary_flow"
	case apiEventPublicationEndpointTemplate:
		readback.Kind = "template_input"
	}
	return readback
}

type apiEventPublicationAdmission struct {
	kind      apiEventPublicationEndpointKind
	flowID    string
	flowPath  string
	eventType events.EventType
}

type publicInputAdmission struct {
	endpointID string
	flowID     string
	pinName    string
	eventType  events.EventType
	plan       runtimepinrouting.ConnectRoutePlan
}

func NewOrdinaryFlowAPIEventPublicationEndpoint(source semanticview.Source, flowID, eventType string) (APIEventPublicationEndpoint, error) {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.Trim(strings.TrimSpace(eventType), "/")
	if source == nil || flowID == "" || eventType == "" {
		return APIEventPublicationEndpoint{}, fmt.Errorf("ordinary flow event publication endpoint is incomplete")
	}
	scope, ok := semanticview.FlowScopeByID(source, flowID)
	if !ok {
		return APIEventPublicationEndpoint{}, fmt.Errorf("ordinary flow event publication flow %q is not declared", flowID)
	}
	if strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
		return APIEventPublicationEndpoint{}, fmt.Errorf("template flow %q requires exact public-input admission", flowID)
	}
	proof := semanticview.ResolveFlowEventProof(source, flowID, eventType)
	if !proof.HasSchema || strings.Trim(strings.TrimSpace(proof.Canonical), "/") != eventType {
		return APIEventPublicationEndpoint{}, fmt.Errorf("ordinary flow event publication flow %q does not own %q", flowID, eventType)
	}
	return APIEventPublicationEndpoint{
		kind:      apiEventPublicationEndpointOrdinaryFlow,
		flowID:    flowID,
		eventType: events.EventType(eventType),
	}, nil
}

func NewTemplateAPIEventPublicationEndpoint(source semanticview.Source, endpoint semanticview.AuthoredEventEndpoint) (APIEventPublicationEndpoint, error) {
	if source == nil || strings.TrimSpace(endpoint.ID) == "" {
		return APIEventPublicationEndpoint{}, fmt.Errorf("template event publication endpoint is incomplete")
	}
	resolved, ok := semanticview.BuildAuthoredEventEndpointCensus(source).Endpoint(endpoint.ID)
	if !ok || resolved.Direction != semanticview.EventEndpointInputPin || resolved.Kind != semanticview.EventEndpointFlowInputPin ||
		strings.TrimSpace(resolved.FlowID) != strings.TrimSpace(endpoint.FlowID) ||
		strings.TrimSpace(resolved.PinName) != strings.TrimSpace(endpoint.PinName) ||
		strings.TrimSpace(resolved.Event.Canonical) != strings.TrimSpace(endpoint.Event.Canonical) {
		return APIEventPublicationEndpoint{}, fmt.Errorf("template event publication endpoint %q does not match the selected contract census", endpoint.ID)
	}
	return APIEventPublicationEndpoint{
		kind:        apiEventPublicationEndpointTemplate,
		publicInput: resolved,
	}, nil
}

func (e APIEventPublicationEndpoint) admit(source semanticview.Source, evt events.Event) (apiEventPublicationAdmission, *publicInputAdmission, error) {
	switch e.kind {
	case apiEventPublicationEndpointOrdinaryFlow:
		scope, ok := semanticview.FlowScopeByID(source, e.flowID)
		if !ok || strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
			return apiEventPublicationAdmission{}, nil, fmt.Errorf("ordinary flow event publication flow %q is not admitted", e.flowID)
		}
		flowPath := strings.Trim(strings.TrimSpace(scope.Path), "/")
		proof := semanticview.ResolveFlowEventProof(source, e.flowID, string(e.eventType))
		if flowPath == "" || !proof.HasSchema || events.EventType(strings.Trim(strings.TrimSpace(proof.Canonical), "/")) != e.eventType {
			return apiEventPublicationAdmission{}, nil, fmt.Errorf("ordinary flow event publication endpoint %q/%s no longer resolves exactly", e.flowID, e.eventType)
		}
		admission := apiEventPublicationAdmission{
			kind: apiEventPublicationEndpointOrdinaryFlow, flowID: e.flowID,
			flowPath: flowPath, eventType: e.eventType,
		}
		if err := admission.validateEvent(evt); err != nil {
			return apiEventPublicationAdmission{}, nil, err
		}
		return admission, nil, nil
	case apiEventPublicationEndpointTemplate:
		publicInput, err := newPublicInputAdmission(source, e.publicInput)
		if err != nil {
			return apiEventPublicationAdmission{}, nil, err
		}
		if err := publicInput.validateEvent(evt); err != nil {
			return apiEventPublicationAdmission{}, nil, err
		}
		return apiEventPublicationAdmission{kind: apiEventPublicationEndpointTemplate, eventType: publicInput.eventType}, &publicInput, nil
	default:
		return apiEventPublicationAdmission{}, nil, fmt.Errorf("API event publication endpoint is incomplete")
	}
}

func (a apiEventPublicationAdmission) validateEvent(evt events.Event) error {
	if a.kind != apiEventPublicationEndpointOrdinaryFlow || a.flowID == "" || a.flowPath == "" || a.eventType == "" {
		return fmt.Errorf("ordinary flow event publication admission is incomplete")
	}
	if evt.AdmissionClass() != events.EventAdmissionRootIngress {
		return fmt.Errorf("ordinary flow event publication admission requires a new-run root ingress event")
	}
	if evt.Type() != a.eventType {
		return fmt.Errorf("ordinary flow event publication endpoint %s resolves %s, not %s", a.flowID, a.eventType, evt.Type())
	}
	return nil
}

func withAPIEventPublicationAdmission(ctx context.Context, admission apiEventPublicationAdmission) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, apiEventPublicationAdmissionContextKey{}, admission)
}

func apiEventPublicationAdmissionFromContext(ctx context.Context) (apiEventPublicationAdmission, bool) {
	if ctx == nil {
		return apiEventPublicationAdmission{}, false
	}
	admission, ok := ctx.Value(apiEventPublicationAdmissionContextKey{}).(apiEventPublicationAdmission)
	return admission, ok
}

func newPublicInputAdmission(source semanticview.Source, endpoint semanticview.AuthoredEventEndpoint) (publicInputAdmission, error) {
	plan, issue := runtimepinrouting.LowerPublicInputRoutePlan(source, endpoint)
	if !issue.Failure.Empty() {
		return publicInputAdmission{}, fmt.Errorf("public input route %s.%s: %s (%s)",
			strings.TrimSpace(endpoint.FlowID), strings.TrimSpace(endpoint.PinName), issue.Failure.Code(), strings.TrimSpace(issue.Detail))
	}
	readback := plan.ReceiverEndpoint().Readback()
	eventType := events.EventType(strings.TrimSpace(readback.ResolvedEvent))
	if eventType == "" {
		return publicInputAdmission{}, fmt.Errorf("public input route %s.%s has no resolved event identity", endpoint.FlowID, endpoint.PinName)
	}
	return publicInputAdmission{
		endpointID: strings.TrimSpace(endpoint.ID),
		flowID:     strings.TrimSpace(endpoint.FlowID),
		pinName:    strings.TrimSpace(endpoint.PinName),
		eventType:  eventType,
		plan:       plan,
	}, nil
}

func (a publicInputAdmission) validateEvent(evt events.Event) error {
	if a.endpointID == "" || a.flowID == "" || a.pinName == "" || a.eventType == "" {
		return fmt.Errorf("public input admission is incomplete")
	}
	if evt.AdmissionClass() != events.EventAdmissionRootIngress {
		return fmt.Errorf("public input admission requires a new-run root ingress event")
	}
	if evt.Type() != a.eventType {
		return fmt.Errorf("public input endpoint %s.%s resolves %s, not %s", a.flowID, a.pinName, a.eventType, evt.Type())
	}
	return nil
}

func withPublicInputAdmission(ctx context.Context, admission publicInputAdmission) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, publicInputAdmissionContextKey{}, admission)
}

func publicInputAdmissionFromContext(ctx context.Context) (publicInputAdmission, bool) {
	if ctx == nil {
		return publicInputAdmission{}, false
	}
	admission, ok := ctx.Value(publicInputAdmissionContextKey{}).(publicInputAdmission)
	return admission, ok
}

func requirePublicInputRoutePlan(ctx context.Context, routePlan RoutePlan) error {
	admission, required := publicInputAdmissionFromContext(ctx)
	if !required {
		return nil
	}
	if !routePlan.TargetFailure.Empty() {
		return fmt.Errorf("public input endpoint %s.%s is not routable: %s", admission.flowID, admission.pinName, routePlan.TargetFailure.Code())
	}
	if !routePlan.CanonicalRouteOwnerMatched() || routePlan.AuthorityOwner != routePlanSourceConnectRoutePlan {
		return fmt.Errorf("public input endpoint %s.%s did not select the canonical connect route owner", admission.flowID, admission.pinName)
	}
	if len(routePlan.DeliveryRoutes()) == 0 {
		return fmt.Errorf("public input endpoint %s.%s selected zero durable deliveries", admission.flowID, admission.pinName)
	}
	return nil
}

// PublishPublicInputAcknowledged admits one census-proven public template input
// through the canonical route/lifecycle transaction before acknowledging it.
func (eb *EventBus) PublishPublicInputAcknowledged(ctx context.Context, evt events.Event, endpoint semanticview.AuthoredEventEndpoint) error {
	if eb == nil {
		return fmt.Errorf("event bus is required")
	}
	admission, err := newPublicInputAdmission(eb.semanticSource, endpoint)
	if err != nil {
		return err
	}
	if err := admission.validateEvent(evt); err != nil {
		return err
	}
	return eb.PublishAcknowledged(withPublicInputAdmission(ctx, admission), evt)
}
