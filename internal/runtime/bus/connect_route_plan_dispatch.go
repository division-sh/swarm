package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type connectRoutePlanDescriptorLoader func(context.Context) ([]runtimepinrouting.Descriptor, error)

type connectRoutePlanResolver struct {
	source          semanticview.Source
	routeTable      *RouteTable
	plans           []runtimepinrouting.ConnectRoutePlan
	issues          []runtimepinrouting.ConnectRoutePlanIssue
	loadDescriptors connectRoutePlanDescriptorLoader
	lifecycle       templateInstanceLifecycleOwner
}

type connectRoutePlanDispatch struct {
	Matched          bool
	Failure          runtimepinrouting.TargetFailure
	LiveRecipients   []RoutePlanLiveRecipient
	DeliveryIntents  []RoutePlanDeliveryIntent
	RoutedRecipients []Subscriber
	ExtraDetail      map[string]any
}

func newConnectRoutePlanResolver(source semanticview.Source, routeTable *RouteTable, loadDescriptors connectRoutePlanDescriptorLoader, activator runtimepipeline.FlowInstanceActivator) connectRoutePlanResolver {
	if source == nil {
		return connectRoutePlanResolver{routeTable: routeTable, loadDescriptors: loadDescriptors}
	}
	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	return connectRoutePlanResolver{
		source:          source,
		routeTable:      routeTable,
		plans:           append([]runtimepinrouting.ConnectRoutePlan(nil), plans...),
		issues:          append([]runtimepinrouting.ConnectRoutePlanIssue(nil), issues...),
		loadDescriptors: loadDescriptors,
		lifecycle:       newTemplateInstanceLifecycleOwner(source, loadDescriptors, activator),
	}
}

func (r connectRoutePlanResolver) Plan(ctx context.Context, evt events.Event) (connectRoutePlanDispatch, error) {
	if len(r.plans) == 0 && len(r.issues) == 0 {
		return connectRoutePlanDispatch{}, nil
	}
	for _, issue := range r.issues {
		if r.connectIssueMatchesEvent(issue, evt) {
			return connectRoutePlanDispatch{
				Matched: true,
				Failure: connectRoutePlanTargetFailure(issue.Failure),
				ExtraDetail: map[string]any{
					"connect_route_plan_failure": string(issue.Failure),
					"connect_route_plan_detail":  strings.TrimSpace(issue.Detail),
				},
			}, nil
		}
	}

	matched := r.matchedPlans(evt)
	if len(matched) == 0 {
		return connectRoutePlanDispatch{}, nil
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	descriptors, err := r.descriptorsForPlans(ctx, matched)
	if err != nil {
		return connectRoutePlanDispatch{}, err
	}
	values := connectRoutePlanMatchValues(evt)
	out := connectRoutePlanDispatch{
		Matched: true,
		ExtraDetail: map[string]any{
			"connect_route_plans_count": len(matched),
		},
	}
	for _, plan := range matched {
		materialized, decision, err := r.materializeConnectRoutePlan(ctx, evt, plan, values, descriptors)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if materialized.Failure != "" {
			out.Failure = connectRoutePlanTargetFailure(materialized.Failure)
			out.ExtraDetail["connect_route_plan_failure"] = string(materialized.Failure)
			out.ExtraDetail["connect_route_plan_source_event"] = plan.Source.ResolvedEvent
			out.ExtraDetail["connect_route_plan_receiver_event"] = plan.Receiver.ResolvedEvent
			return out, nil
		}
		if !decision.Empty() {
			out.ExtraDetail["connect_route_plan_template_instance_lifecycle"] = decision.Detail()
		}
		cleanupPreview, err := r.installTemplateInstanceLifecyclePreview(decision)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		routes, subscribers := r.deliveryRoutesForMaterialization(plan, materialized)
		if cleanupPreview != nil {
			cleanupPreview()
		}
		if len(routes) == 0 {
			out.Failure = runtimepinrouting.FailureTargetNotSubscribed
			out.ExtraDetail["connect_route_plan_failure"] = string(runtimepinrouting.FailureTargetNotSubscribed)
			out.ExtraDetail["connect_route_plan_source_event"] = plan.Source.ResolvedEvent
			out.ExtraDetail["connect_route_plan_receiver_event"] = plan.Receiver.ResolvedEvent
			return out, nil
		}
		out.DeliveryIntents = append(out.DeliveryIntents, routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerConnectRoutePlan)...)
		out.LiveRecipients = append(out.LiveRecipients, connectRoutePlanLiveRecipients(routes)...)
		out.RoutedRecipients = append(out.RoutedRecipients, subscribers...)
		if len(plan.Reply) > 0 {
			out.ExtraDetail["connect_route_plan_reply"] = cloneStringMap(plan.Reply)
		}
	}
	out.LiveRecipients = normalizeRoutePlanLiveRecipients(out.LiveRecipients)
	out.DeliveryIntents = normalizeRoutePlanDeliveryIntents(out.DeliveryIntents)
	out.RoutedRecipients = dedupeSubscribers(out.RoutedRecipients)
	return out, nil
}

func (r connectRoutePlanResolver) materializeConnectRoutePlan(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, values map[string]string, descriptors []runtimepinrouting.Descriptor) (runtimepinrouting.ConnectRoutePlanMaterialization, TemplateInstanceLifecycleDecision, error) {
	if materialized, decision, handled, err := r.lifecycle.Materialize(ctx, evt, plan, values, descriptors); handled || err != nil {
		return materialized, decision, err
	}
	return runtimepinrouting.MaterializeConnectRoutePlan(plan, runtimepinrouting.ConnectRoutePlanMaterializationInput{
		MatchValues:             values,
		Descriptors:             descriptors,
		SupportedAddressTargets: runtimepinrouting.SupportedConnectAddressTargets(r.source, plan),
	}), TemplateInstanceLifecycleDecision{}, nil
}

func (r connectRoutePlanResolver) installTemplateInstanceLifecyclePreview(decision TemplateInstanceLifecycleDecision) (func(), error) {
	if strings.TrimSpace(decision.Action) != templateInstanceLifecycleActionPreviewCreate {
		return nil, nil
	}
	if r.routeTable == nil {
		return nil, nil
	}
	identity := decision.Route()
	if !identity.Valid() {
		return nil, nil
	}
	if len(r.routeTable.MaterializedRoutes(identity)) > 0 {
		return nil, nil
	}
	if err := r.routeTable.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
		Identity:            identity,
		ActivationVariables: decision.ActivationVariables(),
	}); err != nil {
		return nil, err
	}
	return func() {
		r.routeTable.RemoveFlowInstanceRoute(identity)
	}, nil
}

func (r connectRoutePlanResolver) matchedPlans(evt events.Event) []runtimepinrouting.ConnectRoutePlan {
	if len(r.plans) == 0 {
		return nil
	}
	out := make([]runtimepinrouting.ConnectRoutePlan, 0, len(r.plans))
	for _, plan := range r.plans {
		if connectEndpointMatchesEvent(plan.Source, evt) {
			out = append(out, plan)
		}
	}
	return out
}

func (r connectRoutePlanResolver) descriptorsForPlans(ctx context.Context, plans []runtimepinrouting.ConnectRoutePlan) ([]runtimepinrouting.Descriptor, error) {
	needsDescriptors := false
	for _, plan := range plans {
		if plan.RequiresRuntimeResolution {
			needsDescriptors = true
			break
		}
	}
	if !needsDescriptors || r.loadDescriptors == nil {
		return nil, nil
	}
	return r.loadDescriptors(ctx)
}

func (r connectRoutePlanResolver) deliveryRoutesForMaterialization(plan runtimepinrouting.ConnectRoutePlan, materialized runtimepinrouting.ConnectRoutePlanMaterialization) ([]events.DeliveryRoute, []Subscriber) {
	targets := connectMaterializedTargets(materialized)
	if len(targets) == 0 {
		return nil, nil
	}
	routes := make([]events.DeliveryRoute, 0, len(targets))
	subscribers := make([]Subscriber, 0, len(targets))
	for _, target := range targets {
		target = target.Normalized()
		matchedSubscribers := r.resolveSelectedReceiverCarriers(plan, target)
		if len(matchedSubscribers) == 0 {
			return nil, nil
		}
		subscribers = append(subscribers, matchedSubscribers...)
		for _, subscriber := range matchedSubscribers {
			subscriberType := strings.TrimSpace(subscriber.Type)
			if subscriberType == "" {
				subscriberType = "node"
			}
			routes = append(routes, events.DeliveryRoute{
				SubscriberType: subscriberType,
				SubscriberID:   strings.TrimSpace(subscriber.ID),
				Target:         target,
			})
		}
	}
	return events.NormalizeDeliveryRoutes(routes), dedupeSubscribers(subscribers)
}

func (r connectRoutePlanResolver) resolveSelectedReceiverCarriers(plan runtimepinrouting.ConnectRoutePlan, target events.RouteIdentity) []Subscriber {
	if r.routeTable == nil {
		return nil
	}
	keys := connectReceiverCarrierRouteKeys(plan, target)
	out := make([]Subscriber, 0, len(keys))
	for _, key := range keys {
		for _, subscriber := range r.routeTable.Resolve(key) {
			if !connectSubscriberMatchesTarget(subscriber, target) {
				continue
			}
			out = append(out, subscriber)
		}
	}
	return dedupeSubscribers(out)
}

func (r connectRoutePlanResolver) connectIssueMatchesEvent(issue runtimepinrouting.ConnectRoutePlanIssue, evt events.Event) bool {
	if r.source == nil || issue.Failure == "" {
		return false
	}
	from, err := issue.Connect.FromRef()
	if err != nil {
		return false
	}
	outputPin, ok := r.source.FlowOutputEventPin(from.FlowID, from.Pin)
	if !ok {
		return false
	}
	endpoint := runtimepinrouting.ConnectRoutePlanEndpoint{
		Root:          from.Root,
		FlowID:        strings.TrimSpace(from.FlowID),
		FlowPath:      strings.Trim(strings.TrimSpace(routeFlowPath(r.source, from.FlowID)), "/"),
		Pin:           strings.TrimSpace(from.Pin),
		Event:         strings.TrimSpace(outputPin.EventType()),
		ResolvedEvent: strings.TrimSpace(r.source.ResolveFlowEventReference(from.FlowID, outputPin.EventType())),
	}
	return connectEndpointMatchesEvent(endpoint, evt)
}

func connectEndpointMatchesEvent(endpoint runtimepinrouting.ConnectRoutePlanEndpoint, evt events.Event) bool {
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	if eventType == "" {
		return false
	}
	if endpoint.Root && !eventFlowInstanceMatchesSourcePath(evt.FlowInstance(), "") {
		return false
	}
	sourceLocal := strings.Trim(strings.TrimSpace(endpoint.Event), "/")
	sourceResolved := strings.Trim(strings.TrimSpace(endpoint.ResolvedEvent), "/")
	sourcePath := strings.Trim(strings.TrimSpace(endpoint.FlowPath), "/")
	if sourcePath == "" {
		sourcePath = strings.Trim(strings.TrimSpace(endpoint.FlowID), "/")
	}
	sourceScoped := sourceLocal
	if sourcePath != "" && sourceLocal != "" {
		sourceScoped = sourcePath + "/" + sourceLocal
	}
	for _, candidate := range uniqueStrings([]string{sourceResolved, sourceScoped}) {
		if candidate != "" && eventType == candidate {
			return true
		}
	}
	if sourceLocal != "" && eventType == sourceLocal {
		return eventFlowInstanceMatchesSourcePath(evt.FlowInstance(), sourcePath)
	}
	flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/")
	if flowInstance != "" && sourceLocal != "" && eventType == flowInstance+"/"+sourceLocal {
		return eventFlowInstanceMatchesSourcePath(flowInstance, sourcePath)
	}
	for _, key := range routedEventKeysForPlan(evt) {
		key = strings.Trim(strings.TrimSpace(key), "/")
		if key != "" && (key == sourceResolved || key == sourceScoped) {
			return true
		}
	}
	return false
}

func eventFlowInstanceMatchesSourcePath(flowInstance, sourcePath string) bool {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	sourcePath = strings.Trim(strings.TrimSpace(sourcePath), "/")
	if sourcePath == "" {
		return flowInstance == ""
	}
	if flowInstance == sourcePath {
		return true
	}
	return runtimeflowidentity.SemanticScopeFromInstancePath(flowInstance) == sourcePath
}

func connectMaterializedTargets(materialized runtimepinrouting.ConnectRoutePlanMaterialization) []events.RouteIdentity {
	if !materialized.Target.Empty() {
		return []events.RouteIdentity{materialized.Target.Normalized()}
	}
	return uniqueRouteIdentities(materialized.TargetSet)
}

func connectReceiverCarrierRouteKeys(plan runtimepinrouting.ConnectRoutePlan, target events.RouteIdentity) []string {
	target = target.Normalized()
	local := strings.Trim(strings.TrimSpace(plan.Receiver.Event), "/")
	resolved := strings.Trim(strings.TrimSpace(plan.Receiver.ResolvedEvent), "/")
	receiverPath := strings.Trim(strings.TrimSpace(plan.Receiver.FlowPath), "/")
	if receiverPath == "" {
		receiverPath = strings.Trim(strings.TrimSpace(plan.Receiver.FlowID), "/")
	}
	out := make([]string, 0, 6)
	if target.FlowInstance != "" && local != "" {
		out = append(out, target.FlowInstance+"/"+local)
	}
	if receiverPath != "" && local != "" {
		out = append(out, receiverPath+"/"+local)
	}
	out = append(out, resolved)
	if receiverPath == "" {
		out = append(out, local)
	}
	return uniqueStrings(out)
}

func connectSubscriberMatchesTarget(subscriber Subscriber, target events.RouteIdentity) bool {
	if strings.TrimSpace(subscriber.ID) == "" {
		return false
	}
	subscriberType := strings.TrimSpace(subscriber.Type)
	if subscriberType == "" {
		return false
	}
	if target.Empty() {
		return true
	}
	return routeMatchesInternalSubscriber(target, subscriber)
}

func connectRoutePlanLiveRecipients(routes []events.DeliveryRoute) []RoutePlanLiveRecipient {
	routes = events.NormalizeDeliveryRoutes(routes)
	if len(routes) == 0 {
		return nil
	}
	out := make([]RoutePlanLiveRecipient, 0, len(routes))
	for _, route := range routes {
		subscriberType := strings.TrimSpace(route.SubscriberType)
		subscriberID := strings.TrimSpace(route.SubscriberID)
		if subscriberType == "" || subscriberID == "" {
			continue
		}
		out = append(out, RoutePlanLiveRecipient{
			RecipientID:       subscriberID,
			SubscriberType:    subscriberType,
			PersistAsDelivery: subscriberType == routePlanSubscriberAgent,
			Producer:          routeIntentProducerConnectRoutePlan,
		})
	}
	return normalizeRoutePlanLiveRecipients(out)
}

func connectRoutePlanTargetFailure(failure runtimepinrouting.ConnectRoutePlanFailure) runtimepinrouting.TargetFailure {
	if failure == "" {
		return ""
	}
	return runtimepinrouting.TargetFailure(strings.TrimSpace(string(failure)))
}

func connectRoutePlanMatchValues(evt events.Event) map[string]string {
	out := map[string]string{}
	for key, value := range flattenConnectRouteValues("payload", payloadObject(evt.Payload())) {
		out[key] = value
		if leaf := connectExpressionLeaf(key); leaf != "" {
			out[leaf] = value
		}
	}
	for key, value := range flattenConnectRouteValues("event", evt.ContextMap("")) {
		out[key] = value
		if leaf := connectExpressionLeaf(key); leaf != "" {
			out[leaf] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func payloadObject(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func flattenConnectRouteValues(prefix string, source map[string]any) map[string]string {
	out := map[string]string{}
	var walk func(string, any)
	walk = func(path string, value any) {
		path = strings.Trim(strings.TrimSpace(path), ".")
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				key = strings.TrimSpace(key)
				if key == "" {
					continue
				}
				next := key
				if path != "" {
					next = path + "." + key
				}
				walk(next, child)
			}
		default:
			if path == "" || value == nil {
				return
			}
			str := strings.TrimSpace(fmt.Sprint(value))
			if str != "" {
				out[path] = str
			}
		}
	}
	walk(strings.TrimSpace(prefix), source)
	return out
}

func connectExpressionLeaf(expr string) string {
	expr = strings.TrimSpace(expr)
	if idx := strings.LastIndex(expr, "."); idx >= 0 && idx < len(expr)-1 {
		return strings.TrimSpace(expr[idx+1:])
	}
	return expr
}
