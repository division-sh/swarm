package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
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
	replyStore      runtimereplycontext.Store
}

type connectRoutePlanDispatch struct {
	Matched              bool
	Failure              runtimepinrouting.TargetFailure
	LiveRecipients       []RoutePlanLiveRecipient
	DeliveryIntents      []RoutePlanDeliveryIntent
	RoutedRecipients     []Subscriber
	ExtraDetail          map[string]any
	ReplyContextConsumed bool
}

func newConnectRoutePlanResolver(source semanticview.Source, routeTable *RouteTable, loadDescriptors connectRoutePlanDescriptorLoader, activator runtimepipeline.FlowInstanceActivator, stores ...any) connectRoutePlanResolver {
	var replyStore runtimereplycontext.Store
	if len(stores) > 0 {
		replyStore, _ = stores[0].(runtimereplycontext.Store)
	}
	if source == nil {
		return connectRoutePlanResolver{routeTable: routeTable, loadDescriptors: loadDescriptors, replyStore: replyStore}
	}
	plans, issues := runtimepinrouting.LowerCompositionConnectRoutePlans(source)
	if targetFree, ok := semanticview.SourceCapability[interface {
		ProviderTriggerTargetFreeAuthorizations() []runtimeprovideroutput.Authorization
	}](source); ok {
		directPlans, directIssues := runtimepinrouting.LowerTargetFreeInputRoutePlans(source, targetFree.ProviderTriggerTargetFreeAuthorizations())
		plans = append(plans, directPlans...)
		issues = append(issues, directIssues...)
	}
	return connectRoutePlanResolver{
		source:          source,
		routeTable:      routeTable,
		plans:           append([]runtimepinrouting.ConnectRoutePlan(nil), plans...),
		issues:          append([]runtimepinrouting.ConnectRoutePlanIssue(nil), issues...),
		loadDescriptors: loadDescriptors,
		lifecycle:       newTemplateInstanceLifecycleOwner(source, routeTable, loadDescriptors, activator),
		replyStore:      replyStore,
	}
}

func (r connectRoutePlanResolver) Plan(ctx context.Context, evt events.Event) (connectRoutePlanDispatch, error) {
	if len(r.plans) == 0 && len(r.issues) == 0 {
		return connectRoutePlanDispatch{}, nil
	}
	for _, issue := range r.issues {
		if r.connectIssueMatchesEvent(ctx, issue, evt) {
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

	matched := r.matchedPlans(ctx, evt)
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
		if plan.ReplyResolution != nil && plan.ReplyResolution.Role == runtimepinrouting.ConnectReplyRoleResponse {
			routes, subscribers, failure, detail, err := r.materializeReplyResponse(ctx, evt, plan, values)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
			if failure != "" {
				out.Failure = failure
				for key, value := range detail {
					out.ExtraDetail[key] = value
				}
				return out, nil
			}
			out.ReplyContextConsumed = true
			out.DeliveryIntents = append(out.DeliveryIntents, routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerConnectRoutePlan)...)
			out.LiveRecipients = append(out.LiveRecipients, connectRoutePlanLiveRecipients(routes)...)
			out.RoutedRecipients = append(out.RoutedRecipients, subscribers...)
			continue
		}
		materialized, decision, err := r.materializeConnectRoutePlan(ctx, evt, plan, values, descriptors)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if materialized.Failure != "" {
			out.Failure = connectRoutePlanTargetFailure(materialized.Failure)
			out.ExtraDetail["connect_route_plan_failure"] = string(materialized.Failure)
			out.ExtraDetail["connect_route_plan_source_event"] = plan.Source.ResolvedEvent
			out.ExtraDetail["connect_route_plan_receiver_event"] = plan.Receiver.ResolvedEvent
			for key, value := range connectRoutePlanFailureDetail(plan, materialized.Failure, values, descriptors) {
				out.ExtraDetail[key] = value
			}
			return out, nil
		}
		if !decision.Empty() {
			out.ExtraDetail["connect_route_plan_template_instance_lifecycle"] = decision.Detail()
		}
		cleanupPreview, err := r.installTemplateInstanceLifecyclePreview(decision)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		routes, subscribers, err := r.deliveryRoutesForMaterialization(ctx, plan, materialized, decision)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if plan.ReplyResolution != nil && plan.ReplyResolution.Role == runtimepinrouting.ConnectReplyRoleRequest {
			routes, err = r.materializeReplyRequest(ctx, evt, plan, routes, values)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
		}
		if cleanupPreview != nil {
			cleanupPreview()
		}
		if strings.TrimSpace(decision.Action) == templateInstanceLifecycleActionCreated {
			refreshed, err := r.descriptorsForPlans(ctx, matched)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
			descriptors = refreshed
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
	}
	out.LiveRecipients = normalizeRoutePlanLiveRecipients(out.LiveRecipients)
	out.DeliveryIntents = normalizeRoutePlanDeliveryIntents(out.DeliveryIntents)
	out.RoutedRecipients = dedupeSubscribers(out.RoutedRecipients)
	return out, nil
}

func (r connectRoutePlanResolver) materializeReplyRequest(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, routes []events.DeliveryRoute, values map[string]string) ([]events.DeliveryRoute, error) {
	reply := plan.ReplyResolution
	if reply == nil || reply.Role != runtimepinrouting.ConnectReplyRoleRequest {
		return routes, nil
	}
	origin := evt.SourceRoute().Normalized()
	if origin.Empty() {
		origin = events.RouteIdentity{
			FlowID:       strings.TrimSpace(reply.RequesterFlowID),
			FlowInstance: strings.Trim(evt.FlowInstance(), "/"),
			EntityID:     strings.TrimSpace(evt.EntityID()),
		}.Normalized()
	}
	if origin.Empty() {
		return nil, fmt.Errorf("reply request %s.%s has no concrete origin route", reply.RequesterFlowID, reply.RequestOutputPin)
	}
	correlation := strings.TrimSpace(evt.ID())
	if key := strings.TrimSpace(reply.CorrelationKey); key != "" {
		correlation = strings.TrimSpace(values["payload."+key])
		if correlation == "" {
			return nil, fmt.Errorf("reply request %s.%s is missing declared carried correlation key %s", reply.RequesterFlowID, reply.RequestOutputPin, key)
		}
	}
	now := evt.CreatedAt()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	record := runtimereplycontext.Record{
		RunID:                evt.RunID(),
		RequestEventID:       evt.ID(),
		RequesterFlowID:      reply.RequesterFlowID,
		RequestOutputPin:     reply.RequestOutputPin,
		ReplyInputPin:        reply.ReplyInputPin,
		ProviderFlowID:       reply.ProviderFlowID,
		ProviderInputPin:     reply.ProviderInputPin,
		ProviderOutputPin:    reply.ProviderOutputPin,
		Origin:               origin,
		RequestCorrelationID: correlation,
		CorrelationKey:       reply.CorrelationKey,
		State:                runtimereplycontext.StateOpen,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	record.ID = runtimereplycontext.DeterministicID(record.RequestEventID, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, record.Origin)
	if r.replyStore == nil {
		return nil, fmt.Errorf("ReplyContextStore is required for resolution mode reply")
	}
	if !templateInstanceLifecyclePreview(ctx) {
		if err := r.replyStore.CreateReplyContext(ctx, record); err != nil {
			return nil, err
		}
	}
	deliveryContext := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}}
	for i := range routes {
		routes[i].Context = deliveryContext
	}
	return events.NormalizeDeliveryRoutes(routes), nil
}

func (r connectRoutePlanResolver) materializeReplyResponse(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, values map[string]string) ([]events.DeliveryRoute, []Subscriber, runtimepinrouting.TargetFailure, map[string]any, error) {
	reply := plan.ReplyResolution
	contextID := events.DeliveryContextFromContext(ctx).ReplyContextID()
	detail := map[string]any{
		"connect_route_plan_resolution_mode": "reply",
		"connect_route_plan_request_pin":     reply.RequesterFlowID + "." + reply.RequestOutputPin,
		"connect_route_plan_reply_pin":       reply.RequesterFlowID + "." + reply.ReplyInputPin,
	}
	if contextID == "" || r.replyStore == nil {
		detail["connect_route_plan_failure"] = string(runtimepinrouting.FailureStaleArrival)
		return nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
	}
	record, err := r.replyStore.LoadReplyContext(ctx, contextID)
	if err != nil {
		if errors.Is(err, runtimereplycontext.ErrNotFound) {
			detail["connect_route_plan_failure"] = string(runtimepinrouting.FailureStaleArrival)
			return nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
		}
		return nil, nil, "", nil, err
	}
	if record.RequesterFlowID != reply.RequesterFlowID || record.RequestOutputPin != reply.RequestOutputPin || record.ReplyInputPin != reply.ReplyInputPin || record.ProviderFlowID != reply.ProviderFlowID || record.ProviderInputPin != reply.ProviderInputPin || record.ProviderOutputPin != reply.ProviderOutputPin || record.CorrelationKey != strings.TrimSpace(reply.CorrelationKey) {
		detail["connect_route_plan_failure"] = string(runtimepinrouting.FailureStaleArrival)
		return nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
	}
	if key := record.CorrelationKey; key != "" {
		actual := strings.TrimSpace(values["payload."+key])
		if actual == "" || actual != record.RequestCorrelationID {
			detail["connect_route_plan_failure"] = string(runtimepinrouting.FailureStaleArrival)
			detail["reply_context_id"] = contextID
			detail["reply_correlation_key"] = key
			detail["request_correlation_id"] = record.RequestCorrelationID
			if actual != "" {
				detail["reply_correlation_id"] = actual
			}
			return nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
		}
	}
	target := record.Origin.Normalized()
	subscribers := r.resolveSelectedReceiverCarriers(ctx, plan, target)
	if target.Empty() || len(subscribers) == 0 {
		detail["connect_route_plan_failure"] = string(runtimepinrouting.FailureStaleArrival)
		detail["reply_origin"] = target
		return nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
	}
	claimed := record
	outcome := runtimereplycontext.ClaimAccepted
	if templateInstanceLifecyclePreview(ctx) {
		if record.State == runtimereplycontext.StateTerminal {
			if record.AcceptedReplyEventID == evt.ID() {
				outcome = runtimereplycontext.ClaimIdempotent
			} else {
				outcome = runtimereplycontext.ClaimTerminal
			}
		}
	} else {
		claimed, outcome, err = r.replyStore.ClaimReplyContext(ctx, contextID, evt.ID())
		if err != nil {
			return nil, nil, "", nil, err
		}
	}
	if outcome == runtimereplycontext.ClaimTerminal {
		detail["connect_route_plan_failure"] = string(runtimepinrouting.FailureReplyAlreadyTerminal)
		detail["accepted_reply_event_id"] = claimed.AcceptedReplyEventID
		return nil, nil, runtimepinrouting.FailureReplyAlreadyTerminal, detail, nil
	}
	routes := make([]events.DeliveryRoute, 0, len(subscribers))
	for _, subscriber := range subscribers {
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
	detail["reply_context_id"] = contextID
	detail["request_event_id"] = record.RequestEventID
	detail["request_correlation_id"] = record.RequestCorrelationID
	detail["reply_claim_outcome"] = outcome
	return events.NormalizeDeliveryRoutes(routes), dedupeSubscribers(subscribers), "", detail, nil
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
		_ = r.routeTable.RemoveFlowInstanceRoute(identity)
	}, nil
}

func (r connectRoutePlanResolver) matchedPlans(ctx context.Context, evt events.Event) []runtimepinrouting.ConnectRoutePlan {
	if len(r.plans) == 0 {
		return nil
	}
	out := make([]runtimepinrouting.ConnectRoutePlan, 0, len(r.plans))
	for _, plan := range r.plans {
		if connectRoutePlanMatchesEvent(ctx, plan, evt) {
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

func (r connectRoutePlanResolver) deliveryRoutesForMaterialization(ctx context.Context, plan runtimepinrouting.ConnectRoutePlan, materialized runtimepinrouting.ConnectRoutePlanMaterialization, decision TemplateInstanceLifecycleDecision) ([]events.DeliveryRoute, []Subscriber, error) {
	targets := connectMaterializedTargets(materialized)
	if len(targets) == 0 {
		return nil, nil, nil
	}
	projection, err := syntheticDeliveryPayloadProjection(plan, decision)
	if err != nil {
		return nil, nil, err
	}
	routes := make([]events.DeliveryRoute, 0, len(targets))
	subscribers := make([]Subscriber, 0, len(targets))
	for _, target := range targets {
		target = target.Normalized()
		matchedSubscribers := r.resolveSelectedReceiverCarriers(ctx, plan, target)
		if len(matchedSubscribers) == 0 {
			return nil, nil, nil
		}
		subscribers = append(subscribers, matchedSubscribers...)
		for _, subscriber := range matchedSubscribers {
			subscriberType := strings.TrimSpace(subscriber.Type)
			if subscriberType == "" {
				subscriberType = "node"
			}
			routes = append(routes, events.DeliveryRoute{
				SubscriberType:    subscriberType,
				SubscriberID:      strings.TrimSpace(subscriber.ID),
				Target:            target,
				PayloadProjection: projection,
			})
		}
	}
	return events.NormalizeDeliveryRoutes(routes), dedupeSubscribers(subscribers), nil
}

func syntheticDeliveryPayloadProjection(plan runtimepinrouting.ConnectRoutePlan, decision TemplateInstanceLifecycleDecision) (events.DeliveryPayloadProjection, error) {
	if plan.InstanceKey == nil || strings.TrimSpace(plan.InstanceKey.Mint) == "" {
		return events.DeliveryPayloadProjection{}, nil
	}
	if decision.Empty() {
		return events.DeliveryPayloadProjection{}, fmt.Errorf("create resolution for %s requires lifecycle key material before delivery route construction", strings.TrimSpace(plan.Receiver.FlowID))
	}
	fields := make(map[string]string, len(decision.KeyMaterial))
	for _, key := range decision.KeyMaterial {
		fields[key.Field] = key.Value
	}
	projection, err := events.NewDeliveryPayloadProjection(fields)
	if err != nil {
		return events.DeliveryPayloadProjection{}, fmt.Errorf("create resolution for %s produced invalid synthetic carry material: %w", strings.TrimSpace(plan.Receiver.FlowID), err)
	}
	return projection, nil
}

func (r connectRoutePlanResolver) resolveSelectedReceiverCarriers(ctx context.Context, plan runtimepinrouting.ConnectRoutePlan, target events.RouteIdentity) []Subscriber {
	tables := []*RouteTable{r.routeTable}
	if staged := transactionRouteTableFromContext(ctx); staged != nil && staged != r.routeTable {
		tables = append(tables, staged)
	}
	if len(tables) == 0 {
		return nil
	}
	keys := connectReceiverCarrierRouteKeys(plan, target)
	out := make([]Subscriber, 0, len(keys))
	for _, routeTable := range tables {
		if routeTable == nil {
			continue
		}
		for _, key := range keys {
			for _, subscriber := range routeTable.Resolve(key) {
				if !connectSubscriberMatchesPlanTarget(plan, subscriber, target) {
					continue
				}
				out = append(out, subscriber)
			}
		}
	}
	return dedupeSubscribers(out)
}

func connectSubscriberMatchesPlanTarget(plan runtimepinrouting.ConnectRoutePlan, subscriber Subscriber, target events.RouteIdentity) bool {
	if connectSubscriberMatchesTarget(subscriber, target) {
		return true
	}
	if plan.FanIn == nil {
		return false
	}
	path := strings.Trim(strings.TrimSpace(subscriber.Path), "/")
	receiverPath := strings.Trim(strings.TrimSpace(plan.Receiver.FlowPath), "/")
	targetPath := strings.Trim(strings.TrimSpace(target.FlowInstance), "/")
	if path == "" || receiverPath == "" || targetPath == "" || path != receiverPath {
		return false
	}
	return targetPath == receiverPath || strings.HasPrefix(targetPath, receiverPath+"/")
}

func (r connectRoutePlanResolver) connectIssueMatchesEvent(ctx context.Context, issue runtimepinrouting.ConnectRoutePlanIssue, evt events.Event) bool {
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
	if !connectEndpointMatchesEvent(endpoint, evt) {
		return false
	}
	return providerOutputAuthorizationMatches(ctx, issue.ProviderOutputAuthorization)
}

func connectRoutePlanMatchesEvent(ctx context.Context, plan runtimepinrouting.ConnectRoutePlan, evt events.Event) bool {
	return connectEndpointMatchesEvent(plan.Source, evt) && providerOutputAuthorizationMatches(ctx, plan.ProviderOutputAuthorization)
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

func connectRoutePlanFailureDetail(plan runtimepinrouting.ConnectRoutePlan, failure runtimepinrouting.ConnectRoutePlanFailure, values map[string]string, descriptors []runtimepinrouting.Descriptor) map[string]any {
	if plan.InstanceKey == nil {
		return nil
	}
	mode := strings.TrimSpace(plan.InstanceKey.Mode)
	if mode != runtimecontracts.FlowInputResolutionModeSelect && mode != runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		return nil
	}
	fields := plan.InstanceKey.Fields
	if len(fields) != 1 {
		return nil
	}
	keyField := strings.TrimSpace(fields[0])
	if keyField == "" {
		return nil
	}
	out := map[string]any{
		"connect_route_plan_resolution_mode":     mode,
		"connect_route_plan_receiver_flow":       strings.TrimSpace(plan.Receiver.FlowID),
		"connect_route_plan_instance_key_field":  keyField,
		"connect_route_plan_failure_remediation": connectRoutePlanInstanceResolutionRemediation(plan, failure, keyField, "", mode),
	}
	material, materialFailure := runtimepinrouting.InstanceKeyMaterialForConnectRoutePlan(plan, values)
	if materialFailure != "" {
		if failure == runtimepinrouting.ConnectFailureAddressValueMissing {
			out["connect_route_plan_failure_remediation"] = connectRoutePlanInstanceResolutionRemediation(plan, failure, keyField, "", mode)
		}
		return out
	}
	keyValue := ""
	for _, key := range material.Keys {
		if strings.TrimSpace(key.Field) == keyField {
			keyValue = strings.TrimSpace(key.Value)
			break
		}
	}
	if keyValue != "" {
		out["connect_route_plan_instance_key_value"] = keyValue
	}
	if failure == runtimepinrouting.ConnectFailureTargetAmbiguous {
		out["connect_route_plan_matched_instance_count"] = len(runtimepinrouting.InstanceKeyDescriptorRoutesForConnectRoutePlan(plan, material.Keys, descriptors))
	}
	out["connect_route_plan_failure_remediation"] = connectRoutePlanInstanceResolutionRemediation(plan, failure, keyField, keyValue, mode)
	return out
}

func connectRoutePlanInstanceResolutionRemediation(plan runtimepinrouting.ConnectRoutePlan, failure runtimepinrouting.ConnectRoutePlanFailure, keyField, keyValue, mode string) string {
	receiverFlow := strings.TrimSpace(plan.Receiver.FlowID)
	if receiverFlow == "" {
		receiverFlow = "receiver flow"
	}
	keyLabel := strings.TrimSpace(keyField)
	if keyLabel == "" {
		keyLabel = "instance key"
	}
	valueText := ""
	if value := strings.TrimSpace(keyValue); value != "" {
		valueText = " = " + value
	}
	switch failure {
	case runtimepinrouting.ConnectFailureAddressValueMissing:
		return fmt.Sprintf("Provide payload.%s before publishing to %s; resolution mode %s requires a carried key value.", keyLabel, receiverFlow, mode)
	case runtimepinrouting.ConnectFailureTargetAmbiguous:
		return fmt.Sprintf("Ensure exactly one active %s instance has %s%s; resolution mode %s cannot choose between multiple matches.", receiverFlow, keyLabel, valueText, mode)
	default:
		if mode == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
			return fmt.Sprintf("Ensure %s can create or reuse exactly one active instance with %s%s; resolution mode %s must converge on one instance.", receiverFlow, keyLabel, valueText, mode)
		}
		return fmt.Sprintf("Create or connect exactly one active %s instance with %s%s before publishing; resolution mode %s never creates a missing instance.", receiverFlow, keyLabel, valueText, mode)
	}
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
