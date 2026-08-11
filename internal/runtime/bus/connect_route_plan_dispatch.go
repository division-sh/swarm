package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type connectRoutePlanDescriptorLoader func(context.Context) ([]runtimepinrouting.Descriptor, error)
type connectAgentDescriptorLoader func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error)

type connectRoutePlanPreviewRoutesKey struct{}
type closedPublicationPlanningKey struct{}

type connectRoutePlanPreviewRoutes struct {
	table *RouteTable
}

type connectRoutePlanEvaluationMemoKey struct{}

type connectRoutePlanEvaluationMemo struct {
	eventID  string
	dispatch connectRoutePlanDispatch
	snapshot connectRoutePlanSnapshot
	ready    bool
	applied  bool
}

func withConnectRoutePlanEvaluationMemo(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if memo, _ := ctx.Value(connectRoutePlanEvaluationMemoKey{}).(*connectRoutePlanEvaluationMemo); memo != nil {
		return ctx
	}
	return context.WithValue(ctx, connectRoutePlanEvaluationMemoKey{}, &connectRoutePlanEvaluationMemo{})
}

type connectRoutePlanSnapshot struct {
	base    routeTableSnapshotGeneration
	staged  routeTableSnapshotGeneration
	staging bool
}

type staleConnectRoutePlanSnapshotError struct{}

func (staleConnectRoutePlanSnapshotError) Error() string {
	return "connect route snapshot generation is stale"
}

func withClosedPublicationPlanning(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, closedPublicationPlanningKey{}, true)
}

func closedPublicationPlanning(ctx context.Context) bool {
	enabled, _ := ctx.Value(closedPublicationPlanningKey{}).(bool)
	return enabled
}

type connectRoutePlanResolver struct {
	source          semanticview.Source
	routeTable      *RouteTable
	graph           runtimepinrouting.CompiledConnectGraph
	issues          []runtimepinrouting.ConnectRoutePlanIssue
	loadDescriptors connectRoutePlanDescriptorLoader
	loadAgents      connectAgentDescriptorLoader
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
	ActivationPlans      []runtimepipeline.FlowInstanceActivationPlan
	ReplyCreations       []runtimereplycontext.Record
	ReplyClaims          []runtimereplycontext.ClaimCommand
}

func newConnectRoutePlanResolver(source semanticview.Source, routeTable *RouteTable, loadDescriptors connectRoutePlanDescriptorLoader, activator runtimepipeline.FlowInstanceActivator, planner runtimepipeline.FlowInstanceActivationPlanner, replyStore runtimereplycontext.Store) connectRoutePlanResolver {
	if source == nil {
		return connectRoutePlanResolver{routeTable: routeTable, loadDescriptors: loadDescriptors, replyStore: replyStore}
	}
	graph := runtimepinrouting.CompileConnectGraph(source)
	issues := graph.Issues()
	return connectRoutePlanResolver{
		source:          source,
		routeTable:      routeTable,
		graph:           graph,
		issues:          append([]runtimepinrouting.ConnectRoutePlanIssue(nil), issues...),
		loadDescriptors: loadDescriptors,
		lifecycle:       newTemplateInstanceLifecycleOwner(source, routeTable, loadDescriptors, activator, planner),
		replyStore:      replyStore,
	}
}

func (r connectRoutePlanResolver) Plan(ctx context.Context, evt events.Event) (connectRoutePlanDispatch, error) {
	publicAdmission, publicInput := publicInputAdmissionFromContext(ctx)
	if publicInput {
		if err := publicAdmission.validateEvent(evt); err != nil {
			return connectRoutePlanDispatch{}, err
		}
	}
	if !publicInput && len(r.graph.Plans()) == 0 && len(r.issues) == 0 {
		return connectRoutePlanDispatch{}, nil
	}
	if !publicInput {
		for _, issue := range r.issues {
			if r.graph.IssueMatchesEvent(issue, evt) && providerOutputAuthorizationMatches(ctx, issue.ProviderOutputAuthorization()) {
				return connectRoutePlanDispatch{
					Matched: true,
					Failure: connectRoutePlanTargetFailure(issue.Failure),
					ExtraDetail: map[string]any{
						"connect_route_plan_failure": issue.Failure.Code(),
						"connect_route_plan_detail":  strings.TrimSpace(issue.Detail),
					},
				}, nil
			}
		}
	}

	memo, _ := ctx.Value(connectRoutePlanEvaluationMemoKey{}).(*connectRoutePlanEvaluationMemo)
	if memo != nil && memo.ready && memo.eventID == evt.ID() {
		if r.routeTable.snapshotGenerationCurrent(memo.snapshot.base) {
			return memo.dispatch, nil
		}
		memo.ready = false
	}

	matched := r.matchedPlans(ctx, evt)
	if len(matched) == 0 {
		return connectRoutePlanDispatch{}, nil
	}
	for attempt := 0; attempt < 3; attempt++ {
		snapshot := connectRoutePlanSnapshot{base: r.routeTable.snapshotGeneration()}
		evaluationCtx := runtimecorrelation.WithInboundEvent(ctx, evt)
		descriptors, err := r.descriptorsForPlans(evaluationCtx, matched)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		evaluationCtx = withTemplateInstanceLifecyclePreview(evaluationCtx)
		evaluationCtx = context.WithValue(evaluationCtx, connectRoutePlanPreviewRoutesKey{}, &connectRoutePlanPreviewRoutes{})
		evaluated, err := r.planMatched(evaluationCtx, evt, matched, descriptors, connectRoutePlanMatchValues(evt))
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if memo != nil {
			memo.eventID, memo.dispatch, memo.snapshot, memo.ready, memo.applied = evt.ID(), evaluated, snapshot, true, false
		}
		if !evaluated.Failure.Empty() || templateInstanceLifecyclePreview(ctx) || r.routeTable.snapshotGenerationCurrent(snapshot.base) {
			return evaluated, nil
		}
		if memo != nil {
			memo.ready = false
		}
	}
	return connectRoutePlanDispatch{}, staleConnectRoutePlanSnapshotError{}
}

func (r connectRoutePlanResolver) planMatched(ctx context.Context, evt events.Event, matched []runtimepinrouting.ConnectRoutePlan, descriptors []runtimepinrouting.Descriptor, values map[string]string) (connectRoutePlanDispatch, error) {
	out := connectRoutePlanDispatch{
		Matched: true,
		ExtraDetail: map[string]any{
			"connect_route_plans_count": len(matched),
		},
	}
	var receiverPinAdmission runtimepinrouting.ConnectReceiverPinAdmission
	createdRoutes := make(map[runtimeflowidentity.Route]struct{}, len(matched))
	replyContextConsumed := false
	for _, plan := range matched {
		if plan.ReplyResolution() != nil && plan.ReplyResolution().Role() == runtimepinrouting.ConnectReplyRoleResponse {
			routes, subscribers, claim, failure, detail, err := r.materializeReplyResponse(ctx, evt, plan, values)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
			if !failure.Empty() {
				out.Failure = failure
				for key, value := range detail {
					out.ExtraDetail[key] = value
				}
				return out, nil
			}
			routes, err = stampConnectExecutionClaims(plan, routes)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
			if err := receiverPinAdmission.Admit(plan, routes); err != nil {
				return connectRoutePlanDispatch{}, err
			}
			if pins, collision := connectReceiverPinCollisionDetail(receiverPinAdmission); collision {
				out.Failure = connectRoutePlanTargetFailure(runtimepinrouting.ConnectFailureDeliveryTopologyInvalid)
				out.ExtraDetail["connect_route_plan_failure"] = runtimepinrouting.ConnectFailureDeliveryTopologyInvalid.Code()
				out.ExtraDetail["connect_route_plan_receiver_pin_collision"] = pins
				return out, nil
			}
			replyContextConsumed = true
			if claim != nil {
				out.ReplyClaims = append(out.ReplyClaims, *claim)
			}
			out.DeliveryIntents = append(out.DeliveryIntents, routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerConnectRoutePlan)...)
			out.LiveRecipients = append(out.LiveRecipients, connectRoutePlanLiveRecipients(routes)...)
			out.RoutedRecipients = append(out.RoutedRecipients, subscribers...)
			continue
		}
		materialized, decision, err := r.materializeConnectRoutePlan(ctx, evt, plan, values, descriptors)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if !materialized.Failure.Empty() {
			out.Failure = connectRoutePlanTargetFailure(materialized.Failure)
			out.ExtraDetail["connect_route_plan_failure"] = materialized.Failure.Code()
			out.ExtraDetail["connect_route_plan_source_event"] = plan.SourceEndpoint().Readback().ResolvedEvent
			out.ExtraDetail["connect_route_plan_receiver_event"] = plan.ReceiverEndpoint().Readback().ResolvedEvent
			for key, value := range connectRoutePlanFailureDetail(plan, materialized.Failure, values, descriptors) {
				out.ExtraDetail[key] = value
			}
			return out, nil
		}
		if !decision.Empty() {
			out.ExtraDetail["connect_route_plan_template_instance_lifecycle"] = decision.Detail()
		}
		if decision.Activation != nil {
			out.ActivationPlans = append(out.ActivationPlans, *decision.Activation)
		}
		if err := r.installTemplateInstanceLifecyclePreview(ctx, decision); err != nil {
			return connectRoutePlanDispatch{}, err
		}
		action := decision.Action
		if action == templateInstanceLifecycleActionPreviewCreate || action == templateInstanceLifecycleActionCreated {
			if route := decision.Route(); route.Valid() {
				createdRoutes[route] = struct{}{}
			}
		}
		if action == templateInstanceLifecycleActionPreviewCreate {
			descriptors = append(descriptors, runtimepinrouting.Descriptor{
				ID:            strings.TrimSpace(decision.InstanceID),
				EntityID:      strings.TrimSpace(decision.EntityID),
				FlowInstance:  strings.Trim(strings.TrimSpace(decision.InstancePath), "/"),
				AddressFields: decision.ActivationVariables(),
			})
		}
		_, routeCreatedInPlan := createdRoutes[decision.Route()]
		routes, liveRoutes, subscribers, err := r.deliveryRoutesForMaterialization(ctx, plan, materialized, decision, routeCreatedInPlan)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if plan.ReplyResolution() != nil && plan.ReplyResolution().Role() == runtimepinrouting.ConnectReplyRoleRequest {
			var creation *runtimereplycontext.Record
			routes, creation, err = r.materializeReplyRequest(ctx, evt, plan, routes, values)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
			if creation != nil {
				out.ReplyCreations = append(out.ReplyCreations, *creation)
			}
		}
		routes, err = stampConnectExecutionClaims(plan, routes)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		liveRoutes, err = stampConnectExecutionClaims(plan, liveRoutes)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if action == templateInstanceLifecycleActionCreated {
			refreshed, err := r.descriptorsForPlans(ctx, matched)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
			descriptors = refreshed
		}
		if len(routes) == 0 {
			out.Failure = runtimepinrouting.FailureTargetNotSubscribed
			out.ExtraDetail["connect_route_plan_failure"] = runtimepinrouting.FailureTargetNotSubscribed.Code()
			out.ExtraDetail["connect_route_plan_source_event"] = plan.SourceEndpoint().Readback().ResolvedEvent
			out.ExtraDetail["connect_route_plan_receiver_event"] = plan.ReceiverEndpoint().Readback().ResolvedEvent
			return out, nil
		}
		if err := receiverPinAdmission.Admit(plan, routes); err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if pins, collision := connectReceiverPinCollisionDetail(receiverPinAdmission); collision {
			out.Failure = connectRoutePlanTargetFailure(runtimepinrouting.ConnectFailureDeliveryTopologyInvalid)
			out.ExtraDetail["connect_route_plan_failure"] = runtimepinrouting.ConnectFailureDeliveryTopologyInvalid.Code()
			out.ExtraDetail["connect_route_plan_receiver_pin_collision"] = pins
			return out, nil
		}
		out.DeliveryIntents = append(out.DeliveryIntents, connectRoutePlanDeliveryIntents(routes, liveRoutes, routeCreatedInPlan)...)
		out.LiveRecipients = append(out.LiveRecipients, connectRoutePlanLiveRecipients(liveRoutes)...)
		out.RoutedRecipients = append(out.RoutedRecipients, subscribers...)
	}
	out.LiveRecipients = normalizeRoutePlanLiveRecipients(out.LiveRecipients)
	out.DeliveryIntents = normalizeRoutePlanDeliveryIntents(out.DeliveryIntents)
	out.RoutedRecipients = dedupeSubscribers(out.RoutedRecipients)
	out.ReplyContextConsumed = replyContextConsumed
	return out, nil
}

func stampConnectExecutionClaims(plan runtimepinrouting.ConnectRoutePlan, routes []events.DeliveryRoute) ([]events.DeliveryRoute, error) {
	for index := range routes {
		claim, err := runtimepinrouting.ConnectExecutionClaim(plan, routes[index])
		if err != nil {
			return nil, err
		}
		routes[index].ConnectClaim = claim
	}
	return events.NormalizeDeliveryRoutes(routes), nil
}

func connectReceiverPinCollisionDetail(admission runtimepinrouting.ConnectReceiverPinAdmission) ([]string, bool) {
	collisions := admission.Collisions()
	if len(collisions) == 0 {
		return nil, false
	}
	return collisions[0].ReceiverPinDiagnostics(), true
}

func (r connectRoutePlanResolver) materializeReplyRequest(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, routes []events.DeliveryRoute, values map[string]string) ([]events.DeliveryRoute, *runtimereplycontext.Record, error) {
	if plan.ReplyRole() != runtimepinrouting.ConnectReplyRoleRequest {
		return routes, nil, nil
	}
	origin := evt.SourceRoute().Normalized()
	now := evt.CreatedAt()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	record, err := plan.ReplyRequestRecord(evt, origin, runtimepinrouting.AdmitConnectRouteMatchValues(values), now)
	if err != nil {
		return nil, nil, err
	}
	if r.replyStore == nil {
		return nil, nil, fmt.Errorf("ReplyContextStore is required for resolution mode reply")
	}
	deliveryContext := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}}
	for i := range routes {
		routes[i].Context = deliveryContext
	}
	return events.NormalizeDeliveryRoutes(routes), &record, nil
}

func (r connectRoutePlanResolver) materializeReplyResponse(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, values map[string]string) ([]events.DeliveryRoute, []Subscriber, *runtimereplycontext.ClaimCommand, runtimepinrouting.TargetFailure, map[string]any, error) {
	reply := plan.ReplyResolution().Readback()
	contextID := events.DeliveryContextFromContext(ctx).ReplyContextID()
	detail := map[string]any{
		"connect_route_plan_resolution_mode": "reply",
		"connect_route_plan_request_pin":     reply.RequesterFlowID + "." + reply.RequestOutputPin,
		"connect_route_plan_reply_pin":       reply.RequesterFlowID + "." + reply.ReplyInputPin,
	}
	if contextID == "" || r.replyStore == nil {
		detail["connect_route_plan_failure"] = runtimepinrouting.FailureStaleArrival.Code()
		return nil, nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
	}
	record, err := r.replyStore.LoadReplyContext(ctx, contextID)
	if err != nil {
		if errors.Is(err, runtimereplycontext.ErrNotFound) {
			detail["connect_route_plan_failure"] = runtimepinrouting.FailureStaleArrival.Code()
			return nil, nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
		}
		return nil, nil, nil, 0, nil, err
	}
	if !plan.MatchesReplyRecord(record) {
		detail["connect_route_plan_failure"] = runtimepinrouting.FailureStaleArrival.Code()
		return nil, nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
	}
	if key := record.CorrelationKey; key != "" {
		actual, present := plan.ReplyResponseCorrelation(runtimepinrouting.AdmitConnectRouteMatchValues(values))
		if !present || actual != record.RequestCorrelationID {
			detail["connect_route_plan_failure"] = runtimepinrouting.FailureStaleArrival.Code()
			detail["reply_context_id"] = contextID
			detail["reply_correlation_key"] = key
			detail["request_correlation_id"] = record.RequestCorrelationID
			if actual != "" {
				detail["reply_correlation_id"] = actual
			}
			return nil, nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
		}
	}
	target := record.Origin.Normalized()
	subscribers := r.resolveSelectedReceiverCarriers(ctx, plan, target)
	if target.Empty() || len(subscribers) == 0 {
		detail["connect_route_plan_failure"] = runtimepinrouting.FailureStaleArrival.Code()
		detail["reply_origin"] = target
		return nil, nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
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
	}
	if outcome == runtimereplycontext.ClaimTerminal {
		detail["connect_route_plan_failure"] = runtimepinrouting.FailureReplyAlreadyTerminal.Code()
		detail["accepted_reply_event_id"] = claimed.AcceptedReplyEventID
		return nil, nil, nil, runtimepinrouting.FailureReplyAlreadyTerminal, detail, nil
	}
	claim := runtimereplycontext.ClaimCommand{Expected: record, ReplyEventID: evt.ID()}.Normalized()
	if err := claim.Validate(); err != nil {
		return nil, nil, nil, 0, nil, err
	}
	routes := make([]events.DeliveryRoute, 0, len(subscribers))
	for _, subscriber := range subscribers {
		identity, _, err := r.resolveAgentCarrierIdentity(ctx, subscriber, target, TemplateInstanceLifecycleDecision{}, false)
		if err != nil {
			return nil, nil, nil, 0, nil, err
		}
		routes = append(routes, events.DeliveryRoute{
			Recipient:     subscriber.Recipient,
			AgentIdentity: identity,
			Target:        target,
		})
	}
	detail["reply_context_id"] = contextID
	detail["request_event_id"] = record.RequestEventID
	detail["request_correlation_id"] = record.RequestCorrelationID
	detail["reply_claim_outcome"] = outcome
	return events.NormalizeDeliveryRoutes(routes), dedupeSubscribers(subscribers), &claim, 0, detail, nil
}

func (r connectRoutePlanResolver) materializeConnectRoutePlan(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, values map[string]string, descriptors []runtimepinrouting.Descriptor) (runtimepinrouting.ConnectRoutePlanMaterialization, TemplateInstanceLifecycleDecision, error) {
	if plan.ReceiverEndpoint().IsRoot() {
		target := evt.TargetRoute().Normalized()
		if target.EntityID == "" {
			return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetUnresolved}, TemplateInstanceLifecycleDecision{}, nil
		}
		rootFlowID := strings.TrimSpace(r.source.WorkflowName())
		rootInstance := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
		if rootFlowID == "" || rootInstance == "" {
			return runtimepinrouting.ConnectRoutePlanMaterialization{}, TemplateInstanceLifecycleDecision{}, errors.New("root connect receiver requires canonical workflow identity")
		}
		if target.FlowID != "" && target.FlowID != rootFlowID {
			return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetUnresolved}, TemplateInstanceLifecycleDecision{}, nil
		}
		if target.FlowInstance != "" && target.FlowInstance != rootInstance {
			return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureTargetUnresolved}, TemplateInstanceLifecycleDecision{}, nil
		}
		target.FlowID = rootFlowID
		target.FlowInstance = rootInstance
		return runtimepinrouting.ConnectRoutePlanMaterialization{Target: target}, TemplateInstanceLifecycleDecision{}, nil
	}
	if materialized, decision, handled, err := r.lifecycle.Materialize(ctx, evt, plan, values, descriptors); handled || err != nil {
		return materialized, decision, err
	}
	return runtimepinrouting.MaterializeConnectRoutePlan(plan, runtimepinrouting.ConnectRoutePlanMaterializationInput{
		MatchValues: runtimepinrouting.AdmitConnectRouteMatchValues(values),
		Descriptors: descriptors,
	}), TemplateInstanceLifecycleDecision{}, nil
}

func (r connectRoutePlanResolver) installTemplateInstanceLifecyclePreview(ctx context.Context, decision TemplateInstanceLifecycleDecision) error {
	if decision.Action != templateInstanceLifecycleActionPreviewCreate {
		return nil
	}
	var preview *connectRoutePlanPreviewRoutes
	if ctx != nil {
		preview, _ = ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	}
	if preview == nil {
		return errors.New("connect route planning preview table is required before lifecycle materialization")
	}
	if preview.table == nil {
		table, err := DeriveRouteTable(r.source)
		if err != nil {
			return fmt.Errorf("derive connect route planning preview table: %w", err)
		}
		preview.table = table
	}
	identity := decision.Route()
	if !identity.Valid() {
		return nil
	}
	if len(preview.table.MaterializedRoutes(identity)) > 0 {
		return nil
	}
	if err := preview.table.AddFlowInstanceRoute(FlowInstanceRouteMaterializationRequest{
		Identity:            identity,
		ActivationVariables: decision.ActivationVariables(),
	}); err != nil {
		return err
	}
	return nil
}

func (r connectRoutePlanResolver) matchedPlans(ctx context.Context, evt events.Event) []runtimepinrouting.ConnectRoutePlan {
	if admission, ok := publicInputAdmissionFromContext(ctx); ok {
		if admission.validateEvent(evt) == nil {
			return []runtimepinrouting.ConnectRoutePlan{admission.plan}
		}
		return nil
	}
	candidates := r.graph.MatchingPlans(evt)
	out := make([]runtimepinrouting.ConnectRoutePlan, 0, len(candidates))
	for _, plan := range candidates {
		if providerOutputAuthorizationMatches(ctx, plan.ProviderOutputAuthorization()) {
			out = append(out, plan)
		}
	}
	return out
}

func (r connectRoutePlanResolver) descriptorsForPlans(ctx context.Context, plans []runtimepinrouting.ConnectRoutePlan) ([]runtimepinrouting.Descriptor, error) {
	needsDescriptors := false
	for _, plan := range plans {
		if plan.RequiresRuntimeResolution() {
			needsDescriptors = true
			break
		}
	}
	if !needsDescriptors || r.loadDescriptors == nil {
		return nil, nil
	}
	return r.loadDescriptors(ctx)
}

func (r connectRoutePlanResolver) deliveryRoutesForMaterialization(ctx context.Context, plan runtimepinrouting.ConnectRoutePlan, materialized runtimepinrouting.ConnectRoutePlanMaterialization, decision TemplateInstanceLifecycleDecision, routeCreatedInPlan bool) ([]events.DeliveryRoute, []events.DeliveryRoute, []Subscriber, error) {
	targets := connectMaterializedTargets(materialized)
	if plan.ReceiverEndpoint().IsRoot() && len(targets) == 0 {
		targets = []events.RouteIdentity{{}}
	}
	if len(targets) == 0 {
		return nil, nil, nil, nil
	}
	projection, err := syntheticDeliveryPayloadProjection(plan, decision)
	if err != nil {
		return nil, nil, nil, err
	}
	routes := make([]events.DeliveryRoute, 0, len(targets))
	liveRoutes := make([]events.DeliveryRoute, 0, len(targets))
	subscribers := make([]Subscriber, 0, len(targets))
	for _, target := range targets {
		target = target.Normalized()
		selectionTarget := target
		if plan.ReceiverEndpoint().IsRoot() {
			// Root receiver registration is pin-owned and has no instance path.
			// Keep exact execution identity on the delivery route while matching
			// the registered root carrier through its entity-bearing pin target.
			selectionTarget = events.RouteIdentity{EntityID: target.EntityID}
		}
		matchedSubscribers := r.resolveSelectedReceiverCarriers(ctx, plan, selectionTarget)
		if len(matchedSubscribers) == 0 {
			return nil, nil, nil, nil
		}
		subscribers = append(subscribers, matchedSubscribers...)
		for _, subscriber := range matchedSubscribers {
			identity, live, err := r.resolveAgentCarrierIdentity(ctx, subscriber, target, decision, routeCreatedInPlan)
			if err != nil {
				return nil, nil, nil, err
			}
			route := events.DeliveryRoute{
				Recipient:         subscriber.Recipient,
				AgentIdentity:     identity,
				Target:            target,
				PayloadProjection: projection,
			}
			routes = append(routes, route)
			if live {
				liveRoutes = append(liveRoutes, route)
			}
		}
	}
	return events.NormalizeDeliveryRoutes(routes), events.NormalizeDeliveryRoutes(liveRoutes), dedupeSubscribers(subscribers), nil
}

func (r connectRoutePlanResolver) resolveAgentCarrierIdentity(
	ctx context.Context,
	subscriber Subscriber,
	target events.RouteIdentity,
	decision TemplateInstanceLifecycleDecision,
	routeCreatedInPlan bool,
) (agentidentity.Identity, bool, error) {
	if !subscriber.Recipient.IsAgent() {
		return agentidentity.Identity{}, true, nil
	}
	agentID := subscriber.Recipient.ID()
	matches := make([]agentidentity.Identity, 0, 1)
	available := false
	if r.loadAgents != nil {
		descriptors, loaded, err := r.loadAgents(ctx)
		if err != nil {
			return agentidentity.Identity{}, false, err
		}
		available = loaded
		if loaded {
			for identity, descriptor := range descriptors {
				identity = identity.Normalize()
				if identity.AgentID() != agentID || descriptor.Identity.Normalize() != identity {
					continue
				}
				if target.Empty() {
					if identity.Route.Presence != agentidentity.RouteRoot {
						continue
					}
				} else if !routeMatchesAgentDescriptor(target, descriptor) {
					continue
				}
				matches = append(matches, identity)
			}
			sort.Slice(matches, func(left, right int) bool {
				return agentidentity.Less(matches[left], matches[right])
			})
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], true, nil
	case 0:
		if identity, planned, err := plannedCreateAgentCarrierIdentity(subscriber, target, decision, routeCreatedInPlan); planned || err != nil {
			return identity, false, err
		}
		if r.loadAgents == nil {
			return agentidentity.Identity{}, false, errors.New("connect agent carrier requires the canonical active-agent identity owner")
		}
		if !available {
			return agentidentity.Identity{}, false, fmt.Errorf(
				"connect agent carrier identity is unavailable for lifecycle action %q and route %q",
				templateInstanceLifecycleActionCode(decision.Action),
				subscriber.AgentIdentity.FlowInstance(),
			)
		}
		return agentidentity.Identity{}, false, fmt.Errorf("connect agent carrier %q has no live identity for target %#v", agentID, target.Normalized())
	default:
		candidates := make([]string, 0, len(matches))
		for _, identity := range matches {
			candidates = append(candidates, identity.Description())
		}
		return agentidentity.Identity{}, false, fmt.Errorf("connect agent carrier %q is ambiguous; candidates: %s", agentID, strings.Join(candidates, ", "))
	}
}

func plannedCreateAgentCarrierIdentity(
	subscriber Subscriber,
	target events.RouteIdentity,
	decision TemplateInstanceLifecycleDecision,
	routeCreatedInPlan bool,
) (agentidentity.Identity, bool, error) {
	if !routeCreatedInPlan {
		return agentidentity.Identity{}, false, nil
	}
	identity := subscriber.AgentIdentity.Normalize()
	if err := identity.Validate(); err != nil {
		return agentidentity.Identity{}, true, fmt.Errorf("created connect agent carrier %q has no canonical declared identity: %w", subscriber.Recipient.ID(), err)
	}
	if identity.AgentID() != subscriber.Recipient.ID() {
		return agentidentity.Identity{}, true, fmt.Errorf("created connect agent carrier %q identity names %q", subscriber.Recipient.ID(), identity.AgentID())
	}
	route := decision.Route()
	if !route.Valid() {
		return agentidentity.Identity{}, true, fmt.Errorf("created connect agent carrier %q has no canonical flow route", subscriber.Recipient.ID())
	}
	expectedRoute, err := route.AgentIdentityRoute()
	if err != nil {
		return agentidentity.Identity{}, true, fmt.Errorf("created connect agent carrier %q flow route: %w", subscriber.Recipient.ID(), err)
	}
	if identity.Route != expectedRoute {
		return agentidentity.Identity{}, true, fmt.Errorf(
			"created connect agent carrier %q identity route %q does not match lifecycle route %q",
			subscriber.Recipient.ID(),
			identity.FlowInstance(),
			route.InstancePath,
		)
	}
	if !target.Empty() && !routeMatchesAgentDescriptor(target, ActiveAgentDescriptor{Identity: identity, EntityID: target.EntityID}) {
		return agentidentity.Identity{}, true, fmt.Errorf("created connect agent carrier %q identity does not match target %#v", subscriber.Recipient.ID(), target.Normalized())
	}
	return identity, true, nil
}

func syntheticDeliveryPayloadProjection(plan runtimepinrouting.ConnectRoutePlan, decision TemplateInstanceLifecycleDecision) (events.DeliveryPayloadProjection, error) {
	if plan.InstanceKey() == nil || !plan.InstanceKey().RequiresDeliveryProjection() {
		return events.DeliveryPayloadProjection{}, nil
	}
	receiver := plan.ReceiverEndpoint().Readback()
	if decision.Empty() {
		return events.DeliveryPayloadProjection{}, fmt.Errorf("create resolution for %s requires lifecycle key material before delivery route construction", receiver.FlowID)
	}
	fields := make(map[string]string, len(decision.KeyMaterial))
	for _, key := range decision.KeyMaterial {
		fields[key.Field.Path()] = key.Value
	}
	projection, err := events.NewDeliveryPayloadProjection(fields)
	if err != nil {
		return events.DeliveryPayloadProjection{}, fmt.Errorf("create resolution for %s produced invalid synthetic carry material: %w", receiver.FlowID, err)
	}
	return projection, nil
}

func (r connectRoutePlanResolver) resolveSelectedReceiverCarriers(ctx context.Context, plan runtimepinrouting.ConnectRoutePlan, target events.RouteIdentity) []Subscriber {
	tables := []*RouteTable{r.routeTable}
	if ctx != nil {
		if preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes); preview != nil && preview.table != nil {
			tables = append(tables, preview.table)
		}
	}
	if len(tables) == 0 {
		return nil
	}
	out := make([]Subscriber, 0)
	for _, routeTable := range tables {
		if routeTable == nil {
			continue
		}
		evaluation := routeTable.evaluateConnectPlan(plan, []events.RouteIdentity{target})
		out = append(out, connectRecipientSubscribers(evaluation)...)
	}
	return dedupeSubscribers(out)
}

func connectRoutePlanMatchesEvent(ctx context.Context, plan runtimepinrouting.ConnectRoutePlan, evt events.Event) bool {
	return (runtimepinrouting.CompiledConnectGraph{}).PlanMatchesEvent(plan, evt) && providerOutputAuthorizationMatches(ctx, plan.ProviderOutputAuthorization())
}

func connectMaterializedTargets(materialized runtimepinrouting.ConnectRoutePlanMaterialization) []events.RouteIdentity {
	if !materialized.Target.Empty() {
		return []events.RouteIdentity{materialized.Target.Normalized()}
	}
	return uniqueRouteIdentities(materialized.TargetSet)
}

func connectRoutePlanLiveRecipients(routes []events.DeliveryRoute) []RoutePlanLiveRecipient {
	routes = events.NormalizeDeliveryRoutes(routes)
	if len(routes) == 0 {
		return nil
	}
	out := make([]RoutePlanLiveRecipient, 0, len(routes))
	for _, route := range routes {
		if route.Recipient.Empty() {
			continue
		}
		out = append(out, RoutePlanLiveRecipient{
			Recipient:         route.Recipient,
			AgentIdentity:     route.AgentIdentity,
			PersistAsDelivery: route.Recipient.IsAgent(),
			Producer:          routeIntentProducerConnectRoutePlan,
		})
	}
	return normalizeRoutePlanLiveRecipients(out)
}

func connectRoutePlanDeliveryIntents(routes, liveRoutes []events.DeliveryRoute, routeCreatedInPlan bool) []RoutePlanDeliveryIntent {
	intents := routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerConnectRoutePlan)
	liveAgents := make(map[agentidentity.Identity]struct{}, len(liveRoutes))
	for _, route := range events.NormalizeDeliveryRoutes(liveRoutes) {
		if route.Recipient.IsAgent() {
			liveAgents[route.AgentIdentity] = struct{}{}
		}
	}
	for index := range intents {
		intent := &intents[index]
		if !intent.Recipient.IsAgent() {
			continue
		}
		if _, live := liveAgents[intent.AgentIdentity]; !live && routeCreatedInPlan {
			intent.PendingAgentLifecycle = true
		}
	}
	return intents
}

func connectRoutePlanTargetFailure(failure runtimepinrouting.ConnectRoutePlanFailure) runtimepinrouting.TargetFailure {
	if failure.Empty() {
		return 0
	}
	return runtimepinrouting.TargetFailureFromConnect(failure)
}

func connectRoutePlanFailureDetail(plan runtimepinrouting.ConnectRoutePlan, failure runtimepinrouting.ConnectRoutePlanFailure, values map[string]string, descriptors []runtimepinrouting.Descriptor) map[string]any {
	if plan.InstanceKey() == nil {
		return nil
	}
	mode := plan.InstanceKey().Mode()
	if mode != runtimecontracts.FlowInputResolutionModeSelect && mode != runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		return nil
	}
	keyField := plan.InstanceKey().Field().Path()
	if keyField == "" {
		return nil
	}
	out := map[string]any{
		"connect_route_plan_resolution_mode":     runtimecontracts.FlowInputResolutionModeCode(mode),
		"connect_route_plan_receiver_flow":       plan.ReceiverEndpoint().Readback().FlowID,
		"connect_route_plan_instance_key_field":  keyField,
		"connect_route_plan_failure_remediation": connectRoutePlanInstanceResolutionRemediation(plan, failure, keyField, "", mode),
	}
	material, materialFailure := runtimepinrouting.InstanceKeyMaterialForConnectRoutePlan(plan, runtimepinrouting.AdmitConnectRouteMatchValues(values))
	if !materialFailure.Empty() {
		if failure == runtimepinrouting.ConnectFailureInstanceSourceValueMissing {
			out["connect_route_plan_failure_remediation"] = connectRoutePlanInstanceResolutionRemediation(plan, failure, keyField, "", mode)
		}
		return out
	}
	keyValue := ""
	for _, key := range material.Keys {
		if key.Field.Path() == keyField {
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

func connectRoutePlanInstanceResolutionRemediation(plan runtimepinrouting.ConnectRoutePlan, failure runtimepinrouting.ConnectRoutePlanFailure, keyField, keyValue string, mode runtimecontracts.FlowInputResolutionMode) string {
	receiverFlow := plan.ReceiverEndpoint().Readback().FlowID
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
	sourcePath := ""
	if plan.InstanceKey() != nil {
		sourcePath = plan.InstanceKey().Readback().SourcePath
	}
	if sourcePath == "" {
		sourcePath = "the authored instance-key source"
	}
	switch failure {
	case runtimepinrouting.ConnectFailureInstanceSourceValueMissing:
		return fmt.Sprintf("Provide %s before publishing to %s; resolution mode %s requires a carried key value.", sourcePath, receiverFlow, runtimecontracts.FlowInputResolutionModeCode(mode))
	case runtimepinrouting.ConnectFailureTargetAmbiguous:
		return fmt.Sprintf("Ensure exactly one active %s instance has %s%s; resolution mode %s cannot choose between multiple matches.", receiverFlow, keyLabel, valueText, runtimecontracts.FlowInputResolutionModeCode(mode))
	default:
		if mode == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
			return fmt.Sprintf("Ensure %s can create or reuse exactly one active instance with %s%s; resolution mode %s must converge on one instance.", receiverFlow, keyLabel, valueText, runtimecontracts.FlowInputResolutionModeCode(mode))
		}
		return fmt.Sprintf("Create or connect exactly one active %s instance with %s%s before publishing; resolution mode %s never creates a missing instance.", receiverFlow, keyLabel, valueText, runtimecontracts.FlowInputResolutionModeCode(mode))
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
