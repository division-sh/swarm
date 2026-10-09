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
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type connectAgentDescriptorLoader func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error)

type connectRoutePlanPreviewRoutesKey struct{}
type closedPublicationPlanningKey struct{}

type connectRoutePlanPreviewRoutes struct {
	table          *RouteTable
	inputProducers *runtimepinrouting.FlowInputProducerResolver
	selected       map[string][]runtimeflowidentity.Instance
	plans          []runtimepipeline.FlowInstanceActivationPlan
}

func withConnectRoutePlanPreview(ctx context.Context) context.Context {
	if preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes); preview != nil {
		return ctx
	}
	return context.WithValue(ctx, connectRoutePlanPreviewRoutesKey{}, &connectRoutePlanPreviewRoutes{})
}

type staleConnectRoutePlanSnapshotError struct{}

func (staleConnectRoutePlanSnapshotError) Error() string {
	return "connect route snapshot generation is stale"
}

func exhaustedConnectRoutePlanSnapshotError() error {
	return runtimefailures.Wrap(
		runtimefailures.ClassDependencyUnavailable,
		"connect_route_snapshot_stale",
		"eventbus",
		"plan_connect_routes",
		map[string]any{"reason": "route_table_generation_changed"},
		staleConnectRoutePlanSnapshotError{},
	)
}

func withClosedPublicationPlanning(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, closedPublicationPlanningKey{}, true)
}

type connectRoutePlanResolver struct {
	source     semanticview.Source
	routeTable *RouteTable
	graph      runtimepinrouting.CompiledConnectGraph
	issues     []runtimepinrouting.ConnectRoutePlanIssue
	loadAgents connectAgentDescriptorLoader
	lifecycle  connectInstanceSelector
	replyStore runtimereplycontext.Store
}

type connectRoutePlanDispatch struct {
	Matched              bool
	Failure              runtimepinrouting.TargetFailure
	Evaluation           events.ConnectEvaluationLedger
	LiveRecipients       []RoutePlanLiveRecipient
	DeliveryIntents      []RoutePlanDeliveryIntent
	RoutedRecipients     []Subscriber
	ExtraDetail          map[string]any
	ReplyContextConsumed bool
	ActivationPlans      []runtimepipeline.FlowInstanceActivationPlan
	ReplyCreations       []runtimereplycontext.Record
	ReplyClaims          []runtimereplycontext.ClaimCommand
}

func newConnectRoutePlanResolver(source semanticview.Source, routeTable *RouteTable, planner runtimepipeline.FlowInstanceActivationPlanner, index runtimepipeline.FlowInstanceIndexReader, replyStore runtimereplycontext.Store) connectRoutePlanResolver {
	if source == nil {
		return connectRoutePlanResolver{routeTable: routeTable, replyStore: replyStore}
	}
	graph := runtimepinrouting.CompileConnectGraph(source)
	issues := graph.Issues()
	return connectRoutePlanResolver{
		source:     source,
		routeTable: routeTable,
		graph:      graph,
		issues:     append([]runtimepinrouting.ConnectRoutePlanIssue(nil), issues...),
		lifecycle:  connectInstanceSelector{source: source, plan: planner, index: index},
		replyStore: replyStore,
	}
}

func (r connectRoutePlanResolver) Plan(ctx context.Context, evt events.Event) (connectRoutePlanDispatch, error) {
	if evt.RoutingSource().Kind() == events.RoutingSourceDeploymentFeed {
		if _, err := runtimepinrouting.AdmitDeploymentFeedDeclaration(r.source, evt.Type(), evt.RoutingSource()); err != nil {
			return connectRoutePlanDispatch{}, err
		}
	}
	emptyEvaluation, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		return connectRoutePlanDispatch{}, err
	}
	if len(r.graph.Plans()) == 0 && len(r.issues) == 0 {
		return connectRoutePlanDispatch{Evaluation: emptyEvaluation}, nil
	}
	{
		for _, issue := range r.issues {
			if r.graph.IssueMatchesEvent(issue, evt) && issue.AcceptsReceiverTarget(explicitRootPublicationTarget(evt), evt.RunID()) && providerOutputAuthorizationMatches(ctx, issue.ProviderOutputAuthorization()) {
				return connectRoutePlanDispatch{
					Matched:    true,
					Failure:    connectRoutePlanTargetFailure(issue.Failure),
					Evaluation: emptyEvaluation,
					ExtraDetail: map[string]any{
						"connect_route_plan_failure": issue.Failure.Code(),
						"connect_route_plan_detail":  strings.TrimSpace(issue.Detail),
					},
				}, nil
			}
		}
	}

	matched := r.matchedPlans(ctx, evt)
	if len(matched) == 0 {
		return connectRoutePlanDispatch{Evaluation: emptyEvaluation}, nil
	}
	matched, replyRecord, err := r.resolveReplyPlans(ctx, matched)
	if err != nil {
		return connectRoutePlanDispatch{}, err
	}
	evaluationCtx := runtimecorrelation.WithInboundEvent(ctx, evt)
	evaluationCtx = withConnectPlanningPreview(evaluationCtx)
	evaluationCtx = withConnectRoutePlanPreview(evaluationCtx)
	return r.planMatched(evaluationCtx, evt, matched, connectRoutePlanMatchValues(evt), replyRecord)
}

// A paired response resumes retained return authority, not every ordinary edge
// that happens to match its event. Missing authority still reaches typed refusal.
func (r connectRoutePlanResolver) resolveReplyPlans(ctx context.Context, matched []runtimepinrouting.ConnectRoutePlan) ([]runtimepinrouting.ConnectRoutePlan, runtimereplycontext.Record, error) {
	var responses []runtimepinrouting.ConnectRoutePlan
	for _, plan := range matched {
		if plan.ReplyRole() == runtimepinrouting.ConnectReplyRoleResponse {
			responses = append(responses, plan)
		}
	}
	if len(responses) == 0 {
		return matched, runtimereplycontext.Record{}, nil
	}
	id := events.DeliveryContextFromContext(ctx).ReplyContextID()
	if id == "" || r.replyStore == nil {
		return responses, runtimereplycontext.Record{}, nil
	}
	record, err := r.replyStore.LoadReplyContext(ctx, id)
	if errors.Is(err, runtimereplycontext.ErrNotFound) {
		return responses, runtimereplycontext.Record{}, nil
	}
	if err != nil {
		return nil, runtimereplycontext.Record{}, err
	}
	var exact []runtimepinrouting.ConnectRoutePlan
	for _, plan := range responses {
		if plan.MatchesReplyRecord(record) {
			exact = append(exact, plan)
		}
	}
	if len(exact) == 0 {
		return responses, record, nil
	}
	return exact, record, nil
}

func (r connectRoutePlanResolver) planMatched(ctx context.Context, evt events.Event, matched []runtimepinrouting.ConnectRoutePlan, values map[string]string, replyRecord runtimereplycontext.Record) (connectRoutePlanDispatch, error) {
	emptyEvaluation, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		return connectRoutePlanDispatch{}, err
	}
	out := connectRoutePlanDispatch{
		Matched:    true,
		Evaluation: emptyEvaluation,
		ExtraDetail: map[string]any{
			"connect_route_plans_count": len(matched),
		},
	}
	var receiverPinAdmission runtimepinrouting.ConnectReceiverPinAdmission
	createdRoutes := make(map[runtimeflowidentity.Route]struct{}, len(matched))
	replyContextConsumed := false
	for _, plan := range matched {
		if plan.ReplyResolution() != nil && plan.ReplyResolution().Role() == runtimepinrouting.ConnectReplyRoleResponse {
			routes, subscribers, claim, failure, detail, err := r.materializeReplyResponse(ctx, evt, plan, values, replyRecord)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
			if !failure.Empty() {
				if err := r.appendBlockedPlanEvaluation(&out, plan, nil); err != nil {
					return connectRoutePlanDispatch{}, err
				}
				out.Failure = failure
				for key, value := range detail {
					out.ExtraDetail[key] = value
				}
				return out, nil
			}
			targets := make([]events.RouteIdentity, 0, len(routes))
			for _, route := range routes {
				targets = append(targets, route.Target)
			}
			if err := r.appendMaterializedPlanEvaluation(ctx, evt.RunID(), &out, plan, targets); err != nil {
				return connectRoutePlanDispatch{}, err
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
			receiverEvent := plan.ReceiverLocalEvent()
			intents, err := routePlanDeliveryIntentsFromConnectRoutes(evt.RunID(), routes, routeIntentProducerConnectRoutePlan, receiverEvent)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
			liveRecipients, err := connectRoutePlanLiveRecipients(evt.RunID(), routes)
			if err != nil {
				return connectRoutePlanDispatch{}, err
			}
			out.DeliveryIntents = append(out.DeliveryIntents, intents...)
			out.LiveRecipients = append(out.LiveRecipients, liveRecipients...)
			out.RoutedRecipients = append(out.RoutedRecipients, subscribers...)
			continue
		}
		materialized, decision, err := r.materializeConnectRoutePlan(ctx, evt, plan, values)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if !materialized.Failure.Empty() {
			if err := r.appendBlockedPlanEvaluation(&out, plan, connectMaterializedTargets(materialized)); err != nil {
				return connectRoutePlanDispatch{}, err
			}
			out.Failure = connectRoutePlanTargetFailure(materialized.Failure)
			out.ExtraDetail["connect_route_plan_failure"] = materialized.Failure.Code()
			out.ExtraDetail["connect_route_plan_source_event"] = plan.SourceEndpoint().Readback().ResolvedEvent
			out.ExtraDetail["connect_route_plan_receiver_event"] = plan.ReceiverEndpoint().Readback().ResolvedEvent
			for key, value := range connectRoutePlanFailureDetail(plan, materialized.Failure, values) {
				out.ExtraDetail[key] = value
			}
			return out, nil
		}
		if target := explicitRootPublicationTarget(evt); !target.Empty() {
			var selected bool
			materialized, selected = materialized.SelectReceiverTarget(target)
			if !selected {
				continue
			}
		}
		if !decision.Empty() {
			out.ExtraDetail["connect_route_plan_template_instance_lifecycle"] = decision.Detail()
		}
		if decision.Activation != nil {
			out.ActivationPlans = append(out.ActivationPlans, *decision.Activation)
		}
		if decision.Activation != nil {
			if err := r.installFlowConstructionPreview(ctx, evt.RunID(), *decision.Activation); err != nil {
				return connectRoutePlanDispatch{}, err
			}
		}
		if !decision.Empty() {
			if err := selectConnectionConstruction(ctx, decision.identity); err != nil {
				return connectRoutePlanDispatch{}, err
			}
		}
		if decision.Activation != nil {
			if route := decision.Route(); route.Valid() {
				createdRoutes[route] = struct{}{}
			}
		}
		_, routeCreatedInPlan := createdRoutes[decision.Route()]
		routes, liveRoutes, subscribers, evaluation, err := r.deliveryRoutesForMaterialization(ctx, evt.RunID(), plan, materialized, decision, routeCreatedInPlan)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		if err := mergeConnectEvaluation(&out.Evaluation, evaluation); err != nil {
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
		if len(routes) == 0 {
			out.ExtraDetail["connect_route_plan_source_event"] = plan.SourceEndpoint().Readback().ResolvedEvent
			out.ExtraDetail["connect_route_plan_receiver_event"] = plan.ReceiverEndpoint().Readback().ResolvedEvent
			continue
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
		intents, err := connectRoutePlanDeliveryIntents(evt.RunID(), plan, routes, liveRoutes, routeCreatedInPlan, r.routeTable.staticAgentDeclarationPlans())
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		out.DeliveryIntents = append(out.DeliveryIntents, intents...)
		liveRecipients, err := connectRoutePlanLiveRecipients(evt.RunID(), liveRoutes)
		if err != nil {
			return connectRoutePlanDispatch{}, err
		}
		out.LiveRecipients = append(out.LiveRecipients, liveRecipients...)
		out.RoutedRecipients = append(out.RoutedRecipients, subscribers...)
	}
	out.LiveRecipients = normalizeRoutePlanLiveRecipients(out.LiveRecipients)
	out.DeliveryIntents = normalizeRoutePlanDeliveryIntents(out.DeliveryIntents)
	out.RoutedRecipients = dedupeSubscribers(out.RoutedRecipients)
	out.ReplyContextConsumed = replyContextConsumed
	return out, nil
}

func stampConnectExecutionClaims(plan runtimepinrouting.ConnectRoutePlan, routes []runtimepinrouting.ConnectDeliveryRoute) ([]runtimepinrouting.ConnectDeliveryRoute, error) {
	for index := range routes {
		claim, err := runtimepinrouting.ConnectExecutionClaim(plan, routes[index])
		if err != nil {
			return nil, err
		}
		routes[index].ConnectClaim = claim
	}
	return runtimepinrouting.NormalizeConnectDeliveryRoutes(routes), nil
}

func connectReceiverPinCollisionDetail(admission runtimepinrouting.ConnectReceiverPinAdmission) ([]string, bool) {
	collisions := admission.Collisions()
	if len(collisions) == 0 {
		return nil, false
	}
	return collisions[0].ReceiverPinDiagnostics(), true
}

func (r connectRoutePlanResolver) materializeReplyRequest(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, routes []runtimepinrouting.ConnectDeliveryRoute, values map[string]string) ([]runtimepinrouting.ConnectDeliveryRoute, *runtimereplycontext.Record, error) {
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
	return runtimepinrouting.NormalizeConnectDeliveryRoutes(routes), &record, nil
}

func (r connectRoutePlanResolver) materializeReplyResponse(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, values map[string]string, record runtimereplycontext.Record) ([]runtimepinrouting.ConnectDeliveryRoute, []Subscriber, *runtimereplycontext.ClaimCommand, runtimepinrouting.TargetFailure, map[string]any, error) {
	reply := plan.ReplyResolution().Readback()
	contextID := events.DeliveryContextFromContext(ctx).ReplyContextID()
	detail := map[string]any{
		"connect_route_plan_resolution_mode": "reply",
		"connect_route_plan_request_pin":     reply.RequesterFlowID + "." + reply.RequestOutputPin,
		"connect_route_plan_reply_pin":       reply.RequesterFlowID + "." + reply.ReplyInputPin,
	}
	if contextID == "" || record.ID != contextID {
		detail["connect_route_plan_failure"] = runtimepinrouting.FailureStaleArrival.Code()
		return nil, nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
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
	subscribers, err := r.resolveSelectedReceiverCarriers(ctx, evt.RunID(), plan, target)
	if err != nil {
		return nil, nil, nil, 0, nil, err
	}
	if target.Empty() || len(subscribers) == 0 {
		detail["connect_route_plan_failure"] = runtimepinrouting.FailureStaleArrival.Code()
		detail["reply_origin"] = target
		return nil, nil, nil, runtimepinrouting.FailureStaleArrival, detail, nil
	}
	claimed := record
	outcome := runtimereplycontext.ClaimAccepted
	if connectPlanningPreview(ctx) {
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
	returnEvent := string(plan.ReceiverLocalEvent())
	routes := make([]runtimepinrouting.ConnectDeliveryRoute, 0, len(subscribers))
	for _, subscriber := range subscribers {
		identity, _, err := r.resolveAgentCarrierIdentity(ctx, subscriber, target, connectInstanceSelection{}, false)
		if err != nil {
			return nil, nil, nil, 0, nil, err
		}
		plan := agentidentity.Plan{}
		if subscriber.Recipient.IsAgent() {
			plan, err = identity.Plan()
			if err != nil {
				return nil, nil, nil, 0, nil, fmt.Errorf("project reply agent plan: %w", err)
			}
		}
		deliveryContext := events.DeliveryContext{}
		if node, isNode := subscriber.Recipient.Node(); isNode {
			for _, receipt := range record.ReturnJoins {
				if receipt.Ref.Node().Equal(node) {
					deliveryContext.Joins = append(deliveryContext.Joins, receipt)
				}
			}
			for _, joinPlan := range runtimepipeline.WorkflowJoinAdmissionPlans(r.source, node, returnEvent) {
				declaration, err := timeridentity.NewJoinRef(joinPlan.Node, joinPlan.HandlerEvent, joinPlan.Spec.Stage, joinPlan.Spec.EffectiveID())
				if err != nil {
					return nil, nil, nil, 0, nil, err
				}
				if _, present := deliveryContext.JoinAdmission(declaration); !present {
					return nil, nil, nil, 0, nil, fmt.Errorf("reply context %s is missing its retained join return admission", record.ID)
				}
			}
		}
		routes = append(routes, runtimepinrouting.ConnectDeliveryRoute{
			Recipient: subscriber.Recipient,
			AgentPlan: plan,
			Target:    target,
			Handler:   subscriber.connectHandler,
			Context:   deliveryContext,
		})
	}
	detail["reply_context_id"] = contextID
	detail["request_event_id"] = record.RequestEventID
	detail["request_correlation_id"] = record.RequestCorrelationID
	detail["reply_claim_outcome"] = outcome
	return runtimepinrouting.NormalizeConnectDeliveryRoutes(routes), dedupeSubscribers(subscribers), &claim, 0, detail, nil
}

func (r connectRoutePlanResolver) materializeConnectRoutePlan(ctx context.Context, evt events.Event, plan runtimepinrouting.ConnectRoutePlan, values map[string]string) (runtimepinrouting.ConnectRoutePlanMaterialization, connectInstanceSelection, error) {
	if plan.ReceiverEndpoint().IsRoot() || !plan.RequiresRuntimeResolution() {
		return r.materializeKeylessConnect(ctx, evt, plan)
	}
	if materialized, decision, handled, err := r.lifecycle.Materialize(ctx, evt, plan, values); handled || err != nil {
		return materialized, decision, err
	}
	return runtimepinrouting.ConnectRoutePlanMaterialization{Failure: runtimepinrouting.ConnectFailureReceiverResolutionMissing}, connectInstanceSelection{}, nil
}

func (r connectRoutePlanResolver) installFlowConstructionPreview(ctx context.Context, runID string, plan runtimepipeline.FlowInstanceActivationPlan) error {
	preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	if preview == nil {
		return fmt.Errorf("construction preparation requires its operation-local tree")
	}
	preview.plans = append(preview.plans, plan)
	for _, construction := range plan.ConstructionPlans() {
		if err := construction.Validate(); err != nil {
			return err
		}
		if err := r.installConstructionIdentityPreview(ctx, runID, construction.Identity, construction.ActivationVariables); err != nil {
			return err
		}
	}
	return nil
}

func (r connectRoutePlanResolver) installConstructionIdentityPreview(ctx context.Context, runID string, instance runtimeflowidentity.Instance, variables map[string]string) error {
	var preview *connectRoutePlanPreviewRoutes
	if ctx != nil {
		preview, _ = ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
	}
	if preview == nil {
		return errors.New("connect route planning preview table is required before lifecycle materialization")
	}
	if preview.table == nil {
		if r.routeTable == nil || !r.routeTable.compiledSourceReady {
			return errors.New("connect route preview requires paired compiled route source")
		}
		inputProducers := r.routeTable.inputProducers
		table, err := deriveRouteTableWithInputProducers(r.source, r.routeTable.connectGraph, inputProducers)
		if err != nil {
			return fmt.Errorf("derive connect route planning preview table: %w", err)
		}
		preview.table = table
		preview.inputProducers = &inputProducers
	}
	liveIdentity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, instance.Route())
	if err != nil {
		return fmt.Errorf("compose construction route planning preview identity: %w", err)
	}
	if len(preview.table.MaterializedRoutes(liveIdentity)) > 0 {
		return nil
	}
	if err := preview.table.addFlowInstanceRouteForContextWithInputProducers(ctx, FlowInstanceRouteMaterializationRequest{
		Identity: liveIdentity, Instance: instance,
		ActivationVariables: cloneRouteActivationVariables(variables),
	}, preview.inputProducers); err != nil {
		return err
	}
	return nil
}

func (r connectRoutePlanResolver) matchedPlans(ctx context.Context, evt events.Event) []runtimepinrouting.ConnectRoutePlan {
	candidates := r.graph.MatchingPlans(evt)
	out := make([]runtimepinrouting.ConnectRoutePlan, 0, len(candidates))
	target := explicitRootPublicationTarget(evt)
	for _, plan := range candidates {
		if plan.AcceptsReceiverTarget(target, evt.RunID()) && providerOutputAuthorizationMatches(ctx, plan.ProviderOutputAuthorization()) {
			out = append(out, plan)
		}
	}
	// Ancestor connections prepare/select their exact owner before descendant
	// connections consume it. Equal-depth plans retain compiled order.
	sort.SliceStable(out, func(i, j int) bool {
		return strings.Count(out[i].ReceiverEndpoint().Readback().FlowPath, "/") < strings.Count(out[j].ReceiverEndpoint().Readback().FlowPath, "/")
	})
	return out
}

// This scope supplies lookup candidates, never execution permission. Native
// headers supply actual paths; later target admission still checks ownership.
func (r connectRoutePlanResolver) selectedTargetScope(ctx context.Context, evt events.Event) (selectedTargetOwnerLookupScope, bool, error) {
	paths := make(map[string]struct{})
	scope := selectedTargetOwnerLookupScope{}
	add := func(route events.RouteIdentity) {
		if path := route.Normalized().FlowInstance; path != "" {
			paths[path] = struct{}{}
		}
	}
	add(evt.RoutingSource().Route())
	add(evt.SourceRoute())
	add(evt.TargetRoute())
	for _, route := range evt.TargetRoutes() {
		add(route)
	}
	if evt.RoutingSource().Kind() == events.RoutingSourceRoot || evt.RoutingSource().Kind() == events.RoutingSourceDeploymentFeed {
		paths["."] = struct{}{}
		if runID := strings.TrimSpace(evt.RunID()); runID != "" {
			paths[runID] = struct{}{}
		}
	}
	flows := make(map[string]struct{})
	for _, plan := range r.matchedPlans(ctx, evt) {
		flows[plan.ReceiverEndpoint().Readback().FlowID] = struct{}{}
	}
	if evt.RoutingSource().Kind() == events.RoutingSourceExternalIngress {
		flows[evt.RoutingSource().Route().FlowID] = struct{}{}
	}
	if len(flows) > 0 {
		if err := r.addIndexedLookupPaths(ctx, evt.RunID(), flows, paths); err != nil {
			return selectedTargetOwnerLookupScope{}, false, err
		}
	}
	if len(flows) == 0 && len(paths) == 0 {
		return selectedTargetOwnerLookupScope{}, false, nil
	}
	out := make([]string, 0, len(paths))
	for path := range paths {
		out = append(out, path)
	}
	sort.Strings(out)
	scope.instancePaths = out
	return scope, true, nil
}

func (r connectRoutePlanResolver) deliveryRoutesForMaterialization(ctx context.Context, runID string, plan runtimepinrouting.ConnectRoutePlan, materialized runtimepinrouting.ConnectRoutePlanMaterialization, decision connectInstanceSelection, routeCreatedInPlan bool) ([]runtimepinrouting.ConnectDeliveryRoute, []runtimepinrouting.ConnectDeliveryRoute, []Subscriber, events.ConnectEvaluationLedger, error) {
	targets := connectMaterializedTargets(materialized)
	if plan.ReceiverEndpoint().IsRoot() && len(targets) == 0 {
		targets = []events.RouteIdentity{{}}
	}
	if len(targets) == 0 {
		evaluation, err := r.evaluateSelectedReceiverCarriers(ctx, runID, plan, nil)
		if err != nil {
			return nil, nil, nil, events.ConnectEvaluationLedger{}, err
		}
		ledger, err := evaluation.Ledger()
		return nil, nil, nil, ledger, err
	}
	selectionTargets := make([]events.RouteIdentity, 0, len(targets))
	for _, target := range targets {
		selectionTargets = append(selectionTargets, target.Normalized())
	}
	evaluation, err := r.evaluateSelectedReceiverCarriers(ctx, runID, plan, selectionTargets)
	if err != nil {
		return nil, nil, nil, events.ConnectEvaluationLedger{}, err
	}
	ledger, err := evaluation.Ledger()
	if err != nil {
		return nil, nil, nil, events.ConnectEvaluationLedger{}, err
	}
	projection, err := syntheticDeliveryPayloadProjection(plan, decision)
	if err != nil {
		return nil, nil, nil, events.ConnectEvaluationLedger{}, err
	}
	routes := make([]runtimepinrouting.ConnectDeliveryRoute, 0, len(targets))
	liveRoutes := make([]runtimepinrouting.ConnectDeliveryRoute, 0, len(targets))
	subscribers := make([]Subscriber, 0, len(targets))
	for _, target := range targets {
		target = target.Normalized()
		matchedSubscribers, err := r.resolveSelectedReceiverCarriers(ctx, runID, plan, target)
		if err != nil {
			return nil, nil, nil, events.ConnectEvaluationLedger{}, err
		}
		if len(matchedSubscribers) == 0 {
			return nil, nil, nil, ledger, nil
		}
		subscribers = append(subscribers, matchedSubscribers...)
		for _, subscriber := range matchedSubscribers {
			identity, live, err := r.resolveAgentCarrierIdentity(ctx, subscriber, target, decision, routeCreatedInPlan)
			if err != nil {
				return nil, nil, nil, events.ConnectEvaluationLedger{}, err
			}
			agentPlan := agentidentity.Plan{}
			if subscriber.Recipient.IsAgent() {
				agentPlan, err = identity.Plan()
				if err != nil {
					return nil, nil, nil, events.ConnectEvaluationLedger{}, fmt.Errorf("project connect agent plan: %w", err)
				}
			}
			route := runtimepinrouting.ConnectDeliveryRoute{
				Recipient:         subscriber.Recipient,
				AgentPlan:         agentPlan,
				Target:            target,
				Handler:           subscriber.connectHandler,
				PayloadProjection: projection,
			}
			routes = append(routes, route)
			if live {
				liveRoutes = append(liveRoutes, route)
			}
		}
	}
	return runtimepinrouting.NormalizeConnectDeliveryRoutes(routes), runtimepinrouting.NormalizeConnectDeliveryRoutes(liveRoutes), dedupeSubscribers(subscribers), ledger, nil
}

func (r connectRoutePlanResolver) resolveAgentCarrierIdentity(
	ctx context.Context,
	subscriber Subscriber,
	target events.RouteIdentity,
	decision connectInstanceSelection,
	routeCreatedInPlan bool,
) (agentidentity.Identity, bool, error) {
	if !subscriber.Recipient.IsAgent() {
		return agentidentity.Identity{}, true, nil
	}
	agentID := subscriber.Recipient.ID()
	matches := make([]agentidentity.Identity, 0, 1)
	available := false
	runID := runtimecorrelation.RunIDFromContext(ctx)
	root := rootExecutionCoordinate(r.source, runID)
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
				} else if !routeMatchesAgentDescriptor(target, descriptor, root) {
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
		if identity, planned, err := plannedCreateAgentCarrierIdentity(subscriber, target, decision, routeCreatedInPlan, runID, root); planned || err != nil {
			return identity, false, err
		}
		if r.loadAgents == nil {
			return agentidentity.Identity{}, false, errors.New("connect agent carrier requires the canonical active-agent identity owner")
		}
		if !available {
			return agentidentity.Identity{}, false, fmt.Errorf(
				"connect agent carrier identity is unavailable for lifecycle action %q and route %q",
				decision.ActionCode(),
				subscriber.AgentPlan.FlowInstance(),
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
	decision connectInstanceSelection,
	routeCreatedInPlan bool,
	runID string,
	root semanticview.RootExecutionCoordinate,
) (agentidentity.Identity, bool, error) {
	plan := subscriber.AgentPlan.Normalize()
	if err := plan.Validate(); err != nil {
		if !routeCreatedInPlan {
			return agentidentity.Identity{}, false, nil
		}
		return agentidentity.Identity{}, true, fmt.Errorf("created connect agent carrier %q has no canonical declaration route plan: %w", subscriber.Recipient.ID(), err)
	}
	identity, err := plan.Live(runID)
	if err != nil {
		return agentidentity.Identity{}, true, fmt.Errorf("created connect agent carrier %q live identity: %w", subscriber.Recipient.ID(), err)
	}
	if identity.AgentID() != subscriber.Recipient.ID() {
		return agentidentity.Identity{}, true, fmt.Errorf("created connect agent carrier %q identity names %q", subscriber.Recipient.ID(), identity.AgentID())
	}
	if routeCreatedInPlan {
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
	}
	if !target.Empty() {
		target = target.Normalized()
		_, _, instance, err := identity.ExecutionCoordinates()
		if err != nil {
			return agentidentity.Identity{}, true, err
		}
		matches := instance == target.FlowInstance
		if identity.Route.Presence == agentidentity.RouteRoot {
			matches = identity.RunID == root.RunID() && root.Matches(target.FlowID, target.FlowInstance)
		}
		if !matches {
			return agentidentity.Identity{}, true, fmt.Errorf("created connect agent carrier %q identity does not match target %#v", subscriber.Recipient.ID(), target)
		}
	}
	return identity, true, nil
}

func syntheticDeliveryPayloadProjection(plan runtimepinrouting.ConnectRoutePlan, decision connectInstanceSelection) (events.DeliveryPayloadProjection, error) {
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

func (r connectRoutePlanResolver) resolveSelectedReceiverCarriers(ctx context.Context, runID string, plan runtimepinrouting.ConnectRoutePlan, target events.RouteIdentity) ([]Subscriber, error) {
	evaluation, err := r.evaluateSelectedReceiverCarriers(ctx, runID, plan, []events.RouteIdentity{target})
	if err != nil {
		return nil, err
	}
	subscribers := connectRecipientSubscribers(evaluation)
	for index := range subscribers {
		subscriber := &subscribers[index]
		if !subscriber.Recipient.IsNode() {
			continue
		}
		handler, err := runtimepipeline.AdmitDeliveryTargetHandler(
			r.source, subscriber.handlerNode,
		)
		if err != nil {
			return nil, fmt.Errorf("resolve connect receiver target handler: %w", err)
		}
		subscriber.targetHandler = handler
	}
	return subscribers, nil
}

func (r connectRoutePlanResolver) evaluateSelectedReceiverCarriers(ctx context.Context, runID string, plan runtimepinrouting.ConnectRoutePlan, targets []events.RouteIdentity) (runtimepinrouting.ConnectRecipientEvaluation, error) {
	if len(targets) == 0 {
		return r.graph.EvaluateMaterializedRecipients(plan, nil, nil), nil
	}
	orderedTargets := append([]events.RouteIdentity(nil), targets...)
	sort.Slice(orderedTargets, func(i, j int) bool {
		left, right := orderedTargets[i].Normalized(), orderedTargets[j].Normalized()
		if left.FlowID != right.FlowID {
			return left.FlowID < right.FlowID
		}
		if left.FlowInstance != right.FlowInstance {
			return left.FlowInstance < right.FlowInstance
		}
		return left.EntityID < right.EntityID
	})
	var registrations []runtimepinrouting.ConnectRecipientRegistration
	for _, target := range orderedTargets {
		instance, err := r.indexedConnectIdentity(ctx, runID, target)
		if err != nil {
			return runtimepinrouting.ConnectRecipientEvaluation{}, err
		}
		bound, err := r.routeTable.ConnectReceiverDefinitions(runID, instance)
		if err != nil {
			return runtimepinrouting.ConnectRecipientEvaluation{}, err
		}
		registrations = append(registrations, bound...)
	}
	return r.graph.EvaluateMaterializedRecipients(plan, targets, registrations), nil
}

func (r connectRoutePlanResolver) appendMaterializedPlanEvaluation(ctx context.Context, runID string, out *connectRoutePlanDispatch, plan runtimepinrouting.ConnectRoutePlan, targets []events.RouteIdentity) error {
	evaluation, err := r.evaluateSelectedReceiverCarriers(ctx, runID, plan, targets)
	if err != nil {
		return err
	}
	ledger, err := evaluation.Ledger()
	if err != nil {
		return err
	}
	return mergeConnectEvaluation(&out.Evaluation, ledger)
}

func (r connectRoutePlanResolver) appendBlockedPlanEvaluation(out *connectRoutePlanDispatch, plan runtimepinrouting.ConnectRoutePlan, targets []events.RouteIdentity) error {
	planID, err := runtimepinrouting.ConnectPlanIdentity(plan)
	if err != nil {
		return err
	}
	entry, err := events.NewConnectPlanEvaluation(planID, events.ConnectPlanResolutionBlocked, targets, nil)
	if err != nil {
		return err
	}
	ledger, err := events.NewConnectEvaluationLedger([]events.ConnectPlanEvaluation{entry})
	if err != nil {
		return err
	}
	return mergeConnectEvaluation(&out.Evaluation, ledger)
}

func mergeConnectEvaluation(target *events.ConnectEvaluationLedger, addition events.ConnectEvaluationLedger) error {
	if target == nil || !addition.Present() {
		return nil
	}
	plans := append(target.Plans(), addition.Plans()...)
	merged, err := events.NewConnectEvaluationLedger(plans)
	if err != nil {
		return err
	}
	*target = merged
	return nil
}

func connectMaterializedTargets(materialized runtimepinrouting.ConnectRoutePlanMaterialization) []events.RouteIdentity {
	if !materialized.Target.Empty() {
		return []events.RouteIdentity{materialized.Target.Normalized()}
	}
	return uniqueRouteIdentities(materialized.TargetSet)
}

func connectRoutePlanLiveRecipients(runID string, routes []runtimepinrouting.ConnectDeliveryRoute) ([]RoutePlanLiveRecipient, error) {
	routes = runtimepinrouting.NormalizeConnectDeliveryRoutes(routes)
	if len(routes) == 0 {
		return nil, nil
	}
	out := make([]RoutePlanLiveRecipient, 0, len(routes))
	for _, route := range routes {
		if route.Recipient.Empty() {
			continue
		}
		identity := agentidentity.Identity{}
		var err error
		if route.Recipient.IsAgent() {
			identity, err = route.AgentPlan.Live(runID)
			if err != nil {
				return nil, fmt.Errorf("compose connect live recipient identity: %w", err)
			}
		}
		out = append(out, RoutePlanLiveRecipient{
			Recipient:         route.Recipient,
			AgentIdentity:     identity,
			PersistAsDelivery: route.Recipient.IsAgent(),
			Producer:          routeIntentProducerConnectRoutePlan,
		})
	}
	return normalizeRoutePlanLiveRecipients(out), nil
}

func connectRoutePlanDeliveryIntents(runID string, plan runtimepinrouting.ConnectRoutePlan, routes, liveRoutes []runtimepinrouting.ConnectDeliveryRoute, routeCreatedInPlan bool, staticPlans map[agentidentity.Plan]struct{}) ([]RoutePlanDeliveryIntent, error) {
	planID, err := runtimepinrouting.ConnectPlanIdentity(plan)
	if err != nil {
		return nil, err
	}
	receiverEvent := plan.ReceiverLocalEvent()
	intents, err := routePlanDeliveryIntentsFromConnectRoutes(runID, routes, routeIntentProducerConnectRoutePlan, receiverEvent)
	if err != nil {
		return nil, err
	}
	liveAgents := make(map[agentidentity.Plan]struct{}, len(liveRoutes))
	for _, route := range runtimepinrouting.NormalizeConnectDeliveryRoutes(liveRoutes) {
		if route.Recipient.IsAgent() {
			liveAgents[route.AgentPlan] = struct{}{}
		}
	}
	for index := range intents {
		intent := &intents[index]
		intent.ConnectPlan = planID
		if !intent.Recipient.IsAgent() {
			continue
		}
		plan, err := intent.AgentIdentity.Plan()
		if err != nil {
			return nil, err
		}
		if _, live := liveAgents[plan]; !live {
			intent.AgentLifecycle = agentLifecycleAdmissionMaterializingFlow
			if _, declared := staticPlans[plan.Normalize()]; declared && !routeCreatedInPlan {
				intent.AgentLifecycle = agentLifecycleAdmissionStaticDeclaration
			}
		}
	}
	return intents, nil
}

func connectRoutePlanTargetFailure(failure runtimepinrouting.ConnectRoutePlanFailure) runtimepinrouting.TargetFailure {
	if failure.Empty() {
		return 0
	}
	return runtimepinrouting.TargetFailureFromConnect(failure)
}

func connectRoutePlanFailureDetail(plan runtimepinrouting.ConnectRoutePlan, failure runtimepinrouting.ConnectRoutePlanFailure, values map[string]string) map[string]any {
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
