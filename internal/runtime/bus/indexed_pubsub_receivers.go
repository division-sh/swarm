package bus

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (r connectRoutePlanResolver) resolvePubsubSubscribers(ctx context.Context, event events.Event, keys []string, publication ordinaryPublicationSource) ([]Subscriber, error) {
	if len(keys) == 0 || r.source == nil {
		return nil, nil
	}
	if r.routeTable == nil || !r.routeTable.compiledSourceReady {
		return nil, fmt.Errorf("pubsub planning requires its compiled source")
	}
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present {
		return nil, fmt.Errorf("pubsub planning requires its admitted source")
	}
	scope, err := r.pubsubLookupScope(event, publication, fact)
	if err != nil {
		return nil, err
	}
	if len(scope.FlowIDs()) == 0 && len(scope.Coordinates()) == 0 {
		return nil, nil
	}
	if event.RoutingSource().Kind() == events.RoutingSourceDeploymentFeed {
		keys = append([]string(nil), keys...)
		for _, coordinate := range scope.Coordinates() {
			keys = append(keys, coordinate.Route.InstancePath+"/"+string(event.Type()))
		}
	}
	if r.lifecycle.index == nil {
		return nil, fmt.Errorf("pubsub planning requires its compiled source and native instance index")
	}
	instances, err := r.pubsubInstanceData(ctx, scope)
	if err != nil {
		return nil, err
	}
	owners := make([]string, 0, len(instances))
	for owner := range instances {
		owners = append(owners, owner)
	}
	sort.Strings(owners)
	var out []Subscriber
	for _, owner := range owners {
		bound, err := r.routeTable.PubsubReceiverDefinitions(event.RunID(), instances[owner], keys)
		if err != nil {
			return nil, err
		}
		for _, subscriber := range bound {
			out = appendUniqueSubscriber(out, subscriber)
		}
	}
	return out, nil
}

func (r connectRoutePlanResolver) pubsubInstanceData(ctx context.Context, scope pipeline.FlowInstanceLookupScope) (map[string]flowidentity.Instance, error) {
	instances := make(map[string]flowidentity.Instance)
	if r.lifecycle.runProposal.Present() {
		if err := r.lifecycle.runProposal.Validate(scope.RunID(), scope.SourceFact()); err != nil {
			return nil, err
		}
	} else {
		observations, err := r.indexedInstances(ctx, scope)
		if err != nil {
			return nil, err
		}
		for _, instance := range observations {
			owner := flowidentity.RunScopedFlowInstance{RunID: scope.RunID(), Route: instance.Route()}
			instances[owner.Key()] = instance
		}
	}
	proposals, err := r.prospectiveConnectInstances(ctx, scope.RunID(), scope.SourceFact())
	if err != nil {
		return nil, err
	}
	for _, instance := range proposals {
		owner := flowidentity.RunScopedFlowInstance{RunID: scope.RunID(), Route: instance.Route()}
		if !pubsubScopeIncludes(scope, owner, instance.TemplateID) {
			continue
		}
		if previous, exists := instances[owner.Key()]; exists && previous != instance {
			return nil, fmt.Errorf("prospective pubsub receiver contradicts its native construction")
		}
		instances[owner.Key()] = instance
	}
	return instances, nil
}

func (r connectRoutePlanResolver) pubsubLookupScope(event events.Event, publication ordinaryPublicationSource, fact correlation.SourceArtifactFact) (pipeline.FlowInstanceLookupScope, error) {
	route := publication.route
	if route.Empty() {
		route = event.RoutingSource().Route()
	}
	if event.RoutingSource().Kind() == events.RoutingSourceRoot {
		route = events.RouteIdentity{FlowID: semanticview.RootExecutionFlowID(r.source), FlowInstance: event.RunID()}
	}
	if event.RoutingSource().Kind() == events.RoutingSourceDeploymentFeed {
		if _, err := pinrouting.AdmitDeploymentFeedDeclaration(r.source, event.Type(), event.RoutingSource()); err != nil {
			return pipeline.FlowInstanceLookupScope{}, err
		}
		if route.FlowID == semanticview.RootExecutionFlowID(r.source) {
			route.FlowInstance = event.RunID()
		}
	}
	var exact []flowidentity.RunScopedFlowInstance
	if !publication.declarationOnly && route.FlowID != "" && route.FlowInstance != "" {
		owner, err := flowidentity.NewRunScopedFlowInstance(event.RunID(), flowidentity.StoredRoute(flowidentity.ScopeKey(r.source, route.FlowID), "", route.FlowInstance))
		if err != nil {
			return pipeline.FlowInstanceLookupScope{}, err
		}
		if len(r.routeTable.templates[owner.Route.ScopeKey].Subscribers) > 0 {
			exact = append(exact, owner)
		}
	}
	return pipeline.NewFlowInstanceLookupScope(r.source, fact, event.RunID(), nil, exact)
}

func pubsubScopeIncludes(scope pipeline.FlowInstanceLookupScope, owner flowidentity.RunScopedFlowInstance, flowID string) bool {
	if slices.Contains(scope.FlowIDs(), flowID) {
		return true
	}
	for _, coordinate := range scope.Coordinates() {
		if coordinate.Key() == owner.Key() {
			return true
		}
	}
	return false
}

func (r connectRoutePlanResolver) indexedInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]flowidentity.Instance, error) {
	observations, err := r.lifecycle.index.ListFlowInstances(ctx, scope)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(observations))
	instances := make([]flowidentity.Instance, 0, len(observations))
	for _, observed := range observations {
		if observed.Owner().RunID != scope.RunID() || !pubsubScopeIncludes(scope, observed.Owner(), observed.Identity().TemplateID) {
			return nil, fmt.Errorf("receiver inventory crosses its compiled lookup scope")
		}
		request, err := pipeline.NewExactFlowInstanceLookup(scope.Source(), scope.SourceFact(), observed.Owner())
		if err != nil {
			return nil, err
		}
		if err := observed.ValidateSelection(request); err != nil {
			return nil, err
		}
		if err := validatePubsubExactCoordinates(scope, observed); err != nil {
			return nil, err
		}
		if _, duplicate := seen[observed.Owner().Key()]; duplicate {
			return nil, fmt.Errorf("receiver inventory repeats a native coordinate")
		}
		seen[observed.Owner().Key()] = struct{}{}
		instances = append(instances, observed.Identity())
	}
	return instances, nil
}

func validatePubsubExactCoordinates(scope pipeline.FlowInstanceLookupScope, observed pipeline.FlowInstanceObservation) error {
	for _, coordinate := range scope.Coordinates() {
		if coordinate.Key() != observed.Owner().Key() {
			continue
		}
		request, err := pipeline.NewExactFlowInstanceLookup(scope.Source(), scope.SourceFact(), coordinate)
		if err != nil {
			return err
		}
		if err := observed.ValidateSelection(request); err != nil {
			return err
		}
	}
	return nil
}

// PubsubReceiverDefinitions binds source-owned subscription definitions to an
// identity admitted by the caller. It grants neither existence nor readiness.
func (rt *RouteTable) PubsubReceiverDefinitions(runID string, instance flowidentity.Instance, keys []string) ([]Subscriber, error) {
	if rt == nil || !rt.compiledSourceReady {
		return nil, fmt.Errorf("pubsub binding requires its compiled source")
	}
	if err := instance.ValidateConstruction(rt.source, runID); err != nil {
		return nil, err
	}
	declaration, found := rt.templates[instance.ScopeKey]
	if !found || declaration.FlowID != instance.TemplateID {
		return nil, fmt.Errorf("pubsub binding requires its exact compiled declaration")
	}
	route, err := instance.Route().AgentIdentityRoute()
	if err != nil {
		return nil, err
	}
	return rt.bindPubsubDefinitions(declaration, instance.InstancePath, route, keys)
}

// PubsubDeclarationDefinitions is non-executing descriptive data. Like its
// connection counterpart it cannot establish an instance or attach resources.
func (rt *RouteTable) PubsubDeclarationDefinitions(flowID string, keys []string) ([]Subscriber, error) {
	if rt == nil || !rt.compiledSourceReady {
		return nil, fmt.Errorf("pubsub declaration requires its compiled source")
	}
	scope, found := rt.source.FlowScopeByID(flowID)
	if !found {
		return nil, fmt.Errorf("pubsub declaration is outside its compiled source")
	}
	path := scope.Path
	if flowID == semanticview.RootExecutionFlowID(rt.source) {
		path = "."
	}
	route, err := flowidentity.StoredRoute("", "", path).AgentIdentityRoute()
	if err != nil {
		return nil, err
	}
	return rt.bindPubsubDefinitions(rt.templates[flowidentity.ScopeKey(rt.source, flowID)], path, route, keys)
}

func (rt *RouteTable) bindPubsubDefinitions(declaration routeFlowTemplate, path string, route agentidentity.Route, keys []string) ([]Subscriber, error) {
	var out []Subscriber
	for _, definition := range declaration.Subscribers {
		subscriber, err := bindPubsubSubscriber(path, route, definition)
		if err != nil {
			return nil, err
		}
		for _, pattern := range definition.Patterns {
			out = append(out, rt.matchPubsubDefinition(declaration, subscriber, pattern, keys)...)
		}
	}
	return dedupeSubscribers(out), nil
}

func (rt *RouteTable) matchPubsubDefinition(declaration routeFlowTemplate, subscriber Subscriber, pattern routeSubscriberTemplatePattern, keys []string) []Subscriber {
	var out []Subscriber
	for _, resolved := range routeProjectAdmittedSubscriberPatterns(pattern.admission, declaration.FlowID, subscriber.Path, pattern.inputEvent, rt.inputProducers) {
		for _, key := range keys {
			if !RouteMatches(resolved.EventPattern, key) || pattern.admission.Pattern() && !pubsubPatternEventDeclared(pattern, declaration, subscriber.Path, key) {
				continue
			}
			bound := routeApplyResolvedPattern(subscriber, resolved)
			bound.MatchPattern = resolved.EventPattern
			out = append(out, projectSubscriberEvents([]Subscriber{bound}, key)...)
		}
	}
	return out
}

func pubsubPatternEventDeclared(pattern routeSubscriberTemplatePattern, declaration routeFlowTemplate, path, key string) bool {
	local, matched := pattern.admission.LocalEventAt(path, key)
	_, declared := declaration.LocalEvents[local]
	return matched && declared
}

func bindPubsubSubscriber(path string, route agentidentity.Route, definition routeSubscriberTemplate) (Subscriber, error) {
	var name agentidentity.Name
	var err error
	if definition.Kind == subscriberAgent {
		name, err = definition.AgentNamePlan.Materialize()
		if err != nil {
			return Subscriber{}, err
		}
	}
	recipient, err := subscriberRecipient(definition.Kind, name.AgentID, definition.HandlerNode)
	if err != nil {
		return Subscriber{}, err
	}
	subscriber := Subscriber{Recipient: recipient, Path: path, handlerNode: definition.HandlerNode, targetHandler: definition.TargetHandler}
	if recipient.IsAgent() {
		subscriber.AgentPlan, err = agentidentity.NewPlan(name, route)
		if err != nil {
			return Subscriber{}, err
		}
	}
	return subscriber, nil
}

func pubsubEventKeys(event events.Event, source ordinaryPublicationSource) []string {
	keys := source.eventKeys(event)
	if !source.declarationOnly && event.RoutingSource().Kind() == events.RoutingSourceRoot && eventidentity.IsCanonicalName(string(event.Type())) {
		keys = append(keys, event.RunID()+"/"+string(event.Type()))
	}
	return uniqueStrings(keys)
}
