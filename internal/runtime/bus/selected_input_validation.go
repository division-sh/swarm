package bus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// SelectedInputValidation is immutable input resolution data. Execution still
// requires the selected owner, binding and exact recipient-plan guards.
type SelectedInputValidation struct {
	original           events.Event
	source             semanticview.Source
	bundleHash         string
	flowID             string
	recipients         []forkrecipient.Evidence
	projectedPayload   []byte
	agentConstructions map[agentidentity.Plan]flowidentity.Instance
}

func RevalidateSelectedInput(source semanticview.Source, original events.Event) (SelectedInputValidation, error) {
	switch original.AdmissionClass() {
	case events.EventAdmissionRootIngress, events.EventAdmissionOperatorInjected:
	default:
		return SelectedInputValidation{}, fmt.Errorf("selected input requires recorded ingress publication")
	}
	payload, ok := original.PayloadAdmission()
	if !ok {
		return SelectedInputValidation{}, fmt.Errorf("selected input lacks persisted schema identity")
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return SelectedInputValidation{}, fmt.Errorf("selected input requires selected artifact")
	}
	hash, err := contracts.BundleHash(bundle)
	if err != nil {
		return SelectedInputValidation{}, err
	}
	flowID := payload.Binding().FlowID()
	switch routing := original.RoutingSource(); routing.Kind() {
	case events.RoutingSourceRoot:
		if flowID != "." {
			return SelectedInputValidation{}, fmt.Errorf("selected root input requires its exact root schema binding")
		}
		endpoint, err := NewRootInputAPIEventPublicationEndpoint(source, string(original.Type()))
		if err != nil {
			return SelectedInputValidation{}, err
		}
		if _, _, err := endpoint.admit(source, original); err != nil {
			return SelectedInputValidation{}, err
		}
	case events.RoutingSourceExternalIngress:
		if routing.Authority() != events.RoutingSourceAuthorityProviderAdmissionPlan || routing.Route().FlowID != flowID {
			return SelectedInputValidation{}, fmt.Errorf("selected provider input differs from its admitted declaring flow")
		}
		if !source.SemanticCapabilities().HasProviderIngressEvent(flowID, string(original.Type())) {
			return SelectedInputValidation{}, fmt.Errorf("selected provider input lacks its exact compiled ingress binding")
		}
	default:
		return SelectedInputValidation{}, fmt.Errorf("selected input requires exact root or provider ingress source")
	}
	return SelectedInputValidation{original: original, source: source, bundleHash: hash, flowID: flowID}, nil
}

func (v SelectedInputValidation) Present() bool { return v.original.ID() != "" }

func (v SelectedInputValidation) AllowsSubscriber(subscriber Subscriber) bool {
	if !v.Present() {
		return false
	}
	// Compare source coordinates before binding root ownership to the child run.
	flowID := subscriber.handlerNode.FlowPath()
	if subscriber.Recipient.IsAgent() {
		var construction semanticview.AgentExecutionConstruction
		if instance, found := v.agentConstructions[subscriber.AgentPlan]; found {
			construction = instance
		}
		scope, err := semanticview.ResolveAgentPlanExecutionSemanticScope(v.source, v.original.RunID(), subscriber.AgentPlan, construction)
		if err != nil || scope.Identity().AgentID() != subscriber.Recipient.ID() {
			return false
		}
		flowID = scope.Declaration().OwnerFlowID
	}
	if flowID == "." {
		subscriber.Path = "."
	}
	if v.recipients != nil {
		actual, err := subscriber.SelectedRecipient(v.original.Type())
		if err != nil {
			return false
		}
		selected := false
		for _, recipient := range v.recipients {
			if equal, err := forkrecipient.Equal(recipient, actual); err == nil && equal {
				selected = true
			}
		}
		if !selected {
			return false
		}
	}
	if len(eventDeliveryTargetRoutes(v.original)) > 0 && !eventTargetsRoutedSubscriber(v.source, v.original, subscriber) {
		return false
	}
	// This owner filters only independently subscribed local consumers. The
	// compiled graph separately proves every cross-flow recipient relation.
	return flowID == v.flowID && independentPubsubSubscriber(subscriber)
}

// SelectRecipients narrows revalidated input resolution to the existing fixed
// frontier's disposition. In particular, completed recipients stay excluded.
func (v SelectedInputValidation) SelectRecipients(recipients []forkrecipient.Evidence, constructions map[agentidentity.Plan]flowidentity.Instance) (SelectedInputValidation, error) {
	canonical, err := forkrecipient.CanonicalSet(recipients)
	if err != nil {
		return SelectedInputValidation{}, err
	}
	v.recipients = append(make([]forkrecipient.Evidence, 0, len(canonical)), canonical...)
	v.agentConstructions = make(map[agentidentity.Plan]flowidentity.Instance)
	for _, recipient := range canonical {
		if !recipient.Recipient.IsAgent() {
			continue
		}
		plan := recipient.AgentPlan
		var construction semanticview.AgentExecutionConstruction
		if instance, found := constructions[plan]; found {
			construction = instance
			v.agentConstructions[plan] = instance
		}
		if _, err := semanticview.ResolveAgentPlanExecutionSemanticScope(v.source, v.original.RunID(), plan, construction); err != nil {
			return SelectedInputValidation{}, fmt.Errorf("selected input recipient construction: %w", err)
		}
	}
	return v, nil
}

// WithStoreProjectedPayload binds the exact payload returned by selected-store
// source-event preparation after the original publication has been rechecked.
func (v SelectedInputValidation) WithStoreProjectedPayload(payload []byte) (SelectedInputValidation, error) {
	if !v.Present() || !json.Valid(payload) {
		return SelectedInputValidation{}, fmt.Errorf("selected input requires a valid store-projected payload")
	}
	v.projectedPayload = append([]byte(nil), payload...)
	return v, nil
}

func (v SelectedInputValidation) FilterSubscribers(in []Subscriber) []Subscriber {
	out := make([]Subscriber, 0, len(in))
	for _, subscriber := range in {
		if v.AllowsSubscriber(subscriber) {
			out = append(out, subscriber)
		}
	}
	return out
}

type selectedInputValidationContextKey struct{}

func (v SelectedInputValidation) bind(ctx context.Context, event events.Event, bundleHash string) (context.Context, error) {
	if !v.Present() {
		return ctx, nil
	}
	lineage, ok := event.SelectedForkLineage()
	payload := v.original.Payload()
	if v.projectedPayload != nil {
		payload = v.projectedPayload
	}
	if !ok || lineage.SourceRunID() != v.original.RunID() || lineage.SourceEventID() != v.original.ID() ||
		event.Type() != v.original.Type() || event.ExecutionMode() != v.original.ExecutionMode() ||
		!bytes.Equal(event.Payload(), payload) || bundleHash != v.bundleHash {
		return nil, fmt.Errorf("selected input validation differs from exact source/event/artifact")
	}
	return context.WithValue(ctx, selectedInputValidationContextKey{}, v), nil
}

func selectedInputValidationFromContext(ctx context.Context, event events.Event) (SelectedInputValidation, bool) {
	v, ok := ctx.Value(selectedInputValidationContextKey{}).(SelectedInputValidation)
	lineage, selected := event.SelectedForkLineage()
	payload := v.original.Payload()
	if v.projectedPayload != nil {
		payload = v.projectedPayload
	}
	return v, ok && v.Present() && selected && lineage.SourceEventID() == v.original.ID() && lineage.SourceRunID() == v.original.RunID() &&
		event.Type() == v.original.Type() && event.ExecutionMode() == v.original.ExecutionMode() && bytes.Equal(event.Payload(), payload)
}
