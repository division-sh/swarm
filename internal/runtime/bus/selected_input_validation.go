package bus

import (
	"bytes"
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// SelectedInputValidation is immutable input resolution data. Execution still
// requires the selected owner, binding and exact recipient-plan guards.
type SelectedInputValidation struct {
	original   events.Event
	source     semanticview.Source
	bundleHash string
	admission  apiEventPublicationAdmission
	recipients []forkrecipient.Evidence
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
	var endpoint APIEventPublicationEndpoint
	if payload.Binding().FlowID() == "." {
		association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint(".", string(original.Type()))
		if err := association.Err(); err != nil {
			return SelectedInputValidation{}, fmt.Errorf("selected root input: %w", err)
		}
		endpoint, err = NewRootInputAPIEventPublicationEndpoint(source, string(original.Type()))
	} else if payload.Binding().FlowID() == "" {
		// The ordinary root API has no flow-scoped admission. Require its
		// exact root declaration, as the API resolver does; absence alone is
		// never a root-input endpoint or permission to fan out to children.
		endpoint, err = NewOrdinaryFlowAPIEventPublicationEndpoint(source, ".", string(original.Type()))
	} else {
		endpoint, err = NewOrdinaryFlowAPIEventPublicationEndpoint(source, payload.Binding().FlowID(), string(original.Type()))
	}
	if err != nil {
		return SelectedInputValidation{}, err
	}
	admission, _, err := endpoint.admit(source, original)
	if err != nil {
		return SelectedInputValidation{}, err
	}
	return SelectedInputValidation{original: original, source: source, bundleHash: hash, admission: admission}, nil
}

func (v SelectedInputValidation) Present() bool { return v.original.ID() != "" }

func (v SelectedInputValidation) AllowsSubscriber(subscriber Subscriber) bool {
	if !v.Present() {
		return false
	}
	// Compare source coordinates before binding root ownership to the child run.
	flowID := subscriber.handlerNode.FlowPath()
	if subscriber.Recipient.IsAgent() {
		scope, err := semanticview.ResolveAgentPlanExecutionSemanticScope(v.source, v.original.RunID(), subscriber.AgentPlan)
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
	if subscriber.Recipient.IsAgent() {
		if v.admission.kind == apiEventPublicationEndpointOrdinaryFlow {
			return flowID == v.admission.flowID && subscriber.Path == v.admission.flowPath
		}
		return flowID == "."
	}
	if v.admission.authorizesSubscriber(v.source, v.original, subscriber) {
		return true
	}
	return v.admission.kind == apiEventPublicationEndpointRootInput &&
		(routedRootInputFlowNodeMatchesNoTargetEvent(v.original, subscriber) || routedRootNodeMatchesNoTargetEvent(v.original, subscriber, "."))
}

// SelectRecipients narrows revalidated input resolution to the existing fixed
// frontier's disposition. In particular, completed recipients stay excluded.
func (v SelectedInputValidation) SelectRecipients(recipients []forkrecipient.Evidence) (SelectedInputValidation, error) {
	canonical, err := forkrecipient.CanonicalSet(recipients)
	if err != nil {
		return SelectedInputValidation{}, err
	}
	v.recipients = append(make([]forkrecipient.Evidence, 0, len(canonical)), canonical...)
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
	if !ok || lineage.SourceRunID() != v.original.RunID() || lineage.SourceEventID() != v.original.ID() ||
		event.Type() != v.original.Type() || event.ExecutionMode() != v.original.ExecutionMode() ||
		!bytes.Equal(event.Payload(), v.original.Payload()) || bundleHash != v.bundleHash {
		return nil, fmt.Errorf("selected input validation differs from exact source/event/artifact")
	}
	return context.WithValue(ctx, selectedInputValidationContextKey{}, v), nil
}

func selectedInputValidationFromContext(ctx context.Context, event events.Event) (SelectedInputValidation, bool) {
	v, ok := ctx.Value(selectedInputValidationContextKey{}).(SelectedInputValidation)
	lineage, selected := event.SelectedForkLineage()
	return v, ok && v.Present() && selected && lineage.SourceEventID() == v.original.ID() && lineage.SourceRunID() == v.original.RunID() &&
		event.Type() == v.original.Type() && event.ExecutionMode() == v.original.ExecutionMode() && bytes.Equal(event.Payload(), v.original.Payload())
}
