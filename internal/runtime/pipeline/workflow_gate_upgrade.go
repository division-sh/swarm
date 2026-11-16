package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// PrepareGateUpgrade derives replacement data, not authority. The source-switch
// owner calls it against the locked current header and exact pending issuance.
func PrepareGateUpgrade(ctx context.Context, current, candidate semanticview.Source, card decisioncard.Card, instance WorkflowInstance, cadence decisioncard.CadencePolicy, now time.Time) (*WorkflowGateUpgrade, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateDecisionCardGateMutation(card, instance); err != nil {
		return nil, err
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return nil, err
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return nil, err
	}
	activation, _, err := gateruntime.Load(carrier.StateBuckets, anchor.FlowID, card.Snapshot.Decision)
	if err != nil {
		return nil, err
	}
	if instance.Life <= 0 {
		return nil, fmt.Errorf("gate replacement requires its positive current life")
	}
	if err := validateStageGateInstanceOwner(anchor, instance, activation); err != nil {
		return nil, err
	}
	issued, err := decisioncard.IssuedGateOffer(card, activation)
	if err != nil {
		return nil, err
	}
	prior, err := decisioncard.CompileGateOffer(current, anchor.FlowID, anchor.Stage)
	if err != nil {
		return nil, err
	}
	if !issued.Equal(prior) {
		return nil, fmt.Errorf("pending gate differs from its admitted current contract")
	}
	next, err := decisioncard.CompileGateOffer(candidate, anchor.FlowID, anchor.Stage)
	if err != nil || issued.Equal(next) {
		return nil, err
	}
	plan, found := candidate.WorkflowGateForStage(anchor.FlowID, anchor.Stage)
	if !found {
		return nil, fmt.Errorf("replacement lacks its exact candidate wait")
	}
	bundle, found := semanticview.Bundle(candidate)
	if !found || bundle == nil {
		return nil, fmt.Errorf("replacement lacks its admitted candidate source")
	}
	root, err := bundle.RootedSource()
	if err != nil {
		return nil, err
	}
	routes, err := next.RoutesJSON()
	if err != nil {
		return nil, err
	}
	replacement, err := gateruntime.Replacement(activation, plan.Decision, root.Admission().BundleHash(), routes, now)
	if err != nil {
		return nil, err
	}
	outcomes, err := decisioncard.ResolveGateOutcomes(candidate, plan)
	if err != nil {
		return nil, err
	}
	newCard, err := buildWorkflowDecisionCard(candidate, cadence, card.ExecutionMode, card.RunID, anchor.Route, identity.NormalizeEntityID(anchor.EntityID), instance, replacement, plan, outcomes)
	if err != nil {
		return nil, err
	}
	if !activation.Supersede(decisioncard.SupersededByUpdate, now) {
		return nil, fmt.Errorf("replacement predecessor is not open")
	}
	if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
		return nil, err
	}
	if err := gateruntime.Store(carrier.StateBuckets, replacement); err != nil {
		return nil, err
	}
	instance.StateBuckets = carrier.PersistedStateBuckets()
	state, err := workflowEngineStateRecord(flowidentity.RunScopedFlowInstance{RunID: card.RunID, Route: anchor.Route}, instance, instance.CurrentState, instance.Revision, WorkflowEngineStateTransitionUpdateStateAndCompanion, now.UTC())
	if err != nil {
		return nil, err
	}
	return &WorkflowGateUpgrade{Previous: card, Replacement: newCard, State: state}, nil
}

type WorkflowGateUpgrade struct {
	Previous, Replacement decisioncard.Card
	State                 WorkflowEngineStateRecord
}
