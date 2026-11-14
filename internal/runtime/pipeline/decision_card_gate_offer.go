package pipeline

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// PreparedGateOffer is a request-local observation, not mutation authority. The
// native owner checks it again against its locked source, instance and issuance.
type PreparedGateOffer struct {
	root          sourceartifact.RootAdmissionReference
	runID, cardID string
	life          int64
	offer         decisioncard.GateOffer
}

func (pc *PipelineCoordinator) prepareGateOffer(ctx context.Context, card decisioncard.Card, instance WorkflowInstance, activation gateruntime.Activation) (PreparedGateOffer, error) {
	var empty PreparedGateOffer
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return empty, err
	}
	if err := validateStageGateInstanceOwner(anchor, instance, activation); err != nil {
		return empty, err
	}
	bundle, found := semanticview.Bundle(pc.SemanticSource())
	if !found || bundle == nil || instance.Life <= 0 {
		return empty, fmt.Errorf("gate execution requires its compiled source and instance life")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return empty, err
	}
	current, err := decisioncard.CompileGateOffer(semanticview.Wrap(bundle), activation.FlowID, activation.Stage)
	if err != nil {
		return empty, err
	}
	p := PreparedGateOffer{root: rooted.Admission().Reference(), runID: card.RunID, cardID: card.CardID, life: instance.Life, offer: current}
	return p, p.require(ctx, card, instance, activation)
}

func (p PreparedGateOffer) require(ctx context.Context, card decisioncard.Card, instance WorkflowInstance, activation gateruntime.Activation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	root, supplied, err := correlation.RootAdmissionFromContext(ctx)
	if err != nil {
		return err
	}
	if !found || !supplied || p.root.Validate() != nil || root.Reference() != p.root || fact.BundleHash() != p.root.BundleHash || card.RunID != p.runID || card.CardID != p.cardID || instance.Life != p.life {
		return fmt.Errorf("gate offer differs from the current source, card or instance life")
	}
	issued, err := decisioncard.IssuedGateOffer(card, activation)
	if err != nil {
		return err
	}
	if !issued.Equal(p.offer) {
		return fmt.Errorf("issued gate offer differs from the current execution contract")
	}
	return nil
}

func (p PreparedGateOffer) RequireMutation(ctx context.Context, card decisioncard.Card, instance WorkflowInstance) error {
	if err := ValidateDecisionCardGateMutation(card, instance); err != nil {
		return err
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return err
	}
	activation, _, err := gateruntime.Load(carrier.StateBuckets, anchor.FlowID, card.Snapshot.Decision)
	if err != nil {
		return err
	}
	return p.require(ctx, card, instance, activation)
}
