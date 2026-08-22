package pipeline

import (
	"context"
	"fmt"
	"time"

	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
)

type GateDecisionFence interface {
	CommitDecision(context.Context, decisioncard.Card, string, time.Time) error
}

// GateRouteAdmissionReader owns the selected-store projection used to decide
// whether a persisted run may receive a gate decision route.
type GateRouteAdmissionReader interface {
	RequireGateRouteAdmitted(context.Context, string) error
}

func (s *workflowInstanceStore) CommitDecision(ctx context.Context, card decisioncard.Card, eventID string, now time.Time) error {
	switch card.Anchor.Kind() {
	case decisioncard.AnchorKindStageGate:
		return s.commitGateDecision(ctx, card, eventID, now)
	case decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect:
		return nil
	default:
		return fmt.Errorf("decision-card anchor kind %q is not registered", card.Anchor.Kind())
	}
}

func (s *workflowInstanceStore) commitGateDecision(ctx context.Context, card decisioncard.Card, eventID string, now time.Time) error {
	if s == nil || !s.enabled() {
		return fmt.Errorf("workflow instance store is required for gate decision")
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	if s.engineMutations == nil {
		return fmt.Errorf("gate decision requires the selected workflow engine mutation owner")
	}
	instance, found, err := s.Load(ctx, anchor.Route)
	if err != nil {
		return err
	}
	if !found {
		return &WorkflowInstanceLookupMiss{RequestedKey: anchor.Route.InstancePath}
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return err
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, anchor.FlowID, card.Snapshot.Decision)
	if err != nil {
		return err
	}
	if !found || activation.ActivationID != anchor.StageActivationID || activation.CardID != card.CardID || activation.Stage != anchor.Stage || instance.CurrentState != anchor.Stage {
		return fmt.Errorf("decision card is superseded by the current stage activation")
	}
	if err := activation.CommitDecision(eventID, now); err != nil {
		return err
	}
	if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
		return err
	}
	instance.StateBuckets = carrier.PersistedStateBuckets()
	record, err := workflowEngineStateRecord(card.RunID, anchor.Route, instance, instance.CurrentState, instance.Revision, WorkflowEngineStateTransitionUpdateStateAndCompanion, now.UTC())
	if err != nil {
		return err
	}
	_, err = s.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: record})
	return err
}

func (s *workflowInstanceStore) RequireGateRouteAdmitted(ctx context.Context, runID string) error {
	if s == nil || s.gateRoutes == nil {
		return fmt.Errorf("workflow instance store is required for gate routing")
	}
	return s.gateRoutes.RequireGateRouteAdmitted(ctx, runID)
}
