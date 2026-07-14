package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/google/uuid"
)

func (s *WorkflowInstanceStore) supersedeWorkflowInstanceGates(ctx context.Context, instance *WorkflowInstance, reason string, now time.Time) error {
	if s == nil || s.decisionCards == nil || instance == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		return err
	}
	activations, err := gateruntime.List(carrier.StateBuckets)
	if err != nil {
		return err
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if runID == "" {
		runID = strings.TrimSpace(asString(instance.Metadata["run_id"]))
	}
	entityID := strings.TrimSpace(firstNonEmptyString(instance.StorageRef, asString(instance.Metadata["entity_id"]), instance.InstanceID))
	for _, activation := range activations {
		if activation.Status == gateruntime.StatusDecisionCommitted {
			return fmt.Errorf("flow cannot terminate while decision card %s has a committed verdict awaiting its frozen route", activation.CardID)
		}
		if !activation.Supersede(reason, now) {
			continue
		}
		card, err := s.decisionCards.GetDecisionCard(ctx, activation.CardID)
		if err != nil {
			return fmt.Errorf("load terminated flow decision card: %w", err)
		}
		if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
			return err
		}
		if err := s.decisionCards.SupersedeDecisionCardsForStage(ctx, runID, entityID, activation.ActivationID, activation.SupersededReason, now); err != nil {
			return fmt.Errorf("supersede terminated flow decision card: %w", err)
		}
		if s.gateEvents == nil {
			return fmt.Errorf("transactional event publisher is required to terminate gated flow %s", instance.InstanceID)
		}
		payload, err := canonicaljson.Bytes(map[string]any{
			"card_id": activation.CardID, "anchor_kind": decisioncard.AnchorKindStageGate, "stage_activation_id": activation.ActivationID, "reason": activation.SupersededReason,
		})
		if err != nil {
			return err
		}
		anchor, err := card.Anchor.StageGate()
		if err != nil {
			return err
		}
		evt := events.NewRuntimeControlEvent(uuid.NewString(), events.EventType("mailbox.card_superseded"), "platform", "", payload, 0, runID, "",
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, anchor.EntityID), anchor.FlowInstance), now.UTC())
		if err := s.gateEvents.PublishInMutation(ctx, evt); err != nil {
			return fmt.Errorf("publish terminated flow decision card supersession: %w", err)
		}
	}
	instance.StateBuckets = carrier.PersistedStateBuckets()
	return nil
}
