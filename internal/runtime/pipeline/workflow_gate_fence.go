package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
)

type GateDecisionFence interface {
	CommitDecision(context.Context, decisioncard.Card, string, time.Time) error
}

func (s *WorkflowInstanceStore) CommitDecision(ctx context.Context, card decisioncard.Card, eventID string, now time.Time) error {
	switch card.Anchor.Kind() {
	case decisioncard.AnchorKindStageGate:
		return s.commitGateDecision(ctx, card, eventID, now)
	case decisioncard.AnchorKindHumanTask:
		return nil
	default:
		return fmt.Errorf("decision-card anchor kind %q is not registered", card.Anchor.Kind())
	}
}

func (s *WorkflowInstanceStore) commitGateDecision(ctx context.Context, card decisioncard.Card, eventID string, now time.Time) error {
	if s == nil || !s.Enabled() {
		return fmt.Errorf("workflow instance store is required for gate decision")
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	return s.MutateE(ctx, anchor.EntityID, func(instance *WorkflowInstance) error {
		carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
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
		return nil
	})
}

func (s *WorkflowInstanceStore) RequireGateRouteAdmitted(ctx context.Context, runID string) error {
	if s == nil || !s.Enabled() {
		return fmt.Errorf("workflow instance store is required for gate routing")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("gate route run id is required")
	}
	query := `SELECT LOWER(COALESCE(status, '')) FROM runs WHERE run_id = $1::uuid`
	if s.isSQLite() {
		query = `SELECT LOWER(COALESCE(status, '')) FROM runs WHERE run_id = ?`
	}
	var status string
	if err := dbQueryRowContext(ctx, s.db, query, runID).Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("gate route run %s is unavailable", runID)
		}
		return err
	}
	if strings.TrimSpace(status) != "running" {
		return fmt.Errorf("gate route run %s is not routable in status %s", runID, strings.TrimSpace(status))
	}
	return nil
}
