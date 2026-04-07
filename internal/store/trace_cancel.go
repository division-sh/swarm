package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
	runtimedelivery "swarm/internal/runtime/deliverylifecycle"
)

func (s *PostgresStore) CancelActiveRunWorkByProducer(ctx context.Context, producerID string) ([]runtimedelivery.Transition, error) {
	producerID = strings.TrimSpace(producerID)
	if producerID == "" {
		return nil, nil
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin cancel runs tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	runRows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT e.run_id::text
		FROM events e
		WHERE e.run_id IS NOT NULL
		  AND COALESCE(e.produced_by, '') = $1
		  AND e.event_name NOT LIKE 'platform.%'
		  AND EXISTS (
			SELECT 1
			FROM events re
			JOIN event_deliveries d ON d.event_id = re.event_id
			WHERE re.run_id = e.run_id
			  AND d.status IN ('pending', 'in_progress')
		  )
		ORDER BY e.run_id::text ASC
	`, producerID)
	if err != nil {
		return nil, fmt.Errorf("query active runs for producer %s: %w", producerID, err)
	}
	defer runRows.Close()

	runIDs := make([]string, 0, 8)
	for runRows.Next() {
		var runID string
		if err := runRows.Scan(&runID); err != nil {
			return nil, fmt.Errorf("scan active run id: %w", err)
		}
		runID = strings.TrimSpace(runID)
		if runID != "" {
			runIDs = append(runIDs, runID)
		}
	}
	if err := runRows.Err(); err != nil {
		return nil, fmt.Errorf("read active run ids: %w", err)
	}
	if len(runIDs) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit empty cancel runs tx: %w", err)
		}
		committed = true
		return nil, nil
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			d.event_id::text,
			d.subscriber_id
		FROM event_deliveries d
		JOIN events e ON e.event_id = d.event_id
		WHERE e.run_id::text = ANY($1::text[])
		  AND d.subscriber_type = 'agent'
		  AND d.status IN ('pending', 'in_progress')
		ORDER BY d.event_id::text ASC, d.subscriber_id ASC
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("query kill_previous delivery targets: %w", err)
	}
	defer rows.Close()

	type cancelTarget struct {
		eventID string
		agentID string
	}
	targets := make([]cancelTarget, 0, 16)
	for rows.Next() {
		var (
			eventID string
			agentID string
		)
		if err := rows.Scan(&eventID, &agentID); err != nil {
			return nil, fmt.Errorf("scan kill_previous delivery target: %w", err)
		}
		targets = append(targets, cancelTarget{
			eventID: strings.TrimSpace(eventID),
			agentID: strings.TrimSpace(agentID),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read kill_previous delivery targets: %w", err)
	}

	transitions := make([]runtimedelivery.Transition, 0, len(targets))
	for _, target := range targets {
		delivery, err := s.lockAgentDeliveryTx(ctx, tx, target.eventID, target.agentID)
		if err != nil {
			return nil, err
		}
		if !delivery.found {
			continue
		}
		prevState, ok := runtimedelivery.StateFromDelivery(delivery.status, delivery.activeSessionID)
		if !ok || (prevState != runtimedelivery.StateQueued && prevState != runtimedelivery.StateLaunching && prevState != runtimedelivery.StateActive) {
			continue
		}
		receipt, err := s.applyDeliveryBackedTerminalTransitionTx(ctx, tx, target.eventID, target.agentID, delivery, deliveryBackedTerminalTransitionRequest{
			reasonCode:   "cancelled_by_kill_previous",
			errorText:    "cancelled by --kill-previous",
			retryAdvance: 0,
		})
		if err != nil {
			return nil, fmt.Errorf("cancel kill_previous deliveries/receipts: %w", err)
		}
		transitions = append(transitions, runtimedelivery.Transition{
			EventID:         target.eventID,
			AgentID:         target.agentID,
			EntityID:        delivery.entityID,
			State:           runtimedelivery.StateExhausted,
			PreviousState:   prevState,
			Reason:          "cancelled_by_kill_previous",
			TerminalOutcome: "cancelled_by_kill_previous",
			Error:           receipt.Error,
			RetryCount:      receipt.RetryCount,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit cancel runs tx: %w", err)
	}
	committed = true
	return transitions, nil
}
