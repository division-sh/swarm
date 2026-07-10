package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

type normalRunCompletionCandidate struct {
	RunID  string
	Status string
}

func (s *PostgresStore) ConvergeNormalRunCompletion(ctx context.Context, eventID string, workflowTerminalStates []string, flowTerminalStates map[string][]string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	eventID = sanitizeOptionalUUID(eventID)
	if eventID == "" {
		return nil
	}
	workflowTerminals := normalRunCompletionStateSet(workflowTerminalStates)
	flowTerminals := normalRunCompletionFlowStateSets(flowTerminalStates)
	if len(workflowTerminals) == 0 && len(flowTerminals) == 0 {
		return nil
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if !normalRunCompletionSupported(caps) {
		return nil
	}
	return withEventStoreRetry(ctx, nil, func() error {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin normal run completion tx: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		candidate, found, err := lockNormalRunCompletionCandidateTx(ctx, tx, eventID)
		if err != nil || !found {
			return err
		}
		switch candidate.Status {
		case "completed":
			return nil
		case "running":
		default:
			return nil
		}
		if platformRec, platformFound, err := loadStandaloneRuntimePlatformRunRecord(ctx, tx, eventID); err != nil {
			return err
		} else if platformFound && isStandaloneRuntimePlatformRunRecord(platformRec) {
			return nil
		}
		ready, err := normalRunCompletionRunReadyTx(ctx, tx, candidate.RunID, workflowTerminals, flowTerminals)
		if err != nil || !ready {
			return err
		}
		if _, err := storerunlifecycle.MarkTerminal(ctx, tx, candidate.RunID, "completed", nil, time.Now().UTC(), runLifecycleOptions(caps)); err != nil {
			return fmt.Errorf("converge normal run completion: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit normal run completion tx: %w", err)
		}
		committed = true
		return nil
	})
}

func normalRunCompletionSupported(caps StoreSchemaCapabilities) bool {
	if !caps.Events.HasRuns || !caps.Events.RunTriggerColumns || !caps.Events.RunTerminalFields {
		return false
	}
	if caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogRunID {
		return false
	}
	if caps.EntityState != SchemaFlavorCanonical || !caps.EntityRunID {
		return false
	}
	if caps.Events.Deliveries != SchemaFlavorCanonical || !caps.Events.DeliveryRunID {
		return false
	}
	if caps.Events.Receipts != SchemaFlavorCanonical {
		return false
	}
	if caps.Schedules != SchemaFlavorCanonical {
		return false
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical || !caps.Conversations.SessionRunID {
		return false
	}
	return true
}

func lockNormalRunCompletionCandidateTx(ctx context.Context, tx *sql.Tx, eventID string) (normalRunCompletionCandidate, bool, error) {
	var candidate normalRunCompletionCandidate
	err := tx.QueryRowContext(ctx, `
		SELECT
			r.run_id::text,
			LOWER(COALESCE(r.status, ''))
		FROM events e
		INNER JOIN runs r ON r.run_id = e.run_id
		WHERE e.event_id = $1::uuid
		FOR UPDATE OF r
	`, eventID).Scan(&candidate.RunID, &candidate.Status)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return normalRunCompletionCandidate{}, false, nil
	case err != nil:
		return normalRunCompletionCandidate{}, false, fmt.Errorf("lock normal run completion candidate: %w", err)
	default:
		candidate.RunID = strings.TrimSpace(candidate.RunID)
		candidate.Status = strings.TrimSpace(candidate.Status)
		return candidate, candidate.RunID != "", nil
	}
}

func normalRunCompletionRunReadyTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	workflowTerminals map[string]struct{},
	flowTerminals map[string]map[string]struct{},
) (bool, error) {
	runID = nullUUIDString(runID)
	if runID == "" {
		return false, nil
	}
	checks := []func(context.Context, *sql.Tx, string) (bool, error){
		normalRunCompletionPipelinesSettledTx,
		normalRunCompletionDeliveriesSettledTx,
		normalRunCompletionTimersSettledTx,
		normalRunCompletionSessionLeasesSettledTx,
	}
	for _, check := range checks {
		ready, err := check(ctx, tx, runID)
		if err != nil || !ready {
			return ready, err
		}
	}
	return normalRunCompletionEntitiesTerminalTx(ctx, tx, runID, workflowTerminals, flowTerminals)
}

func normalRunCompletionPipelinesSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var unsettled bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events e
			LEFT JOIN event_receipts r
				ON r.event_id = e.event_id
				AND r.subscriber_type = 'platform'
				AND r.subscriber_id = 'pipeline'
			WHERE e.run_id = $1::uuid
			  AND e.event_name <> '`+runtimeLogEventName+`'
			  AND (r.event_id IS NULL OR COALESCE(r.outcome, '') <> 'success')
		)
	`, runID).Scan(&unsettled); err != nil {
		return false, fmt.Errorf("check normal run pipeline settlement: %w", err)
	}
	return !unsettled, nil
}

func normalRunCompletionDeliveriesSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries d
			WHERE d.run_id = $1::uuid
			  AND `+activeRunQuiescenceDeliveryPredicateSQL("d")+`
		)
	`, runID).Scan(&active); err != nil {
		return false, fmt.Errorf("check normal run active deliveries: %w", err)
	}
	return !active, nil
}

func normalRunCompletionTimersSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM timers
			WHERE run_id = $1::uuid
			  AND status = 'active'
		)
	`, runID).Scan(&active); err != nil {
		return false, fmt.Errorf("check normal run active timers: %w", err)
	}
	return !active, nil
}

func normalRunCompletionSessionLeasesSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agent_sessions
			WHERE run_id = $1::uuid
			  AND status = 'active'
			  AND lease_holder IS NOT NULL
			  AND lease_expires_at IS NOT NULL
			  AND lease_expires_at > now()
		)
	`, runID).Scan(&active); err != nil {
		return false, fmt.Errorf("check normal run active session leases: %w", err)
	}
	return !active, nil
}

func normalRunCompletionEntitiesTerminalTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	workflowTerminals map[string]struct{},
	flowTerminals map[string]map[string]struct{},
) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			LOWER(COALESCE(es.current_state, '')),
			COALESCE(es.flow_instance, ''),
			COALESCE(fi.flow_template, '')
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = $1::uuid
		FOR SHARE OF es
	`, runID)
	if err != nil {
		return false, fmt.Errorf("load normal run entity terminality: %w", err)
	}
	defer rows.Close()
	seen := false
	for rows.Next() {
		seen = true
		var state, flowInstance, flowTemplate string
		if err := rows.Scan(&state, &flowInstance, &flowTemplate); err != nil {
			return false, fmt.Errorf("scan normal run entity terminality: %w", err)
		}
		terminals, ok := normalRunCompletionTerminalSetForEntity(flowTemplate, flowInstance, workflowTerminals, flowTerminals)
		if !ok {
			return false, nil
		}
		if _, ok := terminals[strings.TrimSpace(strings.ToLower(state))]; !ok {
			return false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read normal run entity terminality: %w", err)
	}
	return seen, nil
}

func normalRunCompletionTerminalSetForEntity(
	flowTemplate, flowInstance string,
	workflowTerminals map[string]struct{},
	flowTerminals map[string]map[string]struct{},
) (map[string]struct{}, bool) {
	for _, raw := range []string{flowTemplate, flowInstance} {
		key := strings.Trim(strings.TrimSpace(raw), "/")
		if key == "" {
			continue
		}
		if terminals, ok := normalRunCompletionBestFlowTerminalSet(key, flowTerminals); ok {
			return terminals, true
		}
	}
	if strings.TrimSpace(flowInstance) == "" && len(workflowTerminals) > 0 {
		return workflowTerminals, true
	}
	return nil, false
}

func normalRunCompletionBestFlowTerminalSet(key string, sets map[string]map[string]struct{}) (map[string]struct{}, bool) {
	key = strings.Trim(strings.TrimSpace(key), "/")
	if key == "" || len(sets) == 0 {
		return nil, false
	}
	var (
		best    map[string]struct{}
		bestLen int
	)
	for candidate, terminals := range sets {
		candidate = strings.Trim(strings.TrimSpace(candidate), "/")
		if candidate == "" || len(terminals) == 0 {
			continue
		}
		if key == candidate || strings.HasPrefix(key, candidate+"/") {
			if len(candidate) > bestLen {
				best = terminals
				bestLen = len(candidate)
			}
		}
	}
	return best, best != nil
}

func normalRunCompletionStateSet(states []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, state := range states {
		state = strings.TrimSpace(strings.ToLower(state))
		if state != "" {
			out[state] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalRunCompletionFlowStateSets(states map[string][]string) map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}
	for key, values := range states {
		key = strings.Trim(strings.TrimSpace(key), "/")
		if key == "" {
			continue
		}
		if set := normalRunCompletionStateSet(values); len(set) > 0 {
			out[key] = set
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
