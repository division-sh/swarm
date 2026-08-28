package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (s *RunForkSQLiteOwner) requireRunForkSelectedContractExecutionAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunForkSQLiteOwner) LoadRunForkSelectedContractSourceEventModes(ctx context.Context, sourceRunID string, sourceEventIDs []string) (modes []executionmode.Mode, err error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite store is required")
	}
	sourceRunID = strings.TrimSpace(sourceRunID)
	if sourceRunID == "" {
		return nil, fmt.Errorf("source run_id is required")
	}
	ids := uniqueNonEmptyStrings(sourceEventIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	err = s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var sourceStatus string
		if err := tx.QueryRowContext(txctx, `SELECT status FROM runs WHERE run_id = $1`, sourceRunID).Scan(&sourceStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return &runtimerunlifecycle.RunNotFoundError{RunID: sourceRunID}
			}
			return fmt.Errorf("load selected-contract source event admission status: %w", err)
		}
		if !runForkSelectedContractBranchSourceStatusSupported(sourceStatus) {
			state, parseErr := runtimerunlifecycle.ParseState(sourceStatus)
			if parseErr != nil {
				return parseErr
			}
			return fmt.Errorf("selected-contract source event admission state %s is unsupported", state)
		}
		events, err := loadSQLiteRunForkSelectedContractEvents(txctx, tx, ids)
		if err != nil {
			return fmt.Errorf("load selected-contract source event modes: %w", err)
		}
		if len(events) != len(ids) {
			return fmt.Errorf("selected-contract source event admission found %d of %d events", len(events), len(ids))
		}
		modes = make([]executionmode.Mode, 0, len(events))
		for _, event := range events {
			if event.RunID() != sourceRunID {
				return fmt.Errorf("selected-contract source event %s does not belong to source run %s", event.ID(), sourceRunID)
			}
			modes = append(modes, event.ExecutionMode())
		}
		return nil
	})
	return modes, err
}

func (s *RunForkSQLiteOwner) LoadRunForkSelectedContractSourceEvents(ctx context.Context, sourceRunID, forkRunID string, sourceEventIDs []string, workflowStates []runfork.RunForkSelectedContractWorkflowState) (out []runfork.RunForkSelectedContractSourceEvent, err error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite store is required")
	}
	sourceRunID = strings.TrimSpace(sourceRunID)
	forkRunID = strings.TrimSpace(forkRunID)
	if sourceRunID == "" || forkRunID == "" {
		return nil, fmt.Errorf("source and fork run_id are required")
	}
	ids := uniqueNonEmptyStrings(sourceEventIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	err = s.runRuntimeMutation(ctx, "sqlite selected-contract source event preparation", func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		var sourceStatus string
		if err := tx.QueryRowContext(txctx, `SELECT status FROM runs WHERE run_id = $1`, sourceRunID).Scan(&sourceStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return &runtimerunlifecycle.RunNotFoundError{RunID: sourceRunID}
			}
			return fmt.Errorf("load selected-contract source event preparation status: %w", err)
		}
		if !runForkSelectedContractBranchSourceStatusSupported(sourceStatus) {
			state, parseErr := runtimerunlifecycle.ParseState(sourceStatus)
			if parseErr != nil {
				return parseErr
			}
			return fmt.Errorf("selected-contract source event preparation state %s is unsupported", state)
		}
		if err := requireSQLiteRunActive(txctx, tx, forkRunID); err != nil {
			return fmt.Errorf("admit selected-contract source event preparation fork: %w", err)
		}
		events, err := loadSQLiteRunForkSelectedContractEvents(txctx, tx, ids)
		if err != nil {
			return fmt.Errorf("load selected-contract source events: %w", err)
		}
		out = make([]runfork.RunForkSelectedContractSourceEvent, 0, len(ids))
		for _, event := range events {
			if event.RunID() != sourceRunID {
				return fmt.Errorf("selected-contract source event %s does not belong to source run %s", event.ID(), sourceRunID)
			}
			out = append(out, runfork.RunForkSelectedContractSourceEvent{
				SourceEventID: event.ID(), EventName: string(event.Type()), ExecutionMode: event.ExecutionMode(),
				EntityID: event.EntityID(), FlowInstance: event.FlowInstance(), Scope: string(event.Scope()),
				RoutingSource: event.RoutingSource(), Payload: event.Payload(),
			})
		}
		for idx := range out {
			projected, err := projectRunForkSelectedContractSourceEventWorkflowState(sourceRunID, forkRunID, workflowStates, out[idx])
			if err != nil {
				return err
			}
			prepared, err := prepareRunForkSelectedContractSourceEvent(txctx, tx, story, forkRunID, projected)
			if err != nil {
				return err
			}
			out[idx] = prepared
		}
		if err := story.Finalize(txctx); err != nil {
			return fmt.Errorf("finalize selected-contract source event author activity: %w", err)
		}
		if _, err := runforkrevision.FinalizeSQLite(txctx, tx, runforkrevision.NewEffects()); err != nil {
			return fmt.Errorf("finalize selected-contract source event preparation revisions: %w", err)
		}
		return nil
	})
	return out, err
}

func (s *RunForkSQLiteOwner) EnsureRunForkNoPostForkCommittedReplayScopeMarkers(ctx context.Context, sourceRunID, forkEventID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite store is required")
	}
	return s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := runforkrevision.ValidateCompleteSQLite(txctx, tx, sourceRunID); err != nil {
			return err
		}
		var revision int64
		if err := tx.QueryRowContext(txctx, `
			SELECT MIN(revision)
			FROM run_fork_fact_revisions
			WHERE run_id = $1 AND family = 'events' AND fact_key = $2 AND present
		`, sourceRunID, forkEventID).Scan(&revision); err != nil {
			return fmt.Errorf("resolve committed replay-scope fork revision: %w", err)
		}
		if revision <= 0 {
			return fmt.Errorf("committed replay-scope fork event is not revisioned; recreate the store and retry")
		}
		return ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision(txctx, tx, sourceRunID, revision)
	})
}

func (s *RunForkSQLiteOwner) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (result runfork.RunForkActivation, err error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkActivation{}, fmt.Errorf("sqlite store is required")
	}
	return activateRunForkForSelectedContractExecution(ctx, req, sqliteRunForkSelectedContractActivationPort(s))
}

func (s *RunForkSQLiteOwner) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite store is required")
	}
	return discardMaterializedSelectedContractExecutionFork(ctx, forkRunID, sqliteRunForkSelectedContractDiscardPort(s))
}

func guardSQLiteSelectedContractForkDependencies(ctx context.Context, tx *sql.Tx, forkRunID string) error {
	var dependentRunID string
	err := tx.QueryRowContext(ctx, `
		SELECT run_id FROM runs
		WHERE forked_from_run_id = $1 AND run_id <> $1
		ORDER BY run_id LIMIT 1
	`, forkRunID).Scan(&dependentRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect selected-contract fork dependencies: %w", err)
	}
	return fmt.Errorf("cannot delete source run %s while dependent fork %s remains", forkRunID, dependentRunID)
}

func addRunForkActivationBlocker(result *runfork.RunForkActivation, err error) error {
	if blocker, fact, ok := runForkReplayResumeBlockerFromError(err); ok {
		result.UnsupportedBlockers = appendRunForkBlocker(result.UnsupportedBlockers, blocker)
		result.ReplayResumeAdmission = runForkReplayResumeAdmissionWithBlocker(result.ReplayResumeAdmission, fact, blocker)
	}
	return err
}

func insertSQLiteRunForkSelectedContractBranchDivergence(ctx context.Context, tx *sql.Tx, divergence runfork.RunForkSelectedContractBranchDivergence) error {
	if divergence.CreatedAt.IsZero() {
		divergence.CreatedAt = time.Now().UTC()
	}
	facts, err := json.Marshal(uniqueNonEmptyStrings(divergence.SourceAdvancedFacts))
	if err != nil {
		return fmt.Errorf("encode selected-contract branch divergence facts: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_branch_divergences (
			fork_run_id, source_run_id, fork_event_id, owner, policy,
			source_run_status_at_activation, source_run_status_after_activation,
			source_frozen, source_advanced_facts, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (fork_run_id) DO UPDATE SET
			owner = EXCLUDED.owner, policy = EXCLUDED.policy,
			source_run_status_at_activation = EXCLUDED.source_run_status_at_activation,
			source_run_status_after_activation = EXCLUDED.source_run_status_after_activation,
			source_frozen = EXCLUDED.source_frozen,
			source_advanced_facts = EXCLUDED.source_advanced_facts,
			created_at = EXCLUDED.created_at
	`, divergence.ForkRunID, divergence.SourceRunID, divergence.ForkEventID, divergence.Owner, divergence.Policy,
		divergence.SourceRunStatusAtActivation, divergence.SourceRunStatusAfterActivation, divergence.SourceFrozen, string(facts), divergence.CreatedAt)
	if err != nil {
		return fmt.Errorf("record selected-contract branch divergence: %w", err)
	}
	return nil
}

func ensureSQLiteRunForkSelectedContractExecutionForkState(ctx context.Context, tx *sql.Tx, forkRunID string, allowedSourceEventIDs []string) error {
	allowedEvents := uniqueNonEmptyStrings(allowedSourceEventIDs)
	if len(allowedEvents) == 0 {
		return ensureRunForkActivationNoForkReplayState(ctx, tx, sqliteDeliveryAdapter, forkRunID)
	}
	for _, sourceEventID := range allowedEvents {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_fork_selected_contract_executions WHERE fork_run_id = $1 AND source_event_id = $2)`, forkRunID, sourceEventID).Scan(&exists); err != nil {
			return fmt.Errorf("check selected-contract execution lineage completeness: %w", err)
		}
		if !exists {
			return runForkReplayResumeError("fork_selected_contract_execution_lineage_missing", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_selected_contract_execution_lineage_missing")
		}
	}
	deliverySnapshots, err := sqliteDeliveryAdapter.AgentSnapshotsForRun(ctx, tx, forkRunID)
	if err != nil {
		return fmt.Errorf("check selected-contract fork delivery snapshots: %w", err)
	}
	allowedSet := make(map[string]struct{}, len(allowedEvents))
	for _, eventID := range allowedEvents {
		allowedSet[eventID] = struct{}{}
	}
	selectedAgents := []map[string]string{}
	for _, snapshot := range deliverySnapshots {
		if snapshot.Status != runtimedelivery.StatusDelivered {
			return runForkReplayResumeError("fork_selected_contract_agent_delivery_incomplete", runfork.RunForkReplayResumeFactForkReplayState, fmt.Sprintf("fork activation blocked: fork_selected_contract_agent_delivery_incomplete: delivery %s for %s/%s is %s", snapshot.DeliveryID, snapshot.SubscriberClass, snapshot.SubscriberID, snapshot.Status))
		}
		rows, err := tx.QueryContext(ctx, `SELECT source_event_id FROM run_fork_selected_contract_executions WHERE fork_event_id = $1 AND fork_run_id = $2`, snapshot.EventID, forkRunID)
		if err != nil {
			return fmt.Errorf("check selected-contract delivery lineage: %w", err)
		}
		selected := false
		for rows.Next() {
			var sourceEventID string
			if err := rows.Scan(&sourceEventID); err != nil {
				rows.Close()
				return err
			}
			_, selected = allowedSet[sourceEventID]
			if selected {
				break
			}
		}
		rows.Close()
		if selected {
			identity := snapshot.Route.AgentIdentity.Normalize()
			if err := identity.Validate(); err != nil {
				return fmt.Errorf("check selected-contract delivery agent identity: %w", err)
			}
			if identity.AgentID() != strings.TrimSpace(snapshot.SubscriberID) {
				return fmt.Errorf("check selected-contract delivery agent identity: subscriber %q conflicts with %s", snapshot.SubscriberID, identity.Description())
			}
			selectedAgents = append(selectedAgents, map[string]string{"agent_id": identity.AgentID(), "flow_instance": identity.FlowInstance()})
		}
	}
	allowedJSON, _ := json.Marshal(allowedEvents)
	platformJSON, _ := json.Marshal(runForkSelectedContractForkLocalRuntimePlatformEventNames())
	agentsJSON, _ := json.Marshal(selectedAgents)
	var strayEvents int
	if err := tx.QueryRowContext(ctx, `
		WITH RECURSIVE selected_agents(agent_id, flow_instance) AS (
			SELECT json_extract(value, '$.agent_id'), json_extract(value, '$.flow_instance') FROM json_each($4)
		), selected_tree(event_id) AS (
			SELECT e.event_id FROM events e
			JOIN run_fork_selected_contract_executions x ON x.fork_event_id = e.event_id AND x.fork_run_id = $1
			WHERE e.run_id = $1 AND x.source_event_id IN (SELECT value FROM json_each($2))
			UNION
			SELECT e.event_id FROM events e JOIN selected_agents a
			  ON a.agent_id = e.produced_by AND a.flow_instance = COALESCE(json_extract(e.source_route, '$.flow_instance'), '')
			WHERE e.run_id = $1 AND e.produced_by_type = 'agent'
			UNION
			SELECT child.event_id FROM events child JOIN selected_tree parent ON child.source_event_id = parent.event_id
			WHERE child.run_id = $1 AND (child.event_name NOT LIKE 'platform.%' OR child.event_name IN (SELECT value FROM json_each($3)))
		)
		SELECT COUNT(*) FROM events e WHERE e.run_id = $1 AND NOT EXISTS (SELECT 1 FROM selected_tree tree WHERE tree.event_id = e.event_id)
	`, forkRunID, string(allowedJSON), string(platformJSON), string(agentsJSON)).Scan(&strayEvents); err != nil {
		return fmt.Errorf("check selected-contract fork event lineage: %w", err)
	}
	if strayEvents > 0 {
		return runForkReplayResumeError("fork_events_not_selected_contract_lineage", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_events_not_selected_contract_lineage")
	}
	for _, snapshot := range deliverySnapshots {
		var belongs bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE run_id = $1 AND event_id = $2)`, forkRunID, snapshot.EventID).Scan(&belongs); err != nil {
			return fmt.Errorf("check selected-contract fork delivery event: %w", err)
		}
		if !belongs {
			return runForkReplayResumeError("fork_deliveries_not_selected_contract_lineage", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_deliveries_not_selected_contract_lineage")
		}
	}
	return nil
}
