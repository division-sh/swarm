package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

// Recovery observes revocation, not the attempt's renewable wall-clock lease.
// Its caller owns the read snapshot or the complete native mutation prefix.
func selectAbandonedCompletionAttempts(ctx context.Context, tx *sql.Tx, attempts []completionRecoveryAttempt, now time.Time, sqlite bool) ([]completionRecoveryAttempt, error) {
	actors := make(map[agentidentity.Identity]agentpersistence.AgentExecutionObservation)
	runs := make(map[string]bool)
	var process *processbinding.LeaseStamp
	selected := make([]completionRecoveryAttempt, 0, len(attempts))
	for _, row := range attempts {
		if row.AuthorityKind == string(effects.AuthoritySelectedContractFork) {
			selected = append(selected, row)
			continue
		}
		if row.AuthorityKind == string(effects.AuthorityConversationForkChat) {
			abandoned, err := completionForkChatParentAbandoned(ctx, tx, row, now, sqlite)
			if err != nil {
				return nil, err
			}
			if abandoned {
				selected = append(selected, row)
			}
			continue
		}
		attempt, _, err := completionRecoverySettlement(row, effects.StateTerminalFailure, nil, now)
		if err != nil {
			return nil, err
		}
		if attempt.Authority.Kind != effects.AuthorityNormalAgent || !attempt.Authority.Valid() {
			return nil, fmt.Errorf("completion recovery has invalid ordinary authority for %s", row.AttemptID)
		}
		token := attempt.Authority.Normal
		actor, found := actors[token.Identity]
		if !found {
			actor, err = agentpersistence.ReadAgentExecution(ctx, tx, token, sqlite)
			if err != nil {
				return nil, err
			}
			actors[token.Identity] = actor
		}
		if !actor.Found || actor.State.RuntimeEpoch != token.RuntimeEpoch || actor.State.Generation != token.Generation || actor.State.Phase != manager.AgentLifecycleRunning {
			selected = append(selected, row)
			continue
		}
		if process == nil {
			stamp, err := generationauthority.ReadProcessGeneration(ctx, tx, sqlite)
			if err != nil {
				return nil, err
			}
			if err := stamp.Validate(); err != nil {
				return nil, fmt.Errorf("completion recovery process evidence: %w", err)
			}
			process = &stamp
		}
		binding := actor.State.ProcessBinding
		coordinates := processbinding.LeaseCoordinates{LeaseID: binding.ProcessAuthorityID, OwnerID: binding.ProcessOwnerID, BootID: binding.ProcessBootID}
		if process.Coordinates() != coordinates {
			selected = append(selected, row)
			continue
		}
		cancellation, err := readProviderOriginCancellation(ctx, tx, !sqlite, attempt, true, recoveryInspection)
		if err != nil {
			return nil, err
		}
		if cancellation.disposition != providerOriginNotCanceled {
			selected = append(selected, row)
			continue
		}
		runID := token.Identity.RunID
		active, known := runs[runID]
		if !known {
			query := `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id=$1 AND status IN (` + runLifecycleActiveStateSQLValues + `))`
			if !sqlite {
				query = strings.ReplaceAll(query, "run_id=$1", "run_id=$1::uuid")
			}
			if err := tx.QueryRowContext(ctx, query, runID).Scan(&active); err != nil {
				return nil, fmt.Errorf("completion recovery run admission: %w", err)
			}
			runs[runID] = active
		}
		if !active {
			selected = append(selected, row)
		}
	}
	return selected, nil
}

func completionForkChatParentAbandoned(ctx context.Context, tx *sql.Tx, row completionRecoveryAttempt, now time.Time, sqlite bool) (bool, error) {
	query := `SELECT EXISTS (
		SELECT 1 FROM conversation_fork_turns t
		JOIN conversation_forks f ON f.fork_id=t.fork_id
		JOIN runtime_external_effect_operations o ON o.fork_turn_id=t.fork_turn_id
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		WHERE o.operation_id=$1::uuid AND a.attempt_id=$2::uuid
		  AND t.fork_turn_id::text=a.authority_evidence->>'fork_turn_id'
		  AND t.fork_id::text=a.authority_evidence->>'fork_id'
		  AND f.source_run_id::text=a.authority_evidence->>'source_run_id'
		  AND t.bundle_hash=a.authority_evidence->>'bundle_hash'
		  AND t.actor_token_id=a.authority_evidence->>'actor_token_id'
		  AND t.request_occurrence_id::text=a.authority_evidence->>'request_occurrence_id'
		  AND t.request_hash=a.authority_evidence->>'request_hash'
		  AND t.execution_owner=a.execution_owner AND t.fence_generation=a.fence_generation
		  AND t.state IN ('prepared','executing') AND t.lease_expires_at<=$3
	)`
	if sqlite {
		query = strings.ReplaceAll(strings.ReplaceAll(query, "::uuid", ""), "::text", "")
	}
	var abandoned bool
	if err := tx.QueryRowContext(ctx, query, row.OperationID, row.AttemptID, now).Scan(&abandoned); err != nil {
		return false, fmt.Errorf("completion recovery exact forkchat parent: %w", err)
	}
	return abandoned, nil
}
