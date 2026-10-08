package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func recoverUnstartedCanceledTurn(ctx context.Context, tx *sql.Tx, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, row canceledTurnRecoveryRow) (effects.TurnExecutionResult, error) {
	if !row.originEvidence.Valid || row.currentAttempt.Valid || row.reason != string(deliverylifecycle.CancellationTerminate) ||
		(!row.admittedAttempt.Valid && (row.firstAttempt.Valid || row.bound.Valid || row.emit.Valid || row.timeoutEvent.Valid || row.launched != nil)) {
		return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery has incomplete or contradictory origin evidence")
	}
	origin, owner, err := decodeUnstartedOrigin([]byte(row.originEvidence.String))
	if err != nil {
		return effects.TurnExecutionResult{}, err
	}
	id, _, err := businessTurnIdentity(origin)
	if err != nil || id != row.turnID || owner.RunID != row.runID || owner.Route.InstancePath != row.flow {
		return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery contradicts its actual origin/owner")
	}
	if row.admittedAttempt.Valid {
		at, valid, err := sqliteTimeValue(row.requested)
		if err != nil || !valid {
			return effects.TurnExecutionResult{}, fmt.Errorf("retried unstarted turn lacks its intent timestamp")
		}
		anchor, err := loadCanceledFirstAttempt(ctx, tx, postgres, row.admittedAttempt.String, at)
		if err != nil {
			return effects.TurnExecutionResult{}, err
		}
		anchorOwner, err := businessTurnOwner(anchor.Authority)
		if err != nil || anchorOwner != owner || anchor.Authority.Target.AgentID != row.agentID {
			return effects.TurnExecutionResult{}, fmt.Errorf("retried unstarted turn contradicts its historical owner")
		}
		current := anchor
		current.Origin = origin
		if err := validateBusinessTurnAnchor(anchor, current); err != nil {
			return effects.TurnExecutionResult{}, err
		}
		if row.firstAttempt.Valid {
			first, err := loadCanceledFirstAttempt(ctx, tx, postgres, row.firstAttempt.String, at)
			if err != nil {
				return effects.TurnExecutionResult{}, err
			}
			if err := validateBusinessTurnAnchor(first, current); err != nil {
				return effects.TurnExecutionResult{}, err
			}
		}
	}
	launched, hasLaunch, err := sqliteTimeValue(row.launched)
	if err != nil || hasLaunch != row.firstAttempt.Valid || row.bound.Valid != row.emit.Valid || row.bound.Valid != row.timeoutEvent.Valid {
		return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery has contradictory historical clock evidence")
	}
	clock := effects.LogicalTurnClock{Origin: origin, FirstAttempt: row.firstAttempt.String, LaunchedAt: launched, TimeoutEvent: row.timeoutEvent.String}
	if row.bound.Valid {
		clock.Timeout = &timeridentity.TurnTimeout{After: time.Duration(row.bound.Int64), Emit: row.emit.String}
		if err := clock.Timeout.Validate(); err != nil {
			return effects.TurnExecutionResult{}, err
		}
		clock.DeadlineAt = launched.Add(clock.Timeout.After)
	}
	if hasLaunch {
		if err := clock.Validate(); err != nil {
			return effects.TurnExecutionResult{}, err
		}
	}
	var directive agentcontrol.DirectiveOperation
	if origin.Kind == effects.CompletionOriginDirective {
		if directives == nil {
			return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery lacks its directive owner")
		}
		directive, err = directives.DirectiveTurnOriginTx(ctx, tx, origin.Directive, false)
		if err != nil {
			return effects.TurnExecutionResult{}, err
		}
	}
	if err := validateUnstartedTurnOwner(ctx, tx, delivery, origin, owner, row.agentID, directive); err != nil {
		return effects.TurnExecutionResult{}, err
	}
	physical, err := unstartedPhysicalAttemptCount(ctx, tx, origin)
	if err != nil {
		return effects.TurnExecutionResult{}, err
	}
	if physical != 0 {
		return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery cannot consume physical attempt ownership")
	}
	at, valid, err := sqliteTimeValue(row.requested)
	if err != nil || !valid {
		return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery has no exact intent timestamp")
	}
	intent := effects.TurnCancellation{Committed: true, Requested: true, Origin: origin, Reason: deliverylifecycle.CancellationTerminate, CauseEvent: row.cause, RequestedAt: at}
	if err := intent.ValidateIntent(); err != nil {
		return effects.TurnExecutionResult{}, err
	}
	// No provider attempt or execution mode exists to re-admit. This result
	// carries only the already-canceled work origin for owned cleanup/settlement.
	return effects.TurnExecutionResult{Attempt: effects.Attempt{Origin: origin}, Cancellation: intent}, nil
}
