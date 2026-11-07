package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

type turnOwnershipContextKey struct{}

type turnOwnershipObservation struct {
	tx       *sql.Tx
	actors   map[agentidentity.Identity]events.InstanceOwnership
	hashes   map[string]string
	attempts map[string]retainedTurnCoordinate
	locked   map[agentidentity.Identity]bool
}

type retainedTurnCoordinate struct {
	id, operation, hash, evidence, origin, delivery, directive string
	actor                                                      agentidentity.Identity
}

func peekRetainedTurnTx(ctx context.Context, tx *sql.Tx, id string) (retainedTurnCoordinate, error) {
	var row retainedTurnCoordinate
	err := tx.QueryRowContext(ctx, `SELECT CAST(a.attempt_id AS TEXT),CAST(a.operation_id AS TEXT),o.bundle_hash,
		CAST(a.authority_evidence AS TEXT),COALESCE(a.origin_kind,''),COALESCE(CAST(a.origin_delivery_id AS TEXT),''),COALESCE(CAST(a.origin_directive_operation_id AS TEXT),'')
		FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE a.attempt_id=$1 AND o.effect_kind='provider_turn' AND o.authority_kind IN ('normal_agent','selected_contract_fork')`, id).
		Scan(&row.id, &row.operation, &row.hash, &row.evidence, &row.origin, &row.delivery, &row.directive)
	if err != nil {
		return row, err
	}
	var evidence completionRecoveryAuthorityEvidence
	if err := json.Unmarshal([]byte(row.evidence), &evidence); err != nil {
		return row, err
	}
	row.actor = evidence.UsageTarget.AgentIdentity
	if row.actor.Validate() != nil || row.actor != row.actor.Normalize() || evidence.UsageTarget.RunID != row.actor.RunID {
		return row, fmt.Errorf("retained turn has contradictory actor coordinates")
	}
	return row, nil
}

// Peeks choose locks only. No decoder or anchor can acquire a missing prefix.
func prepareRetainedTurnOwnershipTx(ctx context.Context, tx *sql.Tx, postgres bool, owner providerDrainDirectiveOwner, ids []string, extraActors ...agentidentity.Identity) (context.Context, error) {
	unique := make(map[string]bool, len(ids))
	var peeks []retainedTurnCoordinate
	actors := append([]agentidentity.Identity(nil), extraActors...)
	for _, id := range ids {
		if id == "" || unique[id] {
			continue
		}
		unique[id] = true
		row, err := peekRetainedTurnTx(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		peeks, actors = append(peeks, row), append(actors, row.actor)
	}
	if len(peeks) == 0 && len(actors) == 0 {
		return ctx, nil
	}
	if prior, _ := ctx.Value(turnOwnershipContextKey{}).(*turnOwnershipObservation); prior != nil && prior.tx == tx {
		for _, actor := range actors {
			if !prior.locked[actor] {
				return nil, fmt.Errorf("retained turn cannot extend an acquired ownership prefix")
			}
		}
		for _, row := range peeks {
			if prior.attempts[row.id] != row {
				return nil, fmt.Errorf("retained turn differs from its outer observed coordinates")
			}
		}
		return ctx, nil
	}
	if owner == nil && len(peeks) != 0 {
		return nil, fmt.Errorf("retained turn requires its native ownership reader")
	}
	if err := agentpersistence.LockTurnOwnershipActorsTx(ctx, tx, actors, !postgres); err != nil {
		return nil, err
	}
	// Actor locks are complete before the ordered source/run suffix begins.
	sort.Slice(peeks, func(i, j int) bool {
		if peeks[i].actor.RunID != peeks[j].actor.RunID {
			return peeks[i].actor.RunID < peeks[j].actor.RunID
		}
		return agentidentity.Less(peeks[i].actor, peeks[j].actor)
	})
	scope := &turnOwnershipObservation{tx: tx, actors: make(map[agentidentity.Identity]events.InstanceOwnership), hashes: make(map[string]string), attempts: make(map[string]retainedTurnCoordinate), locked: make(map[agentidentity.Identity]bool)}
	for _, actor := range actors {
		scope.locked[actor] = true
	}
	for _, row := range peeks {
		scope.attempts[row.id] = row
		if hash, found := scope.hashes[row.actor.RunID]; found && hash != row.hash {
			return nil, fmt.Errorf("retained turn set crosses its exact native source")
		}
		if _, found := scope.actors[row.actor]; found {
			continue
		}
		observed, err := owner.ReadRetainedTurnOwnershipTx(ctx, tx, row.actor, row.hash)
		if err != nil {
			return nil, err
		}
		if err := observed.Validate(); err != nil {
			return nil, err
		}
		scope.actors[row.actor], scope.hashes[row.actor.RunID] = observed.Clone(), row.hash
	}
	for _, before := range peeks {
		after, err := peekRetainedTurnTx(ctx, tx, before.id)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(before, after) {
			return nil, fmt.Errorf("retained turn coordinates changed under the ownership prefix")
		}
	}
	return context.WithValue(ctx, turnOwnershipContextKey{}, scope), nil
}

func withAdmittedTurnOwnership(ctx context.Context, tx *sql.Tx, actor agentidentity.Identity, observed events.InstanceOwnership) context.Context {
	return context.WithValue(ctx, turnOwnershipContextKey{}, &turnOwnershipObservation{tx: tx,
		actors: map[agentidentity.Identity]events.InstanceOwnership{actor: observed.Clone()}})
}

func projectRetainedTurnOwnership(ctx context.Context, tx *sql.Tx, attempt effects.Attempt) (effects.Attempt, error) {
	scope, _ := ctx.Value(turnOwnershipContextKey{}).(*turnOwnershipObservation)
	if scope == nil || scope.tx != tx {
		return effects.Attempt{}, fmt.Errorf("retained turn requires its outer native ownership prefix")
	}
	current, found := scope.actors[attempt.Authority.Target.AgentIdentity]
	if !found || current.Validate() != nil {
		return effects.Attempt{}, fmt.Errorf("retained turn differs from its observed actor set")
	}
	if scope.attempts != nil {
		row, found := scope.attempts[attempt.AttemptID]
		if !found || row.operation != attempt.OperationID || row.actor != attempt.Authority.Target.AgentIdentity {
			return effects.Attempt{}, fmt.Errorf("retained turn differs from its observed attempt set")
		}
		var evidence completionRecoveryAuthorityEvidence
		if err := json.Unmarshal([]byte(row.evidence), &evidence); err != nil {
			return effects.Attempt{}, err
		}
		if len(evidence.InstanceOwnership) != 0 {
			var captured events.InstanceOwnership
			if json.Unmarshal(evidence.InstanceOwnership, &captured) != nil || !captured.Equal(attempt.InstanceOwnership) {
				return effects.Attempt{}, fmt.Errorf("retained turn differs from its captured ownership")
			}
		} else {
			// No caller-supplied current life can replace absent retained evidence.
			attempt.InstanceOwnership = events.InstanceOwnership{}
		}
	}
	if attempt.InstanceOwnership.Validate() == nil {
		// A physical tail retains its captured life; current publication/admission
		// checks decide staleness, never a late restamp by this decoder.
		if attempt.InstanceOwnership.RunOwned != current.RunOwned || attempt.InstanceOwnership.Life != nil &&
			(attempt.InstanceOwnership.Life.RunID != current.Life.RunID || attempt.InstanceOwnership.Life.FlowInstance != current.Life.FlowInstance) {
			return effects.Attempt{}, fmt.Errorf("retained turn contradicts its native ownership")
		}
		return attempt, nil
	}
	if attempt.InstanceOwnership.RunOwned || attempt.InstanceOwnership.Life != nil {
		return effects.Attempt{}, fmt.Errorf("retained turn has malformed captured ownership")
	}
	// R103/R104 applies to no-counter real-deployment data only. Candidate
	// scratch stores are outside that promise; no discriminator or repair here.
	projected := current.Clone()
	if projected.Life != nil {
		projected.Life.Life = 1
	}
	attempt.InstanceOwnership = projected
	return attempt, nil
}

func requireCurrentTurnOwnership(ctx context.Context, tx *sql.Tx, attempt effects.Attempt) error {
	if attempt.Kind != effects.KindProviderTurn || !attempt.Authority.HasBusinessTurnOrigin() {
		return nil
	}
	scope, _ := ctx.Value(turnOwnershipContextKey{}).(*turnOwnershipObservation)
	if scope == nil || scope.tx != tx {
		return fmt.Errorf("turn currentness requires its native ownership observation")
	}
	current, found := scope.actors[attempt.Authority.Target.AgentIdentity]
	if !found || !attempt.InstanceOwnership.Equal(current) {
		return turnInstanceOwnershipChanged(attempt.Authority, attempt.InstanceOwnership, current)
	}
	return nil
}

type turnAnchorCoordinates struct {
	admitted, current, first sql.NullString
}

func peekTurnAnchorsTx(ctx context.Context, tx *sql.Tx, attempt effects.Attempt) (turnAnchorCoordinates, error) {
	id, _, err := businessTurnIdentity(attempt.Origin)
	if err != nil {
		return turnAnchorCoordinates{}, err
	}
	var anchors turnAnchorCoordinates
	err = tx.QueryRowContext(ctx, `SELECT CAST(admitted_attempt_id AS TEXT),CAST(current_attempt_id AS TEXT),CAST(first_attempt_id AS TEXT)
		FROM runtime_agent_turn_lifetimes WHERE turn_id=$1`, id).Scan(&anchors.admitted, &anchors.current, &anchors.first)
	if errors.Is(err, sql.ErrNoRows) {
		return anchors, nil
	}
	return anchors, err
}

// The outer owner enters before origin/attempt locks, including every retained
// anchor it may compare. The decoder consumes this transaction-local observation.
func prepareTurnAttemptOwnershipTx(ctx context.Context, tx *sql.Tx, postgres bool, owner providerDrainDirectiveOwner, attempt effects.Attempt) (context.Context, effects.Attempt, error) {
	if attempt.Kind != effects.KindProviderTurn || !attempt.Authority.HasBusinessTurnOrigin() {
		return ctx, attempt, nil
	}
	before, err := peekTurnAnchorsTx(ctx, tx, attempt)
	if err != nil {
		return nil, effects.Attempt{}, err
	}
	ctx, err = prepareRetainedTurnOwnershipTx(ctx, tx, postgres, owner,
		[]string{attempt.AttemptID, before.admitted.String, before.current.String, before.first.String})
	if err != nil {
		return nil, effects.Attempt{}, err
	}
	after, err := peekTurnAnchorsTx(ctx, tx, attempt)
	if err != nil || before != after {
		return nil, effects.Attempt{}, fmt.Errorf("turn anchors changed under the ownership prefix")
	}
	attempt, err = projectRetainedTurnOwnership(ctx, tx, attempt)
	return ctx, attempt, err
}

func peekProviderRecoveryIDsTx(ctx context.Context, tx *sql.Tx, runID string, actor *agentidentity.Identity) ([]string, error) {
	// Peeks include the anchors of unfinished logical turns, even when their
	// original physical attempts have already settled. This is the existing
	// recovery population, never a scan of arbitrary historical attempts.
	var agentID, nameOwner, nameSource, presence, scope, instance, path string
	if actor != nil {
		fields, err := actor.StorageFields()
		if err != nil {
			return nil, err
		}
		agentID, nameOwner, nameSource, presence = fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence
		scope, instance, path = fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath
	}
	rows, err := tx.QueryContext(ctx, `SELECT CAST(a.attempt_id AS TEXT) FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		WHERE o.effect_kind='provider_turn' AND o.authority_kind IN ('normal_agent','selected_contract_fork')
		AND (o.authority_kind='normal_agent' OR $1<>'') AND ($1='' OR CAST(o.agent_run_id AS TEXT)=$1)
		AND ($2='' OR (o.agent_id=$2 AND o.agent_name_owner=$3 AND o.agent_name_source=$4
		 AND o.agent_route_presence=$5 AND o.flow_scope_key=$6 AND o.flow_instance_id=$7 AND o.flow_instance=$8))
		AND a.origin_kind IN ('delivery','directive')
		AND (a.state IN ('authorized','launched','response_observed') OR EXISTS
		 (SELECT 1 FROM runtime_agent_turn_lifetimes t WHERE t.settled_at IS NULL AND
		  (a.attempt_id=t.admitted_attempt_id OR a.attempt_id=t.current_attempt_id OR a.attempt_id=t.first_attempt_id)))
		ORDER BY a.attempt_id`, runID, agentID, nameOwner, nameSource, presence, scope, instance, path)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return ids, nil
}

func prepareProviderRecoveryOwnershipTx(ctx context.Context, tx *sql.Tx, postgres bool, owner providerDrainDirectiveOwner, runID string, actor *agentidentity.Identity) (context.Context, error) {
	ids, err := peekProviderRecoveryIDsTx(ctx, tx, runID, actor)
	if err != nil {
		return nil, err
	}
	var extra []agentidentity.Identity
	if actor != nil {
		for _, id := range ids {
			row, err := peekRetainedTurnTx(ctx, tx, id)
			if err != nil {
				return nil, err
			}
			if row.actor != *actor {
				return nil, fmt.Errorf("startup turn recovery crosses its exact lifecycle actor")
			}
		}
		extra = append(extra, *actor)
	}
	ctx, err = prepareRetainedTurnOwnershipTx(ctx, tx, postgres, owner, ids, extra...)
	if err != nil {
		return nil, err
	}
	current, err := peekProviderRecoveryIDsTx(ctx, tx, runID, actor)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(ids, current) {
		return nil, fmt.Errorf("provider recovery population changed under its ownership prefix")
	}
	return ctx, nil
}

func (s *EffectPostgresOwner) PrepareStartupTurnOwnershipTx(ctx context.Context, tx *sql.Tx, actor agentidentity.Identity) (context.Context, error) {
	return prepareProviderRecoveryOwnershipTx(ctx, tx, true, s.directives, actor.RunID, &actor)
}

func (s *EffectSQLiteOwner) PrepareStartupTurnOwnershipTx(ctx context.Context, tx *sql.Tx, actor agentidentity.Identity) (context.Context, error) {
	return prepareProviderRecoveryOwnershipTx(ctx, tx, false, s.directives, actor.RunID, &actor)
}

func (s *EffectPostgresOwner) PrepareSelectedTurnOwnershipTx(ctx context.Context, tx *sql.Tx, runID string) (context.Context, error) {
	return prepareProviderRecoveryOwnershipTx(ctx, tx, true, s.directives, runID, nil)
}

func (s *EffectSQLiteOwner) PrepareSelectedTurnOwnershipTx(ctx context.Context, tx *sql.Tx, runID string) (context.Context, error) {
	return prepareProviderRecoveryOwnershipTx(ctx, tx, false, s.directives, runID, nil)
}
