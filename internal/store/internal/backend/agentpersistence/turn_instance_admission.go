package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

// Retained turns name their original actor, not a currently executable token.
// All actors are acquired before any source/run reader enters its suffix.
func LockTurnOwnershipActorsTx(ctx context.Context, tx *sql.Tx, actors []agentidentity.Identity, sqlite bool) error {
	unique := make(map[agentidentity.Identity]bool, len(actors))
	for _, actor := range actors {
		if actor.Validate() != nil || actor != actor.Normalize() {
			return fmt.Errorf("retained turn requires its exact actor")
		}
		unique[actor] = true
	}
	ordered := make([]agentidentity.Identity, 0, len(unique))
	for actor := range unique {
		ordered = append(ordered, actor)
	}
	sort.Slice(ordered, func(i, j int) bool { return agentidentity.Less(ordered[i], ordered[j]) })
	for _, actor := range ordered {
		if err := lockLifecycleIdentityTx(ctx, tx, actor, sqlite); err != nil {
			return err
		}
		if _, _, found, err := loadAgentLifecycleState(ctx, tx, actor, sqlite, true); err != nil {
			return err
		} else if !found {
			return fmt.Errorf("retained turn actor is missing")
		}
	}
	return nil
}

func (s *AgentPostgresOwner) ReadRetainedTurnOwnershipTx(ctx context.Context, tx *sql.Tx, actor agentidentity.Identity, hash string) (events.InstanceOwnership, error) {
	if s.pipeline == nil {
		return events.InstanceOwnership{}, fmt.Errorf("retained turn ownership reader is not bound")
	}
	return s.pipeline.ReadRetainedAgentInstanceOwnershipTx(ctx, tx, actor, hash)
}

func (s *AgentSQLiteOwner) ReadRetainedTurnOwnershipTx(ctx context.Context, tx *sql.Tx, actor agentidentity.Identity, hash string) (events.InstanceOwnership, error) {
	if s.pipeline == nil {
		return events.InstanceOwnership{}, fmt.Errorf("retained turn ownership reader is not bound")
	}
	return s.pipeline.ReadRetainedAgentInstanceOwnershipTx(ctx, tx, actor, hash)
}

func (s *AgentPostgresOwner) PrepareAgentTurnOwnershipTx(ctx context.Context, tx *sql.Tx, carried agentcontrol.TurnInstanceSource, token effects.LifecycleToken) (events.InstanceOwnership, error) {
	return prepareAgentTurnOwnershipTx(ctx, tx, carried, token, false, s.pipeline)
}

func (s *AgentSQLiteOwner) PrepareAgentTurnOwnershipTx(ctx context.Context, tx *sql.Tx, carried agentcontrol.TurnInstanceSource, token effects.LifecycleToken) (events.InstanceOwnership, error) {
	return prepareAgentTurnOwnershipTx(ctx, tx, carried, token, true, s.pipeline)
}

func prepareAgentTurnOwnershipTx(ctx context.Context, tx *sql.Tx, carried agentcontrol.TurnInstanceSource, token effects.LifecycleToken, sqlite bool, owner DirectivePipelineOwner) (events.InstanceOwnership, error) {
	source, canonical := carried.(semanticview.Source)
	if owner == nil || !canonical || source == nil {
		return events.InstanceOwnership{}, fmt.Errorf("turn admission requires its canonical source and native ownership reader")
	}
	fact, err := lockAgentTurnAdmission(ctx, tx, token, sqlite)
	if err != nil {
		return events.InstanceOwnership{}, err
	}
	return owner.ReadAgentInstanceOwnershipTx(ctx, tx, source, fact, token.Identity)
}

// This prefix precedes run/origin/attempt locks. In particular, do not call it
// from a shared recovery decoder or after locking a directive operation.
func lockAgentTurnAdmission(ctx context.Context, tx *sql.Tx, token effects.LifecycleToken, sqlite bool) (correlation.SourceArtifactFact, error) {
	if tx == nil || !token.Valid() {
		return correlation.SourceArtifactFact{}, fmt.Errorf("turn admission requires its native transaction and acquired actor")
	}
	if err := lockLifecycleIdentityTx(ctx, tx, token.Identity, sqlite); err != nil {
		return correlation.SourceArtifactFact{}, err
	}
	current, err := LockAgentExecutionTx(ctx, tx, token.Identity, token, sqlite)
	if err != nil {
		return correlation.SourceArtifactFact{}, err
	}
	if !current.Current {
		return correlation.SourceArtifactFact{}, fmt.Errorf("turn admission actor is no longer current")
	}
	if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
		return correlation.SourceArtifactFact{}, err
	}
	// This is an ownership read, not a new flow-activation permission. Keep
	// static source-set currentness without requiring an activation grant for
	// standalone run-owned control work. Provider authority is checked next.
	if current.State.Topology.Authority.Kind == agenttopology.AuthorityStaticDeclarationPlan {
		plan, found, err := generationauthority.ReadSourceSetTx(ctx, tx, sqlite, true)
		static := current.State.Topology.Authority.Static
		if err != nil {
			return correlation.SourceArtifactFact{}, err
		}
		if !found || plan.Revision != static.SourceSetRevision || !generationauthority.SourceSetContainsBundle(plan, static.BundleHash) {
			return correlation.SourceArtifactFact{}, fmt.Errorf("turn admission actor source set is no longer current")
		}
	}
	var native correlation.SourceArtifactFact
	if sqlite {
		native, err = sourceadmission.LoadSQLiteTx(ctx, tx, token.Identity.RunID, true, false)
	} else {
		native, err = sourceadmission.LoadPostgresTx(ctx, tx, token.Identity.RunID, true, false)
	}
	if err != nil {
		return correlation.SourceArtifactFact{}, err
	}
	captured, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found || !native.Matches(captured) || current.State.ProcessBinding.BundleHash != native.BundleHash() {
		return correlation.SourceArtifactFact{}, fmt.Errorf("turn admission crosses its exact actor and native run source")
	}
	return native, nil
}
