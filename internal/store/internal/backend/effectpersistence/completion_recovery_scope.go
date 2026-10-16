package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func completionRecoveryTurnContext(ctx context.Context, tx *sql.Tx, recovered completionRecoveryAttempt, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement, postgres bool) (context.Context, error) {
	if settlement.AgentTurn == nil {
		return ctx, nil
	}
	source, err := loadExternalEffectStorySource(ctx, tx, recovered.AttemptID, postgres)
	if err != nil {
		return nil, err
	}
	frame, err := agentframe.DecodeDurable(recovered.AgentFrame)
	if err != nil {
		return nil, fmt.Errorf("completion recovery scope frame: %w", err)
	}
	turn := settlement.AgentTurn
	if source.AttemptID != attempt.AttemptID || source.Kind != string(runtimeeffects.KindProviderTurn) ||
		source.AuthorityKind != string(attempt.Authority.Kind) || source.AuthorityID != attempt.Authority.ID ||
		source.AgentID != turn.AgentID || source.ExecutionMode != string(attempt.Authority.ExecutionMode) ||
		source.BundleHash != frame.Session.SourceArtifact.Hash ||
		turn.TurnID != attempt.Authority.Target.ID || turn.RunID != frame.Turn.Event.RunID ||
		turn.TriggerEventID != frame.Turn.Event.ID || turn.Identity != frame.Session.AgentIdentity {
		return nil, fmt.Errorf("completion recovery source scope conflicts for attempt %s", recovered.AttemptID)
	}
	// The recovery scan may contain several bundles. Its current runtime is the
	// occurrence owner; each immutable operation remains the bundle owner.
	scope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, source.BundleHash)
	if err != nil {
		return nil, err
	}
	return runtimeauthoractivity.WithScope(ctx, scope), nil
}
