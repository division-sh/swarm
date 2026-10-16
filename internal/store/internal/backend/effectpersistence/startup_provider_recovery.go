package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type startupProviderTakeover struct {
	request runtimemanager.AgentLifecycleTransition
	result  runtimemanager.AgentLifecycleTransitionResult
}

func (s *EffectPostgresOwner) RecoverStartupProviderAttemptsPostgresTx(ctx context.Context, mutation *mutationprotocol.Attempt, request runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	takeover := startupProviderTakeover{request: request, result: result}
	return mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := takeover.validate(ctx, tx, false); err != nil {
			return err
		}
		summary, err := reconcileCompletionAttemptsPostgres(ctx, tx, s.llm, s.delivery, s.directives, mutation, nil, request.Now, "", &takeover)
		if err != nil || summary == (runtimeeffects.RecoverySummary{}) {
			return err
		}
		terminal, err := externalEffectRunTerminal(ctx, tx, true, request.Identity.RunID)
		if err != nil || terminal {
			return err
		}
		_, err = mutation.RequestCompletion(ctx, s.lifecycle, request.Identity.RunID, nil)
		return err
	})
}

func (s *EffectSQLiteOwner) RecoverStartupProviderAttemptsSQLiteTx(ctx context.Context, mutation *mutationprotocol.Attempt, request runtimemanager.AgentLifecycleTransition, result runtimemanager.AgentLifecycleTransitionResult) error {
	takeover := startupProviderTakeover{request: request, result: result}
	return mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := takeover.validate(ctx, tx, true); err != nil {
			return err
		}
		summary, err := reconcileCompletionAttemptsSQLite(ctx, tx, s.llm, s.delivery, s.directives, mutation, nil, request.Now, "", &takeover)
		if err != nil || summary == (runtimeeffects.RecoverySummary{}) {
			return err
		}
		terminal, err := externalEffectRunTerminal(ctx, tx, false, request.Identity.RunID)
		if err != nil || terminal {
			return err
		}
		_, err = mutation.RequestCompletion(ctx, s.lifecycle, request.Identity.RunID, nil)
		return err
	})
}

func (t startupProviderTakeover) validate(ctx context.Context, tx *sql.Tx, sqlite bool) error {
	req, result := t.request, t.result
	if req.OperationKind != "process_takeover" || req.Agent != nil || req.Now.IsZero() ||
		result.OperationID != req.OperationID || result.Identity != req.Identity ||
		result.RuntimeEpoch != req.TargetEpoch || result.Generation != req.TargetGeneration ||
		result.Phase != req.TargetPhase || !result.ProcessBinding.Equal(req.ProcessBinding) {
		return fmt.Errorf("startup provider recovery requires the exact native takeover result")
	}
	if err := storeagent.AuthorizeGenerationMutationTx(ctx, tx, req, sqlite); err != nil {
		return err
	}
	observation, err := storeagent.LockAgentExecutionTx(ctx, tx, req.Identity, runtimeeffects.LifecycleToken{
		Identity: req.Identity, AgentID: req.AgentID, RuntimeEpoch: result.RuntimeEpoch, Generation: result.Generation,
	}, sqlite)
	if err != nil {
		return err
	}
	state := observation.State
	if !observation.Found || state.RuntimeEpoch != result.RuntimeEpoch || state.Generation != result.Generation ||
		state.Phase != result.Phase || state.LastTransitionID != result.TransitionID ||
		state.ConfigRevision != result.ConfigRevision || state.RunMode != result.RunMode ||
		!state.Topology.Equal(result.Topology) || !state.ProcessBinding.Equal(req.ProcessBinding) {
		return fmt.Errorf("startup provider recovery differs from the transaction's exact lifecycle cell")
	}
	return nil
}

func startupCompletionFilter(takeover *startupProviderTakeover, postgres bool) (string, []any, error) {
	if takeover == nil {
		return "", nil, nil
	}
	fields, err := agentIdentityFields(takeover.request.Identity)
	if err != nil {
		return "", nil, err
	}
	predicate := ` AND o.authority_kind='normal_agent' AND o.agent_run_id=? AND o.agent_id=?
		AND o.agent_name_owner=? AND o.agent_name_source=? AND o.agent_route_presence=?
		AND o.flow_scope_key=? AND o.flow_instance_id=? AND o.flow_instance=?`
	if postgres {
		predicate = ` AND o.authority_kind='normal_agent' AND o.agent_run_id=$3::uuid AND o.agent_id=$4
			AND o.agent_name_owner=$5 AND o.agent_name_source=$6 AND o.agent_route_presence=$7
			AND o.flow_scope_key=$8 AND o.flow_instance_id=$9 AND o.flow_instance=$10`
	}
	return predicate, []any{fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath}, nil
}
