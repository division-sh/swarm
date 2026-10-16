package agentpersistence

import (
	"context"
	"database/sql"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type startupTakeoverFact struct {
	transitionID, operationID, trigger, kind, operationState string
	previousPhase, phase                                     runtimemanager.AgentLifecyclePhase
	previousGeneration, generation                           uint64
	epoch, expectedEpoch                                     int64
	expectedGeneration, targetGeneration                     uint64
	configRevision                                           string
	runMode                                                  runtimemanager.AgentRunMode
	binding                                                  runtimemanager.ProcessExecutionBinding
	result                                                   runtimemanager.AgentLifecycleTransitionResult
}

// ReadStartupProviderTakeoverChainTx proves an already captured running drain
// crossed only recorded startup takeovers. It grants no late-provider authority.
func ReadStartupProviderTakeoverChainTx(ctx context.Context, tx *sql.Tx, request runtimemanager.AgentLifecycleTransition, current runtimemanager.AgentLifecycleState, predecessor runtimeeffects.LifecycleToken, operationID, transitionID string, epoch int64, generation uint64, sqlite bool) (bool, error) {
	if tx == nil || request.OperationKind != "process_takeover" || request.Identity != predecessor.Identity ||
		current.Identity != predecessor.Identity || current.Phase != runtimemanager.AgentLifecycleRunning ||
		!current.ProcessBinding.Equal(request.ProcessBinding) || current.RuntimeEpoch != request.TargetEpoch ||
		current.Generation != request.TargetGeneration || generation == 0 || generation >= current.Generation {
		return false, nil
	}
	if err := AuthorizeGenerationMutationTx(ctx, tx, request, sqlite); err != nil {
		return false, err
	}
	fields, err := IdentityFields(predecessor.Identity)
	if err != nil {
		return false, err
	}
	query := `SELECT f.transition_id::text,f.operation_id::text,f.trigger,f.previous_phase,f.next_phase,
		f.previous_generation,f.next_generation,f.runtime_epoch,f.config_revision,f.run_mode,
		f.process_authority_id::text,f.process_owner_id,f.process_boot_id::text,f.generation_grant_id::text,
		f.bundle_hash,f.runtime_instance_id::text,f.runtime_generation,
		o.operation_kind,o.state,o.expected_epoch,o.expected_generation,o.target_generation,o.result
		FROM agent_lifecycle_transition_facts f JOIN agent_lifecycle_operations o ON o.operation_id=f.operation_id
		WHERE f.run_id=$1::uuid AND f.agent_id=$2 AND f.agent_name_owner=$3 AND f.agent_name_source=$4
		AND f.agent_route_presence=$5 AND f.flow_scope_key=$6 AND f.flow_instance_id=$7 AND f.flow_instance=$8
		AND o.run_id=f.run_id AND o.agent_id=f.agent_id AND o.agent_name_owner=f.agent_name_owner
		AND o.agent_name_source=f.agent_name_source AND o.agent_route_presence=f.agent_route_presence
		AND o.flow_scope_key=f.flow_scope_key AND o.flow_instance_id=f.flow_instance_id AND o.flow_instance=f.flow_instance
		AND o.target_phase=f.next_phase AND o.config_revision=f.config_revision AND o.run_mode=f.run_mode
		AND o.process_authority_id=f.process_authority_id AND o.process_owner_id=f.process_owner_id
		AND o.process_boot_id=f.process_boot_id AND o.generation_grant_id=f.generation_grant_id
		AND o.bundle_hash=f.bundle_hash AND o.runtime_instance_id=f.runtime_instance_id
		AND o.runtime_generation=f.runtime_generation
		AND f.next_generation >= $9 AND f.next_generation <= $10
		ORDER BY f.next_generation,f.transition_id`
	if sqlite {
		query = strings.ReplaceAll(strings.ReplaceAll(query, "::text", ""), "::uuid", "")
	}
	rows, err := tx.QueryContext(ctx, query, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, generation, current.Generation)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	previousEpoch, previousGeneration := predecessor.RuntimeEpoch, predecessor.Generation
	first, lastTransition := true, ""
	var lastBinding runtimemanager.ProcessExecutionBinding
	for rows.Next() {
		fact, err := scanStartupTakeoverFact(rows)
		if err != nil {
			return false, err
		}
		if !fact.matches(current, previousEpoch, previousGeneration) {
			return false, nil
		}
		if first && (fact.transitionID != transitionID || fact.operationID != operationID || fact.epoch != epoch || fact.generation != generation) {
			return false, nil
		}
		first = false
		previousEpoch, previousGeneration, lastTransition = fact.epoch, fact.generation, fact.transitionID
		lastBinding = fact.binding
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return !first && previousEpoch == current.RuntimeEpoch && previousGeneration == current.Generation &&
		lastTransition == current.LastTransitionID && lastBinding.Equal(current.ProcessBinding), nil
}

func scanStartupTakeoverFact(rows *sql.Rows) (startupTakeoverFact, error) {
	var fact startupTakeoverFact
	var raw []byte
	err := rows.Scan(&fact.transitionID, &fact.operationID, &fact.trigger, &fact.previousPhase, &fact.phase,
		&fact.previousGeneration, &fact.generation, &fact.epoch, &fact.configRevision, &fact.runMode,
		&fact.binding.ProcessAuthorityID, &fact.binding.ProcessOwnerID, &fact.binding.ProcessBootID,
		&fact.binding.GenerationGrantID, &fact.binding.BundleHash, &fact.binding.RuntimeInstanceID, &fact.binding.RuntimeGeneration,
		&fact.kind, &fact.operationState, &fact.expectedEpoch, &fact.expectedGeneration, &fact.targetGeneration, &raw)
	if err == nil {
		err = canonicaljson.DecodeInto(raw, &fact.result)
	}
	return fact, err
}

func (f startupTakeoverFact) matches(current runtimemanager.AgentLifecycleState, previousEpoch int64, previousGeneration uint64) bool {
	if f.trigger != "process_takeover" || f.kind != "process_takeover" || f.operationState != "succeeded" ||
		f.previousPhase != runtimemanager.AgentLifecycleRunning || f.phase != f.previousPhase ||
		f.expectedEpoch != previousEpoch || f.expectedGeneration != previousGeneration ||
		f.previousGeneration != previousGeneration || f.generation != previousGeneration+1 ||
		f.epoch != previousEpoch+1 || f.targetGeneration != f.generation ||
		f.configRevision != current.ConfigRevision || f.runMode != current.RunMode ||
		f.binding.BundleHash != current.ProcessBinding.BundleHash || f.binding.Validate() != nil {
		return false
	}
	return f.matchesResult(current, previousEpoch, previousGeneration)
}

func (f startupTakeoverFact) matchesResult(current runtimemanager.AgentLifecycleState, previousEpoch int64, previousGeneration uint64) bool {
	r := f.result
	return r.Identity == current.Identity && r.AgentID == current.AgentID && r.OperationID == f.operationID &&
		r.TransitionID == f.transitionID && r.PreviousEpoch == previousEpoch && r.RuntimeEpoch == f.epoch &&
		r.PreviousGeneration == previousGeneration && r.Generation == f.generation &&
		r.PreviousPhase == f.previousPhase && r.Phase == f.phase && r.ConfigRevision == f.configRevision &&
		r.RunMode == f.runMode && r.Topology.Validate() == nil && r.Topology.Equal(current.Topology) &&
		r.ProcessBinding.Equal(f.binding) && r.DiagnosticProvenance.Validate() == nil &&
		r.Subordinate.Action == sessions.LifecycleMutationNone && len(r.Subordinate.Sessions) == 0
}
