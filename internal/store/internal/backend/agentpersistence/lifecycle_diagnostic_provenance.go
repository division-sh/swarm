package agentpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func snapshotLifecycleDiagnostic(ctx context.Context, tx *sql.Tx, req runtimemanager.AgentLifecycleTransition, result *runtimemanager.AgentLifecycleTransitionResult, eventOwner LifecycleDiagnosticEventOwner, originOwner LifecycleDiagnosticOriginValidator) error {
	if eventOwner == nil || originOwner == nil {
		return errors.New("lifecycle diagnostic persistence owners are required")
	}
	if err := req.DiagnosticOrigin.Validate(); err != nil {
		return err
	}
	f, err := IdentityFields(req.Identity)
	if err != nil {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT runtime_descriptor FROM agents WHERE run_id=$1 AND agent_id=$2 AND agent_name_owner=$3 AND agent_name_source=$4 AND agent_route_presence=$5 AND flow_scope_key=$6 AND flow_instance_id=$7 AND flow_instance=$8`, f.RunID, f.AgentID, f.NameOwner, f.NameSource, f.RoutePresence, f.FlowScopeKey, f.FlowInstanceID, f.FlowInstancePath).Scan(&raw); err != nil {
		return fmt.Errorf("load exact lifecycle diagnostic descriptor: %w", err)
	}
	desc, err := decodePersistedAgentRuntimeDescriptor(raw)
	if err != nil {
		return err
	}
	p := runtimemanager.LifecycleDiagnosticProvenance{Origin: req.DiagnosticOrigin, ActorMode: executionmode.Mode(desc.ExecutionMode), EventMode: executionmode.Mode(desc.ExecutionMode)}
	if p.Origin.Causality == runtimemanager.LifecycleDiagnosticAcceptedEvent {
		parent, found, err := eventOwner.LoadLifecycleDiagnosticEventTx(ctx, tx, p.Origin.ParentEventID)
		if err != nil {
			return err
		}
		if !found || parent.Event().RunID() != req.Identity.RunID {
			return errors.New("lifecycle diagnostic causal parent is missing or foreign")
		}
		p.EventMode = parent.Event().ExecutionMode()
	}
	result.DiagnosticProvenance = p
	binding, err := originOwner.ValidateLifecycleDiagnosticOriginTx(ctx, tx, *result, false)
	if err != nil {
		return err
	}
	result.DiagnosticProvenance.BindingID = binding
	return result.DiagnosticProvenance.Validate()
}

func lifecycleProvenanceBytes(result runtimemanager.AgentLifecycleTransitionResult) ([]byte, error) {
	if err := result.DiagnosticProvenance.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(result.DiagnosticProvenance)
}
