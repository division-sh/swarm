package pipelinepersistence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetimer "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/timerobligation"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
	"github.com/google/uuid"
)

func commitFlowInstanceRestart(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, store eventCommitTxStore, postgres bool, plan pipeline.FlowInstanceActivationPlan, record pipeline.FlowInstanceActivationRecord) (bool, pipeline.CommittedWorkflowLifecycleMutation, []agentidentity.Identity, error) {
	var empty pipeline.CommittedWorkflowLifecycleMutation
	if err := record.Validate(); err != nil {
		return false, empty, nil, err
	}
	if postgres {
		if err := requirePostgresRunActive(ctx, tx, record.Identity.RunID); err != nil {
			return false, empty, nil, err
		}
	} else if err := requireSQLiteRunActive(ctx, tx, record.Identity.RunID); err != nil {
		return false, empty, nil, err
	}
	if err := requireFlowInstanceConstructionSelector(ctx, tx, postgres, record); err != nil {
		return false, empty, nil, err
	}
	life := events.InstanceLife{RunID: record.Identity.RunID, FlowInstance: record.Identity.Route.InstancePath, Life: plan.Restart.Life}
	actors, err := agentpersistence.LockInstanceLifeActorsTx(ctx, tx, life, plan.Restart.ProcessBinding, !postgres)
	if err != nil {
		return false, empty, nil, err
	}
	header, found, err := workflowheader.LoadForMutation(ctx, tx, postgres, life.RunID, record.EntityID, life.FlowInstance)
	if err != nil {
		return false, empty, nil, err
	}
	if !found {
		return false, empty, nil, fmt.Errorf("instance restart requires its retained header")
	}
	if header.Life != life.Life {
		if header.Life >= record.State.Life {
			equal, err := committedInstanceLifeRestartEqual(ctx, tx, record)
			if err != nil {
				return false, empty, nil, err
			}
			if equal {
				return false, empty, nil, nil
			}
		}
		return false, empty, nil, pipeline.InstanceRestarted(record.Identity, life.Life, header.Life)
	}
	if header.Revision != plan.Restart.Revision || header.Stage != plan.Restart.FinalStage.ID() {
		return false, empty, nil, workflowEngineStateRevisionConflict(record.State)
	}
	target, err := loadWorkflowTargetPersistence(ctx, tx, record.Identity, identity.NormalizeEntityID(record.EntityID), !postgres)
	if err != nil {
		return false, empty, nil, err
	}
	if err := requireHistoricalWorkflowDescriptor(target.Lifecycle, record.State); err != nil {
		return false, empty, nil, err
	}
	if !target.Lifecycle.CreatedAt.Equal(record.CreatedAt) {
		return false, empty, nil, fmt.Errorf("instance restart cannot replace physical birth")
	}
	if err := requireInstanceLifeQuiescentTx(ctx, tx, store, postgres, life, time.Now().UTC()); err != nil {
		return false, empty, nil, err
	}
	before, err := loadWorkflowEngineStateProjection(ctx, tx, postgres, record.State)
	if err != nil {
		return false, empty, nil, err
	}
	restarted, err := rotateInstanceLifeActorsTx(ctx, attempt, store.instanceLifeLifecycleOwner(), plan, actors)
	if err != nil {
		return false, empty, nil, err
	}
	if err := commitWorkflowEngineState(ctx, attempt, postgres, record.State); err != nil {
		return false, empty, nil, err
	}
	lifecycle, err := commitWorkflowEngineLifecycle(ctx, attempt, store.workflowDecisionLifecycleOwner(), store.genericScheduleTxOwner(), nil, postgres, plan.Lifecycle)
	if err != nil {
		return false, empty, nil, err
	}
	if err := commitWorkflowEngineMutationLog(ctx, attempt, store, postgres, record.State, before, nil); err != nil {
		return false, empty, nil, err
	}
	return true, lifecycle, restarted, nil
}

func committedInstanceLifeRestartEqual(ctx context.Context, tx *sql.Tx, record pipeline.FlowInstanceActivationRecord) (bool, error) {
	metadata, projection, found, err := runforkrevision.ReadEntityLife(ctx, tx, record.Identity.RunID, record.EntityID, record.State.Life, true)
	if err != nil || !found {
		return false, err
	}
	if metadata.FlowInstance != record.Identity.Route.InstancePath || metadata.EntityType != record.EntityType ||
		metadata.FlowTemplate != record.WorkflowName || metadata.Mode != record.Mode ||
		metadata.ParentInstance != record.State.ParentInstance || metadata.InstanceKey != record.State.InstanceKey ||
		metadata.Slug != record.Slug || metadata.Name != record.Name || metadata.CurrentState != record.CurrentState ||
		!metadata.CreatedAt.Equal(record.CreatedAt) || !metadata.EnteredStateAt.Equal(record.EnteredStageAt) ||
		!metadata.UpdatedAt.Equal(record.State.UpdatedAt) || !workflowCommitJSONEqual(metadata.FlowConfig, record.Config) {
		return false, nil
	}
	want, err := workflowEngineStateProjection(record.State)
	if err != nil {
		return false, err
	}
	actualRaw, err := json.Marshal(projection)
	if err != nil {
		return false, err
	}
	wantRaw, err := json.Marshal(want)
	return err == nil && workflowCommitJSONEqual(actualRaw, wantRaw), err
}

func requireInstanceLifeQuiescentTx(ctx context.Context, tx *sql.Tx, store eventCommitTxStore, postgres bool, life events.InstanceLife, observedAt time.Time) error {
	deliveries, err := store.ReadActiveInstanceLifeDeliveriesTx(ctx, tx, life)
	if err != nil {
		return err
	}
	pending, err := readUnsettledInstanceLifeEventsTx(ctx, tx, postgres, life)
	if err != nil {
		return err
	}
	leases, err := llmpersistence.ReadActiveInstanceLifeSessionLeasesTx(ctx, tx, life, observedAt)
	if err != nil {
		return err
	}
	fanout, err := readPendingInstanceLifeFanOutTx(ctx, tx, postgres, life)
	if err != nil {
		return err
	}
	scope, err := runtimetimer.InstanceLife(life)
	if err != nil {
		return err
	}
	var timers runtimetimer.Snapshot
	if postgres {
		timers, err = timerobligation.ReadPostgresTx(ctx, tx, scope, observedAt)
	} else {
		timers, err = timerobligation.ReadSQLiteTx(ctx, tx, scope, observedAt)
	}
	if err != nil {
		return err
	}
	run, _ := timers.Run(life.RunID)
	due := run.Totals().DueCount
	if len(deliveries)+len(pending)+leases+fanout+due == 0 {
		return nil
	}
	return failures.New(failures.ClassLifecycleConflict, "instance_not_quiescent", "flow-instance", "restart", map[string]any{
		"flow_instance": life.FlowInstance, "life": life.Life, "deliveries": len(deliveries),
		"pipeline_events": len(pending), "session_leases": leases, "fanout": fanout, "due_timers": due,
	})
}

func rotateInstanceLifeActorsTx(ctx context.Context, attempt *mutationprotocol.Attempt, owner InstanceLifeLifecycleTxOwner, plan pipeline.FlowInstanceActivationPlan, actors []manager.AgentLifecycleState) ([]agentidentity.Identity, error) {
	if owner == nil {
		return nil, fmt.Errorf("instance restart requires its full agent lifecycle owner")
	}
	identities := make([]agentidentity.Identity, 0, len(actors))
	for _, actor := range actors {
		raw, err := json.Marshal(struct {
			Identity agentidentity.Identity
			State    pipeline.WorkflowInstance
			Input    pipeline.FlowConstructionInput
		}{actor.Identity, plan.Instance, plan.CreatingInput})
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(raw)
		operationID := uuid.NewSHA1(uuid.NameSpaceOID, digest[:]).String()
		_, err = owner.CommitAgentLifecycleTransitionTx(ctx, attempt, manager.AgentLifecycleTransition{
			DiagnosticOrigin: plan.Restart.DiagnosticOrigin, OperationID: operationID,
			OperationKind: "restart", RequestHash: hex.EncodeToString(digest[:]), Identity: actor.Identity,
			AgentID: actor.AgentID, Trigger: "instance_restart", ExpectedEpoch: actor.RuntimeEpoch,
			ExpectedGeneration: actor.Generation, ExpectedPhase: actor.Phase, TargetEpoch: actor.RuntimeEpoch + 1,
			TargetGeneration: actor.Generation + 1, TargetPhase: manager.AgentLifecycleRegistered,
			ConfigRevision: actor.ConfigRevision, RunMode: manager.AgentRunModeStopped, Topology: actor.Topology,
			ProcessBinding: plan.Restart.ProcessBinding, Now: plan.OccurredAt,
			Subordinate: sessions.LifecycleMutationPlan{Action: sessions.LifecycleMutationRotateCurrentSet,
				TerminationReason: sessions.TerminationReasonNormal, TerminationDetail: "instance_restarted"},
		})
		if err != nil {
			return nil, err
		}
		identities = append(identities, actor.Identity)
	}
	return identities, nil
}
