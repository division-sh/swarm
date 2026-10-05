package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"

	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type EntityFieldMutation struct {
	RunID    string
	EntityID string
	Owner    runtimeflowidentity.RunScopedFlowInstance
	FlowID   string
	Source   semanticview.Source
	Mutation entityruntime.Mutation
	Writer   mutationlog.Writer
}

func (pc *PipelineCoordinator) ApplyEntityFieldMutation(ctx context.Context, command EntityFieldMutation) (EntityFieldMutationResult, error) {
	if pc == nil || pc.workflowStore == nil || pc.workflowStore.engineMutations == nil || command.Source == nil {
		return EntityFieldMutationResult{}, fmt.Errorf("entity field mutation requires the admitted pipeline writer")
	}
	if err := command.Owner.Validate(); err != nil || command.Owner.RunID != command.RunID || command.EntityID == "" || strings.TrimSpace(command.FlowID) == "" {
		return EntityFieldMutationResult{}, fmt.Errorf("entity field mutation requires exact run, route, flow and entity")
	}
	if command.Writer.Type != "agent" || command.Writer.ID == "" || command.Writer.HandlerStep != "save_entity_field" {
		return EntityFieldMutationResult{}, fmt.Errorf("entity field mutation requires exact agent attribution")
	}
	if command.Mutation.ProjectionSource != "" {
		return EntityFieldMutationResult{}, fmt.Errorf("agent mutation cannot claim materialized projection authority")
	}
	switch command.Mutation.Operation {
	case "":
		if command.Mutation.HasKey || command.Mutation.HasIndex {
			return EntityFieldMutationResult{}, fmt.Errorf("whole-value set cannot carry key or index")
		}
	case "append":
		if command.Mutation.HasKey || command.Mutation.HasIndex {
			return EntityFieldMutationResult{}, fmt.Errorf("append cannot carry key or index")
		}
	case "update":
		if !command.Mutation.HasIndex || command.Mutation.HasKey {
			return EntityFieldMutationResult{}, fmt.Errorf("list update requires exactly its integer index")
		}
	case "set":
		if !command.Mutation.HasKey || command.Mutation.HasIndex {
			return EntityFieldMutationResult{}, fmt.Errorf("map set requires exactly its key")
		}
	default:
		return EntityFieldMutationResult{}, fmt.Errorf("unsupported agent field operation %q", command.Mutation.Operation)
	}
	bundle, found := semanticview.Bundle(command.Source)
	if !found || bundle == nil || bundle.SourceArtifact == nil {
		return EntityFieldMutationResult{}, fmt.Errorf("entity mutation requires its admitted source artifact")
	}
	sourceFact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		return EntityFieldMutationResult{}, err
	}
	if current, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx); ok && !current.Matches(sourceFact) {
		return EntityFieldMutationResult{}, fmt.Errorf("entity mutation source contradicts its admitted context")
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, sourceFact)
	ctx = runtimecorrelation.WithRunID(ctx, command.RunID)
	address := runtimeengine.StateAddress{EntityID: identity.NormalizeEntityID(command.EntityID), FlowID: identity.NormalizeFlowID(command.FlowID), FlowInstance: command.Owner}
	for {
		if err := ctx.Err(); err != nil {
			return EntityFieldMutationResult{}, err
		}
		result, err := pc.applyEntityFieldMutationAttempt(ctx, command, address)
		if result.Acknowledged || !failures.IsStateContention(err) {
			return result, err
		}
	}
}

func (pc *PipelineCoordinator) applyEntityFieldMutationAttempt(ctx context.Context, command EntityFieldMutation, address runtimeengine.StateAddress) (EntityFieldMutationResult, error) {
	unlock := pc.lockWorkflowEntity(command.EntityID)
	defer unlock()
	repo := pipelineEngineStateRepo{coordinator: pc}
	snapshot, found, err := repo.LoadState(ctx, address)
	if err != nil || !found {
		if err == nil {
			err = runtimeengine.ErrUnconstructedWorkflowTarget
		}
		return EntityFieldMutationResult{}, err
	}
	evaluated, err := evaluatedWorkflowInstance(pc.SemanticSource(), address, snapshot)
	if err != nil {
		return EntityFieldMutationResult{}, err
	}
	if evaluated.instance.Status != "active" {
		return EntityFieldMutationResult{}, failures.New(failures.ClassAuthorizationDenied, "entity_target_not_active", "pipeline", "entity_field_operation", map[string]any{"action": "entity_write", "entity_id": command.EntityID, "status": evaluated.instance.Status})
	}
	contract, found := entityruntime.ResolveForFlow(command.Source, command.FlowID)
	if !found || contract.EntityType != snapshot.Control.EntityType {
		return EntityFieldMutationResult{}, fmt.Errorf("entity mutation source disagrees with the exact constructed contract")
	}
	fields, err := entityruntime.ApplyMutations(contract, snapshot.Fields, []entityruntime.Mutation{command.Mutation})
	if err != nil {
		if errors.Is(err, entityruntime.ErrImmutableMutation) {
			return EntityFieldMutationResult{}, failures.Wrap(failures.ClassAuthorizationDenied, "immutable_field_write_forbidden", "pipeline", "entity_field_operation", map[string]any{"action": "entity_write", "field": command.Mutation.Target}, err)
		}
		return EntityFieldMutationResult{}, failures.Wrap(failures.ClassSchemaInvalid, "invalid_tool_input", "pipeline", "entity_field_operation", map[string]any{"field": command.Mutation.Target}, err)
	}
	mutation := runtimeengine.StateMutation{StateCarrier: runtimeengine.NewStateCarrierWithOwners(fields, snapshot.Bookkeeping, snapshot.Control, snapshot.Gates, snapshot.StateBuckets)}
	prepared, err := repo.prepareMutation(ctx, runtimeengine.EngineMutation{Address: address, EvaluatedState: snapshot, State: mutation})
	if err != nil {
		return EntityFieldMutationResult{}, err
	}
	record, err := prepared.record()
	if err != nil {
		return EntityFieldMutationResult{}, err
	}
	sourceFact, _ := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: record, Writer: &command.Writer, WriterSource: sourceFact})
	if !committed.Committed {
		if err == nil {
			err = fmt.Errorf("entity mutation has no acknowledged commit")
		}
		return EntityFieldMutationResult{}, err
	}
	return EntityFieldMutationResult{Acknowledged: true, Revision: int(snapshot.Revision + 1)}, err
}

type EntityFieldMutationResult struct {
	Revision     int
	Acknowledged bool
}

type EntityFieldMutationWriter interface {
	ApplyEntityFieldMutation(context.Context, EntityFieldMutation) (EntityFieldMutationResult, error)
}
