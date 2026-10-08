package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func (o SelectedContractExecutionOwner) settleRecoveredSelectedCancellations(ctx context.Context, request runcontrol.SelectedForkRecoveryRequest, pending runfork.SelectedForkRecoveryResult, environment SelectedForkRecoveryEnvironment) (result runfork.SelectedForkRecoveryResult, finalErr error) {
	result = pending
	commands := make([]effects.CanceledTurnCommand, 0, len(pending.PendingCancellations))
	var preparation *PreparedSelectedFork
	var reactions []struct {
		owner *bus.EventBus
		plan  effects.TurnReactionPlan
	}
	defer func() {
		for _, reaction := range reactions {
			finalErr = errors.Join(finalErr, reaction.owner.ReleaseTurnTimeoutReaction(context.WithoutCancel(ctx), reaction.plan))
		}
		finalErr = errors.Join(finalErr, preparation.Close())
	}()
	for _, turn := range pending.PendingCancellations {
		command := effects.CanceledTurnCommand{Origin: turn.Cancellation.Origin}
		if turn.Attempt.AttemptID != "" {
			attempt := turn.Attempt
			command.Attempt = &attempt
		}
		if turn.Cancellation.Reason == deliverylifecycle.CancellationTurnTimeout {
			if preparation == nil {
				var err error
				preparation, err = o.beginSelectedCancellationInspection(ctx, request, environment.SourceInspector)
				if err != nil {
					return result, err
				}
			}
			// Each plan retains the exact physical predecessor. A recovery bus
			// has no receiver admission, provider controller or executable grant.
			publication, err := o.selectedCancellationPublication(request, pending.ExecutionID, preparation.loadedSource, turn.Attempt.Authority, environment)
			if err != nil {
				return result, err
			}
			command.Publication, err = publication.PrepareRecoveredTurnTimeoutReaction(ctx, turn)
			if command.Publication != nil {
				reactions = append(reactions, struct {
					owner *bus.EventBus
					plan  effects.TurnReactionPlan
				}{publication, command.Publication})
			}
			if err != nil {
				return result, err
			}
		}
		commands = append(commands, command)
	}
	request.Cancellations = commands
	committed, err := o.ports.fork.RecoverSelectedFork(ctx, request)
	if committed.RunID != pending.RunID || committed.ExecutionID != pending.ExecutionID || len(committed.CanceledTurns) != len(commands) {
		return result, errors.Join(err, errors.New("selected cancellation recovery lacks exact acknowledged settlements"))
	}
	// Acknowledged evidence survives auxiliary cleanup errors. Never release it
	// as if the transaction rolled back or dispatch before startup admission.
	result = committed
	if validationErr := validateRecoveredSelectedCancellationCommits(commands, committed); validationErr != nil {
		return result, errors.Join(err, validationErr)
	}
	result.Effects.PrelaunchTerminal += pending.Effects.PrelaunchTerminal
	result.Effects.OutcomeUncertain += pending.Effects.OutcomeUncertain
	result.CanceledTurns = append(append([]effects.CanceledTurnCommit(nil), pending.CanceledTurns...), committed.CanceledTurns...)
	return result, err
}

func validateRecoveredSelectedCancellationCommits(commands []effects.CanceledTurnCommand, committed runfork.SelectedForkRecoveryResult) error {
	for i, settled := range committed.CanceledTurns {
		if settled.Validate() != nil || !settled.Origin.Same(commands[i].Origin) {
			return errors.New("selected cancellation recovery substituted its origin")
		}
		plan := commands[i].Publication
		if (plan == nil) != (settled.Publication == nil) || plan != nil && settled.Publication.CommittedDurablePublicationEventID() != plan.DurablePublicationEventID() {
			return errors.New("selected cancellation recovery substituted its reaction")
		}
	}
	return nil
}

func (o SelectedContractExecutionOwner) beginSelectedCancellationInspection(ctx context.Context, request runcontrol.SelectedForkRecoveryRequest, inspector SelectedContractSourceInspector) (p *PreparedSelectedFork, err error) {
	if inspector == nil {
		return nil, errors.New("selected timeout recovery requires immutable source inspection")
	}
	op, err := beginSelectedContractOperation(ctx)
	if err != nil {
		return nil, err
	}
	p = &PreparedSelectedFork{owner: o, operation: op}
	contexts := o.ports.contexts
	contexts.mu.Lock()
	if contexts.retired || !contexts.recovering || contexts.process != op.process {
		contexts.mu.Unlock()
		return p, errors.Join(errors.New("selected cancellation inspection lost startup ownership"), op.Finish())
	}
	if contexts.entries == nil {
		contexts.entries = make(map[*selectedContractOperation]*selectedForkContext)
	}
	contexts.entries[op] = &selectedForkContext{operation: op, binding: request.Entry.Binding, done: make(chan struct{})}
	contexts.mu.Unlock()
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || fact.BundleHash() != request.Entry.BundleHash {
		return p, errors.New("selected cancellation inspection lacks exact source fact")
	}
	sourceRequest := SelectedContractSourceLoadRequest{
		SourceRunID: request.Entry.Binding.ForkRunID, BundleHash: request.Entry.BundleHash,
		SourceArtifactFact: fact, Selection: request.Entry.Binding.ContractSelection,
	}
	p.loadedSource, err = inspector.InspectRunForkSelectedContractSourceForRequest(op.PreparationContext(), sourceRequest)
	if err != nil {
		return p, err
	}
	if err := validateLoadedSelectedContractSource(sourceRequest, p.loadedSource); err != nil {
		return p, err
	}
	if p.loadedSource.RuntimeProjection != nil || p.loadedSource.Cleanup != nil {
		return p, errors.New("selected cancellation inspection allocated execution resources")
	}
	descriptors, err := rootruntime.AuthorActivityEventDescriptors(p.loadedSource.Source)
	if err != nil {
		return p, err
	}
	scope, ok := authoractivity.ScopeFromContext(ctx)
	if !ok {
		return p, errors.New("selected cancellation inspection lacks exact author scope")
	}
	p.descriptorLease, err = o.ports.fork.RegisterAuthorActivityEventCatalog(scope, descriptors)
	return p, err
}

func (o SelectedContractExecutionOwner) selectedCancellationPublication(request runcontrol.SelectedForkRecoveryRequest, executionID string, loaded LoadedSelectedContractSource, authority effects.Authority, environment SelectedForkRecoveryEnvironment) (*bus.EventBus, error) {
	variant, err := eventreceiver.SelectedRecoveryPublication(authority)
	if err != nil {
		return nil, err
	}
	if authority.ID != executionID || authority.SelectedFork.ForkRunID != request.Entry.Binding.ForkRunID {
		return nil, fmt.Errorf("selected cancellation publication differs from its exact predecessor")
	}
	delivery, err := deliverylifecycle.NewSelectedExecutionAuthority(loaded.SourceArtifactFact, authority.ID, authority.SelectedFork.ForkRunID, authority.SelectedFork.Generation)
	if err != nil {
		return nil, err
	}
	return bus.NewEventBusWithOptions(o.ports.events, bus.EventBusOptions{
		ExecutionPosture:   environment.AgentRuntime.ExecutionPosture,
		RuntimeInstanceID:  request.Process.RuntimeInstanceID,
		SourceArtifactFact: loaded.SourceArtifactFact, ContractBundle: loaded.Source,
		ReceiverExecution: variant, DeliveryAuthority: delivery,
		Durable: o.ports.busDurable, PipelineObligations: o.ports.pipelineObligations,
		PayloadAdmitter: rootruntime.NewRuntimePayloadAdmitter(nil, loaded.Source, loaded.SourceArtifactFact),
	})
}
