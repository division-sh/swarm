package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func (o SelectedContractExecutionOwner) replaySelectedForkOperation(ctx context.Context, request SelectedContractExecutionRequest, elected *runfork.ForkOperationReplayRequiredError) (_ SelectedContractExecutionResult, found bool, finalErr error) {
	canonical, err := detachedSelectedForkOperation(request.ForkOperation)
	if err != nil {
		return SelectedContractExecutionResult{}, false, err
	}
	operation, err := o.beginPreparation(ctx)
	if err != nil {
		return SelectedContractExecutionResult{}, false, err
	}
	observer := &PreparedSelectedFork{owner: o, operation: operation}
	defer func() { finalErr = errors.Join(finalErr, o.completePreparation(observer)) }()
	ctx = operation.PreparationContext()
	for {
		record, exists, live, err := o.observeSelectedForkOperation(ctx, *canonical, operation)
		if err != nil || !exists {
			return SelectedContractExecutionResult{}, exists, err
		}
		if elected != nil && (record.Request.OperationID != elected.OperationID || record.ForkRunID != elected.ForkRunID) {
			return SelectedContractExecutionResult{}, true, errors.New("selected operation observation differs from its native election")
		}
		switch record.Status {
		case runfork.ForkOperationActivated:
			return selectedForkRecordedOutcome(record), true, nil
		case runfork.ForkOperationFailed, runfork.ForkOperationUncertain:
			return SelectedContractExecutionResult{}, true, fmt.Errorf("durable fork operation ended with %s: %s: %s",
				record.Status, record.Failure.Code, record.Failure.Reason)
		case runfork.ForkOperationMaterialized:
			if !live {
				result, err := o.recoverSelectedForkOperation(ctx, record, operation, request)
				return result, true, err
			}
		}
		// Read the durable outcome, not the context's retirement signal: an
		// activated runtime can remain retained after the response is available.
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return SelectedContractExecutionResult{}, true, ctx.Err()
		case <-timer.C:
		}
	}
}

func isolatedSelectedForkOperationReplay(err error) *runfork.ForkOperationReplayRequiredError {
	var replay *runfork.ForkOperationReplayRequiredError
	if errors.As(err, &replay) && failures.OnlyBranches(err, func(branch error) bool { return branch == replay }) {
		return replay
	}
	return nil
}

func (o SelectedContractExecutionOwner) continueElectedSelectedForkOperation(ctx context.Context, request SelectedContractExecutionRequest, elected *runfork.ForkOperationReplayRequiredError) (SelectedContractExecutionResult, error) {
	result, found, err := o.replaySelectedForkOperation(ctx, request, elected)
	if !found && err == nil {
		err = errors.New("native fork election has no original durable operation")
	}
	return result, err
}

func (o SelectedContractExecutionOwner) observeSelectedForkOperation(ctx context.Context, request runfork.ForkOperationRequest, operation *selectedContractOperation) (runfork.ForkOperationRecord, bool, bool, error) {
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return runfork.ForkOperationRecord{}, false, false, err
	}
	if contexts.retired || !contexts.recovered {
		return runfork.ForkOperationRecord{}, false, false, worklifetime.ErrRetired
	}
	var record runfork.ForkOperationRecord
	var found bool
	var err error
	if request.IdempotencyKey != "" {
		record, found, err = o.ports.fork.LoadForkOperation(ctx, request.Actor, request.IdempotencyKey, request.TransportHash)
	} else {
		record, found, err = o.ports.fork.LoadForkOperationByID(ctx, request.OperationID)
	}
	if err != nil || ctx.Err() != nil {
		return runfork.ForkOperationRecord{}, false, false, errors.Join(err, ctx.Err())
	}
	if !found {
		return record, false, false, nil
	}
	if err := validateObservedSelectedForkOperation(record, request); err != nil {
		return runfork.ForkOperationRecord{}, false, false, err
	}
	if record.Status != runfork.ForkOperationMaterialized {
		return record, true, false, nil
	}
	for _, entry := range contexts.entries {
		binding := entry.binding
		if binding.ForkRunID != record.ForkRunID {
			continue
		}
		if err := validateSelectedContractExecutionBinding(record.ForkRunID, binding); err != nil {
			return runfork.ForkOperationRecord{}, false, false, err
		}
		if binding.BindingID != record.BindingID || binding.SourceRunID != record.Request.SourceRunID ||
			!sameSelectedForkPointIdentity(binding.ForkPoint, *record.Request.ResolvedPoint) || binding.ContractSelection != record.Request.ContractSelection {
			return runfork.ForkOperationRecord{}, false, false, errors.New("live selected preparation differs from its durable operation binding")
		}
		return record, true, true, nil
	}
	// Reuse this admitted operation's existing inventory entry. The reservation
	// closes the native-recovery-to-bind gap without granting execution authority.
	entry := contexts.entries[operation]
	if entry == nil || entry.retiring {
		return runfork.ForkOperationRecord{}, false, false, worklifetime.ErrRetired
	}
	binding, err := o.ports.fork.RequireRunForkSelectedContractBinding(ctx, record.ForkRunID)
	if err != nil {
		return runfork.ForkOperationRecord{}, false, false, err
	}
	if err := validateSelectedContractExecutionBinding(record.ForkRunID, binding); err != nil {
		return runfork.ForkOperationRecord{}, false, false, err
	}
	if binding.BindingID != record.BindingID || binding.SourceRunID != record.Request.SourceRunID ||
		!sameSelectedForkPointIdentity(binding.ForkPoint, *record.Request.ResolvedPoint) || binding.ContractSelection != record.Request.ContractSelection {
		return runfork.ForkOperationRecord{}, false, false, errors.New("selected recovery reservation differs from its durable operation binding")
	}
	entry.binding, entry.recoveryReserved = binding, true
	return record, true, false, nil
}

func validateObservedSelectedForkOperation(record runfork.ForkOperationRecord, request runfork.ForkOperationRequest) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if record.Request.Actor != request.Actor || record.Request.IdempotencyKey != request.IdempotencyKey ||
		record.Request.TransportHash != request.TransportHash || record.Request.SourceRunID != request.SourceRunID ||
		record.Request.ForkEventID != request.ForkEventID || record.Request.AtStart != request.AtStart ||
		(request.IdempotencyKey == "" && record.Request.OperationID != request.OperationID) {
		return errors.New("selected operation observation differs from original request coordinates")
	}
	if request.OperationID == record.Request.OperationID {
		_, hash, err := request.Canonical()
		if err != nil {
			return err
		}
		if hash != record.SemanticHash || request.ResolvedPoint != nil &&
			(!request.ResolvedPoint.SameIdentity(*record.Request.ResolvedPoint) || request.ResolvedPoint.Input != record.Request.ResolvedPoint.Input) {
			return errors.New("selected operation observation differs from original invocation semantics")
		}
	}
	return nil
}

func selectedForkRecordedOutcome(record runfork.ForkOperationRecord) SelectedContractExecutionResult {
	result := record.Result
	return SelectedContractExecutionResult{
		Owner: runfork.RunForkSelectedContractExecutionOwner,
		Materialization: runfork.RunForkMaterialization{
			SourceRunID: result.SourceRunID, ForkRunID: result.ForkRunID, ForkPoint: result.ForkPoint, ForkRunStatus: result.ForkRunStatus,
			DataPins: append([]durabledata.Pin(nil), result.DataPins...),
		},
		Activation: runfork.RunForkActivation{
			SourceRunID: result.SourceRunID, ForkRunID: result.ForkRunID, ForkPoint: result.ForkPoint,
			ForkRunStatus: result.ForkRunStatus, SourceRunStatus: result.SourceRunStatus, SourceFrozen: result.SourceFrozen, Activated: true,
		},
		ExecutedEventCount: result.ExecutedEventCount,
	}
}
