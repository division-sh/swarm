package runforkpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func (a runForkSourceStateAdmission) publishedBarrier(ctx context.Context, tx *sql.Tx, event runfork.RunForkSelectedContractSourceEvent, state *runForkProjectedSourceState, actual []loopruntime.Activation, ref timeridentity.JoinRef) (runfork.RunForkSelectedContractSourceEvent, error) {
	if state == nil {
		return event, fmt.Errorf("published barrier requires its fixed-cut state owner")
	}
	intent, valid := ref.FanOutDelivery()
	if !valid {
		return event, fmt.Errorf("published barrier requires its exact intent")
	}
	key := fanoutobligation.IntentKey{RunID: a.forkRunID, TriggeringDeliveryID: intent.TriggeringDeliveryID(), ElementRef: contracts.FanOutElementRefFrom(intent.DeclarationIdentity())}
	child, err := pipelinepersistence.LoadFanOutBarrierContinuationTx(ctx, tx, a.postgres, key)
	if err != nil {
		return event, err
	}
	source, err := a.publishedBarrierSource(key)
	if err != nil {
		return event, err
	}
	var reference *loopruntime.ForkChildReference
	if ref.Generation().Valid() {
		if state.correspondence == nil {
			return event, fmt.Errorf("published barrier lacks exact generation correspondence")
		}
		original, err := state.correspondence.AdmitSource(ref.Generation())
		if err != nil {
			return event, err
		}
		projected, err := state.correspondence.Bind(original)
		if err != nil {
			return event, err
		}
		if err := state.correspondence.ValidateChild(projected, actual); err != nil {
			return event, err
		}
		reference = &projected
	}
	continuation, route, err := prepareRunForkBarrierTransfer(ctx, tx, a.snapshot, *source, a.forkRunID, child.Registration.PlanRef, reference, child.Registration.CreatedAt)
	if err != nil {
		return event, err
	}
	if err := requireMaterializedBarrierPublication(event, continuation, child); err != nil {
		return event, err
	}
	projected, err := continuation.Event(activityidentity.ForkLineageEventID(a.forkRunID, event.SourceEventID), runfork.RunForkSelectedContractExecutionOwner)
	if err != nil {
		return event, err
	}
	if event.RoutingSource != projected.RoutingSource() {
		return event, fmt.Errorf("selected barrier producer contradicts projected owner")
	}
	plan := runfork.RunForkPlan{SourceRunID: a.snapshot.RunID}
	plan.Entities, err = loadRunForkEntityStates(a.snapshot)
	if err != nil {
		return event, err
	}
	plan.Entities, _, err = attachRunForkMaterializedEntitySnapshotMetadata(a.snapshot, plan.Entities)
	if err != nil {
		return event, err
	}
	for _, reply := range a.snapshot.ReplyContexts {
		plan.ReplyContexts = append(plan.ReplyContexts, reply.Record)
	}
	route, err = projectRunForkPublishedJoinRoute(plan, continuation.ChildCommand(), route)
	if err != nil {
		return event, err
	}
	event.JoinContinuation, event.JoinRoute, event.Payload = continuation, route, projected.Payload()
	return event, nil
}

func requireMaterializedBarrierPublication(event runfork.RunForkSelectedContractSourceEvent, continuation genericschedule.JoinContinuation, child fanoutbarrier.Barrier) error {
	original := continuation.SourceEvent()
	if original.ID() != event.SourceEventID || string(original.Type()) != event.EventName || original.ExecutionMode() != event.ExecutionMode || !bytes.Equal(original.Payload(), event.Payload) {
		return fmt.Errorf("selected barrier source contradicts original publication")
	}
	retained, err := genericschedule.NewTransferredFanOutBarrierOccurrence(child)
	if err != nil {
		return err
	}
	want, err := continuation.ChildCommand().ImmutableHash()
	if err != nil {
		return err
	}
	got, err := retained.Command.ImmutableHash()
	if err != nil || want != got {
		return fmt.Errorf("selected barrier source contradicts materialized child")
	}
	publication, err := continuation.RetainedPublication(runfork.RunForkSelectedContractExecutionOwner)
	if err != nil {
		return err
	}
	if retained.Publication != publication {
		return fmt.Errorf("selected barrier retained publication contradicts source lineage")
	}
	return nil
}

func (a runForkSourceStateAdmission) publishedBarrierSource(child fanoutobligation.IntentKey) (*fanoutbarrier.Barrier, error) {
	var source *fanoutbarrier.Barrier
	for i := range a.snapshot.FanOutFacts {
		fact := &a.snapshot.FanOutFacts[i]
		if fact.FactKind != "barrier" || fact.TriggeringDeliveryID != child.TriggeringDeliveryID || fact.FlowPath != child.ElementRef.FlowPath || fact.DeclarationFamily != child.ElementRef.Family || fact.SemanticPath != child.ElementRef.SemanticPath {
			continue
		}
		if source != nil {
			return nil, fmt.Errorf("published barrier repeats its exact source owner")
		}
		sourceKey := child
		sourceKey.RunID = a.snapshot.RunID
		var err error
		source, err = projectRunForkFanOutBarrier(a.snapshot.RunID, fact, sourceKey)
		if err != nil {
			return nil, err
		}
	}
	if source == nil {
		return nil, fmt.Errorf("published barrier lacks its captured owner")
	}
	return source, nil
}
