package eventpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

type eventCommitTxStore interface {
	appendAdmittedEventTxOutcome(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, events.AdmittedEvent, events.RouteSettlement) (runtimebus.EventAppendOutcome, error)
	RequirePipelinePublicationClaimTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim) error
	CommitInitialDeliveryObligationsTx(context.Context, *sql.Tx, string, string, []events.DeliveryRoute, runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error)
	CommitInitialPipelineScopeTx(context.Context, *sql.Tx, string, runtimepipelineobligation.CommittedScope) error
	CommitInitialPipelineDispositionTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) error
	RecordDeadLetterTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, runtimedeadletters.Record, bool) error
	createReplyContextTx(context.Context, *sql.Tx, runtimereplycontext.Record) error
	claimReplyContextTx(context.Context, *sql.Tx, runtimereplycontext.ClaimCommand) error
	PrepareDynamicFlowCreationOccurrenceCommitTx(context.Context, *sql.Tx, runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (bool, error)
	CommitFlowInstanceActivationsTx(context.Context, *sql.Tx, *privateauthoractivity.Mutation, []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error)
	ReplaceFlowInstanceRouteTopologyTx(context.Context, *sql.Tx, []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error)
	MarkDynamicFlowCreationOccurrenceCommittedTx(context.Context, *sql.Tx, runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) error
}

func (s *EventPostgresOwner) CommitDirectiveEventTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return (sqlPublishCommitter{tx: tx, store: s, story: story}).commitNamedEvent(ctx, "reserve directive operation", events.EventAdmissionDiagnosticDirect, events.EventTypePlatformAgentDirective, runtimebus.CommitPublishRequest{
		Event: admitted, RouteSettlement: settlement, ReplayScope: runtimepipelineobligation.ScopeDirect,
	})
}

func (s *EventSQLiteOwner) CommitDirectiveEventTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return (sqlPublishCommitter{tx: tx, store: s, story: story}).commitNamedEvent(ctx, "reserve directive operation", events.EventAdmissionDiagnosticDirect, events.EventTypePlatformAgentDirective, runtimebus.CommitPublishRequest{
		Event: admitted, RouteSettlement: settlement, ReplayScope: runtimepipelineobligation.ScopeDirect,
	})
}

type sqlPublishCommitter struct {
	tx    *sql.Tx
	store eventCommitTxStore
	story runtimeauthoractivity.Mutation
}

func runtimeAuthorActivityMutation(story *privateauthoractivity.Mutation) runtimeauthoractivity.Mutation {
	if story == nil {
		return nil
	}
	return story
}

func (c sqlPublishCommitter) commitNamedEvent(ctx context.Context, operation string, class events.EventAdmissionClass, eventType events.EventType, req runtimebus.CommitPublishRequest) (runtimebus.EventAppendOutcome, error) {
	if c.tx == nil || c.store == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s event commit transaction is required", operation)
	}
	if err := events.ValidateNamedEvent(req.Event, class, eventType); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s: %w", operation, err)
	}
	if err := req.ValidateRouteSettlement(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s: %w", operation, err)
	}
	outcome, err := c.store.appendAdmittedEventTxOutcome(ctx, c.tx, c.story, req.Event, req.RouteSettlement)
	if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
		return outcome, err
	}
	if outcome != runtimebus.EventAppendInserted {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event commit returned invalid append outcome")
	}
	if err := c.commitInitialSideEffects(ctx, req, class != events.EventAdmissionDiagnosticDirect); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func (c sqlPublishCommitter) commitInitialSideEffects(ctx context.Context, req runtimebus.CommitPublishRequest, requirePublicationClaim bool) error {
	proofs, err := c.commitInitialSideEffectEvidence(ctx, req, requirePublicationClaim)
	if err != nil {
		return err
	}
	if len(proofs) != 0 {
		return fmt.Errorf("named event commit must return executable delivery handoffs as typed evidence")
	}
	return nil
}

func (c sqlPublishCommitter) commitInitialSideEffectEvidence(ctx context.Context, req runtimebus.CommitPublishRequest, requirePublicationClaim bool) ([]runtimedelivery.DurableHandoffProof, error) {
	for _, record := range req.ReplyCreations {
		if err := c.store.createReplyContextTx(ctx, c.tx, record); err != nil {
			return nil, fmt.Errorf("commit reply context creation: %w", err)
		}
	}
	for _, claim := range req.ReplyClaims {
		if err := c.store.claimReplyContextTx(ctx, c.tx, claim); err != nil {
			return nil, fmt.Errorf("commit reply context claim: %w", err)
		}
	}
	if requirePublicationClaim {
		if err := c.store.RequirePipelinePublicationClaimTx(ctx, c.tx, req.Event.ID(), req.PipelineClaim); err != nil {
			return nil, fmt.Errorf("executable event commit requires its current publication claim: %w", err)
		}
	}
	proofs, err := c.store.CommitInitialDeliveryObligationsTx(
		ctx, c.tx, req.Event.ID(), req.Event.Event().RunID(), req.DeliveryRoutes, req.DeliveryAuthority,
	)
	if err != nil {
		return nil, err
	}
	if err := c.store.CommitInitialPipelineScopeTx(ctx, c.tx, req.Event.ID(), req.ReplayScope); err != nil {
		return nil, err
	}
	if req.Disposition != nil {
		if err := c.store.CommitInitialPipelineDispositionTx(ctx, c.tx, req.Event.ID(), req.PipelineClaim, *req.Disposition); err != nil {
			return nil, err
		}
	}
	if req.DeadLetter != nil {
		if err := c.store.RecordDeadLetterTx(ctx, c.tx, c.story, *req.DeadLetter, true); err != nil {
			return nil, err
		}
	}
	return proofs, nil
}

func validateSelectedForkCommitRequest(req runtimebus.CommitSelectedForkEventRequest) error {
	if err := events.ValidatePersistentEvent(req.Commit.Event.Event()); err != nil {
		return err
	}
	if req.Commit.Event.Class() != events.EventAdmissionSelectedForkReplay {
		return fmt.Errorf("selected-fork operation requires selected_fork_replay event class")
	}
	if req.Commit.RouteSettlement.WriteClass() != events.EventWriteSelectedForkPublication {
		return fmt.Errorf("selected-fork operation requires selected-fork publication settlement")
	}
	if err := req.Commit.ValidateRouteSettlement(); err != nil {
		return err
	}
	event := req.Commit.Event.Event()
	lineage, ok := event.SelectedForkLineage()
	if !ok {
		return fmt.Errorf("selected-fork operation requires typed event lineage")
	}
	want := req.Lineage
	if strings.TrimSpace(want.ForkRunID) != event.RunID() ||
		strings.TrimSpace(want.ForkEventID) != event.ID() ||
		strings.TrimSpace(want.SourceRunID) != lineage.SourceRunID() ||
		strings.TrimSpace(want.SourceEventID) != lineage.SourceEventID() ||
		strings.TrimSpace(want.EventName) != string(event.Type()) ||
		strings.TrimSpace(want.SelectionAuthority) != lineage.AuthorityStamp() {
		return fmt.Errorf("selected-fork operation lineage does not exactly match the admitted event")
	}
	return nil
}

func commitSelectedForkEvent(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	store eventCommitTxStore,
	insertLineage func(context.Context, *sql.Tx, runfork.RunForkSelectedContractExecutionLineage) error,
	req runtimebus.CommitSelectedForkEventRequest,
) (runtimebus.CommittedSelectedForkEvent, error) {
	if err := validateSelectedForkCommitRequest(req); err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	result := runtimebus.CommittedSelectedForkEvent{}
	committer := sqlPublishCommitter{tx: tx, store: store, story: story}
	var err error
	result.AppendOutcome, err = store.appendAdmittedEventTxOutcome(ctx, tx, story, req.Commit.Event, req.Commit.RouteSettlement)
	if err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	if result.AppendOutcome == runtimebus.EventAppendExactDuplicate {
		return result, result.Validate()
	}
	if result.AppendOutcome != runtimebus.EventAppendInserted {
		return runtimebus.CommittedSelectedForkEvent{}, fmt.Errorf("selected-fork operation returned invalid append outcome")
	}
	if err := insertLineage(ctx, tx, req.Lineage); err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	result.DeliveryHandoffs, err = committer.commitInitialSideEffectEvidence(ctx, req.Commit, true)
	if err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	return result, result.Validate()
}

func (s *EventPostgresOwner) CommitSelectedForkTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	if s.runFork == nil {
		return runtimebus.CommittedSelectedForkEvent{}, fmt.Errorf("event PostgreSQL run-fork owner is required")
	}
	return commitSelectedForkEvent(ctx, tx, story, s, s.runFork.InsertSelectedForkExecutionLineageTx, req)
}

func (s *EventSQLiteOwner) CommitSelectedForkTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	if s.runFork == nil {
		return runtimebus.CommittedSelectedForkEvent{}, fmt.Errorf("event SQLite run-fork owner is required")
	}
	return commitSelectedForkEvent(ctx, tx, story, s, s.runFork.InsertSelectedForkExecutionLineageTx, req)
}

func commitPublication(
	ctx context.Context,
	store eventCommitTxStore,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	command runtimebus.PublicationCommand,
) (runtimebus.CommittedPublication, error) {
	var err error
	ctx, err = publicationCommitContext(ctx, command)
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	_, postgres := store.(*EventPostgresOwner)
	result, err := withRunLifecycleCandidateHandoffResult(ctx, func(handoff *runLifecycleCandidateHandoffReservation) (runtimebus.CommittedPublication, error) {
		result := runtimebus.CommittedPublication{}
		err := run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var err error
			result, err = commitPublicationTx(txctx, tx, story, store, postgres, command, handoff)
			return err
		})
		return result, err
	})
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, fmt.Errorf("validate committed publication: %w", err)
	}
	return result, nil
}

func publicationCommitContext(ctx context.Context, command runtimebus.PublicationCommand) (context.Context, error) {
	if err := command.Validate(); err != nil {
		return ctx, err
	}
	if command.HasAuthorScope {
		ctx = runtimeauthoractivity.WithScope(ctx, command.AuthorScope)
	}
	ctx = runtimeauthoractivity.WithoutResolvedEventDescriptor(ctx)
	if !command.HasAuthorDescriptor {
		return ctx, nil
	}
	scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
	if !ok || scope.Kind != runtimeauthoractivity.ScopeBundle {
		return ctx, fmt.Errorf("publication author descriptor requires exact bundle scope")
	}
	return runtimeauthoractivity.WithResolvedEventDescriptor(ctx, scope, command.AuthorDescriptor)
}

func (s *EventPostgresOwner) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (result runtimebus.CommittedAPIEventPublication, err error) {
	if err := command.Validate(); err != nil {
		return result, err
	}
	if strings.TrimSpace(command.Idempotency.IdempotencyKey) == "" {
		result.Publication, err = s.CommitPublication(ctx, command.Publication)
		result.Completion = command.Completion
		return result, err
	}
	lease, err := storeapiidempotency.AcquirePostgresRequest(ctx, s.apiIdempotency, command.Idempotency)
	if err != nil {
		return result, err
	}
	defer func() {
		if releaseErr := lease.Release(ctx); releaseErr != nil {
			result = runtimebus.CommittedAPIEventPublication{}
			err = errors.Join(err, fmt.Errorf("release API event publication idempotency authority: %w", releaseErr))
		}
	}()
	if completion, replay := lease.Replay(); replay {
		return runtimebus.CommittedAPIEventPublication{Completion: completion, Replay: true}, nil
	}
	ctx, err = publicationCommitContext(ctx, command.Publication)
	if err != nil {
		return result, err
	}
	result.Completion = command.Completion
	result.Publication, err = withRunLifecycleCandidateHandoffResult(ctx, func(handoff *runLifecycleCandidateHandoffReservation) (runtimebus.CommittedPublication, error) {
		committed := runtimebus.CommittedPublication{}
		err := s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var commitErr error
			committed, commitErr = commitPublicationTx(txctx, tx, story, s, true, command.Publication, handoff)
			if commitErr != nil {
				return commitErr
			}
			return storeapiidempotency.StorePostgresCompletionTx(txctx, lease, tx, command.Completion)
		})
		return committed, err
	})
	if err != nil {
		return runtimebus.CommittedAPIEventPublication{}, err
	}
	return result, result.Validate()
}

func (s *EventSQLiteOwner) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (result runtimebus.CommittedAPIEventPublication, err error) {
	if err := command.Validate(); err != nil {
		return result, err
	}
	if strings.TrimSpace(command.Idempotency.IdempotencyKey) == "" {
		result.Publication, err = s.CommitPublication(ctx, command.Publication)
		result.Completion = command.Completion
		return result, err
	}
	lease, err := storeapiidempotency.AcquireSQLiteRequest(ctx, s.apiIdempotency, command.Idempotency)
	if err != nil {
		return result, err
	}
	defer lease.Release()
	if completion, replay := lease.Replay(); replay {
		return runtimebus.CommittedAPIEventPublication{Completion: completion, Replay: true}, nil
	}
	ctx, err = publicationCommitContext(ctx, command.Publication)
	if err != nil {
		return result, err
	}
	result.Completion = command.Completion
	result.Publication, err = withRunLifecycleCandidateHandoffResult(ctx, func(handoff *runLifecycleCandidateHandoffReservation) (runtimebus.CommittedPublication, error) {
		committed := runtimebus.CommittedPublication{}
		err := s.runPrivateAuthorActivityMutation(ctx, "sqlite API event publication commit", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var commitErr error
			committed, commitErr = commitPublicationTx(txctx, tx, story, s, false, command.Publication, handoff)
			if commitErr != nil {
				return commitErr
			}
			return storeapiidempotency.StoreSQLiteCompletionTx(txctx, lease, tx, command.Completion)
		})
		return committed, err
	})
	if err != nil {
		return runtimebus.CommittedAPIEventPublication{}, err
	}
	return result, result.Validate()
}

func commitPublicationTx(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	store eventCommitTxStore,
	postgres bool,
	command runtimebus.PublicationCommand,
	handoff *runLifecycleCandidateHandoffReservation,
) (runtimebus.CommittedPublication, error) {
	if tx == nil || story == nil {
		return runtimebus.CommittedPublication{}, fmt.Errorf("publication commit requires private transaction and story owners")
	}
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	if command.HasAuthorScope {
		ctx = runtimeauthoractivity.WithScope(ctx, command.AuthorScope)
	}
	ctx = runtimeauthoractivity.WithoutResolvedEventDescriptor(ctx)
	if command.HasAuthorDescriptor {
		scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
		if !ok || scope.Kind != runtimeauthoractivity.ScopeBundle {
			return runtimebus.CommittedPublication{}, fmt.Errorf("publication author descriptor requires exact bundle scope")
		}
		var err error
		ctx, err = runtimeauthoractivity.WithResolvedEventDescriptor(ctx, scope, command.AuthorDescriptor)
		if err != nil {
			return runtimebus.CommittedPublication{}, err
		}
	}
	request := command.Commit
	committer := sqlPublishCommitter{tx: tx, store: store, story: runtimeAuthorActivityMutation(story)}
	creationAlreadyCommitted := false
	var err error
	if command.DynamicFlowCreation != nil {
		creationAlreadyCommitted, err = store.PrepareDynamicFlowCreationOccurrenceCommitTx(ctx, tx, *command.DynamicFlowCreation)
		if err != nil {
			return runtimebus.CommittedPublication{}, err
		}
	}
	outcome, err := store.appendAdmittedEventTxOutcome(ctx, tx, committer.story, request.Event, request.RouteSettlement)
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	result := runtimebus.CommittedPublication{AppendOutcome: outcome}
	if outcome == runtimebus.EventAppendExactDuplicate {
		if command.DynamicFlowCreation != nil && !creationAlreadyCommitted {
			return runtimebus.CommittedPublication{}, fmt.Errorf("dynamic flow creation event exists before readiness completion")
		}
		result.Activations, err = store.CommitFlowInstanceActivationsTx(ctx, tx, story, command.Activations)
		if err != nil {
			return runtimebus.CommittedPublication{}, err
		}
		result.RouteTopology, err = store.ReplaceFlowInstanceRouteTopologyTx(ctx, tx, command.RouteTopology)
		if err != nil {
			return runtimebus.CommittedPublication{}, err
		}
		return result, nil
	}
	if outcome != runtimebus.EventAppendInserted {
		return runtimebus.CommittedPublication{}, fmt.Errorf("publication commit returned invalid append outcome")
	}
	if request.Event.RunDisposition() == events.AdmittedRunCreateAuthorized {
		var standalone bool
		switch selected := store.(type) {
		case *EventPostgresOwner:
			var loadErr error
			standalone, loadErr = selected.RunLifecyclePostgresOwner.IsStandaloneRuntimePlatformEventTx(ctx, tx, request.Event.Event().ID())
			if loadErr != nil {
				return runtimebus.CommittedPublication{}, loadErr
			}
			if standalone {
				if _, err := selected.RequestCompletionCandidateTx(ctx, tx, request.Event.Event().RunID(), nil, handoff); err != nil {
					return runtimebus.CommittedPublication{}, err
				}
			}
		case *EventSQLiteOwner:
			var loadErr error
			standalone, loadErr = selected.RunLifecycleSQLiteOwner.IsStandaloneRuntimePlatformEventTx(ctx, tx, request.Event.Event().ID())
			if loadErr != nil {
				return runtimebus.CommittedPublication{}, loadErr
			}
			if standalone {
				if _, err := selected.RequestCompletionCandidateTx(ctx, tx, request.Event.Event().RunID(), nil, handoff); err != nil {
					return runtimebus.CommittedPublication{}, err
				}
			}
		}
	}
	if command.DynamicFlowCreation != nil && creationAlreadyCommitted {
		return runtimebus.CommittedPublication{}, fmt.Errorf("dynamic flow readiness is complete without its creation event")
	}
	result.Activations, err = store.CommitFlowInstanceActivationsTx(ctx, tx, story, command.Activations)
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	result.RouteTopology, err = store.ReplaceFlowInstanceRouteTopologyTx(ctx, tx, command.RouteTopology)
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	result.DeliveryHandoffs, err = committer.commitInitialSideEffectEvidence(ctx, request, true)
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	if command.DynamicFlowCreation != nil {
		if err := store.MarkDynamicFlowCreationOccurrenceCommittedTx(ctx, tx, *command.DynamicFlowCreation); err != nil {
			return runtimebus.CommittedPublication{}, err
		}
	}
	return result, nil
}

func (s *EventPostgresOwner) CommitPublicationTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, command runtimebus.PublicationCommand, handoff *runLifecycleCandidateHandoffReservation) (runtimebus.CommittedPublication, error) {
	return commitPublicationTx(ctx, tx, story, s, true, command, handoff)
}

func (s *EventSQLiteOwner) CommitPublicationTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, command runtimebus.PublicationCommand, handoff *runLifecycleCandidateHandoffReservation) (runtimebus.CommittedPublication, error) {
	return commitPublicationTx(ctx, tx, story, s, false, command, handoff)
}

func (s *EventPostgresOwner) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return commitPublication(ctx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, fn)
	}, command)
}

func (s *EventSQLiteOwner) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return commitPublication(ctx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite publication commit", fn)
	}, command)
}

func commitRuntimeLogEvent(ctx context.Context, store eventCommitTxStore, run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	if err := events.ValidateNamedEvent(admitted, events.EventAdmissionDiagnosticDirect, events.EventTypePlatformRuntimeLog); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("runtime-log operation: %w", err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteRuntimeLogDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	outcome := runtimebus.EventAppendOutcomeUnknown
	err = run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		outcome, err = store.appendAdmittedEventTxOutcome(txctx, tx, runtimeAuthorActivityMutation(story), admitted, settlement)
		return err
	})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func (s *EventPostgresOwner) CommitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	return commitRuntimeLogEvent(ctx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, fn)
	}, admitted)
}

func (s *EventSQLiteOwner) CommitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	return commitRuntimeLogEvent(ctx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite runtime-log event commit", fn)
	}, admitted)
}
