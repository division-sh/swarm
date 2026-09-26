package eventpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/apiidempotency"
	runtimedata "github.com/division-sh/swarm/internal/durabledata"
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
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type eventCommitTxStore interface {
	standaloneCompletionOwner() standaloneCompletionCapability
	appendAdmittedEventTxOutcome(context.Context, *mutationprotocol.Attempt, events.AdmittedEvent, events.RouteSettlement) (runtimebus.EventAppendOutcome, error)
	RequirePipelinePublicationClaimTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim) error
	CommitInitialDeliveryObligationsTx(context.Context, *mutationprotocol.Attempt, string, string, []events.DeliveryRoute, runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error)
	CommitInitialPipelineScopeTx(context.Context, *mutationprotocol.Attempt, string, runtimepipelineobligation.CommittedScope) error
	CommitInitialPipelineDispositionTx(context.Context, *mutationprotocol.Attempt, string, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) error
	RecordDeadLetterTx(context.Context, *mutationprotocol.Attempt, runtimedeadletters.Record, bool) error
	createReplyContextTx(context.Context, *mutationprotocol.Attempt, runtimereplycontext.Record) error
	claimReplyContextTx(context.Context, *mutationprotocol.Attempt, runtimereplycontext.ClaimCommand) error
	PrepareDynamicFlowCreationOccurrenceCommitTx(context.Context, *sql.Tx, runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (bool, error)
	CommitFlowInstanceActivationsTx(context.Context, *mutationprotocol.Attempt, []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error)
	ReplaceFlowInstanceRouteTopologyTx(context.Context, *sql.Tx, []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error)
	MarkDynamicFlowCreationOccurrenceCommittedTx(context.Context, *sql.Tx, runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest) error
}

type standaloneCompletionCapability interface {
	mutationprotocol.CandidateWriter
	IsStandaloneRuntimePlatformEventTx(context.Context, *sql.Tx, string) (bool, error)
}

func (s *EventPostgresOwner) standaloneCompletionOwner() standaloneCompletionCapability {
	if s == nil || s.RunLifecyclePostgresOwner == nil {
		return nil
	}
	return s.RunLifecyclePostgresOwner
}

func (s *EventSQLiteOwner) standaloneCompletionOwner() standaloneCompletionCapability {
	if s == nil || s.RunLifecycleSQLiteOwner == nil {
		return nil
	}
	return s.RunLifecycleSQLiteOwner
}

func requestStandalonePublicationCompletion(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, owner standaloneCompletionCapability, eventID, runID string) error {
	if owner == nil {
		return fmt.Errorf("publication requires standalone completion capability")
	}
	standalone, err := owner.IsStandaloneRuntimePlatformEventTx(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if !standalone {
		return nil
	}
	_, err = attempt.RequestCompletion(ctx, owner, runID, nil)
	return err
}

func (s *EventPostgresOwner) CommitDirectiveEventTx(ctx context.Context, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return (sqlPublishCommitter{attempt: attempt, store: s}).commitNamedEvent(ctx, "reserve directive operation", events.EventAdmissionDiagnosticDirect, events.EventTypePlatformAgentDirective, runtimebus.CommitPublishRequest{
		Event: admitted, RouteSettlement: settlement, ReplayScope: runtimepipelineobligation.ScopeDirect,
	})
}

func (s *EventSQLiteOwner) CommitDirectiveEventTx(ctx context.Context, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return (sqlPublishCommitter{attempt: attempt, store: s}).commitNamedEvent(ctx, "reserve directive operation", events.EventAdmissionDiagnosticDirect, events.EventTypePlatformAgentDirective, runtimebus.CommitPublishRequest{
		Event: admitted, RouteSettlement: settlement, ReplayScope: runtimepipelineobligation.ScopeDirect,
	})
}

type sqlPublishCommitter struct {
	attempt *mutationprotocol.Attempt
	store   eventCommitTxStore
}

func (c sqlPublishCommitter) commitNamedEvent(ctx context.Context, operation string, class events.EventAdmissionClass, eventType events.EventType, req runtimebus.CommitPublishRequest) (runtimebus.EventAppendOutcome, error) {
	if c.attempt == nil || c.store == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s event commit attempt is required", operation)
	}
	if err := events.ValidateNamedEvent(req.Event, class, eventType); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s: %w", operation, err)
	}
	if err := req.ValidatePreparedEvent(); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s: %w", operation, err)
	}
	outcome, err := c.store.appendAdmittedEventTxOutcome(ctx, c.attempt, req.Event, req.RouteSettlement)
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
		if err := c.store.createReplyContextTx(ctx, c.attempt, record); err != nil {
			return nil, fmt.Errorf("commit reply context creation: %w", err)
		}
	}
	for _, claim := range req.ReplyClaims {
		if err := c.store.claimReplyContextTx(ctx, c.attempt, claim); err != nil {
			return nil, fmt.Errorf("commit reply context claim: %w", err)
		}
	}
	if requirePublicationClaim {
		if err := c.attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return c.store.RequirePipelinePublicationClaimTx(ctx, tx, req.Event.ID(), req.PipelineClaim)
		}); err != nil {
			return nil, fmt.Errorf("executable event commit requires its current publication claim: %w", err)
		}
	}
	proofs, err := c.store.CommitInitialDeliveryObligationsTx(
		ctx, c.attempt, req.Event.ID(), req.Event.Event().RunID(), req.DeliveryRoutes, req.DeliveryAuthority,
	)
	if err != nil {
		return nil, err
	}
	if err := c.store.CommitInitialPipelineScopeTx(ctx, c.attempt, req.Event.ID(), req.ReplayScope); err != nil {
		return nil, err
	}
	if req.Disposition != nil {
		if err := c.store.CommitInitialPipelineDispositionTx(ctx, c.attempt, req.Event.ID(), req.PipelineClaim, *req.Disposition); err != nil {
			return nil, err
		}
	}
	if req.DeadLetter != nil {
		if err := c.store.RecordDeadLetterTx(ctx, c.attempt, *req.DeadLetter, true); err != nil {
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
	if err := req.Commit.ValidatePreparedEvent(); err != nil {
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
	attempt *mutationprotocol.Attempt,
	store eventCommitTxStore,
	insertLineage func(context.Context, *sql.Tx, runfork.RunForkSelectedContractExecutionLineage) error,
	req runtimebus.CommitSelectedForkEventRequest,
) (runtimebus.CommittedSelectedForkEvent, error) {
	if err := validateSelectedForkCommitRequest(req); err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	result := runtimebus.CommittedSelectedForkEvent{}
	committer := sqlPublishCommitter{attempt: attempt, store: store}
	var err error
	result.AppendOutcome, err = store.appendAdmittedEventTxOutcome(ctx, attempt, req.Commit.Event, req.Commit.RouteSettlement)
	if err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	if result.AppendOutcome == runtimebus.EventAppendExactDuplicate {
		return result, result.Validate()
	}
	if result.AppendOutcome != runtimebus.EventAppendInserted {
		return runtimebus.CommittedSelectedForkEvent{}, fmt.Errorf("selected-fork operation returned invalid append outcome")
	}
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return insertLineage(ctx, tx, req.Lineage)
	}); err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	result.DeliveryHandoffs, err = committer.commitInitialSideEffectEvidence(ctx, req.Commit, true)
	if err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	return result, result.Validate()
}

func (s *EventPostgresOwner) CommitSelectedForkTx(ctx context.Context, attempt *mutationprotocol.Attempt, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	if s.runFork == nil {
		return runtimebus.CommittedSelectedForkEvent{}, fmt.Errorf("event PostgreSQL run-fork owner is required")
	}
	return commitSelectedForkEvent(ctx, attempt, s, s.runFork.InsertSelectedForkExecutionLineageTx, req)
}

func (s *EventSQLiteOwner) CommitSelectedForkTx(ctx context.Context, attempt *mutationprotocol.Attempt, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	if s.runFork == nil {
		return runtimebus.CommittedSelectedForkEvent{}, fmt.Errorf("event SQLite run-fork owner is required")
	}
	return commitSelectedForkEvent(ctx, attempt, s, s.runFork.InsertSelectedForkExecutionLineageTx, req)
}

func commitPublication(
	ctx context.Context,
	store eventCommitTxStore,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (runtimebus.CommittedPublication, error)) mutationprotocol.Result[runtimebus.CommittedPublication],
	command runtimebus.PublicationCommand,
) (runtimebus.CommittedPublication, error) {
	var err error
	ctx, err = publicationCommitContext(ctx, command)
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	outcome := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.CommittedPublication, error) {
		return commitPublicationTx(txctx, attempt, store, command)
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimebus.CommittedPublication{}, outcome.Err()
	}
	result.Acknowledged = true
	if err := result.Validate(); err != nil {
		return result, errors.Join(outcome.Err(), fmt.Errorf("validate committed publication: %w", err))
	}
	return result, outcome.Err()
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

func (s *EventPostgresOwner) LookupAPIEventPublication(ctx context.Context, request apiidempotency.Request) (completion apiidempotency.Completion, replay bool, err error) {
	if strings.TrimSpace(request.IdempotencyKey) == "" {
		return apiidempotency.Completion{}, false, nil
	}
	if method := strings.TrimSpace(request.Method); method != "event.publish" && method != "run.start" {
		return apiidempotency.Completion{}, false, fmt.Errorf("API event publication lookup requires event.publish or run.start method authority")
	}
	lease, err := storeapiidempotency.AcquirePostgresRequest(ctx, s.apiIdempotency, request)
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	defer func() {
		if releaseErr := lease.Release(ctx); releaseErr != nil {
			completion = apiidempotency.Completion{}
			replay = false
			err = errors.Join(err, fmt.Errorf("release API event publication lookup authority: %w", releaseErr))
		}
	}()
	completion, replay = lease.Replay()
	return completion, replay, nil
}

func (s *EventSQLiteOwner) LookupAPIEventPublication(ctx context.Context, request apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	if strings.TrimSpace(request.IdempotencyKey) == "" {
		return apiidempotency.Completion{}, false, nil
	}
	if method := strings.TrimSpace(request.Method); method != "event.publish" && method != "run.start" {
		return apiidempotency.Completion{}, false, fmt.Errorf("API event publication lookup requires event.publish or run.start method authority")
	}
	lease, err := storeapiidempotency.AcquireSQLiteRequest(ctx, s.apiIdempotency, request)
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	defer lease.Release()
	completion, replay := lease.Replay()
	return completion, replay, nil
}

func (s *EventPostgresOwner) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (result runtimebus.CommittedAPIEventPublication, err error) {
	if err := command.Validate(); err != nil {
		return result, err
	}
	var lease *storeapiidempotency.PostgresRequestLease
	if strings.TrimSpace(command.Idempotency.IdempotencyKey) != "" {
		lease, err = storeapiidempotency.AcquirePostgresRequest(ctx, s.apiIdempotency, command.Idempotency)
		if err != nil {
			return result, err
		}
		defer func() {
			if releaseErr := lease.Release(ctx); releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("release API event publication idempotency authority: %w", releaseErr))
			}
		}()
		if completion, replay := lease.Replay(); replay {
			return runtimebus.CommittedAPIEventPublication{Completion: completion, Replay: true}, nil
		}
	}
	ctx, err = publicationCommitContext(ctx, command.Publication)
	if err != nil {
		return result, err
	}
	outcome := runPostgresEventMutationResult(ctx, s, true, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.CommittedAPIEventPublication, error) {
		var result runtimebus.CommittedAPIEventPublication
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var plan *storedurabledata.RunCreationPlan
			if command.RunCreation != nil {
				prepared, prepareErr := storedurabledata.PrepareRunCreationTx(s.durableData, txctx, tx, *command.RunCreation)
				if prepareErr != nil {
					return prepareErr
				}
				plan = &prepared
				record := prepared.Record()
				if prepared.Replay() {
					if prepared.Failed() {
						result = runtimebus.CommittedAPIEventPublication{RunCreation: &record, Replay: true}
						return nil
					}
					completion, bindErr := bindRunCreationCompletion(command.Completion, record)
					if bindErr != nil {
						return bindErr
					}
					result = runtimebus.CommittedAPIEventPublication{Completion: completion, RunCreation: &record, Replay: true}
					if lease != nil {
						return storeapiidempotency.StorePostgresCompletionTx(txctx, lease, tx, completion)
					}
					return nil
				}
				if prepared.Failed() {
					result = runtimebus.CommittedAPIEventPublication{RunCreation: &record}
					return nil
				}
				if commitErr := storedurabledata.CommitRunCreationImportsTx(s.durableData, txctx, tx, plan); commitErr != nil {
					return commitErr
				}
			}
			committed, commitErr := commitPublicationTx(txctx, attempt, s, command.Publication)
			if commitErr != nil {
				return commitErr
			}
			completion := command.Completion
			result = runtimebus.CommittedAPIEventPublication{Publication: committed, Completion: completion}
			if plan != nil {
				if committed.AppendOutcome != runtimebus.EventAppendInserted {
					return fmt.Errorf("run creation event was already committed without its permanent parent receipt")
				}
				var status string
				if err := tx.QueryRowContext(txctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, command.RunCreation.RunID).Scan(&status); err != nil {
					return fmt.Errorf("load created run for data pins: %w", err)
				}
				record, completeErr := storedurabledata.CompleteRunCreationTx(s.durableData, txctx, tx, plan, command.RunCreation.EventID, status)
				if completeErr != nil {
					return completeErr
				}
				if err := storedurabledata.CommitRunCreationFeedsTx(txctx, tx, plan, s.PipelinePostgresOwner); err != nil {
					return err
				}
				completion, completeErr = bindRunCreationCompletion(completion, record)
				if completeErr != nil {
					return completeErr
				}
				result.Completion = completion
				result.RunCreation = &record
			}
			if lease != nil {
				return storeapiidempotency.StorePostgresCompletionTx(txctx, lease, tx, result.Completion)
			}
			return nil
		})
		return result, err
	})
	var acknowledged bool
	result, acknowledged = outcome.Value()
	if !acknowledged {
		return result, outcome.Err()
	}
	result.Acknowledged = true
	return result, errors.Join(outcome.Err(), result.Validate())
}

func (s *EventSQLiteOwner) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (result runtimebus.CommittedAPIEventPublication, err error) {
	if err := command.Validate(); err != nil {
		return result, err
	}
	var lease *storeapiidempotency.SQLiteRequestLease
	if strings.TrimSpace(command.Idempotency.IdempotencyKey) != "" {
		lease, err = storeapiidempotency.AcquireSQLiteRequest(ctx, s.apiIdempotency, command.Idempotency)
		if err != nil {
			return result, err
		}
		defer lease.Release()
		if completion, replay := lease.Replay(); replay {
			return runtimebus.CommittedAPIEventPublication{Completion: completion, Replay: true}, nil
		}
	}
	ctx, err = publicationCommitContext(ctx, command.Publication)
	if err != nil {
		return result, err
	}
	outcome := runSQLiteEventMutationResult(ctx, s, "sqlite API event publication commit", true, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.CommittedAPIEventPublication, error) {
		var result runtimebus.CommittedAPIEventPublication
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var plan *storedurabledata.RunCreationPlan
			if command.RunCreation != nil {
				prepared, prepareErr := storedurabledata.PrepareRunCreationTx(s.durableData, txctx, tx, *command.RunCreation)
				if prepareErr != nil {
					return prepareErr
				}
				plan = &prepared
				record := prepared.Record()
				if prepared.Replay() {
					if prepared.Failed() {
						result = runtimebus.CommittedAPIEventPublication{RunCreation: &record, Replay: true}
						return nil
					}
					completion, bindErr := bindRunCreationCompletion(command.Completion, record)
					if bindErr != nil {
						return bindErr
					}
					result = runtimebus.CommittedAPIEventPublication{Completion: completion, RunCreation: &record, Replay: true}
					if lease != nil {
						return storeapiidempotency.StoreSQLiteCompletionTx(txctx, lease, tx, completion)
					}
					return nil
				}
				if prepared.Failed() {
					result = runtimebus.CommittedAPIEventPublication{RunCreation: &record}
					return nil
				}
				if commitErr := storedurabledata.CommitRunCreationImportsTx(s.durableData, txctx, tx, plan); commitErr != nil {
					return commitErr
				}
			}
			committed, commitErr := commitPublicationTx(txctx, attempt, s, command.Publication)
			if commitErr != nil {
				return commitErr
			}
			completion := command.Completion
			result = runtimebus.CommittedAPIEventPublication{Publication: committed, Completion: completion}
			if plan != nil {
				if committed.AppendOutcome != runtimebus.EventAppendInserted {
					return fmt.Errorf("run creation event was already committed without its permanent parent receipt")
				}
				var status string
				if err := tx.QueryRowContext(txctx, `SELECT status FROM runs WHERE run_id = ?`, command.RunCreation.RunID).Scan(&status); err != nil {
					return fmt.Errorf("load created run for data pins: %w", err)
				}
				record, completeErr := storedurabledata.CompleteRunCreationTx(s.durableData, txctx, tx, plan, command.RunCreation.EventID, status)
				if completeErr != nil {
					return completeErr
				}
				if err := storedurabledata.CommitRunCreationFeedsTx(txctx, tx, plan, s.PipelineSQLiteOwner); err != nil {
					return err
				}
				completion, completeErr = bindRunCreationCompletion(completion, record)
				if completeErr != nil {
					return completeErr
				}
				result.Completion = completion
				result.RunCreation = &record
			}
			if lease != nil {
				return storeapiidempotency.StoreSQLiteCompletionTx(txctx, lease, tx, result.Completion)
			}
			return nil
		})
		return result, err
	})
	var acknowledged bool
	result, acknowledged = outcome.Value()
	if !acknowledged {
		return result, outcome.Err()
	}
	result.Acknowledged = true
	return result, errors.Join(outcome.Err(), result.Validate())
}

func bindRunCreationCompletion(completion apiidempotency.Completion, record runtimedata.RunCreationOperationRecord) (apiidempotency.Completion, error) {
	if err := record.Validate(); err != nil {
		return apiidempotency.Completion{}, fmt.Errorf("cannot bind contradictory run creation: %w", err)
	}
	if record.Summary.Outcome != "created" {
		return apiidempotency.Completion{}, fmt.Errorf("cannot bind failed run creation to success completion")
	}
	var response map[string]any
	if err := json.Unmarshal(completion.Response, &response); err != nil || response == nil {
		return apiidempotency.Completion{}, fmt.Errorf("decode run creation API completion")
	}
	response["data_binding"] = record.Binding
	raw, err := json.Marshal(response)
	if err != nil {
		return apiidempotency.Completion{}, err
	}
	completion.Response = raw
	return completion, nil
}

func commitPublicationTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	store eventCommitTxStore,
	command runtimebus.PublicationCommand,
) (runtimebus.CommittedPublication, error) {
	if attempt == nil {
		return runtimebus.CommittedPublication{}, fmt.Errorf("publication commit requires a mutation attempt")
	}
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	return commitValidatedPublicationTx(ctx, attempt, store, command)
}

func commitValidatedPublicationTx(
	ctx context.Context, attempt *mutationprotocol.Attempt, store eventCommitTxStore, command runtimebus.PublicationCommand,
) (runtimebus.CommittedPublication, error) {
	var result runtimebus.CommittedPublication
	err := attempt.WithSQL(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var writeErr error
		result, writeErr = commitValidatedPublicationSQL(txctx, tx, attempt, store, command)
		return writeErr
	})
	return result, err
}

func commitValidatedPublicationSQL(
	ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, store eventCommitTxStore, command runtimebus.PublicationCommand,
) (runtimebus.CommittedPublication, error) {
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
	committer := sqlPublishCommitter{attempt: attempt, store: store}
	creationAlreadyCommitted := false
	var err error
	if command.DynamicFlowCreation != nil {
		creationAlreadyCommitted, err = store.PrepareDynamicFlowCreationOccurrenceCommitTx(ctx, tx, *command.DynamicFlowCreation)
		if err != nil {
			return runtimebus.CommittedPublication{}, err
		}
	}
	outcome, err := store.appendAdmittedEventTxOutcome(ctx, attempt, request.Event, request.RouteSettlement)
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	result := runtimebus.CommittedPublication{AppendOutcome: outcome}
	if outcome == runtimebus.EventAppendExactDuplicate {
		if command.DynamicFlowCreation != nil && !creationAlreadyCommitted {
			return runtimebus.CommittedPublication{}, fmt.Errorf("dynamic flow creation event exists before readiness completion")
		}
		result.Activations, err = store.CommitFlowInstanceActivationsTx(ctx, attempt, command.Activations)
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
		if err := requestStandalonePublicationCompletion(ctx, tx, attempt, store.standaloneCompletionOwner(), request.Event.Event().ID(), request.Event.Event().RunID()); err != nil {
			return runtimebus.CommittedPublication{}, err
		}
	}
	if command.DynamicFlowCreation != nil && creationAlreadyCommitted {
		return runtimebus.CommittedPublication{}, fmt.Errorf("dynamic flow readiness is complete without its creation event")
	}
	result.Activations, err = store.CommitFlowInstanceActivationsTx(ctx, attempt, command.Activations)
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

func (s *EventPostgresOwner) CommitPublicationTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return commitPublicationTx(ctx, attempt, s, command)
}

func (s *EventSQLiteOwner) CommitPublicationTx(ctx context.Context, attempt *mutationprotocol.Attempt, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return commitPublicationTx(ctx, attempt, s, command)
}

func (s *EventPostgresOwner) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return commitPublication(ctx, s, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (runtimebus.CommittedPublication, error)) mutationprotocol.Result[runtimebus.CommittedPublication] {
		return runPostgresEventMutationResult(ctx, s, true, fn)
	}, command)
}

func (s *EventSQLiteOwner) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return commitPublication(ctx, s, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) (runtimebus.CommittedPublication, error)) mutationprotocol.Result[runtimebus.CommittedPublication] {
		return runSQLiteEventMutationResult(ctx, s, "sqlite publication commit", true, fn)
	}, command)
}

func commitRuntimeLogEventTx(ctx context.Context, store eventCommitTxStore, attempt *mutationprotocol.Attempt, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	if attempt == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("runtime-log mutation attempt is required")
	}
	if err := events.ValidateNamedEvent(admitted, events.EventAdmissionDiagnosticDirect, events.EventTypePlatformRuntimeLog); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("runtime-log operation: %w", err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteRuntimeLogDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return store.appendAdmittedEventTxOutcome(ctx, attempt, admitted, settlement)
}

func (s *EventPostgresOwner) CommitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	return runPostgresEventMutation(ctx, s, false, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.EventAppendOutcome, error) {
		return commitRuntimeLogEventTx(ctx, s, attempt, admitted)
	})
}

func (s *EventSQLiteOwner) CommitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	return runSQLiteEventMutation(ctx, s, "sqlite runtime-log event commit", false, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.EventAppendOutcome, error) {
		return commitRuntimeLogEventTx(ctx, s, attempt, admitted)
	})
}
