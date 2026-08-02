package store

import (
	"context"
	"database/sql"
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
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/authoractivity"
)

type eventCommitTxStore interface {
	appendAdmittedEventTxOutcome(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
	requirePipelinePublicationClaimTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim) error
	commitInitialDeliveryObligationsTx(context.Context, *sql.Tx, string, string, []events.DeliveryRoute, runtimedelivery.ExecutionAuthority) ([]runtimedelivery.DurableHandoffProof, error)
	commitInitialPipelineScopeTx(context.Context, *sql.Tx, string, runtimepipelineobligation.CommittedScope) error
	commitInitialPipelineDispositionTx(context.Context, *sql.Tx, string, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) error
	recordDeadLetterTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, runtimedeadletters.Record, bool) error
	createReplyContextTx(context.Context, *sql.Tx, runtimereplycontext.Record) error
	claimReplyContextTx(context.Context, *sql.Tx, runtimereplycontext.ClaimCommand) error
}

type sqlPublishCommitter struct {
	tx             *sql.Tx
	store          eventCommitTxStore
	story          runtimeauthoractivity.Mutation
	activeEventIDs []string
}

func runtimeAuthorActivityMutation(story *privateauthoractivity.Mutation) runtimeauthoractivity.Mutation {
	if story == nil {
		return nil
	}
	return story
}

type preparedPublishEventTxReader interface {
	loadPreparedPublishEventTx(context.Context, *sql.Tx, string) (events.AdmittedEvent, bool, error)
}

func (c *sqlPublishCommitter) LoadPreparedPublishEvent(ctx context.Context, eventID string) (events.AdmittedEvent, bool, error) {
	if c == nil || c.tx == nil || c.store == nil {
		return events.AdmittedEvent{}, false, fmt.Errorf("event commit transaction is required")
	}
	reader, ok := c.store.(preparedPublishEventTxReader)
	if !ok {
		return events.AdmittedEvent{}, false, fmt.Errorf("selected store does not expose durable event identity lookup")
	}
	return reader.loadPreparedPublishEventTx(ctx, c.tx, eventID)
}

func (c *sqlPublishCommitter) BeginPreparedPublish(ctx context.Context, event runtimebus.PreparedPublishEvent) (runtimebus.EventAppendOutcome, error) {
	if c == nil || c.tx == nil || c.store == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event commit transaction is required")
	}
	admitted := event.AdmittedEvent()
	if err := events.ValidateGenericPublishEvent(admitted.Event()); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if err := events.ValidatePersistentEvent(admitted.Event()); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	outcome, err := c.store.appendAdmittedEventTxOutcome(ctx, c.tx, c.story, admitted)
	if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
		return outcome, err
	}
	if outcome != runtimebus.EventAppendInserted {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event commit returned invalid append outcome")
	}
	c.activeEventIDs = append(c.activeEventIDs, admitted.ID())
	return outcome, nil
}

func (c *sqlPublishCommitter) FinalizePreparedPublish(ctx context.Context, finalization runtimebus.PreparedPublishFinalization) error {
	if c == nil || c.tx == nil || c.store == nil {
		return fmt.Errorf("event commit transaction is required")
	}
	req := finalization.Request()
	if len(c.activeEventIDs) == 0 || c.activeEventIDs[len(c.activeEventIDs)-1] != req.Event.ID() {
		return fmt.Errorf("prepared event finalization does not match the active event")
	}
	if err := c.commitInitialSideEffects(ctx, req, true); err != nil {
		return err
	}
	c.activeEventIDs = c.activeEventIDs[:len(c.activeEventIDs)-1]
	return nil
}

func (c sqlPublishCommitter) commitNamedEvent(ctx context.Context, operation string, class events.EventAdmissionClass, eventType events.EventType, req runtimebus.CommitPublishRequest) (runtimebus.EventAppendOutcome, error) {
	if c.tx == nil || c.store == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s event commit transaction is required", operation)
	}
	if err := events.ValidateNamedEvent(req.Event, class, eventType); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s: %w", operation, err)
	}
	outcome, err := c.store.appendAdmittedEventTxOutcome(ctx, c.tx, c.story, req.Event)
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
	for _, record := range req.ReplyCreations {
		if err := c.store.createReplyContextTx(ctx, c.tx, record); err != nil {
			return fmt.Errorf("commit reply context creation: %w", err)
		}
	}
	for _, claim := range req.ReplyClaims {
		if err := c.store.claimReplyContextTx(ctx, c.tx, claim); err != nil {
			return fmt.Errorf("commit reply context claim: %w", err)
		}
	}
	if requirePublicationClaim {
		if err := c.store.requirePipelinePublicationClaimTx(ctx, c.tx, req.Event.ID(), req.PipelineClaim); err != nil {
			return fmt.Errorf("executable event commit requires its current publication claim: %w", err)
		}
	}
	proofs, err := c.store.commitInitialDeliveryObligationsTx(
		ctx, c.tx, req.Event.ID(), req.Event.Event().RunID(), req.DeliveryRoutes, req.DeliveryAuthority,
	)
	if err != nil {
		return err
	}
	if req.DeliveryReceipt == nil {
		if len(proofs) != 0 {
			return fmt.Errorf("executable event commit requires a delivery handoff receipt")
		}
	} else if err := req.DeliveryReceipt.Record(proofs); err != nil {
		return err
	}
	if err := c.store.commitInitialPipelineScopeTx(ctx, c.tx, req.Event.ID(), req.ReplayScope); err != nil {
		return err
	}
	if req.Disposition != nil {
		if err := c.store.commitInitialPipelineDispositionTx(ctx, c.tx, req.Event.ID(), req.PipelineClaim, *req.Disposition); err != nil {
			return err
		}
	}
	if req.DeadLetter != nil {
		if err := c.store.recordDeadLetterTx(ctx, c.tx, c.story, *req.DeadLetter, true); err != nil {
			return err
		}
	}
	return nil
}

func validateSelectedForkCommitRequest(req runtimebus.CommitSelectedForkEventRequest) error {
	if err := events.ValidatePersistentEvent(req.Commit.Event.Event()); err != nil {
		return err
	}
	if req.Commit.Event.Class() != events.EventAdmissionSelectedForkReplay {
		return fmt.Errorf("selected-fork operation requires selected_fork_replay event class")
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
	store eventCommitTxStore,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	insertLineage func(context.Context, *sql.Tx, runfork.RunForkSelectedContractExecutionLineage) error,
	req runtimebus.CommitSelectedForkEventRequest,
) (runtimebus.EventAppendOutcome, error) {
	if err := validateSelectedForkCommitRequest(req); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	outcome := runtimebus.EventAppendOutcomeUnknown
	err := run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		runtimeStory := runtimeAuthorActivityMutation(story)
		committer := sqlPublishCommitter{tx: tx, store: store, story: runtimeStory}
		var err error
		outcome, err = store.appendAdmittedEventTxOutcome(txctx, tx, runtimeStory, req.Commit.Event)
		if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
			return err
		}
		if outcome != runtimebus.EventAppendInserted {
			return fmt.Errorf("selected-fork operation returned invalid append outcome")
		}
		if err := insertLineage(txctx, tx, req.Lineage); err != nil {
			return err
		}
		return committer.commitInitialSideEffects(txctx, req.Commit, true)
	})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func (s *PostgresStore) CommitSelectedForkEvent(ctx context.Context, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.EventAppendOutcome, error) {
	state, err := s.lockPostgresPipelineClaim(req.Commit.PipelineClaim)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	defer state.operationMu.Unlock()
	claimCtx, err := state.postgresLease.BindContext(ctx)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, runtimepipelineobligation.ErrStaleClaim
	}
	return commitSelectedForkEvent(claimCtx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, fn)
	}, insertPostgresSelectedForkExecutionLineageTx, req)
}

func (s *SQLiteRuntimeStore) CommitSelectedForkEvent(ctx context.Context, req runtimebus.CommitSelectedForkEventRequest) (runtimebus.EventAppendOutcome, error) {
	state, err := s.lockSQLitePipelineClaim(req.Commit.PipelineClaim)
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	defer state.operationMu.Unlock()
	return commitSelectedForkEvent(ctx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite selected-fork event commit", fn)
	}, insertSQLiteSelectedForkExecutionLineageTx, req)
}

func commitPublish(ctx context.Context, store eventCommitTxStore, run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	if plan == nil {
		return runtimebus.PreparedPublish{}, fmt.Errorf("event publish plan is required")
	}
	var prepared runtimebus.PreparedPublish
	err := run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		committer := &sqlPublishCommitter{tx: tx, store: store, story: runtimeAuthorActivityMutation(story)}
		txctx = runtimebus.WithCommitPublishTransaction(txctx, committer)
		var err error
		prepared, err = plan.PrepareCommitPublish(txctx)
		return err
	})
	if err != nil {
		return runtimebus.PreparedPublish{}, err
	}
	return prepared, nil
}

func (s *PostgresStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return commitPublish(ctx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, fn)
	}, plan)
}

func (s *SQLiteRuntimeStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return commitPublish(ctx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite publication commit", fn)
	}, plan)
}

func commitRuntimeLogEvent(ctx context.Context, store eventCommitTxStore, run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	if err := events.ValidateNamedEvent(admitted, events.EventAdmissionDiagnosticDirect, events.EventTypePlatformRuntimeLog); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("runtime-log operation: %w", err)
	}
	outcome := runtimebus.EventAppendOutcomeUnknown
	err := run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		outcome, err = store.appendAdmittedEventTxOutcome(txctx, tx, runtimeAuthorActivityMutation(story), admitted)
		return err
	})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func (s *PostgresStore) commitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	return commitRuntimeLogEvent(ctx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, fn)
	}, admitted)
}

func (s *SQLiteRuntimeStore) commitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	return commitRuntimeLogEvent(ctx, s, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite runtime-log event commit", fn)
	}, admitted)
}

func eventCommitterForPipelineContext(ctx context.Context, store eventCommitTxStore, story runtimeauthoractivity.Mutation) (context.Context, bool) {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok {
		return ctx, false
	}
	committer := &sqlPublishCommitter{tx: tx, store: store, story: story}
	return runtimebus.WithCommitPublishTransaction(ctx, committer), true
}
