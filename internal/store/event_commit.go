package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type eventCommitTxStore interface {
	appendAdmittedEventTxOutcome(context.Context, *sql.Tx, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
	InsertEventDeliveryRoutesTx(context.Context, *sql.Tx, string, []events.DeliveryRoute) error
	UpsertCommittedReplayScopeTx(context.Context, *sql.Tx, string, runtimereplayclaim.CommittedReplayScope) error
	UpsertPipelineReceiptTx(context.Context, *sql.Tx, string, string, *runtimefailures.Envelope) error
	RecordDeadLetterTx(context.Context, *sql.Tx, runtimedeadletters.Record) error
}

type sqlPublishCommitter struct {
	tx             *sql.Tx
	store          eventCommitTxStore
	activeEventIDs []string
}

func (c *sqlPublishCommitter) BeginPreparedPublish(ctx context.Context, event runtimebus.PreparedPublishEvent) (runtimebus.EventAppendOutcome, error) {
	if c == nil || c.tx == nil || c.store == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event commit transaction is required")
	}
	admitted := event.AdmittedEvent()
	if admitted.ID() == "" {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("admitted event is required")
	}
	switch admitted.Class() {
	case events.EventAdmissionDiagnosticDirect, events.EventAdmissionSelectedForkReplay:
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event class %q requires its closed named persistence operation", admitted.Class())
	}
	outcome, err := c.store.appendAdmittedEventTxOutcome(ctx, c.tx, admitted)
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
	if err := c.commitInitialSideEffects(ctx, req); err != nil {
		return err
	}
	c.activeEventIDs = c.activeEventIDs[:len(c.activeEventIDs)-1]
	return nil
}

func (c sqlPublishCommitter) commitNamedEvent(ctx context.Context, operation string, class events.EventAdmissionClass, req runtimebus.CommitPublishRequest) (runtimebus.EventAppendOutcome, error) {
	if c.tx == nil || c.store == nil {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s event commit transaction is required", operation)
	}
	if req.Event.ID() == "" || req.Event.Class() != class {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("%s requires admitted %s event class", operation, class)
	}
	outcome, err := c.store.appendAdmittedEventTxOutcome(ctx, c.tx, req.Event)
	if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
		return outcome, err
	}
	if outcome != runtimebus.EventAppendInserted {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event commit returned invalid append outcome")
	}
	if err := c.commitInitialSideEffects(ctx, req); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func (c sqlPublishCommitter) commitInitialSideEffects(ctx context.Context, req runtimebus.CommitPublishRequest) error {
	if err := c.store.InsertEventDeliveryRoutesTx(ctx, c.tx, req.Event.ID(), req.DeliveryRoutes); err != nil {
		return err
	}
	if err := c.store.UpsertCommittedReplayScopeTx(ctx, c.tx, req.Event.ID(), req.ReplayScope); err != nil {
		return err
	}
	if req.PipelineReceipt != nil {
		if err := c.store.UpsertPipelineReceiptTx(ctx, c.tx, req.Event.ID(), req.PipelineReceipt.Status, req.PipelineReceipt.Failure); err != nil {
			return err
		}
	}
	if req.DeadLetter != nil {
		if err := c.store.RecordDeadLetterTx(ctx, c.tx, *req.DeadLetter); err != nil {
			return err
		}
	}
	return nil
}

type CommitSelectedForkEventRequest struct {
	Commit  runtimebus.CommitPublishRequest
	Lineage RunForkSelectedContractExecutionLineage
}

func validateSelectedForkCommitRequest(req CommitSelectedForkEventRequest) error {
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
	run func(context.Context, func(context.Context, *sql.Tx) error) error,
	insertLineage func(context.Context, *sql.Tx, RunForkSelectedContractExecutionLineage) error,
	req CommitSelectedForkEventRequest,
) (runtimebus.EventAppendOutcome, error) {
	if err := validateSelectedForkCommitRequest(req); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	outcome := runtimebus.EventAppendOutcomeUnknown
	err := run(ctx, func(txctx context.Context, tx *sql.Tx) error {
		committer := sqlPublishCommitter{tx: tx, store: store}
		var err error
		outcome, err = store.appendAdmittedEventTxOutcome(txctx, tx, req.Commit.Event)
		if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
			return err
		}
		if outcome != runtimebus.EventAppendInserted {
			return fmt.Errorf("selected-fork operation returned invalid append outcome")
		}
		if err := insertLineage(txctx, tx, req.Lineage); err != nil {
			return err
		}
		return committer.commitInitialSideEffects(txctx, req.Commit)
	})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func (s *PostgresStore) CommitSelectedForkEvent(ctx context.Context, req CommitSelectedForkEventRequest) (runtimebus.EventAppendOutcome, error) {
	return commitSelectedForkEvent(ctx, s, s.runEventTransaction, insertPostgresSelectedForkExecutionLineageTx, req)
}

func (s *SQLiteRuntimeStore) CommitSelectedForkEvent(ctx context.Context, req CommitSelectedForkEventRequest) (runtimebus.EventAppendOutcome, error) {
	return commitSelectedForkEvent(ctx, s, s.runEventTransaction, insertSQLiteSelectedForkExecutionLineageTx, req)
}

func commitPublish(ctx context.Context, store eventCommitTxStore, run func(context.Context, func(context.Context, *sql.Tx) error) error, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	if plan == nil {
		return runtimebus.PreparedPublish{}, fmt.Errorf("event publish plan is required")
	}
	var prepared runtimebus.PreparedPublish
	err := run(ctx, func(txctx context.Context, tx *sql.Tx) error {
		committer := &sqlPublishCommitter{tx: tx, store: store}
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
	return commitPublish(ctx, s, s.runEventTransaction, plan)
}

func (s *SQLiteRuntimeStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return commitPublish(ctx, s, s.runEventTransaction, plan)
}

func commitRuntimeLogEvent(ctx context.Context, store eventCommitTxStore, run func(context.Context, func(context.Context, *sql.Tx) error) error, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	if admitted.Class() != events.EventAdmissionDiagnosticDirect || admitted.Event().Type() != events.EventTypePlatformRuntimeLog {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("runtime-log operation requires a diagnostic_direct platform.runtime_log event")
	}
	outcome := runtimebus.EventAppendOutcomeUnknown
	err := run(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		outcome, err = store.appendAdmittedEventTxOutcome(txctx, tx, admitted)
		return err
	})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return outcome, nil
}

func (s *PostgresStore) commitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	return commitRuntimeLogEvent(ctx, s, s.runEventTransaction, admitted)
}

func (s *SQLiteRuntimeStore) commitRuntimeLogEvent(ctx context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	return commitRuntimeLogEvent(ctx, s, s.runEventTransaction, admitted)
}

func eventCommitterForPipelineContext(ctx context.Context, store eventCommitTxStore) (context.Context, bool) {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok {
		return ctx, false
	}
	committer := &sqlPublishCommitter{tx: tx, store: store}
	return runtimebus.WithCommitPublishTransaction(ctx, committer), true
}
