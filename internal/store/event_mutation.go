package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type eventMutationDeadLetterTxRecorder interface {
	RecordDeadLetterTx(context.Context, *sql.Tx, runtimedeadletters.Record) error
}

type sqlEventMutation struct {
	ctx     context.Context
	tx      *sql.Tx
	txStore runtimebus.TransactionalEventStore
	store   any
}

func newSQLEventMutation(ctx context.Context, tx *sql.Tx, txStore runtimebus.TransactionalEventStore, store any) runtimebus.EventMutation {
	mutation := &sqlEventMutation{
		ctx:     ctx,
		tx:      tx,
		txStore: txStore,
		store:   store,
	}
	mutation.ctx = runtimebus.WithEventMutationContext(ctx, mutation)
	return mutation
}

func (m *sqlEventMutation) Context() context.Context {
	if m == nil || m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *sqlEventMutation) operationContext(ctx context.Context) context.Context {
	if ctx == nil {
		return m.Context()
	}
	return ctx
}

func (m *sqlEventMutation) AppendEvent(ctx context.Context, evt events.Event) error {
	if m == nil || m.txStore == nil {
		return fmt.Errorf("event mutation store is required")
	}
	return m.txStore.AppendEventTx(m.operationContext(ctx), m.tx, evt)
}

func (m *sqlEventMutation) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	if m == nil || m.txStore == nil {
		return fmt.Errorf("event mutation store is required")
	}
	return m.txStore.InsertEventDeliveriesTx(m.operationContext(ctx), m.tx, eventID, agentIDs)
}

func (m *sqlEventMutation) InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	if m == nil || m.txStore == nil {
		return fmt.Errorf("event mutation store is required")
	}
	if writer, ok := m.store.(runtimebus.TransactionalEventDeliveryRoutePersistence); ok && writer != nil {
		return writer.InsertEventDeliveriesWithTargetsTx(m.operationContext(ctx), m.tx, eventID, agentIDs, deliveryTargets)
	}
	return m.txStore.InsertEventDeliveriesTx(m.operationContext(ctx), m.tx, eventID, agentIDs)
}

func (m *sqlEventMutation) InsertEventDeliveryRoutes(ctx context.Context, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	if m == nil {
		return fmt.Errorf("event mutation store is required")
	}
	if writer, ok := m.store.(runtimebus.TransactionalEventDeliveryRouteSetPersistence); ok && writer != nil {
		return writer.InsertEventDeliveryRoutesTx(m.operationContext(ctx), m.tx, eventID, deliveryRoutes)
	}
	return fmt.Errorf("event mutation store does not support typed delivery routes")
}

func (m *sqlEventMutation) UpsertCommittedReplayScope(ctx context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	if m == nil {
		return runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	if writer, ok := m.store.(runtimebus.TransactionalEventReplayScopePersistence); ok && writer != nil {
		return writer.UpsertCommittedReplayScopeTx(m.operationContext(ctx), m.tx, eventID, scope)
	}
	return runtimereplayclaim.ErrMissingCommittedReplayScope
}

func (m *sqlEventMutation) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	if m == nil || m.txStore == nil {
		return fmt.Errorf("event mutation store is required")
	}
	return m.txStore.UpsertPipelineReceiptTx(m.operationContext(ctx), m.tx, eventID, status, errText)
}

func (m *sqlEventMutation) RecordDeadLetter(ctx context.Context, rec runtimedeadletters.Record) error {
	if m == nil {
		return nil
	}
	recorder, ok := m.store.(eventMutationDeadLetterTxRecorder)
	if !ok || recorder == nil {
		return nil
	}
	return recorder.RecordDeadLetterTx(m.operationContext(ctx), m.tx, rec)
}

func (s *SQLiteRuntimeStore) RunEventMutation(ctx context.Context, fn func(runtimebus.EventMutation) error) error {
	if fn == nil {
		return nil
	}
	return s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return fn(newSQLEventMutation(txctx, tx, s, s))
	})
}

func (s *SQLiteRuntimeStore) EventMutationFromContext(ctx context.Context) (runtimebus.EventMutation, bool) {
	if mutation, ok := runtimebus.EventMutationFromContext(ctx); ok {
		return mutation, true
	}
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return nil, false
	}
	return newSQLEventMutation(ctx, tx, s, s), true
}

func (s *PostgresStore) RunEventMutation(ctx context.Context, fn func(runtimebus.EventMutation) error) error {
	if fn == nil {
		return nil
	}
	return s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return fn(newSQLEventMutation(txctx, tx, s, s))
	})
}

func (s *PostgresStore) EventMutationFromContext(ctx context.Context) (runtimebus.EventMutation, bool) {
	if mutation, ok := runtimebus.EventMutationFromContext(ctx); ok {
		return mutation, true
	}
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return nil, false
	}
	return newSQLEventMutation(ctx, tx, s, s), true
}
