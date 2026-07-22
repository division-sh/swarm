package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
)

type pipelineTestDeliveryOwner struct {
	db      *sql.DB
	dialect runtimedelivery.Dialect
	adapter *runtimedelivery.Adapter
}

func newPipelineTestDeliveryOwnerForDB(t interface {
	Helper()
	Fatalf(string, ...any)
}, db *sql.DB) *pipelineTestDeliveryOwner {
	t.Helper()
	driverType := fmt.Sprintf("%T", db.Driver())
	return newPipelineTestDeliveryOwner(t, db, strings.Contains(strings.ToLower(driverType), "sqlite"))
}

func newPipelineTestDeliveryOwner(t interface {
	Helper()
	Fatalf(string, ...any)
}, db *sql.DB, sqlite bool) *pipelineTestDeliveryOwner {
	t.Helper()
	dialect := runtimedelivery.DialectPostgres
	if sqlite {
		dialect = runtimedelivery.DialectSQLite
	}
	adapter, err := runtimedelivery.NewAdapter(dialect)
	if err != nil {
		t.Fatalf("create pipeline test delivery owner: %v", err)
	}
	return &pipelineTestDeliveryOwner{db: db, dialect: dialect, adapter: adapter}
}

func (s *pipelineTestDeliveryOwner) mutate(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	storyDialect := runtimeauthoractivity.DialectPostgres
	if s.dialect == runtimedelivery.DialectSQLite {
		storyDialect = runtimeauthoractivity.DialectSQLite
	}
	storyctx, err := runtimeauthoractivity.Begin(ctx, tx, storyDialect)
	if err != nil {
		return err
	}
	if err := fn(storyctx, tx); err != nil {
		return err
	}
	if err := runtimeauthoractivity.Finalize(storyctx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *pipelineTestDeliveryOwner) commitInitial(ctx context.Context, event events.Event, route events.DeliveryRoute) error {
	return s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := s.adapter.CommitInitial(ctx, tx, event.ID(), event.RunID(), []events.DeliveryRoute{route})
		return err
	})
}

func (s *pipelineTestDeliveryOwner) commitNode(ctx context.Context, event events.Event, nodeID string, target events.RouteIdentity) error {
	return s.commitInitial(ctx, event, events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberNode),
		SubscriberID:   strings.TrimSpace(nodeID),
		Target:         target.Normalized(),
	})
}

func (s *pipelineTestDeliveryOwner) loadEvent(ctx context.Context, eventID string) (events.Event, error) {
	dialect := runtimeauthoractivity.DialectPostgres
	if s.dialect == runtimedelivery.DialectSQLite {
		dialect = runtimeauthoractivity.DialectSQLite
	}
	return eventfixture.Load(ctx, s.db, dialect, eventID)
}

func seedPipelineTestNodeDelivery(t interface {
	Helper()
	Fatalf(string, ...any)
}, ctx context.Context, db *sql.DB, eventID, nodeID string, target events.RouteIdentity) *pipelineTestDeliveryOwner {
	t.Helper()
	owner := newPipelineTestDeliveryOwnerForDB(t, db)
	event, err := owner.loadEvent(ctx, eventID)
	if err != nil {
		t.Fatalf("load pipeline test delivery event %s: %v", eventID, err)
	}
	if err := owner.commitNode(ctx, event, nodeID, target); err != nil {
		t.Fatalf("commit pipeline test delivery %s/%s: %v", eventID, nodeID, err)
	}
	return owner
}

func configurePipelineTestDeliveryOwner(t interface {
	Helper()
	Fatalf(string, ...any)
}, pc *PipelineCoordinator) *pipelineTestDeliveryOwner {
	t.Helper()
	if pc == nil || pc.workflowStore == nil || pc.workflowStore.db == nil {
		t.Fatalf("pipeline test delivery owner requires a configured workflow store")
	}
	if owner, ok := pc.workflowStore.DeliveryLifecycleStore().(*pipelineTestDeliveryOwner); ok {
		return owner
	}
	owner := newPipelineTestDeliveryOwnerForDB(t, pc.workflowStore.db)
	pc.workflowStore.ConfigureDeliveryLifecycleStore(owner)
	return owner
}

func (s *pipelineTestDeliveryOwner) ClaimAgentDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (out runtimedelivery.ClaimedObligation, err error) {
	err = s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = s.adapter.ClaimExact(ctx, tx, event, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return out, err
}

func (s *pipelineTestDeliveryOwner) ClaimNodeDelivery(ctx context.Context, event events.Event, route events.DeliveryRoute) (out runtimedelivery.ClaimedObligation, err error) {
	return s.ClaimAgentDelivery(ctx, event, route)
}

func (*pipelineTestDeliveryOwner) ClaimAgentBacklog(context.Context, string, int) ([]runtimedelivery.AgentExecution, error) {
	return nil, fmt.Errorf("pipeline unit fixture does not hydrate agent backlog")
}

func (*pipelineTestDeliveryOwner) ClaimNodeBacklog(context.Context, string, int) ([]runtimedelivery.NodeExecution, error) {
	return nil, fmt.Errorf("pipeline unit fixture does not hydrate node backlog")
}

func (s *pipelineTestDeliveryOwner) BindAgentSession(ctx context.Context, claim runtimedelivery.Claim, sessionID string) (out runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = s.adapter.BindAgentSession(ctx, tx, claim, sessionID)
		return err
	})
	return out, err
}

func (s *pipelineTestDeliveryOwner) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (out runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = s.adapter.RenewClaim(ctx, tx, claim, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return out, err
}

func (s *pipelineTestDeliveryOwner) SettleSuccess(ctx context.Context, claim runtimedelivery.Claim, effects []string, duration time.Duration) (out runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = s.adapter.SettleSuccess(ctx, tx, claim, effects, duration)
		return err
	})
	return out, err
}

func (s *pipelineTestDeliveryOwner) SettleFailure(ctx context.Context, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (out runtimedelivery.Snapshot, err error) {
	err = s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = s.adapter.SettleFailure(ctx, tx, claim, settlement)
		return err
	})
	return out, err
}

func (s *pipelineTestDeliveryOwner) Snapshot(ctx context.Context, deliveryID string) (runtimedelivery.Snapshot, error) {
	return s.adapter.Snapshot(ctx, s.db, deliveryID)
}

func (s *pipelineTestDeliveryOwner) Outcomes(ctx context.Context, deliveryID string) ([]runtimedelivery.Outcome, error) {
	return s.adapter.Outcomes(ctx, s.db, deliveryID)
}

func (s *pipelineTestDeliveryOwner) ProveHandoff(ctx context.Context, eventID string, route events.DeliveryRoute) (runtimedelivery.DurableHandoffProof, error) {
	return s.adapter.ProveHandoff(ctx, s.db, eventID, route)
}

func (s *pipelineTestDeliveryOwner) SummarizeRun(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
	return s.adapter.SummarizeRun(ctx, s.db, runID)
}

func (s *pipelineTestDeliveryOwner) TerminalizeRun(ctx context.Context, runID, reason string) (out []runtimedelivery.Terminalization, err error) {
	err = s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = s.adapter.TerminalizeRun(ctx, tx, runID, reason)
		return err
	})
	return out, err
}

var _ runtimedelivery.Store = (*pipelineTestDeliveryOwner)(nil)
