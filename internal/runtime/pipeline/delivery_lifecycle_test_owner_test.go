package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	deliveryfixture "github.com/division-sh/swarm/internal/store/testutil/deliveryfixture"
)

type pipelineTestContinuationOwner struct {
	mu   sync.Mutex
	held map[string]bool
}

func newPipelineTestContinuationOwner() *pipelineTestContinuationOwner {
	return &pipelineTestContinuationOwner{held: make(map[string]bool)}
}

func (o *pipelineTestContinuationOwner) Acquire(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return nil, fmt.Errorf("pipeline test delivery id is required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if held, exists := o.held[deliveryID]; exists && !held {
		return nil, fmt.Errorf("pipeline test delivery %s is already carrier-owned", deliveryID)
	}
	o.held[deliveryID] = false
	return &pipelineTestContinuation{owner: o, deliveryID: deliveryID}, nil
}

func (o *pipelineTestContinuationOwner) Retain(snapshot runtimedelivery.Snapshot) error {
	deliveryID := strings.TrimSpace(snapshot.DeliveryID)
	if deliveryID == "" {
		return fmt.Errorf("pipeline test retained delivery id is required")
	}
	o.mu.Lock()
	o.held[deliveryID] = true
	o.mu.Unlock()
	return nil
}

func (o *pipelineTestContinuationOwner) Release(deliveryID string) error {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return fmt.Errorf("pipeline test released delivery id is required")
	}
	o.mu.Lock()
	delete(o.held, deliveryID)
	o.mu.Unlock()
	return nil
}

type pipelineTestContinuation struct {
	mu         sync.Mutex
	owner      *pipelineTestContinuationOwner
	deliveryID string
	settled    bool
}

func (c *pipelineTestContinuation) DeliveryID() string { return c.deliveryID }

func (c *pipelineTestContinuation) Resolve(_ context.Context, intent worklifetime.DeliveryContinuationIntent) (worklifetime.DeliveryContinuationResolution, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settled {
		return 0, fmt.Errorf("pipeline test delivery continuation is already settled")
	}
	c.owner.mu.Lock()
	if intent == worklifetime.DeliveryContinuationReturn {
		c.owner.held[c.deliveryID] = true
	} else if intent == worklifetime.DeliveryContinuationConsume {
		c.owner.held[c.deliveryID] = false
	} else {
		c.owner.mu.Unlock()
		return 0, fmt.Errorf("pipeline test delivery continuation intent is invalid")
	}
	c.owner.mu.Unlock()
	c.settled = true
	if intent == worklifetime.DeliveryContinuationReturn {
		return worklifetime.DeliveryContinuationReturned, nil
	}
	return worklifetime.DeliveryContinuationConsumed, nil
}

type pipelineTestDeliveryOwner struct {
	db      *sql.DB
	dialect deliveryfixture.Dialect
	adapter *deliveryfixture.Adapter
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
	dialect := deliveryfixture.DialectPostgres
	if sqlite {
		dialect = deliveryfixture.DialectSQLite
	}
	adapter, err := deliveryfixture.NewAdapter(dialect)
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
	storyDialect := authoractivityfixture.DialectPostgres
	if s.dialect == deliveryfixture.DialectSQLite {
		storyDialect = authoractivityfixture.DialectSQLite
	}
	storyctx, err := authoractivityfixture.Begin(ctx, tx, storyDialect)
	if err != nil {
		return err
	}
	if err := fn(storyctx, tx); err != nil {
		return err
	}
	if err := authoractivityfixture.Finalize(storyctx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *pipelineTestDeliveryOwner) commitInitial(ctx context.Context, event events.Event, route events.DeliveryRoute) error {
	return s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		authority, err := s.authorityForRun(ctx, tx, event.RunID())
		if err != nil {
			return err
		}
		_, err = s.adapter.CommitInitial(ctx, tx, event.ID(), event.RunID(), []events.DeliveryRoute{route}, authority)
		return err
	})
}

func (s *pipelineTestDeliveryOwner) authorityForRun(ctx context.Context, tx *sql.Tx, runID string) (runtimedelivery.ExecutionAuthority, error) {
	query := `SELECT bundle_hash, bundle_source FROM runs WHERE run_id=$1::uuid`
	if s.dialect == deliveryfixture.DialectSQLite {
		query = `SELECT bundle_hash, bundle_source FROM runs WHERE run_id=?`
	}
	var bundleHash, bundleSource string
	if err := tx.QueryRowContext(ctx, query, runID).Scan(&bundleHash, &bundleSource); err != nil {
		return runtimedelivery.ExecutionAuthority{}, err
	}
	source, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return runtimedelivery.ExecutionAuthority{}, err
	}
	return runtimedelivery.NewNormalExecutionAuthority(source, "pipeline-test-normal-runtime", 1)
}

func (s *pipelineTestDeliveryOwner) commitNode(ctx context.Context, event events.Event, nodeID string, target events.RouteIdentity) error {
	return s.commitInitial(ctx, event, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(strings.TrimSpace(nodeID)), Target: events.MustExistingEntityTarget(target.Normalized())})
}

func (s *pipelineTestDeliveryOwner) loadEvent(ctx context.Context, eventID string) (events.Event, error) {
	dialect := authoractivityfixture.DialectPostgres
	if s.dialect == deliveryfixture.DialectSQLite {
		dialect = authoractivityfixture.DialectSQLite
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
	if pc == nil || pc.workflowStore == nil || pc.workflowStore.testDB() == nil {
		t.Fatalf("pipeline test delivery owner requires a configured workflow store")
	}
	if owner, ok := pc.deliveryStore.(*pipelineTestDeliveryOwner); ok {
		if bus, ok := pc.bus.(interface {
			configurePipelineTestDeliveryOwner(*pipelineTestDeliveryOwner)
		}); ok {
			bus.configurePipelineTestDeliveryOwner(owner)
		}
		if runtime, ok := pc.bus.(WorkflowDeliveryRuntime); ok {
			pc.deliveryRuntime = runtime
		}
		pc.workflowStore.deliveryStore = owner
		return owner
	}
	owner := newPipelineTestDeliveryOwnerForDB(t, pc.workflowStore.testDB())
	pc.deliveryStore = owner
	pc.workflowStore.deliveryStore = owner
	if bus, ok := pc.bus.(interface {
		configurePipelineTestDeliveryOwner(*pipelineTestDeliveryOwner)
	}); ok {
		bus.configurePipelineTestDeliveryOwner(owner)
	}
	if runtime, ok := pc.bus.(WorkflowDeliveryRuntime); ok {
		pc.deliveryRuntime = runtime
	}
	return owner
}

func (s *pipelineTestDeliveryOwner) activeExecutionAuthority(ctx context.Context) (runtimedelivery.ExecutionAuthority, error) {
	query := `
		SELECT execution_authority_kind, authority_bundle_hash, authority_bundle_source,
		       execution_authority_id, execution_authority_generation
		FROM event_deliveries
		ORDER BY created_at DESC, delivery_id DESC
		LIMIT 1`
	var (
		kind       runtimedelivery.ExecutionAuthorityKind
		bundleHash string
		source     string
		execution  string
		generation uint64
	)
	if err := s.db.QueryRowContext(ctx, query).Scan(
		&kind,
		&bundleHash,
		&source,
		&execution,
		&generation,
	); err != nil {
		return runtimedelivery.ExecutionAuthority{}, err
	}
	return runtimedelivery.DecodeExecutionAuthority(kind, bundleHash, source, execution, "", generation)
}

func (s *pipelineTestDeliveryOwner) makeRetryEligible(ctx context.Context, deliveryID string) error {
	query := `
		UPDATE event_deliveries
		SET next_eligible_at=CURRENT_TIMESTAMP
		WHERE delivery_id=$1::uuid AND status='failed'`
	if s.dialect == deliveryfixture.DialectSQLite {
		query = `
			UPDATE event_deliveries
			SET next_eligible_at=CURRENT_TIMESTAMP
			WHERE delivery_id=? AND status='failed'`
	}
	result, err := s.db.ExecContext(ctx, query, strings.TrimSpace(deliveryID))
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("pipeline test delivery %s was not retry-scheduled", deliveryID)
	}
	return nil
}

func (s *pipelineTestDeliveryOwner) ActivateDeliveryAuthority(ctx context.Context, authority runtimedelivery.ExecutionAuthority) (err error) {
	err = s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return s.adapter.ActivateNormalAuthority(ctx, tx, authority)
	})
	return err
}

func (s *pipelineTestDeliveryOwner) InspectDeliveryRecovery(
	ctx context.Context,
	source runtimecorrelation.BundleSourceFact,
) (runtimedelivery.RecoveryInventory, error) {
	return s.adapter.InspectRecovery(ctx, s.db, source)
}

func (s *pipelineTestDeliveryOwner) ClaimDelivery(ctx context.Context, authority runtimedelivery.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (out runtimedelivery.ClaimResult, err error) {
	err = s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = s.adapter.ClaimExactResult(ctx, tx, authority, event, route, runtimedelivery.DefaultLeaseTTL)
		return err
	})
	return out, err
}

func (s *pipelineTestDeliveryOwner) ScanDeliveryContinuations(ctx context.Context, authority runtimedelivery.ExecutionAuthority, cursor runtimedelivery.ContinuationCursor, limit int) (out runtimedelivery.ContinuationPage, err error) {
	err = s.mutate(ctx, func(ctx context.Context, tx *sql.Tx) error {
		out, err = s.adapter.ScanContinuations(ctx, tx, authority, cursor, limit)
		return err
	})
	if err != nil {
		return runtimedelivery.ContinuationPage{}, err
	}
	for index := range out.Items {
		out.Items[index].Event, err = s.loadEvent(ctx, out.Items[index].Snapshot.EventID)
		if err != nil {
			return runtimedelivery.ContinuationPage{}, err
		}
	}
	return out, nil
}

func (s *pipelineTestDeliveryOwner) ObserveDeliveryContinuation(
	ctx context.Context,
	authority runtimedelivery.ExecutionAuthority,
	deliveryID string,
) (runtimedelivery.ContinuationObservation, error) {
	return s.adapter.ObserveContinuation(ctx, s.db, authority, deliveryID)
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
