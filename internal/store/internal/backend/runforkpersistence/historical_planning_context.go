package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type historicalPlanningContextKey struct{}

type historicalPlanningReconstruction struct {
	tx       *sql.Tx
	source   semanticview.Source
	run      runlifecycle.Snapshot
	resolver capturedConstructionResolver
	closed   atomic.Bool
	reading  atomic.Bool
}

func (s *RunForkPostgresOwner) WithHistoricalFlowInstancePlanningTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot, prepare func(context.Context) error) error {
	return withHistoricalFlowInstancePlanningTx(ctx, tx, source, run, prepare)
}

func (s *RunForkSQLiteOwner) WithHistoricalFlowInstancePlanningTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot, prepare func(context.Context) error) error {
	return withHistoricalFlowInstancePlanningTx(ctx, tx, source, run, prepare)
}

func withHistoricalFlowInstancePlanningTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot, prepare func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil || source == nil || run.Validate() != nil || prepare == nil {
		return fmt.Errorf("historical planning requires its exact source, run, transaction and callback")
	}
	if prior, _ := ctx.Value(historicalPlanningContextKey{}).(*historicalPlanningReconstruction); prior != nil && prior.tx == tx {
		return fmt.Errorf("historical planning transaction already has its reconstruction owner")
	}
	scope := &historicalPlanningReconstruction{
		tx: tx, source: source, run: run,
		resolver: capturedConstructionResolver{tx: tx, snapshots: make(map[constructionCut]*runForkRevisionSnapshot), active: make(map[string]bool)},
	}
	defer scope.closed.Store(true)
	return prepare(context.WithValue(ctx, historicalPlanningContextKey{}, scope))
}

func historicalPlanningForTx(ctx context.Context, tx *sql.Tx) (*historicalPlanningReconstruction, error) {
	scope, _ := ctx.Value(historicalPlanningContextKey{}).(*historicalPlanningReconstruction)
	if scope == nil {
		return nil, nil
	}
	if err := scope.require(ctx, tx); err != nil {
		return nil, err
	}
	return scope, nil
}

func (scope *historicalPlanningReconstruction) require(ctx context.Context, tx *sql.Tx) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if scope.closed.Load() {
		return fmt.Errorf("historical planning read lifetime has ended")
	}
	if tx != scope.tx {
		return fmt.Errorf("historical planning cannot cross its read transaction")
	}
	return nil
}

func requireHistoricalPlanningRun(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot) error {
	scope, err := historicalPlanningForTx(ctx, tx)
	if err != nil {
		return err
	}
	if scope != nil && !reflect.DeepEqual(run, scope.run) {
		return fmt.Errorf("historical planning cannot cross its admitted run")
	}
	return nil
}

// Each existing synchronous snapshot read enters this owner within its own TX.
// Reject concurrent use rather than introducing another lock or SQL lifetime.
func beginHistoricalPlanningRead(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot) (func() error, error) {
	scope, err := historicalPlanningForTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if scope == nil {
		return func() error { return nil }, nil
	}
	if !reflect.DeepEqual(run, scope.run) || (source != nil && !reflect.DeepEqual(source, scope.source)) {
		return nil, fmt.Errorf("historical planning cannot cross its admitted run or source")
	}
	if !scope.reading.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("historical planning reads require the serialized planning owner")
	}
	return func() error {
		defer scope.reading.Store(false)
		return scope.require(ctx, tx)
	}, nil
}
