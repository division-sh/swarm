package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func FaultFlowConstructorHeaderForTest(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, field pipelinepersistence.FlowConstructorHeaderFaultField, value any) (func(context.Context) error, error) {
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.FaultFlowConstructorHeaderForTest(ctx, owner, field, value)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.FaultFlowConstructorHeaderForTest(ctx, owner, field, value)
	default:
		return nil, fmt.Errorf("constructor header fault requires a selected store")
	}
}
