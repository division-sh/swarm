package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func InstallDynamicFlowCreationCompletionFailureForTest(ctx context.Context, selected any, request pipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (func(context.Context) error, error) {
	postgres, err := workflowProjectionNativePostgres(selected)
	if err != nil {
		return nil, err
	}
	var fault pipelinepersistence.DynamicFlowCreationCompletionFault
	err = runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		fault, err = pipelinepersistence.InstallDynamicFlowCreationCompletionFailureTx(ctx, tx, postgres, request)
		return err
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		return runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			return pipelinepersistence.RemoveDynamicFlowCreationCompletionFailureTx(ctx, tx, fault)
		})
	}, nil
}
