package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SourceRevisionDiagnosticStorage = private.SourceRevisionDiagnosticStorage

func ReadSourceRevisionDiagnosticStorage(ctx context.Context, selected any, runID string) ([]SourceRevisionDiagnosticStorage, error) {
	return private.ReadSourceRevisionDiagnosticStorageForTest(ctx, selected, runID)
}

func ReadRunLifecycleProjectionLogPayloads(ctx context.Context, selected any, runID string) ([][]byte, error) {
	return private.ReadRunLifecycleProjectionLogPayloadsForTest(ctx, selected, runID)
}
