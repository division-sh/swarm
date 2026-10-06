package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type NotifyAllChildrenItemStorage = private.NotifyAllChildrenItemStorage
type NotifyAllChildrenDiagnosticStorage = private.NotifyAllChildrenDiagnosticStorage

func ReadNotifyAllChildrenDiagnosticStorage(ctx context.Context, selected any) ([]NotifyAllChildrenDiagnosticStorage, error) {
	return private.ReadNotifyAllChildrenDiagnosticStorageForTest(ctx, selected)
}

func ReadNotifyAllChildrenItemStorage(ctx context.Context, selected any, runID, sourceEventID string) ([]NotifyAllChildrenItemStorage, error) {
	return private.ReadNotifyAllChildrenItemStorageForTest(ctx, selected, runID, sourceEventID)
}

func ReadNotifyAllChildrenMetadataStorage(ctx context.Context, selected any, instancePath string) (string, error) {
	return private.ReadNotifyAllChildrenMetadataStorageForTest(ctx, selected, instancePath)
}
