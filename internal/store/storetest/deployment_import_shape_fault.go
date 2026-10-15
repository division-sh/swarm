package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CorruptRootReadyImportShapeFault(ctx context.Context, selected any, bundleHash string) error {
	return private.CorruptRootReadyImportShapeFaultForTest(ctx, selected, bundleHash)
}
