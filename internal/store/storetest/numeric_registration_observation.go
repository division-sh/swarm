package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type NumericRegistrationStorage = private.NumericRegistrationStorage

func ReadNumericRegistrationStorage(ctx context.Context, selected any, runID string) ([]NumericRegistrationStorage, error) {
	return private.ReadNumericRegistrationStorageForTest(ctx, selected, runID)
}
