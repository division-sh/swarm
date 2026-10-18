package runtimepersistence

import (
	"errors"

	private "github.com/division-sh/swarm/internal/store/internal/startupownership"
)

type OriginalProcessCaptureForTest = private.OriginalProcessCaptureForTest
type OriginalSessionInterruptionForTest = private.OriginalSessionInterruptionForTest

func CaptureOriginalProcessForTest(selected any) (*OriginalProcessCaptureForTest, error) {
	if store, ok := selected.(*PostgresStore); ok && store != nil {
		return store.startupPostgresOwner.CaptureOriginalProcessForTest()
	}
	return nil, errors.New("original process capture requires the PostgreSQL construction owner")
}
