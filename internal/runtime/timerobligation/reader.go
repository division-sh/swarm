package timerobligation

import (
	"context"
	"time"
)

// Reader owns the canonical runtime-execution projection. SQL, dialect, and
// row decoding stay behind the selected-store adapter.
type Reader interface {
	ReadTimerObligations(context.Context, Scope, time.Time) (Snapshot, error)
}
