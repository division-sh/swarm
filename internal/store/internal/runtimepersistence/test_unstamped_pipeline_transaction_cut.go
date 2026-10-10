package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/correlation"
)

// Hold an original-coordinator writer during the unstamped routing refusal.
// Neither its transaction nor its context crosses this named temporal cut.
func HoldUnstampedPipelineAdmissionTransactionForTest(ctx context.Context, selected any, run string) (func() error, error) {
	if ctx == nil || correlation.RunIDFromContext(ctx) != run {
		return nil, fmt.Errorf("pipeline transaction cut requires its exact run context")
	}
	if err := validateWorkflowProjectionFaultRun(run); err != nil {
		return nil, err
	}
	if _, err := workflowProjectionNativePostgres(selected); err != nil {
		return nil, err
	}
	ready, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	aborted := errors.New("unstamped pipeline transaction cut released")
	var once sync.Once
	var result error
	closeCut := func() error {
		once.Do(func() { close(release) })
		<-done
		return result
	}
	go func() {
		err := runWorkflowProjectionFault(ctx, selected, func(ctx context.Context, _ *sql.Tx) error {
			close(ready)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return aborted
		})
		if err != aborted {
			result = err
		}
		close(done)
	}()
	select {
	case <-ready:
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, closeCut())
		}
		return closeCut, nil
	case <-done:
		return nil, result
	case <-ctx.Done():
		return nil, errors.Join(ctx.Err(), closeCut())
	}
}
