package pipelinepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestFanOutReadDispositionRequiresNativeEvidence(t *testing.T) {
	independent := errors.New("independent read failure")
	uncertain := failures.New(failures.ClassOutcomeUncertain, "read_disposal_uncertain", "test", "read", nil)
	for _, tc := range []struct {
		name               string
		cause              error
		disposed, canceled bool
		want               pipeline.FanOutReadDisposition
	}{
		{"lost_connection_disposed", driver.ErrBadConn, true, false, pipeline.FanOutReadRetry},
		{"lost_connection_not_disposed", driver.ErrBadConn, false, false, pipeline.FanOutReadUncertain},
		{"deadline_of_read_only", context.DeadlineExceeded, true, false, pipeline.FanOutReadRetry},
		{"mixed_cancel_disposal", errors.Join(context.Canceled, driver.ErrBadConn, independent), true, true, pipeline.FanOutReadYield},
		{"independent_disposed", independent, true, false, pipeline.FanOutReadRetry},
		{"uncertain_disposed", uncertain, true, false, pipeline.FanOutReadUncertain},
		{"known_data_defect", fanOutInvalidRead(independent), true, false, pipeline.FanOutReadDefect},
		{"stale", fanoutobligation.ErrStaleClaim, true, false, pipeline.FanOutReadYield},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			err := fanOutReadFailure(ctx, "admit_read", tc.cause, tc.disposed)
			var evidence *pipeline.FanOutEvaluationLoadError
			if !errors.As(err, &evidence) || evidence.Disposition != tc.want || !errors.Is(err, tc.cause) {
				t.Fatalf("evidence=%+v cause=%v", evidence, err)
			}
			if tc.name == "mixed_cancel_disposal" && (!errors.Is(err, independent) || !errors.Is(err, driver.ErrBadConn)) {
				t.Fatal("cancellation hid independent disposal failure")
			}
		})
	}
}
