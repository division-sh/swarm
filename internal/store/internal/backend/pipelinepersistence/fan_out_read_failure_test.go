package pipelinepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestFanOutReadDispositionRequiresNativeEvidence(t *testing.T) {
	independent := errors.New("independent read failure")
	uncertain := failures.New(failures.ClassOutcomeUncertain, "read_disposal_uncertain", "test", "read", nil)
	integrity := durabledata.NewDomainError(durabledata.CodeIntegrity, "exact pinned payload contradicts its version")
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
		{"uncertain_sibling", errors.Join(failures.New(failures.ClassDependencyUnavailable, "read_unavailable", "test", "read", nil), uncertain), true, false, pipeline.FanOutReadUncertain},
		{"defect_with_uncertain_disposal", errors.Join(fanOutInvalidRead(independent), uncertain), true, false, pipeline.FanOutReadUncertain},
		{"known_data_defect", fanOutInvalidRead(independent), true, false, pipeline.FanOutReadDefect},
		{"pinned_integrity", fanOutPinnedSourceError(integrity), false, false, pipeline.FanOutReadDefect},
		{"pinned_integrity_wrapped", fanOutPinnedSourceError(fmt.Errorf("pinned read: %w", integrity)), false, false, pipeline.FanOutReadDefect},
		{"pinned_integrity_cleanup", fanOutPinnedSourceError(errors.Join(integrity, independent)), false, false, pipeline.FanOutReadUncertain},
		{"pinned_integrity_uncertain", fanOutPinnedSourceError(errors.Join(integrity, uncertain)), true, false, pipeline.FanOutReadUncertain},
		{"pinned_read_failure", fanOutPinnedSourceError(driver.ErrBadConn), false, false, pipeline.FanOutReadUncertain},
		{"pinned_access_refusal", fanOutPinnedSourceError(durabledata.NewDomainError(durabledata.CodeAccessDenied, "no exact source")), false, false, pipeline.FanOutReadUncertain},
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
