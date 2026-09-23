package genericschedule

import (
	"context"
	"errors"
	"testing"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func TestMalformedScheduleAcknowledgedTerminalizationContinuesRestoreScan(t *testing.T) {
	cleanupErr := errors.New("cleanup failed after commit")
	malformed := &malformedActivationError{cause: errors.New("corrupt activation"), family: persistedScheduleFamilyNonJoin}
	for _, testCase := range []struct {
		name         string
		acknowledged bool
		wantScanned  int
		wantActive   int
	}{
		{name: "acknowledged cleanup failure", acknowledged: true, wantScanned: 2, wantActive: 1},
		{name: "unacknowledged failure", wantScanned: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			scanned := 0
			terminalized := 0
			active, err := collectActiveGenericScheduleActivations(
				context.Background(),
				[]string{"malformed", "healthy"},
				func(_ context.Context, id string) (runtimegenericschedule.Activation, bool, error) {
					scanned++
					if id == "malformed" {
						return runtimegenericschedule.Activation{}, false, malformed
					}
					return runtimegenericschedule.Activation{ID: id, Status: runtimegenericschedule.StatusActive}, true, nil
				},
				func(_ context.Context, id string, cause error) (bool, error) {
					if id != "malformed" || !errors.Is(cause, malformed) {
						t.Fatalf("terminalization = id:%q cause:%v", id, cause)
					}
					terminalized++
					return testCase.acknowledged, cleanupErr
				},
			)
			if scanned != testCase.wantScanned || terminalized != 1 || len(active) != testCase.wantActive {
				t.Fatalf("scan = scanned:%d terminalized:%d active:%+v err:%v", scanned, terminalized, active, err)
			}
			if testCase.acknowledged && err != nil {
				t.Fatalf("acknowledged terminalization aborted restore: %v", err)
			}
			if !testCase.acknowledged && !errors.Is(err, cleanupErr) {
				t.Fatalf("unacknowledged terminalization error = %v", err)
			}
		})
	}
}
