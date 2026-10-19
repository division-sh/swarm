package startupownership

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type activityRecoverySessionProbe struct {
	*retainedSessionProbe
	called  int
	failure error
}

func (s *activityRecoverySessionProbe) ReconcileDisplacedHTTPActivities(context.Context) (pipeline.ActivityRecoverySummary, error) {
	s.called++
	return pipeline.ActivityRecoverySummary{}, s.failure
}

func TestActivityRecoveryUsesOnlyOriginalLiveSession(t *testing.T) {
	if _, err := ReconcileDisplacedHTTPActivities(context.Background(), nil); err == nil {
		t.Fatal("missing process capability admitted activity recovery")
	}
	base, session, _ := testCapability(t)
	if _, err := ReconcileDisplacedHTTPActivities(context.Background(), base); err == nil {
		t.Fatal("missing native recovery role was silently accepted")
	}
	if err := base.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A new retained probe supplies the optional role; production implementations
	// remain the original native process sessions, not a caller-selected adapter.
	probe := &activityRecoverySessionProbe{retainedSessionProbe: &retainedSessionProbe{authority: session.authority, plan: session.plan}}
	capability, err := NewProcessCapability(probe)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	result, err := ReconcileDisplacedHTTPActivities(context.Background(), capability)
	if err != nil || probe.called != 1 || !reflect.DeepEqual(result, pipeline.ActivityRecoverySummary{}) {
		t.Fatalf("original session recovery was not joined: %v", err)
	}
	refusal := errors.New("original activity owner refused its immutable evidence")
	probe.failure = refusal
	if _, err := ReconcileDisplacedHTTPActivities(context.Background(), capability); !errors.Is(err, refusal) || probe.called != 2 {
		t.Fatalf("native semantic refusal was not retained: %v", err)
	}
	if err := capability.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := ReconcileDisplacedHTTPActivities(context.Background(), capability); err == nil || probe.called != 2 {
		t.Fatal("released process entered activity recovery")
	}
}
