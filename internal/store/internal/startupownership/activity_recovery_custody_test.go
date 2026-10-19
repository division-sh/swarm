package startupownership

import (
	"context"
	"errors"
	"reflect"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// A caller-supplied recovery role must not fit either native binding.
type displacedHTTPActivityRecoveryStub struct {
	activityRecoveryRoleProbe
}

func (*displacedHTTPActivityRecoveryStub) ReconcileDisplacedHTTPActivitiesTx(context.Context, *mutationprotocol.Attempt, displacedHTTPActivityCut) (runtimepipeline.ActivityRecoverySummary, error) {
	panic("a substituted recovery role must never be called")
}

func TestDisplacedHTTPActivityRecoveryStubCannotEnableRecovery(t *testing.T) {
	for _, binding := range []reflect.Type{
		reflect.TypeOf((*StartupPostgresOwner).BindActivityJournalRole),
		reflect.TypeOf((*StartupSQLiteOwner).BindActivityJournalRole),
	} {
		if reflect.TypeOf(&displacedHTTPActivityRecoveryStub{}).AssignableTo(binding.In(1)) {
			t.Fatal("a substituted recovery role fits a native journal binding")
		}
	}
	for _, journal := range []reflect.Type{
		reflect.TypeOf(&activityjournal.ActivityPostgresOwner{}),
		reflect.TypeOf(&activityjournal.ActivitySQLiteOwner{}),
	} {
		if _, exposed := journal.MethodByName("ReconcileDisplacedHTTPActivitiesTx"); exposed {
			t.Fatal("journal exports its raw recovery mutation")
		}
	}
	if reflect.TypeOf(displacedHTTPActivityCut{}).NumMethod() != 0 {
		t.Fatal("recovery cut exports access to its native mutation")
	}
	for _, cell := range []string{"missing_attempt", "inactive_attempt", "cancelled", "missing_context"} {
		t.Run(cell, func(t *testing.T) {
			ctx := context.Background()
			attempt := &mutationprotocol.Attempt{}
			switch cell {
			case "missing_attempt":
				attempt = nil
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			case "missing_context":
				ctx = nil
			}
			result, err := reconcileDisplacedHTTPActivities(ctx, attempt, displacedHTTPActivityCut{}, activityjournal.DialectSQLite)
			if err == nil || len(result.Results) != 0 {
				t.Fatalf("non-enabling stub produced recovery candidates: result=%+v err=%v", result, err)
			}
			if cell == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("recovery refusal lost its cause: %v", err)
			}
		})
	}
}
