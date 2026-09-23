package runtime

import (
	"context"
	"errors"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type rejectedDeliveryActivationStore struct {
	runtimedelivery.Store
	err error
}

func (s rejectedDeliveryActivationStore) ActivateDeliveryAuthorityOutcome(context.Context, runtimedelivery.ExecutionAuthority) (runtimedelivery.ActivationCommit, error) {
	return runtimedelivery.ActivationCommit{}, s.err
}

func TestManagedExecutionRejectsUnacknowledgedDeliveryAuthorityActivation(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "write_failure", err: errors.New("delivery activation write failed")},
		{name: "missing_acknowledgement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := &Runtime{
				Options:       RuntimeOptions{SourceArtifactFact: sourceartifactfixture.Fact()},
				Bus:           &runtimebus.EventBus{},
				deliveryStore: rejectedDeliveryActivationStore{err: tc.err},
			}
			_, err := rt.admitManagedExecution(context.Background(), runtimestartupownership.GrantEvidence{GrantID: uuid.NewString(), RuntimeGeneration: 1})
			if err == nil || tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("unacknowledged activation error = %v, want %v", err, tc.err)
			}
			if rt.deliveryContinuations != nil || rt.startupAdmission.ID != "" {
				t.Fatalf("unacknowledged activation installed continuation or admission: continuation=%v admission=%+v", rt.deliveryContinuations, rt.startupAdmission)
			}
		})
	}
}
