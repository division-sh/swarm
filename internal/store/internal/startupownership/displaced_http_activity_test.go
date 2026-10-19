package startupownership

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestDisplacedHTTPActivityCutRequiresOriginalActiveMutation(t *testing.T) {
	expected := processbinding.LeaseStamp{LeaseIdentity: processbinding.LeaseIdentity{
		LeaseID: uuid.NewString(), OwnerID: "retained-successor", BootID: uuid.NewString(),
		RuntimeInstanceID: uuid.NewString(), Backend: processbinding.SQLiteLeaseBackend,
	}, Generation: 2}
	binding := processbinding.ActivityExecutionBinding{
		Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3,
	}
	for _, scenario := range []string{"zero_cut", "foreign_attempt", "inactive_attempt", "missing_attempt", "invalid_binding", "invalid_successor"} {
		t.Run(scenario, func(t *testing.T) {
			attempt := &mutationprotocol.Attempt{}
			cut := DisplacedHTTPActivityCut{attempt: attempt, successor: expected, sqlite: true}
			input := binding
			switch scenario {
			case "zero_cut":
				cut = DisplacedHTTPActivityCut{}
			case "foreign_attempt":
				attempt = &mutationprotocol.Attempt{}
			case "missing_attempt":
				attempt = nil
			case "invalid_binding":
				input.StateVersion = 0
			case "invalid_successor":
				cut.successor = processbinding.LeaseStamp{}
			}
			if _, displaced, err := cut.ProveDisplacedGrantTx(context.Background(), attempt, input); err == nil || displaced {
				t.Fatal("unissued or inactive recovery cut admitted historical activity")
			}
		})
	}
	if _, err := newDisplacedHTTPActivityCut(context.Background(), nil, expected, true); err == nil {
		t.Fatal("recovery cut issued without a native mutation")
	}
}
