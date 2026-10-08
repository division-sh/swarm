package eventpersistence

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func TestInboundConstructionReconciliationRequiresIsolatedPreCommitConflict(t *testing.T) {
	owner, err := flowidentity.NewRunScopedFlowInstance("80670e31-82b9-45fd-b7f3-56ed650ab17a", flowidentity.StoredRoute("child", "one", "child/one"))
	if err != nil {
		t.Fatal(err)
	}
	cause := failures.New(failures.ClassConflictingDuplicate, "flow_instance_already_exists", "flow-instance-activation", "commit", nil)
	conflict := &pipeline.FlowInstanceActivationConflict{Owner: owner, Cause: cause}
	for _, test := range []struct {
		name  string
		phase mutationprotocol.Phase
		err   error
		want  bool
	}{
		{"exact rolled-back construction", mutationprotocol.DomainWrite, conflict, true},
		{"wrapped isolated construction", mutationprotocol.DomainWrite, errors.Join(fmt.Errorf("publication: %w", conflict)), true},
		{"ordinary duplicate", mutationprotocol.DomainWrite, cause, false},
		{"same text", mutationprotocol.DomainWrite, errors.New(conflict.Error()), false},
		{"independent cleanup", mutationprotocol.DomainWrite, errors.Join(conflict, errors.New("cleanup failed")), false},
		{"cancellation", mutationprotocol.DomainWrite, errors.Join(conflict, context.Canceled), false},
		{"before write", mutationprotocol.BeforeAttempt, conflict, false},
		{"commit uncertain", mutationprotocol.CommitAdmission, conflict, false},
		{"acknowledged", mutationprotocol.PostCommit, conflict, false},
		{"invalid coordinate", mutationprotocol.DomainWrite, &pipeline.FlowInstanceActivationConflict{Cause: cause}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := rolledBackInboundConstruction(test.phase, test.err)
			if (result.RolledBackConstruction != nil) != test.want {
				t.Fatalf("reconciliation witness=%+v want=%t", result, test.want)
			}
			if result.Acknowledged || result.RolledBackConstruction != nil && *result.RolledBackConstruction != owner {
				t.Fatal("rollback witness fabricated committed or foreign authority")
			}
			if result.CanReconcileConstruction(test.err) != test.want {
				t.Fatal("runtime consumption disagrees with the transaction witness")
			}
			if result.CanReconcileConstruction(errors.Join(test.err, errors.New("independent boundary failure"))) {
				t.Fatal("boundary cleanup failure was suppressed")
			}
		})
	}
}
