package runforkexecution

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type selectedControlBindingFixture struct {
	SelectedContractForkLifecycle
	binding runfork.RunForkSelectedContractBinding
	exists  bool
	err     error
	calls   int
}

func (s *selectedControlBindingFixture) LoadRunForkSelectedContractBinding(_ context.Context, runID string) (runfork.RunForkSelectedContractBinding, bool, error) {
	s.calls++
	return s.binding, s.exists, s.err
}

func TestSelectedForkControlAdmissionUsesExactBinding(t *testing.T) {
	runID := uuid.NewString()
	binding := testSelectedContractBinding(runID)
	binding.BindingID = uuid.NewString()
	store := &selectedControlBindingFixture{binding: binding, exists: true}
	owner := SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{fork: store}}
	for _, op := range []runfork.SelectedControl{
		runfork.ControlRunPause, runfork.ControlRunContinue, runfork.ControlAgentRestart,
		runfork.ControlAgentDirective, runfork.ControlAgentReplay, runfork.ControlEventReplay,
		runfork.ControlEventPublish, runfork.ControlMailboxDecide, runfork.ControlMailboxDefer,
		runfork.ControlMailboxBeginInput, runfork.ControlMailboxCancelInput,
	} {
		t.Run(string(op), func(t *testing.T) {
			var refusal *runfork.SelectedForkControlUnsupported
			if err := owner.RequireNormalControl(context.Background(), runID, op); !errors.As(err, &refusal) || refusal.RunID != runID || refusal.BindingID != binding.BindingID || refusal.Operation != op {
				t.Fatalf("lost selected control identity: %v", err)
			}
		})
	}
	store.exists = false
	if err := owner.RequireNormalControl(context.Background(), runID, runfork.ControlRunPause); err != nil {
		t.Fatalf("normal run was classified as selected: %v", err)
	}
	store.exists = true
	store.binding.ForkRunID = uuid.NewString()
	var refusal *runfork.SelectedForkControlUnsupported
	if err := owner.RequireNormalControl(context.Background(), runID, runfork.ControlRunPause); err == nil || errors.As(err, &refusal) {
		t.Fatalf("corrupt binding was accepted as valid refusal evidence: %v", err)
	}
	store.err = errors.New("read failed")
	if err := owner.RequireNormalControl(context.Background(), runID, runfork.ControlRunPause); !errors.Is(err, store.err) {
		t.Fatal("binding failure fell through to normal control")
	}
	before := store.calls
	if err := owner.RequireNormalControl(context.Background(), runID, "run.stop"); err == nil || store.calls != before {
		t.Fatal("terminal control was admitted by unsupported-control classifier")
	}
}
