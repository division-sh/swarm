package runtime

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

type digestTerminalStateSpy struct {
	states []string
}

func (s *digestTerminalStateSpy) SetTerminalInstanceStates(states []string) {
	s.states = append([]string(nil), states...)
}

func (*digestTerminalStateSpy) CountActiveInstances(context.Context) (int, error) {
	return 0, nil
}

func (*digestTerminalStateSpy) ListInstanceDigestRows(context.Context, int) ([]InstanceDigestRow, error) {
	return nil, nil
}

type wrappedSemanticSource struct {
	semanticview.Source
}

func TestBindRuntimeTerminalInstanceStates_BindsDigestStore(t *testing.T) {
	spy := &digestTerminalStateSpy{}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			TerminalStages: []string{"done"},
		},
	})

	bindRuntimeTerminalInstanceStates(Stores{DigestStore: spy}, source)

	if got, want := strings.Join(spy.states, ","), "done"; got != want {
		t.Fatalf("bound terminal states = %q, want %q", got, want)
	}
}

func TestNewRuntimePromptResolver_RejectsNonBundleSemanticSource(t *testing.T) {
	source := wrappedSemanticSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
	}

	_, err := newRuntimePromptResolver(source)
	if err == nil || !strings.Contains(err.Error(), "bundle-backed semantic source is required") {
		t.Fatalf("newRuntimePromptResolver err = %v, want bundle-backed source error", err)
	}
}
