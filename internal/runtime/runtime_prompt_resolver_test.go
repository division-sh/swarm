package runtime

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type wrappedSemanticSource struct {
	semanticview.Source
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
