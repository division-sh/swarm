package dashboard

import (
	"os"
	"testing"

	"empireai/internal/commgraph"
	runtimepipeline "empireai/internal/runtime/pipeline"
)

func TestMain(m *testing.M) {
	commgraph.SetDefaultPolicyFactory(func() commgraph.Policy {
		return commgraph.NewGenericTestPolicy()
	})
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule {
		return runtimepipeline.NewGenericTestWorkflowModule()
	})
	os.Exit(m.Run())
}
