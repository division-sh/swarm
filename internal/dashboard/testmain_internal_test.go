package dashboard

import (
	"os"
	"testing"

	"empireai/internal/commgraph"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
)

func TestMain(m *testing.M) {
	commgraph.SetDefaultPolicyFactory(func() commgraph.Policy {
		return commgraph.NewGenericTestPolicy()
	})
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule {
		return runtimepipeline.NewGenericTestWorkflowModule()
	})
	runtimeproductpolicy.SetDefaultFactory(func() runtimeproductpolicy.Policy {
		return runtimeproductpolicy.NewGenericTestPolicy()
	})
	os.Exit(m.Run())
}
