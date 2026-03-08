package manager_test

import (
	runtimepipeline "empireai/internal/runtime/pipeline"
	empirepipeline "empireai/internal/runtime/pipeline/empire"
)

func init() {
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule {
		return empirepipeline.NewModule()
	})
}
