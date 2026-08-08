package serveapp

import (
	"fmt"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func admittedRuntimeModuleAndSource(rt *runtimepkg.Runtime) (runtimepipeline.WorkflowModule, semanticview.Source, error) {
	if rt == nil {
		return nil, nil, fmt.Errorf("admitted runtime is required")
	}
	module := rt.Options.WorkflowModule
	if module == nil {
		return nil, nil, fmt.Errorf("admitted runtime workflow module is required")
	}
	source := module.SemanticSource()
	if source == nil {
		return nil, nil, fmt.Errorf("admitted runtime semantic source is required")
	}
	return module, source, nil
}
