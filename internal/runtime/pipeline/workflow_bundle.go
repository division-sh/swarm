package pipeline

import (
	"sync"

	runtimecontracts "empireai/internal/runtime/contracts"
)

var (
	empireWorkflowBundleOnce sync.Once
	empireWorkflowBundle     *runtimecontracts.WorkflowContractBundle
	empireWorkflowBundleErr  error
)

func empireContractBundle() *runtimecontracts.WorkflowContractBundle {
	empireWorkflowBundleOnce.Do(func() {
		empireWorkflowBundle, empireWorkflowBundleErr = runtimecontracts.LoadEmpireWorkflowContractBundle(workflowRepoRoot())
	})
	if empireWorkflowBundleErr != nil {
		panic(empireWorkflowBundleErr)
	}
	return empireWorkflowBundle
}
