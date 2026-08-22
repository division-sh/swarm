package cliapp

import (
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type swarmWorkflowModule struct {
	bundle         *runtimecontracts.WorkflowContractBundle
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	nodes          []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
}

func NewSwarmWorkflowModule(RepoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
	return NewSwarmWorkflowModuleWithPackBase(RepoRoot, contractsRoot, platformSpecPath, nil)
}

func NewSwarmWorkflowModuleWithPackBase(RepoRoot, contractsRoot, platformSpecPath string, base *packartifact.PlatformPackInventory) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(RepoRoot, contractsRoot, platformSpecPath, runtimecontracts.WorkflowContractLoadOptions{
		PlatformPackBase: base, AdmitPackInventory: packadmission.AdmitInventory,
	})
	if err != nil {
		return nil, nil, err
	}
	module, _, err := NewSwarmWorkflowModuleForBundle(bundle)
	if err != nil {
		return nil, nil, err
	}
	return module, bundle, nil
}

func NewSwarmWorkflowModuleWithRuntimeConfig(repoRoot, contractsRoot, platformSpecPath string, cfgResult RuntimeConfigLoadResult) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
	base, err := LoadConfiguredPlatformPackBase(repoRoot, cfgResult)
	if err != nil {
		return nil, nil, err
	}
	return NewSwarmWorkflowModuleWithPackBase(repoRoot, contractsRoot, platformSpecPath, base)
}

func loadConfiguredCLIWorkflowModule(repoRoot string, opts CLIContractPlatformSpecPathOptions) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, CLIContractPlatformSpecPaths, error) {
	cfgResult, err := loadPackInventoryConfig(repoRoot, opts.ConfigPath)
	if err != nil {
		return nil, nil, CLIContractPlatformSpecPaths{}, err
	}
	paths, err := resolveCLIContractPlatformSpecPathsFromConfig(repoRoot, opts, cfgResult.cli)
	if err != nil {
		return nil, nil, CLIContractPlatformSpecPaths{}, err
	}
	contractsRoot, err := NormalizeContractsRoot(paths.ContractsPath)
	if err != nil {
		return nil, nil, CLIContractPlatformSpecPaths{}, err
	}
	base, err := LoadConfiguredPlatformPackBase(repoRoot, cfgResult)
	if err != nil {
		return nil, nil, CLIContractPlatformSpecPaths{}, err
	}
	module, bundle, err := NewSwarmWorkflowModuleWithPackBase(repoRoot, contractsRoot, paths.PlatformSpecPath, base)
	if err != nil {
		return nil, nil, CLIContractPlatformSpecPaths{}, err
	}
	paths.ContractsPath = contractsRoot
	return module, bundle, paths, nil
}

func NewSwarmWorkflowModuleForBundle(bundle *runtimecontracts.WorkflowContractBundle) (runtimepipeline.WorkflowModule, semanticview.Source, error) {
	source := semanticview.Wrap(bundle)
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		return nil, nil, err
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		return nil, nil, err
	}
	return &swarmWorkflowModule{
		bundle:         bundle,
		source:         source,
		workflow:       workflow,
		nodes:          nodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}, source, nil
}

func (m *swarmWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (m *swarmWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}
func (m *swarmWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}
func (m *swarmWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry { return m.guardRegistry }
func (m *swarmWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actionRegistry
}
