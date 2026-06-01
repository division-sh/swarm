package pipeline

import (
	"path/filepath"
	"sync"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type genericTestModule struct {
	once           sync.Once
	contractBundle *runtimecontracts.WorkflowContractBundle
	workflow       *WorkflowDefinition
	workflowNodes  []WorkflowNode
	guardRegistry  GuardRegistry
	actionRegistry ActionRegistry
	loadErr        error
}

func NewGenericTestWorkflowModule() WorkflowModule {
	return &genericTestModule{}
}

func (m *genericTestModule) init() {
	m.once.Do(func() {
		repoRoot := WorkflowRepoRoot()
		contractsDir := filepath.Join(repoRoot, "internal", "runtime", "testdata", "generic-swarm-bundle")
		platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
		m.contractBundle, m.loadErr = runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsDir, platformSpec)
		if m.loadErr != nil {
			return
		}
		source := semanticview.Wrap(m.contractBundle)
		m.workflow, m.loadErr = LoadWorkflowDefinition(source)
		if m.loadErr != nil {
			return
		}
		m.workflowNodes, m.loadErr = LoadWorkflowNodes(source)
		if m.loadErr != nil {
			return
		}
		m.guardRegistry = NewContractGuardRegistry(source)
		m.actionRegistry = NewContractActionRegistry(source)
	})
	if m.loadErr != nil {
		panic(m.loadErr)
	}
}

func (m *genericTestModule) SemanticSource() semanticview.Source {
	m.init()
	return semanticview.Wrap(m.contractBundle)
}

func (m *genericTestModule) WorkflowDefinition() *WorkflowDefinition {
	m.init()
	return m.workflow
}

func (m *genericTestModule) WorkflowNodes() []WorkflowNode {
	m.init()
	out := make([]WorkflowNode, 0, len(m.workflowNodes))
	for _, node := range m.workflowNodes {
		out = append(out, node)
	}
	return out
}

func (m *genericTestModule) GuardRegistry() GuardRegistry {
	m.init()
	return m.guardRegistry
}

func (m *genericTestModule) ActionRegistry() ActionRegistry {
	m.init()
	return m.actionRegistry
}
