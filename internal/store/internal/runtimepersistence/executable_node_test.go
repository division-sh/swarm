package runtimepersistence

import runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"

func mustPersistenceRootNode(nodeID string) runtimeidentity.ExecutableNode {
	return mustPersistenceNode("", nodeID)
}

func mustPersistenceNode(flowID, nodeID string) runtimeidentity.ExecutableNode {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(runtimeidentity.RootPackageKey, flowID, nodeID)
	if err != nil {
		panic(err)
	}
	return node
}

func mustPersistencePackageNode(packageKey, flowID, nodeID string) runtimeidentity.ExecutableNode {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(packageKey, flowID, nodeID)
	if err != nil {
		panic(err)
	}
	return node
}
