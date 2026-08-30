package runtimepersistence

import runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"

func mustPersistenceRootNode(nodeID string) runtimeidentity.ExecutableNode {
	return mustPersistenceNode(".", nodeID)
}

func mustPersistenceNode(flowID, nodeID string) runtimeidentity.ExecutableNode {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(flowID, nodeID)
	if err != nil {
		panic(err)
	}
	return node
}
