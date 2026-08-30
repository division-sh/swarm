package semanticview

import (
	"fmt"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// ResolveExecutableNodeDeclaration admits a local node coordinate only when
// exactly one declaration owns it in the selected semantic source.
func ResolveExecutableNodeDeclaration(source Source, flowID, nodeID string) (runtimeidentity.ExecutableNode, error) {
	if source == nil {
		return runtimeidentity.ExecutableNode{}, fmt.Errorf("executable node lookup requires semantic source")
	}
	flowID = strings.TrimSpace(flowID)
	nodeID = strings.TrimSpace(nodeID)
	var found runtimeidentity.ExecutableNode
	count := 0
	for _, record := range source.ExecutableNodeRecords() {
		candidate, err := record.Identity()
		if err != nil || candidate.FlowPath() != flowID || candidate.NodeID() != nodeID {
			continue
		}
		found = candidate
		count++
	}
	if count != 1 {
		return runtimeidentity.ExecutableNode{}, fmt.Errorf("executable node %q in flow %q requires exactly one declaration, found %d", nodeID, flowID, count)
	}
	return found, nil
}
