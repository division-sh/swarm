package conformance

import (
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func compiledConnectPlans(source semanticview.Source) ([]runtimepinrouting.ConnectRoutePlan, []runtimepinrouting.ConnectRoutePlanIssue) {
	graph := runtimepinrouting.CompileConnectGraph(source)
	return graph.Plans(), graph.Issues()
}
