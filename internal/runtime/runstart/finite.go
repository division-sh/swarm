package runstart

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type FiniteStartError struct {
	FlowID string
}

func (e *FiniteStartError) Error() string {
	return fmt.Sprintf("flow %s: this flow never completes; use `serve`, or mark its end stages `final`", e.FlowID)
}

// ValidateFinite checks a necessary completion condition, not eventual
// termination. Constructor children and possible compiled connections count;
// CEL results, filesystem membership and retaining configuration do not.
func ValidateFinite(source semanticview.Source, selectedFeeds []pinrouting.SourceEvent) error {
	if source == nil {
		return fmt.Errorf("finite start requires the selected semantic source")
	}
	connections := pinrouting.CompileConnectGraph(source)
	if issues := connections.Issues(); len(issues) != 0 {
		return fmt.Errorf("finite start requires admitted compiled connections: %v", issues[0])
	}
	root := semanticview.RootExecutionFlowID(source)
	pending := []string{root}
	// A feed publishes a declared output without constructing its producer.
	// Its exact compiled receivers are independent activation roots.
	for _, feed := range selectedFeeds {
		for _, plan := range connections.MatchingSourceEvent(feed) {
			pending = append(pending, plan.ReceiverEndpoint().FlowID())
		}
	}
	seen := map[string]bool{}
	for len(pending) != 0 {
		flowID := pending[0]
		pending = pending[1:]
		if seen[flowID] {
			continue
		}
		seen[flowID] = true
		graph, ok := semanticview.WorkflowStageTopology(source, flowID)
		if !ok || !graph.ValidStageCatalog() || graph.FlowID != flowID {
			return fmt.Errorf("finite start flow %s has no exact compiled stage catalog", flowID)
		}
		if len(graph.FinalStageIDs()) == 0 {
			return &FiniteStartError{FlowID: flowID}
		}
		children, err := flowidentity.KeylessChildFlowIDs(source, flowID)
		if err != nil {
			return err
		}
		pending = append(pending, children...)
		for _, plan := range connections.Plans() {
			if plan.SourceEndpoint().FlowID() == flowID {
				pending = append(pending, plan.ReceiverEndpoint().FlowID())
			}
		}
	}
	return nil
}
