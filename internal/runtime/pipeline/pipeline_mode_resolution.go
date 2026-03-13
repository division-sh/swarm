package pipeline

import (
	"empireai/internal/runtime/semanticview"
)

const scanModePolicyFlowID = "discovery"

func scanModePolicyValue(source semanticview.Source, key string) (any, bool) {
	if pv, ok := semanticview.PolicyValueForFlow(source, scanModePolicyFlowID, key); ok {
		return pv.Value, true
	}
	if pv, ok := semanticview.PolicyValueForFlow(source, "", key); ok {
		return pv.Value, true
	}
	return nil, false
}
