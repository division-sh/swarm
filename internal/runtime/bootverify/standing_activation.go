package bootverify

import (
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func standingActivatedFlow(source semanticview.Source, flowID string) bool {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return false
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return false
	}
	for _, scope := range source.ProjectScopes() {
		for _, ref := range scope.Manifest.Flows {
			if strings.TrimSpace(ref.ID) == flowID &&
				ref.HasStandingActivation() {
				_, err := bundle.ResolveFlowSingleton(flowID)
				return err == nil
			}
		}
	}
	return false
}
