package bootverify

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func standingActivatedFlow(source semanticview.Source, flowID string) bool {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return false
	}
	for _, scope := range source.ProjectScopes() {
		for _, ref := range scope.Manifest.Flows {
			if strings.TrimSpace(ref.ID) == flowID &&
				strings.TrimSpace(ref.Mode) == runtimecontracts.FlowModeSingleton &&
				ref.HasStandingActivation() {
				return true
			}
		}
	}
	return false
}
