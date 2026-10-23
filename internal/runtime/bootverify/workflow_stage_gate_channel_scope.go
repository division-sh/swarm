package bootverify

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const stageGateChannelScopeCheckID = "stage_gate_channel_scope"

func checkStageGateChannelScope(c *checkerContext) []Finding {
	var findings []Finding
	for _, gate := range c.source.WorkflowGates() {
		providers, err := semanticview.ProviderDirectoriesInScope(c.source, gate.FlowID)
		if err != nil {
			findings = append(findings, NewHardInvalidityFinding(stageGateChannelScopeCheckID,
				stageGateLocation(gate.FlowID, gate.Stage, gate.Decision),
				fmt.Sprintf("cannot establish human gate channel scope: %v", err),
				"Verify the exact admitted source directory before using this human gate."))
			continue
		}
		visible := false
		for _, provider := range providers {
			if _, present := provider.Body(packmodel.BodyChannel); present {
				visible = true
				break
			}
		}
		if !visible {
			findings = append(findings, NewHardInvalidityFinding(stageGateChannelScopeCheckID,
				stageGateLocation(gate.FlowID, gate.Stage, gate.Decision),
				"human gate requires a channel.yaml provider directory in this flow or an enclosing flow; sibling and descendant providers are not visible, and nearer same-key directories replace the whole provider",
				"Run `swarm import telegram` from this flow or an enclosing flow to copy a provider with channel.yaml. Structural visibility does not claim a connected or ready channel."))
		}
	}
	return findings
}
