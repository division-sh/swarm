package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/flowownedprojectagent"
)

func TestRequiredAgentVerificationRejectsAmbiguousFlowOwnedProjectDeclarations(t *testing.T) {
	source := flowownedprojectagent.LoadExplicitRequiredSource(t, runtimecontracts.FlowModeStatic, true)
	report := Run(context.Background(), source, Options{})
	for _, finding := range report.Findings {
		if finding.CheckID == "required_agents_match" && strings.Contains(finding.Message, "ambiguous across declarations") && strings.Contains(finding.Message, "left/agents.yaml") && strings.Contains(finding.Message, "right/agents.yaml") {
			return
		}
	}
	t.Fatalf("required-agent findings = %#v, want exact ambiguous declaration failure", report.Findings)
}
