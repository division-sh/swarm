package agentframe

import (
	"strings"
	"testing"
)

func TestAgentFrameEffectiveInspectionRequiresExactConcreteIdentity(t *testing.T) {
	session, _, _ := testExecutionFrameInputs(t)
	prompt, err := session.ProviderPrompt.Text()
	if err != nil {
		t.Fatal(err)
	}
	seed := PreviewSeed{
		BundleHash: testBundleHash, BundleSource: "persisted", AgentID: session.AgentIdentity.AgentID(), AuthoredFlow: "root",
		AgentIdentity: &session.AgentIdentity, Role: session.Role, FlowID: session.FlowID, Intent: session.Intent,
		Criteria: session.Criteria, Provider: &Provider{RuntimeMode: session.RuntimeMode, Provider: session.Provider, Transport: session.Transport, Model: session.Model},
		ProviderPrompt: prompt,
	}
	_, err = InspectEffective(InspectionSelector{AgentID: "different-root-agent", Root: true}, seed)
	if err == nil || !strings.Contains(err.Error(), "does not match concrete agent identity") {
		t.Fatalf("root effective selector mismatch error = %v", err)
	}
}
