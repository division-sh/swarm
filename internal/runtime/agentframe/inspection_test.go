package agentframe

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
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
		Criteria: session.Criteria, Provider: &Provider{RuntimeMode: session.RuntimeMode, Provider: session.Provider, Transport: session.Transport, ModelAlias: session.ModelAlias, Model: session.Model},
		ProviderPrompt: prompt,
	}
	_, err = InspectEffective(InspectionSelector{AgentID: "different-root-agent", Root: true}, seed)
	if err == nil || !strings.Contains(err.Error(), "does not match concrete agent identity") {
		t.Fatalf("root effective selector mismatch error = %v", err)
	}
}

func TestAgentFrameInspectionRejectsNoncanonicalPathAliases(t *testing.T) {
	session, _, _ := testExecutionFrameInputs(t)
	prompt, err := session.ProviderPrompt.Text()
	if err != nil {
		t.Fatal(err)
	}
	seed := PreviewSeed{
		BundleHash: testBundleHash, AgentID: session.AgentIdentity.AgentID(), Intent: session.Intent,
		Criteria: session.Criteria, ProviderPrompt: prompt,
	}
	for _, flow := range []string{"/review/", " review ", "review/"} {
		if _, err := InspectStatic(InspectionSelector{BundleHash: testBundleHash, Flow: flow, AgentID: seed.AgentID}, seed); err == nil {
			t.Fatalf("static inspection accepted noncanonical flow alias %q", flow)
		}
	}

	identity := agentidentitytest.Runtime(t, seed.AgentID, "agent-frame-inspection-test", "review", "one", "review/one")
	seed.AgentIdentity = &identity
	seed.BundleSource = "persisted"
	for _, flowInstance := range []string{"/review/one/", " review/one ", "review/one/"} {
		if _, err := InspectEffective(InspectionSelector{AgentID: seed.AgentID, FlowInstance: flowInstance}, seed); err == nil {
			t.Fatalf("effective inspection accepted noncanonical flow-instance alias %q", flowInstance)
		}
	}
}

func TestAgentFrameInspectionRejectsNoncanonicalScalarAliases(t *testing.T) {
	session, _, _ := testExecutionFrameInputs(t)
	prompt, err := session.ProviderPrompt.Text()
	if err != nil {
		t.Fatal(err)
	}
	seed := PreviewSeed{
		BundleHash: testBundleHash, AgentID: session.AgentIdentity.AgentID(), Intent: session.Intent,
		Criteria: session.Criteria, ProviderPrompt: prompt,
	}
	for _, selector := range []InspectionSelector{
		{BundleHash: " " + testBundleHash, Flow: "review", AgentID: seed.AgentID},
		{BundleHash: testBundleHash, Flow: "review", AgentID: " " + seed.AgentID},
	} {
		if _, err := InspectStatic(selector, seed); err == nil {
			t.Fatalf("static inspection accepted noncanonical scalar selector %#v", selector)
		}
	}

	identity := agentidentitytest.RootRuntime(t, seed.AgentID, "agent-frame-inspection-test")
	seed.AgentIdentity = &identity
	seed.BundleSource = "persisted"
	if _, err := InspectEffective(InspectionSelector{AgentID: " " + seed.AgentID, Root: true}, seed); err == nil {
		t.Fatal("effective inspection accepted noncanonical agent_id")
	}
}
