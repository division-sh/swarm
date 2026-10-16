package agentframe

import (
	"encoding/json"
	"testing"
)

func Test2293Q2ExecutionAndInspectionOmitRetiredCriteria(t *testing.T) {
	seed, event, surface := testExecutionFrameInputs(t)
	frame := completeTestFrame(t, seed, TurnDraft{Kind: TurnInitial, Event: event}, surface)
	assertNoCriteria := func(t *testing.T, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var object map[string]any
		if err := json.Unmarshal(raw, &object); err != nil {
			t.Fatal(err)
		}
		if _, present := object["criteria"]; present {
			t.Fatal("retired criteria authority remains in the public session contract")
		}
		if _, present := object["intent"]; !present {
			t.Fatal("intent identity disappeared with criteria")
		}
		if _, present := object["provider_prompt"]; !present {
			t.Fatal("provider rendering disappeared with criteria")
		}
	}
	t.Run("execution", func(t *testing.T) { assertNoCriteria(t, frame.Session) })
	text, err := seed.ProviderPrompt.Text()
	if err != nil {
		t.Fatal(err)
	}
	preview := PreviewSeed{
		BundleHash: testBundleHash, AgentID: seed.AgentIdentity.AgentID(), Intent: seed.Intent,
		ProviderPrompt: text, AgentIdentity: &seed.AgentIdentity,
		Provider: &Provider{RuntimeMode: seed.RuntimeMode, Provider: seed.Provider, Transport: seed.Transport, ModelAlias: seed.ModelAlias, Model: seed.Model},
	}
	t.Run("static", func(t *testing.T) {
		inspection, err := InspectStatic(InspectionSelector{BundleHash: testBundleHash, Flow: "root", AgentID: preview.AgentID}, preview)
		if err != nil {
			t.Fatal(err)
		}
		assertNoCriteria(t, inspection.Session)
	})
	t.Run("effective", func(t *testing.T) {
		inspection, err := InspectEffective(InspectionSelector{Root: true, AgentID: preview.AgentID}, preview)
		if err != nil {
			t.Fatal(err)
		}
		assertNoCriteria(t, inspection.Session)
	})
}
