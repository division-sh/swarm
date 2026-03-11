package testcases

import (
	"testing"
)

func TestGenericBundle_AgentLifecyclePatterns(t *testing.T) {
	bundle := loadGenericMASBundle(t)
	workerEntry, ok := bundle.Agents["worker-a"]
	if !ok {
		t.Fatal("expected worker-a runtime agent entry")
	}
	if workerEntry.ManagerFallback != "coordinator" {
		t.Fatalf("expected coordinator manager fallback, got %+v", workerEntry)
	}

	reconfigured := agentConfigFromEntry("worker-a", workerEntry)
	reconfigured.Role = bundle.Agents["coordinator"].Role
	reconfigured.Subscriptions = []string{"item.review_requested"}
	if reconfigured.Role != "coordinator" || !hasAll(reconfigured.Subscriptions, "item.review_requested") {
		t.Fatalf("unexpected reconfigured agent shape: %+v", reconfigured)
	}

	delivery := bundle.FlowSchemas["delivery"]
	if delivery.Mode != "template" || delivery.AutoEmitOnCreate.Event != "item.completed" {
		t.Fatalf("unexpected delivery flow lifecycle semantics: %+v", delivery)
	}
	if len(bundle.FlowRequiredAgents("delivery")) == 0 {
		t.Fatal("expected delivery flow required agents")
	}
}
