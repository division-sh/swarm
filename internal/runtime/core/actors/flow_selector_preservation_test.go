package actors

import "testing"

func TestS1RuntimeDescriptorPreservesRawFlowSelector(t *testing.T) {
	for _, flow := range []string{".", " . ", "\t.\n", "sibling", " sibling ", "/sibling/", "child/../sibling", "team-bot", "team_bot", "SIBLING", "./sibling", "child//sibling", "sibling\\", "", " "} {
		cfg := AgentConfig{ID: " listener ", Role: " worker ", FlowID: flow, FlowPath: " /sibling/one/ ", Model: " regular "}
		for attempt := 0; attempt < 3; attempt++ {
			cfg.NormalizeRuntimeDescriptor()
			if cfg.FlowID != flow {
				t.Errorf("attempt %d repaired raw flow %q into %q", attempt, flow, cfg.FlowID)
			}
			if cfg.ID != "listener" || cfg.Role != "worker" || cfg.Model != "regular" || cfg.FlowPath != "sibling/one" {
				t.Fatalf("unrelated descriptor normalization changed: %+v", cfg)
			}
		}
	}
}
