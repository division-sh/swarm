package contracts

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestRuleActivityPinnedNamesPreserveEveryInventoriedBinding(t *testing.T) {
	for _, tc := range []struct {
		name, flow, node, event, label, tool, id string
	}{
		{"topology", ".", "activity-node", "request", "fallback", "send", "activity_node_request_fallback_send"},
		{"diagnostic", ".", "worker", "proof.requested", "1", "send", "worker_proof_requested_1_send"},
		{"admission", ".", "worker", "task.ready", "", "search", "worker_task_ready_rule_0_search"},
		{"approval", "review", "target-node", "task.started", "", "review", "target_node_task_started_rule_0_review"},
		{"provider", ".", "provider-sender", "provider.rule_requested", "send", "provider.send", "provider_rule_send"},
		{"root generator", ".", "activity-node", "request", "send_rule", "send", "send"},
		{"child generator", "child", "activity-node", "request", "send_rule", "send", "send"},
		{"selected loop", "worker-flow", "activity-node", "worker.execute", "selected", "send", "terminal_probe"},
		{"unselected loop", "worker-flow", "activity-node", "worker.execute", "unselected", "send", "unselected_probe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := identitytest.ExecutableNode(t, tc.flow, tc.node)
			base := tc.id
			if tc.flow != "." {
				base = tc.flow + "/" + base
			}
			want := ActivityResultEvents{
				ActivityID: tc.id, SuccessEvent: base + ".succeeded", FailureEvent: base + ".failed",
				RevisionRequested: base + ".revision_requested", Rejected: base + ".rejected",
			}
			for _, index := range []int{0, 7} {
				rows := make([]HandlerRuleEntry, index+1)
				rows[index].Activity = ActivitySpec{ID: tc.id, Tool: tc.tool}
				if index == 0 {
					rows[index].ID = tc.label
				}
				sites := ActivitySitesForNode(node, map[string]SystemNodeEventHandler{tc.event: {Rules: rows}})
				if len(sites) != 1 {
					t.Fatalf("sites = %#v", sites)
				}
				got := ActivityResultEventsForSite(sites[0])
				if got != want {
					t.Fatalf("position %d: events = %#v, want %#v", index, got, want)
				}
			}
		})
	}
}
