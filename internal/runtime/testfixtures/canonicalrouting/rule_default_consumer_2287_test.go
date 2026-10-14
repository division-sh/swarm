package canonicalrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestRule2287CanonicalConsumersKeepSelectionAndExplicitActivityIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		copy func(testing.TB) string
	}{
		{"publication/root", func(t testing.TB) string { return CopyPublicationStateResult(t, "root") }},
		{"publication/static", func(t testing.TB) string { return CopyPublicationStateResult(t, "static") }},
		{"selection_retry", CopySelectionRetry},
		{"artifact/template", func(t testing.TB) string { return CopyArtifactActionResultDelivery(t, "template", false) }},
		{"artifact/static", func(t testing.TB) string { return CopyArtifactActionResultDelivery(t, "static", false) }},
		{"served_followup", CopyRootIngressServedFollowUp},
		{"selection_trace", CopyHandlerRuleSelectionProof},
		{"gate_completion", CopyGateCompletionDiagnostic},
		{"carriers/original", func(t testing.TB) string { return CopyLifecycleSelectedCarriers(t, false) }},
		{"carriers/reordered", func(t testing.TB) string { return CopyLifecycleSelectedCarriers(t, true) }},
		{"selected_activity", func(t testing.TB) string { return CopySelectedForkReadiness(t, 1, "activity_loop_rule") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, tc.copy(t), contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			switch tc.name {
			case "selection_trace":
				node := bundle.FlowTree.ByPath["."].Nodes["selection-node"]
				for _, event := range []string{"rules.selected", "rules.fallback", "rules.evaluation_failed"} {
					rows := node.EventHandlers[event].Rules
					index := len(rows) - 1
					if index < 0 || rows[index].Condition != "true" || rows[index].PolicyRow.Kind != contracts.PolicySheetRowKindWhen {
						t.Fatalf("effectless selection control lost its explicit predicate: %s: %#v", event, rows)
					}
				}
			case "artifact/template", "artifact/static":
				rows := bundle.FlowTree.ByPath["repo-scaffold"].Nodes["repo-scaffold-node"].EventHandlers["repo_scaffold.repo_commit_requested"].Rules
				if len(rows) != 2 || rows[1].Condition != "" || rows[1].PolicyRow.Kind != contracts.PolicySheetRowKindDefault || rows[1].Emit.EventType() != "repo_scaffold.repo_commit_failed" {
					t.Fatalf("artifact result lost its complete structural default: %#v", rows)
				}
			case "carriers/original", "carriers/reordered":
				for _, flow := range bundle.FlowTree.ByPath {
					for _, event := range []string{"work.first", "work.second"} {
						rows := flow.Nodes["controller"].EventHandlers[event].Rules
						if len(rows) != 2 || rows[0].PolicyRow.Kind != contracts.PolicySheetRowKindWhen || rows[1].PolicyRow.Kind != contracts.PolicySheetRowKindWhen {
							t.Fatalf("%s/%s lost a real carrier or retained an empty default: %#v", flow.Path, event, rows)
						}
					}
				}
			case "selected_activity":
				rows := bundle.FlowTree.ByPath["worker-flow"].Nodes["activity-node"].EventHandlers["worker.execute"].Rules
				if len(rows) != 2 || rows[0].Condition != "true" || rows[0].Activity.ID != "terminal_probe" || rows[1].Condition != "" || rows[1].PolicyRow.Kind != contracts.PolicySheetRowKindDefault || rows[1].Activity.ID != "unselected_probe" {
					t.Fatalf("selected activity identity or unselected companion changed: %#v", rows)
				}
			}
		})
	}
}
