package contracts

import (
	"strings"
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
				got, err := ActivityResultEventsForSite(sites[0])
				if err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Fatalf("position %d: events = %#v, want %#v", index, got, want)
				}
			}
		})
	}
}

func TestRuleActivityAdmissionRequiresExplicitID(t *testing.T) {
	for _, tc := range []struct{ fragment, diagnostic string }{
		{"tool: send", "rule activity.id is required"},
		{"id: null, tool: send", "must be a scalar id, got null"},
		{"id: '', tool: send", "rule activity.id is required"},
		{"id: '  ', tool: send", "rule activity.id is required"},
		{"id: [], tool: send", "must be a scalar id, got empty_sequence"},
		{"id: {}, tool: send", "must be a scalar id, got empty_mapping"},
	} {
		var handler SystemNodeEventHandler
		err := decodeNodeTestYAML([]byte("rules: [{else: true, activity: {"+tc.fragment+"}}]"), &handler)
		if err == nil || !strings.Contains(err.Error(), tc.diagnostic) || !strings.Contains(err.Error(), `["rules"][0]["activity"]`) {
			t.Fatalf("rule activity %s: error = %v, want %s at the activity source", tc.fragment, err, tc.diagnostic)
		}
	}
	for _, source := range []string{
		"rules:\n  - else: true\n    activity: {id: explicit_send, tool: send}\n",
		"_note: &activity {id: explicit_send, tool: send}\nrules: [{else: true, activity: *activity}]\n",
		"_note: &activity {id: explicit_send, tool: send}\nrules: [{else: true, activity: {<<: *activity}}]\n",
	} {
		var handler SystemNodeEventHandler
		if err := decodeNodeTestYAML([]byte(source), &handler); err != nil {
			t.Fatal(err)
		}
		if handler.Rules[0].Activity.ID != "explicit_send" {
			t.Fatalf("activity identity = %#v", handler.Rules[0].Activity)
		}
	}
}

func TestActivitySiteContextCannotBeInferredFromLabelOrIndex(t *testing.T) {
	node := identitytest.RootNode(t, "scanner")
	for _, site := range []ActivitySite{
		{Node: node, HandlerEventKey: "source.requested", Spec: ActivitySpec{ID: "explicit", Tool: "read"}},
		{Node: node, HandlerEventKey: "source.requested", Kind: ActivitySiteHandler, RuleIndex: 0, Spec: ActivitySpec{ID: "explicit", Tool: "read"}},
		{Node: node, HandlerEventKey: "source.requested", Kind: ActivitySiteRule, RuleIndex: -1, Spec: ActivitySpec{ID: "explicit", Tool: "read"}},
		{Node: node, HandlerEventKey: "source.requested", Kind: ActivitySiteRule, RuleIndex: 0, Spec: ActivitySpec{Tool: "read"}},
	} {
		got, err := ActivityResultEventsForSite(site)
		if err == nil || got != (ActivityResultEvents{}) {
			t.Fatalf("invalid site manufactured result names: %#v, %#v, %v", site, got, err)
		}
	}
	site := ActivitySite{Node: node, HandlerEventKey: "source.requested", Kind: ActivitySiteHandler, RuleIndex: -1, Spec: ActivitySpec{Tool: "source_scrape"}}
	got, err := ActivityResultEventsForSite(site)
	want := ActivityResultEvents{
		ActivityID:   "scanner_source_requested_source_scrape",
		SuccessEvent: "scanner_source_requested_source_scrape.succeeded", FailureEvent: "scanner_source_requested_source_scrape.failed",
		RevisionRequested: "scanner_source_requested_source_scrape.revision_requested", Rejected: "scanner_source_requested_source_scrape.rejected",
	}
	if err != nil || got != want {
		t.Fatalf("handler default changed: %#v, %v", got, err)
	}
}

func TestUnnamedProgrammaticRuleActivityCannotCompileGeneratedCatalog(t *testing.T) {
	for _, id := range []string{"", "  "} {
		bundle := &WorkflowContractBundle{
			Nodes: map[string]SystemNodeContract{"worker": {EventHandlers: map[string]SystemNodeEventHandler{
				"request": {Rules: []HandlerRuleEntry{{ID: "not-authority", Activity: ActivitySpec{ID: id, Tool: "send"}}}},
			}}},
			Tools: map[string]ToolSchemaEntry{"send": MustToolSchemaEntry(
				WithToolHandler(ToolHandlerHTTP), WithToolEffect(ActivityEffectClassReadOnly),
				WithToolSchemas(MustToolInputSchema(ToolSchemaObject), MustToolInputSchema(ToolSchemaObject)),
				WithToolHTTP(HTTPToolSpec{Method: "GET", URL: "https://proof.invalid"}),
			)},
		}
		root := &FlowContractView{Path: ".", Paths: FlowContractPaths{FlowPath: "."}, Nodes: bundle.Nodes, Tools: bundle.Tools}
		bundle.FlowTree = FlowTree{Root: root, ByID: map[string]*FlowContractView{".": root}, ByPath: map[string]*FlowContractView{".": root}}
		err := CompileWorkflowSemantics(bundle)
		if err == nil || !strings.Contains(err.Error(), "activity.id is required") {
			t.Fatalf("unnamed rule activity compiled: %v", err)
		}
		if len(bundle.GeneratedActivityEventEntries()) != 0 || len(bundle.GeneratedActivityEventSchemas()) != 0 || len(bundle.GeneratedActivityEventsForExecutableNode(identitytest.RootNode(t, "worker"))) != 0 {
			t.Fatal("refused programmatic rule produced an apparently valid generated catalog")
		}
	}
}
