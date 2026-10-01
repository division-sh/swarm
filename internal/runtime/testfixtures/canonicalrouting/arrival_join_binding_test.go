package canonicalrouting

import (
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestArrivalJoinRoutingFixturesRetainExactReceiverPolicyAndLifecycle(t *testing.T) {
	for _, variant := range []ArrivalJoinRoutingFixture{
		ArrivalJoinBoundReply, ArrivalJoinBoundReplyObserver, ArrivalJoinFieldlessReply,
		ArrivalJoinPayloadDirected, ArrivalJoinPayloadDirectedMultipleRecipients, ArrivalJoinMultiUntil,
	} {
		t.Run(fmt.Sprint(variant), func(t *testing.T) {
			files := ArrivalJoinRoutingFiles(t, variant)
			root := t.TempDir()
			for path, body := range files {
				writeClosedVariantFile(t, root, path, body)
			}
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(t), root, contracts.DefaultPlatformSpecFile(RepoRoot(t)))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			graph := pinrouting.CompileConnectGraph(source)
			if issues := graph.Issues(); len(issues) != 0 {
				t.Fatalf("closed fixture lost compiler-admitted ordinary connections: %#v", issues)
			}
			wantEdges := 1
			switch variant {
			case ArrivalJoinBoundReply, ArrivalJoinFieldlessReply, ArrivalJoinPayloadDirectedMultipleRecipients, ArrivalJoinMultiUntil:
				wantEdges = 2
			case ArrivalJoinBoundReplyObserver:
				wantEdges = 4
			}
			if edges := graph.Plans(); len(edges) != wantEdges {
				t.Fatalf("closed fixture connect count=%d, want %d", len(edges), wantEdges)
			}
			if variant == ArrivalJoinFieldlessReply {
				if _, _, exists := bundle.FlowPrimaryEntityContract("requester"); exists {
					t.Fatal("fieldless requester gained an entity")
				}
				return
			}
			flow, event := "orders", "item.completed"
			if variant == ArrivalJoinBoundReply || variant == ArrivalJoinBoundReplyObserver {
				flow, event = "requester", "provider.replied"
			}
			if bundle.FlowTree.ByPath[flow].Schema.Instance.Path() != "order_id" {
				t.Fatal("closed fixture lost exact authored receiving key")
			}
			plan, found := semanticview.WorkflowJoinPlanForHandler(source, identitytest.FlowNode(t, flow, "collector"), event)
			if !found || plan.Spec.Stage != "awaiting" || plan.Spec.Members.From != "state.expected" || plan.Spec.Members.By != "payload.member_id" || plan.Spec.Output != "payload.result" || plan.ResultType.Type != "JoinResult" {
				t.Fatalf("closed fixture lost exact join membership/result: %#v", plan)
			}
			if plan.Spec.Deadline == nil || plan.Spec.Deadline.After != "1h" || plan.Spec.Deadline.From != contracts.JoinDeadlineFromStageEntry || plan.Spec.OnDeadline.AdvancesTo != "attention" {
				t.Fatalf("closed fixture lost lifecycle deadline ownership: %#v", plan.Spec)
			}
			files["schema.yaml"] = "mutated caller copy"
			if ArrivalJoinRoutingFiles(t, variant)["schema.yaml"] == files["schema.yaml"] {
				t.Fatal("closed fixture source aliases a previous caller")
			}
		})
	}
}
