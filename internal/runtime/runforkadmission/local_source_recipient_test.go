package runforkadmission

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestLocalSourceRecipientsAgreeForFrontierAndHistory(t *testing.T) {
	root := canonicalrouting.CopySelectedForkReadiness(t, 0, "node")
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	const instance = "worker-flow/worker-001"
	const sibling = "worker-flow/worker-002"
	origin := eventtest.ConcreteTemplateRoutingSource("worker-flow", instance, eventtest.UUID("worker-001"))
	for _, classification := range []string{runfork.RunForkPendingClassificationPending, runfork.RunForkPendingClassificationDeliveredCompleted} {
		for _, tc := range []struct {
			name  string
			event string
			from  events.RoutingSource
			want  bool
		}{
			{"flow-qualified", "worker-flow/worker.inspect", origin, true},
			{"concrete", instance + "/worker.inspect", origin, true},
			{"local", "worker.inspect", origin, true},
			{"foreign-name", "unrelated/worker.inspect", origin, false},
			{"missing-source", "worker-flow/worker.inspect", events.NoRoutingSource(), false},
		} {
			t.Run(classification+"/"+tc.name, func(t *testing.T) {
				plan := testRunForkPlan(tc.event, classification, "node", "old-subscriber")
				plan.PendingWork[0].RoutingSource = tc.from
				// A different historical receiver is evidence, not the source.
				plan.PendingWork[0].FlowInstance = sibling
				plan = withConstructedHeader(t, plan, source, "worker-flow", "worker-001")
				plan = withConstructedHeader(t, plan, source, "worker-flow", "worker-002")
				before := recipientAuthorityJSON(t, plan)
				got, _ := recipientAuthorityAdmit(t, plan, source, classification)
				if tc.want {
					if len(got) != 1 || got[0].Path != instance || got[0].HandlerNode() != identitytest.FlowNode(t, "worker-flow", "inspect-node") || got[0].HandlerEvent() != "worker.inspect" {
						t.Fatalf("lost exact local consumer or borrowed sibling identity: %+v", got)
					}
					if _, _, connected := got[0].Connect(); connected {
						t.Fatal("ordinary local recipient gained a connection claim")
					}
				} else if len(got) != 0 {
					t.Fatalf("missing or foreign source acquired a local consumer: %+v", got)
				}
				if after := recipientAuthorityJSON(t, plan); after != before {
					t.Fatal("recipient derivation changed historical evidence")
				}
			})
		}
	}
}
