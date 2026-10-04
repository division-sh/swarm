package pinrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRootReplyBoundaryUsesLocalEventsAndDeclaredChildPins(t *testing.T) {
	for _, rootRequester := range []bool{true, false} {
		for _, explicit := range []bool{false, true} {
			name := "root-provider"
			if rootRequester {
				name = "root-requester"
			}
			if explicit {
				name += "/explicit"
			} else {
				name += "/event-id"
			}
			t.Run(name, func(t *testing.T) {
				root := canonicalrouting.CopyRootReplyBoundary(t, rootRequester, explicit)
				repo := canonicalrouting.RepoRoot(t)
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				source := semanticview.Wrap(bundle)
				graph := CompileConnectGraph(source)
				if len(graph.Issues()) != 0 {
					t.Fatalf("compile issues: %+v", graph.Issues())
				}
				roles := map[ConnectReplyRole]int{}
				for _, plan := range graph.Plans() {
					if plan.ReplyResolution() == nil {
						continue
					}
					roles[plan.ReplyRole()]++
					r := plan.Readback()
					local, child := r.Source, r.Receiver
					if !plan.SourceEndpoint().IsRoot() {
						local, child = child, local
					}
					if !local.LocalEndpoint || local.EventSchemaDigest == "" || local.PinDigest != "" || child.LocalEndpoint || child.PinDigest == "" {
						t.Fatalf("local/boundary evidence conflated: %+v", r)
					}
				}
				if roles[ConnectReplyRoleRequest] != 1 || roles[ConnectReplyRoleResponse] != 1 {
					t.Fatalf("paired roles: %+v", roles)
				}
				if _, ok := source.FlowInputEventPin(".", "provider.replied"); ok {
					t.Fatal("reply became a public root input")
				}
				if _, ok := source.FlowOutputEventPin(".", "provider.requested"); ok {
					t.Fatal("request became a public root output")
				}
			})
		}
	}
}
