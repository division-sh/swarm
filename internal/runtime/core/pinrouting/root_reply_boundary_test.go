package pinrouting

import (
	"fmt"
	"strings"
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

func TestRootReplyCorrelationRequiresBothSchemas(t *testing.T) {
	for _, rootRequester := range []bool{true, false} {
		for _, renamed := range []bool{false, true} {
			for _, mutation := range []canonicalrouting.RootReplyNegativeMutation{
				canonicalrouting.RootReplyCorrelationAbsent,
				canonicalrouting.RootReplyCorrelationOptional,
				canonicalrouting.RootReplyCorrelationList,
				canonicalrouting.RootReplyCorrelationIncompatible,
			} {
				t.Run(fmt.Sprintf("requester_%t/renamed_%t/mutation_%d", rootRequester, renamed, mutation), func(t *testing.T) {
					root := canonicalrouting.CopyRootReplyBoundary(t, rootRequester, true)
					if renamed {
						canonicalrouting.RenameRootReplyEndpoints(t, root, rootRequester)
					}
					if graph := CompileConnectGraph(loadRootReplyBoundarySource(t, root)); len(graph.Issues()) != 0 {
						t.Fatalf("valid explicit correlation refused: %+v", graph.Issues())
					}
					canonicalrouting.ApplyRootReplyBoundaryNegativeMutation(t, root, rootRequester, mutation)
					graph := CompileConnectGraph(loadRootReplyBoundarySource(t, root))
					for _, issue := range graph.Issues() {
						if issue.Failure == ConnectFailureReplyLineageMissing && strings.Contains(issue.Detail, "correlation_key") && strings.Contains(issue.Detail, "reply") {
							return
						}
					}
					t.Fatalf("invalid response correlation acquired a plan: %+v", graph)
				})
			}
		}
	}
}

func TestRootReplyLocalReceiverRequiresActualConsumer(t *testing.T) {
	for _, rootRequester := range []bool{true, false} {
		for _, reply := range []bool{false, true} {
			for _, nested := range []bool{false, true} {
				for _, agent := range []bool{false, true} {
					t.Run(fmt.Sprintf("requester_%t/reply_%t/nested_%t/agent_%t", rootRequester, reply, nested, agent), func(t *testing.T) {
						root, owner := canonicalrouting.CopyRootReplyConsumerBoundary(t, rootRequester, reply, nested, agent)
						if graph := CompileConnectGraph(loadRootReplyBoundarySource(t, root)); len(graph.Issues()) != 0 {
							t.Fatalf("valid local consumer refused: %+v", graph.Issues())
						}
						restore := canonicalrouting.RemoveRootReplyConsumer(t, owner, rootRequester, agent)
						graph := CompileConnectGraph(loadRootReplyBoundarySource(t, root))
						found := false
						for _, issue := range graph.Issues() {
							if issue.Failure == ConnectFailureDeliveryTopologyInvalid && strings.Contains(issue.Detail, "requires a genuine consumer") {
								found = true
							}
						}
						if !found {
							t.Fatalf("schema-only parent receiver acquired a plan: %+v", graph)
						}
						restore()
						if graph := CompileConnectGraph(loadRootReplyBoundarySource(t, root)); len(graph.Issues()) != 0 {
							t.Fatalf("restored consumer refused: %+v", graph.Issues())
						}
					})
				}
			}
		}
	}
}

func loadRootReplyBoundarySource(t testing.TB, root string) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func TestParentLocalRootExportDoesNotRequireInternalConsumer(t *testing.T) {
	root := canonicalrouting.CopyRootReplyBoundary(t, false, false)
	canonicalrouting.ApplyRootReplyBoundaryNegativeMutation(t, root, false, canonicalrouting.RootReplyExportObserverAbsent)
	source := loadRootReplyBoundarySource(t, root)
	if _, exported := semanticview.SelectedRootOutputPin(source, "request.finished"); !exported {
		t.Fatal("fixture lost its explicit root export")
	}
	if consumers := semanticview.BuildAuthoredEventEndpointCensus(source).MatchingConsumers(".", "request.finished"); len(consumers) != 0 {
		t.Fatalf("fixture invented an internal export consumer: %+v", consumers)
	}
	if graph := CompileConnectGraph(source); len(graph.Issues()) != 0 {
		t.Fatalf("lawful zero-recipient root export refused: %+v", graph.Issues())
	}
}
