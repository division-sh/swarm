package pinrouting

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
)

func TestDeploymentFeedSelectedRootRequiresInputNotOutput(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind canonicalrouting.DeploymentFeedRoot
	}{
		{"connected", canonicalrouting.DeploymentFeedConnectedRoot},
		{"same-flow", canonicalrouting.DeploymentFeedSameFlowRoot},
		{"keyed-root", canonicalrouting.DeploymentFeedKeyedRoot},
		{"output-only", canonicalrouting.DeploymentFeedOutputOnlyRoot},
		{"private", canonicalrouting.DeploymentFeedPrivateRoot},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind := tc.kind
			source := loadRootReplyBoundarySource(t, canonicalrouting.CopyRootInputFeed(t, kind))
			routingSource, err := events.NewDeploymentFeedRoutingSource(".")
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := AdmitDeploymentFeedDeclaration(source, "item.registered", routingSource)
			if kind == canonicalrouting.DeploymentFeedOutputOnlyRoot || kind == canonicalrouting.DeploymentFeedPrivateRoot {
				if err == nil || !strings.Contains(err.Error(), "no exact declared root input pin") {
					t.Fatalf("non-input root feed admitted or wrong refusal: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ruled root-input feed refused: %v", err)
			}
			if _, input := semanticview.SelectedRootInputPin(source, "item.registered"); !input || PinDeclaredOutput(source, ".", "item.registered") {
				t.Fatal("positive fixture acquired an output echo or lost its input")
			}
			if admitted.eventType != "item.registered" || admitted.kind != events.RoutingSourceDeploymentFeed || admitted.route != routingSource.Route() {
				t.Fatalf("admission changed the exact source declaration: %#v", admitted)
			}
			if kind == canonicalrouting.DeploymentFeedConnectedRoot {
				graph := CompileConnectGraph(source)
				plans := graph.MatchingSourceEvent(admitted)
				if len(graph.Issues()) != 0 || len(plans) != 1 || plans[0].ReceiverEndpoint().FlowID() != "registry" {
					t.Fatalf("root input lost its compiled connection: issues=%v plans=%v", graph.Issues(), plans)
				}
			}
		})
	}
}

func TestDeploymentFeedRootInputAdmissionPreservesExactDeclaration(t *testing.T) {
	source := loadRootReplyBoundarySource(t, canonicalrouting.CopyRootInputFeed(t, canonicalrouting.DeploymentFeedConnectedRoot))
	rootSource, err := events.NewDeploymentFeedRoutingSource(".")
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := events.NewDeploymentFeedRoutingSource("registry")
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := events.NewRootRoutingSource("root-entity")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		event  events.EventType
		source events.RoutingSource
	}{
		{"unknown", "other.registered", rootSource},
		{"child-qualified", "registry/item.registered", rootSource},
		{"foreign-declaration", "item.registered", foreign},
		{"ordinary-instance", "item.registered", ordinary},
		{"absent-source", "item.registered", events.RoutingSource{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := AdmitDeploymentFeedDeclaration(source, tc.event, tc.source); err == nil {
				t.Fatal("crossed or undeclared feed admitted")
			}
		})
	}
	if _, err := AdmitDeploymentFeedDeclaration(nil, "item.registered", rootSource); err == nil {
		t.Fatal("missing compiled source admitted")
	}
	child := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
	childSource, err := events.NewDeploymentFeedRoutingSource("portfolio")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AdmitDeploymentFeedDeclaration(child, "portfolio/account.registered", childSource); err != nil {
		t.Fatalf("unchanged child-declared output feed refused: %v", err)
	}
}
