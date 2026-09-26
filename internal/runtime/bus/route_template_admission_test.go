package bus

import (
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTemplateSubscriptionProjectionMatchesFreshAdmission(t *testing.T) {
	scatter, _ := topologyOperationFixture(t)
	sources := map[string]semanticview.Source{
		"scatter":           scatter.Source,
		"template_observer": loadHarnessRouteSource(t, canonicalrouting.CopyTemplateOutputRootConnect(t)),
		"external_input":    loadHarnessRouteSource(t, canonicalrouting.CopyInputPinExternalScope(t)),
	}
	checked := 0
	for name, source := range sources {
		t.Run(name, func(t *testing.T) {
			table, err := DeriveRouteTable(source)
			if err != nil {
				t.Fatal(err)
			}
			_, inputProducers := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(source)
			for path, template := range table.templates {
				for _, subscriber := range template.Subscribers {
					for _, pattern := range subscriber.Patterns {
						for _, instancePath := range []string{path + "/first", path + "/second"} {
							want, err := routeResolveSubscriberPatternsWithInputProducers(source, subscriber.Kind, template.FlowID, template.InputEvents, path, instancePath, template.LocalEvents, pattern.raw, inputProducers)
							if err != nil {
								t.Fatal(err)
							}
							got := routeProjectAdmittedSubscriberPatterns(pattern.admission, template.FlowID, instancePath, pattern.inputEvent, inputProducers)
							if !reflect.DeepEqual(got, want) {
								t.Fatalf("%s %s %s: compiled projection = %#v, fresh admission = %#v", path, pattern.raw, instancePath, got, want)
							}
							checked++
						}
					}
				}
			}
		})
	}
	if checked == 0 {
		t.Fatal("fixtures did not exercise a concrete template subscription")
	}
}

type templateInputProbeSource struct {
	semanticview.Source
	inputLookups atomic.Int64
	nodeLookups  atomic.Int64
}

func (s *templateInputProbeSource) FlowHasInputEvent(flowID, eventType string) bool {
	s.inputLookups.Add(1)
	return s.Source.FlowHasInputEvent(flowID, eventType)
}

func (s *templateInputProbeSource) ExecutableNode(node runtimeidentity.ExecutableNode) (runtimecontracts.ScopedNodeRecord, bool) {
	s.nodeLookups.Add(1)
	return s.Source.ExecutableNode(node)
}

func TestTemplateMaterializationDoesNotRepeatDeclarationAdmission(t *testing.T) {
	fixture, _ := topologyOperationFixture(t)
	source := &templateInputProbeSource{Source: fixture.Source}
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, template := range table.templates {
		for _, subscriber := range template.Subscribers {
			if subscriber.Kind != subscriberNode {
				continue
			}
			want, err := runtimepipeline.AdmitDeliveryTargetHandler(source.Source, subscriber.HandlerNode)
			if err != nil || !subscriber.TargetHandler.Equal(want) {
				t.Fatalf("compiled node handler = %#v, canonical admission = %#v, err = %v", subscriber.TargetHandler, want, err)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("fixture has no concrete template node handler")
	}
	_, inputProducers := runtimepinrouting.CompileConnectGraphWithInputProducerResolver(source)
	source.inputLookups.Store(0)
	source.nodeLookups.Store(0)
	var inputLookups, nodeLookups []int64
	for _, id := range []string{"alpha", "beta", "gamma"} {
		if _, err := table.addFlowInstanceRouteForTopology(FlowInstanceRouteMaterializationRequest{
			Identity: topologyOperationIdentity(t, id),
		}, &inputProducers); err != nil {
			t.Fatal(err)
		}
		inputLookups = append(inputLookups, source.inputLookups.Load())
		nodeLookups = append(nodeLookups, source.nodeLookups.Load())
	}
	if inputLookups[1] != inputLookups[0] || inputLookups[2] != inputLookups[0] ||
		nodeLookups[1] != nodeLookups[0] || nodeLookups[2] != nodeLookups[0] {
		t.Fatalf("later instances re-admitted template declarations: inputs=%v nodes=%v", inputLookups, nodeLookups)
	}
}

type invalidTemplateSubscriptionSource struct{ semanticview.Source }

func (s invalidTemplateSubscriptionSource) ExecutableNodeRuntimeSubscriptions(runtimeidentity.ExecutableNode) []string {
	return []string{"foreign/unauthorized"}
}

func TestTemplateSubscriptionAdmissionStillRejectsForeignScope(t *testing.T) {
	source, _ := topologyOperationFixture(t)
	_, err := DeriveRouteTable(invalidTemplateSubscriptionSource{source.Source})
	if err == nil || !strings.Contains(err.Error(), "subscription") {
		t.Fatalf("foreign-scope subscription admission = %v, want fail-closed rejection", err)
	}
}

type missingTemplateNodeSource struct{ semanticview.Source }

func (s missingTemplateNodeSource) ExecutableNode(node runtimeidentity.ExecutableNode) (runtimecontracts.ScopedNodeRecord, bool) {
	if node.FlowPath() == "workers" {
		return runtimecontracts.ScopedNodeRecord{}, false
	}
	return s.Source.ExecutableNode(node)
}

func TestTemplateHandlerAdmissionRejectsMissingExactNode(t *testing.T) {
	source, _ := topologyOperationFixture(t)
	_, err := DeriveRouteTable(missingTemplateNodeSource{source.Source})
	if err == nil || !strings.Contains(err.Error(), "route subscriber node") || !strings.Contains(err.Error(), "no exact declaration") {
		t.Fatalf("missing exact template node admission = %v, want fail-closed rejection", err)
	}
}
