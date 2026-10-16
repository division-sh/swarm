package bus

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type refusedSuffixReader struct {
	constructionIndexTestReader
	failures map[string]error
	reads    map[string]int
}

func (r *refusedSuffixReader) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	if failure, selected := r.failures[request.FlowID()]; selected {
		r.reads[request.FlowID()]++
		return pipeline.FlowInstanceObservation{}, false, failure
	}
	return r.constructionIndexTestReader.LookupFlowInstance(ctx, request)
}

func refusedSuffixSource(t *testing.T, intermediate string) semanticview.Source {
	t.Helper()
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyUnavailableConnectionScan(t, intermediate))
	plans, issues := compiledConnectPlans(source)
	want := 4
	if intermediate == "collision" {
		want++
	}
	if len(plans) != want || len(issues) != 0 {
		t.Fatalf("refusal scan requires lawful compiled connections: plans=%d issues=%+v", len(plans), issues)
	}
	return source
}

func refusedSuffixEvent(t *testing.T, source semanticview.Source, intermediate string) events.Event {
	t.Helper()
	producer := ConstructedFlowInstanceIdentityFixture(source, "producer", "", busInternalTestRunID)
	payload := []byte(`{}`)
	if intermediate == "create" {
		payload = []byte(`{"instance_key":"suffix-only"}`)
	}
	return connectRoutePlanStaticProducerEvent(eventtest.UUID("terminal-before-intermediate-refusal"), "producer/work.ready", "test", "", payload, 0,
		busInternalTestRunID, "", events.EventEnvelope{Source: events.RouteIdentity{
			FlowID: producer.TemplateID, FlowInstance: producer.InstancePath, EntityID: producer.EntityID,
		}}, time.Now().UTC())
}

func newRefusedSuffixReader(t *testing.T, source semanticview.Source, failures map[string]error) *refusedSuffixReader {
	t.Helper()
	reader := &refusedSuffixReader{failures: failures, reads: make(map[string]int)}
	for _, id := range []string{".", "producer", "aterminal", "bintermediate", "yerror", "zerror"} {
		schema, _ := source.FlowSchemaByID(id)
		if !schema.Instance.Empty() {
			continue
		}
		identity := ConstructedFlowInstanceIdentityFixture(source, id, "", busInternalTestRunID)
		observation := constructionIndexObservation(t, source, busInternalTestRunID, identity, "")
		if id == "aterminal" {
			observation = TerminatedFlowInstanceObservationFixture(t, source, busInternalTestRunID, identity)
		}
		reader.observations = append(reader.observations, observation)
	}
	return reader
}

func TestUnavailableConnectionExaminesErrorsAfterIntermediateRefusal(t *testing.T) {
	for _, intermediate := range []string{"none", "collision", "missing_key"} {
		for _, cause := range []string{"store", "cancellation", "joined"} {
			t.Run(intermediate+"/"+cause, func(t *testing.T) {
				source := refusedSuffixSource(t, intermediate)
				first := errors.New("independent penultimate receiver read")
				last := errors.New("independent last receiver read")
				if cause == "cancellation" {
					last = context.Canceled
				} else if cause == "joined" {
					last = errors.Join(last, context.Canceled)
				}
				reader := newRefusedSuffixReader(t, source, map[string]error{"yerror": first, "zerror": last})
				resolver := newConnectRoutePlanResolver(source, derivedRouteTableFixture(t, source), nil, reader, nil)
				ctx := withConnectRoutePlanPreview(constructionIndexContext(t, source))
				preview := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
				result, err := resolver.Plan(ctx, refusedSuffixEvent(t, source, intermediate))
				var terminal *pipeline.TerminatedReceiverError
				if !errors.Is(err, first) || !errors.Is(err, last) || !errors.As(err, &terminal) || terminal.FlowID != "aterminal" ||
					reader.reads["yerror"] != 1 || reader.reads["zerror"] != 1 ||
					!reflect.DeepEqual(result, connectRoutePlanDispatch{}) || len(preview.plans) != 0 || len(preview.selected) != 0 {
					t.Fatalf("refusal hid later errors or leaked suffix preparation: reads=%v result=%+v preview=%+v err=%v", reader.reads, result, preview, err)
				}
			})
		}
	}
}

func TestUnavailableConnectionDoesNotExportSuccessfulSuffix(t *testing.T) {
	for _, intermediate := range []string{"none", "collision", "missing_key", "create"} {
		t.Run(intermediate, func(t *testing.T) {
			source := refusedSuffixSource(t, intermediate)
			reader := newRefusedSuffixReader(t, source, nil)
			planner := &refusedSuffixActivationPlanner{owner: newTestFlowInstanceActivationOwner(nil)}
			resolver := newConnectRoutePlanResolver(source, derivedRouteTableFixture(t, source), planner, reader, nil)
			ctx := withConnectRoutePlanPreview(constructionIndexContext(t, source))
			preview := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes)
			result, err := resolver.Plan(ctx, refusedSuffixEvent(t, source, intermediate))
			var terminal *pipeline.TerminatedReceiverError
			if err != nil || result.Failure != pinrouting.FailureTargetUnreachableTerminated ||
				!errors.As(result.unavailableReceiverError, &terminal) || terminal.FlowID != "aterminal" ||
				len(result.DeliveryIntents) != 0 || len(result.LiveRecipients) != 0 || len(result.ActivationPlans) != 0 ||
				len(result.ReplyCreations) != 0 || len(result.ReplyClaims) != 0 ||
				len(preview.plans) != 0 || len(preview.selected) != 0 {
				t.Fatalf("suffix replaced refusal or escaped its private preview: result=%+v preview=%+v err=%v", result, preview, err)
			}
			if intermediate == "create" && planner.prepared != 1 {
				t.Fatalf("creation suffix was not genuinely prepared: count=%d", planner.prepared)
			}
		})
	}
}

type refusedSuffixActivationPlanner struct {
	owner    pipeline.FlowInstanceActivationPlanner
	prepared int
}

func (p *refusedSuffixActivationPlanner) PrepareFlowInstanceActivation(ctx context.Context, request pipeline.FlowInstanceActivationRequest) (pipeline.FlowInstanceActivationPlan, error) {
	plan, err := p.owner.PrepareFlowInstanceActivation(ctx, request)
	if err == nil {
		p.prepared++
	}
	return plan, err
}
