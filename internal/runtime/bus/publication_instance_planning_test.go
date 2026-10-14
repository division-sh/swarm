package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type publicationPlanningBoundaryReader struct {
	constructionIndexTestReader
	passes int
	fault  error
	inside bool
}

func (r *publicationPlanningBoundaryReader) WithFlowInstancePlanningSnapshot(ctx context.Context, _ pipeline.FlowInstanceLookupScope, prepare func(context.Context, pipeline.FlowInstanceIndexReader) error) error {
	r.passes++
	if r.fault != nil {
		return r.fault
	}
	r.inside = true
	defer func() { r.inside = false }()
	return prepare(ctx, &r.constructionIndexTestReader)
}

func TestPublicationPlanningReadsDurableAgentsBeforeIndexCut(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	event := admitRunProposalEvent(t, false).Event()
	reader := &publicationPlanningBoundaryReader{}
	reads := 0
	planner := deliveryPlanner{
		connectPlanner: connectRoutePlanResolver{lifecycle: connectInstanceSelector{index: reader}},
		recipientPolicy: deliveryRecipientPolicy{loadActiveAgentDescriptors: func(ctx context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
			reads++
			if reader.inside {
				return nil, false, errors.New("durable agents require a second pool connection")
			}
			return nil, true, ctx.Err()
		}},
	}
	bus := &EventBus{semanticSource: source}
	if err := bus.withPublicationInstancePlanning(constructionIndexContext(t, source), event, planner, func(ctx context.Context, bounded deliveryPlanner) error {
		if !reader.inside {
			t.Fatal("index planning escaped its finite cut")
		}
		for read := 0; read < 3; read++ {
			if _, available, err := bounded.recipientPolicy.loadActiveAgentDescriptors(ctx); err != nil || !available {
				return errors.New("policy lost exact active-agent projection")
			}
			if _, available, err := bounded.connectPlanner.loadAgents(ctx); err != nil || !available {
				return errors.New("connect planner lost exact active-agent projection")
			}
		}
		return nil
	}); err != nil || reads != 1 || reader.passes != 1 {
		t.Fatalf("agent reads crossed the index cut: reads=%d passes=%d err=%v", reads, reader.passes, err)
	}
	fault := errors.New("durable active-agent refusal")
	planner.recipientPolicy.loadActiveAgentDescriptors = func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
		return nil, true, fault
	}
	if err := bus.withPublicationInstancePlanning(constructionIndexContext(t, source), event, planner, func(context.Context, deliveryPlanner) error {
		t.Fatal("failed active-agent read admitted planning")
		return nil
	}); !errors.Is(err, fault) || reader.passes != 1 {
		t.Fatalf("independent active-agent refusal was suppressed: passes=%d err=%v", reader.passes, err)
	}
}

func TestPublicationPlanningKeepsNestedReplyReadOutsideIndexCut(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	ctx := constructionIndexContext(t, source)
	event := admitRunProposalEvent(t, false).Event()
	reader := &publicationPlanningBoundaryReader{fault: errors.New("reply planning opened a nested index transaction")}
	planner := deliveryPlanner{connectPlanner: connectRoutePlanResolver{lifecycle: connectInstanceSelector{index: reader}}}
	bus := &EventBus{semanticSource: source}
	ctx = events.WithDeliveryContext(ctx, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "exact-reply-context"}})
	called := false
	if err := bus.withPublicationInstancePlanning(ctx, event, planner, func(_ context.Context, got deliveryPlanner) error {
		called = true
		if got.connectPlanner.lifecycle.index != reader {
			t.Fatal("reply path borrowed a different index authority")
		}
		return nil
	}); err != nil || !called || reader.passes != 0 {
		t.Fatalf("independent reply read lost its original transaction boundary: called=%t passes=%d err=%v", called, reader.passes, err)
	}
	// A non-reply publication still enters the canonical owner and preserves its
	// independent error rather than treating it as construction absence.
	if err := bus.withPublicationInstancePlanning(constructionIndexContext(t, source), event, planner, func(context.Context, deliveryPlanner) error {
		t.Fatal("failed planning owner called its consumer")
		return nil
	}); !errors.Is(err, reader.fault) || reader.passes != 1 {
		t.Fatalf("ordinary planning bypassed its native owner: passes=%d err=%v", reader.passes, err)
	}
}
