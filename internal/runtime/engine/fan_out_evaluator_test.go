package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type fanOutCensusCountingSource struct {
	semanticview.Source
	calls int
}

type preparedFanOutPayload struct {
	Value, Index, Count int64
	ShapedFor           string `json:"shaped_for"`
}

func (s *fanOutCensusCountingSource) ExecutableNodeRecords() []rc.ScopedNodeRecord {
	s.calls++
	return s.Source.ExecutableNodeRecords()
}

func preparedFanOutFixture(t testing.TB) (*Executor, fanoutobligation.Intent, events.Event, *fanOutCensusCountingSource) {
	t.Helper()
	node := testRootExecutableNode(t, "worker")
	handler, err := rc.QualifySystemNodeHandlerRuleRefsForEvent(node, "batch.ready", rc.SystemNodeEventHandler{FanOut: &rc.FanOutSpec{
		ItemsFrom: "payload.items", As: "row", Identity: "string(row)",
		Emit: rc.EmitSpec{Event: "item.ready", Fields: map[string]rc.ExpressionValue{
			"value": rc.CELExpression("row + entity.offset"), "index": rc.CELExpression("fan_out.index"), "count": rc.CELExpression("fan_out.count"),
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	schema := rc.FlowSchemaDocument{Pins: rc.FlowPins{Outputs: rc.FlowOutputPins{EventPins: []rc.FlowOutputEventPin{{Event: "item.ready", Sink: rc.FlowOutputSinkHarness}}}}}
	catalog := map[string]rc.EventCatalogEntry{
		"batch.ready": requiredEventPayload(map[string]rc.EventFieldSpec{"items": {Type: "[integer]"}}),
		"item.ready":  requiredEventPayload(map[string]rc.EventFieldSpec{"value": {Type: "integer"}, "index": {Type: "integer"}, "count": {Type: "integer"}}),
	}
	root := &rc.FlowContractView{Path: ".", Paths: rc.FlowContractPaths{FlowPath: ".", SchemaFile: "schema.yaml"}, Schema: schema, Events: catalog}
	bundle := &rc.WorkflowContractBundle{
		RootSchema: &schema, Events: catalog,
		FlowSources:  map[string]rc.FlowSource{".": {FlowPath: ".", Schema: "schema.yaml"}},
		FlowTree:     rc.FlowTree{Root: root, ByID: map[string]*rc.FlowContractView{".": root}, ByPath: map[string]*rc.FlowContractView{".": root}},
		Nodes:        map[string]rc.SystemNodeContract{"worker": {EventHandlers: map[string]rc.SystemNodeEventHandler{"batch.ready": handler}}},
		Semantics:    rc.WorkflowSemanticView{Name: "root", Version: "v-test", NodeHandlers: map[string]map[string]rc.SystemNodeEventHandler{"worker": {"batch.ready": handler}}},
		RootEntities: rc.EntityContractsDocument{"subject": {Fields: map[string]rc.EntityFieldDecl{"offset": {Type: "integer"}}}},
	}
	source := &fanOutCensusCountingSource{Source: fanOutSourceWithBundleIdentity(t, bundle)}
	exec, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, Dispatcher: stubDispatcher{}, PayloadShaper: stubPayloadShaper{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	items := make([]int, 33)
	for i := range items {
		items[i] = i
	}
	raw, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		t.Fatal(err)
	}
	trigger := eventtest.RunCreatingRootIngress(eventtest.UUID("prepared-fan-out"), "batch.ready", "", "", raw, 0, semanticExecutionFixtureRunID, "", events.EventEnvelope{}, time.Now().UTC())
	result, err := exec.ExecuteSemanticFixture(context.Background(), ExecutionRequest{EntityID: identity.NormalizeEntityID("entity-1"), Node: node, Event: trigger, Handler: handler, State: testStateSnapshot("pending", map[string]any{"offset": int64(5)}, nil, nil)})
	if err != nil || result.FanOutIntent == nil {
		t.Fatalf("capture: %v %#v", err, result.FanOutIntent)
	}
	now := time.Now().UTC()
	intent := fanoutobligation.Intent{Request: *result.FanOutIntent, Source: result.FanOutIntent.Source, Status: fanoutobligation.StatusOpen, NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	return exec, intent, trigger, source
}

func TestFanOutPreparationBoundsCensusAndPreservesOrdinals(t *testing.T) {
	exec, intent, trigger, source := preparedFanOutFixture(t)
	source.calls = 0
	p, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
	if err != nil {
		t.Fatal(err)
	}
	preparationCalls := source.calls
	if preparationCalls == 0 {
		t.Fatal("fixture did not exercise canonical endpoint census")
	}
	intent.Request.Capsule.StateFields["offset"] = int64(900)
	for ordinal := 0; ordinal < 32; ordinal++ {
		emit, err := p.EvaluateOrdinal(context.Background(), int64(ordinal), ordinal)
		if err != nil {
			t.Fatal(err)
		}
		var payload preparedFanOutPayload
		if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Value != int64(ordinal+5) || payload.Index != int64(ordinal) || payload.Count != 33 || payload.ShapedFor != "item.ready" {
			t.Fatalf("ordinal %d: %v", ordinal, payload)
		}
		if emit.Event.ID() != "" || emit.Event.ParentEventID() != trigger.ID() || emit.Event.RunID() != intent.Request.Key.RunID || emit.Event.RoutingSource() != intent.Request.Capsule.ProducerSource {
			t.Fatal("ordinal lineage/source changed or evaluator minted publication identity")
		}
	}
	if source.calls != preparationCalls {
		t.Fatalf("per-ordinal endpoint census: prepare=%d after=%d", preparationCalls, source.calls)
	}
	for _, ordinal := range []int{-1, 32, 33} {
		if _, err := p.EvaluateOrdinal(context.Background(), int64(1), ordinal); err == nil {
			t.Fatalf("accepted ordinal %d outside chunk", ordinal)
		}
	}
	intent.Cursor = 32
	next, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
	if err != nil {
		t.Fatal(err)
	}
	emit, err := next.EvaluateOrdinal(context.Background(), int64(32), 32)
	if err != nil {
		t.Fatal(err)
	}
	var payload preparedFanOutPayload
	if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil || payload.Value != 932 {
		t.Fatalf("new capsule borrowed prior preparation: %v %v", payload, err)
	}
}

type mutatingFanOutShaper struct{ calls int }

func (s *mutatingFanOutShaper) ShapeEmitPayload(_ context.Context, req ExecutionRequest, _ string, payload map[string]any) (map[string]any, error) {
	s.calls++
	if req.State.Fields["offset"] != int64(5) {
		return nil, fmt.Errorf("previous shaper changed capsule")
	}
	req.State.Fields["offset"] = int64(999)
	if payload["index"] == int64(1) {
		return nil, &EmitPayloadContractError{Kind: EmitPayloadSchemaMismatch, Cause: errors.New("hostile item")}
	}
	return payload, nil
}

func TestFanOutPreparationPreservesPerItemShapingAndIsolation(t *testing.T) {
	exec, intent, trigger, _ := preparedFanOutFixture(t)
	shaper := &mutatingFanOutShaper{}
	exec.deps.PayloadShaper = shaper
	p, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := 0; ordinal < 3; ordinal++ {
		_, err := p.EvaluateOrdinal(context.Background(), int64(ordinal), ordinal)
		if ordinal == 1 {
			if !errors.Is(err, ErrEmitPayloadContractViolation) {
				t.Fatalf("typed item failure: %v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if shaper.calls != 3 {
		t.Fatalf("payload validation skipped: %d", shaper.calls)
	}
}

func TestFanOutPreparationRejectsHostilePinnedEvidence(t *testing.T) {
	exec, intent, trigger, _ := preparedFanOutFixture(t)
	for _, tc := range []struct {
		name   string
		change func(*fanoutobligation.Intent)
	}{
		{"bundle", func(i *fanoutobligation.Intent) { i.Request.PlanRef.BundleHash = "other-bundle" }},
		{"digest", func(i *fanoutobligation.Intent) { i.Request.PlanRef.SemanticDigest = "other-plan" }},
		{"handler", func(i *fanoutobligation.Intent) { i.Request.Capsule.HandlerEventKey = "other.ready" }},
		{"trigger", func(i *fanoutobligation.Intent) {
			i.Request.Capsule.Lineage.ParentEventID = eventtest.UUID("other-trigger")
		}},
		{"receiver", func(i *fanoutobligation.Intent) {
			i.Request.Capsule.Receiver = &fanoutobligation.ExecutionReceiver{Node: testRootExecutableNode(t, "worker"), Target: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: ".", EntityID: "other-entity"})}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hostile := intent
			tc.change(&hostile)
			if _, err := exec.PrepareFanOutEvaluation(context.Background(), hostile, trigger); err == nil {
				t.Fatal("hostile pinned evidence was accepted")
			}
		})
	}
}

func BenchmarkFanOutEvaluationPreparation(b *testing.B) {
	for _, prepared := range []bool{false, true} {
		b.Run(fmt.Sprintf("chunk_prepared=%t", prepared), func(b *testing.B) {
			exec, intent, trigger, _ := preparedFanOutFixture(b)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var p *FanOutEvaluation
				var err error
				if prepared {
					p, err = exec.PrepareFanOutEvaluation(ctx, intent, trigger)
					if err != nil {
						b.Fatal(err)
					}
				}
				for ordinal := 0; ordinal < 32; ordinal++ {
					if prepared {
						_, err = p.EvaluateOrdinal(ctx, int64(ordinal), ordinal)
					} else {
						_, err = exec.EvaluateFanOutOrdinal(ctx, intent, trigger, int64(ordinal), ordinal)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
