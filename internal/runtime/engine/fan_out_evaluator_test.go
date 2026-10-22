package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
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

func preparedFanOutFixture(t testing.TB, r2Mixed ...bool) (*Executor, fanoutobligation.Intent, events.Event, *fanOutCensusCountingSource) {
	t.Helper()
	return preparedFanOutFixtureWithContext(t, events.DeliveryContext{}, r2Mixed...)
}

func preparedFanOutFixtureWithContext(t testing.TB, incoming events.DeliveryContext, r2Mixed ...bool) (*Executor, fanoutobligation.Intent, events.Event, *fanOutCensusCountingSource) {
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
	itemFields := map[string]rc.EventFieldSpec{"value": {Type: "integer"}, "index": {Type: "integer"}, "count": {Type: "integer"}}
	if len(r2Mixed) > 0 && r2Mixed[0] {
		admitted, err := loadNodeHandlerFixture(`emit:
  event: value.completed
  fields:
    note: 'row=${row},meta=${{"missing":null}}'
`)
		if err != nil {
			t.Fatal(err)
		}
		handler.FanOut.Emit.Fields["note"] = admitted.Emit.Fields["note"]
		itemFields["note"] = rc.EventFieldSpec{Type: "text"}
	}
	schema := rc.FlowSchemaDocument{Pins: rc.FlowPins{Outputs: rc.FlowOutputPins{EventPins: []rc.FlowOutputEventPin{{Event: "item.ready"}}}}}
	catalog := map[string]rc.EventCatalogEntry{
		"batch.ready": requiredEventPayload(map[string]rc.EventFieldSpec{"items": {Type: "[integer]"}}),
		"item.ready":  requiredEventPayload(itemFields),
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
	// The two compiled fixture variants must not claim the same artifact.
	fixtureRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(fixtureRoot, "schema.yaml"), []byte("name: engine-fan-out-test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nodeSource, err := json.Marshal(bundle.Nodes)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixtureRoot, "nodes.yaml"), nodeSource, 0o644); err != nil {
		t.Fatal(err)
	}
	bundle.SourceArtifact, err = sourceartifact.AdmitDirectory(fixtureRoot)
	if err != nil {
		t.Fatal(err)
	}
	source := &fanOutCensusCountingSource{Source: mustCompileEngineSource(bundle)}
	exec, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: stubStateRepo{}, MutationOwner: stubMutationOwner{}, Locker: stubLocker{}, PayloadShaper: stubPayloadShaper{}}, nil)
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
	ctx := context.Background()
	if !incoming.Empty() {
		route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Context: incoming,
			Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: trigger.RunID(), EntityID: "entity-1"})}
		routeID, err := route.Identity()
		if err != nil {
			t.Fatal(err)
		}
		deliveryID, err := deliverylifecycle.DeliveryID(trigger.ID(), route)
		if err != nil {
			t.Fatal(err)
		}
		claim, err := deliverylifecycle.AdmitPersistedClaim(deliveryID, trigger.RunID(), events.EncodeDeliveryRouteIdentity(routeID), eventtest.UUID("prepared-fan-out-claim"), 1, deliverylifecycle.SubscriberNode, node.Key())
		if err != nil {
			t.Fatal(err)
		}
		ctx = events.WithDeliveryContext(deliverylifecycle.WithRoute(deliverylifecycle.WithClaim(ctx, claim), route), incoming)
	}
	result, err := exec.ExecuteSemanticFixture(ctx, ExecutionRequest{EntityID: identity.NormalizeEntityID("entity-1"), Node: node, Event: trigger, Handler: handler, State: testStateSnapshot("pending", map[string]any{"offset": int64(5)}, nil, nil)})
	if err != nil || result.FanOutIntent == nil {
		t.Fatalf("capture: %v %#v", err, result.FanOutIntent)
	}
	now := time.Now().UTC()
	intent := fanoutobligation.Intent{Request: *result.FanOutIntent, Source: result.FanOutIntent.Source, Status: fanoutobligation.StatusOpen, NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	return exec, intent, trigger, source
}

func TestDeferredFanOutRetainsCapturedReplyForEveryOrdinal(t *testing.T) {
	declaration, err := timeridentity.NewJoinRef(testRootExecutableNode(t, "worker"), "batch.ready", "pending", "consumed-input")
	if err != nil {
		t.Fatal(err)
	}
	incoming := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:original-request"},
		Joins: []events.JoinAdmissionReceipt{{Ref: declaration, Disposition: events.JoinAdmissionEarly}}}
	exec, intent, trigger, _ := preparedFanOutFixtureWithContext(t, incoming)
	if intent.Request.Capsule.Context.ReplyContextID() != incoming.ReplyContextID() || len(intent.Request.Capsule.Context.Joins) != 0 {
		t.Fatalf("capture lost the reply or copied consumed input admissions: %+v", intent.Request.Capsule.Context)
	}
	raw, err := fanoutobligation.MarshalCapsule(intent.Request.Capsule)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fanoutobligation.Capsule
	if err := json.Unmarshal(raw, &decoded); err != nil || !decoded.Equal(intent.Request.Capsule) {
		t.Fatalf("durable capsule lost exact continuation: %+v error=%v", decoded.Context, err)
	}
	intent.Request.Capsule = decoded
	prepared, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := range 2 {
		// A later pump context cannot supply or replace the original input.
		ctx := events.WithDeliveryContext(context.Background(), events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:unrelated-worker"}})
		emit, err := prepared.EvaluateOrdinal(ctx, int64(ordinal), ordinal)
		if err != nil || emit.Context.ReplyContextID() != incoming.ReplyContextID() {
			t.Fatalf("deferred ordinal %d lost its captured reply: context=%+v error=%v", ordinal, emit.Context, err)
		}
		emit.Context.Reply.ID = "reply-v1:mutated-output"
	}
	if intent.Request.Capsule.Context.ReplyContextID() != incoming.ReplyContextID() {
		t.Fatal("output mutation changed the frozen request")
	}
}

func TestDeferredFanOutWithoutReplyDoesNotBorrowPumpContext(t *testing.T) {
	exec, intent, trigger, _ := preparedFanOutFixture(t)
	prepared, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
	if err != nil {
		t.Fatal(err)
	}
	ctx := events.WithDeliveryContext(context.Background(), events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:unrelated-worker"}})
	emit, err := prepared.EvaluateOrdinal(ctx, int64(0), 0)
	if err != nil || !emit.Context.Empty() {
		t.Fatalf("absence borrowed a worker's return authority: %+v error=%v", emit.Context, err)
	}
}

func TestPreparedFanOutEvaluatesAuthoredR2MixedValue(t *testing.T) {
	exec, intent, trigger, _ := preparedFanOutFixture(t, true)
	preparation, err := exec.PrepareFanOutEvaluation(context.Background(), intent, trigger)
	if err != nil {
		t.Fatal(err)
	}
	emit, err := preparation.EvaluateOrdinal(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["note"] != `row=0,meta={"missing":null}` || payload["value"] != float64(5) {
		t.Fatalf("prepared R2 payload = %#v", payload)
	}
}

func TestFanOutSelectedEmitUsesFrozenSourceAndExactCompiledPlan(t *testing.T) {
	_, original, trigger, _ := preparedFanOutFixture(t)
	selectedExecutor, selected, _, _ := preparedFanOutFixture(t, true)
	if original.Request.PlanRef.SemanticDigest == selected.Request.PlanRef.SemanticDigest {
		t.Fatal("fixture did not change the compiled emit semantics")
	}
	if original.Request.PlanRef.BundleHash == selected.Request.PlanRef.BundleHash {
		t.Fatal("different fixture declarations claimed the same source artifact")
	}
	request := original.Request
	request.PlanRef = selected.Request.PlanRef
	original.Request = request
	prepared, err := selectedExecutor.PrepareFanOutEvaluation(context.Background(), original, trigger)
	if err != nil {
		t.Fatal(err)
	}
	emit, err := prepared.EvaluateOrdinal(context.Background(), int64(1), 1)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["value"] != float64(6) || payload["note"] != `row=1,meta={"missing":null}` || payload["index"] != float64(1) || payload["count"] != float64(33) {
		t.Fatalf("selected emit did not use the retained source frame and ordinal: %#v", payload)
	}
	if original.Request.Capsule.SourcePlanRef == original.Request.PlanRef || original.Request.Capsule.SourceProjection.Emit.Fields["note"].CEL != "" {
		t.Fatal("selected plan replaced the immutable original witness")
	}
	wrong := original
	wrong.Request.PlanRef = wrong.Request.Capsule.SourcePlanRef
	if _, err := selectedExecutor.PrepareFanOutEvaluation(context.Background(), wrong, trigger); err == nil {
		t.Fatal("loaded selected source accepted an unselected executable reference")
	}
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
