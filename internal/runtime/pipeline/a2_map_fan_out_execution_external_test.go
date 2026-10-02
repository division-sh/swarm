package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

// Delay only the selected selector handoff. The retained owner and coordinator
// below perform the actual claim, immutable read, evaluation and publication.
type a2MapFanOutHandoff struct {
	owner pipeline.FanOutObligationOwner
	key   fanoutobligation.IntentKey
}

type a2MapFanOutSelector struct {
	selected chan a2MapFanOutHandoff
	errors   chan error
	done     chan struct{}
}

func (s *a2MapFanOutSelector) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	s.selected <- a2MapFanOutHandoff{owner: owner, key: key}
	<-ctx.Done()
	close(s.done)
	return pipeline.FanOutTurnResult{}, nil
}

func (s *a2MapFanOutSelector) ReportFanOutServingError(_ context.Context, err error) {
	select {
	case s.errors <- err:
	default:
	}
}

type a2MapFanOutExecution struct {
	selected gateRecoveryStoreCase
	ctx      context.Context
	runID    string
	source   semanticview.Source
	fact     correlation.SourceArtifactFact
	work     *worklifetime.RuntimeOccurrence
	bus      *runtimebus.EventBus
	probe    *lifecycleprobe.Probe
	module   proposedEffectProofModule
	pc       *pipeline.PipelineCoordinator
	grant    startupownership.LiveGenerationGrant
	schedule *genericschedule.Lifecycle
	driver   *exactJoinScheduleDriver
}

func a2MapFanOutFiles(gather bool) map[string]string {
	files := map[string]string{
		"schema.yaml": `name: a2-map-fan-out
stages:
  active: {initial: true}
  awaiting: {}
  ready: {terminal: true}
  attention: {terminal: true}
pins:
  inputs:
    events: [seed, batch.ready, batch.replace]
  outputs:
    events: [item.ready]
`,
		"entities.yaml": "work:\n  items:\n    type: map[text][integer]\n    initial: {before: [999]}\n",
		"events.yaml": `seed:
batch.ready:
  items: map[text][integer]
batch.replace:
  items: map[text][integer]
item.ready:
  member_id: text
  index: integer
  count: integer
  result: "[integer]"
`,
		"nodes.yaml": `writer:
  execution_type: system_node
  event_handlers:
    seed: {}
    batch.ready:
      data_accumulation:
        writes: [{target_field: items, value: "${payload.items}"}]
      fan_out:
        items_from: entity.items
        as: key
        emit:
          event: item.ready
          fields:
            member_id: "${key}"
            index: "${fan_out.index}"
            count: "${fan_out.count}"
            result: "${payload.items[?key].orValue([])}"
    batch.replace:
      data_accumulation:
        writes: [{target_field: items, value: "${payload.items}"}]
`,
	}
	if gather {
		files["nodes.yaml"] = strings.Replace(files["nodes.yaml"], "    batch.ready:\n", "    batch.ready:\n      advances_to: awaiting\n", 1)
		files["nodes.yaml"] += `collector:
  execution_type: system_node
  event_handlers:
    item.ready:
      join:
        stage: awaiting
        members: {from: state.items, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        on_complete: {advances_to: ready}
        on_deadline: {advances_to: attention}
`
	}
	return files
}

func newA2MapFanOutExecution(t *testing.T, selected gateRecoveryStoreCase, gather bool) *a2MapFanOutExecution {
	t.Helper()
	return newA2MapFanOutExecutionFromFiles(t, selected, gather, a2MapFanOutFiles(gather))
}

func newA2MapFanOutExecutionFromFiles(t *testing.T, selected gateRecoveryStoreCase, gather bool, files map[string]string) *a2MapFanOutExecution {
	t.Helper()
	bundle := loadPipelineLifecycleFixtureBundle(t, files)
	source := semanticview.Wrap(bundle)
	fact := mustAuthorActivityTestSourceArtifactFactForHash(bundle.SourceArtifact.BundleHash())
	runID, runtimeID := uuid.NewString(), uuid.NewString()
	ctx := correlation.WithSourceArtifactFact(correlation.WithRunID(context.Background(), runID), fact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, fact.BundleHash()))
	process := worklifetime.NewProcess()
	work, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: runtimeID, BundleHash: fact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := work.RetireAndWait(deadline); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(deadline); err != nil {
			t.Error(err)
		}
	})
	ctx = withLiveGateExecution(worklifetime.WithOccurrence(ctx, work))
	fixture := runlifecyclefixture.Fixture{RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact}
	if selected.postgres {
		runlifecyclefixture.RequirePostgres(t, ctx, selected.db, fixture)
	} else {
		runlifecyclefixture.RequireSQLite(t, ctx, selected.db, fixture)
	}
	descriptors, err := swarmruntime.AuthorActivityEventDescriptors(source)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := selected.events.RegisterAuthorActivityEventCatalog(authoractivity.BundleScope(runtimeID, fact.BundleHash()), descriptors)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.Release)
	admitter := swarmruntime.NewRuntimePayloadAdmitter(nil, source, fact)
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(fact, runtimeID, 1)
	if err != nil {
		t.Fatal(err)
	}
	selected.events.(swarmruntime.EventPayloadAdmissionBinder).SetEventPayloadAdmitter(admitter)
	probe := lifecycleprobe.New()
	var scheduleEvents []string
	if gather {
		scheduleEvents = []string{"platform.join_complete", "platform.join_timeout"}
	}
	bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: fact, RuntimeInstanceID: runtimeID,
		WorkOwner: work, PayloadAdmitter: admitter, TestLifecycleProbe: probe, DeliveryAuthority: authority,
	}, scheduleEvents...)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := pipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	p := &a2MapFanOutExecution{selected: selected, ctx: ctx, runID: runID, source: source, fact: fact,
		work: work, bus: bus, probe: probe, module: proposedEffectProofModule{source: source, nodes: nodes}}
	if gather {
		p.schedule, p.driver = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
	}
	p.restart(t)
	capability, err := selected.events.(interface {
		AcquireProcessCapability(context.Context, startupownership.AcquireRequest) (startupownership.ProcessCapability, error)
	}).AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "a2-map-fan-out", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	p.grant, err = capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fact.BundleHash(), RuntimeInstanceID: runtimeID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	if err := selected.events.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	continuations, err := deliverycontinuation.New(selected.events, selected.events, authority, work, bus, func(_ context.Context, err error) {
		t.Errorf("actual map delivery continuation: %v", err)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.SetDeliveryContinuationOwner(continuations); err != nil {
		t.Fatal(err)
	}
	if err := continuations.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := continuations.Retire(deadline); err != nil {
			t.Error(err)
		}
	})
	commitKeylessConstructorComponent(t, ctx, selected, p.pc, source)
	p.publish(t, "seed", nil)
	return p
}

func (p *a2MapFanOutExecution) restart(t *testing.T) {
	t.Helper()
	selected := p.selected
	if p.pc != nil {
		// Both backend fixtures provide an independently constructed store over
		// the same full schema. Do not carry coordinator or state caches forward.
		var ok bool
		selected.events, ok = selected.trace.(gateRecoverySelectedStore)
		if !ok {
			t.Fatal("map proof lacks reconstructed selected-store handle")
		}
		selected.events.(swarmruntime.EventPayloadAdmissionBinder).SetEventPayloadAdmitter(swarmruntime.NewRuntimePayloadAdmitter(nil, p.source, p.fact))
		selected.persistence = pipeline.NewWorkflowPersistence(selected.events.(pipeline.WorkflowPersistenceOwner))
	}
	p.pc = newGateRecoveryCoordinator(p.bus, selected, pipeline.PipelineCoordinatorOptions{
		Module: p.module, SourceArtifactFact: p.fact, WorkOwner: p.work,
		GenericSchedules: p.schedule, TestLifecycleProbe: p.probe,
	})
	if p.pc == nil {
		t.Fatal("compiled map coordinator construction failed")
	}
	p.bus.SetInterceptors(p.pc)
}

func (p *a2MapFanOutExecution) publish(t *testing.T, name string, items map[string]any) events.Event {
	t.Helper()
	payload := map[string]any{}
	if items != nil {
		payload["items"] = items
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(name), "operator", "", raw, 0,
		p.runID, events.EnvelopeForEntityID(events.EventEnvelope{}, p.runID), eventtest.RootRoutingSource(p.runID), time.Now().UTC())
	if err := p.bus.PublishAcknowledged(p.ctx, event); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
	defer cancel()
	writer := externalPipelineSourceNode(t, p.source, ".", "writer")
	completed, err := p.probe.WaitForHandlerCompleted(waitCtx, event.ID(), writer.Key())
	if err != nil || completed.Status != "completed" {
		t.Fatalf("actual %s handler: status=%s err=%v", name, completed.Status, err)
	}
	if err := p.bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatal(err)
	}
	assertExactJoinDeliveryStatus(t, p.selected, p.ctx, event.ID(), writer.Key(), "delivered")
	return event
}

func (p *a2MapFanOutExecution) handoff(t *testing.T) a2MapFanOutHandoff {
	t.Helper()
	selector := &a2MapFanOutSelector{selected: make(chan a2MapFanOutHandoff, 1), errors: make(chan error, 1), done: make(chan struct{})}
	workers := 1
	registration, err := startupownership.StartFanOutServing(p.ctx, p.grant, p.work, &workers, selector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	select {
	case handoff := <-selector.selected:
		registration.Close()
		<-selector.done
		return handoff
	case err := <-selector.errors:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("selected-store selector did not hand off compiled map intent")
	}
	return a2MapFanOutHandoff{}
}

func (p *a2MapFanOutExecution) instance(t *testing.T) pipeline.WorkflowInstance {
	t.Helper()
	instance, found, err := p.pc.Load(p.ctx, testRunScopedWorkflowInstanceForRun(p.runID, p.runID))
	if err != nil || !found {
		t.Fatalf("load map state: found=%v err=%v", found, err)
	}
	return instance
}

type a2MapFanOutOutput struct {
	Ordinal int
	EventID string
	Payload struct {
		Key    string  `json:"member_id"`
		Index  int     `json:"index"`
		Count  int     `json:"count"`
		Result []int64 `json:"result"`
	}
}

func (p *a2MapFanOutExecution) outputs(t *testing.T) []a2MapFanOutOutput {
	t.Helper()
	rows, err := p.selected.db.QueryContext(p.ctx, `SELECT o.ordinal, o.event_id, e.payload, o.outcome_kind
		FROM fan_out_outcomes o JOIN events e ON e.event_id=o.event_id WHERE o.run_id=$1 ORDER BY o.ordinal`, p.runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var outputs []a2MapFanOutOutput
	for rows.Next() {
		var output a2MapFanOutOutput
		var payload []byte
		var kind string
		if err := rows.Scan(&output.Ordinal, &output.EventID, &payload, &kind); err != nil {
			t.Fatal(err)
		}
		if kind != "committed" {
			t.Fatalf("ordinal %d outcome=%s", output.Ordinal, kind)
		}
		if err := json.Unmarshal(payload, &output.Payload); err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, output)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return outputs
}

func (p *a2MapFanOutExecution) assertProgress(t *testing.T, cursor, cardinality int, status string, sourceMutation string) {
	t.Helper()
	var intents, outcomes, eventsCount, gotCursor, gotCardinality int
	var gotStatus, mutation string
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, p.runID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT cursor,cardinality,status,source_mutation_id FROM fan_out_intents WHERE run_id=$1`, p.runID).Scan(&gotCursor, &gotCardinality, &gotStatus, &mutation); err != nil {
		t.Fatal(err)
	}
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, p.runID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='item.ready'`, p.runID).Scan(&eventsCount); err != nil {
		t.Fatal(err)
	}
	if intents != 1 || outcomes != cursor || eventsCount != cursor || gotCursor != cursor || gotCardinality != cardinality || gotStatus != status || mutation != sourceMutation {
		t.Fatalf("durable progress: intents=%d outcomes=%d events=%d cursor=%d cardinality=%d status=%s mutation=%s; want %d/%d/%s/%s",
			intents, outcomes, eventsCount, gotCursor, gotCardinality, gotStatus, mutation, cursor, cardinality, status, sourceMutation)
	}
}

// These are compiled pipeline component receipts, not served/boot/CLI proofs.
// All successful source revisions, publications, settlements and continuations
// are produced by the existing execution lifecycle; SQL below is read-only.
func TestA2MapFanOutCompositeLifecycleOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, proof := range []struct {
			name string
			run  func(*testing.T, gateRecoveryStoreCase)
		}{
			{"map_to_list_duplicate_ordinals", runA2MapToListDuplicateOrdinals},
			{"zero_map_exact_continuation", runA2ZeroMapExactContinuation},
			{"later_entries_same_keys_new_values", runA2MapLaterEntries},
		} {
			t.Run(backend.name+"/"+proof.name, func(t *testing.T) { proof.run(t, backend.open(t)) })
		}
	}
}

type a2CompositeMapReceipt struct {
	key         fanoutobligation.IntentKey
	plan        contracts.FanOutCompiledPlan
	source      fanoutobligation.SourceRef
	capsule     fanoutobligation.Capsule
	cardinality int
	cursor      int
	status      string
}

func (p *a2MapFanOutExecution) compositeReceipt(t *testing.T, trigger events.Event, nodeName string, wantSource any) a2CompositeMapReceipt {
	t.Helper()
	node := externalPipelineSourceNode(t, p.source, ".", nodeName)
	plans := p.source.FanOutPlansForHandler(node, string(trigger.Type()))
	if len(plans) != 1 {
		t.Fatalf("compiled %s source plans = %#v", nodeName, plans)
	}
	r := a2CompositeMapReceipt{plan: plans[0]}
	r.key.RunID, r.key.ElementRef = p.runID, r.plan.Ref.ElementRef
	var capsule []byte
	var digest string
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT i.triggering_delivery_id, i.semantic_digest, i.capsule,
		i.source_kind, COALESCE(CAST(i.source_event_id AS TEXT),''), COALESCE(CAST(i.source_run_id AS TEXT),''), COALESCE(CAST(i.source_entity_id AS TEXT),''),
		i.source_field, COALESCE(CAST(i.source_mutation_id AS TEXT),''), i.cardinality, i.cursor, i.status
		FROM fan_out_intents i JOIN event_deliveries d ON d.delivery_id=i.triggering_delivery_id
		WHERE i.run_id=$1 AND d.event_id=$2 AND d.subscriber_type='node' AND d.subscriber_id=$3
		AND i.flow_path=$4 AND i.declaration_family=$5 AND i.semantic_path=$6`,
		p.runID, trigger.ID(), node.Key(), r.key.ElementRef.FlowPath, r.key.ElementRef.Family, r.key.ElementRef.SemanticPath).
		Scan(&r.key.TriggeringDeliveryID, &digest, &capsule, &r.source.Kind, &r.source.EventID, &r.source.RunID,
			&r.source.EntityID, &r.source.Field, &r.source.MutationID, &r.cardinality, &r.cursor, &r.status); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(capsule, &r.capsule); err != nil {
		t.Fatal(err)
	}
	if digest != r.plan.Ref.SemanticDigest || !reflect.DeepEqual(r.capsule.SourceProjection, r.plan.SemanticEvidence()) ||
		r.capsule.Lineage.ParentEventID != trigger.ID() {
		t.Fatalf("retained source disagrees with actual compiled declaration/trigger: %#v", r)
	}
	if err := r.capsule.SourceProjection.Validate(r.plan.Ref); err != nil {
		t.Fatal(err)
	}
	if err := r.source.Validate(true); err != nil {
		t.Fatal(err)
	}
	var frozen []byte
	switch r.source.Kind {
	case fanoutobligation.SourceEntityField:
		if r.source.RunID != p.runID || r.source.EntityID != p.runID || r.source.Field != "items" || !r.plan.SourceAfterWrites {
			t.Fatalf("writer did not retain its exact post-write field: %#v", r.source)
		}
		if err := p.selected.db.QueryRowContext(p.ctx, `SELECT new_value FROM entity_mutations
			WHERE mutation_id=$1 AND run_id=$2 AND entity_id=$3 AND domain='authored_field' AND path=$4`,
			r.source.MutationID, r.source.RunID, r.source.EntityID, r.source.Field).Scan(&frozen); err != nil {
			t.Fatal(err)
		}
	case fanoutobligation.SourceEventPayloadField:
		if r.source.EventID != trigger.ID() || r.source.Field != "result" {
			t.Fatalf("nested list did not retain its exact parent publication: %#v", r.source)
		}
		prepared, found, err := p.selected.events.LoadPreparedPublishEvent(p.ctx, r.source.EventID)
		if err != nil || !found {
			t.Fatalf("retained list source publication: found=%v err=%v", found, err)
		}
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(prepared.Event.Event().Payload(), &payload); err != nil {
			t.Fatal(err)
		}
		frozen = payload[r.source.Field]
	default:
		t.Fatalf("unexpected authored source: %#v", r.source)
	}
	want, err := json.Marshal(wantSource)
	if err != nil || a2AccumulatorJSONHash(t, frozen) != a2AccumulatorJSONHash(t, want) {
		t.Fatalf("retained source changed: got=%s want=%s err=%v", frozen, want, err)
	}
	return r
}

func (p *a2MapFanOutExecution) pumpCompositeReceipt(t *testing.T, owner pipeline.FanOutObligationOwner, r a2CompositeMapReceipt, wantItems []any) {
	t.Helper()
	intent, claim, found, err := owner.ClaimFanOutIntent(p.ctx, pipeline.FanOutClaimRequest{
		Owner: "a2-composite-map-readback", BundleHash: p.fact.BundleHash(), Candidate: &r.key, Now: time.Now().UTC(), Lease: time.Minute,
	})
	if err != nil || !found || intent.Request.PlanRef != r.plan.Ref || intent.Source != r.source ||
		intent.Request.Cardinality != len(wantItems) || !reflect.DeepEqual(intent.Request.Capsule.SourceProjection, r.plan.SemanticEvidence()) {
		t.Fatalf("claim actual compiled source: key=%#v ref=%#v source=%#v found=%v err=%v", r.key, intent.Request.PlanRef, intent.Source, found, err)
	}
	input, err := owner.LoadFanOutEvaluation(p.ctx, claim)
	actual, _ := json.Marshal(input.Items)
	want, _ := json.Marshal(wantItems)
	if err != nil || input.StartOrdinal != 0 || a2AccumulatorJSONHash(t, actual) != a2AccumulatorJSONHash(t, want) {
		t.Fatalf("real immutable source reader: %#v want=%s err=%v", input, want, err)
	}
	if settlement, err := owner.ReleaseFanOutClaim(p.ctx, claim); err != nil || !settlement.Acknowledged {
		t.Fatalf("release source readback claim: %#v err=%v", settlement, err)
	}
	if _, err := p.pc.ServeFanOutCandidate(p.ctx, owner, r.key); err != nil {
		failure, _ := failures.EnvelopeFromError(err)
		t.Fatalf("actual composite fan-out pump: %v failure=%+v", err, failure)
	}
	waitForGateRecoveryQuiescence(t, p.bus, p.ctx)
}

func (p *a2MapFanOutExecution) compositePublication(t *testing.T, eventID, eventName string, wantPayload map[string]any, nodeName string, arm *joinruntime.Activation) events.Event {
	t.Helper()
	prepared, found, err := p.selected.events.LoadPreparedPublishEvent(p.ctx, eventID)
	if err != nil || !found {
		t.Fatalf("durable publication: found=%v err=%v", found, err)
	}
	event := prepared.Event.Event()
	want, _ := json.Marshal(wantPayload)
	if string(event.Type()) != eventName || a2AccumulatorJSONHash(t, event.Payload()) != a2AccumulatorJSONHash(t, want) {
		t.Fatalf("publication: got=%s/%s want=%s/%s", event.Type(), event.Payload(), eventName, want)
	}
	node := externalPipelineSourceNode(t, p.source, ".", nodeName)
	bound := 0
	for _, route := range prepared.DeliveryRoutes {
		if route.Recipient.ID() != node.Key() {
			continue
		}
		if arm == nil && len(route.Context.Joins) == 0 {
			bound++
		} else if arm != nil && len(route.Context.Joins) == 1 &&
			route.Context.Joins[0].Disposition == events.JoinAdmissionBound && route.Context.Joins[0].Ref.Equal(arm.JoinRef()) {
			bound++
		}
	}
	if bound != 1 {
		t.Fatalf("publication lacks exact recipient/stage-entry binding: %#v", prepared.DeliveryRoutes)
	}
	assertExactJoinDeliveryStatus(t, p.selected, p.ctx, eventID, node.Key(), "delivered")
	assertExactJoinDeliveryCount(t, p.selected, p.ctx, eventID, node.Key(), 1)
	return event
}

func (p *a2MapFanOutExecution) compositeOutputs(t *testing.T, r a2CompositeMapReceipt, eventName string, wantPayloads []map[string]any, nodeName string, arm *joinruntime.Activation) []events.Event {
	t.Helper()
	rows, err := p.selected.db.QueryContext(p.ctx, `SELECT o.ordinal, o.event_id, o.outcome_kind FROM fan_out_outcomes o
		WHERE o.run_id=$1 AND o.triggering_delivery_id=$2 AND o.flow_path=$3 AND o.declaration_family=$4 AND o.semantic_path=$5
		ORDER BY o.ordinal`, p.runID, r.key.TriggeringDeliveryID, r.key.ElementRef.FlowPath, r.key.ElementRef.Family, r.key.ElementRef.SemanticPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var outputs []events.Event
	seen := map[string]bool{}
	for rows.Next() {
		var ordinal int
		var eventID, kind string
		if err := rows.Scan(&ordinal, &eventID, &kind); err != nil {
			t.Fatal(err)
		}
		if ordinal != len(outputs) || ordinal >= len(wantPayloads) || kind != "committed" || seen[eventID] {
			t.Fatalf("lost independent ordinal publication: ordinal=%d kind=%s event=%s", ordinal, kind, eventID)
		}
		seen[eventID] = true
		event := p.compositePublication(t, eventID, eventName, wantPayloads[ordinal], nodeName, arm)
		if event.ParentEventID() != r.capsule.Lineage.ParentEventID {
			t.Fatalf("ordinal %d lost its exact parent lineage: %s", ordinal, event.ParentEventID())
		}
		outputs = append(outputs, event)
	}
	if err := rows.Err(); err != nil || len(outputs) != len(wantPayloads) {
		t.Fatalf("committed ordinal count=%d want=%d err=%v", len(outputs), len(wantPayloads), err)
	}
	var cursor, cardinality int
	var status string
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT cursor,cardinality,status FROM fan_out_intents
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`,
		p.runID, r.key.TriggeringDeliveryID, r.key.ElementRef.FlowPath, r.key.ElementRef.Family, r.key.ElementRef.SemanticPath).
		Scan(&cursor, &cardinality, &status); err != nil || cursor != len(outputs) || cardinality != len(outputs) || status != "closed" {
		t.Fatalf("durable intent progress=%d/%d/%s err=%v", cursor, cardinality, status, err)
	}
	return outputs
}

func (p *a2MapFanOutExecution) compositeArm(t *testing.T, entry timeridentity.StageEntryRef, nodeName string) joinruntime.Activation {
	t.Helper()
	node := externalPipelineSourceNode(t, p.source, ".", nodeName)
	var matches []joinruntime.Activation
	for _, arm := range a2KnownTargetArms(t, p.instance(t)) {
		if arm.JoinRef().Node().Equal(node) && arm.JoinRef().StageEntry() == entry {
			matches = append(matches, arm)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("exact %s stage-entry arms=%#v entry=%#v", nodeName, matches, entry)
	}
	return matches[0]
}

func (p *a2MapFanOutExecution) compositeEntry(t *testing.T, trigger events.Event) timeridentity.StageEntryRef {
	t.Helper()
	instance := p.instance(t)
	entry, found, err := workflowlifecycle.LoadStageEntry(instance.Bookkeeping)
	if err != nil || !found || entry.EventID != trigger.ID() || entry.Stage != "awaiting" || entry.Cause != "delivery" ||
		entry.OccurrenceID == "" || entry.TransitionID == "" || instance.CurrentState != "awaiting" {
		t.Fatalf("actual delivered stage entry=%#v found=%v err=%v state=%s", entry, found, err, instance.CurrentState)
	}
	if err := entry.RequireOwner(p.runID, testRunScopedWorkflowInstanceForRun(p.runID, p.runID).Route.ScopeKey,
		instance.InstanceID, instance.StorageRef, instance.EntityID, "awaiting"); err != nil {
		t.Fatal(err)
	}
	return entry
}

func assertA2CompositeResults(t *testing.T, arm joinruntime.Activation, want any, count int) {
	t.Helper()
	results, err := arm.Results()
	actual, _ := json.Marshal(results)
	expected, _ := json.Marshal(want)
	if err != nil || arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonComplete ||
		arm.Completed() != count || arm.Expected() != count || a2AccumulatorJSONHash(t, actual) != a2AccumulatorJSONHash(t, expected) {
		t.Fatalf("exact ordered results=%s want=%s arm=%#v err=%v", actual, expected, arm, err)
	}
}

func (p *a2MapFanOutExecution) compositeContinuation(t *testing.T, pending genericschedule.Activation, initial joinruntime.Activation) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var eventID string
	for time.Now().Before(deadline) {
		activation, found, err := p.selected.events.(genericschedule.Store).LoadGenericScheduleActivation(p.ctx, pending.ID)
		if err != nil || !found {
			t.Fatalf("read exact continuation schedule: found=%v err=%v", found, err)
		}
		if activation.CurrentEventID != "" && activation.Status == genericschedule.StatusFired {
			eventID = activation.CurrentEventID
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if eventID == "" {
		t.Fatal("exact continuation schedule did not fire")
	}
	waitCtx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
	defer cancel()
	completed, err := p.probe.WaitForHandlerCompleted(waitCtx, eventID, initial.JoinRef().Node().Key())
	if err != nil || completed.Status != "completed" {
		t.Fatalf("actual exact join continuation: status=%s err=%v", completed.Status, err)
	}
	if err := p.bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatal(err)
	}
	assertExactJoinDeliveryStatus(t, p.selected, p.ctx, eventID, initial.JoinRef().Node().Key(), "delivered")
	assertExactJoinDeliveryCount(t, p.selected, p.ctx, eventID, initial.JoinRef().Node().Key(), 1)
	assertExactJoinFiredSchedule(t, p.selected, p.ctx, pending, eventID)
	for _, arm := range a2KnownTargetArms(t, p.instance(t)) {
		if arm.JoinRef().Equal(initial.JoinRef()) {
			if !arm.OutcomeFired || arm.OutcomePending || arm.CloseReason != joinruntime.CloseReasonComplete {
				t.Fatalf("continuation did not fire exact retained join: %#v", arm)
			}
			return eventID
		}
	}
	t.Fatal("continuation lost original exact arm")
	return ""
}

func runA2MapToListDuplicateOrdinals(t *testing.T, selected gateRecoveryStoreCase) {
	files := a2MapFanOutFiles(false)
	files["schema.yaml"] = strings.Replace(files["schema.yaml"], "events: [item.ready]", "events: [item.ready, leaf.ready]", 1)
	files["entities.yaml"] += `  leaf_results:
    type: "[integer]"
    initial: []
`
	files["events.yaml"] += `leaf.ready:
  member_id: text
  parent_key: text
  parent_index: integer
  index: integer
  count: integer
  value: integer
`
	files["nodes.yaml"] = strings.Replace(files["nodes.yaml"], "    batch.ready:\n", "    batch.ready:\n      advances_to: awaiting\n", 1)
	files["nodes.yaml"] += `expander:
  execution_type: system_node
  event_handlers:
    item.ready:
      fan_out:
        items_from: payload.result
        as: entry
        emit:
          event: leaf.ready
          fields:
            member_id: "${payload.member_id + ':' + string(fan_out.index)}"
            parent_key: "${payload.member_id}"
            parent_index: "${payload.index}"
            index: "${fan_out.index}"
            count: "${fan_out.count}"
            value: "${entry}"
leaf-collector:
  execution_type: system_node
  event_handlers:
    leaf.ready:
      join:
        stage: awaiting
        members: {count: 6, by: payload.member_id}
        output: payload.value
        deadline: {after: 1h, from: stage_entry}
        on_complete:
          advances_to: ready
          data_accumulation:
            writes: [{target_field: leaf_results, value: "${join.results}"}]
        on_deadline: {advances_to: attention}
`
	p := newA2MapFanOutExecutionFromFiles(t, selected, true, files)
	original := map[string]any{"A": []int64{11, 11, 12}, "a": []int64{21, 21, 22}}
	trigger := p.publish(t, "batch.ready", original)
	entry := p.compositeEntry(t, trigger)
	leafArm := p.compositeArm(t, entry, "leaf-collector")
	if len(a2KnownTargetArms(t, p.instance(t))) != 1 || leafArm.Completed() != 0 || leafArm.Expected() != 6 {
		t.Fatal("nested execution did not arm its exact stage-entry gather")
	}
	p.publish(t, "batch.replace", map[string]any{"replacement": []int64{999}})
	p.restart(t)
	r := p.compositeReceipt(t, trigger, "writer", original)
	handoff := p.handoff(t)
	if handoff.key != r.key {
		t.Fatalf("actual selector picked foreign map intent: %#v want=%#v", handoff.key, r.key)
	}
	p.pumpCompositeReceipt(t, handoff.owner, r, []any{"A", "a"})
	parents := p.compositeOutputs(t, r, "item.ready", []map[string]any{
		{"member_id": "A", "index": 0, "count": 2, "result": original["A"]},
		{"member_id": "a", "index": 1, "count": 2, "result": original["a"]},
	}, "expander", nil)
	children := make([]a2CompositeMapReceipt, len(parents))
	for index, parent := range parents {
		children[index] = p.compositeReceipt(t, parent, "expander", original[[]string{"A", "a"}[index]])
	}
	seenLeaves := map[string]bool{}
	seenChildren := map[fanoutobligation.IntentKey]bool{}
	for turn := range children {
		childHandoff := p.handoff(t)
		parentIndex := -1
		for index, child := range children {
			if child.key == childHandoff.key {
				parentIndex = index
			}
		}
		if parentIndex < 0 || seenChildren[childHandoff.key] {
			t.Fatalf("nested selector returned a foreign/repeated child: %#v", childHandoff.key)
		}
		seenChildren[childHandoff.key] = true
		key := []string{"A", "a"}[parentIndex]
		values := original[key].([]int64)
		child := children[parentIndex]
		p.pumpCompositeReceipt(t, childHandoff.owner, child, []any{values[0], values[1], values[2]})
		want := make([]map[string]any, len(values))
		for ordinal, value := range values {
			want[ordinal] = map[string]any{"member_id": fmt.Sprintf("%s:%d", key, ordinal), "parent_key": key, "parent_index": parentIndex, "index": ordinal, "count": 3, "value": value}
		}
		for _, leaf := range p.compositeOutputs(t, child, "leaf.ready", want, "leaf-collector", &leafArm) {
			if seenLeaves[leaf.ID()] {
				t.Fatal("nested duplicate values reused a publication identity across parents")
			}
			seenLeaves[leaf.ID()] = true
		}
		partial := p.compositeArm(t, entry, "leaf-collector")
		if partial.Completed() != (turn+1)*3 || !partial.JoinRef().Equal(leafArm.JoinRef()) ||
			(turn == 0 && partial.Status != joinruntime.StatusOpen) {
			t.Fatalf("nested list ordinals did not independently settle into the exact gather: %#v", partial)
		}
	}
	leafCompleted := p.compositeArm(t, entry, "leaf-collector")
	assertA2CompositeResults(t, leafCompleted, []int64{11, 11, 12, 21, 21, 22}, 6)
	leafPending := exactJoinPendingSchedule(t, selected, p.ctx, leafCompleted)
	if err := p.driver.Resume(p.ctx); err != nil {
		t.Fatal(err)
	}
	p.compositeContinuation(t, leafPending, leafCompleted)
	terminal := waitForExactJoinState(t, p.ctx, p.pc, flowidentity.RouteForInstancePath(p.runID), "ready")
	actual, _ := json.Marshal(terminal.Fields["leaf_results"])
	expected, _ := json.Marshal([]int64{11, 11, 12, 21, 21, 22})
	if a2AccumulatorJSONHash(t, actual) != a2AccumulatorJSONHash(t, expected) {
		t.Fatalf("actual nested continuation lost list multiplicity/order: %s", actual)
	}
}

func runA2ZeroMapExactContinuation(t *testing.T, selected gateRecoveryStoreCase) {
	p := newA2MapFanOutExecution(t, selected, true)
	empty := map[string]any{}
	trigger := p.publish(t, "batch.ready", empty)
	entry := p.compositeEntry(t, trigger)
	r := p.compositeReceipt(t, trigger, "writer", empty)
	arm := p.compositeArm(t, entry, "collector")
	assertA2CompositeResults(t, arm, []any{}, 0)
	if len(arm.Members) != 0 || r.cardinality != 0 || r.cursor != 0 || r.status != "closed" || !arm.OutcomePending || arm.OutcomeFired {
		t.Fatalf("actual zero-map did not close exactly without outputs: receipt=%#v arm=%#v", r, arm)
	}
	p.publish(t, "batch.replace", map[string]any{"later": []int64{99}})
	p.restart(t)
	p.compositeReceipt(t, trigger, "writer", empty)
	selector := &a2MapFanOutSelector{selected: make(chan a2MapFanOutHandoff, 1), errors: make(chan error, 1), done: make(chan struct{})}
	workers := 1
	registration, err := startupownership.StartFanOutServing(p.ctx, p.grant, p.work, &workers, selector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	scanned := make(chan bool, 1)
	registration.SetTestScanObserver(func(_ startupownership.FanOutCandidate, found bool, err error) {
		if err != nil {
			selector.ReportFanOutServingError(p.ctx, err)
			return
		}
		select {
		case scanned <- found:
		default:
		}
	})
	registration.Wake()
	select {
	case found := <-scanned:
		if found {
			t.Fatal("zero-map serving scan found publication work")
		}
	case err := <-selector.errors:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("actual zero-map serving scan did not complete")
	}
	registration.Close()
	select {
	case <-selector.selected:
		t.Fatal("zero-map incorrectly handed off a pump candidate")
	default:
	}
	p.assertProgress(t, 0, 0, "closed", r.source.MutationID)
	pending := exactJoinPendingSchedule(t, selected, p.ctx, arm)
	if err := p.driver.Resume(p.ctx); err != nil {
		t.Fatal(err)
	}
	p.compositeContinuation(t, pending, arm)
	terminal := waitForExactJoinState(t, p.ctx, p.pc, flowidentity.RouteForInstancePath(p.runID), "ready")
	assertA2CompositeResults(t, exactJoinPersistedArm(t, terminal), []any{}, 0)
	p.assertProgress(t, 0, 0, "closed", r.source.MutationID)
}

func runA2MapLaterEntries(t *testing.T, selected gateRecoveryStoreCase) {
	files := a2MapFanOutFiles(true)
	// Completing this stage is intentionally nonterminal: re-entry is legal
	// workflow execution, not new ingress into a completed run.
	files["schema.yaml"] = strings.Replace(files["schema.yaml"], "ready: {terminal: true}", "ready: {}", 1)
	p := newA2MapFanOutExecutionFromFiles(t, selected, true, files)
	var previous joinruntime.Activation
	var previousReceipt a2CompositeMapReceipt
	var previousContinuation string
	for index, original := range []map[string]any{
		{"A": []int64{11, 11, 12}, "a": []int64{21, 21, 22}},
		{"A": []int64{31, 31, 32}, "a": []int64{41, 41, 42}},
	} {
		p.driver.mu.Lock()
		p.driver.started = false
		p.driver.mu.Unlock()
		trigger := p.publish(t, "batch.ready", original)
		entry := p.compositeEntry(t, trigger)
		arm := p.compositeArm(t, entry, "collector")
		if !reflect.DeepEqual(arm.Members, []string{"A", "a"}) || arm.Completed() != 0 || arm.Status != joinruntime.StatusOpen ||
			len(a2KnownTargetArms(t, p.instance(t))) != index+1 {
			t.Fatalf("later entry did not independently snapshot reused keys: %#v", arm)
		}
		p.publish(t, "batch.replace", map[string]any{"A": []int64{991}, "a": []int64{992}})
		p.restart(t)
		r := p.compositeReceipt(t, trigger, "writer", original)
		if index > 0 && (entry == previous.JoinRef().StageEntry() || entry.EventID == previous.JoinRef().StageEntry().EventID ||
			entry.OccurrenceID == previous.JoinRef().StageEntry().OccurrenceID || entry.TransitionID == previous.JoinRef().StageEntry().TransitionID ||
			r.source.MutationID == previousReceipt.source.MutationID || r.key.TriggeringDeliveryID == previousReceipt.key.TriggeringDeliveryID) {
			t.Fatal("later stage entry reused prior lifecycle/source identity")
		}
		handoff := p.handoff(t)
		if handoff.key != r.key {
			t.Fatalf("later entry selector picked wrong intent: %#v want=%#v", handoff.key, r.key)
		}
		p.pumpCompositeReceipt(t, handoff.owner, r, []any{"A", "a"})
		p.compositeOutputs(t, r, "item.ready", []map[string]any{
			{"member_id": "A", "index": 0, "count": 2, "result": original["A"]},
			{"member_id": "a", "index": 1, "count": 2, "result": original["a"]},
		}, "collector", &arm)
		completed := p.compositeArm(t, entry, "collector")
		assertA2CompositeResults(t, completed, []any{original["A"], original["a"]}, 2)
		if index > 0 {
			retained := p.compositeArm(t, previous.JoinRef().StageEntry(), "collector")
			if !reflect.DeepEqual(retained, previous) {
				t.Fatal("later same-key contributions changed the earlier exact snapshot/results")
			}
		}
		pending := exactJoinPendingSchedule(t, selected, p.ctx, completed)
		if err := p.driver.Resume(p.ctx); err != nil {
			t.Fatal(err)
		}
		continuation := p.compositeContinuation(t, pending, completed)
		waitForExactJoinState(t, p.ctx, p.pc, flowidentity.RouteForInstancePath(p.runID), "ready")
		if continuation == previousContinuation {
			t.Fatal("later entry reused the prior exact continuation publication")
		}
		previous, previousReceipt, previousContinuation = p.compositeArm(t, entry, "collector"), r, continuation
		assertA2CompositeResults(t, previous, []any{original["A"], original["a"]}, 2)
	}
}

func TestA2MapFanOutCompiledExecutionImmutableSourceOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, gather := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/gather=%t", backend.name, gather), func(t *testing.T) {
				runA2MapFanOutCompiledImmutableSource(t, backend.open(t), gather, []string{"A", "a", "z"})
			})
		}
	}
}

func TestA2MapFanOutCompiledWhitespaceKeysPreservedOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, gather := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/gather=%t", backend.name, gather), func(t *testing.T) {
				// Colliding trimmed forms must remain distinct business keys, including
				// the final z/z-space pair in the second durable chunk.
				runA2MapFanOutCompiledImmutableSource(t, backend.open(t), gather, []string{" a ", "a", "a ", "z", "z ", " ", "\t"})
			})
		}
	}
}

func runA2MapFanOutCompiledImmutableSource(t *testing.T, selected gateRecoveryStoreCase, gather bool, keys []string) {
	t.Helper()
	p := newA2MapFanOutExecution(t, selected, gather)
	original, replacement := map[string]any{}, map[string]any{}
	for index := 0; index < fanoutobligation.MaxChunkSize; index++ {
		keys = append(keys, fmt.Sprintf("key-%02d", index))
	}
	for index, key := range keys {
		original[key] = []int64{int64(index), int64(index), int64(index + 100)}
		replacement[fmt.Sprintf("replacement-%02d", index)] = []int64{int64(1000 + index)}
	}
	sort.Strings(keys)
	trigger := p.publish(t, "batch.ready", original)
	prepared, found, err := p.selected.events.LoadPreparedPublishEvent(p.ctx, trigger.ID())
	if err != nil || !found {
		t.Fatalf("retained map publication: found=%v err=%v", found, err)
	}
	var admitted struct {
		Items map[string][]int64 `json:"items"`
	}
	if err := json.Unmarshal(prepared.Event.Event().Payload(), &admitted); err != nil {
		t.Fatal(err)
	}
	admittedRaw, err := json.Marshal(admitted.Items)
	wantWritten, _ := json.Marshal(original)
	if err != nil || string(admittedRaw) != string(wantWritten) {
		t.Fatalf("canonical publication changed exact authored map keys/values: %s err=%v", admittedRaw, err)
	}
	written, err := json.Marshal(p.instance(t).Fields["items"])
	if err != nil || a2AccumulatorJSONHash(t, written) != a2AccumulatorJSONHash(t, wantWritten) {
		var persisted []byte
		if readErr := p.selected.db.QueryRowContext(p.ctx, `SELECT fields FROM entity_state WHERE run_id=$1 AND entity_id=$2`, p.runID, p.runID).Scan(&persisted); readErr != nil {
			t.Fatal(readErr)
		}
		t.Fatalf("compiled writer changed exact admitted map keys/values: %s; persisted=%s err=%v", written, persisted, err)
	}
	if len(p.outputs(t)) != 0 {
		t.Fatal("compiled handler eagerly published fan-out outputs")
	}
	var arm joinruntime.Activation
	if gather {
		arm = exactJoinPersistedArm(t, p.instance(t))
		if !reflect.DeepEqual(arm.Members, keys) || arm.Completed() != 0 {
			t.Fatalf("same-field stage-entry membership = %#v", arm)
		}
	}
	p.publish(t, "batch.replace", replacement)
	live, err := json.Marshal(p.instance(t).Fields["items"])
	wantLive, _ := json.Marshal(replacement)
	if err != nil || a2AccumulatorJSONHash(t, live) != a2AccumulatorJSONHash(t, wantLive) {
		t.Fatalf("replacement handler did not commit same-cardinality live map: %s err=%v", live, err)
	}
	p.restart(t)
	handoff := p.handoff(t)
	if handoff.key.RunID != p.runID {
		t.Fatalf("selector returned foreign run: %#v", handoff.key)
	}
	// Inspect the real selected-store immutable source before releasing its
	// claim. No evaluation input or publication is supplied by the test.
	intent, claim, found, err := handoff.owner.ClaimFanOutIntent(p.ctx, pipeline.FanOutClaimRequest{
		Owner: "a2-map-source-readback", BundleHash: p.fact.BundleHash(), Candidate: &handoff.key, Now: time.Now().UTC(), Lease: time.Minute,
	})
	if err != nil || !found {
		t.Fatalf("claim compiled intent: found=%v err=%v", found, err)
	}
	if intent.Source.Kind != fanoutobligation.SourceEntityField || intent.Source.MutationID == "" || intent.Source.Field != "items" ||
		intent.Request.Cardinality != len(keys) || !intent.Request.Capsule.SourceProjection.SourceAfterWrites || intent.Request.Capsule.Lineage.ParentEventID != trigger.ID() {
		t.Fatalf("handler did not pin its post-write state revision: %#v", intent)
	}
	var frozen []byte
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT new_value FROM entity_mutations
					WHERE mutation_id=$1 AND run_id=$2 AND entity_id=$3 AND domain='authored_field' AND path=$4`,
		intent.Source.MutationID, intent.Source.RunID, intent.Source.EntityID, intent.Source.Field).Scan(&frozen); err != nil {
		t.Fatal(err)
	}
	wantFrozen, _ := json.Marshal(original)
	if a2AccumulatorJSONHash(t, frozen) != a2AccumulatorJSONHash(t, wantFrozen) {
		t.Fatal("pinned authored mutation did not preserve meaningful map values")
	}
	input, err := handoff.owner.LoadFanOutEvaluation(p.ctx, claim)
	if err != nil || len(input.Items) != fanoutobligation.MaxChunkSize {
		t.Fatalf("immutable source read: %#v err=%v", input, err)
	}
	for ordinal, item := range input.Items {
		if item != keys[ordinal] {
			t.Fatalf("immutable lexical key %d=%#v, want %q", ordinal, item, keys[ordinal])
		}
	}
	if settlement, err := handoff.owner.ReleaseFanOutClaim(p.ctx, claim); err != nil || !settlement.Acknowledged {
		t.Fatalf("release readback claim: %#v err=%v", settlement, err)
	}
	p.assertProgress(t, 0, len(keys), "open", intent.Source.MutationID)
	if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, handoff.key); err != nil {
		failure, _ := failures.EnvelopeFromError(err)
		t.Fatalf("actual first compiled map pump: %v; failure=%+v", err, failure)
	}
	waitForGateRecoveryQuiescence(t, p.bus, p.ctx)
	first := p.outputs(t)
	if len(first) != fanoutobligation.MaxChunkSize {
		t.Fatalf("first durable chunk=%d, want %d", len(first), fanoutobligation.MaxChunkSize)
	}
	if gather {
		collector := externalPipelineSourceNode(t, p.source, ".", "collector")
		for _, output := range first {
			assertExactJoinDeliveryStatus(t, p.selected, p.ctx, output.EventID, collector.Key(), "delivered")
		}
		if partial := exactJoinPersistedArm(t, p.instance(t)); partial.Completed() != len(first) || partial.Status != joinruntime.StatusOpen || !partial.JoinRef().Equal(arm.JoinRef()) {
			t.Fatalf("first actual chunk did not retain exact partial gather: %#v", partial)
		}
	}
	p.assertProgress(t, len(first), len(keys), "open", intent.Source.MutationID)
	p.restart(t)
	if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, handoff.key); err != nil {
		t.Fatalf("reconstructed coordinator next chunk: %v", err)
	}
	waitForGateRecoveryQuiescence(t, p.bus, p.ctx)
	outputs := p.outputs(t)
	if len(outputs) != len(keys) || !reflect.DeepEqual(first, outputs[:len(first)]) {
		t.Fatal("restart repeated, reordered or changed committed first-chunk outcomes")
	}
	for ordinal, output := range outputs {
		if output.Ordinal != ordinal || output.Payload.Key != keys[ordinal] || output.Payload.Index != ordinal || output.Payload.Count != len(keys) ||
			!reflect.DeepEqual(output.Payload.Result, original[keys[ordinal]]) {
			t.Fatalf("lexical ordinal %d lost source payload: %#v", ordinal, output)
		}
	}
	if gather {
		collector := externalPipelineSourceNode(t, p.source, ".", "collector")
		for _, output := range outputs[len(first):] {
			assertExactJoinDeliveryStatus(t, p.selected, p.ctx, output.EventID, collector.Key(), "delivered")
		}
		completed := exactJoinPersistedArm(t, p.instance(t))
		if !completed.JoinRef().Equal(arm.JoinRef()) || !reflect.DeepEqual(completed.Members, keys) || completed.Status != joinruntime.StatusClosed || completed.Completed() != len(keys) {
			prepared, found, err := p.selected.events.LoadPreparedPublishEvent(p.ctx, outputs[0].EventID)
			t.Fatalf("live map replacement changed exact join membership or results: %#v; first ordinal routes=%#v found=%v err=%v", completed, prepared.DeliveryRoutes, found, err)
		}
		results, err := completed.Results()
		wantResults := make([]any, len(keys))
		for index, key := range keys {
			wantResults[index] = original[key]
		}
		actual, _ := json.Marshal(results)
		want, _ := json.Marshal(wantResults)
		if err != nil || a2AccumulatorJSONHash(t, actual) != a2AccumulatorJSONHash(t, want) {
			t.Fatalf("exact join lost meaningful ordered map results: %s err=%v", actual, err)
		}
		completionSchedule := exactJoinPendingSchedule(t, p.selected, p.ctx, completed)
		if err := p.driver.Resume(p.ctx); err != nil {
			t.Fatalf("resume actual map join schedule: %v", err)
		}
		completionID := exactJoinOccurrenceEventID(t, p.selected, p.ctx, p.runID, "platform.join_complete")
		waitCtx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
		signal, err := p.probe.WaitForHandlerCompleted(waitCtx, completionID, collector.Key())
		cancel()
		if err != nil || signal.Status != "completed" {
			t.Fatalf("actual map join terminal continuation: status=%s err=%v", signal.Status, err)
		}
		terminal := waitForExactJoinState(t, p.ctx, p.pc, flowidentity.RouteForInstancePath(p.runID), "ready")
		fired := exactJoinPersistedArm(t, terminal)
		if !fired.JoinRef().Equal(arm.JoinRef()) || !fired.OutcomeFired || fired.OutcomePending || fired.CloseReason != joinruntime.CloseReasonComplete {
			t.Fatalf("map join outcome did not fire exactly: %#v", fired)
		}
		assertExactJoinFiredSchedule(t, p.selected, p.ctx, completionSchedule, completionID)
	}
	p.assertProgress(t, len(keys), len(keys), "closed", intent.Source.MutationID)
	beforeDuplicate := p.instance(t)
	p.restart(t)
	if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, handoff.key); err != nil {
		t.Fatalf("completed intent retry: %v", err)
	}
	for _, output := range outputs {
		prepared, found, err := p.selected.events.LoadPreparedPublishEvent(p.ctx, output.EventID)
		if err != nil || !found {
			t.Fatalf("read durable ordinal publication: found=%v err=%v", found, err)
		}
		if gather {
			collector := externalPipelineSourceNode(t, p.source, ".", "collector")
			bound := 0
			for _, route := range prepared.DeliveryRoutes {
				if route.Recipient.ID() == collector.Key() && len(route.Context.Joins) == 1 &&
					route.Context.Joins[0].Disposition == events.JoinAdmissionBound && route.Context.Joins[0].Ref.Equal(arm.JoinRef()) {
					bound++
				}
			}
			if bound != 1 {
				t.Fatalf("ordinal %d lacks its exact stage-entry receipt: %#v", output.Ordinal, prepared.DeliveryRoutes)
			}
		}
		if err := p.bus.PublishAcknowledged(p.ctx, prepared.Event.Event()); err != nil {
			t.Fatalf("duplicate durable ordinal publication: %v", err)
		}
	}
	if err := p.bus.PublishAcknowledged(p.ctx, trigger); err != nil {
		t.Fatalf("duplicate original scatter trigger: %v", err)
	}
	if err := p.bus.PublishAcknowledged(p.ctx, trigger); err != nil {
		t.Fatalf("duplicate actual trigger: %v", err)
	}
	waitForGateRecoveryQuiescence(t, p.bus, p.ctx)
	if !reflect.DeepEqual(outputs, p.outputs(t)) {
		t.Fatal("duplicate publications or completed-intent retry changed ordinals/outcomes")
	}
	p.assertProgress(t, len(keys), len(keys), "closed", intent.Source.MutationID)
	afterDuplicate := p.instance(t)
	if beforeDuplicate.Revision != afterDuplicate.Revision || !reflect.DeepEqual(beforeDuplicate.StateBuckets, afterDuplicate.StateBuckets) {
		t.Fatal("duplicate durable publications changed exact join state or entity revision")
	}
}

func TestA2MapFanOutRetainedProjectionAndRangeRefuseOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			p := newA2MapFanOutExecution(t, backend.open(t), false)
			original := map[string]any{"A": []int64{11, 11}, "a": []int64{22}, "z": []int64{33}}
			p.publish(t, "batch.ready", original)
			p.publish(t, "batch.replace", map[string]any{"x": []int64{101}, "y": []int64{102}, "z": []int64{103}})
			p.restart(t)
			handoff := p.handoff(t)
			intent, claim, found, err := handoff.owner.ClaimFanOutIntent(p.ctx, pipeline.FanOutClaimRequest{
				Owner: "a2-map-hostile-readback", BundleHash: p.fact.BundleHash(), Candidate: &handoff.key, Now: time.Now().UTC(), Lease: time.Minute,
			})
			if err != nil || !found {
				t.Fatalf("claim actual compiled map: found=%v err=%v", found, err)
			}
			baseline, err := handoff.owner.LoadFanOutEvaluation(p.ctx, claim)
			if err != nil || !reflect.DeepEqual(baseline.Items, []any{"A", "a", "z"}) || baseline.StartOrdinal != 0 {
				t.Fatalf("actual immutable map source: %#v err=%v", baseline, err)
			}
			var capsuleRaw []byte
			if err := p.selected.db.QueryRowContext(p.ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1`, p.runID).Scan(&capsuleRaw); err != nil {
				t.Fatal(err)
			}
			footprint := func(t *testing.T) string {
				t.Helper()
				var mutations, facts, revisions int
				var cursor, cardinality int
				var status, owner, generation, lease, retained string
				if err := p.selected.db.QueryRowContext(p.ctx, `SELECT
					(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1),
					(SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1),
					(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1)`, p.runID).Scan(&mutations, &facts, &revisions); err != nil {
					t.Fatal(err)
				}
				if err := p.selected.db.QueryRowContext(p.ctx, `SELECT cursor,cardinality,status,claim_owner,
					CAST(claim_generation AS TEXT),CAST(lease_expires_at AS TEXT),CAST(capsule AS TEXT)
					FROM fan_out_intents WHERE run_id=$1`, p.runID).Scan(&cursor, &cardinality, &status, &owner, &generation, &lease, &retained); err != nil {
					t.Fatal(err)
				}
				state, err := json.Marshal(p.instance(t))
				if err != nil {
					t.Fatal(err)
				}
				p.assertProgress(t, 0, cardinality, "open", intent.Source.MutationID)
				return fmt.Sprintf("%d/%d/%d/%d/%d/%s/%s/%s/%s/%s/%s", mutations, facts, revisions, cursor, cardinality,
					status, owner, generation, lease, a2AccumulatorJSONHash(t, []byte(retained)), a2AccumulatorJSONHash(t, state))
			}
			setCapsule := func(t *testing.T, raw []byte) {
				t.Helper()
				// Hostile retained-metadata setup only. The business source revision
				// above was created by the actual compiled writer, never by SQL.
				result, err := p.selected.db.ExecContext(p.ctx, `UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, string(raw), p.runID)
				if err != nil {
					t.Fatal(err)
				}
				if count, err := result.RowsAffected(); err != nil || count != 1 {
					t.Fatalf("hostile retained capsule affected %d rows: %v", count, err)
				}
			}
			for _, corruption := range []struct {
				name string
				edit func(*fanoutobligation.Capsule)
				want string
			}{
				{"wrong_valid_list_projection", func(c *fanoutobligation.Capsule) {
					c.SourceProjection.CollectionType = contracts.CatalogTypeReference{Type: "[text]"}
					projection, err := contracts.AdmitCollectionProjection(c.SourceProjection.CollectionType)
					if err != nil {
						t.Fatal(err)
					}
					c.SourceProjection.CollectionProjection = projection
					c.SourceProjection.ItemType = projection.ItemType()
				}, "source projection semantic digest disagrees with plan"},
				{"corrupt_projection_kind", func(c *fanoutobligation.Capsule) {
					c.SourceProjection.CollectionProjection.Kind = contracts.CollectionListItems
				}, "invalid admitted collection projection"},
				{"corrupt_projection_item_type", func(c *fanoutobligation.Capsule) {
					c.SourceProjection.ItemType.Kind = contracts.CatalogTypeInteger
				}, "source projection contradicts catalog-admitted type"},
			} {
				t.Run(corruption.name, func(t *testing.T) {
					var capsule fanoutobligation.Capsule
					if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
						t.Fatal(err)
					}
					corruption.edit(&capsule)
					raw, err := fanoutobligation.MarshalCapsule(capsule)
					if err != nil {
						t.Fatal(err)
					}
					setCapsule(t, raw)
					t.Cleanup(func() { setCapsule(t, capsuleRaw) })
					before := footprint(t)
					input, err := handoff.owner.LoadFanOutEvaluation(p.ctx, claim)
					if err == nil || !strings.Contains(err.Error(), corruption.want) || input.Trigger.ID() != "" || len(input.Items) != 0 {
						t.Fatalf("real owner admitted corrupt projection: input=%#v err=%v, want %s", input, err, corruption.want)
					}
					if footprint(t) != before {
						t.Fatal("projection refusal changed claim, state, history or outcomes")
					}
				})
			}
			t.Run("source_cardinality_range_mismatch", func(t *testing.T) {
				if _, err := p.selected.db.ExecContext(p.ctx, `UPDATE fan_out_intents SET cardinality=$1 WHERE run_id=$2`, 4, p.runID); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if _, err := p.selected.db.ExecContext(p.ctx, `UPDATE fan_out_intents SET cardinality=$1 WHERE run_id=$2`, 3, p.runID); err != nil {
						t.Error(err)
					}
				})
				before := footprint(t)
				input, err := handoff.owner.LoadFanOutEvaluation(p.ctx, claim)
				if err == nil || !strings.Contains(err.Error(), "immutable source cardinality = 3, want 4") || len(input.Items) != 0 {
					t.Fatalf("real owner admitted source/range mismatch: input=%#v err=%v", input, err)
				}
				if footprint(t) != before {
					t.Fatal("source/range refusal changed claim, state, history or outcomes")
				}
			})
			failure := engine.NormalizeFailure(&engine.EmitPayloadContractError{
				Event: "item.ready", Kind: engine.EmitPayloadSchemaMismatch, Path: "$.member_id", Constraint: "type",
				Expected: "text", Actual: "null", Detail: "hostile range control requires rejected text member_id",
			}, "test", "a2_map_hostile_range")
			rejection, err := failures.MarshalEnvelope(failure.Failure)
			if err != nil {
				t.Fatal(err)
			}
			for _, hostileRange := range []struct {
				name     string
				ordinals []int
				want     string
			}{
				{"skip_initial_ordinal", []int{1}, "want contiguous 0"},
				{"gap_after_first_ordinal", []int{0, 2}, "want contiguous 1"},
				{"repeat_first_ordinal", []int{0, 0}, "want contiguous 1"},
				{"exceed_source_range", []int{0, 1, 2, 3}, "chunk exceeds claimed range"},
			} {
				t.Run(hostileRange.name, func(t *testing.T) {
					command := pipeline.FanOutChunkCommand{Claim: claim, Now: time.Now().UTC()}
					for _, ordinal := range hostileRange.ordinals {
						command.Outcomes = append(command.Outcomes, pipeline.FanOutChunkOutcome{Ordinal: ordinal, Failure: rejection})
					}
					if err := command.Validate(); err != nil {
						t.Fatalf("hostile range did not reach persisted-owner admission: %v", err)
					}
					before := footprint(t)
					committed, err := handoff.owner.CommitFanOutChunk(p.ctx, command)
					if err == nil || !strings.Contains(err.Error(), hostileRange.want) || len(committed.Publications) != 0 {
						t.Fatalf("real owner admitted hostile range: committed=%#v err=%v, want %s", committed, err, hostileRange.want)
					}
					if footprint(t) != before {
						t.Fatal("range refusal leaked a tentative ordinal, cursor, state or history")
					}
				})
			}
			restored, err := handoff.owner.LoadFanOutEvaluation(p.ctx, claim)
			if err != nil || !reflect.DeepEqual(restored.Items, baseline.Items) || restored.StartOrdinal != baseline.StartOrdinal || restored.Trigger.ID() != baseline.Trigger.ID() {
				t.Fatalf("hostile refusals poisoned original admitted source/claim: %#v err=%v", restored, err)
			}
			if settlement, err := handoff.owner.ReleaseFanOutClaim(p.ctx, claim); err != nil || !settlement.Acknowledged {
				t.Fatalf("release actual source claim after refusals: %#v err=%v", settlement, err)
			}
			if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, handoff.key); err != nil {
				t.Fatalf("actual compiled pump after refusal controls: %v", err)
			}
			outputs := p.outputs(t)
			if len(outputs) != len(baseline.Items) {
				t.Fatalf("legal pump lost source range after refusal controls: %#v", outputs)
			}
			for ordinal, output := range outputs {
				if output.Ordinal != ordinal || output.Payload.Key != baseline.Items[ordinal] || output.Payload.Index != ordinal || output.Payload.Count != 3 ||
					!reflect.DeepEqual(output.Payload.Result, original[output.Payload.Key]) {
					t.Fatalf("legal exact map ordinal changed after refusal controls: %#v", output)
				}
			}
			p.assertProgress(t, 3, 3, "closed", intent.Source.MutationID)
		})
	}
}
