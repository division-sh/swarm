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
		"events.yaml": `seed: {}
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
    seed: {create_entity: true}
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
	bundle := loadPipelineLifecycleFixtureBundle(t, a2MapFanOutFiles(gather))
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
	bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: fact, RuntimeInstanceID: runtimeID,
		WorkOwner: work, PayloadAdmitter: admitter, TestLifecycleProbe: probe, DeliveryAuthority: authority,
	}, "platform.join_complete", "platform.join_timeout")
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
