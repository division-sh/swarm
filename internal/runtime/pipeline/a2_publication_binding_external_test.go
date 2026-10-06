package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowroutefixture"
	"github.com/google/uuid"
)

// Pause the real claimed worker before preparation. Output evaluation, routing,
// publication, admission, execution and settlement are not replaced.
type a2HeldWorkerProbe struct {
	*lifecycleprobe.Probe
	eventID string
	nodeID  string
	started chan lifecycleprobe.Signal
	release chan struct{}
	once    sync.Once
}

func (p *a2HeldWorkerProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	p.Probe.NotifyLifecycle(ctx, signal)
	if signal.Kind == lifecycleprobe.HandlerStarted && (signal.EventID == p.eventID || p.nodeID != "" && signal.SubscriberID == p.nodeID) {
		if p.started != nil {
			select {
			case p.started <- signal:
			default:
			}
		}
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	}
}

func (p *a2HeldWorkerProbe) resume() { p.once.Do(func() { close(p.release) }) }

// Hold only the first prepared business commit. The selected store still
// performs the actual lifecycle-entry fence and rollback; no synthetic CAS error.
type a2HeldPublicationCommit struct {
	runtimepipeline.WorkflowPersistenceOwner
	nodeID              string
	prepared            chan runtimepipeline.WorkflowEngineMutationCommand
	result              chan error
	release             chan struct{}
	holdOnce            sync.Once
	endOnce             sync.Once
	retried             chan runtimepipeline.WorkflowEngineMutationCommand
	retryRelease        chan struct{}
	retryOnce, retryEnd sync.Once
}

func (p *a2HeldPublicationCommit) CommitWorkflowEngineMutation(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	held := false
	if command.DeliverySuccess != nil && command.DeliverySuccess.Claim.SubscriberID() == p.nodeID {
		p.holdOnce.Do(func() {
			held = true
			p.prepared <- command
			select {
			case <-p.release:
			case <-ctx.Done():
			}
		})
		if !held {
			p.retryOnce.Do(func() {
				p.retried <- command
				select {
				case <-p.retryRelease:
				case <-ctx.Done():
				}
			})
		}
	}
	result, err := p.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	if held {
		p.result <- err
	}
	return result, err
}

func (p *a2HeldPublicationCommit) resume()      { p.endOnce.Do(func() { close(p.release) }) }
func (p *a2HeldPublicationCommit) resumeRetry() { p.retryEnd.Do(func() { close(p.retryRelease) }) }

func TestA2StageEntryPayloadDirectedOutputOnBothStores(t *testing.T) {
	testA2StageEntryPublicationBinding(t, []int{0, 1, 2, 3})
}

func TestA2StageEntryMultiRecipientBindingOnBothStores(t *testing.T) {
	testA2StageEntryPublicationBinding(t, []int{4})
}

func TestA2StageEntryMissingComputedKeyRefusesWithoutReceiverMutationOnBothStores(t *testing.T) {
	testA2StageEntryPublicationBinding(t, []int{5})
}

func TestA2StageEntryFirstPublicationRacesTransitionAndRetriesOnBothStores(t *testing.T) {
	testA2StageEntryPublicationBinding(t, []int{6})
}

func TestA2StageEntryFirstPublicationRacesCloseAndRetainsLateRefusalOnBothStores(t *testing.T) {
	testA2StageEntryPublicationBinding(t, []int{7})
}

func testA2StageEntryPublicationBinding(t *testing.T, scenarios []int) {
	t.Helper()
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, scenario := range scenarios {
			t.Run(backend.name+"/"+[]string{"new_entry", "original_entry", "reversed_candidates_new_entry", "reversed_candidates_original_entry", "independent_recipients", "missing_computed_key", "first_commit_races_transition", "first_commit_races_close"}[scenario], func(t *testing.T) {
				selectedCollector, multipleRecipients := scenario%2, scenario == 4
				if scenario >= 6 {
					selectedCollector = 0
				}
				selected := backend.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				variant := canonicalrouting.ArrivalJoinPayloadDirected
				flows := []string{"orders", "orders"}
				if multipleRecipients {
					flows[1] = "mirror"
					variant = canonicalrouting.ArrivalJoinPayloadDirectedMultipleRecipients
				}
				files := canonicalrouting.ArrivalJoinRoutingFiles(t, variant)
				source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, files))
				worker := externalPipelineSourceNode(t, source, ".", "worker")
				collectors := []identity.ExecutableNode{externalPipelineSourceNode(t, source, flows[0], "collector"), externalPipelineSourceNode(t, source, flows[1], "collector")}
				dispatchers := []identity.ExecutableNode{externalPipelineSourceNode(t, source, flows[0], "dispatcher"), externalPipelineSourceNode(t, source, flows[1], "dispatcher")}
				triggerID := uuid.NewString()
				var commit *a2HeldPublicationCommit
				if scenario >= 6 {
					commit = &a2HeldPublicationCommit{WorkflowPersistenceOwner: selected.events.(runtimepipeline.WorkflowPersistenceOwner), nodeID: worker.Key(),
						prepared: make(chan runtimepipeline.WorkflowEngineMutationCommand, 1), result: make(chan error, 1), release: make(chan struct{}),
						retried: make(chan runtimepipeline.WorkflowEngineMutationCommand, 1), retryRelease: make(chan struct{})}
					t.Cleanup(commit.resume)
					t.Cleanup(commit.resumeRetry)
					selected.persistence = runtimepipeline.NewWorkflowPersistence(commit)
				}
				probe := &a2HeldWorkerProbe{Probe: lifecycleprobe.New(), eventID: triggerID, release: make(chan struct{})}
				logger := &exactJoinRuntimeLogger{}
				t.Cleanup(probe.resume)
				module := proposedEffectProofModule{source: source, nodes: []runtimepipeline.WorkflowNode{
					{Node: worker, Subscriptions: []events.EventType{"work.requested"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
					{Node: collectors[0], Subscriptions: []events.EventType{"orders/item.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
					{Node: dispatchers[0], Subscriptions: []events.EventType{"orders/manual.abort", "orders/dispatch.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
				}}
				if multipleRecipients {
					module.nodes = append(module.nodes,
						runtimepipeline.WorkflowNode{Node: collectors[1], Subscriptions: []events.EventType{"mirror/item.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
						runtimepipeline.WorkflowNode{Node: dispatchers[1], Subscriptions: []events.EventType{"mirror/manual.abort", "mirror/dispatch.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType})
				}
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, TestLifecycleProbe: probe, Logger: logger},
					"platform.join_complete", "platform.join_timeout")
				if err != nil {
					t.Fatal(err)
				}
				schedules, _ := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
				options := runtimepipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe}
				pc := newGateRecoveryCoordinator(bus, selected, options)
				bus.SetInterceptors(pc)
				now := time.Now().UTC()
				parent := commitA2FixtureConstruction(t, pc, selected.events, ctx, testRunScopedWorkflowInstanceForRun(runID, runID), runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "active", EntityType: "root_state", Fields: map[string]any{"work_count": int64(0)},
				}, now)
				keys := []string{uuid.NewString(), uuid.NewString()}
				if multipleRecipients {
					keys[1] = keys[0]
				}
				paths := []string{flows[0] + "/" + keys[0], flows[1] + "/" + keys[1]}
				load := func(index int) runtimepipeline.WorkflowInstance {
					t.Helper()
					item, found, err := pc.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, paths[index]))
					if err != nil || !found {
						t.Fatalf("load exact collector %d: found=%v err=%v", index, found, err)
					}
					return item
				}
				entry := func(index int) timeridentity.StageEntryRef {
					t.Helper()
					value, found, err := workflowlifecycle.LoadStageEntry(load(index).Bookkeeping)
					if err != nil || !found {
						t.Fatalf("collector entry: found=%v err=%v", found, err)
					}
					return value
				}
				constructionOrder := []int{0, 1}
				if scenario == 2 || scenario == 3 {
					constructionOrder = []int{1, 0}
				}
				for _, index := range constructionOrder {
					path := paths[index]
					readiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
						Identity: flowidentity.Instance{TemplateID: flows[index], ScopeKey: flows[index], InstanceID: keys[index], InstancePath: path, EntityID: flowidentity.EntityID(path), HasStoredPath: true},
						RunID:    runID, BundleHash: authorActivityTestSourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
					}
					readiness.Identity.ParentRoute = flowidentity.ParentRoute{FlowID: parent.Identity.TemplateID, FlowInstance: parent.Identity.InstancePath, EntityID: parent.Identity.EntityID}
					readiness.Identity.ParentEntityID = parent.Identity.EntityID
					constructed := commitA2FixtureConstruction(t, pc, selected.events, ctx, testRunScopedWorkflowInstanceForRun(runID, path), runtimepipeline.WorkflowInstance{
						InstanceID: keys[index], StorageRef: path, EntityID: flowidentity.EntityID(path), WorkflowName: flows[index], WorkflowVersion: source.WorkflowVersion(),
						ParentFlowID: parent.Identity.TemplateID, ParentFlowInstance: parent.Identity.InstancePath, ParentEntityID: parent.Identity.EntityID,
						Mode: "template", RuntimeReadiness: &readiness, CurrentState: "awaiting", EntityType: "order_state", Fields: map[string]any{"order_id": keys[index], "expected": []any{"a", "b"}},
					}, now)
					markGateRecoveryTopologyReadyFixture(t, selected, readiness, now)
					if err := flowroutefixture.Publish(bus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedWorkflowInstanceForRun(runID, path), Instance: constructed.Identity}); err != nil {
						t.Fatal(err)
					}
				}
				original := []timeridentity.StageEntryRef{entry(0), entry(1)}
				payload, err := json.Marshal(map[string]any{"prefix": keys[selectedCollector][:18], "suffix": keys[selectedCollector][18:]})
				if scenario == 5 {
					payload = []byte(`{"prefix":"","suffix":""}`)
				}
				if err != nil {
					t.Fatal(err)
				}
				trigger := eventtest.ExistingRunRootIngressWithRoutingSource(triggerID, "work.requested", "operator", "", payload, 0,
					runID, events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), now)
				if err := bus.PublishAcknowledged(ctx, trigger); err != nil {
					t.Fatal(err)
				}
				waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err = probe.WaitForHandlerStarted(waitCtx, triggerID, worker.Key())
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				reenter := func(index int) {
					t.Helper()
					for _, name := range []string{"manual.abort", "dispatch.completed"} {
						path := paths[index]
						event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(path+"/"+name), "operator", "", []byte("{}"), 0,
							runID, events.EnvelopeForEntityID(events.EventEnvelope{}, flowidentity.EntityID(path)),
							eventtest.ConcreteTemplateRoutingSource(flows[index], path, flowidentity.EntityID(path)), now)
						if err := bus.PublishAcknowledged(ctx, event); err != nil {
							t.Fatal(err)
						}
						waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
						completed, err := probe.WaitForHandlerCompleted(waitCtx, event.ID(), dispatchers[index].Key())
						cancel()
						if err != nil || completed.Status != "completed" {
							t.Fatalf("reenter collector: status=%s err=%v", completed.Status, err)
						}
						assertExactJoinDeliveryStatus(t, selected, ctx, event.ID(), dispatchers[index].Key(), "delivered")
					}
				}
				if commit != nil {
					probe.resume()
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					select {
					case command := <-commit.prepared:
						if len(command.Publications) != 1 {
							t.Fatalf("first commit has %d publications", len(command.Publications))
						}
						publication := command.Publications[0].(runtimebus.EnginePublicationPlan).PublicationCommand()
						if len(publication.Commit.DeliveryRoutes) != 1 || len(publication.Commit.DeliveryRoutes[0].Context.Joins) != 1 ||
							publication.Commit.DeliveryRoutes[0].Context.Joins[0].Ref.StageEntry() != original[0] || len(publication.Commit.JoinAdmissionFences) != 1 {
							t.Fatalf("race did not reach real E1 admission: %#v", publication.Commit)
						}
					case <-waitCtx.Done():
						t.Fatalf("publication preparation did not reach commit: %v", waitCtx.Err())
					}
					cancel()
				}
				if scenario == 7 {
					for _, member := range []string{"a", "b"} {
						payload, err := json.Marshal(map[string]any{"order_id": keys[0], "member_id": member, "result": map[string]any{"value": member}})
						if err != nil {
							t.Fatal(err)
						}
						event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(paths[0]+"/item.completed"), "operator", "", payload, 0,
							runID, events.EnvelopeForEntityID(events.EventEnvelope{}, flowidentity.EntityID(paths[0])),
							eventtest.ConcreteTemplateRoutingSource(flows[0], paths[0], flowidentity.EntityID(paths[0])), now)
						if err := bus.PublishAcknowledged(ctx, event); err != nil {
							t.Fatal(err)
						}
						arrivalCtx, cancelArrival := context.WithTimeout(ctx, 5*time.Second)
						arrival, arrivalErr := probe.WaitForHandlerCompleted(arrivalCtx, event.ID(), collectors[0].Key())
						cancelArrival()
						if arrivalErr != nil || arrival.Status != "completed" {
							t.Fatalf("close-race arrival: status=%s err=%v", arrival.Status, arrivalErr)
						}
						assertExactJoinDeliveryStatus(t, selected, ctx, event.ID(), collectors[0].Key(), "delivered")
					}
					closed := exactJoinPersistedArm(t, load(0))
					if closed.Status != joinruntime.StatusClosed || !closed.OutcomePending || closed.OutcomeFired || closed.Completed() != 2 || entry(0) != original[0] {
						t.Fatalf("publication race did not reach a real same-entry close: %#v", closed)
					}
				} else {
					reenter(0)
					if entry(0) == original[0] || entry(1) != original[1] {
						t.Fatal("reentry aliased the original or sibling entry")
					}
				}
				wantEntries := []timeridentity.StageEntryRef{entry(0), entry(1)}
				beforeCollectors := []runtimepipeline.WorkflowInstance{load(0), load(1)}
				probe.resume()
				if commit != nil {
					commit.resume()
				}
				if commit != nil {
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					select {
					case commitErr := <-commit.result:
						refusal, typed := failures.EnvelopeFromError(commitErr)
						if !typed || refusal.Class != failures.ClassLifecycleConflict || refusal.Detail.Code != "join_publication_entry_changed" {
							t.Fatalf("wrong real first-publication fence: %v", commitErr)
						}
					case <-waitCtx.Done():
						t.Fatal(waitCtx.Err())
					}
					select {
					case command := <-commit.retried:
						if len(command.Publications) != 1 {
							t.Fatal("fresh evaluation lost the single publication")
						}
						publication := command.Publications[0].(runtimebus.EnginePublicationPlan).PublicationCommand()
						if len(publication.Commit.DeliveryRoutes) != 1 || len(publication.Commit.DeliveryRoutes[0].Context.Joins) != 1 ||
							publication.Commit.DeliveryRoutes[0].Context.Joins[0].Ref.StageEntry() != wantEntries[0] {
							t.Fatalf("fresh publication did not retain the actual current entry: %#v", publication.Commit)
						}
					case <-waitCtx.Done():
						t.Fatal("contention did not re-evaluate within the same delivery attempt")
					}
					cancel()
					assertExactJoinDeliveryStatus(t, selected, ctx, triggerID, worker.Key(), "in_progress")
					var publications int
					if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE source_event_id=$1", triggerID).Scan(&publications); err != nil || publications != 0 {
						t.Fatalf("rejected first publication leaked: count=%d err=%v", publications, err)
					}
					root, found, err := pc.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, runID))
					if err != nil || !found || root.Fields["work_count"] != int64(0) {
						t.Fatalf("failed publication did not roll back its worker write: %#v err=%v", root.Fields, err)
					}
					for index, before := range beforeCollectors {
						if after := load(index); !reflect.DeepEqual(after, before) {
							t.Fatalf("failed publication changed collector %d", index)
						}
					}
					retained, found, err := selected.events.LoadPreparedPublishEvent(ctx, triggerID)
					if err != nil || !found || len(retained.DeliveryRoutes) != 1 {
						t.Fatalf("failed worker lost its exact durable input: found=%v err=%v", found, err)
					}
					commit.resumeRetry()
				}
				waitCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
				completed, completedCursor, err := probe.WaitAfter(waitCtx, lifecycleprobe.Cursor{}, lifecycleprobe.Signal{Kind: lifecycleprobe.HandlerCompleted, EventID: triggerID, SubscriberType: "node", SubscriberID: worker.Key()})
				cancel()
				if commit != nil {
					var retries, attempts, matches int
					for _, row := range storetest.ObserveDeliveryEventEvidence(t, ctx, selected.events, triggerID).Deliveries {
						if row.SubscriberID == worker.Key() {
							matches++
							retries = row.RetryCount
							attempts = len(row.Attempts)
						}
					}
					if matches != 1 || retries != 0 || attempts != 1 {
						t.Fatalf("contention consumed delivery attempts: matches=%d retries=%d attempts=%d", matches, retries, attempts)
					}
				}
				if scenario == 5 {
					quietCtx, quietCancel := context.WithTimeout(ctx, 5*time.Second)
					quietErr := bus.WaitForQuiescence(quietCtx)
					quietCancel()
					if quietErr != nil {
						t.Fatal(quietErr)
					}
					if err != nil || completed.Status != "completed" {
						t.Fatalf("upstream worker did not complete: status=%s err=%v", completed.Status, err)
					}
					assertExactJoinDeliveryStatus(t, selected, ctx, triggerID, worker.Key(), "delivered")
					var outputID string
					if err := selected.db.QueryRowContext(ctx, "SELECT event_id FROM events WHERE source_event_id=$1", triggerID).Scan(&outputID); err != nil {
						t.Fatal(err)
					}
					blocked, found, err := selected.events.LoadPreparedPublishEvent(ctx, outputID)
					if err != nil || !found || len(blocked.DeliveryRoutes) != 0 || blocked.Settlement.Reason() != events.NoDeliveryResolutionBlocked {
						t.Fatalf("missing key did not retain route refusal: publication=%#v found=%v err=%v", blocked, found, err)
					}
					var raw string
					if err := selected.db.QueryRowContext(ctx, "SELECT CAST(failure AS TEXT) FROM dead_letters WHERE original_event_id=$1", outputID).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var refusal failures.Envelope
					if err := json.Unmarshal([]byte(raw), &refusal); err != nil || refusal.Detail.Code != pinrouting.ConnectFailureInstanceSourceValueMissing.Code() {
						t.Fatalf("missing key lacks typed refusal: %s err=%v", raw, err)
					}
					var routes int
					if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1", outputID).Scan(&routes); err != nil || routes != 0 {
						t.Fatalf("missing key invented an executable receiver: count=%d err=%v", routes, err)
					}
					root, found, err := pc.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, runID))
					if err != nil || !found || root.Fields["work_count"] != int64(1) {
						t.Fatalf("upstream worker write was not retained: fields=%#v found=%v err=%v", root.Fields, found, err)
					}
					for index, before := range beforeCollectors {
						if after := load(index); after.Revision != before.Revision || !reflect.DeepEqual(after.StateBuckets, before.StateBuckets) {
							t.Fatalf("missing key mutated collector %d", index)
						}
					}
					return
				}
				if err != nil || completed.Status != "completed" {
					settleCtx, settleCancel := context.WithTimeout(ctx, 5*time.Second)
					_, _, settleErr := probe.WaitAfter(settleCtx, completedCursor, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: triggerID, SubscriberType: "node", SubscriberID: worker.Key()})
					quietErr := bus.WaitForQuiescence(settleCtx)
					settleCancel()
					var failure string
					readErr := selected.db.QueryRowContext(ctx, "SELECT CAST(COALESCE(failure, '{}') AS TEXT) FROM event_deliveries WHERE event_id=$1 AND subscriber_id=$2", triggerID, worker.Key()).Scan(&failure)
					t.Fatalf("real worker output: status=%s err=%v settlement=%v quiet=%v read=%v failure=%s logs=%s", completed.Status, err, settleErr, quietErr, readErr, failure, logger.String())
				}
				var outputID string
				if err := selected.db.QueryRowContext(ctx, "SELECT event_id FROM events WHERE source_event_id=$1 AND event_name=$2", triggerID, "item.completed").Scan(&outputID); err != nil {
					t.Fatal(err)
				}
				prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, outputID)
				wantRoutes := 1
				if multipleRecipients {
					wantRoutes = 2
				}
				if err != nil || !found || len(prepared.DeliveryRoutes) != wantRoutes {
					t.Fatalf("actual worker publication: found=%v routes=%d err=%v settlement=%#v logs=%s", found, len(prepared.DeliveryRoutes), err, prepared.Settlement, logger.String())
				}
				if scenario == 7 {
					route := prepared.DeliveryRoutes[0]
					if len(route.Context.Joins) != 1 || route.Context.Joins[0].Ref.StageEntry() != original[0] {
						t.Fatalf("retry of new output invented a newer arm: %#v", route)
					}
					assertExactJoinDeliveryStatus(t, selected, ctx, outputID, collectors[0].Key(), "dead_letter")
					var raw string
					if err := selected.db.QueryRowContext(ctx, "SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1 AND subscriber_id=$2", outputID, collectors[0].Key()).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var refusal failures.Envelope
					if err := json.Unmarshal([]byte(raw), &refusal); err != nil || refusal.Class != failures.ClassStaleArrival {
						t.Fatalf("closed receiver lacks counted late refusal: %s err=%v", raw, err)
					}
					if after := load(0); !reflect.DeepEqual(after.StateBuckets, beforeCollectors[0].StateBuckets) || after.Revision != beforeCollectors[0].Revision {
						t.Fatal("late output changed the already-closed arm")
					}
					root, found, err := pc.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, runID))
					if err != nil || !found || root.Fields["work_count"] != int64(1) {
						t.Fatalf("retried publication did not commit its worker write once: fields=%#v found=%v err=%v", root.Fields, found, err)
					}
					reenter(0)
					pc = newGateRecoveryCoordinator(bus, selected, options)
					bus.SetInterceptors(pc)
					beforeReplay := load(0)
					if err := bus.PublishAcknowledged(ctx, prepared.Event.Event()); err != nil {
						t.Fatal(err)
					}
					quietCtx, quietCancel := context.WithTimeout(ctx, 5*time.Second)
					quietErr := bus.WaitForQuiescence(quietCtx)
					quietCancel()
					if quietErr != nil {
						t.Fatal(quietErr)
					}
					if after := load(0); after.Revision != beforeReplay.Revision || !reflect.DeepEqual(after.StateBuckets, beforeReplay.StateBuckets) {
						t.Fatal("late output replay was rebound into E2")
					}
					replayed, found, err := selected.events.LoadPreparedPublishEvent(ctx, outputID)
					if err != nil || !found || !reflect.DeepEqual(replayed.DeliveryRoutes, prepared.DeliveryRoutes) {
						t.Fatalf("late output replay changed the committed manifest: found=%v err=%v", found, err)
					}
					assertExactJoinDeliveryCount(t, selected, ctx, outputID, collectors[0].Key(), 1)
					assertExactJoinDeliveryStatus(t, selected, ctx, outputID, collectors[0].Key(), "dead_letter")
					return
				}
				for _, route := range prepared.DeliveryRoutes {
					index := selectedCollector
					if multipleRecipients && route.Target.Route().FlowInstance == paths[1] {
						index = 1
					}
					assertExactJoinDeliveryStatus(t, selected, ctx, outputID, collectors[index].Key(), "delivered")
					if route.Target.Route().FlowInstance != paths[index] || route.Recipient.ID() != collectors[index].Key() || len(route.Context.Joins) != 1 || route.Context.Joins[0].Ref.StageEntry() != wantEntries[index] {
						t.Fatalf("output bound an inferred or sibling receiver: %#v", route)
					}
				}
				for index := range paths {
					item := load(index)
					state, err := runtimeengine.StateCarrierFromPersisted(item.Fields, item.Bookkeeping, item.Gates, item.StateBuckets)
					if err != nil {
						t.Fatal(err)
					}
					arms, err := joinruntime.List(state.StateBuckets)
					if err != nil {
						t.Fatal(err)
					}
					completed := 0
					for _, arm := range arms {
						completed += arm.Completed()
					}
					want := 0
					if multipleRecipients || index == selectedCollector {
						want = 1
					}
					if completed != want {
						t.Fatalf("collector %d contributions=%d want=%d", index, completed, want)
					}
				}
				replayIndices := []int{selectedCollector}
				if multipleRecipients {
					replayIndices = []int{0, 1}
				}
				for _, index := range replayIndices {
					reenter(index)
					if entry(index) == wantEntries[index] {
						t.Fatal("replay control did not advance receiver entry")
					}
				}
				pc = newGateRecoveryCoordinator(bus, selected, options)
				bus.SetInterceptors(pc)
				before := []runtimepipeline.WorkflowInstance{load(0), load(1)}
				if err := bus.PublishAcknowledged(ctx, prepared.Event.Event()); err != nil {
					t.Fatalf("exact output replay after restart: %v", err)
				}
				waitCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
				err = bus.WaitForQuiescence(waitCtx)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				replayed, found, err := selected.events.LoadPreparedPublishEvent(ctx, outputID)
				if err != nil || !found || !reflect.DeepEqual(replayed.DeliveryRoutes, prepared.DeliveryRoutes) {
					t.Fatalf("replay rebound committed output: found=%v err=%v", found, err)
				}
				for index := range paths {
					if after := load(index); after.Revision != before[index].Revision {
						t.Fatalf("replay changed receiver %d revision: %d/%d", index, before[index].Revision, after.Revision)
					}
				}
				for _, index := range replayIndices {
					assertExactJoinDeliveryCount(t, selected, ctx, outputID, collectors[index].Key(), 1)
				}
			})
		}
	}
}
