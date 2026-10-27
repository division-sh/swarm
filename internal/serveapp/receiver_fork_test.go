package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

type receiverForkPublicationBarrier struct {
	*lifecycleprobe.Probe
	entered chan struct{}
	release chan struct{}
	start   sync.Once
	finish  sync.Once
}

func (b *receiverForkPublicationBarrier) unblock() { b.finish.Do(func() { close(b.release) }) }

func (b *receiverForkPublicationBarrier) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	b.Probe.NotifyLifecycle(ctx, signal)
	if signal.Kind != lifecycleprobe.PostCommitDispatchStarted || signal.EventType != "work.requested" {
		return
	}
	b.start.Do(func() { close(b.entered) })
	select {
	case <-b.release:
	case <-ctx.Done():
	}
}

func TestReceiverCompositionForkBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, surface := range []string{"admitted", "admitted_http", "pending_refusal", "fieldless_settled"} {
			t.Run(string(backend)+"/"+surface, func(t *testing.T) {
				admitted := strings.HasPrefix(surface, "admitted") || surface == "fieldless_settled"
				fieldless := strings.HasPrefix(surface, "fieldless_")
				var selected *selectedStoreOwner
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
					selected = owner
					return previous(owner)
				}
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				root := canonicalrouting.CopyReceiverOptionalChild(t, true)
				if fieldless {
					root = canonicalrouting.CopyReceiverFieldlessFork(t)
				}
				reached, release := make(chan struct{}, 1), make(chan struct{})
				probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
				publication := &receiverForkPublicationBarrier{Probe: probe.Raw(), entered: make(chan struct{}), release: make(chan struct{})}
				var observer lifecycleprobe.Observer = probe
				if surface == "pending_refusal" {
					observer = publication
				}
				configureOwnedMockLifecycleProbe(t, observer)
				start := startServedTestSetupEntitiesProofRuntimeFromSource
				if admitted {
					start = startServedLiveFlowProofRuntimeFromSource
				}
				rt := start(t, backend, root, func(ctx context.Context, _ string, evt events.Event) error {
					if surface != "pending_refusal" || evt.Type() != "work.requested" {
						return nil
					}
					reached <- struct{}{}
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				t.Cleanup(func() { close(release) })
				t.Cleanup(publication.unblock)
				var seedEventID string
				params := map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-request"}
				if !fieldless {
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-seed"})
					if admitted {
						requireServedLiveRootMode(t, rt, seed.RunID, seed.EventID)
					}
					seedEventID = seed.EventID
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "active")
					if admitted {
						waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
						requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "receiver-fork-pause"})
					}
					params = map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-request"}
				} else {
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "fork.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{}, "idempotency_key": "empty-fork-seed"})
					requireServedLiveRootMode(t, rt, seed.RunID, seed.EventID)
					seedEventID = seed.EventID
					waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
					params = map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-request"}
				}
				request := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if surface == "pending_refusal" {
					select {
					case <-publication.entered:
					case <-time.After(10 * time.Second):
						t.Fatal("missing source publication barrier")
					}
					publication.unblock()
					select {
					case <-reached:
					case <-time.After(10 * time.Second):
						t.Fatal("missing fork frontier barrier")
					}
				} else if !admitted {
					waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, request.RunID)
				}
				if fieldless {
					// Fieldless instances still have canonical construction headers;
					// absence of business fields is no longer absence of construction.
					var count int
					if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, request.RunID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("fieldless fixture has %d field rows: %v", count, err)
					}
				}
				var forkRunID string
				forkPoint := request.EventID
				if fieldless {
					// The original ingress remains a non-agent historical-replay
					// refusal; its settled output admits a constructed fieldless cut.
					var outputID string
					deadline := time.Now().Add(servedProofPollDeadline)
					for {
						err := rt.DB.QueryRow(`SELECT e.event_id FROM events e WHERE e.run_id=$1 AND e.event_name='work.completed' AND EXISTS (SELECT 1 FROM event_receipts r WHERE r.event_id=e.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline')`, request.RunID).Scan(&outputID)
						if err == nil {
							break
						}
						if !errors.Is(err, sql.ErrNoRows) {
							t.Fatal(err)
						}
						if time.Now().After(deadline) {
							t.Fatalf("entityless output did not settle: %s", servedEventPublishDebugSummary(t, rt.DB, rt.Backend, request.RunID))
						}
						time.Sleep(10 * time.Millisecond)
					}
					waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, request.RunID)
					requireLifecycleEventCount(t, rt, request.RunID, "work.completed", 1)
					requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": request.RunID, "idempotency_key": "empty-fork-pause"})
					if surface == "fieldless_settled" {
						forkPoint = outputID
					}
				}
				if surface == "historical_refusal" {
					// Terminal deliveries alone do not join deferred publication settlement.
					deadline := time.Now().Add(servedProofPollDeadline)
					for {
						var diagnosis cliapp.DiagnosticRunDiagnosisResult
						requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": request.RunID}, &diagnosis)
						if diagnosis.Run.RunID != request.RunID || diagnosis.TestQuiescence == nil || diagnosis.TestQuiescence.Ready == nil {
							t.Fatalf("source lacks exact public run settlement evidence: %+v", diagnosis)
						}
						if *diagnosis.TestQuiescence.Ready {
							break
						}
						if time.Now().After(deadline) {
							t.Fatalf("source publication/receipt settlement did not finish: %+v", diagnosis.TestQuiescence)
						}
						time.Sleep(25 * time.Millisecond)
					}
				}
				// Join the exact publications, not the independently held node.
				// Completed publication includes its diagnostic tail.
				deadline := time.Now().Add(servedEventPublishLifecycleProbeWaitTimeout)
				probe.Expect(seedEventID).PostCommitDispatchStarted().PostCommitDispatchCompleted().Within(servedEventPublishLifecycleProbeWaitTimeout)
				remaining := time.Until(deadline)
				if remaining <= 0 {
					t.Fatal("source publication consumed the complete fingerprint deadline")
				}
				probe.Expect(request.EventID).PostCommitDispatchStarted().PostCommitDispatchCompleted().Within(remaining)
				forkCtx := servedControlProofAuthorActivityContext(t, rt)
				if surface == "admitted_http" {
					// Branch isolation needs later business history, not excluded runtime logs.
					if rt.SQLite != nil {
						advanceLifecycleForkSourceHistory(t, forkCtx, rt.SQLite, request.RunID)
					} else {
						advanceLifecycleForkSourceHistory(t, forkCtx, rt.Postgres, request.RunID)
					}
				}
				before := repeatedStaticRunSnapshot(t, rt.DB, request.RunID)
				checkSource := func(boundary string) {
					after := repeatedStaticRunSnapshot(t, rt.DB, request.RunID)
					if !reflect.DeepEqual(before, after) {
						for table, rows := range before {
							if !reflect.DeepEqual(rows, after[table]) {
								t.Errorf("source %s changed at %s", table, boundary)
								for _, row := range after[table] {
									if !slices.ContainsFunc(rows, func(previous []string) bool { return reflect.DeepEqual(previous, row) }) {
										t.Logf("source %s added row at %s: %#v", table, boundary, row)
									}
								}
							}
						}
						t.Fatal("fork mutated source headers, fields, events or settlement evidence")
					}
				}
				checkSource("before fork")
				wantExecuted := 1
				if surface == "admitted_http" {
					params := map[string]any{"source_run_id": request.RunID, "fork_event_id": request.EventID, "allow_source_freeze": true, "idempotency_key": "receiver-fork"}
					var fork, duplicate apiv1.RunForkExecutionResult
					requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
					requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)
					if fork.ForkRunID != duplicate.ForkRunID || fork.ExecutedEventCount != wantExecuted || duplicate.ExecutedEventCount != wantExecuted || fork.SourceRunStatus != "paused" || duplicate.SourceRunStatus != "paused" || fork.SourceFrozen || duplicate.SourceFrozen {
						t.Fatalf("fork replay disagreement: %#v / %#v", fork, duplicate)
					}
					forkRunID = fork.ForkRunID
				} else {
					family, ok := selected.RunFork()
					if !ok {
						t.Fatal("missing fork owner")
					}
					use, lookup, _, err := rt.Contexts.AcquireRun(forkCtx, request.RunID)
					if err != nil || use == nil || !lookup.Loaded() {
						t.Fatalf("acquire actual receiver runtime: loaded=%t err=%v", lookup.Loaded(), err)
					}
					defer func() {
						if err := use.Done(); err != nil {
							t.Error(err)
						}
					}()
					selection := runforkadmission.SelectedContractSelection(semanticview.Wrap(loadWorkflowValidationBundleAt(t, root)))
					operation := runfork.ForkOperationRequest{
						OperationID: uuid.NewString(), Actor: "receiver-composition", IdempotencyKey: surface, TransportHash: surface,
						SourceRunID: request.RunID, ForkEventID: forkPoint, TargetBundleHash: rt.BundleHash,
						AllowSourceFreeze: true, ContractSelection: selection,
					}
					execution := runtimerunforkexecution.SelectedContractExecutionRequest{
						SourceRunID: request.RunID, At: forkPoint, AllowSourceFreeze: true, ExpectedBundleHash: rt.BundleHash,
						SourceLoader:      runtimerunforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootForTest(), Store: selected.SourceArtifactStore()},
						ContractSelection: selection,
						ForkOperation:     &operation,
						AgentRuntime:      rt.ForkRuntime,
						LiveRuntime:       rt.Runtime,
						LiveContexts:      rt.Contexts,
						LiveRuns:          selected.LiveRuns(),
					}
					if !admitted {
						// This pre-handoff refusal retains the component's discard reach,
						// not a durable operation's separately retained failure history.
						execution.ForkOperation = nil
					}
					result, err := family.Execute(use.WorkContext(), execution)
					if admitted {
						if err != nil {
							t.Fatalf("canonical admitted fork failed: %v", err)
						}
						if result.ExecutedEventCount != wantExecuted || len(result.ForkEvents) != wantExecuted || result.ForkEvents[0].SourceEventID != forkPoint {
							t.Fatalf("fork did not execute its exact captured input: count=%d events=%+v", result.ExecutedEventCount, result.ForkEvents)
						}
						forkRunID = result.Materialization.ForkRunID
					} else {
						want := "source_committed_replay_scope_advanced_after_fork_point"
						if surface == "pending_refusal" {
							want = "source_active_conversation_session_coupling_after_fork_point"
						}
						if err == nil || !strings.Contains(err.Error(), want) || result.Activation.Activated || result.Activation.SourceFrozen {
							t.Fatalf("unsupported replay did not fail closed: %v activated=%t sourceFrozen=%t", err, result.Activation.Activated, result.Activation.SourceFrozen)
						}
						t.Logf("refused receiver fork cleanup: run=%s err=%v", result.Materialization.ForkRunID, err)
						if result.Materialization.ForkRunID == "" {
							var children int
							if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, request.RunID).Scan(&children); err != nil || children != 0 {
								t.Fatalf("pre-materialization refusal retained child runs: count=%d err=%v", children, err)
							}
							checkSource("after pre-materialization refusal")
							return
						}
						// The canonical discard retains completion evidence in a cancelled
						// run tombstone, but removes executable fork state and deliveries.
						var status string
						if err := rt.DB.QueryRow(`SELECT status FROM runs WHERE run_id=$1 AND forked_from_run_id=$2`, result.Materialization.ForkRunID, request.RunID).Scan(&status); err != nil {
							t.Fatal(err)
						}
						if status != "cancelled" {
							t.Fatalf("refused fork retained executable run status: %s", status)
						}
						for _, table := range []string{"events", "event_deliveries", "entity_state", "flow_instances", "flow_instance_runtime_readiness"} {
							var count int
							if err := rt.DB.QueryRow(`SELECT count(*) FROM `+table+` WHERE run_id=$1`, result.Materialization.ForkRunID).Scan(&count); err != nil {
								t.Fatal(err)
							}
							if count != 0 {
								t.Fatalf("refused fork retained %s: %d", table, count)
							}
						}
						checkSource("after refused fork")
						return
					}
				}
				if forkRunID == "" || forkRunID == request.RunID {
					t.Fatalf("fork did not execute selected receiver: %s", forkRunID)
				}
				waitServedSelectedForkFrontier(t, rt.ReceiverStateReader, rt.ReadRunDeliveries, forkRunID, forkPoint)
				checkSource("after supported fork")
				rootType, childType, rootState := "work", "receipt", "done"
				var rootFields, childFields map[string]any = map[string]any{}, map[string]any{}
				if fieldless {
					rootType, childType, rootState = "", "", "pending"
					rootFields, childFields = nil, nil
				}
				rootPhase := "ready"
				rootRevision := 2
				if fieldless {
					// The retained root is planned without inventing a field companion.
					rootRevision = 1
				}
				requireReceiverConstructedInstance(t, rt, forkRunID, forkRunID, ".", forkRunID, rootType, rootState, "", rootPhase, rootRevision, rootFields)
				requireReceiverConstructedInstance(t, rt, forkRunID, "sink", "sink", flowidentity.EntityID("sink"), childType, "pending", forkRunID, "ready", 1, childFields)
				var headers, companions int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, forkRunID).Scan(&headers); err != nil || headers != 2 {
					t.Fatalf("fork construction census: got=%d want=2 err=%v", headers, err)
				}
				wantCompanions := 2
				if fieldless {
					wantCompanions = 0
				}
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, forkRunID).Scan(&companions); err != nil || companions != wantCompanions {
					t.Fatalf("fork field companion census: got=%d want=%d err=%v", companions, wantCompanions, err)
				}
				requireReceiverPublicReadback(t, rt, forkRunID)
				requireReceiverPublicReadback(t, rt, request.RunID)
			})
		}
	}
}
