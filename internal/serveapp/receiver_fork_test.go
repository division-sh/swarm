package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestReceiverCompositionForkBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, surface := range []string{"admitted", "admitted_http", "historical_refusal", "pending_refusal", "fieldless_settled", "fieldless_post_output_refusal"} {
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
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root, func(ctx context.Context, _ string, evt events.Event) error {
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
				params := map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-request"}
				if !fieldless {
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-seed"})
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "active")
					if admitted {
						waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
						requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "receiver-fork-pause"})
					}
					params = map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-request"}
				} else {
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "fork.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{}, "idempotency_key": "empty-fork-seed"})
					waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
					params = map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-request"}
				}
				request := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if surface == "pending_refusal" {
					select {
					case <-reached:
					case <-time.After(10 * time.Second):
						t.Fatal("missing fork frontier barrier")
					}
				} else if !admitted {
					waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, request.RunID)
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
					waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, request.RunID)
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
				before := repeatedStaticRunSnapshot(t, rt.DB, request.RunID)
				checkSource := func(boundary string) {
					after := repeatedStaticRunSnapshot(t, rt.DB, request.RunID)
					if !reflect.DeepEqual(before, after) {
						for table, rows := range before {
							if !reflect.DeepEqual(rows, after[table]) {
								t.Errorf("source %s changed at %s", table, boundary)
							}
						}
						t.Fatal("fork mutated source headers, fields, events or settlement evidence")
					}
				}
				checkSource("before fork")
				if surface == "admitted_http" {
					params := map[string]any{"source_run_id": request.RunID, "fork_event_id": request.EventID, "allow_source_freeze": true, "idempotency_key": "receiver-fork"}
					var fork, duplicate apiv1.RunForkExecutionResult
					requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
					requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)
					if fork.ForkRunID != duplicate.ForkRunID || fork.ExecutedEventCount != 1 || fork.SourceRunStatus != "paused" || duplicate.SourceRunStatus != "paused" || fork.SourceFrozen || duplicate.SourceFrozen {
						t.Fatalf("fork replay disagreement: %#v / %#v", fork, duplicate)
					}
					forkRunID = fork.ForkRunID
				} else {
					family, ok := selected.RunFork()
					if !ok {
						t.Fatal("missing fork owner")
					}
					result, err := family.Execute(servedControlProofAuthorActivityContext(t, rt), runtimerunforkexecution.SelectedContractExecutionRequest{
						SourceRunID: request.RunID, At: forkPoint, AllowSourceFreeze: true, ExpectedBundleHash: rt.BundleHash,
						SourceLoader:      runtimerunforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore()},
						ContractSelection: runforkadmission.SelectedContractSelection(semanticview.Wrap(loadWorkflowValidationBundleAt(t, root))),
						AgentRuntime:      rt.ForkRuntime,
					})
					if admitted {
						if err != nil {
							t.Fatalf("canonical admitted fork failed: %v", err)
						}
						if result.ExecutedEventCount != 1 {
							t.Fatalf("fork execution count: %d", result.ExecutedEventCount)
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
				checkSource("after supported fork")
				rootType, childType, rootState := "work", "receipt", "done"
				var rootFields, childFields map[string]any = map[string]any{}, map[string]any{}
				if fieldless {
					rootType, childType, rootState = "", "", "pending"
					rootFields, childFields = nil, nil
				}
				rootPhase := "planned"
				rootRevision := 2
				if fieldless {
					// Historical source-only headers do not acquire attachment.
					rootPhase = ""
					rootRevision = 1
				}
				requireReceiverConstructedInstance(t, rt, forkRunID, forkRunID, ".", forkRunID, rootType, rootState, "", rootPhase, rootRevision, rootFields)
				requireReceiverConstructedInstance(t, rt, forkRunID, "sink", "sink", flowidentity.EntityID("sink"), childType, "pending", forkRunID, "ready", 2, childFields)
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
