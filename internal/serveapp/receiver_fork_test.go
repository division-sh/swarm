package serveapp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestReceiverCompositionForkBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, surface := range []string{"admitted", "admitted_http", "historical_refusal", "pending_refusal", "empty_snapshot_refusal"} {
			t.Run(string(backend)+"/"+surface, func(t *testing.T) {
				admitted := strings.HasPrefix(surface, "admitted")
				var selected *selectedStoreOwner
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
					selected = owner
					return previous(owner)
				}
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				root := canonicalrouting.CopyReceiverOptionalChild(t, true)
				if surface == "empty_snapshot_refusal" {
					root = canonicalrouting.CopyReceiverEntitylessFork(t)
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
				if surface != "empty_snapshot_refusal" {
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "fork-seed"})
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "active")
					if admitted {
						waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
						requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "receiver-fork-pause"})
					}
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
				var forkRunID string
				var beforeState, beforeFields string
				var beforeRevision int
				if surface != "empty_snapshot_refusal" {
					if err := rt.DB.QueryRow(`SELECT current_state,revision,CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1`, request.RunID).Scan(&beforeState, &beforeRevision, &beforeFields); err != nil {
						t.Fatal(err)
					}
				}
				checkSource := func() {
					if surface == "empty_snapshot_refusal" {
						return
					}
					var state, fields string
					var revision int
					if err := rt.DB.QueryRow(`SELECT current_state,revision,CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1`, request.RunID).Scan(&state, &revision, &fields); err != nil {
						t.Fatal(err)
					}
					if state != beforeState || fields != beforeFields || revision != beforeRevision {
						t.Fatal("fork mutated source entity state")
					}
				}
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
						SourceRunID: request.RunID, At: request.EventID, AllowSourceFreeze: true, ExpectedBundleHash: rt.BundleHash,
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
						if surface == "empty_snapshot_refusal" {
							want = "requires materialized fork entity_state rows"
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
						for _, table := range []string{"events", "event_deliveries", "entity_state"} {
							var count int
							if err := rt.DB.QueryRow(`SELECT count(*) FROM `+table+` WHERE run_id=$1`, result.Materialization.ForkRunID).Scan(&count); err != nil {
								t.Fatal(err)
							}
							if count != 0 {
								t.Fatalf("refused fork retained %s: %d", table, count)
							}
						}
						checkSource()
						return
					}
				}
				if forkRunID == "" || forkRunID == request.RunID {
					t.Fatalf("fork did not execute selected receiver: %s", forkRunID)
				}
				checkSource()
				var entityID, instance, state string
				if err := rt.DB.QueryRow(`SELECT entity_id,flow_instance,current_state FROM entity_state WHERE run_id=$1`, forkRunID).Scan(&entityID, &instance, &state); err != nil {
					t.Fatal(err)
				}
				if entityID != forkRunID || instance != forkRunID || state != "done" {
					t.Fatalf("fork receiver was not reminted into its run: %s/%s/%s", entityID, instance, state)
				}
				var childRows int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id=$1 AND flow_instance='sink'`, forkRunID).Scan(&childRows); err != nil {
					t.Fatal(err)
				}
				if childRows != 0 {
					t.Fatal("fork fabricated optional child state")
				}
				requireReceiverPublicReadback(t, rt, forkRunID)
				requireReceiverPublicReadback(t, rt, request.RunID)
			})
		}
	}
}
