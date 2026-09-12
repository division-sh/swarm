package serveapp

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Source artifact admission is private setup, not proof of public publication.
// Publication, pause, fork and readback below use the assembled HTTP surface.
func TestSelectedForkPendingInputBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, mode := range []string{"unchanged", "compatible", "incompatible", "missing_input", "ambiguous_artifact", "explicit_child", "ordinary_child", "ordinary_root", "reference", "pause_resume"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				var selected *selectedStoreOwner
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence { selected = owner; return previous(owner) }
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				variant := canonicalrouting.PendingInputOriginal
				if mode == "ordinary_root" {
					variant = canonicalrouting.PendingInputOrdinaryRoot
				}
				root := canonicalrouting.CopySelectedForkPendingInput(t, variant)
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "input-seed",
				})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				requirePendingInputStateCount(t, rt, seed.RunID, "ready", 2)
				requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "input-pause"})
				params := map[string]any{"event_name": "work.first", "run_id": seed.RunID, "payload": map[string]any{"token": "proof"}, "idempotency_key": "input-pending"}
				wantDone := 2
				if mode == "reference" {
					params["source_event_id"] = seed.EventID
				}
				if mode == "ordinary_root" {
					wantDone = 1
				}
				if mode == "ordinary_child" {
					params["event_name"] = "child/work.first"
					wantDone = 1
				}
				if mode == "explicit_child" {
					var entity string
					if err := rt.DB.QueryRow(`SELECT entity_id FROM entity_state WHERE run_id=$1 AND flow_instance='child'`, seed.RunID).Scan(&entity); err != nil {
						t.Fatal(err)
					}
					params["target"] = map[string]any{"flow_instance": "child", "entity_id": entity}
					wantDone = 1
				}
				point := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				requirePendingInputStateCount(t, rt, seed.RunID, "ready", 2)
				if mode == "ambiguous_artifact" {
					// Duplicate endpoint identity is rejected by canonical artifact
					// compilation, before a selectable artifact or fork can exist.
					before := snapshotForkReceiverApplication(t, rt)
					candidate := canonicalrouting.CopySelectedForkPendingInput(t, canonicalrouting.PendingInputDuplicateEndpoint)
					_, err := contracts.LoadWorkflowContractBundleWithOverrides(repoRootForTest(), candidate, filepath.Join(repoRootForTest(), defaultPlatformSpecPath))
					if err == nil || !strings.Contains(err.Error(), "declared more than once") {
						t.Fatalf("ambiguous input artifact: %v", err)
					}
					if !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
						t.Fatal("invalid artifact changed application state")
					}
					return
				}
				if mode == "pause_resume" {
					requireServedOKJSONRPC(t, rt.Endpoint, "run.continue", map[string]any{"run_id": seed.RunID, "idempotency_key": "input-resume"})
					waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
					requirePendingInputStateCount(t, rt, seed.RunID, "done", 2)
					requirePendingInputPublicStates(t, rt, seed.RunID, 2)
					return
				}
				forkParams := map[string]any{"source_run_id": seed.RunID, "fork_event_id": point.EventID, "allow_source_freeze": true, "idempotency_key": "input-fork"}
				if mode == "compatible" || mode == "incompatible" || mode == "missing_input" {
					variant := canonicalrouting.PendingInputCompatible
					if mode == "incompatible" {
						variant = canonicalrouting.PendingInputIncompatible
					}
					if mode == "missing_input" {
						variant = canonicalrouting.PendingInputMissingEndpoint
					}
					bundle := loadWorkflowValidationBundleAt(t, canonicalrouting.CopySelectedForkPendingInput(t, variant))
					fact, err := prepareServeSourceArtifact(servedControlProofAuthorActivityContext(t, rt), selected.SourceArtifactWriter(), bundle)
					if err != nil {
						t.Fatal(err)
					}
					if fact.BundleHash() == rt.BundleHash {
						t.Fatal("selected artifact did not change")
					}
					forkParams["bundle_hash"] = fact.BundleHash()
				}
				var before int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM runs`).Scan(&before); err != nil {
					t.Fatal(err)
				}
				response := requestServedJSONRPC(t, rt.Endpoint, "run.fork", forkParams)
				if mode == "incompatible" || mode == "missing_input" {
					if response.Error == nil {
						t.Fatal("invalid selected input was accepted")
					}
					var after int
					var status string
					if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM runs`).Scan(&after); err != nil {
						t.Fatal(err)
					}
					if err := rt.DB.QueryRow(`SELECT status FROM runs WHERE run_id=$1`, seed.RunID).Scan(&status); err != nil {
						t.Fatal(err)
					}
					if after != before || status != "paused" {
						t.Fatalf("invalid preparation mutated fork/source: runs %d -> %d, status %s", before, after, status)
					}
					requirePendingInputStateCount(t, rt, seed.RunID, "ready", 2)
					return
				}
				if response.Error != nil {
					t.Fatalf("run.fork: %+v", response.Error)
				}
				var fork apiv1.RunForkExecutionResult
				if err := json.Unmarshal(response.Result, &fork); err != nil {
					t.Fatal(err)
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
				requirePendingInputStateCount(t, rt, fork.ForkRunID, "done", wantDone)
				requirePendingInputStateCount(t, rt, fork.ForkRunID, "ready", 2-wantDone)
				requirePendingInputPublicStates(t, rt, fork.ForkRunID, wantDone)
				var copied int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='work.seeded'`, fork.ForkRunID).Scan(&copied); err != nil {
					t.Fatal(err)
				}
				if copied != 0 {
					t.Fatal("completed seed input was executed again")
				}
			})
		}
	}
}

func requirePendingInputPublicStates(t *testing.T, rt servedControlProofRuntime, runID string, wantDone int) {
	t.Helper()
	var list operatorread.OperatorEntityListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.list", map[string]any{"run_id": runID}, &list)
	if len(list.Entities) != 2 {
		t.Fatalf("public entity census: %+v", list)
	}
	done := 0
	for _, entity := range list.Entities {
		var full operatorread.OperatorEntityFull
		requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entity.EntityID}, &full)
		if entity.RunID != runID || !reflect.DeepEqual(entity, full.Entity) {
			t.Fatal("public input result lost exact run/entity identity")
		}
		if entity.CurrentState == "done" {
			done++
		} else if entity.CurrentState != "ready" {
			t.Fatalf("unexpected public state: %+v", entity)
		}
	}
	if done != wantDone {
		t.Fatalf("public done count=%d want=%d", done, wantDone)
	}
}

func requirePendingInputStateCount(t *testing.T, rt servedControlProofRuntime, runID, state string, want int) {
	t.Helper()
	var count int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND current_state=$2`, runID, state).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("run %s state %s count=%d, want %d", runID, state, count, want)
	}
}
