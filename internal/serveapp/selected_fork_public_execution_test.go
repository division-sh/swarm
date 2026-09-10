package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Artifact admission below is private setup. This proves the public fork and
// readback boundary, not the publication-to-fork journey owned by #2376/#2322.
func TestSelectedForkPublicChangedTargetExecutionBothStores(t *testing.T) {
	proveSelectedForkPublicChangedTargetExecutionBothStores(t, false)
}

func TestSelectedForkPublicChangedTargetAfterResetBothStores(t *testing.T) {
	proveSelectedForkPublicChangedTargetExecutionBothStores(t, true)
}

func proveSelectedForkPublicChangedTargetExecutionBothStores(t *testing.T, reset bool) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			supervisors := make(chan *processLifecycleSupervisor, 1)
			if reset {
				prior := buildSelectedAPICapabilities
				var once sync.Once
				buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
					once.Do(func() { supervisors <- req.RuntimeSupervisor })
					return prior(owner, req)
				}
				t.Cleanup(func() { buildSelectedAPICapabilities = prior })
			}
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
			writeSelectedForkAgentProofFixture(t, root, "loaded-decoy", "[receiver.closed]", "SOURCE CONFIGURATION MUST NOT EXECUTE IN THE FORK", "return {'text': 'Source-only delivery.', 'usage': {'input_tokens': 2, 'output_tokens': 2}}")
			rt := startServedTestSetupEntitiesProofRuntimeWithWorkspace(t, backend, root, true)
			if reset {
				_, rt.Postgres, rt.SQLite = selectedRuntimeStoreForTest(t, projectServeRuntimePersistence(selected))
				supervisor := <-supervisors
				predecessor := supervisor.selected
				params := map[string]any{"include_source_artifacts": false, "idempotency_key": "selected-reset"}
				response := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", params)
				if response.Error != nil {
					t.Fatalf("retained reset: %+v", response.Error)
				}
				supervisor.operationMu.Lock()
				successor := supervisor.selected
				supervisor.operationMu.Unlock()
				if predecessor == nil || successor == nil || predecessor == successor {
					t.Fatal("reset reused selected family instead of constructing a successor")
				}
				if _, _, err := predecessor.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: "late-predecessor"}); !errors.Is(err, worklifetime.ErrRetired) {
					t.Fatalf("predecessor stop did not remain retired: %v", err)
				}
				expireServedResetTransportCache(t, rt)
				replay := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", params)
				if replay.Error != nil || !reflect.DeepEqual(response.Result, replay.Result) {
					t.Fatalf("reset outcome replay: %+v", replay)
				}
				supervisor.operationMu.Lock()
				same := supervisor.selected == successor
				supervisor.operationMu.Unlock()
				if !same {
					t.Fatal("completed reset retry replaced its selected successor")
				}
				rt.Runtime = supervisor.CurrentRuntime()
			}
			declarations := semanticview.AgentDeclarations(rt.Runtime.Options.WorkflowModule.SemanticSource())
			if len(declarations) != 1 || declarations[0].LocalID != "same-name" || declarations[0].Entry.Role != "loaded-decoy" {
				t.Fatalf("loaded source declaration is not the same-name decoy: %+v", declarations)
			}
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "public-selected-seed",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "public-selected-request",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			var frontier string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, seed.RunID).Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			sourceBefore := readServedForkRecipientSourceDomain(t, rt, seed.RunID)
			targetRoot := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
			writeSelectedForkAgentProofFixture(t, targetRoot, "observer", "[work.ready]", "Observe the explicitly delivered closure event.", "return {'text': 'Observed delivery.', 'usage': {'input_tokens': 1, 'output_tokens': 1}}")
			target := loadWorkflowValidationBundleAt(t, targetRoot)
			fact, err := prepareServeSourceArtifact(servedControlProofAuthorActivityContext(t, rt), selected.SourceArtifactWriter(), target)
			if err != nil || fact.BundleHash() == rt.BundleHash {
				t.Fatalf("admit distinct target fixture: hash=%q err=%v", fact.BundleHash(), err)
			}
			fork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, map[string]any{
				"source_run_id": seed.RunID, "fork_event_id": frontier, "bundle_hash": fact.BundleHash(),
				"allow_source_freeze": true, "idempotency_key": "public-selected-changed-target",
			})
			if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 1 {
				t.Fatalf("public fork lost execution/source evidence: %+v", fork)
			}
			var childEvent, childHash string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, fork.ForkRunID).Scan(&childEvent); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT bundle_hash FROM runs WHERE run_id=$1`, fork.ForkRunID).Scan(&childHash); err != nil || childHash != fact.BundleHash() {
				t.Fatalf("public fork selected a loaded source instead of target: hash=%q err=%v", childHash, err)
			}
			rows := readForkReceiverRows(t, rt, fork.ForkRunID)
			requireSelectedForkMixedBusinessMutation(t, rt, fork.ForkRunID, childEvent, rows["consumer"].ID)
			if !reflect.DeepEqual(sourceBefore, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) {
				t.Fatal("public selected execution changed source domain")
			}
			requireSelectedForkPublicControlBoundary(t, rt, fork.ForkRunID, childEvent, true)
			t.Run("terminal_public_readback", func(t *testing.T) {
				requireSelectedForkDeclaredAgentReads(t, rt, fork.ForkRunID)
				if !reflect.DeepEqual(sourceBefore, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) {
					t.Fatal("terminal selected readback changed source domain")
				}
			})
		})
	}
}

func requireSelectedForkExecutionRPCResult(t *testing.T, endpoint string, params map[string]any) apiv1.RunForkExecutionResult {
	t.Helper()
	// Full preparation/execution uses the same bounded operation budget as the
	// dev-scratch and pre-audit fork controls, including under race instrumentation.
	started := time.Now()
	response := requestServedJSONRPCWithTimeout(t, endpoint, "run.fork", params, 30*time.Second)
	t.Logf("run.fork preparation and execution completed in %s", time.Since(started))
	if response.Error != nil {
		t.Fatalf("run.fork error = %#v", response.Error)
	}
	var result apiv1.RunForkExecutionResult
	if err := json.Unmarshal(response.Result, &result); err != nil {
		t.Fatalf("decode run.fork result: %v", err)
	}
	return result
}
