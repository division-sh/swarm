package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Artifact admission below is private setup. This proves the public fork and
// readback boundary, not the publication-to-fork journey owned by #2376/#2322.
func TestSelectedForkPublicChangedTargetExecutionBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
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
