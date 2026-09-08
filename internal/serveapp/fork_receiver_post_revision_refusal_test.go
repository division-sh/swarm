package serveapp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

type forkReceiverRefusalProbe struct{ signals atomic.Int64 }

func (p *forkReceiverRefusalProbe) NotifyLifecycle(context.Context, lifecycleprobe.Signal) {
	p.signals.Add(1)
}

// #2167 D3 replaces the unsupported success requirement, not the source graph
// or replay policy. The complete future six-arm oracle lives in testdata/
// fork_receiver_post_revision_future_test.go.txt and remains intentionally RED
// when explicitly reproduced. No CI skip or expected-failure wrapper is used.
func TestSelectedForkReceiverPostRevisionPolicyRefusalBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			root := canonicalrouting.CopyForkReceiverOwnership(t, []canonicalrouting.ForkReceiver{{Path: "consumer", Policy: canonicalrouting.ForkReceiverRequiredExisting}}, false)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "fork-refusal-seed",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "fork-refusal-request",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			var frontier, finished string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, seed.RunID).Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='consumer/receiver.finished' AND source_event_id=$2`, seed.RunID, frontier).Scan(&finished); err != nil {
				t.Fatal(err)
			}
			sourceRows := readForkReceiverRows(t, rt, seed.RunID)
			requireForkReceiverEffect(t, rt, seed.RunID, frontier, "consumer", "consumer", "existing_entity", sourceRows["consumer"].ID)
			requireForkReceiverPostRevisionPolicyRefusal(t, rt, selected, root, seed.RunID, frontier, finished)
		})
	}
}

// Geometry callers retain their source business-effect assertions and supply the
// actual producer frontier and a proved later receiver scope. This helper never
// elects recipients from requested targets or latest source entity rows.
func requireForkReceiverPostRevisionPolicyRefusal(t *testing.T, rt servedControlProofRuntime, selected *selectedStoreOwner, root, sourceRunID, frontierEventID, laterScopeEventID string) {
	t.Helper()
	waitForkReceiverSourceCompletion(t, rt, sourceRunID)
	var frontierEventIDRevision, scopeRevision int64
	if err := rt.DB.QueryRow(`SELECT MIN(revision) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2 AND present`, sourceRunID, frontierEventID).Scan(&frontierEventIDRevision); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT MIN(revision) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='committed_replay_scopes' AND fact_key=$2 AND present`, sourceRunID, laterScopeEventID).Scan(&scopeRevision); err != nil || scopeRevision <= frontierEventIDRevision {
		t.Fatalf("fixture lacks exact later committed receiver scope: frontierEventID=%d scope=%d err=%v", frontierEventIDRevision, scopeRevision, err)
	}
	sourceBefore := readServedForkRecipientSourceDomain(t, rt, sourceRunID)
	var sourceStatus string
	if err := rt.DB.QueryRow(`SELECT status FROM runs WHERE run_id=$1`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatal(err)
	}
	family, ok := selected.RunFork()
	if !ok {
		t.Fatal("missing actual selected fork owner")
	}
	probe := &forkReceiverRefusalProbe{}
	ctx, cancel := context.WithTimeout(servedControlProofAuthorActivityContext(t, rt), servedProofPollDeadline)
	defer cancel()
	result, err := family.Execute(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: sourceRunID, At: frontierEventID, ConfirmSourceFreeze: true, ExpectedBundleHash: rt.BundleHash,
		SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{
			RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore(),
		},
		ContractSelection: runforkadmission.SelectedContractSelection(semanticview.Wrap(loadWorkflowValidationBundleAt(t, root))),
		AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{
			ExecutionPosture: rt.Runtime.ExecutionPosture, AgentManagerOptions: runtimemanager.AgentManagerOptions{TestLifecycleProbe: probe},
		},
	})
	const refusal = "selected-contract committed replay-scope marker policy blocked: source_committed_replay_scope_advanced_after_fork_point"
	if err == nil || err.Error() != refusal {
		t.Fatalf("want exact store-owned post-revision refusal, got %v", err)
	}
	materialized := result.Materialization
	child := materialized.ForkRunID
	if child == "" || child == sourceRunID || materialized.SourceRunID != sourceRunID || materialized.ForkPoint.EventID != frontierEventID || materialized.ForkPoint.Revision != frontierEventIDRevision || materialized.MaterializedEntityCount <= 0 || materialized.SelectedContractBinding == nil {
		t.Fatalf("refusal did not follow exact lawful materialization: %+v", materialized)
	}
	if result.ExecutedEventCount != 0 || len(result.ForkEvents) != 0 || probe.signals.Load() != 0 || result.Activation.Activated || result.Activation.SourceFrozen || result.ForkLocalRuntimeContainer == nil {
		t.Fatalf("refused fork executed or activated: events=%d publications=%d signals=%d activation=%+v", result.ExecutedEventCount, len(result.ForkEvents), probe.signals.Load(), result.Activation)
	}
	var childStatus, executionState, failureJSON string
	var ended, terminal, leaseReleased bool
	if err := rt.DB.QueryRow(`SELECT status,ended_at IS NOT NULL FROM runs WHERE run_id=$1 AND forked_from_run_id=$2`, child, sourceRunID).Scan(&childStatus, &ended); err != nil || childStatus != "cancelled" || !ended {
		t.Fatalf("refusal did not retain a terminal child tombstone: status=%s ended=%t err=%v", childStatus, ended, err)
	}
	if err := rt.DB.QueryRow(`SELECT state,terminal_at IS NOT NULL,lease_expires_at IS NULL,CAST(failure AS TEXT) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1 AND source_run_id=$2 AND fork_event_id=$3`, child, sourceRunID, frontierEventID).Scan(&executionState, &terminal, &leaseReleased, &failureJSON); err != nil || executionState != "closed" || !terminal || !leaseReleased {
		t.Fatalf("selected runtime authority was not closed: state=%s terminal=%t leaseReleased=%t err=%v", executionState, terminal, leaseReleased, err)
	}
	var failure runtimefailures.Envelope
	// The completion envelope intentionally does not retain an arbitrary
	// error string. The exact store refusal is asserted above, separately.
	if err := json.Unmarshal([]byte(failureJSON), &failure); err != nil || failure.Class != runtimefailures.ClassInternalFailure || failure.Detail.Code != "unclassified_runtime_error" || failure.Component != runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner || failure.Operation != "execute" {
		t.Fatalf("retained completion lost its canonical failure owner: %+v err=%v", failure, err)
	}
	// Retained revision facts distinguish never-published from published
	// then deleted. Entity materialization itself is lawful nonzero work.
	var materializedOwners, removedOwners int
	if err := rt.DB.QueryRow(`SELECT COUNT(DISTINCT fact_key) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND present`, child).Scan(&materializedOwners); err != nil || materializedOwners != materialized.MaterializedEntityCount {
		t.Fatalf("missing committed materialization evidence: owners=%d err=%v", materializedOwners, err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(DISTINCT fact_key) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND NOT present`, child).Scan(&removedOwners); err != nil || removedOwners != materializedOwners {
		t.Fatalf("cleanup lost materialized owner tombstones: removed=%d owners=%d err=%v", removedOwners, materializedOwners, err)
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family IN ('events','event_deliveries','committed_replay_scopes','event_receipts','dead_letters') AND present`,
		`SELECT COUNT(*) FROM events WHERE run_id=$1`,
		`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`,
		`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`,
		`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1`,
		`SELECT COUNT(*) FROM activity_attempts WHERE run_id=$1`,
		`SELECT COUNT(*) FROM timers WHERE run_id=$1`,
		`SELECT COUNT(*) FROM agents WHERE run_id=$1`,
		`SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id=$1`,
		`SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id=$1`,
		`SELECT COUNT(*) FROM runtime_external_effect_operations WHERE selected_execution_id IN (SELECT execution_id FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1)`,
	} {
		var count int
		if err := rt.DB.QueryRow(query, child).Scan(&count); err != nil || count != 0 {
			t.Fatalf("refused fork retained work or published before cleanup: count=%d query=%s err=%v", count, query, err)
		}
	}
	// Discard retains companion metadata with the cancelled run tombstone.
	// Ask the actual readiness owner whether it remains eligible; an active
	// status label alone is neither a grant nor proof of executable work.
	projection, err := rt.Runtime.Manager.InspectDynamicFlowRuntimeReadinessForSource(ctx, rt.Runtime.Options.SourceArtifactFact)
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range [][]runtimepipeline.DynamicFlowRuntimeReadiness{projection.CurrentCompleted, projection.CurrentPending, projection.SourceTransitionRequired} {
		for _, item := range group {
			if item.Plan.RunID == child {
				t.Fatalf("discarded child retained executable readiness: %+v", item)
			}
		}
	}

	if !reflect.DeepEqual(sourceBefore, readServedForkRecipientSourceDomain(t, rt, sourceRunID)) {
		t.Fatal("refusal or cleanup mutated completed source domain")
	}
	var afterStatus string
	if err := rt.DB.QueryRow(`SELECT status FROM runs WHERE run_id=$1`, sourceRunID).Scan(&afterStatus); err != nil || afterStatus != sourceStatus {
		t.Fatalf("refusal froze or changed source status: before=%s after=%s err=%v", sourceStatus, afterStatus, err)
	}
	t.Logf("exact refusal after lawful materialization: frontierEventID_revision=%d later_scope_revision=%d owners=%d removed=%d selected_publications=0", frontierEventIDRevision, scopeRevision, materializedOwners, removedOwners)
}
