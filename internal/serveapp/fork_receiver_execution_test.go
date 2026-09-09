package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/servedparity"
)

type forkReceiverExecutionObservation struct {
	ctx    context.Context
	signal lifecycleprobe.Signal
}

type forkReceiverExecutionBarrier struct {
	claimed, settled                chan forkReceiverExecutionObservation
	resumeClaim, resumeSettlement   chan struct{}
	claimOnce, settlementOnce       sync.Once
	releaseClaim, releaseSettlement sync.Once
}

func (b *forkReceiverExecutionBarrier) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.DeliveryStatusChanged || signal.EventType != "producer/work.ready" || signal.SubscriberType != "node" {
		return
	}
	observe := func(out chan forkReceiverExecutionObservation, resume chan struct{}) {
		select {
		case out <- forkReceiverExecutionObservation{ctx: ctx, signal: signal}:
		case <-ctx.Done():
			return
		}
		select {
		case <-resume:
		case <-ctx.Done():
		}
	}
	switch signal.Status {
	case "in_progress":
		b.claimOnce.Do(func() { observe(b.claimed, b.resumeClaim) })
	case "delivered", "dead_letter":
		b.settlementOnce.Do(func() { observe(b.settled, b.resumeSettlement) })
	}
}

// The original post-revision emitting success oracle is preserved explicitly in
// testdata/fork_receiver_post_revision_future_test.go.txt. Current policy is
// exercised by TestSelectedForkReceiverPostRevisionPolicyRefusalBothStores.

// This effect-only companion supplements the archived emitting-path proof; it does
// not discharge that path's post-frontier committed replay-scope refusal.
func TestSelectedForkReceiverEffectOnlyFailureSettlementBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, name := range []string{"available_control", "declared_agent_control", "unavailable_after_commit", "newer_claim_store_fence"} {
			unavailable := name == "unavailable_after_commit" || name == "newer_claim_store_fence"
			newerClaim := name == "newer_claim_store_fence"
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				var selected *selectedStoreOwner
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
					selected = owner
					return previous(owner)
				}
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
				// Authored state mutation reaches the supported fork surface without
				// a post-frontier publication crossing the replay-scope policy.
				root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
				targetRoot := root
				if name == "declared_agent_control" {
					targetRoot = canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
					if err := os.MkdirAll(filepath.Join(targetRoot, "consumer", "prompts"), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.MkdirAll(filepath.Join(targetRoot, "consumer", "mocks"), 0o755); err != nil {
						t.Fatal(err)
					}
					for file, contents := range map[string]string{
						"agents.yaml":         "same-name:\n  id: same-name\n  role: observer\n  model: regular\n  intent: prompts/observer.md\n  subscriptions: [work.ready]\n  emit_events: []\n  mock:\n    kind: python\n    module: mocks/observer.py\n",
						"prompts/observer.md": "Observe the explicitly delivered closure event.\n",
						"mocks/observer.py":   "def handle(input):\n    return {'text': 'Observed delivery.', 'usage': {'input_tokens': 1, 'output_tokens': 1}}\n",
					} {
						if err := os.WriteFile(filepath.Join(targetRoot, "consumer", file), []byte(contents), 0o644); err != nil {
							t.Fatal(err)
						}
					}
				}
				checkBusiness := func(t *testing.T, rt servedControlProofRuntime, runID, eventID, entityID string) {
					if name == "declared_agent_control" {
						requireSelectedForkMixedBusinessMutation(t, rt, runID, eventID, entityID)
					} else {
						requireForkReceiverBusinessMutation(t, rt, runID, eventID, entityID)
					}
				}
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
					"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "fork-settlement-seed",
				})
				waitForkReceiverSourceCompletion(t, rt, seed.RunID)
				requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
					"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "fork-settlement-request",
				})
				waitForkReceiverSourceCompletion(t, rt, seed.RunID)
				var frontier string
				if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, seed.RunID).Scan(&frontier); err != nil {
					t.Fatal(err)
				}
				sourceRows := readForkReceiverRows(t, rt, seed.RunID)
				requireForkReceiverBusinessMutation(t, rt, seed.RunID, frontier, sourceRows["consumer"].ID)
				sourceBefore := readServedForkRecipientSourceDomain(t, rt, seed.RunID)
				family, ok := selected.RunFork()
				if !ok {
					t.Fatal("missing selected fork owner")
				}
				barrier := &forkReceiverExecutionBarrier{
					claimed: make(chan forkReceiverExecutionObservation, 1), settled: make(chan forkReceiverExecutionObservation, 1),
					resumeClaim: make(chan struct{}), resumeSettlement: make(chan struct{}),
				}
				resumeClaim := func() { barrier.releaseClaim.Do(func() { close(barrier.resumeClaim) }) }
				resumeSettlement := func() { barrier.releaseSettlement.Do(func() { close(barrier.resumeSettlement) }) }
				t.Cleanup(resumeClaim)
				t.Cleanup(resumeSettlement)
				ctx, cancel := context.WithTimeout(servedControlProofAuthorActivityContext(t, rt), 30*time.Second)
				defer cancel()
				forkOptions := rt.ForkRuntime
				if name == "declared_agent_control" {
					manager := workspace.NewHostManager()
					cfg := workspace.DefaultHostConfig()
					cfg.WorkspaceRoot = t.TempDir()
					manager.SetConfig(cfg)
					forkOptions.Workspace = manager
				}
				forkOptions.AgentManagerOptions = runtimemanager.AgentManagerOptions{TestLifecycleProbe: barrier}
				request := runforkexecution.SelectedContractExecutionRequest{
					SourceRunID: seed.RunID, At: frontier, AllowSourceFreeze: true, ExpectedBundleHash: rt.BundleHash,
					SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{
						RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore(),
					},
					ContractSelection: runforkadmission.SelectedContractSelection(semanticview.Wrap(loadWorkflowValidationBundleAt(t, root))),
					AgentRuntime:      forkOptions,
				}
				if name == "declared_agent_control" {
					target := loadWorkflowValidationBundleAt(t, targetRoot)
					fact, err := prepareServeSourceArtifact(ctx, selected.SourceArtifactWriter(), target)
					if err != nil || fact.BundleHash() == rt.BundleHash {
						t.Fatalf("register distinct unloaded target: %v", err)
					}
					request.ContractSelection = runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: fact.BundleHash()}
					request.ExpectedBundleHash = fact.BundleHash()
				}
				type executionResult struct {
					result runforkexecution.SelectedContractExecutionResult
					err    error
				}
				finished := make(chan executionResult, 1)
				executionDone := make(chan struct{})
				go func() {
					defer close(executionDone)
					result, err := family.Execute(ctx, request)
					finished <- executionResult{result: result, err: err}
				}()
				t.Cleanup(func() {
					resumeClaim()
					resumeSettlement()
					cancel()
					select {
					case <-executionDone:
					case <-time.After(10 * time.Second):
						t.Error("fork execution did not join test cleanup")
					}
				})
				await := func(ch <-chan forkReceiverExecutionObservation) forkReceiverExecutionObservation {
					t.Helper()
					select {
					case observation := <-ch:
						return observation
					case result := <-finished:
						t.Fatalf("fork returned before receiver barrier: %v", result.err)
					case <-ctx.Done():
						t.Fatalf("fork receiver barrier: %v", ctx.Err())
					}
					return forkReceiverExecutionObservation{}
				}
				claimed := await(barrier.claimed)
				claim, ok := runtimedelivery.ClaimFromContext(claimed.ctx)
				if !ok || claim.Validate() != nil || claim.RunID() == seed.RunID {
					t.Fatal("barrier did not receive a real child-fork claim")
				}
				admission, ok := managedexecution.FromContext(claimed.ctx)
				if !ok || admission.Kind != managedexecution.KindSelectedContractFork || admission.RunID != claim.RunID() {
					t.Fatal("receiver did not retain actual selected-fork execution context")
				}
				var rawTarget, status string
				var version int64
				if err := rt.DB.QueryRow(`SELECT CAST(delivery_target_route AS TEXT),status,claim_version FROM event_deliveries WHERE delivery_id=$1 AND run_id=$2`, claim.DeliveryID(), claim.RunID()).Scan(&rawTarget, &status, &version); err != nil {
					t.Fatal(err)
				}
				var target events.DeliveryTargetOwnership
				if err := json.Unmarshal([]byte(rawTarget), &target); err != nil {
					t.Fatal(err)
				}
				if !target.ExistingEntity() || target.Route().FlowID != "consumer" || target.Route().FlowInstance != "consumer" || status != "in_progress" || version != claim.Version() {
					t.Fatalf("claim lacks exact committed receiving target: %s status=%s version=%d", rawTarget, status, version)
				}
				forkRows := readForkReceiverRows(t, rt, claim.RunID())
				receiver := forkRows["consumer"]
				if receiver.ID != target.Route().EntityID || receiver.State != "active" || receiver.Type != "receipt" || receiver.Fields["marker"] != "consumer-owned" || receiver.Fields["processed_token"] != "seeded" || receiver.ID == forkRows["producer"].ID {
					t.Fatalf("fork preparation borrowed producer state: %+v", forkRows)
				}
				var beforeFields string
				var beforeRevision int
				if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT),revision FROM entity_state WHERE run_id=$1 AND entity_id=$2`, claim.RunID(), receiver.ID).Scan(&beforeFields, &beforeRevision); err != nil {
					t.Fatal(err)
				}
				if unavailable {
					// Invalidate the real receiver only after materialization, publication
					// and claim commit. Its target and business fields remain untouched.
					result, err := rt.DB.Exec(`UPDATE entity_state SET current_state='done' WHERE run_id=$1 AND entity_id=$2 AND current_state='active'`, claim.RunID(), receiver.ID)
					if err != nil {
						t.Fatal(err)
					}
					if count, err := result.RowsAffected(); err != nil || count != 1 {
						t.Fatalf("invalidate exact child: count=%d err=%v", count, err)
					}
				}
				wantStatus := "delivered"
				if unavailable {
					wantStatus = "dead_letter"
				}
				if newerClaim {
					requireForkReceiverStaleClaimStoreFence(t, rt, selected, claimed, claim)
				}
				resumeClaim()
				if !newerClaim {
					settled := await(barrier.settled)
					settledClaim, ok := runtimedelivery.ClaimFromContext(settled.ctx)
					if !ok || !settledClaim.Same(claim) || settled.signal.EventID != claimed.signal.EventID || settled.signal.Status != wantStatus {
						t.Fatalf("receiver settled a different claim or disposition: %+v, want %s", settled.signal, wantStatus)
					}
					var afterTarget, afterFields string
					var afterRevision, outcomes, openAttempts int
					if err := rt.DB.QueryRow(`SELECT CAST(delivery_target_route AS TEXT),status,claim_version FROM event_deliveries WHERE delivery_id=$1`, claim.DeliveryID()).Scan(&afterTarget, &status, &version); err != nil {
						t.Fatal(err)
					}
					if status != wantStatus || version != claim.Version() || afterTarget != rawTarget {
						t.Fatalf("settlement changed ownership/version: status=%s version=%d target=%s", status, version, afterTarget)
					}
					if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=$1 AND claim_version=$2 AND outcome=$3`, claim.DeliveryID(), claim.Version(), wantStatus).Scan(&outcomes); err != nil || outcomes != 1 {
						t.Fatalf("exact claim outcomes=%d err=%v", outcomes, err)
					}
					if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1 AND open_marker=TRUE`, claim.DeliveryID()).Scan(&openAttempts); err != nil || openAttempts != 0 {
						t.Fatalf("settled receiver retained open attempts=%d err=%v", openAttempts, err)
					}
					if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT),revision FROM entity_state WHERE run_id=$1 AND entity_id=$2`, claim.RunID(), receiver.ID).Scan(&afterFields, &afterRevision); err != nil {
						t.Fatal(err)
					}
					if unavailable {
						if afterFields != beforeFields || afterRevision != beforeRevision {
							t.Fatal("unavailable receiver settlement mutated business fields or revision")
						}
						receiver.State = "done"
					} else {
						if afterRevision != beforeRevision+1 {
							t.Fatalf("available receiver mutation revision=%d want=%d", afterRevision, beforeRevision+1)
						}
						receiver.Fields["processed_token"] = "receiver-proof"
					}
					forkRows["consumer"] = receiver
					if !reflect.DeepEqual(forkRows, readForkReceiverRows(t, rt, claim.RunID())) {
						t.Fatal("receiver execution changed child or producer state beyond its exact authored write or availability fault")
					}
					if unavailable {
						var emitted int
						if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND NOT (event_class=$3 AND event_name=$4)`, claim.RunID(), claimed.signal.EventID, string(events.EventAdmissionDiagnosticDirect), string(events.EventTypePlatformRuntimeLog)).Scan(&emitted); err != nil || emitted != 0 {
							t.Fatalf("unavailable receiver emitted business work=%d err=%v", emitted, err)
						}
					}
				}
				resumeSettlement()
				select {
				case execution := <-finished:
					if execution.err != nil {
						t.Fatalf("settled fork failed to finish: %v", execution.err)
					}
					if execution.result.Materialization.ForkRunID != claim.RunID() {
						t.Fatal("returned fork differs from actual receiver claim")
					}
				case <-ctx.Done():
					t.Fatalf("settled fork did not relinquish execution: %v", ctx.Err())
				}
				waitForkReceiverExecutionCompletion(t, rt, selected, ctx, claim, unavailable)
				wantReceipt := "success"
				if unavailable {
					wantReceipt = "dead_letter"
				}
				waitServedEventPublishReceiptOutcomeCount(t, rt.DB, rt.Backend, claimed.signal.EventID, "platform", "pipeline", wantReceipt, 1)
				if newerClaim {
					var count int
					if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND NOT (event_class=$3 AND event_name=$4)`, claim.RunID(), claimed.signal.EventID, string(events.EventAdmissionDiagnosticDirect), string(events.EventTypePlatformRuntimeLog)).Scan(&count); err != nil || count != 0 {
						t.Fatalf("stale runtime claim emitted business work=%d err=%v", count, err)
					}
					receiver.State = "done"
					forkRows["consumer"] = receiver
					if !reflect.DeepEqual(forkRows, readForkReceiverRows(t, rt, claim.RunID())) {
						t.Fatal("stale runtime claim changed receiver or producer state")
					}
					var fields string
					var revision int
					if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT),revision FROM entity_state WHERE run_id=$1 AND entity_id=$2`, claim.RunID(), receiver.ID).Scan(&fields, &revision); err != nil || fields != beforeFields || revision != beforeRevision {
						t.Fatalf("stale runtime claim changed business fields/revision: %s revision=%d err=%v", fields, revision, err)
					}
				}
				if !unavailable {
					checkBusiness(t, rt, claim.RunID(), claimed.signal.EventID, receiver.ID)
				}
				if !reflect.DeepEqual(sourceBefore, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) {
					t.Fatal("fork receiver execution or settlement mutated source domain")
				}
				if !unavailable {
					requireSelectedForkPublicControlBoundary(t, rt, claim.RunID(), claimed.signal.EventID, name == "declared_agent_control")
				}
			})
		}
	}
}

func requireSelectedForkPublicControlBoundary(t *testing.T, rt servedControlProofRuntime, runID, eventID string, declaredAgent bool) {
	t.Helper()
	var bindingID string
	if err := rt.DB.QueryRow(`SELECT binding_id FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1`, runID).Scan(&bindingID); err != nil {
		t.Fatal(err)
	}
	type controlCase struct {
		method string
		params map[string]any
	}
	cases := []controlCase{
		{"run.pause", map[string]any{"run_id": runID}},
		{"run.continue", map[string]any{"run_id": runID}},
		{"agent.restart", map[string]any{"run_id": runID, "agent_id": "same-name"}},
		{"agent.send_directive", map[string]any{"run_id": runID, "agent_id": "same-name", "directive": "must not execute"}},
		{"event.publish", map[string]any{"run_id": runID, "event_name": "start.requested", "source_event_id": eventID, "payload": map[string]any{"token": "must-not-execute"}}},
		{"event.replay", map[string]any{"event_id": eventID}},
	}
	if declaredAgent {
		var count int
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM agents WHERE run_id=$1 AND agent_id='same-name'`, runID).Scan(&count); err != nil || count != 1 {
			t.Logf("agent rows: %v", snapshotForkReceiverApplication(t, rt)["agents"])
			t.Fatalf("selected declaration did not materialize the control target: count=%d err=%v", count, err)
		}
		cases = append(cases, controlCase{"agent.replay", map[string]any{"run_id": runID, "agent_id": "same-name", "event_id": eventID}})
		requireSelectedForkDeclaredAgentReads(t, rt, runID)
	}
	for _, test := range cases {
		t.Run(test.method, func(t *testing.T) {
			before := snapshotForkReceiverApplication(t, rt)
			err := requireServedJSONRPCError(t, rt.Endpoint, test.method, test.params)
			if err.Data["code"] != "SELECTED_FORK_CONTROL_UNSUPPORTED" {
				t.Fatalf("selected control escaped through loaded same-hash runtime: %+v", err)
			}
			details, ok := err.Data["details"].(map[string]any)
			if !ok || details["operation"] != test.method || details["run_id"] != runID || details["binding_id"] != bindingID {
				t.Fatalf("selected refusal lost exact binding: %+v", err)
			}
			after := snapshotForkReceiverApplication(t, rt)
			if !reflect.DeepEqual(before, after) {
				for table, rows := range before {
					if !reflect.DeepEqual(rows, after[table]) {
						t.Errorf("refused %s changed %s: before=%v after=%v", test.method, table, rows, after[table])
					}
				}
				t.Fatal("refused selected control changed the database")
			}
		})
	}
	var result struct {
		OK bool `json:"ok"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "run.stop", map[string]any{"run_id": runID, "idempotency_key": "selected-exact-stop"}, &result)
	if !result.OK {
		t.Fatal("selected public stop did not return its terminal result")
	}
	var terminal struct {
		Run struct {
			Status string `json:"status"`
		} `json:"run"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "run.get", map[string]any{"run_id": runID}, &terminal)
	if terminal.Run.Status != "cancelled" {
		t.Fatalf("public selected stop readback = %+v", terminal)
	}
}

func requireSelectedForkDeclaredAgentReads(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	before := snapshotForkReceiverApplication(t, rt)
	params := map[string]any{"run_id": runID, "agent_id": "same-name", "flow_instance": "consumer"}
	var detail operatorread.OperatorAgentDetail
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.get", params, &detail)
	if detail.Agent.AgentID != "same-name" || detail.Agent.FlowInstance != "consumer" || detail.Agent.Role != "observer" || detail.Agent.ExecutionMode != "mock" {
		t.Fatalf("selected agent read lost persisted target: %+v", detail)
	}
	var diagnosis operatorread.OperatorAgentDiagnosis
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.diagnose", params, &diagnosis)
	if diagnosis.AgentID != "same-name" || diagnosis.Status != detail.Agent.Status || diagnosis.Queue.PendingCount != 0 {
		t.Fatalf("selected agent diagnosis disagrees with settled target: %+v", diagnosis)
	}
	var diagnostics operatorread.OperatorAgentDeliveryDiagnostics
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.delivery_diagnostics", params, &diagnostics)
	if diagnostics.AgentID != "same-name" || len(diagnostics.Failures) != 0 || len(diagnostics.DeadLetters) != 0 {
		t.Fatalf("selected agent diagnostics include foreign or failed work: %+v", diagnostics)
	}
	var lifecycle operatorread.OperatorAgentDeliveryLifecycleList
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.delivery_lifecycle", params, &lifecycle)
	if lifecycle.AgentID != "same-name" || len(lifecycle.Deliveries) != 1 {
		t.Fatalf("selected agent lifecycle omitted its one execution: %+v", lifecycle)
	}
	if delivery := lifecycle.Deliveries[0]; delivery.RunID != runID || delivery.Status != "delivered" || delivery.EventName != "producer/work.ready" {
		t.Fatalf("selected agent lifecycle borrowed another run: %+v", delivery)
	}
	var usage operatorread.OperatorAgentUsage
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.usage", params, &usage)
	if usage.AgentID != "same-name" || usage.Usage.Estimated.LedgerEntries != 1 || usage.Usage.Exact.LedgerEntries != 0 {
		t.Fatalf("selected agent usage lost its exact mock turn: %+v", usage)
	}
	requireSelectedForkDurablePublicReads(t, rt, runID)
	if !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
		t.Fatal("selected agent reads changed durable state")
	}
}

func requireSelectedForkMixedBusinessMutation(t *testing.T, rt servedControlProofRuntime, runID, eventID, entityID string) {
	t.Helper()
	var event operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
	if event.RunID != runID || len(event.Deliveries) != 2 {
		t.Fatalf("mixed frontier did not retain both recipients: %+v", event)
	}
	kinds := map[string]int{}
	for _, delivery := range event.Deliveries {
		kinds[delivery.SubscriberType]++
		if delivery.Status != "delivered" || delivery.Failure != nil || delivery.RetryCount != 0 {
			t.Fatalf("mixed frontier failed to settle: %+v", delivery)
		}
		if delivery.SubscriberType == "node" && delivery.Target != (operatorread.OperatorDeliveryTarget{Kind: "existing_entity", FlowID: "consumer", FlowInstance: "consumer", EntityID: entityID}) {
			t.Fatalf("mixed frontier borrowed another entity: %+v", delivery)
		}
		var outcomes int
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=$1 AND claim_version=1 AND outcome='delivered'`, delivery.DeliveryID).Scan(&outcomes); err != nil || outcomes != 1 {
			t.Fatalf("mixed frontier missing exact settlement: count=%d err=%v", outcomes, err)
		}
	}
	if kinds["node"] != 1 || kinds["agent"] != 1 {
		t.Fatalf("mixed frontier recipient census: %v", kinds)
	}
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(new_value AS TEXT) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='processed_token' AND writer_type='platform' AND writer_id='workflow_engine' AND handler_step='mutate'`, runID, entityID, eventID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := json.Unmarshal([]byte(raw), &value); err != nil || value != "receiver-proof" {
		t.Fatalf("mixed frontier lost authored mutation: %s %v", raw, err)
	}
}

// Failed deliveries have no successful pipeline handoff stamp. The lifecycle
// owner decides terminality; callers retain their exact outcome/receipt checks.
func waitForkReceiverExecutionCompletion(t *testing.T, rt servedControlProofRuntime, selected *selectedStoreOwner, ctx context.Context, claim runtimedelivery.Claim, terminalFailure bool) {
	t.Helper()
	if !terminalFailure {
		waitForkReceiverSourceCompletion(t, rt, claim.RunID())
		return
	}
	owner := selected.RuntimeDeps().DeliveryStore
	snapshot, err := owner.Snapshot(ctx, claim.DeliveryID())
	if err != nil || snapshot.Status != runtimedelivery.StatusDeadLetter {
		t.Fatalf("failed receiver lacks terminal dead-letter settlement: status=%s err=%v", snapshot.Status, err)
	}
	continuation, err := owner.ObserveDeliveryContinuation(ctx, snapshot.Authority, claim.DeliveryID())
	if err != nil || continuation.Disposition != runtimedelivery.ClaimTerminal {
		t.Fatalf("terminal receiver retained recovery work: %+v err=%v", continuation, err)
	}
}

// This row challenges the real store with the old token while the runtime is
// held. It proves mutation fencing, not private in-memory continuation identity.
// Only canonical parent terminalization is allowed to retire the newer claim.
func requireForkReceiverStaleClaimStoreFence(t *testing.T, rt servedControlProofRuntime, selected *selectedStoreOwner, observation forkReceiverExecutionObservation, old runtimedelivery.Claim) {
	t.Helper()
	deps := selected.RuntimeDeps()
	owner := deps.DeliveryStore
	snapshot, err := owner.Snapshot(observation.ctx, old.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	prepared, found, err := deps.EventBusDurable.PreparedEvents.LoadPreparedPublishEvent(observation.ctx, observation.signal.EventID)
	if err != nil || !found || prepared.Validate() != nil {
		t.Fatalf("load actual committed fork event: found=%t err=%v", found, err)
	}
	// Age the exact obligation/attempt timestamp pairs atomically, as in the
	// lifecycle conformance fixture. ClaimDelivery alone mints the successor.
	tx, err := rt.DB.BeginTx(observation.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	startedAt := time.Now().Add(-2 * time.Hour).UTC()
	expiresAt := time.Now().Add(-time.Hour).UTC()
	result, err := tx.Exec(`UPDATE event_deliveries SET created_at=$1,started_at=$1,updated_at=$2 WHERE delivery_id=$3 AND run_id=$4 AND claim_version=$5 AND status='in_progress'`, startedAt, expiresAt, old.DeliveryID(), old.RunID(), old.Version())
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("age actual obligation: count=%d err=%v", count, err)
	}
	result, err = tx.Exec(`UPDATE event_delivery_attempts SET started_at=$1,lease_expires_at=$2 WHERE delivery_id=$3 AND claim_version=$4 AND claim_token=$5 AND open_marker=TRUE`,
		startedAt, expiresAt, old.DeliveryID(), old.Version(), old.PersistenceToken())
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("expire actual claim: count=%d err=%v", count, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	claimed, err := owner.ClaimDelivery(observation.ctx, snapshot.Authority, prepared.Event.Event(), snapshot.Route)
	if err != nil {
		t.Fatal(err)
	}
	newer, ok := claimed.Acquired()
	if !ok || newer.Claim.Same(old) || newer.Claim.Version() != old.Version()+1 {
		t.Fatal("real store did not acquire a distinct successor claim")
	}
	before, err := owner.Snapshot(observation.ctx, old.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.RenewClaim(observation.ctx, old); !errors.Is(err, runtimedelivery.ErrConflict) {
		t.Fatalf("stale renewal was not fenced: %v", err)
	}
	failure := runtimefailures.FromError(errors.New("stale receiver attempt must not settle successor"), "fork-receiver-test", "settlement")
	if _, err := owner.SettleFailure(observation.ctx, old, runtimedelivery.Settlement{
		Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "stale_receiver_test",
		Failure: &failure.Failure, RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
	}); !errors.Is(err, runtimedelivery.ErrConflict) {
		t.Fatalf("stale failure settlement was not fenced: %v", err)
	}
	after, err := owner.Snapshot(observation.ctx, old.DeliveryID())
	if err != nil || !reflect.DeepEqual(before, after) || after.Status != runtimedelivery.StatusInProgress {
		t.Fatalf("stale mutations damaged the new claim: before=%+v after=%+v err=%v", before, after, err)
	}
	if outcomes, err := owner.Outcomes(observation.ctx, old.DeliveryID()); err != nil || len(outcomes) != 0 {
		t.Fatalf("stale attempt invented terminal evidence: outcomes=%+v err=%v", outcomes, err)
	}
	if renewed, err := owner.RenewClaim(observation.ctx, newer.Claim); err != nil || renewed.ClaimVersion != newer.Claim.Version() {
		t.Fatalf("new claimant could not perform its own mutation: %+v err=%v", renewed, err)
	}
	continuation, err := owner.ObserveDeliveryContinuation(observation.ctx, snapshot.Authority, old.DeliveryID())
	if err != nil || continuation.Disposition != runtimedelivery.ClaimBusy {
		t.Fatalf("new claim lost durable continuation eligibility: %+v err=%v", continuation, err)
	}
	terminalized, err := owner.TerminalizeRun(observation.ctx, old.RunID(), "fork_receiver_test_cleanup")
	if err != nil || len(terminalized) != 1 || terminalized[0].Previous.ClaimVersion != newer.Claim.Version() || terminalized[0].Current.Status != runtimedelivery.StatusDeadLetter {
		t.Fatalf("canonical parent cleanup did not retire exact successor: %+v err=%v", terminalized, err)
	}
}
