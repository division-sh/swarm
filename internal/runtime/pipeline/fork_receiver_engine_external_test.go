package pipeline_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providertriggers"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type forkEngineObservation struct {
	ctx context.Context
	id  string
}

type forkEngineAcknowledgment struct {
	result runforkexecution.SelectedContractExecutionResult
	err    error
}

type forkEngineProbe struct {
	claimed       chan forkEngineObservation
	settled       chan lifecycleprobe.Signal
	resume        chan struct{}
	resumeSettled chan struct{}
	once          sync.Once
	settledOnce   sync.Once
}

func (p *forkEngineProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.DeliveryStatusChanged || signal.EventType != "producer/work.ready" {
		return
	}
	if runtimedelivery.Status(signal.Status).Terminal() {
		p.settledOnce.Do(func() {
			p.settled <- signal
			select {
			case <-p.resumeSettled:
			case <-ctx.Done():
			}
		})
		return
	}
	if signal.Status != "in_progress" {
		return
	}
	p.once.Do(func() {
		p.claimed <- forkEngineObservation{ctx: ctx, id: signal.EventID}
		select {
		case <-p.resume:
		case <-ctx.Done():
		}
	})
}

// The supported fork delivery path must retain exact receiver ownership and
// selected-store mutation behavior on both backends.
func TestSelectedForkReceiverSupportedDeliveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, missing := range []bool{false, true} {
			presence := "required_present"
			if missing {
				presence = "required_missing"
			}
			t.Run(backend+"/"+presence, func(t *testing.T) {
				root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
				path := filepath.Join(root, "consumer", "nodes.yaml")
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				old := "          - {target_field: processed_token, value: payload.token}\n"
				if strings.Count(string(raw), old) != 1 {
					t.Fatal("canonical receiver fixture mutation changed")
				}
				write := "          - {target_field: marker, value: \"consumer-engine\"}\n"
				if err := os.WriteFile(path, []byte(strings.Replace(string(raw), old, write, 1)), 0o600); err != nil {
					t.Fatal(err)
				}
				rt, selected, db, repo, capability := startForkEngineRuntime(t, backend, root)
				ctx := runtimecorrelation.WithRuntimeInstanceID(context.Background(), rt.Options.RuntimeInstanceID)
				ctx = runtimecorrelation.WithSourceArtifactFact(ctx, rt.Options.SourceArtifactFact)
				ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(rt.Options.RuntimeInstanceID, rt.Options.SourceArtifactFact.BundleHash()))
				ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
				ctx = worklifetime.WithProcess(ctx, rt.Options.ProcessWorkOwner)
				runID := uuid.NewString()
				seed := eventtest.RunCreatingRootIngress(uuid.NewString(), "start.seeded", "fork-engine-proof", "", []byte(`{"token":"engine-proof"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
				if err := rt.Bus.PublishAndWait(ctx, seed); err != nil {
					t.Fatal(err)
				}
				waitForkEngineCompletion(t, db, runID)
				requestEvent := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.requested", "fork-engine-proof", "", []byte(`{"token":"engine-proof"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := rt.Bus.PublishAndWait(ctx, requestEvent); err != nil {
					t.Fatal(err)
				}
				waitForkEngineCompletion(t, db, runID)
				var frontier, sourceConsumer string
				if err := db.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, runID).Scan(&frontier); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance='consumer'`, runID).Scan(&sourceConsumer); err != nil || forkEngineMarker(t, sourceConsumer) != "consumer-engine" {
					t.Fatalf("normal receiver did not perform independently expected write: %s err=%v", sourceConsumer, err)
				}
				bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, filepath.Join(repo, "platform-spec.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				family, ok := selected.RunFork()
				if !ok {
					t.Fatal("missing actual selected fork owner")
				}
				probe := &forkEngineProbe{
					claimed: make(chan forkEngineObservation, 1), settled: make(chan lifecycleprobe.Signal, 1),
					resume: make(chan struct{}), resumeSettled: make(chan struct{}),
				}
				var resumeOnce, resumeSettledOnce sync.Once
				resume := func() { resumeOnce.Do(func() { close(probe.resume) }) }
				resumeSettled := func() { resumeSettledOnce.Do(func() { close(probe.resumeSettled) }) }
				forkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				// PublishAndWait joins the exact source delivery trees, not the live sweeper.
				baselineWork := rt.Options.ProcessWorkOwner.ActiveCount()
				acknowledged := make(chan forkEngineAcknowledgment, 1)
				done := make(chan struct{})
				go func() {
					defer close(done)
					result, err := family.Execute(forkCtx, runforkexecution.SelectedContractExecutionRequest{
						SourceRunID: runID, At: frontier, ExpectedBundleHash: rt.Options.SourceArtifactFact.BundleHash(), AllowSourceFreeze: true,
						SourceLoader:      runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml"), Store: selected.SourceArtifactStore()},
						ContractSelection: runforkadmission.SelectedContractSelection(semanticview.Wrap(bundle)),
						AgentRuntime:      runforkexecution.SelectedContractAgentRuntimeOptions{ProcessCapability: capability, ExecutionPosture: rt.ExecutionPosture, AgentManagerOptions: runtimemanager.AgentManagerOptions{TestLifecycleProbe: probe}},
					})
					acknowledged <- forkEngineAcknowledgment{result: result, err: err}
				}()
				t.Cleanup(func() {
					cancel()
					resume()
					resumeSettled()
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Error("fork activation acknowledgment did not join")
					}
				})
				// Execute acknowledges activation, not the retained engine's outcome.
				var result runforkexecution.SelectedContractExecutionResult
				select {
				case ack := <-acknowledged:
					if ack.err != nil {
						t.Fatalf("real fork did not acknowledge activation: %v", ack.err)
					}
					result = ack.result
				case <-forkCtx.Done():
					t.Fatal(forkCtx.Err())
				}
				proof := result.ForkLocalRuntimeContainer
				if result.Owner != runfork.RunForkSelectedContractExecutionOwner || !result.Activation.Activated ||
					result.Activation.SourceRunID != runID || result.Materialization.ForkRunID == "" ||
					result.Materialization.ForkRunID == runID || result.Activation.ForkRunID != result.Materialization.ForkRunID ||
					proof == nil || proof.ForkRunID != result.Activation.ForkRunID || proof.SourceRunID != runID ||
					proof.Owner != runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner || proof.RuntimeExecutionID == "" {
					t.Fatalf("activation acknowledgment lacks exact fresh-child execution evidence: %+v", result)
				}
				var observed forkEngineObservation
				select {
				case observed = <-probe.claimed:
				case <-forkCtx.Done():
					t.Fatalf("acknowledged fork did not reach engine claim: %v", forkCtx.Err())
				}
				claim, ok := runtimedelivery.ClaimFromContext(observed.ctx)
				admission, admitted := managedexecution.FromContext(observed.ctx)
				if !ok || !admitted || !admission.AuthorizesSelected(proof.RuntimeExecutionID, proof.ForkRunID, proof.RuntimeGeneration) ||
					admission.RunID != claim.RunID() || claim.RunID() == runID || claim.RunID() != result.Activation.ForkRunID || observed.id == frontier {
					t.Fatal("engine lacks actual fresh-child fork authority")
				}
				deps := selected.RuntimeDeps()
				snapshot, err := deps.DeliveryStore.Snapshot(observed.ctx, claim.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				if snapshot.EventID != observed.id || snapshot.RunID != claim.RunID() || snapshot.Status != runtimedelivery.StatusInProgress || snapshot.ClaimVersion != claim.Version() {
					t.Fatalf("engine claim does not match the exact selected delivery: %+v", snapshot)
				}
				target := snapshot.Route.Target.Route()
				proofRequest := forkEngineExecutionProofRequest(t, result, frontier, observed.id, claim)
				claimedProof := readForkEngineExecutionProof(t, forkCtx, selected, proofRequest)
				assertForkEngineCommittedEvidence(t, claimedProof, result, frontier)
				committedRows := forkEngineConstructedRowCounts(t, claimedProof)
				var before string
				var beforeRevision int64
				if err := db.QueryRow(`SELECT CAST(fields AS TEXT),revision FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance='consumer'`, claim.RunID(), target.EntityID).Scan(&before, &beforeRevision); err != nil || forkEngineMarker(t, before) != "consumer-owned" {
					t.Fatalf("fixed-revision receiver was not reconstructed independently of current source: %s err=%v", before, err)
				}
				if missing {
					result, err := db.Exec(`DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance='consumer'`, claim.RunID(), target.EntityID)
					if err != nil {
						t.Fatal(err)
					}
					if count, err := result.RowsAffected(); err != nil || count != 1 {
						t.Fatalf("remove exact required child: %d %v", count, err)
					}
				}
				resume()
				want := runtimedelivery.StatusDelivered
				if missing {
					want = runtimedelivery.StatusDeadLetter
				}
				select {
				case signal := <-probe.settled:
					if signal.EventID != observed.id || signal.SubscriberType != snapshot.Route.Recipient.Code() ||
						signal.SubscriberID != claim.SubscriberID() || signal.Status != string(want) {
						t.Fatalf("settlement changed the claimed recipient or outcome: %+v", signal)
					}
				case <-forkCtx.Done():
					t.Fatal(forkCtx.Err())
				}
				current, err := deps.DeliveryStore.Snapshot(ctx, claim.DeliveryID())
				if err != nil || current.Status != want || current.EventID != observed.id || !current.MatchesSettlementClaim(claim) {
					t.Fatalf("bridge exact settlement status=%s reason=%s failure=%+v claim=%d want=%s err=%v diagnostics=%v", current.Status, current.ReasonCode, current.Failure, current.ClaimVersion, want, err, forkEngineHandlerErrors(t, db, claim.RunID()))
				}
				settledProof := readForkEngineExecutionProof(t, forkCtx, selected, proofRequest)
				assertForkEngineCommittedEvidence(t, settledProof, result, frontier)
				if missing {
					if current.Failure == nil || runtimefailures.ValidateEnvelope(*current.Failure) != nil ||
						current.Failure.Class != runtimefailures.ClassTargetUnreachable || current.Failure.Detail.Code != "workflow_target_unconstructed" ||
						current.Failure.Component != "workflow-runtime" || current.Failure.Operation != "execute_handler" || current.ReasonCode != "handler_terminal_failure" {
						t.Fatalf("required-state refusal lacks the actual workflow handler failure owner: %+v", current.Failure)
					}
					if settledProof.DeadLetterCount != 1 {
						t.Fatalf("required-state refusal lost exact dead-letter settlement: count=%d", settledProof.DeadLetterCount)
					}
				} else if current.Failure != nil {
					t.Fatalf("successful receiving engine retained failure: %+v", current.Failure)
				}
				settlement := current
				assertForkEngineRows(t, db, runID, claim.RunID(), target.EntityID, sourceConsumer, beforeRevision+1, missing)
				resumeSettled()
				if missing {
					committedRows["entity_state"]--
				}
				pipelineSettlement := waitForkEnginePipelineSettlement(t, forkCtx, selected, proofRequest, settlement)
				cancel()
				if active := rt.Options.ProcessWorkOwner.ActiveCount(); active <= baselineWork {
					t.Fatalf("settlement/request completion lost retained selected resource ownership: work=%d baseline=%d", active, baselineWork)
				}
				// Delivery terminality is not run failure. Explicit process retirement
				// joins the retained owner and leaves its predecessor for fenced recovery.
				cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer cleanupCancel()
				retired := make(chan error, 1)
				go func() { retired <- family.RetireSelectedContexts(cleanupCtx) }()
				select {
				case err := <-retired:
					if err != nil {
						t.Fatalf("join actual selected fork cleanup owner: %v", err)
					}
				case <-cleanupCtx.Done():
					t.Fatalf("selected fork resource retirement did not join: %v", cleanupCtx.Err())
				}
				if active := rt.Options.ProcessWorkOwner.ActiveCount(); active != baselineWork {
					t.Fatalf("selected resource retirement retained process work: %d != %d", active, baselineWork)
				}
				if err := family.RetireSelectedContexts(cleanupCtx); err != nil {
					t.Fatalf("empty retired selected ownership did not remain joined: %v", err)
				}
				retiredProof := readForkEngineExecutionProof(t, cleanupCtx, selected, proofRequest)
				assertForkEngineCommittedEvidence(t, retiredProof, result, frontier)
				if retiredProof.ExecutionState != "running" || retiredProof.RunStatus != "running" || len(retiredProof.Failure) != 0 {
					t.Fatalf("process retirement changed the recoverable predecessor into business failure: %+v", retiredProof)
				}
				assertForkEnginePipelineSettlement(t, retiredProof, settlement)
				if retiredProof.DeadLetterCount != settledProof.DeadLetterCount ||
					retiredProof.DeliveryHandoff != pipelineSettlement.DeliveryHandoff ||
					retiredProof.PipelineOutcome != pipelineSettlement.PipelineOutcome || retiredProof.PipelineReason != pipelineSettlement.PipelineReason ||
					!reflect.DeepEqual(retiredProof.PipelineFailure, pipelineSettlement.PipelineFailure) {
					t.Fatalf("process retirement changed pipeline/delivery settlement evidence: before=%+v after=%+v", pipelineSettlement, retiredProof)
				}
				retainedRows := forkEngineConstructedRowCounts(t, retiredProof)
				for table, want := range committedRows {
					if got := retainedRows[table]; got != want {
						t.Fatalf("postactivation retirement changed committed %s rows: %d != %d", table, got, want)
					}
				}
				assertForkEngineRows(t, db, runID, claim.RunID(), target.EntityID, sourceConsumer, beforeRevision+1, missing)
				current, err = deps.DeliveryStore.Snapshot(ctx, claim.DeliveryID())
				if err != nil || current.EventID != observed.id || current.Status != want || !current.MatchesSettlementClaim(claim) ||
					current.ReasonCode != settlement.ReasonCode || !reflect.DeepEqual(current.Failure, settlement.Failure) {
					t.Fatalf("postactivation cleanup lost exact settlement: %+v err=%v", current, err)
				}
			})
		}
	}
}

func forkEngineExecutionProofRequest(t *testing.T, result runforkexecution.SelectedContractExecutionResult, sourceEvent, childEvent string, claim runtimedelivery.Claim) storetest.SelectedForkReceiverExecutionProofRequest {
	t.Helper()
	binding := result.Materialization.SelectedContractBinding
	proof := result.ForkLocalRuntimeContainer
	if binding == nil || proof == nil || result.Activation.SelectedContractBinding == nil ||
		binding.BindingID != result.Activation.SelectedContractBinding.BindingID ||
		binding.ForkRunID != result.Activation.SelectedContractBinding.ForkRunID ||
		binding.SourceRunID != result.Activation.SelectedContractBinding.SourceRunID ||
		binding.ForkPoint.Kind != result.Activation.SelectedContractBinding.ForkPoint.Kind ||
		binding.ForkPoint.Revision != result.Activation.SelectedContractBinding.ForkPoint.Revision ||
		binding.ForkEventID != result.Activation.SelectedContractBinding.ForkEventID ||
		binding.ContractSelection != result.Activation.SelectedContractBinding.ContractSelection {
		t.Fatal("activation lost its exact materialized selected binding")
	}
	return storetest.SelectedForkReceiverExecutionProofRequest{
		SourceRunID: proof.SourceRunID, ChildRunID: proof.ForkRunID, SourceEventID: sourceEvent, ChildEventID: childEvent,
		BindingID: binding.BindingID, ExecutionID: proof.RuntimeExecutionID, Generation: proof.RuntimeGeneration,
		DeliveryID: claim.DeliveryID(), ClaimVersion: uint64(claim.Version()), AuthorityStamp: result.Owner,
	}
}

func readForkEngineExecutionProof(t *testing.T, ctx context.Context, selected *storeselected.Owner, request storetest.SelectedForkReceiverExecutionProofRequest) storetest.SelectedForkReceiverExecutionProof {
	t.Helper()
	if selected == nil || selected.RuntimeDeps().DeliveryStore == nil {
		t.Fatal("receiver execution proof requires its original native delivery store")
	}
	observation, err := storetest.ReadSelectedForkReceiverExecutionProof(ctx, selected.RuntimeDeps().DeliveryStore, request)
	if err != nil {
		t.Fatalf("read exact selected fork receiver execution proof: %v", err)
	}
	return observation
}

func assertForkEngineCommittedEvidence(t *testing.T, observation storetest.SelectedForkReceiverExecutionProof, result runforkexecution.SelectedContractExecutionResult, sourceEvent string) {
	t.Helper()
	if observation.SourceRunID != result.Activation.SourceRunID || observation.SourceEventID != sourceEvent ||
		observation.BindingID != result.Materialization.SelectedContractBinding.BindingID || observation.CutKind != string(result.Activation.ForkPoint.Kind) ||
		observation.CutRevision != result.Activation.ForkPoint.Revision || observation.Mode != result.Materialization.SelectedContractBinding.ContractSelection.Mode {
		t.Fatalf("committed activation/binding lineage changed: %+v", observation)
	}
	if observation.ExecutionCount != 1 {
		t.Fatalf("committed selected execution lost its exact owner: count=%d", observation.ExecutionCount)
	}
	if observation.PublicationCount != 1 {
		t.Fatalf("committed child event/publication lineage changed: count=%d", observation.PublicationCount)
	}
}

func forkEngineConstructedRowCounts(t *testing.T, observation storetest.SelectedForkReceiverExecutionProof) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	for _, table := range []string{"flow_instances", "flow_instance_runtime_readiness", "workflow_instance_initial_materializations", "entity_state"} {
		count, found := observation.ConstructedRows[table]
		if !found {
			t.Fatalf("selected receiver proof omitted constructed %s row count", table)
		}
		counts[table] = count
	}
	return counts
}

func waitForkEnginePipelineSettlement(t *testing.T, ctx context.Context, selected *storeselected.Owner, request storetest.SelectedForkReceiverExecutionProofRequest, settlement runtimedelivery.Snapshot) storetest.SelectedForkReceiverExecutionProof {
	t.Helper()
	var observation storetest.SelectedForkReceiverExecutionProof
	for {
		next, err := storetest.ReadSelectedForkReceiverExecutionProof(ctx, selected.RuntimeDeps().DeliveryStore, request)
		if err != nil {
			t.Fatalf("read child terminal pipeline settlement: handoff=%t outcome=%s reason=%s failure=%s err=%v", observation.DeliveryHandoff, observation.PipelineOutcome, observation.PipelineReason, observation.PipelineFailure, err)
		}
		observation = next
		if observation.PipelineOutcome != "" {
			assertForkEnginePipelineSettlement(t, observation, settlement)
			return observation
		}
		select {
		case <-ctx.Done():
			t.Fatalf("child terminal pipeline receipt did not settle: handoff=%t outcome=%s reason=%s failure=%s err=%v", observation.DeliveryHandoff, observation.PipelineOutcome, observation.PipelineReason, observation.PipelineFailure, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func assertForkEnginePipelineSettlement(t *testing.T, observation storetest.SelectedForkReceiverExecutionProof, settlement runtimedelivery.Snapshot) {
	t.Helper()
	switch settlement.Status {
	case runtimedelivery.StatusDelivered:
		if !observation.DeliveryHandoff || observation.PipelineOutcome != "success" || len(observation.PipelineFailure) != 0 {
			t.Fatalf("successful delivery lost exact successful pipeline receipt/handoff: %+v", observation)
		}
	case runtimedelivery.StatusDeadLetter:
		if observation.DeliveryHandoff || observation.PipelineOutcome != "dead_letter" || observation.PipelineReason != "handler_terminal_failure" {
			t.Fatalf("dead-letter pipeline receipt changed disposition or invented a success handoff: %+v", observation)
		}
		failure, err := runtimefailures.UnmarshalEnvelope(observation.PipelineFailure)
		if err != nil || settlement.Failure == nil || !reflect.DeepEqual(failure, *settlement.Failure) {
			t.Fatalf("dead-letter pipeline receipt changed the exact canonical delivery failure: failure=%s delivery=%+v err=%v", observation.PipelineFailure, settlement.Failure, err)
		}
	default:
		t.Fatalf("receiver proof requires a delivered or dead-letter settlement: %s", settlement.Status)
	}
}

func forkEngineHandlerErrors(t *testing.T, db *sql.DB, runID string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name='platform.runtime_log'`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var failures []string
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var entry struct {
			Details struct {
				Action  string
				Failure *runtimefailures.Envelope
			}
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			t.Fatal(err)
		}
		if entry.Details.Action == "handler_error" {
			encoded, err := json.Marshal(entry.Details.Failure)
			if err != nil {
				t.Fatal(err)
			}
			failures = append(failures, string(encoded))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return failures
}

func forkEngineMarker(t *testing.T, raw string) any {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		t.Fatal(err)
	}
	return fields["marker"]
}

func assertForkEngineRows(t *testing.T, db *sql.DB, sourceRun, childRun, receiver, sourceBefore string, wantRevision int64, missing bool) {
	t.Helper()
	var fields string
	var revision int64
	err := db.QueryRow(`SELECT CAST(fields AS TEXT),revision FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance='consumer'`, childRun, receiver).Scan(&fields, &revision)
	if missing {
		if err != sql.ErrNoRows {
			t.Fatalf("required-state failure recreated the receiver: %s err=%v", fields, err)
		}
	} else if err != nil || forkEngineMarker(t, fields) != "consumer-engine" || revision != wantRevision {
		t.Fatalf("engine did not write receiving entity exactly once: %s revision=%d want=%d err=%v", fields, revision, wantRevision, err)
	}
	if err := db.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance='producer'`, childRun).Scan(&fields); err != nil || forkEngineMarker(t, fields) != "producer-owned" {
		t.Fatalf("engine borrowed producer state: %s err=%v", fields, err)
	}
	if err := db.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance='consumer'`, sourceRun).Scan(&fields); err != nil || fields != sourceBefore {
		t.Fatalf("engine mutated source receiver: %s err=%v", fields, err)
	}
}

func waitForkEngineCompletion(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var pending int
		if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id=$1 AND (d.status IN ('pending','in_progress') OR d.continuation_handoff_at IS NULL OR NOT EXISTS (SELECT 1 FROM event_receipts r WHERE r.event_id=d.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'))`, runID).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("source completion did not settle")
}

func startForkEngineRuntime(t *testing.T, backend, root string) (*swarmruntime.Runtime, *storeselected.Owner, *sql.DB, string, startupownership.ProcessCapability) {
	t.Helper()
	rt, selected, repo, capability, driver, address := startForkEngineRuntimeOwner(t, backend, root, nil)
	db, err := sql.Open(driver, address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return rt, selected, db, repo, capability
}

func startForkEngineRuntimeOwner(t *testing.T, backend, root string, probe lifecycleprobe.Observer) (*swarmruntime.Runtime, *storeselected.Owner, string, startupownership.ProcessCapability, string, string) {
	return startForkEngineRuntimeOwnerAt(t, backend, root, probe, "")
}

func startForkEngineRuntimeOwnerAt(t *testing.T, backend, root string, probe lifecycleprobe.Observer, requestedAddress string) (*swarmruntime.Runtime, *storeselected.Owner, string, startupownership.ProcessCapability, string, string) {
	t.Helper()
	repo, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"SWARM_CONFIG", "SWARM_STORE_BACKEND", "SWARM_SQLITE_PATH", "SWARM_API_TOKEN", "SWARM_API_TOKEN_FILE"} {
		t.Setenv(key, "")
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	dir := t.TempDir()
	sqlitePath := filepath.Join(dir, "runtime.db")
	if backend == "sqlite" && requestedAddress != "" {
		sqlitePath = requestedAddress
	}
	config := map[string]any{
		"runtime": map[string]any{"recovery_on_startup": false},
		"store":   map[string]any{"backend": backend, "sqlite": map[string]any{"path": sqlitePath}},
		"llm":     map[string]any{"backend": "anthropic", "session": map[string]any{"lock_ttl": "10s", "rotate_after_turns": 40, "rotate_on_parse_failures": 3}},
	}
	var dsn string
	driver, address := "sqlite", sqlitePath
	selection := storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: sqlitePath}
	if backend == "postgres" {
		dsn = requestedAddress
		if dsn == "" {
			dsn = testutil.StartPostgresDSN(t)
		}
		driver, address = "postgres", dsn
		connection, err := testpostgres.ParseConnection(dsn)
		if err != nil {
			t.Fatal(err)
		}
		p := connection.Parameters()
		t.Setenv("FORK_ENGINE_DB_PASSWORD", p.Password)
		config["database"] = map[string]any{"host": p.Host, "port": p.Port, "name": p.Database, "user": p.User, "password_env": "FORK_ENGINE_DB_PASSWORD", "sslmode": p.SSLMode}
		selection = storebackend.Selection{Backend: storebackend.BackendPostgres}
	}
	raw, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "swarm.yaml")
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	selected, err := storeselected.OpenRuntime(context.Background(), storeselected.RuntimeRequest{Selection: selection, PostgresDSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = selected.CloseUnactivated() })
	module, bundle, err := cliapp.NewSwarmWorkflowModule(repo, root, filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	report := bootverify.Run(context.Background(), module.SemanticSource(), bootverify.Options{})
	if len(report.Errors()) != 0 || len(report.Warnings()) != 0 {
		t.Fatalf("fork engine fixture boot verification: errors=%+v warnings=%+v", report.Errors(), report.Warnings())
	}
	plans, err := cliapp.StateStoreSchemaPlans(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.Schema().BootstrapSchema(context.Background(), store.SchemaBootstrapRequest{
		PlatformPlans: plans.Platform, StatePlans: plans.State,
		Origin: store.RuntimeStoreOrigin{SwarmVersion: "fork-engine-test", PlatformVersion: bundle.Platform.Platform.Version, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	runtimeID := uuid.NewString()
	ctx := runtimecorrelation.WithRuntimeInstanceID(context.Background(), runtimeID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeID, fact.BundleHash()))
	storetest.RequireBundleDataCatalog(t, ctx, selected.SourceArtifactWriter(), bundle)
	cfgResult, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: configPath})
	if err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	catalog, _, err := providertriggers.NewCatalogSnapshotFromInventory(bundle.PackInventory, bundle.Platform.Platform.Version)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := runtimecredentials.NewFileStore(filepath.Join(dir, "provider-credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	deps := selected.RuntimeDeps()
	deps.Config = cfgResult.Config
	deps.Options = swarmruntime.RuntimeOptions{ExecutionPosture: executionposture.Live, WorkflowModule: module, SourceArtifactFact: fact, RuntimeInstanceID: runtimeID, ProcessWorkOwner: process, ProviderTriggerCatalog: catalog, ProviderCredentials: credentials, TestLifecycleProbe: probe}
	rt, err := swarmruntime.NewRuntime(ctx, deps)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := selected.StartupOwnership().AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "fork-engine-test", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID})
	if err != nil {
		t.Fatal(err)
	}
	family, ok := selected.RunFork()
	if !ok {
		t.Fatal("fork engine proof requires selected fork owner")
	}
	t.Cleanup(func() {
		joinCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := family.RetireSelectedContexts(joinCtx); err != nil {
			t.Error(err)
		}
		if err := rt.Shutdown(); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(joinCtx); err != nil {
			t.Error(err)
		}
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := family.BindSelectedProcess(ctx, process, capability); err != nil {
		t.Fatal(err)
	}
	if _, err := family.RecoverSelectedForkContexts(ctx, runtimeeffects.NewRecoveryRequest(time.Now().UTC(), rt.ExecutionPosture), runforkexecution.SelectedForkRecoveryEnvironment{}); err != nil {
		t.Fatal(err)
	}
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	agents, err := rt.Manager.CompileStaticTopologyDesiredAgents(module.SemanticSource(), coordinate)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, agents)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fact.BundleHash(), RuntimeInstanceID: runtimeID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		t.Fatal(err)
	}
	if err := rt.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return rt, selected, repo, capability, driver, address
}
