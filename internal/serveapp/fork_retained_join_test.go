package serveapp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Ordinary materialization retains its exact blockers. Selected preparation
// supplies the admitted historical evidence for the original success oracles.
func TestServedJoinWriterCompletedHistoryRefusalBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, false, false)
}

func TestServedJoinWriterCompletedHistoryRefusalSeparateCheckpointBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, true, false)
}

func TestServedJoinWriterForkRetainedGenerationsBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, false, true)
}

func TestServedJoinWriterForkRetainedGenerationsSeparateCheckpointBothStores(t *testing.T) {
	testServedJoinWriterCompletedHistoryRefusal(t, true, true)
}

func testServedJoinWriterCompletedHistoryRefusal(t *testing.T, separateCheckpoint, success bool) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			root := canonicalrouting.CopyForkLoopRetainedJoin(t)
			if separateCheckpoint {
				root = canonicalrouting.CopyForkLoopRetainedJoinSeparateCheckpoint(t)
			}
			bundle := loadWorkflowValidationBundleAt(t, root)
			carriage, err := semanticview.CompileOriginalLoopCarriage(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatal(err)
			}
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "work.requested", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "member-one"}, "idempotency_key": "retained-join-start",
			})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			rows, err := rt.DB.Query(`SELECT CAST(d.failure AS TEXT) FROM dead_letters d JOIN events e ON e.event_id=d.original_event_id WHERE e.run_id=$1`, started.RunID)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var failure string
				if err := rows.Scan(&failure); err != nil {
					t.Fatal(err)
				}
				t.Errorf("ordinary join writer dead-lettered before fork: %s", failure)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if t.Failed() {
				t.FailNow()
			}
			waitRetainedJoinPipelineHandoff(t, rt.ReceiverStateReader, started.RunID)
			first, joins := readRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			if len(joins) != 1 || first.Attempt != 1 {
				t.Fatalf("first ordinary join: loop=%+v joins=%+v", first, joins)
			}
			firstRef := joins[0].JoinRef()
			nativeEvents, hasNativeEvents := rt.ReceiverStateReader.(runtimebus.EventStore)
			if !hasNativeEvents || nativeEvents == nil {
				t.Fatal("retained join outcome requires its original selected event reader")
			}
			nativeHandoff := servedWorkspaceProofRuntime{Events: nativeEvents}
			firstCtx, cancelFirst := context.WithTimeout(t.Context(), servedProofPollDeadline)
			firstWaitErr := waitServedRetainedJoinOutcome(firstCtx, started.RunID, firstRef, func() (loopruntime.Activation, []joinruntime.Activation) {
				return readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			})
			cancelFirst()
			if firstWaitErr != nil {
				t.Fatalf("first exact retained join outcome: %v", firstWaitErr)
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)
			first, joins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "review.retry", "run_id": started.RunID, "source_event_id": started.EventID,
				"payload": map[string]any{"revision_id": first.RevisionID, "token": "member-one"}, "idempotency_key": "retained-join-repeat",
			})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitRetainedJoinPipelineHandoff(t, rt.ReceiverStateReader, started.RunID)
			current, sourceJoins := readRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			if current.Attempt != 2 || current.ActivationID != first.ActivationID || len(sourceJoins) != 2 {
				t.Fatalf("ordinary repeat lost retained joins: loop=%+v joins=%+v", current, sourceJoins)
			}
			var currentRef timeridentity.JoinRef
			currentMatches := 0
			for _, join := range sourceJoins {
				if join.Generation() == current.Generation() {
					currentRef = join.JoinRef()
					currentMatches++
				}
			}
			if currentMatches != 1 {
				t.Fatalf("current retained join reference is not unique: %d", currentMatches)
			}
			currentCtx, cancelCurrent := context.WithTimeout(t.Context(), servedProofPollDeadline)
			currentWaitErr := waitServedRetainedJoinOutcome(currentCtx, started.RunID, currentRef, func() (loopruntime.Activation, []joinruntime.Activation) {
				return readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			})
			cancelCurrent()
			if currentWaitErr != nil {
				t.Fatalf("current exact retained join outcome: %v", currentWaitErr)
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitWorkspaceProofPipelineHandoff(t, nativeHandoff, started.RunID)
			current, sourceJoins = readNativeRetainedRootJoins(t, rt.ReceiverStateReader, started.RunID)
			for _, join := range sourceJoins {
				if join.Status != joinruntime.StatusClosed || join.CloseReason != joinruntime.CloseReasonComplete || len(join.Outputs) != 1 || !join.TimerCancelled || join.OutcomePending || !join.OutcomeFired {
					t.Fatalf("ordinary join did not complete truthfully: %+v", join)
				}
			}
			if !separateCheckpoint {
				rows, err := rt.DB.Query(`SELECT payload FROM events WHERE run_id=$1 AND event_name='join.observed'`, started.RunID)
				if err != nil {
					t.Fatal(err)
				}
				observed := map[string]int{}
				for rows.Next() {
					var raw []byte
					if err := rows.Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var payload struct {
						RevisionID string `json:"revision_id"`
						Completed  int    `json:"completed"`
					}
					if err := json.Unmarshal(raw, &payload); err != nil || payload.Completed != 1 {
						t.Fatalf("join outcome lost typed completion: %s %v", raw, err)
					}
					observed[payload.RevisionID]++
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if err := rows.Close(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(observed, map[string]int{first.RevisionID: 1, current.RevisionID: 1}) {
					t.Fatalf("join outcomes substituted captured generation: %#v", observed)
				}
			}
			if separateCheckpoint {
				requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "checkpoint.requested", "run_id": started.RunID, "source_event_id": started.EventID,
					"payload": map[string]any{"revision_id": current.RevisionID}, "idempotency_key": "retained-join-checkpoint",
				})
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
				waitRetainedJoinPipelineHandoff(t, rt.ReceiverStateReader, started.RunID)
			}
			var frontier string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='join.observed' AND CAST(payload AS TEXT) LIKE $2`, started.RunID, "%"+current.RevisionID+"%").Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			var frontierRevision int64
			if err := rt.DB.QueryRow(`SELECT MIN(revision) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2 AND present`, started.RunID, frontier).Scan(&frontierRevision); err != nil || frontierRevision <= 0 {
				t.Fatalf("missing fixed-revision frontier: revision=%d err=%v", frontierRevision, err)
			}
			request := runfork.RunForkMaterializeRequest{SourceRunID: started.RunID, At: frontier, OriginalLoopCarriage: carriage}
			if success {
				requireServedRetainedJoinForkSuccess(t, rt, selected, request, current, sourceJoins)
				return
			}
			// Delivery settlement does not join the lifecycle candidate executor. Stop
			// the real source writers before measuring the store-only refusal.
			if err := rt.Runtime.Shutdown(); err != nil {
				t.Fatalf("join source runtime before refusal snapshot: %v", err)
			}
			before := snapshotForkReceiverApplication(t, rt)
			owner, ok := selected.RunFork()
			if !ok {
				t.Fatal("missing fork owner")
			}
			const refusal = "fork materialization requires execution-ready plan; blockers: timer_history_unproven"
			for attempt := 0; attempt < 2; attempt++ {
				child, err := owner.Materialize(servedControlProofAuthorActivityContext(t, rt), request)
				if err == nil || err.Error() != refusal {
					t.Fatalf("completed ordinary join history must retain its exact policy blocker: result=%+v err=%v", child, err)
				}
				if child.ForkRunID != "" || child.MaterializedEntityCount != 0 {
					t.Fatalf("refused materialization returned child work: %+v", child)
				}
				if child.SourceRunID != started.RunID || child.ForkPoint.EventID != frontier || child.ForkPoint.Revision != frontierRevision || child.ExecutionReady || !child.DeliveryResumeBlocked {
					t.Fatalf("refusal lost exact selected-revision admission: %+v", child)
				}
				var codes []string
				for _, blocker := range child.UnsupportedBlockers {
					codes = append(codes, blocker.Code)
				}
				if !reflect.DeepEqual(codes, []string{runfork.RunForkBlockerTimerHistoryUnproven}) {
					t.Fatalf("refusal lost canonical blocker evidence: %+v", child.UnsupportedBlockers)
				}
				if after := snapshotForkReceiverApplication(t, rt); !reflect.DeepEqual(before, after) {
					for table, rows := range after {
						if !reflect.DeepEqual(before[table], rows) {
							t.Errorf("refused join-history materialization changed table %s", table)
						}
					}
					t.Fatal("refused join-history materialization changed application state")
				}
			}
		})
	}
}

func requireServedRetainedJoinForkSuccess(t *testing.T, rt servedControlProofRuntime, selected *selectedStoreOwner, request runfork.RunForkMaterializeRequest,
	current loopruntime.Activation, sourceJoins []joinruntime.Activation) {
	t.Helper()
	owner, ok := selected.RunFork()
	if !ok {
		t.Fatal("retained join success requires the original selected store owner")
	}
	ctx := servedControlProofAuthorActivityContext(t, rt)
	prepared, err := owner.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: request.SourceRunID, At: request.At, ExpectedBundleHash: rt.BundleHash,
		ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
		SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{
			RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore(),
		},
		AgentRuntime: rt.ForkRuntime,
	})
	if err != nil {
		t.Fatalf("prepare the original completed join success: %v", err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Errorf("join retained join preparation: %v", err)
		}
	})
	admitted, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	materializer, ok := owner.Availability().(issue642TimerStagedMaterializer)
	if !ok {
		t.Fatal("retained join success requires the canonical selected materialization owner")
	}
	sourceBefore := readServedForkRecipientSourceDomain(t, rt, request.SourceRunID)
	before := snapshotForkReceiverApplication(t, rt)
	child, err := materializer.MaterializeRunForkForSelectedContractExecution(ctx, admitted)
	if err != nil {
		if !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
			t.Fatal("failed join-history materialization changed application state")
		}
		t.Fatalf("materialize the original completed join success: %v", err)
	}
	childLoop, childJoins := readNativeRetainedRootJoins(t, rt.ReceiverStateReader, child.ForkRunID)
	wantLoop, err := loopruntime.Fork(current, child.ForkRunID, child.ForkRunID)
	if err != nil || !reflect.DeepEqual(childLoop, wantLoop) || childLoop.Attempt != 2 || len(childJoins) != len(sourceJoins) {
		t.Fatalf("materialized join/loop ownership: loop=%+v joins=%+v error=%v", childLoop, childJoins, err)
	}
	for _, source := range sourceJoins {
		want, err := loopruntime.ForkGeneration(source.JoinRef().Generation(), child.ForkRunID, child.ForkRunID)
		if err != nil {
			t.Fatal(err)
		}
		matches := 0
		for _, projected := range childJoins {
			if projected.JoinRef().Generation() != want {
				continue
			}
			matches++
			if projected.JoinRef().Node() != source.JoinRef().Node() || projected.Key() == source.Key() ||
				!reflect.DeepEqual(projected.Members, source.Members) || !reflect.DeepEqual(projected.Outputs, source.Outputs) ||
				projected.Status != source.Status || projected.CloseReason != source.CloseReason || projected.TimerCancelled != source.TimerCancelled ||
				projected.OutcomePending != source.OutcomePending || projected.OutcomeFired != source.OutcomeFired ||
				projected.TimerEventType() != source.TimerEventType() || projected.JoinRef().Declaration() != source.JoinRef().Declaration() ||
				!reflect.DeepEqual(projected.MemberCount, source.MemberCount) || !projected.DeadlineAt.Equal(source.DeadlineAt) ||
				!projected.ArmedAt.Equal(source.ArmedAt) || !projected.FireAt.Equal(source.FireAt) {
				t.Fatalf("retained join evidence changed: source=%+v child=%+v", source, projected)
			}
		}
		if matches != 1 {
			t.Fatalf("expected exactly one historical child generation %+v, got%d", want, matches)
		}
	}
	again, err := materializer.MaterializeRunForkForSelectedContractExecution(ctx, admitted)
	_, repeated := readNativeRetainedRootJoins(t, rt.ReceiverStateReader, child.ForkRunID)
	if err != nil || again.ForkRunID != child.ForkRunID || !reflect.DeepEqual(childJoins, repeated) ||
		!reflect.DeepEqual(sourceBefore, readServedForkRecipientSourceDomain(t, rt, request.SourceRunID)) {
		t.Fatalf("repeated materialization changed child/source history: %v", err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatalf("join retained join preparation: %v", err)
	}
}

func waitRetainedJoinPipelineHandoff(t *testing.T, reader receiverProofStateReader, runID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var incomplete int
	for time.Now().Before(deadline) {
		var err error
		incomplete, err = storetest.ReadServedIncompletePipelineHandoffCount(context.Background(), reader, runID)
		if err != nil {
			t.Fatal(err)
		}
		if incomplete == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("source pipeline completion did not finish: run=%s incomplete=%d", runID, incomplete)
}

func readRetainedRootJoins(t *testing.T, reader receiverProofStateReader, runID string) (loopruntime.Activation, []joinruntime.Activation) {
	t.Helper()
	_, root := requireReceiverTargetState(t, reader, runID, ".", runID, runID)
	carrier, err := engine.StateCarrierFromPersisted(root.Fields, root.Bookkeeping, root.Gates, root.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	loop, found, err := loopruntime.Load(carrier.StateBuckets, ".", "revision")
	if err != nil || !found {
		t.Fatalf("root loop missing: found=%v err=%v", found, err)
	}
	joins, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	return loop, joins
}
