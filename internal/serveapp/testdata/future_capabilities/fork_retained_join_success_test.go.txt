package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedJoinWriterForkRetainedGenerationsBothStores(t *testing.T) {
	testServedJoinWriterForkRetainedGenerations(t, false)
}

func TestServedJoinWriterForkRetainedGenerationsSeparateCheckpointBothStores(t *testing.T) {
	testServedJoinWriterForkRetainedGenerations(t, true)
}

func testServedJoinWriterForkRetainedGenerations(t *testing.T, separateCheckpoint bool) {
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
				"event_name": "work.bootstrap", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "member-one"}, "idempotency_key": "retained-join-start",
			})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
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
			waitForkReceiverSourceCompletion(t, rt, started.RunID)
			first, joins := readRetainedRootJoins(t, rt, started.RunID)
			if len(joins) != 1 || first.Attempt != 1 {
				t.Fatalf("first ordinary join: loop=%+v joins=%+v", first, joins)
			}
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "review.retry", "run_id": started.RunID, "source_event_id": started.EventID,
				"payload": map[string]any{"revision_id": first.RevisionID, "token": "member-one"}, "idempotency_key": "retained-join-repeat",
			})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
			waitForkReceiverSourceCompletion(t, rt, started.RunID)
			current, sourceJoins := readRetainedRootJoins(t, rt, started.RunID)
			if current.Attempt != 2 || current.ActivationID != first.ActivationID || len(sourceJoins) != 2 {
				t.Fatalf("ordinary repeat lost retained joins: loop=%+v joins=%+v", current, sourceJoins)
			}
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
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
				waitForkReceiverSourceCompletion(t, rt, started.RunID)
			}
			var frontier string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='join.observed' AND CAST(payload AS TEXT) LIKE $2`, started.RunID, "%"+current.RevisionID+"%").Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			before := readServedForkRecipientSourceDomain(t, rt, started.RunID)
			owner, ok := selected.RunFork()
			if !ok {
				t.Fatal("missing fork owner")
			}
			request := runfork.RunForkMaterializeRequest{SourceRunID: started.RunID, At: frontier, OriginalLoopCarriage: carriage}
			child, err := owner.Materialize(servedControlProofAuthorActivityContext(t, rt), request)
			if err != nil {
				if !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, started.RunID)) {
					t.Fatal("refused join-history materialization changed source state")
				}
				t.Fatalf("materialize real completed join history: %v", err)
			}
			childLoop, childJoins := readRetainedRootJoins(t, rt, child.ForkRunID)
			wantLoop, err := loopruntime.Fork(current, child.ForkRunID, child.ForkRunID)
			if err != nil || !reflect.DeepEqual(childLoop, wantLoop) || childLoop.Attempt != 2 || len(childJoins) != len(sourceJoins) {
				t.Fatalf("materialized join/loop ownership: loop=%+v joins=%+v", childLoop, childJoins)
			}
			for _, source := range sourceJoins {
				want, err := loopruntime.ForkGeneration(source.JoinRef().Generation(), child.ForkRunID, child.ForkRunID)
				if err != nil {
					t.Fatal(err)
				}
				matched := false
				for _, projected := range childJoins {
					if projected.JoinRef().Generation() != want {
						continue
					}
					matched = true
					if projected.JoinRef().Node() != source.JoinRef().Node() || projected.Key() == source.Key() ||
						!reflect.DeepEqual(projected.Members, source.Members) || !reflect.DeepEqual(projected.Outputs, source.Outputs) ||
						projected.Status != source.Status || projected.CloseReason != source.CloseReason || projected.TimerCancelled != source.TimerCancelled ||
						projected.OutcomePending != source.OutcomePending || projected.OutcomeFired != source.OutcomeFired ||
						projected.CompletionEvent != source.CompletionEvent || !projected.ArmedAt.Equal(source.ArmedAt) || !projected.FireAt.Equal(source.FireAt) {
						t.Fatalf("retained join evidence changed: source=%+v child=%+v", source, projected)
					}
				}
				if !matched {
					t.Fatalf("missing exact historical child generation %+v", want)
				}
			}
			again, err := owner.Materialize(servedControlProofAuthorActivityContext(t, rt), request)
			_, repeated := readRetainedRootJoins(t, rt, child.ForkRunID)
			if err != nil || again.ForkRunID != child.ForkRunID || !reflect.DeepEqual(childJoins, repeated) || !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, started.RunID)) {
				t.Fatalf("repeated materialization changed child/source history: %v", err)
			}
		})
	}
}

func readRetainedRootJoins(t *testing.T, rt servedControlProofRuntime, runID string) (loopruntime.Activation, []joinruntime.Activation) {
	t.Helper()
	var raw []byte
	if err := rt.DB.QueryRow(`SELECT accumulator FROM entity_state WHERE run_id=$1 AND entity_id=$1`, runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	carrier, err := engine.StateCarrierFromPersisted(nil, nil, nil, persisted)
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
