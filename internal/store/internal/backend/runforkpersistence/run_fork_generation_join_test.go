package runforkpersistence

import (
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestForkGenerationHistoricalJoinProjection(t *testing.T) {
	now := time.Unix(100, 0)
	for _, hostile := range []string{"retained_history", "missing_loop", "duplicate", "occupied_destination"} {
		t.Run(hostile, func(t *testing.T) {
			source, err := loopruntime.New("source", "entity", "", "review", "revision", "start", "draft", 4, now)
			if err != nil {
				t.Fatal(err)
			}
			old := source.Generation()
			ref, err := timeridentity.NewJoinRefForGeneration(forkGenerationRootNode("collector"), "result", "draft", "join", "window", old)
			if err != nil {
				t.Fatal(err)
			}
			handle, err := timeridentity.JoinTimeoutHandle(ref)
			if err != nil {
				t.Fatal(err)
			}
			join, err := joinruntime.NewActivation(handle, []string{"a", "b"}, now, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := join.Add("a", map[string]any{"value": "frozen"}); err != nil {
				t.Fatal(err)
			}
			join.Status = joinruntime.StatusClosed
			join.CloseReason = joinruntime.CloseReasonTimeout
			join.TimerCancelled = true
			join.OutcomePending = true
			buckets := map[string]map[string]any{}
			if err := joinruntime.Store(buckets, join); err != nil {
				t.Fatal(err)
			}
			if _, err := source.Repeat("draft", "repeat", now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if err := source.Close("done", "close", now.Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}
			if err := loopruntime.Store(buckets, source); err != nil {
				t.Fatal(err)
			}
			want, err := loopruntime.ForkGeneration(old, "child", "entity")
			if err != nil {
				t.Fatal(err)
			}
			if hostile == "missing_loop" {
				delete(buckets, loopruntime.BucketKey)
			}
			if hostile == "duplicate" {
				for _, bucket := range buckets {
					if joins, ok := bucket["handler_joins"].(map[string]any); ok {
						joins["duplicate"] = joins[join.Key()]
					}
				}
			}
			if hostile == "occupied_destination" {
				if err := joinruntime.ReplaceGeneration(buckets, join, want); err != nil {
					t.Fatal(err)
				}
				if err := joinruntime.Store(buckets, join); err != nil {
					t.Fatal(err)
				}
			}
			raw := runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets()
			before := projectionJSON(t, raw)
			child, err := forkAttemptGenerationState(raw, "child", "entity")
			if projectionJSON(t, raw) != before {
				t.Fatal("join projection changed source, including on failure")
			}
			if hostile != "retained_history" {
				if err == nil {
					t.Fatal("accepted hostile join evidence")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, child)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := joinruntime.List(carrier.StateBuckets)
			if err != nil || len(actual) != 1 {
				t.Fatalf("join readback: %v %v", actual, err)
			}
			got := actual[0]
			if got.Generation() != want || got.Key() == join.Key() || got.TimerTaskID() == join.TimerTaskID() {
				t.Fatal("join retained source identity or lost historical attempt")
			}
			if !got.JoinRef().Declaration().Equal(join.JoinRef().Declaration()) || !reflect.DeepEqual(got.Outputs, join.Outputs) ||
				got.Status != join.Status || got.CloseReason != join.CloseReason || got.TimerCancelled != join.TimerCancelled ||
				got.OutcomePending != join.OutcomePending || got.OutcomeFired != join.OutcomeFired || got.CompletionEvent != join.CompletionEvent ||
				!got.ArmedAt.Equal(join.ArmedAt) || !got.FireAt.Equal(join.FireAt) || !reflect.DeepEqual(got.Members, join.Members) {
				t.Fatal("join projection changed retained lifecycle evidence")
			}
		})
	}
}

func TestForkAttemptGenerationRemintsJoinHandleIdentity(t *testing.T) {
	now := time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC)
	activation, err := loopruntime.New("source-run", "entity-1", "validation", "revision", "revision_id", "event-1", "drafting", 3, now)
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	joinRef, err := timeridentity.NewJoinRefForGeneration(forkGenerationRootNode("review-node"), "review.result", "review", "review", "", activation.Generation())
	if err != nil {
		t.Fatal(err)
	}
	joinHandle, err := timeridentity.JoinTimeoutHandle(joinRef)
	if err != nil {
		t.Fatal(err)
	}
	join, err := joinruntime.NewActivation(joinHandle, []string{"a"}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := joinruntime.Store(buckets, join); err != nil {
		t.Fatal(err)
	}
	accumulatorRef := timeridentity.NewAccumulatorBucketRefForGeneration(forkGenerationRootNode("review-node"), "review.result", "", activation.Generation())
	nodeBucketKey := forkGenerationRootNode("review-node").Key()
	buckets[nodeBucketKey] = map[string]any{}
	buckets[nodeBucketKey]["handler_accumulators"] = map[string]any{accumulatorRef.Key(): map[string]any{"count": 1}}
	raw := runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets()
	forkedRaw, err := forkAttemptGenerationState(raw, "fork-run", "entity-1")
	if err != nil {
		t.Fatal(err)
	}
	forkedCarrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, forkedRaw)
	if err != nil {
		t.Fatal(err)
	}
	forked, found, err := loopruntime.Load(forkedCarrier.StateBuckets, "validation", "revision")
	if err != nil || !found {
		t.Fatalf("forked activation = found %v err %v", found, err)
	}
	if forked.Attempt != activation.Attempt || forked.MaxAttempts != activation.MaxAttempts || forked.ActivationID == activation.ActivationID || forked.RevisionID == activation.RevisionID {
		t.Fatalf("forked activation = %#v, source %#v", forked, activation)
	}
	forkedJoins, err := joinruntime.List(forkedCarrier.StateBuckets)
	if err != nil || len(forkedJoins) != 1 || !forkedJoins[0].Generation().Equal(forked.Generation()) || forkedJoins[0].Key() == join.Key() {
		t.Fatalf("forked joins = %#v err=%v", forkedJoins, err)
	}
	if !forkedJoins[0].JoinRef().Declaration().Equal(join.JoinRef().Declaration()) ||
		forkedJoins[0].TimerTaskID() == join.TimerTaskID() ||
		forkedJoins[0].TimerHandle().TaskID() != forkedJoins[0].TimerTaskID() {
		t.Fatalf("fork remint left stale declaration/task facts: source=%#v fork=%#v", join.JoinRef(), forkedJoins[0].JoinRef())
	}
	forkedAccumulators, _ := forkedCarrier.StateBuckets[nodeBucketKey]["handler_accumulators"].(map[string]any)
	if len(forkedAccumulators) != 1 {
		t.Fatalf("forked accumulators = %#v", forkedAccumulators)
	}
	for key := range forkedAccumulators {
		if key == accumulatorRef.Key() || !strings.Contains(key, forked.Generation().KeySuffix()) {
			t.Fatalf("forked accumulator key = %q, want fork generation", key)
		}
	}

}

func forkGenerationRootNode(nodeID string) runtimeidentity.ExecutableNode {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", nodeID)
	if err != nil {
		panic(err)
	}
	return node
}
