package joinruntime

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func ruleRemovedJoinFixture(t *testing.T, flowID string, pendingEmpty bool) Activation {
	t.Helper()
	arm := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	entityID := runID
	route := flowidentity.StoredRoute(".", runID, runID)
	node := identitytest.RootNode(t, "join-node")
	event := "item.completed"
	if flowID != "." {
		entityID = uuid.NewString()
		route = flowidentity.StoredRoute(flowID, "order-1", flowID+"/order-1")
		node = identitytest.FlowNode(t, flowID, "join-node")
		event = flowID + "/item.completed"
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, route)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := workflowlifecycle.NewInitialEntry(route, identity.NormalizeEntityID(entityID), "awaiting", executionmode.Mock, arm)
	if err != nil {
		t.Fatal(err)
	}
	entry, enters, err := effect.StageEntry(owner)
	if err != nil || !enters {
		t.Fatalf("canonical removed-rule entry: %+v enters=%t err=%v", entry, enters, err)
	}
	ref, err := timeridentity.NewJoinRef(node, event, "awaiting", "members")
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	members, due := []string{"member-a", "member-b"}, arm.Add(time.Hour)
	if pendingEmpty {
		members, due = []string{}, time.Time{}
	}
	activation, err := NewActivation(ref, members, nil, arm, due)
	if err != nil {
		t.Fatal(err)
	}
	if pendingEmpty {
		if !activation.Close(CloseReasonComplete, true, false) {
			t.Fatal("empty fixture did not close with its pending completion")
		}
		activation, err = activation.WithTimerHandle(activation.TimerHandle(), arm)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		if disposition, err := activation.Add("member-a", map[string]any{"value": "done"}); err != nil || disposition != AddAccepted {
			t.Fatalf("partial original member output: disposition=%s err=%v", disposition, err)
		}
	}
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	return activation
}

func TestCancelForRuleRemovalPreservesOriginalArmAndPartialHistory(t *testing.T) {
	for _, flowID := range []string{".", "orders"} {
		for _, empty := range []bool{false, true} {
			name := flowID + "/partial"
			if empty {
				name = flowID + "/pending_empty"
			}
			t.Run(name, func(t *testing.T) {
				activation := ruleRemovedJoinFixture(t, flowID, empty)
				beforeJSON, err := json.Marshal(activation)
				if err != nil {
					t.Fatal(err)
				}
				var before Activation
				if err := json.Unmarshal(beforeJSON, &before); err != nil {
					t.Fatal(err)
				}
				if empty && (before.Status != StatusClosed || before.CloseReason != CloseReasonComplete || !before.OutcomePending || before.Expected() != 0 || before.Completed() != 0) ||
					!empty && (before.Status != StatusOpen || before.Expected() != 2 || before.Completed() != 1 || !reflect.DeepEqual(before.Missing(), []string{"member-b"})) {
					t.Fatalf("removed-rule fixture lost its exact unsettled phase: %+v", before)
				}
				if err := activation.CancelForRuleRemoval(); err != nil {
					t.Fatal(err)
				}
				if err := activation.Validate(); err != nil {
					t.Fatal(err)
				}
				if activation.Status != StatusClosed || activation.CloseReason != CloseReasonRuleRemoved || !activation.TimerCancelled || activation.OutcomePending || activation.OutcomeFired {
					t.Fatalf("removed rule did not cancel dependent outcome: %+v", activation)
				}
				if !activation.JoinRef().Equal(before.JoinRef()) || activation.Key() != before.Key() || activation.TimerTaskID() != before.TimerTaskID() ||
					activation.TimerHandle().Kind() != before.TimerHandle().Kind() || activation.Generation() != before.Generation() ||
					!activation.ArmedAt.Equal(before.ArmedAt) || !activation.FireAt.Equal(before.FireAt) || !activation.DeadlineAt.Equal(before.DeadlineAt) ||
					!reflect.DeepEqual(activation.Members, before.Members) || !reflect.DeepEqual(activation.MemberCount, before.MemberCount) ||
					!reflect.DeepEqual(activation.Outputs, before.Outputs) || !reflect.DeepEqual(activation.TransferredPublication, before.TransferredPublication) ||
					activation.Expected() != before.Expected() || activation.Completed() != before.Completed() || !reflect.DeepEqual(activation.Missing(), before.Missing()) {
					t.Fatalf("rule removal rewrote original reference/membership/output/due: before=%+v after=%+v", before, activation)
				}
				context, err := activation.Context()
				if err != nil || context["close_reason"] != string(CloseReasonRuleRemoved) || context["timed_out"] != false ||
					context["expected"] != before.Expected() || context["completed"] != before.Completed() || !reflect.DeepEqual(context["missing"], before.Missing()) {
					t.Fatalf("removed-rule consumer context changed history: %+v err=%v", context, err)
				}
				buckets := map[string]map[string]any{}
				if err := Store(buckets, activation); err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(buckets)
				if err != nil {
					t.Fatal(err)
				}
				var raw map[string]any
				if err := json.Unmarshal(encoded, &raw); err != nil {
					t.Fatal(err)
				}
				persisted, err := PersistedBuckets(raw)
				if err != nil {
					t.Fatal(err)
				}
				loaded, found, err := Load(persisted, activation.JoinRef().Node(), activation.Key())
				if err != nil || !found || !reflect.DeepEqual(loaded, activation) || loaded.Status != StatusClosed || loaded.CloseReason != CloseReasonRuleRemoved ||
					!loaded.TimerCancelled || loaded.OutcomePending || loaded.OutcomeFired {
					t.Fatalf("persisted removed-rule history did not roundtrip exactly: %+v found=%t err=%v", loaded, found, err)
				}
				listed, err := List(persisted)
				if err != nil || len(listed) != 1 || !reflect.DeepEqual(listed[0], activation) {
					t.Fatalf("persisted removed-rule census changed original arm: %+v err=%v", listed, err)
				}
			})
		}
	}
}

func TestCancelForRuleRemovalRejectsSettledFiredAndCorruptAtomically(t *testing.T) {
	for _, flowID := range []string{".", "orders"} {
		for _, fault := range []string{"settled", "fired", "stage_exit", "already_removed", "members", "output_hash", "deadline", "open_pending"} {
			t.Run(flowID+"/"+fault, func(t *testing.T) {
				activation := ruleRemovedJoinFixture(t, flowID, false)
				corrupt := false
				switch fault {
				case "settled":
					activation.Close(CloseReasonDeadline, false, false)
				case "fired":
					activation.Close(CloseReasonDeadline, false, true)
				case "stage_exit":
					activation.CloseForStageExit()
				case "already_removed":
					if err := activation.CancelForRuleRemoval(); err != nil {
						t.Fatal(err)
					}
				case "members":
					activation.Members = append(activation.Members, "member-a")
					corrupt = true
				case "output_hash":
					output := activation.Outputs["member-a"]
					output.Hash = "wrong"
					activation.Outputs["member-a"] = output
					corrupt = true
				case "deadline":
					activation.DeadlineAt = activation.ArmedAt
					corrupt = true
				case "open_pending":
					activation.OutcomePending = true
					corrupt = true
				}
				if err := activation.Validate(); (err != nil) != corrupt {
					t.Fatalf("wrong refusal fixture validity: corrupt=%t err=%v", corrupt, err)
				}
				before, err := json.Marshal(activation)
				if err != nil {
					t.Fatal(err)
				}
				if err := activation.CancelForRuleRemoval(); err == nil {
					t.Fatal("rule removal replaced settled/fired/corrupt history")
				}
				after, err := json.Marshal(activation)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("refusal changed original activation: err=%v", err)
				}
			})
		}
	}
	var absent *Activation
	if err := absent.CancelForRuleRemoval(); err == nil {
		t.Fatal("nil join accepted rule removal")
	}
	zero := Activation{}
	if err := zero.CancelForRuleRemoval(); err == nil || !reflect.DeepEqual(zero, Activation{}) {
		t.Fatal("zero join accepted rule removal or changed on refusal")
	}
}

func TestPersistedRuleRemovalRequiresCanceledTimerAndNoOutcome(t *testing.T) {
	for _, fault := range []string{"uncanceled_timer", "pending", "fired", "open"} {
		t.Run(fault, func(t *testing.T) {
			activation := ruleRemovedJoinFixture(t, "orders", false)
			if err := activation.CancelForRuleRemoval(); err != nil {
				t.Fatal(err)
			}
			buckets := map[string]map[string]any{}
			if err := Store(buckets, activation); err != nil {
				t.Fatal(err)
			}
			joins := buckets[joinNodeBucketKey(activation.JoinRef().Node())][bucketKey].(map[string]any)
			raw := joins[activation.Key()].(map[string]any)
			switch fault {
			case "uncanceled_timer":
				raw["timer_cancelled"] = false
			case "pending":
				raw["outcome_pending"] = true
			case "fired":
				raw["outcome_fired"] = true
			case "open":
				raw["status"] = string(StatusOpen)
			}
			if loaded, found, err := Load(buckets, activation.JoinRef().Node(), activation.Key()); err == nil || found || !reflect.DeepEqual(loaded, Activation{}) {
				t.Fatalf("corrupt persisted removal accepted: %+v found=%t err=%v", loaded, found, err)
			}
			if listed, err := List(buckets); err == nil || listed != nil {
				t.Fatalf("corrupt persisted removal reached census: %+v err=%v", listed, err)
			}
		})
	}
}
