package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/uuid"
)

func TestForkRemovedArrivalDependentsPreserveExactCutAndRejectDrift(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, completion := range []bool{false, true} {
			name := map[bool]string{false: "flow", true: "root"}[root] + map[bool]string{false: "/timeout", true: "/completion"}[completion]
			t.Run(name, func(t *testing.T) {
				plan, born, requests, _ := arrivalJoinInventoryFixture(t, root, completion)
				before, err := json.Marshal(plan)
				if err != nil {
					t.Fatal(err)
				}
				for _, request := range requests {
					if request.Source.Status != genericschedule.StatusActive {
						continue
					}
					request.Disposition = genericschedule.ForkJoinRuleRemoved
					_, ref, valid := timeridentity.ParseJoinHandle(request.Child.Payload.Interface().(map[string]any))
					if !valid {
						t.Fatal("fixture lost its admitted child reference")
					}
					expected, _, err := projectedRemovedArrivalArm(plan, workflowTimerProjectionChildRun, ref)
					if err != nil || expected.CloseReason != joinruntime.CloseReasonRuleRemoved || !expected.TimerCancelled ||
						expected.OutcomePending || expected.OutcomeFired {
						t.Fatalf("removed dependent projection = %+v, %v", expected, err)
					}
					row, err := request.Expected(uuid.NewString())
					if err != nil || !row.CancelledAt.Equal(born) || row.CancelCause != "rule_removed" {
						t.Fatalf("schedule and dependent cancellation disagree: %+v err=%v", row, err)
					}
					for _, mutation := range []string{"exact", "missing", "reopened", "cause", "members", "due", "foreign"} {
						t.Run(mutation, func(t *testing.T) {
							actual := expected
							raw := map[string]any{}
							buckets := map[string]map[string]any{}
							switch mutation {
							case "reopened":
								actual.Status, actual.CloseReason, actual.TimerCancelled = joinruntime.StatusOpen, "", false
								if completion {
									actual.Status, actual.CloseReason, actual.OutcomePending = joinruntime.StatusClosed, joinruntime.CloseReasonComplete, true
								}
							case "cause":
								actual.CloseReason = joinruntime.CloseReasonStageExit
							case "members":
								actual.Members = []string{"different"}
							case "due":
								actual.DeadlineAt = actual.DeadlineAt.Add(1)
							case "foreign":
								entry := ref.StageEntry()
								entry.EntityID = uuid.NewString()
								other, err := ref.Declaration().BindStageEntry(entry, ref.Generation())
								if err != nil {
									t.Fatal(err)
								}
								actual, err = actual.WithForkReference(other)
								if err != nil {
									t.Fatal(err)
								}
							}
							if mutation != "missing" {
								if err := joinruntime.Store(buckets, actual); err != nil {
									t.Fatal(err)
								}
							}
							for key, value := range buckets {
								raw[key] = value
							}
							copy, _ := json.Marshal(raw)
							err := requireRunForkRemovedArrivalArmEvidence(raw, ref, expected)
							if (err == nil) != (mutation == "exact") {
								t.Fatalf("%s evidence = %v", mutation, err)
							}
							after, _ := json.Marshal(raw)
							if !reflect.DeepEqual(copy, after) {
								t.Fatal("require-only readback repaired canceled state")
							}
						})
					}
				}
				after, err := json.Marshal(plan)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("removed dependent projection mutated fixed-cut source history")
				}
			})
		}
	}
}
