package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/uuid"
)

func TestCanceledPreparedArrivalRetainsTerminalCutWithoutRearming(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, completion := range []bool{false, true} {
			for _, fault := range []string{"exact", "unknown_history", "wrong_revision", "published", "foreign_source", "cancellation_after_birth"} {
				name := map[bool]string{false: "flow/", true: "root/"}[root] + map[bool]string{false: "timeout/", true: "completion/"}[completion] + fault
				t.Run(name, func(t *testing.T) {
					plan, born, _, _ := arrivalJoinInventoryFixture(t, root, completion)
					var source *genericschedule.Activation
					for index := range plan.JoinSchedules {
						if plan.JoinSchedules[index].Status == genericschedule.StatusActive {
							if source != nil {
								t.Fatal("fixture repeats its unsettled arrival")
							}
							source = &plan.JoinSchedules[index]
						}
					}
					if source == nil {
						t.Fatal("fixture lost its source arm")
					}
					source.CurrentEventID = genericschedule.OccurrenceEventID(source.ID, source.CurrentDueAt)
					source.CurrentEventAdmittedAt = source.CurrentDueAt.Add(time.Microsecond)
					source.Status, source.CancelCause, source.CancelledAt = genericschedule.StatusCancelled, "join_stage_exit", source.CurrentEventAdmittedAt.Add(time.Microsecond)
					_, ref, ok := timeridentity.ParseJoinHandle(source.Command.Payload.Interface().(map[string]any))
					if !ok {
						t.Fatal("source lost its exact prepared reference")
					}
					for index := range plan.Entities {
						entity := &plan.Entities[index]
						if entity.EntityID != source.Command.EntityID {
							continue
						}
						buckets, err := joinruntime.PersistedBuckets(entity.Accumulator)
						if err != nil {
							t.Fatal(err)
						}
						arm, found, err := joinruntime.Load(buckets, ref.Node(), ref.Key())
						if err != nil || !found || !arm.CloseForStageExit() {
							t.Fatalf("canonical stage-exit cancellation: found=%v err=%v", found, err)
						}
						if err := joinruntime.Store(buckets, arm); err != nil {
							t.Fatal(err)
						}
					}
					plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision, nil)
					switch fault {
					case "unknown_history":
						plan = plan.WithHistoricalEvents(0, nil)
					case "wrong_revision":
						plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision+1, nil)
					case "published":
						plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision, []string{source.CurrentEventID})
					case "foreign_source":
						plan.SourceRunID = uuid.NewString()
					case "cancellation_after_birth":
						source.CancelledAt = born.Add(time.Microsecond)
					}
					if err := source.Validate(); err != nil {
						t.Fatalf("source fixture is not lawful terminal evidence: %v", err)
					}
					before, err := json.Marshal(plan)
					if err != nil {
						t.Fatal(err)
					}
					requests, err := prepareRunForkArrivalJoinRequests(plan, workflowTimerProjectionChildRun, born)
					if (err == nil) != (fault == "exact") {
						t.Fatalf("canceled prepared %s cut: %v", fault, err)
					}
					for _, request := range requests {
						child, err := request.Expected(uuid.NewString())
						if err != nil || child.Status != genericschedule.StatusCancelled || child.CancelCause != request.Source.CancelCause ||
							!child.CancelledAt.Equal(born) || child.CurrentEventID != "" || !child.CurrentEventAdmittedAt.IsZero() ||
							!child.CurrentDueAt.Equal(request.Source.CurrentDueAt) || child.ForkJoinOrigin == nil || child.ForkJoinOrigin.SourceActivationID != request.Source.ID {
							t.Fatalf("terminal restoration rearmed or rewrote retained evidence: %+v err=%v", child, err)
						}
					}
					if fault == "exact" && len(requests) != len(plan.JoinSchedules) || fault != "exact" && requests != nil {
						t.Fatal("terminal projection hid work or returned partial refused evidence")
					}
					after, err := json.Marshal(plan)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatal("terminal projection changed original source evidence")
					}
				})
			}
		}
	}
}
