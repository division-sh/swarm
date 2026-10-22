package runforkpersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestForkArrivalScheduleProjectsExactChildEntryWithoutRearming(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, completion := range []bool{false, true} {
			name := map[bool]string{false: "flow", true: "root"}[root] + map[bool]string{false: "/timeout", true: "/completion"}[completion]
			t.Run(name, func(t *testing.T) {
				snapshot, entities, join := arrivalJoinScheduleProjectionFixture(t, root, completion)
				schedules, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
				if err != nil {
					t.Fatal(err)
				}
				// Mode is inherited evidence, not a projection default.
				for i := range schedules {
					schedules[i].Command.ExecutionMode = executionmode.Mock
					schedules[i].ImmutableHash, err = schedules[i].Command.ImmutableHash()
					if err != nil {
						t.Fatal(err)
					}
				}
				plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, Entities: entities, JoinSchedules: schedules}
				childRunID := uuid.NewString()
				before := make([]string, len(schedules))
				for i, row := range schedules {
					before[i], err = row.EvidenceDigest()
					if err != nil {
						t.Fatal(err)
					}
				}
				projected, err := prepareRunForkArrivalJoinSchedules(plan, childRunID)
				if err != nil || len(projected) != len(schedules) {
					t.Fatalf("projection=%v err=%v", projected, err)
				}
				for _, row := range projected {
					if row.command.RunID != childRunID || row.command.ExecutionMode != executionmode.Mock ||
						!row.command.Due.Absolute.Equal(row.source.InitialDueAt) {
						t.Fatal("lost exact child, mode or retained due")
					}
					payload := row.command.Payload.Interface()
					handle, ref, ok := timeridentity.ParseJoinHandle(payload.(map[string]any))
					if !ok || handle.TaskID() != row.command.TaskID || ref.StageEntry().RunID != childRunID ||
						ref.StageEntry().OriginRunID != plan.SourceRunID || ref.StageEntry().EventID != join.JoinRef().StageEntry().EventID {
						t.Fatal("lost exact original and child stage-entry relation")
					}
					if root && row.command.EntityID != childRunID || !root && row.command.EntityID != entities[0].EntityID {
						t.Fatal("paired schedule with the wrong state owner")
					}
					if row.source.Status == "cancelled" && handle.Kind() != timeridentity.TimerHandleJoinTimeout {
						t.Fatal("canceled companion was reinterpreted as completion")
					}
				}
				for i, row := range schedules {
					after, err := row.EvidenceDigest()
					if err != nil || after != before[i] {
						t.Fatal("projection changed source evidence")
					}
				}
			})
		}
	}
}

func TestForkArrivalScheduleProjectionRequiresCompleteSourceRelation(t *testing.T) {
	for _, name := range []string{"missing", "extra", "duplicate", "foreign_source", "same_child", "changed_due"} {
		t.Run(name, func(t *testing.T) {
			snapshot, entities, _ := arrivalJoinScheduleProjectionFixture(t, true, false)
			schedules, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
			if err != nil {
				t.Fatal(err)
			}
			plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, Entities: entities, JoinSchedules: schedules}
			child := uuid.NewString()
			switch name {
			case "missing":
				plan.JoinSchedules = nil
			case "extra":
				plan.Entities = nil
			case "duplicate":
				plan.JoinSchedules = append(plan.JoinSchedules, schedules[0])
			case "foreign_source":
				plan.SourceRunID = uuid.NewString()
			case "same_child":
				child = plan.SourceRunID
			case "changed_due":
				plan.JoinSchedules[0].Command.Due.Absolute = schedules[0].Command.Due.Absolute.Add(time.Second)
			}
			got, err := prepareRunForkArrivalJoinSchedules(plan, child)
			if err == nil {
				t.Fatalf("contradictory %s relation admitted: %v", name, got)
			}
		})
	}
}
