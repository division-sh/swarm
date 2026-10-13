package runforkpersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestHistoricalGenericJoinUsesCanonicalOriginDecoder(t *testing.T) {
	for _, root := range []bool{false, true} {
		t.Run(map[bool]string{false: "flow", true: "root"}[root], func(t *testing.T) {
			snapshot, entities, join := arrivalJoinScheduleProjectionFixture(t, root, false)
			schedules, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
			if err != nil {
				t.Fatal(err)
			}
			plan := runfork.RunForkPlan{
				SourceRunID: snapshot.RunID, Entities: entities, JoinSchedules: schedules,
				ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: join.JoinRef().StageEntry().EventID, Revision: 1},
			}
			requests, err := prepareRunForkArrivalJoinRequests(plan, uuid.NewString(), join.ArmedAt.Add(time.Minute))
			if err != nil || len(requests) != 1 {
				t.Fatalf("exact construction projection: %v %v", requests, err)
			}
			request := requests[0]
			timer := arrivalJoinRevisionTimer(t, request.Child, request.BornAt)
			timer.SourceTimerID, timer.ForkedFromRunID = request.Source.ID, request.Source.Command.RunID
			timer.ForkedFromPointKind, timer.ForkedFromPointRevision = request.PointKind, request.PointRevision
			timer.ForkedFromEventID, timer.ReconstructionOwner = request.PointEventID, genericschedule.ForkJoinReconstructionOwner
			arm := request.Source.AdmittedAt
			timer.SourceArmedAt = &arm
			expected, err := request.Expected(timer.TimerID)
			if err != nil {
				t.Fatal(err)
			}
			want, err := expected.EvidenceDigest()
			if err != nil {
				t.Fatal(err)
			}
			actual, err := projectRunForkGenericActivation(timer)
			if err != nil {
				t.Fatal(err)
			}
			got, err := actual.EvidenceDigest()
			if err != nil || got != want {
				t.Fatal("historical projection lost canonical origin evidence")
			}
			for _, edit := range []struct {
				name  string
				apply func(*runForkRevisionTimer)
			}{
				{"source_timer", func(r *runForkRevisionTimer) { r.SourceTimerID = "" }},
				{"source_run", func(r *runForkRevisionTimer) { r.ForkedFromRunID = "" }},
				{"same_run", func(r *runForkRevisionTimer) { r.ForkedFromRunID = r.RunID }},
				{"point_kind", func(r *runForkRevisionTimer) { r.ForkedFromPointKind = forkpoint.Kind("latest") }},
				{"point_revision", func(r *runForkRevisionTimer) { r.ForkedFromPointRevision = 0 }},
				{"point_event", func(r *runForkRevisionTimer) { r.ForkedFromEventID = "" }},
				{"source_arm", func(r *runForkRevisionTimer) { r.SourceArmedAt = nil }},
				{"owner", func(r *runForkRevisionTimer) { r.ReconstructionOwner = "another-owner" }},
			} {
				t.Run(edit.name, func(t *testing.T) {
					bad := timer
					edit.apply(&bad)
					if _, err := projectRunForkGenericActivation(bad); err == nil {
						t.Fatal("historical decoder bypassed canonical partial-origin refusal")
					}
				})
			}
		})
	}
}
