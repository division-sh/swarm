package runforkpersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

type arrivalGapProbeOwner struct{ rows []genericschedule.Activation }

func (o *arrivalGapProbeOwner) MaterializeRunForkArrivalJoinScheduleTx(context.Context, *mutationprotocol.Attempt, storegenericschedule.ForkJoinRequest) (genericschedule.Activation, error) {
	return genericschedule.Activation{}, errors.New("require must not write")
}

func (o *arrivalGapProbeOwner) ReadRunForkArrivalJoinScheduleInventoryTx(context.Context, *mutationprotocol.Attempt, string) ([]genericschedule.Activation, error) {
	return o.rows, nil
}

func TestArrivalInventoryGapProbe(t *testing.T) {
	snapshot, entities, _ := arrivalJoinScheduleProjectionFixture(t, true, false)
	schedules, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
	if err != nil {
		t.Fatal(err)
	}
	plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, Entities: entities, JoinSchedules: schedules,
		ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}}
	bornAt := schedules[0].AdmittedAt.Add(2 * time.Hour)
	requests, err := prepareRunForkArrivalJoinRequests(plan, workflowTimerProjectionChildRun, bornAt)
	if err != nil || len(requests) != 1 {
		t.Fatalf("fixture requests=%d err=%v", len(requests), err)
	}
	exact, err := requests[0].Expected(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	// A different owner has a lawful, distinct scoped key. Its row was not
	// requested by this plan; expected-only lookup cannot see it.
	extraSnapshot, extraEntities, _ := arrivalJoinScheduleProjectionFixture(t, false, false)
	extraSchedules, err := loadRunForkArrivalJoinSchedules(extraSnapshot, extraEntities)
	if err != nil {
		t.Fatal(err)
	}
	extraPlan := runfork.RunForkPlan{SourceRunID: extraSnapshot.RunID, Entities: extraEntities, JoinSchedules: extraSchedules, ForkPoint: plan.ForkPoint}
	extraRequests, err := prepareRunForkArrivalJoinRequests(extraPlan, workflowTimerProjectionChildRun, bornAt)
	if err != nil || len(extraRequests) != 1 {
		t.Fatalf("extra fixture: %v", err)
	}
	extra, err := extraRequests[0].Expected(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	exactScope, _ := exact.Command.ScopeKey()
	extraScope, _ := extra.Command.ScopeKey()
	if exactScope == extraScope {
		t.Fatal("extra fixture is not a different lawful owner")
	}
	if err := extra.Validate(); err != nil {
		t.Fatal(err)
	}
	owner := &arrivalGapProbeOwner{rows: []genericschedule.Activation{exact, extra}}
	withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
		if _, err := readRunForkArrivalJoinInventory(ctx, attempt, plan, workflowTimerProjectionChildRun, bornAt, workflowTimerMaterializerSelection{}, owner, runForkArrivalScheduleAtCut); err == nil {
			t.Fatal("expected-only readback accepted an unrequested inherited row")
		}
	})
}
