package runforkpersistence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type workflowTimerMaterializerSelection struct {
	removed map[string]bool
	fail    string
}

func (workflowTimerMaterializerSelection) SelectInheritedArrivalJoin(source genericschedule.Activation) (genericschedule.ForkJoinDisposition, error) {
	if err := source.ValidateForkJoinRestorationSource(); err != nil {
		return "", err
	}
	return genericschedule.ForkJoinRetained, nil
}

func (workflowTimerMaterializerSelection) RemovedInheritedArrivalRefs() ([]timeridentity.JoinRef, error) {
	return nil, nil
}

func (s workflowTimerMaterializerSelection) SelectInheritedWorkflowTimerRecord(record pipeline.WorkflowTimerActivationPersistenceRecord) (*pipeline.WorkflowTimerActivationPersistenceRecord, error) {
	projected, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(record)
	if err != nil {
		return nil, err
	}
	if s.fail == projected.Ref.DeclarationKey {
		return nil, errors.New("selected declaration admission failed")
	}
	if s.removed[projected.Ref.DeclarationKey] {
		return nil, nil
	}
	selected := projected.Canonical()
	selected.Ref.DeclarationRevision, selected.OwnerAgent, selected.EventType = "selected-revision", "selected-owner", "timer.selected"
	selected.FireAt = selected.CreatedAt.Add(time.Second)
	if selected.Recurring {
		selected.FireAt = projected.FireAt.Add(projected.RecurrenceInterval)
	}
	selected.Payload = []byte(`{"new_arm_only":true}`)
	value := selected.PersistenceRecord()
	return &value, selected.Validate()
}

type workflowTimerMaterializerOwner struct {
	rows    map[string]pipeline.WorkflowTimerActivation
	inserts int
	cancels int
	writes  int
	reads   int
}

func (o *workflowTimerMaterializerOwner) ReadRunForkWorkflowTimerInventoryTx(ctx context.Context, _ *mutationprotocol.Attempt, _ string) ([]pipeline.WorkflowTimerActivation, error) {
	o.reads++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows := make([]pipeline.WorkflowTimerActivation, 0, len(o.rows))
	for _, row := range o.rows {
		rows = append(rows, row.Canonical())
	}
	return rows, nil
}

func (o *workflowTimerMaterializerOwner) MaterializeRunForkWorkflowTimerTx(_ context.Context, _ *mutationprotocol.Attempt, expected pipeline.WorkflowTimerActivation, removed bool) error {
	o.writes++
	actual, present := o.rows[expected.Ref.ActivationID]
	if present {
		if err := actual.ValidateCauseReplay(expected); err != nil {
			return err
		}
	} else {
		actual = expected.Canonical()
		o.inserts++
	}
	if removed {
		if err := actual.ValidateCancellation(pipeline.WorkflowTimerCancelCauseRuleRemoved, expected.CreatedAt); err != nil {
			return err
		}
		if actual.Status != "active" {
			if actual.Status != "cancelled" || actual.CancelCause != pipeline.WorkflowTimerCancelCauseRuleRemoved || !actual.CancelledAt.Equal(expected.CreatedAt) {
				return fmt.Errorf("conflicting cancellation")
			}
		} else {
			actual.Status, actual.CancelCause, actual.CancelledAt = "cancelled", pipeline.WorkflowTimerCancelCauseRuleRemoved, expected.CreatedAt
			o.cancels++
		}
	}
	o.rows[actual.Ref.ActivationID] = actual
	return nil
}

func workflowTimerMaterializerPlan(source pipeline.WorkflowTimerActivation, point runfork.RunForkPoint) runfork.RunForkPlan {
	return runfork.RunForkPlan{SourceRunID: source.RunID, ForkPoint: point,
		WorkflowTimers: []pipeline.WorkflowTimerActivationPersistenceRecord{source.PersistenceRecord()},
		Entities: []runfork.RunForkEntityState{{EntityID: source.EntityID, CurrentState: "waiting",
			MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
				FlowInstance: source.Route.InstancePath, FlowTemplate: source.Route.ScopeKey,
			}}}}
}

func TestRunForkWorkflowTimerMaterializerRetainsFixedCutClockAndSelectedEffects(t *testing.T) {
	for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointRunStart, runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
		for _, root := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/root=%t", kind, root), func(t *testing.T) {
				source := workflowTimerProjectionSource(t, root)
				source.Recurring, source.RecurrenceInterval = true, time.Hour
				source.FireAt, source.FiredAt = source.CreatedAt.Add(3*time.Hour), source.CreatedAt.Add(2*time.Hour)
				point := runfork.RunForkPoint{Kind: kind, Revision: 7}
				if kind == runfork.RunForkPointEvent {
					point.EventID = "55555555-5555-4555-8555-555555555555"
				}
				plan := workflowTimerMaterializerPlan(source, point)
				fixed := plan.WorkflowTimers[0]
				// A source-later cancellation cannot change the already captured cut.
				source.Status, source.CancelledAt = "cancelled", source.CreatedAt.Add(4*time.Hour)
				bornAt := source.CreatedAt.Add(5 * time.Hour)
				owner := &workflowTimerMaterializerOwner{rows: make(map[string]pipeline.WorkflowTimerActivation)}
				removed, err := materializeRunForkWorkflowTimers(context.Background(), new(mutationprotocol.Attempt), plan,
					workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, owner, bornAt)
				if err != nil || len(removed) != 0 || owner.inserts != 1 || owner.cancels != 0 {
					t.Fatalf("materialization: removed=%+v inserts=%d cancels=%d err=%v", removed, owner.inserts, owner.cancels, err)
				}
				for _, child := range owner.rows {
					if child.RunID != workflowTimerProjectionChildRun || child.Ref.DeclarationRevision != "selected-revision" ||
						child.OwnerAgent != "selected-owner" || child.EventType != "timer.selected" ||
						!child.FireAt.Equal(fixed.FireAt) || !child.SourceArmedAt.Equal(fixed.CreatedAt) ||
						!child.CreatedAt.Equal(bornAt) || child.RecurrenceInterval != time.Hour || !child.FiredAt.IsZero() ||
						child.ForkedFromPointKind != kind || child.ForkedFromPointRevision != point.Revision || child.ForkedFromEventID != point.EventID ||
						string(child.Payload) != string(fixed.Payload) {
						t.Fatalf("retained fixed-cut coordinate or selected effect lost: %+v", child)
					}
				}
				if !reflect.DeepEqual(fixed, plan.WorkflowTimers[0]) {
					t.Fatal("materialization mutated its fixed-cut carrier")
				}
			})
		}
	}
}

func TestRunForkWorkflowTimerMaterializerRemovalIsExactOnce(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	plan := workflowTimerMaterializerPlan(source, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 3})
	bornAt := source.CreatedAt.Add(2 * time.Hour)
	owner := &workflowTimerMaterializerOwner{rows: make(map[string]pipeline.WorkflowTimerActivation)}
	selection := workflowTimerMaterializerSelection{removed: map[string]bool{source.Ref.DeclarationKey: true}}
	for i := 0; i < 2; i++ {
		removed, err := materializeRunForkWorkflowTimers(context.Background(), new(mutationprotocol.Attempt), plan,
			workflowTimerProjectionChildRun, selection, owner, bornAt)
		if err != nil || len(removed) != 1 || removed[0].Status != "cancelled" ||
			removed[0].CancelCause != pipeline.WorkflowTimerCancelCauseRuleRemoved || !removed[0].CancelledAt.Equal(bornAt) ||
			!removed[0].FireAt.Equal(source.FireAt) || !removed[0].SourceArmedAt.Equal(source.CreatedAt) {
			t.Fatalf("exact removal decision: removed=%+v err=%v", removed, err)
		}
	}
	if owner.inserts != 1 || owner.cancels != 1 || owner.writes != 2 || owner.reads != 0 {
		t.Fatalf("replay created another physical timer/cancellation: %+v", owner)
	}
}

func TestRunForkWorkflowTimerMaterializerValidatesBeforeAnyWrite(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 3}
	for _, tc := range []struct {
		name string
		plan runfork.RunForkPlan
	}{
		{"foreign_run", func() runfork.RunForkPlan {
			p := workflowTimerMaterializerPlan(source, point)
			p.SourceRunID = workflowTimerProjectionChildRun
			return p
		}()},
		{"missing_owner", func() runfork.RunForkPlan {
			p := workflowTimerMaterializerPlan(source, point)
			p.Entities = nil
			return p
		}()},
		{"wrong_owner_scope", func() runfork.RunForkPlan {
			p := workflowTimerMaterializerPlan(source, point)
			p.Entities[0].MaterializationMetadata.FlowTemplate = "foreign"
			return p
		}()},
		{"duplicate_timer", func() runfork.RunForkPlan {
			p := workflowTimerMaterializerPlan(source, point)
			p.WorkflowTimers = append(p.WorkflowTimers, p.WorkflowTimers[0])
			return p
		}()},
		{"invalid_record", func() runfork.RunForkPlan {
			p := workflowTimerMaterializerPlan(source, point)
			p.WorkflowTimers[0].TaskID = "not-a-timer"
			return p
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &workflowTimerMaterializerOwner{rows: make(map[string]pipeline.WorkflowTimerActivation)}
			if _, err := materializeRunForkWorkflowTimers(context.Background(), new(mutationprotocol.Attempt), tc.plan,
				workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, owner, source.CreatedAt.Add(2*time.Hour)); err == nil || owner.writes != 0 {
				t.Fatalf("invalid fixed-cut evidence reached writer: writes=%d err=%v", owner.writes, err)
			}
		})
	}
	plan := workflowTimerMaterializerPlan(source, point)
	second := source.Canonical()
	second.Ref.ActivationID, second.Ref.DeclarationKey = "66666666-6666-4666-8666-666666666666", "stage:.:second"
	plan.WorkflowTimers = append(plan.WorkflowTimers, second.PersistenceRecord())
	owner := &workflowTimerMaterializerOwner{rows: make(map[string]pipeline.WorkflowTimerActivation)}
	if _, err := materializeRunForkWorkflowTimers(context.Background(), new(mutationprotocol.Attempt), plan,
		workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{fail: second.Ref.DeclarationKey}, owner, source.CreatedAt.Add(2*time.Hour)); err == nil || owner.writes != 0 {
		t.Fatalf("selection failure became removal or partial writes: writes=%d err=%v", owner.writes, err)
	}
}

func TestRunForkWorkflowTimerMaterializerPreservesExactLoopCorrespondence(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	activation, err := loopruntime.New(source.RunID, source.EntityID, "review", "review", "revision", "source-input", "waiting", 3, source.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	source.Ref.Generation = activation.Generation()
	source.Ref.Cause = timeridentity.WorkflowTimerActivationCauseEvent
	plan := workflowTimerMaterializerPlan(source, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 3})
	plan.Entities[0].Accumulator = engine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets()
	owner := &workflowTimerMaterializerOwner{rows: make(map[string]pipeline.WorkflowTimerActivation)}
	if _, err := materializeRunForkWorkflowTimers(context.Background(), new(mutationprotocol.Attempt), plan,
		workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, owner, source.CreatedAt.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	want, err := loopruntime.ForkGeneration(source.Ref.Generation, workflowTimerProjectionChildRun, workflowTimerProjectionChildRun)
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range owner.rows {
		if !child.Ref.Generation.Equal(want) {
			t.Fatalf("timer did not use entity's exact generation correspondence: got=%+v want=%+v", child.Ref.Generation, want)
		}
	}
	plan.Entities[0].Accumulator = nil
	owner.writes = 0
	if _, err := materializeRunForkWorkflowTimers(context.Background(), new(mutationprotocol.Attempt), plan,
		workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, owner, source.CreatedAt.Add(2*time.Hour)); err == nil || owner.writes != 0 {
		t.Fatalf("missing source loop evidence reached writer: writes=%d err=%v", owner.writes, err)
	}
}

func TestRunForkWorkflowTimerMaterializerDoesNotRearmSettledCutRows(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	source.Status, source.FiredAt = "fired", source.FireAt
	plan := workflowTimerMaterializerPlan(source, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 3})
	if removed, err := materializeRunForkWorkflowTimers(context.Background(), nil, plan, workflowTimerProjectionChildRun,
		nil, nil, source.CreatedAt.Add(2*time.Hour)); err != nil || len(removed) != 0 {
		t.Fatalf("settled fixed-cut timer was made executable: removed=%+v err=%v", removed, err)
	}
}
