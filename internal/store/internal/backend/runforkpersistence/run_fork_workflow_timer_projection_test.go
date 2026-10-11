package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

const (
	workflowTimerProjectionSourceRun = "11111111-1111-4111-8111-111111111111"
	workflowTimerProjectionChildRun  = "22222222-2222-4222-8222-222222222222"
)

func workflowTimerProjectionSource(t *testing.T, root bool) pipeline.WorkflowTimerActivation {
	t.Helper()
	entityID := workflowTimerProjectionSourceRun
	route := flowidentity.Route{ScopeKey: ".", InstanceID: entityID, InstancePath: entityID}
	source, err := events.NewRootRoutingSource(entityID)
	if !root {
		entityID = "33333333-3333-4333-8333-333333333333"
		route = flowidentity.Route{ScopeKey: "review", InstanceID: "item-a", InstancePath: "review/item-a"}
		source, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "review", FlowInstance: route.InstancePath, EntityID: entityID})
	}
	if err != nil {
		t.Fatal(err)
	}
	declarationKey := "stage:review:deadline"
	if root {
		declarationKey = "stage:.:deadline"
	}
	at := time.Date(2026, 10, 9, 0, 0, 0, 123456000, time.UTC)
	return pipeline.WorkflowTimerActivation{
		Ref: timeridentity.WorkflowTimerActivationRef{
			ActivationID: "44444444-4444-4444-8444-444444444444", DeclarationKey: declarationKey,
			DeclarationRevision: "source-revision", Cause: timeridentity.WorkflowTimerActivationCauseInitial,
		},
		RunID: workflowTimerProjectionSourceRun, EntityID: entityID, Route: route, RoutingSource: source,
		OwnerAgent: "source-owner", EventType: "timer.elapsed", ExecutionMode: executionmode.Live, Payload: []byte(`{"retained":true}`),
		CreatedAt: at, FireAt: at.Add(time.Hour), Status: "active",
	}
}

func workflowTimerProjectionRaw(t *testing.T, source pipeline.WorkflowTimerActivation) []byte {
	t.Helper()
	routing, err := json.Marshal(source.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	interval := ""
	if source.Recurring {
		interval = source.RecurrenceInterval.String()
	}
	snapshot := runforkrevision.TimerSnapshot{
		TimerID: source.Ref.ActivationID, TimerName: source.Ref.TaskID(), RunID: source.RunID, EntityID: source.EntityID,
		FlowScopeKey: source.Route.ScopeKey, FlowInstanceID: source.Route.InstanceID, FlowInstance: source.Route.InstancePath,
		RoutingSource: routing, OwnerAgent: source.OwnerAgent, OwnerKind: "system", FireEvent: source.EventType,
		ExecutionMode: string(source.ExecutionMode), FirePayload: source.Payload, CreatedAt: source.CreatedAt, FireAt: source.FireAt,
		Recurring: source.Recurring, RecurrenceInterval: interval, TaskType: "workflow_timer", Status: source.Status,
		SourceTimerID: source.SourceTimerID, ForkedFromRunID: source.ForkedFromRunID, ForkedFromEventID: source.ForkedFromEventID,
		ForkedFromPointKind: source.ForkedFromPointKind, ForkedFromPointRevision: source.ForkedFromPointRevision, ReconstructionOwner: source.ReconstructionOwner,
	}
	if !source.SourceArmedAt.IsZero() {
		snapshot.SourceArmedAt = &source.SourceArmedAt
	}
	if !source.FiredAt.IsZero() {
		snapshot.FiredAt = &source.FiredAt
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func workflowTimerProjectionSelection(t *testing.T, source pipeline.WorkflowTimerActivation, generation attemptgeneration.Generation, bornAt time.Time) pipeline.WorkflowTimerActivation {
	t.Helper()
	selected := source.Canonical()
	ownership, err := projectRunForkEntityOwnership(source.RunID, workflowTimerProjectionChildRun, source.EntityID, source.Route.InstancePath)
	if err != nil {
		t.Fatal(err)
	}
	selected.RunID, selected.EntityID = workflowTimerProjectionChildRun, ownership.Fork.EntityID
	if ownership.Fork.FlowInstance != ownership.Source.FlowInstance {
		selected.Route.InstanceID, selected.Route.InstancePath = workflowTimerProjectionChildRun, workflowTimerProjectionChildRun
	}
	selected.RoutingSource, err = runfork.ProjectProducerOwnership(source.RunID, workflowTimerProjectionChildRun, source.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	selected.Ref.ActivationID = timeridentity.WorkflowTimerActivationID("selected-new-arm", workflowTimerProjectionChildRun, source.Ref.DeclarationKey)
	selected.Ref.DeclarationRevision, selected.Ref.Generation = "selected-revision", generation
	selected.OwnerAgent, selected.EventType = "selected-owner", "timer.selected"
	selected.CreatedAt, selected.FireAt, selected.FiredAt = bornAt, bornAt.Add(time.Second), time.Time{}
	selected.SourceTimerID, selected.ForkedFromRunID, selected.ForkedFromEventID, selected.ReconstructionOwner = "", "", "", ""
	selected.ForkedFromPointKind, selected.ForkedFromPointRevision, selected.SourceArmedAt = "", 0, time.Time{}
	selected.Payload = []byte(`{"new_arm_only":true}`)
	if selected.Recurring {
		selected.RecurrenceInterval = time.Second
	}
	if err := selected.Validate(); err != nil {
		t.Fatal(err)
	}
	return selected
}

func TestRunForkWorkflowTimerProjectionTypedCutsRetainArmedClock(t *testing.T) {
	for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointRunStart, runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
		for _, cause := range []timeridentity.WorkflowTimerActivationCause{timeridentity.WorkflowTimerActivationCauseInitial, timeridentity.WorkflowTimerActivationCauseEvent, timeridentity.WorkflowTimerActivationCauseTransition} {
			t.Run(string(kind)+"/"+string(cause), func(t *testing.T) {
				source := workflowTimerProjectionSource(t, true)
				source.Ref.Cause = cause
				source.Recurring, source.RecurrenceInterval = true, time.Hour
				source.FireAt, source.FiredAt = source.CreatedAt.Add(2*time.Hour), source.CreatedAt.Add(time.Hour)
				bornAt := source.CreatedAt.Add(4 * time.Hour)
				selected := workflowTimerProjectionSelection(t, source, attemptgeneration.Generation{}, bornAt)
				before := selected.Canonical()
				point := runfork.RunForkPoint{Kind: kind, Revision: 7}
				if kind == runfork.RunForkPointEvent {
					point.EventID = "55555555-5555-4555-8555-555555555555"
				}
				raw := workflowTimerProjectionRaw(t, source)
				child, removed, err := projectRunForkWorkflowTimer(raw, source.RunID, workflowTimerProjectionChildRun, point, &selected, nil, bornAt)
				if err != nil || removed {
					t.Fatalf("project exact inherited timer: removed=%v: %v", removed, err)
				}
				if !child.FireAt.Equal(source.FireAt) || !child.SourceArmedAt.Equal(source.CreatedAt) || !child.CreatedAt.Equal(bornAt) ||
					!child.FiredAt.IsZero() || child.RecurrenceInterval != source.RecurrenceInterval || !child.Recurring ||
					!child.FireAt.Before(child.CreatedAt) || string(child.Payload) != string(source.Payload) {
					t.Fatalf("inherited clock/payload was rearmed or source acceptance copied: %+v", child)
				}
				if child.Ref.ActivationID == source.Ref.ActivationID || child.Ref.DeclarationRevision != selected.Ref.DeclarationRevision ||
					child.Ref.Cause != source.Ref.Cause || child.OwnerAgent != selected.OwnerAgent || child.EventType != selected.EventType ||
					child.SourceTimerID != source.Ref.ActivationID || child.ForkedFromRunID != source.RunID || child.ForkedFromPointKind != kind ||
					child.ForkedFromPointRevision != point.Revision || child.ForkedFromEventID != point.EventID || child.ReconstructionOwner == "" {
					t.Fatalf("child identity, selected effect, or exact lineage was lost: %+v", child)
				}
				retry, retryRemoved, err := projectRunForkWorkflowTimer(raw, source.RunID, workflowTimerProjectionChildRun, point, &selected, nil, bornAt)
				if err != nil || retryRemoved || !reflect.DeepEqual(retry, child) || !reflect.DeepEqual(selected.Canonical(), before) ||
					string(raw) != string(workflowTimerProjectionRaw(t, source)) {
					t.Fatal("exact retry drifted or projection changed caller-owned inputs")
				}
				point.Revision++
				other, _, err := projectRunForkWorkflowTimer(raw, source.RunID, workflowTimerProjectionChildRun, point, &selected, nil, bornAt)
				if err != nil || other.Ref.ActivationID == child.Ref.ActivationID {
					t.Fatalf("different typed cut aliased the original child activation: %v", err)
				}
			})
		}
	}
}

func TestRunForkWorkflowTimerProjectionOwnershipAndRemoval(t *testing.T) {
	for _, root := range []bool{false, true} {
		t.Run(map[bool]string{false: "template", true: "root"}[root], func(t *testing.T) {
			source := workflowTimerProjectionSource(t, root)
			bornAt := source.CreatedAt.Add(3 * time.Hour)
			point := runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 4}
			child, removed, err := projectRunForkWorkflowTimer(workflowTimerProjectionRaw(t, source), source.RunID, workflowTimerProjectionChildRun, point, nil, nil, bornAt)
			if err != nil || !removed || child.Status != "active" || !child.FiredAt.IsZero() {
				t.Fatalf("rule removal did not return exact active cancellation target: removed=%v child=%+v: %v", removed, child, err)
			}
			wantEntity, wantRoute := source.EntityID, source.Route
			if root {
				wantEntity = workflowTimerProjectionChildRun
				wantRoute.InstanceID, wantRoute.InstancePath = workflowTimerProjectionChildRun, workflowTimerProjectionChildRun
			}
			if child.EntityID != wantEntity || child.Route != wantRoute || child.RunID != workflowTimerProjectionChildRun ||
				child.Ref.DeclarationRevision != source.Ref.DeclarationRevision || child.EventType != source.EventType {
				t.Fatalf("removed rule invented a selected declaration or rehomed an owner: %+v", child)
			}
		})
	}
}

func TestRunForkWorkflowTimerProjectionForkOfForkPreservesOriginalArm(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	armedAt := source.CreatedAt
	source.CreatedAt = source.CreatedAt.Add(2 * time.Hour)
	source.SourceTimerID, source.ForkedFromRunID = "66666666-6666-4666-8666-666666666666", "77777777-7777-4777-8777-777777777777"
	source.ForkedFromPointKind, source.ForkedFromPointRevision, source.SourceArmedAt, source.ReconstructionOwner = runfork.RunForkPointRunStart, 2, armedAt, "source-projection"
	bornAt := source.CreatedAt.Add(time.Hour)
	selected := workflowTimerProjectionSelection(t, source, attemptgeneration.Generation{}, bornAt)
	child, _, err := projectRunForkWorkflowTimer(workflowTimerProjectionRaw(t, source), source.RunID, workflowTimerProjectionChildRun, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 4}, &selected, nil, bornAt)
	if err != nil || !child.SourceArmedAt.Equal(armedAt) || !child.FireAt.Equal(source.FireAt) || child.SourceTimerID != source.Ref.ActivationID || child.ForkedFromRunID != source.RunID {
		t.Fatalf("fork-of-fork reset arm or borrowed grandparent as immediate source: %+v: %v", child, err)
	}
}

func TestRunForkWorkflowTimerProjectionRejectsContradictoryEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*pipeline.WorkflowTimerActivation, *pipeline.WorkflowTimerActivation, *runfork.RunForkPoint, *time.Time)
	}{
		{"settled_source", func(s, _ *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.Status = "cancelled"
		}},
		{"wrong_source_run", func(s, _ *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.RunID = "88888888-8888-4888-8888-888888888888"
		}},
		{"missing_route_id", func(s, _ *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.Route.InstanceID = ""
		}},
		{"missing_route_scope", func(s, _ *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.Route.ScopeKey = ""
		}},
		{"missing_route_path", func(s, _ *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.Route.InstancePath = ""
		}},
		{"missing_point_revision", func(_, _ *pipeline.WorkflowTimerActivation, p *runfork.RunForkPoint, _ *time.Time) { p.Revision = 0 }},
		{"fake_start_event", func(_, _ *pipeline.WorkflowTimerActivation, p *runfork.RunForkPoint, _ *time.Time) {
			p.EventID = "55555555-5555-4555-8555-555555555555"
		}},
		{"missing_event_identity", func(_, _ *pipeline.WorkflowTimerActivation, p *runfork.RunForkPoint, _ *time.Time) {
			p.Kind = runfork.RunForkPointEvent
		}},
		{"birth_before_source", func(s, _ *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, at *time.Time) {
			*at = s.CreatedAt.Add(-time.Microsecond)
		}},
		{"missing_birth", func(_, _ *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, at *time.Time) {
			*at = time.Time{}
		}},
		{"wrong_child_run", func(_, s *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.RunID = workflowTimerProjectionSourceRun
		}},
		{"wrong_child_route", func(_, s *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.Route.InstanceID = "foreign"
		}},
		{"wrong_declaration", func(_, s *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.Ref.DeclarationKey += ".other"
		}},
		{"missing_selected_revision", func(_, s *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.Ref.DeclarationRevision = ""
		}},
		{"wrong_cause", func(_, s *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.Ref.Cause = timeridentity.WorkflowTimerActivationCauseEvent
		}},
		{"wrong_mode", func(_, s *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.ExecutionMode = executionmode.Mock
		}},
		{"wrong_generation", func(_, s *pipeline.WorkflowTimerActivation, _ *runfork.RunForkPoint, _ *time.Time) {
			s.Ref.Generation.LoopID = "foreign"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := workflowTimerProjectionSource(t, true)
			bornAt := source.CreatedAt.Add(3 * time.Hour)
			selected := workflowTimerProjectionSelection(t, source, attemptgeneration.Generation{}, bornAt)
			point := runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 4}
			test.change(&source, &selected, &point, &bornAt)
			if _, _, err := projectRunForkWorkflowTimer(workflowTimerProjectionRaw(t, source), workflowTimerProjectionSourceRun, workflowTimerProjectionChildRun, point, &selected, nil, bornAt); err == nil {
				t.Fatal("foreign or partial evidence accepted as inherited child authority")
			}
		})
	}
}

func TestRunForkWorkflowTimerProjectionRequiresExactGenerationCorrespondence(t *testing.T) {
	source := workflowTimerProjectionSource(t, false)
	loop, err := loopruntime.New(source.RunID, source.EntityID, "review", "review", "revision", "source-ingress", "waiting", 3, source.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	source.Ref.Generation = loop.Generation()
	correspondence, err := loopruntime.NewForkCorrespondence([]loopruntime.Activation{loop}, workflowTimerProjectionChildRun, source.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := correspondence.AdmitSource(source.Ref.Generation)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := correspondence.Bind(admitted)
	if err != nil {
		t.Fatal(err)
	}
	bornAt := source.CreatedAt.Add(3 * time.Hour)
	selected := workflowTimerProjectionSelection(t, source, bound.Generation(), bornAt)
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 7, EventID: "55555555-5555-4555-8555-555555555555"}
	raw := workflowTimerProjectionRaw(t, source)
	child, _, err := projectRunForkWorkflowTimer(raw, source.RunID, workflowTimerProjectionChildRun, point, &selected, correspondence, bornAt)
	if err != nil || child.Ref.Generation != bound.Generation() || child.Ref.Generation == source.Ref.Generation {
		t.Fatalf("exact source generation was not child-bound: %+v: %v", child.Ref.Generation, err)
	}
	if _, _, err := projectRunForkWorkflowTimer(raw, source.RunID, workflowTimerProjectionChildRun, point, &selected, nil, bornAt); err == nil {
		t.Fatal("source generation accepted without an admitted correspondence")
	}
	foreign, err := loopruntime.NewForkCorrespondence([]loopruntime.Activation{loop}, "foreign-child", source.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := projectRunForkWorkflowTimer(raw, source.RunID, workflowTimerProjectionChildRun, point, &selected, foreign, bornAt); err == nil {
		t.Fatal("foreign child correspondence lent destination ownership")
	}
	source.Ref.Generation.RevisionID = "foreign-revision"
	if _, _, err := projectRunForkWorkflowTimer(workflowTimerProjectionRaw(t, source), source.RunID, workflowTimerProjectionChildRun, point, &selected, correspondence, bornAt); err == nil {
		t.Fatal("foreign source generation accepted within a valid child correspondence")
	}
}
