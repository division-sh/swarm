package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func TestProspectivePublicationStateBindsCompleteMutationAndSource(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	runID := eventtest.UUID("prospective-run")
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute("review", "one", "review/one"))
	if err != nil {
		t.Fatal(err)
	}
	record := WorkflowEngineStateRecord{
		Identity: owner, EntityID: eventtest.UUID("prospective-entity"), WorkflowName: "review", WorkflowVersion: "v1",
		Mode: "template", Status: "active", CurrentState: "review", EntityType: "work",
		Fields: json.RawMessage(`{"case_id":"exact"}`), Bookkeeping: json.RawMessage(`{}`), Gates: json.RawMessage(`{}`),
		Accumulator: json.RawMessage(`{}`), Config: json.RawMessage(`{}`), InitialFields: json.RawMessage(`{}`),
		EnteredStageAt: at, CreatedAt: at, UpdatedAt: at, Transition: WorkflowEngineStateTransitionCreateStateAndCompanion,
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	for _, transition := range []WorkflowEngineStateTransition{WorkflowEngineStateTransitionCreateStateAndCompanion, WorkflowEngineStateTransitionUpdateStateAndCompanion, WorkflowEngineStateTransitionUpdateStateCreateCompanion} {
		t.Run(string(rune('0'+transition)), func(t *testing.T) {
			state := record
			state.Transition = transition
			if transition.UpdatesState() {
				state.ExpectedRevision, state.ExpectedState = 3, "collecting"
			}
			prepared, err := prepareWorkflowPublicationState(state, WorkflowLifecycleMutationPlan{}, "review", fact)
			if err != nil {
				t.Fatal(err)
			}
			if err := prepared.ValidateMutation(state, WorkflowLifecycleMutationPlan{}); err != nil {
				t.Fatal(err)
			}
			candidate := prepared.Candidate()
			if candidate.Materializing || candidate.Route != (events.RouteIdentity{FlowID: "review", FlowInstance: owner.Route.InstancePath, EntityID: state.EntityID}) {
				t.Fatalf("prospective post-commit owner = %#v", candidate)
			}
			for name, change := range map[string]func(*WorkflowEngineStateRecord){
				"run": func(s *WorkflowEngineStateRecord) { s.Identity.RunID = eventtest.UUID("other-run") },
				"instance": func(s *WorkflowEngineStateRecord) {
					s.Identity.Route = flowidentity.StoredRoute("review", "two", "review/two")
				},
				"entity":          func(s *WorkflowEngineStateRecord) { s.EntityID = eventtest.UUID("other-entity") },
				"entity contract": func(s *WorkflowEngineStateRecord) { s.EntityType = "other" },
				"revision":        func(s *WorkflowEngineStateRecord) { s.ExpectedRevision++ },
				"expected stage":  func(s *WorkflowEngineStateRecord) { s.ExpectedState = "other" },
				"stage":           func(s *WorkflowEngineStateRecord) { s.CurrentState = "done" },
				"fields":          func(s *WorkflowEngineStateRecord) { s.Fields = json.RawMessage(`{"case_id":"foreign"}`) },
				"gates":           func(s *WorkflowEngineStateRecord) { s.Gates = json.RawMessage(`{"approved":true}`) },
				"accumulator":     func(s *WorkflowEngineStateRecord) { s.Accumulator = json.RawMessage(`{"items":[1]}`) },
				"lifecycle":       func(s *WorkflowEngineStateRecord) { s.Status = "terminated"; s.TerminatedAt = at },
				"time":            func(s *WorkflowEngineStateRecord) { s.UpdatedAt = at.Add(time.Second) },
				"version":         func(s *WorkflowEngineStateRecord) { s.WorkflowVersion = "v2" },
			} {
				t.Run(name, func(t *testing.T) {
					changed := state
					change(&changed)
					if err := prepared.ValidateMutation(changed, WorkflowLifecycleMutationPlan{}); err == nil {
						t.Fatal("changed mutation consumed prospective authority")
					}
				})
			}
		})
	}
	prepared, err := prepareWorkflowPublicationState(record, WorkflowLifecycleMutationPlan{}, "review", fact)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.ValidateMutation(record, WorkflowLifecycleMutationPlan{RequestCompletionCandidate: true}); err == nil {
		t.Fatal("different lifecycle mutation reused prospective authority")
	}
	t.Run("typed schedule identity", func(t *testing.T) {
		payload, err := canonicaljson.Decode([]byte(`{"nested":{"count":2,"fraction":1.5},"items":[null,true,"x"]}`))
		if err != nil {
			t.Fatal(err)
		}
		routing, err := events.NewRootRoutingSource(record.EntityID)
		if err != nil {
			t.Fatal(err)
		}
		schedule := WorkflowScheduleMutation{Kind: WorkflowScheduleMutationUpsert, Command: genericschedule.AdmissionCommand{
			ScheduleKey: "prospective-schedule", TaskID: "prospective-schedule", RunID: runID, EntityID: record.EntityID,
			OwnerKind: genericschedule.OwnerSystem, OwnerID: "workflow-runtime", EventType: "platform.generic_schedule_proof",
			Payload: payload, RoutingSource: routing, ExecutionMode: executionmode.Live, Due: genericschedule.AbsoluteDue(at.Add(time.Hour)),
		}}
		plan := WorkflowLifecycleMutationPlan{Schedules: []WorkflowScheduleMutation{schedule}}
		bound, err := prepareWorkflowPublicationState(record, plan, "review", fact)
		if err != nil {
			t.Fatal(err)
		}
		if err := bound.ValidateMutation(record, plan); err != nil {
			t.Fatal(err)
		}
		if len(plan.Schedules) != 1 || !plan.Schedules[0].Command.Payload.Equal(payload) {
			t.Fatal("preparation mutated caller schedule")
		}
		for name, change := range map[string]func(*WorkflowScheduleMutation){
			"payload": func(s *WorkflowScheduleMutation) {
				s.Command.Payload, _ = canonicaljson.Decode([]byte(`{"nested":{"count":3,"fraction":1.5},"items":[null,true,"x"]}`))
			},
			"due":   func(s *WorkflowScheduleMutation) { s.Command.Due = genericschedule.AbsoluteDue(at.Add(2 * time.Hour)) },
			"task":  func(s *WorkflowScheduleMutation) { s.Command.TaskID = "other" },
			"owner": func(s *WorkflowScheduleMutation) { s.Command.OwnerID = "other" },
			"mode":  func(s *WorkflowScheduleMutation) { s.Command.ExecutionMode = executionmode.Mock },
			"cancel": func(s *WorkflowScheduleMutation) {
				s.Kind = WorkflowScheduleMutationCancel
				s.CancelCause = "completed"
				s.CancelledAt = at
			},
		} {
			t.Run(name, func(t *testing.T) {
				changed := schedule
				change(&changed)
				if err := changed.Validate(runID); err != nil {
					t.Fatalf("hostile fixture must remain a valid distinct mutation: %v", err)
				}
				if err := bound.ValidateMutation(record, WorkflowLifecycleMutationPlan{Schedules: []WorkflowScheduleMutation{changed}}); err == nil {
					t.Fatal("different schedule reused prospective authority")
				}
			})
		}
		if err := bound.ValidateMutation(record, WorkflowLifecycleMutationPlan{}); err == nil {
			t.Fatal("omitted schedule reused authority")
		}
		plan.Schedules = append(plan.Schedules, schedule)
		if err := bound.ValidateMutation(record, plan); err == nil {
			t.Fatal("additional schedule reused authority")
		}
	})
	descriptor, err := prepared.PinRoutingDescriptor()
	if err != nil || descriptor.ID != "one" || descriptor.AddressFields["entity.case_id"] != "exact" {
		t.Fatalf("prospective selector evidence=%#v err=%v", descriptor, err)
	}
	descriptor.AddressFields["entity.case_id"] = "hostile"
	again, err := prepared.PinRoutingDescriptor()
	if err != nil || again.AddressFields["entity.case_id"] != "exact" {
		t.Fatal("selector projection shared mutable fields")
	}
	for _, target := range []events.RouteIdentity{
		{FlowID: "foreign", FlowInstance: record.Identity.Route.InstancePath, EntityID: record.EntityID},
		{FlowID: "review", FlowInstance: record.Identity.Route.InstancePath, EntityID: eventtest.UUID("foreign-entity")},
	} {
		if err := prepared.ValidateTarget(target); err == nil {
			t.Fatal("contradictory target acquired prospective state")
		}
	}
	if err := prepared.ValidateTarget(events.RouteIdentity{FlowInstance: record.Identity.Route.InstancePath}); err != nil {
		t.Fatalf("instance-only blueprint must reach canonical receiver classification: %v", err)
	}
	if err := prepared.ValidateTarget(events.RouteIdentity{FlowID: "other", FlowInstance: "other/one", EntityID: eventtest.UUID("other-entity")}); err != nil {
		t.Fatalf("unrelated receiver must retain independent admission: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress("", "work.ready", "", "", nil, 0, runID, "", events.EventEnvelope{}, at)
	if err := prepared.ValidatePublication(fact, evt); err != nil {
		t.Fatal(err)
	}
	foreign, _ := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("b", 64))
	if err := prepared.ValidatePublication(foreign, evt); err == nil {
		t.Fatal("foreign source acquired prepared state")
	}
	other := eventtest.RunCreatingRootIngress("", "work.ready", "", "", nil, 0, eventtest.UUID("other-run"), "", events.EventEnvelope{}, at)
	if err := prepared.ValidatePublication(fact, other); err == nil {
		t.Fatal("foreign run acquired prepared state")
	}
	// Caller-owned JSON buffers cannot alter the frozen digest or candidate.
	record.Fields[2] = 'X'
	if err := prepared.ValidateMutation(record, WorkflowLifecycleMutationPlan{}); err == nil {
		t.Fatal("mutated original buffer retained authority")
	}
	record.Status, record.TerminatedAt = "terminated", at
	terminal, err := prepareWorkflowPublicationState(record, WorkflowLifecycleMutationPlan{}, "review", fact)
	if err != nil {
		t.Fatal(err)
	}
	if err := terminal.Candidate().Availability.Validate(nil, "review"); err == nil {
		t.Fatal("prospective terminal owner remained available")
	}
	if err := (PreparedWorkflowPublicationState{}).ValidatePublication(fact, evt); err == nil {
		t.Fatal("zero prospective authority was accepted")
	}
}
