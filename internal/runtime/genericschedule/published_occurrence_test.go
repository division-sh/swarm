package genericschedule

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

func publishedOccurrenceTestActivation(t *testing.T, activation Activation) (Activation, Occurrence, events.Event) {
	t.Helper()
	activation = activation.Canonical()
	activation.CurrentEventID = OccurrenceEventID(activation.ID, activation.CurrentDueAt)
	activation.CurrentEventAdmittedAt = activation.CurrentDueAt.Add(time.Second)
	activation.Status = StatusFired
	activation.FiredAt, activation.AcceptedAt = activation.CurrentEventAdmittedAt.Add(time.Second), activation.CurrentEventAdmittedAt.Add(time.Second)
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	occurrence := Occurrence{ActivationID: activation.ID, DueAt: activation.CurrentDueAt,
		EventID: activation.CurrentEventID, AdmittedAt: activation.CurrentEventAdmittedAt}
	event, err := occurrencePublicationEvent(activation, occurrence)
	if err != nil {
		t.Fatal(err)
	}
	return activation, occurrence, event
}

func publishedOccurrenceTestFacts(event events.Event) events.EventFacts {
	return events.EventFacts{ID: event.ID(), Type: event.Type(),
		Producer: events.ProducerClaim{Type: event.ProducerType(), ID: event.SourceAgent()},
		TaskID:   event.TaskID(), Payload: event.Payload(), ChainDepth: event.ChainDepth(),
		Envelope: event.Envelope(), RoutingSource: event.RoutingSource(),
		CreatedAt: event.CreatedAt(), ExecutionMode: event.ExecutionMode()}
}

func TestGenericSchedulePublishedOccurrenceRetainsExactJoinEvidence(t *testing.T) {
	for _, flow := range []string{"", "orders"} {
		for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
			t.Run(flow+"/"+string(mode), func(t *testing.T) {
				command := testJoinScheduleCommand(t, flow, map[string]string{"orders": "orders/order-1"}[flow], attemptgeneration.Generation{})
				command.ExecutionMode = mode
				activation, occurrence, event := publishedOccurrenceTestActivation(t, exactWorkflowJoinSchedule(t, command))
				before, err := activation.EvidenceDigest()
				if err != nil {
					t.Fatal(err)
				}
				got, err := activation.ValidatePublishedOccurrence(event)
				if err != nil || got != occurrence || !event.CreatedAt().Equal(occurrence.DueAt) || !event.CreatedAt().Before(occurrence.AdmittedAt) {
					t.Fatalf("retained join publication: got=%+v want=%+v err=%v", got, occurrence, err)
				}
				after, err := activation.EvidenceDigest()
				if err != nil || before != after {
					t.Fatal("publication evidence validation changed activation history")
				}
			})
		}
	}
	activation, occurrence, event := publishedOccurrenceTestActivation(t, forkJoinOriginTestActivation(t))
	if got, err := activation.ValidatePublishedOccurrence(event); err != nil || got != occurrence {
		t.Fatalf("inherited publication evidence: got=%+v err=%v", got, err)
	}
	activation.ForkJoinOrigin.Owner = "unrelated-owner"
	if _, err := activation.ValidatePublishedOccurrence(event); err == nil {
		t.Fatal("publication granted evidence through corrupt inherited provenance")
	}
}

func TestGenericSchedulePublishedOccurrenceUsesActualLifecycleNumericProjection(t *testing.T) {
	activation, occurrence, wakeup := lifecyclePreparedOccurrence(t)
	var err error
	activation.Command.Payload, err = canonicaljson.Decode([]byte(`{"integer":7.0,"fraction":7.5,"safe":9007199254740991}`))
	if err != nil {
		t.Fatal(err)
	}
	activation.ImmutableHash, err = activation.Command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	accepted := activation.Canonical()
	accepted.Status = StatusFired
	accepted.FiredAt, accepted.AcceptedAt = occurrence.AdmittedAt.Add(time.Second), occurrence.AdmittedAt.Add(time.Second)
	var published events.Event
	store := &lifecycleProofStore{activation: activation,
		prepared: PreparedOccurrence{Outcome: PrepareReady, Activation: activation, Occurrence: occurrence},
		commit: func(command CommitCommand) (CommitResult, error) {
			plan := command.Publication.(lifecycleProofPlan)
			published = plan.intent.Event
			return CommitResult{Outcome: CommitCommitted, Next: accepted, Publication: lifecycleProofCommit{plan: plan}}, nil
		},
	}
	lifecycle, err := NewLifecycle(store, &lifecycleProofScheduler{}, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	defer stopLifecycleProof(t, lifecycle)
	result, err := lifecycle.fire(context.Background(), wakeup)
	if err != nil || result.Outcome != CommitCommitted || store.commitCalls != 1 || string(published.Payload()) != `{"fraction":7.5,"integer":7,"safe":9007199254740991}` {
		t.Fatalf("actual lifecycle projection: result=%+v payload=%s err=%v", result, published.Payload(), err)
	}
	if got, err := accepted.ValidatePublishedOccurrence(published); err != nil || got != occurrence {
		t.Fatalf("actual lifecycle publication evidence: got=%+v err=%v", got, err)
	}
	for _, payload := range []string{
		`{"fraction":7.5,"integer":7.0,"safe":9007199254740991}`,
		`{"fraction":7.5,"integer":8,"safe":9007199254740991}`,
		`{"fraction":7.5,"integer":7,"safe":9007199254740990}`,
		`{"fraction":7.5,"integer":7}`,
		`{"fraction":7.5,"integer":7,"safe":9007199254740991,"extra":true}`,
	} {
		facts := publishedOccurrenceTestFacts(published)
		facts.Payload = []byte(payload)
		changed, err := events.NewStandaloneRuntimeControlEvent(events.StandaloneRuntimeEventInput{Facts: facts})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := accepted.ValidatePublishedOccurrence(changed); err == nil {
			t.Fatalf("changed projected numeric payload accepted: %s", payload)
		}
	}
	facts := publishedOccurrenceTestFacts(published)
	facts.Payload = []byte(` { "safe":9007199254740991, "integer":7, "fraction":7.5 } `)
	reordered, err := events.NewStandaloneRuntimeControlEvent(events.StandaloneRuntimeEventInput{Facts: facts})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.ValidatePublishedOccurrence(reordered); err != nil {
		t.Fatalf("insignificant payload formatting changed publication evidence: %v", err)
	}
}

func TestGenericSchedulePublishedOccurrenceRejectsUnacceptedState(t *testing.T) {
	command := testJoinScheduleCommand(t, "orders", "orders/order-1", attemptgeneration.Generation{})
	original, _, event := publishedOccurrenceTestActivation(t, exactWorkflowJoinSchedule(t, command))
	for _, edit := range []struct {
		name  string
		apply func(*Activation)
	}{
		{"prepared_only", func(a *Activation) { a.Status, a.FiredAt, a.AcceptedAt = StatusActive, time.Time{}, time.Time{} }},
		{"missing_acceptance", func(a *Activation) { a.AcceptedAt = time.Time{} }},
		{"missing_firing", func(a *Activation) { a.FiredAt = time.Time{} }},
		{"missing_occurrence", func(a *Activation) { a.CurrentEventID, a.CurrentEventAdmittedAt = "", time.Time{} }},
		{"missing_occurrence_admission", func(a *Activation) { a.CurrentEventAdmittedAt = time.Time{} }},
		{"nondeterministic_occurrence", func(a *Activation) { a.CurrentEventID = uuid.NewString() }},
		{"admission_before_due", func(a *Activation) { a.CurrentEventAdmittedAt = a.CurrentDueAt.Add(-time.Second) }},
		{"fired_before_admission", func(a *Activation) { a.FiredAt = a.CurrentEventAdmittedAt.Add(-time.Second) }},
		{"accepted_before_fired", func(a *Activation) { a.AcceptedAt = a.FiredAt.Add(-time.Second) }},
		{"wrong_due", func(a *Activation) { a.CurrentDueAt = a.CurrentDueAt.Add(time.Second) }},
		{"wrong_hash", func(a *Activation) { a.ImmutableHash = "unrelated" }},
	} {
		t.Run(edit.name, func(t *testing.T) {
			activation := original.Canonical()
			edit.apply(&activation)
			before := activation.Canonical()
			if got, err := activation.ValidatePublishedOccurrence(event); err == nil || got != (Occurrence{}) || !reflect.DeepEqual(activation, before) {
				t.Fatalf("unaccepted evidence escaped or mutated: occurrence=%+v err=%v", got, err)
			}
		})
	}
}

func TestGenericSchedulePublishedOccurrenceRejectsEventSubstitution(t *testing.T) {
	command := testJoinScheduleCommand(t, "orders", "orders/order-1", attemptgeneration.Generation{})
	activation, _, event := publishedOccurrenceTestActivation(t, exactWorkflowJoinSchedule(t, command))
	for _, name := range []string{"id", "run", "mode", "type", "producer", "routing", "task", "payload", "due", "accepted_timestamp", "entity", "flow", "chain_depth", "parent", "admission_class"} {
		t.Run(name, func(t *testing.T) {
			facts, runID := publishedOccurrenceTestFacts(event), event.RunID()
			switch name {
			case "id":
				facts.ID = uuid.NewString()
			case "run":
				runID = uuid.NewString()
			case "mode":
				facts.ExecutionMode = executionmode.Mock
			case "type":
				facts.Type = "platform.unrelated"
			case "producer":
				facts.Producer.ID = "runtime.unrelated"
			case "routing":
				facts.RoutingSource = events.NewPlatformControlRoutingSource()
				facts.Envelope.Source = events.RouteIdentity{}
			case "task":
				facts.TaskID = "unrelated-task"
			case "payload":
				facts.Payload = []byte(`{}`)
			case "due":
				facts.CreatedAt = facts.CreatedAt.Add(time.Second)
			case "accepted_timestamp":
				facts.CreatedAt = activation.AcceptedAt
			case "entity":
				route := event.RoutingSource().Route()
				route.EntityID = uuid.NewString()
				var err error
				facts.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(route)
				if err != nil {
					t.Fatal(err)
				}
				facts.Envelope = events.EnvelopeForSourceRoute(facts.Envelope, route)
			case "flow":
				route := event.RoutingSource().Route()
				route.FlowInstance = "orders/another-order"
				var err error
				facts.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(route)
				if err != nil {
					t.Fatal(err)
				}
				facts.Envelope = events.EnvelopeForSourceRoute(facts.Envelope, route)
			case "chain_depth":
				facts.ChainDepth++
			}
			var changed events.Event
			var err error
			switch name {
			case "parent":
				changed, err = events.NewCausalRuntimeControlEvent(events.CausalRuntimeEventInput{Facts: facts, Lineage: events.EventLineage{RunID: runID, ParentEventID: uuid.NewString(), TaskID: facts.TaskID, ExecutionMode: facts.ExecutionMode}})
			case "admission_class":
				facts.RoutingSource = events.NoRoutingSource()
				facts.Envelope.Source = events.RouteIdentity{}
				changed, err = events.NewRunScopedRuntimeDiagnosticEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: runID})
			default:
				changed, err = events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: runID})
			}
			if err != nil {
				t.Fatalf("construct hostile event %s: %v", name, err)
			}
			if got, err := activation.ValidatePublishedOccurrence(changed); err == nil || got != (Occurrence{}) {
				t.Fatalf("substituted event %s accepted: occurrence=%+v err=%v", name, got, err)
			}
		})
	}
}

func TestGenericSchedulePublishedOccurrenceCannotBorrowPrecedingRecurringAcceptance(t *testing.T) {
	for _, flow := range []string{".", "account/poller"} {
		t.Run(flow, func(t *testing.T) {
			command := instanceScheduleCommand(t, flow)
			admittedAt := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
			first, err := command.Due.FirstDue(admittedAt)
			if err != nil {
				t.Fatal(err)
			}
			hash, err := command.ImmutableHash()
			if err != nil {
				t.Fatal(err)
			}
			activation := Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash,
				AdmittedAt: admittedAt, InitialDueAt: first, CurrentDueAt: first, Status: StatusActive}
			prior := Occurrence{ActivationID: activation.ID, DueAt: first, EventID: OccurrenceEventID(activation.ID, first), AdmittedAt: first.Add(time.Second)}
			event, err := occurrencePublicationEvent(activation, prior)
			if err != nil {
				t.Fatal(err)
			}
			activation.CurrentDueAt, err = command.Due.Next(first)
			if err != nil {
				t.Fatal(err)
			}
			activation.FiredAt, activation.AcceptedAt = prior.AdmittedAt.Add(time.Second), prior.AdmittedAt.Add(time.Second)
			for _, prepared := range []bool{false, true} {
				if prepared {
					activation.CurrentEventID = OccurrenceEventID(activation.ID, activation.CurrentDueAt)
					activation.CurrentEventAdmittedAt = activation.CurrentDueAt.Add(time.Second)
				}
				if err := activation.Validate(); err != nil {
					t.Fatal(err)
				}
				if _, err := activation.ValidatePublishedOccurrence(event); err == nil {
					t.Fatal("prior recurring acceptance proved current publication")
				}
				if prepared {
					current, err := occurrencePublicationEvent(activation, Occurrence{ActivationID: activation.ID, DueAt: activation.CurrentDueAt, EventID: activation.CurrentEventID, AdmittedAt: activation.CurrentEventAdmittedAt})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := activation.ValidatePublishedOccurrence(current); err == nil {
						t.Fatal("prepared recurrence borrowed preceding fired/accepted history")
					}
				}
			}
		})
	}
}
