package runforkreadiness

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func inheritedArrivalAdmissionFixture(t *testing.T, flowID string) (*contracts.WorkflowContractBundle, genericschedule.Activation, timeridentity.JoinRef) {
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
		t.Fatalf("canonical arrival entry: %+v enters=%t err=%v", entry, enters, err)
	}
	ref, err := timeridentity.NewJoinRef(node, event, "awaiting", "members")
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	join, err := joinruntime.NewActivation(ref, []string{"member-a", "member-b"}, nil, arm, arm.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	command, err := genericschedule.WorkflowJoinAdmission(join, executionmode.Mock)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	schedule := genericschedule.Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash, AdmittedAt: arm,
		InitialDueAt: join.DeadlineAt, CurrentDueAt: join.DeadlineAt, Status: genericschedule.StatusActive}
	if err := genericschedule.ValidateWorkflowJoinScheduleRelation(join, schedule); err != nil {
		t.Fatal(err)
	}
	bundle := &contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{Joins: []contracts.WorkflowJoinPlan{{
		Node: node, HandlerEvent: event, Mode: contracts.WorkflowJoinModeArrival,
		Spec: contracts.JoinSpec{ID: "members", Stage: "awaiting", Members: contracts.JoinMembersSpec{From: "state.expected", By: "payload.member_id"},
			Deadline: &contracts.JoinDeadlineSpec{After: "1h", From: contracts.JoinDeadlineFromStageEntry}},
	}}}}
	return bundle, schedule, ref
}

func TestInheritedArrivalAdmissionMatchesExactSelectedDeclaration(t *testing.T) {
	for _, flowID := range []string{".", "orders"} {
		for _, change := range []string{"retained", "removed", "wrong_flow", "wrong_event", "wrong_stage", "wrong_join_id", "delay_changed", "prepared_removed", "canceled_removed"} {
			t.Run(flowID+"/"+change, func(t *testing.T) {
				bundle, source, ref := inheritedArrivalAdmissionFixture(t, flowID)
				selected := &bundle.Semantics.Joins[0]
				want := genericschedule.ForkJoinRuleRemoved
				switch change {
				case "retained":
					want = genericschedule.ForkJoinRetained
				case "removed":
					bundle.Semantics.Joins = nil
				case "wrong_flow":
					selected.Node = identitytest.FlowNode(t, "unrelated", "join-node")
				case "wrong_event":
					selected.HandlerEvent = "other.completed"
				case "wrong_stage":
					selected.Spec.Stage = "other"
				case "wrong_join_id":
					selected.Spec.ID = "other"
				case "delay_changed":
					selected.Spec.Deadline.After = "24h"
					want = genericschedule.ForkJoinRetained
				case "prepared_removed":
					bundle.Semantics.Joins = nil
					source.CurrentEventID = genericschedule.OccurrenceEventID(source.ID, source.CurrentDueAt)
					source.CurrentEventAdmittedAt = source.CurrentDueAt.Add(time.Minute)
				case "canceled_removed":
					bundle.Semantics.Joins = nil
					source.Status, source.CancelCause, source.CancelledAt = genericschedule.StatusCancelled, "join_stage_exit", source.CurrentDueAt.Add(time.Minute)
					want = genericschedule.ForkJoinRetained
				}
				if err := source.Validate(); err != nil {
					t.Fatal(err)
				}
				before := source.Canonical()
				arrivals, err := admitInheritedArrivals(semanticview.Wrap(bundle), runfork.RunForkPlan{SourceRunID: source.Command.RunID, JoinSchedules: []genericschedule.Activation{source}})
				if err != nil || len(arrivals) != 1 {
					t.Fatalf("arrival declaration admission: %+v err=%v", arrivals, err)
				}
				digest, err := source.EvidenceDigest()
				if err != nil {
					t.Fatal(err)
				}
				entry := arrivals[source.ID]
				if entry.digest != digest || !entry.ref.Equal(ref) || entry.disposition != want {
					t.Fatalf("admission lost exact source binding: %+v want=%s", entry, want)
				}
				admitted := Admission{sealed: &admittedProjection{arrivals: arrivals}}
				got, err := admitted.SelectInheritedArrivalJoin(source)
				if err != nil || got != want {
					t.Fatalf("selected disposition=%s want=%s err=%v", got, want, err)
				}
				refs, err := admitted.RemovedInheritedArrivalRefs()
				if err != nil || want == genericschedule.ForkJoinRuleRemoved && (len(refs) != 1 || !refs[0].Equal(ref)) || want == genericschedule.ForkJoinRetained && len(refs) != 0 {
					t.Fatalf("removed references do not match admitted disposition: %+v err=%v", refs, err)
				}
				if !reflect.DeepEqual(source, before) || !source.CurrentDueAt.Equal(source.AdmittedAt.Add(time.Hour)) {
					t.Fatal("selected declaration rewrote source due, preparation, cancellation, or owner")
				}
			})
		}
	}
}

func TestInheritedArrivalAdmissionRejectsDuplicateForeignAndCorruptEvidence(t *testing.T) {
	for _, flowID := range []string{".", "orders"} {
		for _, fault := range []string{"duplicate", "foreign_run", "zero", "hash", "changed_due", "candidate_id", "unstamped_candidate"} {
			t.Run(flowID+"/"+fault, func(t *testing.T) {
				bundle, source, _ := inheritedArrivalAdmissionFixture(t, flowID)
				plan := runfork.RunForkPlan{SourceRunID: source.Command.RunID, JoinSchedules: []genericschedule.Activation{source}}
				switch fault {
				case "duplicate":
					plan.JoinSchedules = append(plan.JoinSchedules, source)
				case "foreign_run":
					plan.SourceRunID = uuid.NewString()
				case "zero":
					plan.JoinSchedules[0] = genericschedule.Activation{}
				case "hash":
					plan.JoinSchedules[0].ImmutableHash = ""
				case "changed_due":
					plan.JoinSchedules[0].CurrentDueAt = source.CurrentDueAt.Add(time.Hour)
				case "candidate_id":
					plan.JoinSchedules[0].CurrentEventID = uuid.NewString()
					plan.JoinSchedules[0].CurrentEventAdmittedAt = source.CurrentDueAt.Add(time.Minute)
				case "unstamped_candidate":
					plan.JoinSchedules[0].CurrentEventID = genericschedule.OccurrenceEventID(source.ID, source.CurrentDueAt)
				}
				before := append([]genericschedule.Activation(nil), plan.JoinSchedules...)
				if got, err := admitInheritedArrivals(semanticview.Wrap(bundle), plan); err == nil || got != nil {
					t.Fatalf("bad evidence obtained selected disposition: %+v err=%v", got, err)
				}
				if !reflect.DeepEqual(before, plan.JoinSchedules) {
					t.Fatal("arrival admission refusal changed source evidence")
				}
			})
		}
	}
}

func TestInheritedArrivalSelectorBindsExactSealedEvidence(t *testing.T) {
	for _, flowID := range []string{".", "orders"} {
		bundle, source, _ := inheritedArrivalAdmissionFixture(t, flowID)
		bundle.Semantics.Joins = nil
		arrivals, err := admitInheritedArrivals(semanticview.Wrap(bundle), runfork.RunForkPlan{SourceRunID: source.Command.RunID, JoinSchedules: []genericschedule.Activation{source}})
		if err != nil {
			t.Fatal(err)
		}
		admitted := Admission{sealed: &admittedProjection{arrivals: arrivals}}
		for _, fault := range []string{"zero", "foreign_id", "arm", "mode", "prepared", "canceled", "hash"} {
			t.Run(flowID+"/"+fault, func(t *testing.T) {
				bad := source.Canonical()
				switch fault {
				case "zero":
					bad = genericschedule.Activation{}
				case "foreign_id":
					bad.ID = uuid.NewString()
				case "arm":
					bad.AdmittedAt = bad.AdmittedAt.Add(-time.Minute)
				case "mode":
					bad.Command.ExecutionMode = executionmode.Live
					bad.ImmutableHash, err = bad.Command.ImmutableHash()
					if err != nil {
						t.Fatal(err)
					}
				case "prepared":
					bad.CurrentEventID = genericschedule.OccurrenceEventID(bad.ID, bad.CurrentDueAt)
					bad.CurrentEventAdmittedAt = bad.CurrentDueAt.Add(time.Minute)
				case "canceled":
					bad.Status, bad.CancelCause, bad.CancelledAt = genericschedule.StatusCancelled, "join_stage_exit", bad.CurrentDueAt.Add(time.Minute)
				case "hash":
					bad.ImmutableHash = ""
				}
				if fault != "zero" && fault != "hash" {
					if err := bad.Validate(); err != nil {
						t.Fatalf("drift fixture must remain otherwise valid: %v", err)
					}
				}
				if got, err := admitted.SelectInheritedArrivalJoin(bad); err == nil || got != "" {
					t.Fatalf("changed source retained sealed authority: disposition=%s err=%v", got, err)
				}
				if got, err := admitted.SelectInheritedArrivalJoin(source); err != nil || got != genericschedule.ForkJoinRuleRemoved {
					t.Fatalf("refusal changed exact sealed source: disposition=%s err=%v", got, err)
				}
			})
		}
		if got, err := (Admission{}).SelectInheritedArrivalJoin(source); err == nil || got != "" {
			t.Fatal("zero admission selected inherited arrival")
		}
		if refs, err := (Admission{}).RemovedInheritedArrivalRefs(); err == nil || refs != nil {
			t.Fatal("zero admission returned removal authority")
		}
	}
}

func TestInheritedArrivalAdmissionSealIgnoresExternalDeclarationAndRefMutation(t *testing.T) {
	for _, flowID := range []string{".", "orders"} {
		for _, removed := range []bool{false, true} {
			name := flowID + "/retained"
			if removed {
				name = flowID + "/removed"
			}
			t.Run(name, func(t *testing.T) {
				bundle, source, ref := inheritedArrivalAdmissionFixture(t, flowID)
				declarations := bundle.Semantics.Joins
				want := genericschedule.ForkJoinRetained
				if removed {
					bundle.Semantics.Joins = nil
					want = genericschedule.ForkJoinRuleRemoved
				}
				plan := runfork.RunForkPlan{SourceRunID: source.Command.RunID, JoinSchedules: []genericschedule.Activation{source}}
				arrivals, err := admitInheritedArrivals(semanticview.Wrap(bundle), plan)
				if err != nil {
					t.Fatal(err)
				}
				admitted := Admission{sealed: &admittedProjection{arrivals: arrivals}}
				bundle.Semantics.Joins = nil
				if removed {
					bundle.Semantics.Joins = declarations
				}
				fresh, err := admitInheritedArrivals(semanticview.Wrap(bundle), plan)
				if err != nil || fresh[source.ID].disposition == want {
					t.Fatalf("external mutation did not exercise opposite selected policy: %+v err=%v", fresh, err)
				}
				refs, err := admitted.RemovedInheritedArrivalRefs()
				if err != nil || removed && (len(refs) != 1 || !refs[0].Equal(ref)) || !removed && len(refs) != 0 {
					t.Fatalf("sealed removed refs changed with external source: %+v err=%v", refs, err)
				}
				foreign, err := timeridentity.NewJoinRef(identitytest.FlowNode(t, "unrelated", "join-node"), "other.completed", "other", "other")
				if err != nil {
					t.Fatal(err)
				}
				if removed {
					refs[0] = foreign
				} else {
					refs = append(refs, foreign)
				}
				again, err := admitted.RemovedInheritedArrivalRefs()
				if err != nil || removed && (len(again) != 1 || !again[0].Equal(ref)) || !removed && len(again) != 0 {
					t.Fatalf("returned slice mutation changed sealed refs: %+v err=%v", again, err)
				}
				if got, err := admitted.SelectInheritedArrivalJoin(source); err != nil || got != want {
					t.Fatalf("external mutation changed sealed disposition=%s want=%s err=%v", got, want, err)
				}
			})
		}
	}
}
