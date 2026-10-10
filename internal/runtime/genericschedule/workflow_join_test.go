package genericschedule

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/uuid"
)

func TestWorkflowJoinScheduleUsesRetainedArm(t *testing.T) {
	for _, flow := range []string{"", "orders"} {
		t.Run("flow="+flow, func(t *testing.T) {
			path := ""
			if flow != "" {
				path = "orders/order-1"
			}
			original := testJoinScheduleCommand(t, flow, path, attemptgeneration.Generation{})
			_, ref, ok := timeridentity.ParseJoinHandle(original.Payload.Interface().(map[string]any))
			if !ok {
				t.Fatal("missing typed fixture arm")
			}
			join, err := joinruntime.NewActivation(ref, []string{"a", "b"}, nil, original.Due.Absolute.Add(-time.Hour), original.Due.Absolute)
			if err != nil {
				t.Fatal(err)
			}
			before := join
			command, err := WorkflowJoinAdmission(join, executionmode.Live)
			if err != nil || !reflect.DeepEqual(command, original) {
				t.Fatalf("shared projection changed live command: got=%#v want=%#v err=%v", command, original, err)
			}
			schedule := exactWorkflowJoinSchedule(t, command)
			if err := ValidateWorkflowJoinScheduleRelation(join, schedule); err != nil {
				t.Fatal(err)
			}
			for _, edit := range []struct {
				name  string
				apply func(*AdmissionCommand)
			}{
				{"foreign_run", func(c *AdmissionCommand) { c.RunID = uuid.NewString() }},
				{"different_due", func(c *AdmissionCommand) { c.Due = AbsoluteDue(c.Due.Absolute.Add(time.Second)) }},
				{"reply_context", func(c *AdmissionCommand) { c.ReplyContext = uuid.NewString() }},
				{"foreign_entity", func(c *AdmissionCommand) {
					c.EntityID = uuid.NewString()
					var err error
					if flow == "" {
						c.RoutingSource, err = events.NewRootRoutingSource(c.EntityID)
					} else {
						c.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: flow, FlowInstance: path, EntityID: c.EntityID})
					}
					if err != nil {
						t.Fatal(err)
					}
				}},
			} {
				t.Run(edit.name, func(t *testing.T) {
					changed := command
					edit.apply(&changed)
					if edit.name == "foreign_run" || edit.name == "foreign_entity" {
						entry := ref.StageEntry()
						entry.RunID, entry.EntityID = changed.RunID, changed.EntityID
						if flow == "" {
							entry.InstanceID, entry.InstancePath = changed.RunID, changed.RunID
						}
						changedRef, err := ref.Declaration().BindStageEntry(entry, ref.Generation())
						if err != nil {
							t.Fatal(err)
						}
						handle, err := timeridentity.JoinTimeoutHandle(changedRef)
						if err != nil {
							t.Fatal(err)
						}
						changed.Payload, err = canonicaljson.FromGo(handle.PayloadMetadata())
						if err != nil {
							t.Fatal(err)
						}
						changed.ScheduleKey, changed.TaskID = handle.TaskID(), handle.TaskID()
					}
					// Re-hash a lawful schedule: self-consistency is not arm correspondence.
					candidate := exactWorkflowJoinSchedule(t, changed)
					if err := ValidateWorkflowJoinScheduleRelation(join, candidate); err == nil {
						t.Fatal("different immutable schedule admitted as the retained arm")
					}
				})
			}
			entry := ref.StageEntry()
			entry.InstanceID += "-different-entry"
			otherRef, err := ref.Declaration().BindStageEntry(entry, ref.Generation())
			if err != nil {
				t.Fatal(err)
			}
			other, err := join.WithForkReference(otherRef)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateWorkflowJoinScheduleRelation(other, schedule); err == nil {
				t.Fatal("different stage entry reused the old schedule")
			}
			if !reflect.DeepEqual(join, before) {
				t.Fatal("validation changed the frozen join")
			}
		})
	}
}

func TestArrivalScheduleAdmissionRejectsMismatchedStageEntry(t *testing.T) {
	for _, flow := range []string{"", "orders"} {
		for _, field := range []string{"run", "entity", "instance_path"} {
			t.Run(flow+"/"+field, func(t *testing.T) {
				path := ""
				if flow != "" {
					path = "orders/order-1"
				}
				command := testJoinScheduleCommand(t, flow, path, attemptgeneration.Generation{})
				_, ref, _ := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
				entry := ref.StageEntry()
				switch field {
				case "run":
					entry.RunID = uuid.NewString()
				case "entity":
					entry.EntityID = uuid.NewString()
				case "instance_path":
					entry.InstancePath += "/foreign"
				}
				ref, err := ref.Declaration().BindStageEntry(entry, ref.Generation())
				if err != nil {
					t.Fatal(err)
				}
				handle, err := timeridentity.JoinTimeoutHandle(ref)
				if err != nil {
					t.Fatal(err)
				}
				command.Payload, err = canonicaljson.FromGo(handle.PayloadMetadata())
				if err != nil {
					t.Fatal(err)
				}
				command.ScheduleKey, command.TaskID = handle.TaskID(), handle.TaskID()
				if err := command.Validate(); err == nil {
					t.Fatal("different stage-entry owner admitted by the shared generic schedule command")
				}
			})
		}
	}
}

func TestWorkflowJoinCompletionRetainsSeparateDeadline(t *testing.T) {
	original := testJoinScheduleCommand(t, "orders", "orders/order-1", attemptgeneration.Generation{})
	_, ref, _ := timeridentity.ParseJoinHandle(original.Payload.Interface().(map[string]any))
	arm := original.Due.Absolute.Add(-time.Hour)
	join, err := joinruntime.NewActivation(ref, []string{"a"}, nil, arm, original.Due.Absolute)
	if err != nil {
		t.Fatal(err)
	}
	deadline := exactWorkflowJoinSchedule(t, original)
	deadline.Status, deadline.CancelCause, deadline.CancelledAt = StatusCancelled, "join_closed", arm.Add(time.Minute)
	join.Close(joinruntime.CloseReasonComplete, true, false)
	handle, err := timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	join, err = join.WithTimerHandle(handle, arm.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	command, err := WorkflowJoinAdmission(join, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	completion := exactWorkflowJoinSchedule(t, command)
	for _, schedule := range []Activation{deadline, completion} {
		if err := ValidateWorkflowJoinScheduleRelation(join, schedule); err != nil {
			t.Fatal(err)
		}
	}
	if deadline.Command.TaskID == completion.Command.TaskID || deadline.InitialDueAt.Equal(completion.InitialDueAt) {
		t.Fatal("completion collapsed the original timeout identity or due")
	}
	withoutDeadline := join
	withoutDeadline.DeadlineAt = time.Time{}
	if err := ValidateWorkflowJoinScheduleRelation(withoutDeadline, deadline); err == nil {
		t.Fatal("unproven deadline relation admitted")
	}
}

func exactWorkflowJoinSchedule(t *testing.T, command AdmissionCommand) Activation {
	t.Helper()
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	activation := Activation{
		ID: uuid.NewString(), Command: command, ImmutableHash: hash, Status: StatusActive,
		AdmittedAt: command.Due.Absolute.Add(-time.Hour), InitialDueAt: command.Due.Absolute, CurrentDueAt: command.Due.Absolute,
	}
	if err := activation.Validate(); err != nil {
		t.Fatal(err)
	}
	return activation
}
