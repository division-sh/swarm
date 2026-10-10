package genericschedule

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/uuid"
)

func publishedJoinContinuationCommand(t *testing.T, command AdmissionCommand, ref timeridentity.JoinRef, kind timeridentity.TimerHandleKind) AdmissionCommand {
	t.Helper()
	var handle timeridentity.TimerHandle
	var err error
	if kind == timeridentity.TimerHandleJoinComplete {
		handle, err = timeridentity.JoinCompleteHandle(ref)
	} else {
		handle, err = timeridentity.JoinTimeoutHandle(ref)
	}
	if err != nil {
		t.Fatal(err)
	}
	command.Payload, err = canonicaljson.FromGo(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	entry := ref.StageEntry()
	command.RunID, command.EntityID = entry.RunID, entry.EntityID
	command.TaskID, command.ScheduleKey, command.EventType = handle.TaskID(), handle.TaskID(), handle.EventType()
	if ref.FlowPath() == "." {
		command.FlowInstance = ""
		command.RoutingSource, err = events.NewRootRoutingSource(entry.EntityID)
	} else {
		command.FlowInstance = entry.InstancePath
		command.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
			FlowID: ref.FlowPath(), FlowInstance: entry.InstancePath, EntityID: entry.EntityID,
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func publishedJoinContinuationChild(t *testing.T, source AdmissionCommand) AdmissionCommand {
	t.Helper()
	handle, ref, ok := timeridentity.ParseJoinHandle(source.Payload.Interface().(map[string]any))
	if !ok {
		t.Fatal("missing fixture join handle")
	}
	entry := ref.StageEntry()
	if entry.OriginRunID == "" {
		entry.OriginRunID = entry.RunID
	}
	entry.RunID, entry.EntityID = uuid.NewString(), uuid.NewString()
	entry.InstanceID, entry.InstancePath = "child-instance", "orders/child-instance"
	if ref.FlowPath() == "." {
		entry.EntityID, entry.InstanceID, entry.InstancePath = entry.RunID, entry.RunID, entry.RunID
	}
	generation := ref.Generation()
	if generation.Valid() {
		generation.ActivationID, generation.RevisionID = uuid.NewString(), "child-revision"
	}
	childRef, err := ref.Declaration().BindStageEntry(entry, generation)
	if err != nil {
		t.Fatal(err)
	}
	return publishedJoinContinuationCommand(t, source, childRef, handle.Kind())
}

func publishedJoinContinuationFixture(t *testing.T, flow string, kind timeridentity.TimerHandleKind, mode executionmode.Mode, inherited, generated bool) (Activation, events.Event, AdmissionCommand) {
	t.Helper()
	generation := attemptgeneration.Generation{}
	if generated {
		generation = attemptgeneration.Generation{FlowID: flow, LoopID: "review", ActivationID: uuid.NewString(), RevisionField: "revision", RevisionID: "source-revision", Attempt: 7}
	}
	command := testJoinScheduleCommand(t, flow, map[string]string{"orders": "orders/order-1"}[flow], generation)
	command.ExecutionMode = mode
	_, ref, _ := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
	entry := ref.StageEntry()
	if inherited {
		entry.OriginRunID = uuid.NewString()
	}
	entry.Cause, entry.EventID, entry.OccurrenceID, entry.TransitionID = "delivery", uuid.NewString(), "delivery:original", "rule:original"
	ref, err := ref.Declaration().BindStageEntry(entry, generation)
	if err != nil {
		t.Fatal(err)
	}
	command = publishedJoinContinuationCommand(t, command, ref, kind)
	activation, _, original := publishedOccurrenceTestActivation(t, exactWorkflowJoinSchedule(t, command))
	return activation, original, publishedJoinContinuationChild(t, command)
}

func TestPublishedJoinContinuationPreservesExactPublication(t *testing.T) {
	for _, flow := range []string{"", "orders"} {
		for _, kind := range []timeridentity.TimerHandleKind{timeridentity.TimerHandleJoinComplete, timeridentity.TimerHandleJoinTimeout} {
			for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
				for _, inherited := range []bool{false, true} {
					for _, generated := range []bool{false, true} {
						t.Run(flow+"/"+string(kind)+"/"+string(mode)+"/"+map[bool]string{false: "original", true: "inherited"}[inherited]+"/"+map[bool]string{false: "bare", true: "generation"}[generated], func(t *testing.T) {
							source, original, child := publishedJoinContinuationFixture(t, flow, kind, mode, inherited, generated)
							before, err := source.EvidenceDigest()
							if err != nil {
								t.Fatal(err)
							}
							continuation, err := ProjectPublishedJoinContinuation(source, original, child)
							if err != nil || !continuation.Present() || !reflect.DeepEqual(continuation.ChildCommand(), child.Canonical()) ||
								!reflect.DeepEqual(continuation.SourceEvent(), original) {
								t.Fatalf("continuation=%+v err=%v", continuation, err)
							}
							id := activityidentity.ForkLineageEventID(child.RunID, original.ID())
							event, err := continuation.Event(id, "selection:exact")
							if err != nil {
								t.Fatal(err)
							}
							lineage, found := event.SelectedForkLineage()
							if !found || event.AdmissionClass() != events.EventAdmissionSelectedForkReplay || event.ID() != id || event.RunID() != child.RunID ||
								lineage.SourceRunID() != source.Command.RunID || lineage.SourceEventID() != original.ID() ||
								lineage.AuthorityStamp() != "selection:exact" || lineage.TaskID() != child.TaskID ||
								!event.Producer().Equal(original.Producer()) || !event.CreatedAt().Equal(original.CreatedAt()) ||
								!event.CreatedAt().Equal(source.CurrentDueAt) || event.CreatedAt().Equal(source.CurrentEventAdmittedAt) ||
								event.Type() != original.Type() || event.ExecutionMode() != mode || event.TaskID() != child.TaskID || event.TaskID() == original.TaskID() ||
								event.RoutingSource() != child.RoutingSource || event.Envelope().Source != child.RoutingSource.Route() ||
								event.EntityID() != child.EntityID || event.FlowInstance() != child.FlowInstance ||
								event.ParentEventID() != "" || event.ChainDepth() != 0 || !event.TargetRoute().Empty() {
								t.Fatalf("wrong child publication: event=%+v lineage=%+v", event, lineage)
							}
							expected, err := occurrencePublicationEvent(Activation{Command: child}, Occurrence{EventID: id, DueAt: source.CurrentDueAt})
							if err != nil || !bytes.Equal(event.Payload(), expected.Payload()) {
								t.Fatalf("child payload=%s want=%s err=%v", event.Payload(), expected.Payload(), err)
							}
							if generated && !bytes.Contains(event.Payload(), []byte(`"attempt":7,`)) {
								t.Fatalf("generation numeric emission lost integer kind: %s", event.Payload())
							}
							repeated, err := continuation.Event(id, "selection:exact")
							if err != nil || !reflect.DeepEqual(event, repeated) {
								t.Fatalf("event projection is not deterministic: %v", err)
							}
							after, err := source.EvidenceDigest()
							if err != nil || before != after || source.ValidateForkJoinRestorationSource() == nil {
								t.Fatal("continuation changed or rearmed published source")
							}
						})
					}
				}
			}
		}
	}
}

func TestPublishedJoinContinuationRejectsChangedChild(t *testing.T) {
	for _, change := range []struct {
		name string
		edit func(*testing.T, *AdmissionCommand)
	}{
		{"due", func(_ *testing.T, c *AdmissionCommand) { c.Due = AbsoluteDue(c.Due.Absolute.Add(time.Second)) }},
		{"relative_due", func(_ *testing.T, c *AdmissionCommand) { c.Due = DelayDue(time.Second) }},
		{"recurring", func(_ *testing.T, c *AdmissionCommand) { c.Due = EveryDue(time.Second) }},
		{"mode", func(_ *testing.T, c *AdmissionCommand) { c.ExecutionMode = executionmode.Mock }},
		{"owner", func(_ *testing.T, c *AdmissionCommand) { c.OwnerID = "unrelated" }},
		{"reply", func(_ *testing.T, c *AdmissionCommand) { c.ReplyContext = uuid.NewString() }},
		{"task", func(_ *testing.T, c *AdmissionCommand) { c.TaskID = "unrelated" }},
		{"schedule_key", func(_ *testing.T, c *AdmissionCommand) { c.ScheduleKey = "unrelated" }},
		{"event_type", func(_ *testing.T, c *AdmissionCommand) { c.EventType = "platform.join_timeout" }},
		{"entity", func(_ *testing.T, c *AdmissionCommand) { c.EntityID = uuid.NewString() }},
		{"route", func(t *testing.T, c *AdmissionCommand) {
			c.RoutingSource, _ = events.NewRootRoutingSource(uuid.NewString())
		}},
		{"run_spelling", func(_ *testing.T, c *AdmissionCommand) { c.RunID = "not-a-run-uuid" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			source, original, child := publishedJoinContinuationFixture(t, "", timeridentity.TimerHandleJoinComplete, executionmode.Live, false, false)
			change.edit(t, &child)
			if got, err := ProjectPublishedJoinContinuation(source, original, child); err == nil || got.Present() {
				t.Fatalf("changed child accepted: %+v err=%v", got, err)
			}
		})
	}
	for _, field := range []string{"origin", "cause", "event", "occurrence", "transition", "declaration", "kind", "generation_absent", "generation_loop", "generation_attempt", "generation_field"} {
		t.Run(field, func(t *testing.T) {
			source, original, child := publishedJoinContinuationFixture(t, "", timeridentity.TimerHandleJoinComplete, executionmode.Live, true, true)
			handle, ref, _ := timeridentity.ParseJoinHandle(child.Payload.Interface().(map[string]any))
			entry, generation, declaration, kind := ref.StageEntry(), ref.Generation(), ref.Declaration(), handle.Kind()
			switch field {
			case "origin":
				entry.OriginRunID = source.Command.RunID
			case "cause":
				entry.Cause = "gate"
			case "event":
				entry.EventID = uuid.NewString()
			case "occurrence":
				entry.OccurrenceID = "other-occurrence"
			case "transition":
				entry.TransitionID = "other-transition"
			case "declaration":
				var err error
				declaration, err = timeridentity.NewJoinRef(ref.Node(), "other.event", ref.Stage(), ref.JoinID())
				if err != nil {
					t.Fatal(err)
				}
			case "kind":
				kind = timeridentity.TimerHandleJoinTimeout
			case "generation_absent":
				generation = attemptgeneration.Generation{}
			case "generation_loop":
				generation.LoopID = "different-loop"
			case "generation_attempt":
				generation.Attempt++
			case "generation_field":
				generation.RevisionField = "other_revision"
			}
			ref, err := declaration.BindStageEntry(entry, generation)
			if err != nil {
				t.Fatal(err)
			}
			child = publishedJoinContinuationCommand(t, child, ref, kind)
			if err := child.Validate(); err != nil {
				t.Fatalf("substitution fixture must be self-consistent: %v", err)
			}
			if got, err := ProjectPublishedJoinContinuation(source, original, child); err == nil || got.Present() {
				t.Fatalf("substituted provenance accepted: %+v err=%v", got, err)
			}
		})
	}
	source, original, _ := publishedJoinContinuationFixture(t, "", timeridentity.TimerHandleJoinComplete, executionmode.Live, false, false)
	if got, err := ProjectPublishedJoinContinuation(source, original, source.Command); err == nil || got.Present() {
		t.Fatalf("source reused as child: %+v err=%v", got, err)
	}
}

func TestPublishedJoinContinuationRequiresAcceptedOriginal(t *testing.T) {
	for _, change := range []string{"unpublished", "unaccepted", "wrong_event", "wrong_producer", "wrong_payload", "wrong_task", "wrong_timestamp", "not_join"} {
		t.Run(change, func(t *testing.T) {
			source, original, child := publishedJoinContinuationFixture(t, "", timeridentity.TimerHandleJoinComplete, executionmode.Live, false, false)
			facts := publishedOccurrenceTestFacts(original)
			switch change {
			case "unpublished":
				source.Status, source.CurrentEventID, source.CurrentEventAdmittedAt, source.FiredAt, source.AcceptedAt = StatusActive, "", time.Time{}, time.Time{}, time.Time{}
			case "unaccepted":
				source.AcceptedAt = time.Time{}
			case "wrong_event":
				facts.ID = uuid.NewString()
			case "wrong_producer":
				facts.Producer.ID = "selection:exact"
			case "wrong_payload":
				facts.Payload = []byte(`{}`)
			case "wrong_task":
				facts.TaskID = "other-task"
			case "wrong_timestamp":
				facts.CreatedAt = source.CurrentEventAdmittedAt
			case "not_join":
				source.Command.EventType = "platform.timer_fired"
				source.Command.Payload, _ = canonicaljson.Decode([]byte(`{}`))
				source.ImmutableHash, _ = source.Command.ImmutableHash()
				var err error
				original, err = occurrencePublicationEvent(source, Occurrence{ActivationID: source.ID, EventID: source.CurrentEventID, DueAt: source.CurrentDueAt, AdmittedAt: source.CurrentEventAdmittedAt})
				if err != nil {
					t.Fatal(err)
				}
			}
			if change != "not_join" {
				var err error
				original, err = events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: source.Command.RunID})
				if err != nil {
					t.Fatal(err)
				}
			}
			if got, err := ProjectPublishedJoinContinuation(source, original, child); err == nil || got.Present() {
				t.Fatalf("invalid original accepted: %+v err=%v", got, err)
			}
		})
	}
}

func TestPublishedJoinContinuationSealedValueAndEventIdentity(t *testing.T) {
	var empty PublishedJoinContinuation
	if empty.Present() || !reflect.DeepEqual(empty.SourceEvent(), events.Event{}) || !reflect.DeepEqual(empty.ChildCommand(), AdmissionCommand{}) {
		t.Fatal("zero continuation is not absent")
	}
	if _, err := empty.Event(uuid.NewString(), "selection:exact"); err == nil {
		t.Fatal("absent continuation published")
	}
	source, original, child := publishedJoinContinuationFixture(t, "", timeridentity.TimerHandleJoinComplete, executionmode.Live, false, false)
	continuation, err := ProjectPublishedJoinContinuation(source, original, child)
	if err != nil {
		t.Fatal(err)
	}
	id := activityidentity.ForkLineageEventID(child.RunID, original.ID())
	for _, invalid := range []struct{ id, authority string }{
		{uuid.NewString(), "selection:exact"}, {original.ID(), "selection:exact"}, {" " + id, "selection:exact"}, {id, ""}, {id, " "},
	} {
		if _, err := continuation.Event(invalid.id, invalid.authority); err == nil {
			t.Fatalf("invalid event frame accepted: %+v", invalid)
		}
	}
	source.Command.RunID, child.RunID = uuid.NewString(), uuid.NewString()
	payload := original.Payload()
	payload[0] = '!'
	returned := continuation.ChildCommand()
	returned.TaskID = "changed"
	if _, err := continuation.Event(id, "selection:exact"); err != nil || !reflect.DeepEqual(continuation.SourceEvent(), original) || continuation.ChildCommand().TaskID != continuation.child.TaskID {
		t.Fatalf("input or accessor mutation changed sealed evidence: %v", err)
	}
}

func TestPublishedJoinContinuationEmptyConstructionWithoutDeadline(t *testing.T) {
	command := testJoinScheduleCommand(t, "", "", attemptgeneration.Generation{})
	_, ref, _ := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
	arm, err := joinruntime.NewActivation(ref, []string{}, nil, command.Due.Absolute, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	// Model the lifecycle's empty-membership close and immediate handle, not
	// the connected construction/publication/delivery path.
	arm.Close(joinruntime.CloseReasonComplete, true, false)
	handle, err := timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	arm, err = arm.WithTimerHandle(handle, arm.ArmedAt)
	if err != nil {
		t.Fatal(err)
	}
	if arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonComplete ||
		!arm.OutcomePending || arm.OutcomeFired || !arm.DeadlineAt.IsZero() || !arm.EmptyCompletionWasArmed() {
		t.Fatalf("empty construction did not arm pending completion: %+v", arm)
	}
	command, err = WorkflowJoinAdmission(arm, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	source, _, original := publishedOccurrenceTestActivation(t, exactWorkflowJoinSchedule(t, command))
	continuation, err := ProjectPublishedJoinContinuation(source, original, publishedJoinContinuationChild(t, command))
	if err != nil {
		t.Fatal(err)
	}
	event, err := continuation.Event(activityidentity.ForkLineageEventID(continuation.ChildCommand().RunID, original.ID()), "selection:empty")
	if err != nil || event.Type() != "platform.join_complete" || !arm.OutcomePending || arm.OutcomeFired {
		t.Fatalf("empty completion projection consumed its arm: event=%+v arm=%+v err=%v", event, arm, err)
	}
}

func TestPublishedJoinContinuationRepeatedProjectionPreservesFirstOrigin(t *testing.T) {
	first, original, child := publishedJoinContinuationFixture(t, "orders", timeridentity.TimerHandleJoinComplete, executionmode.Live, false, true)
	if _, err := ProjectPublishedJoinContinuation(first, original, child); err != nil {
		t.Fatal(err)
	}
	// A later published generic occurrence on the inherited arm is a new
	// source publication, while its stage-entry origin remains the first run.
	second, _, secondOriginal := publishedOccurrenceTestActivation(t, exactWorkflowJoinSchedule(t, child))
	grandchild := publishedJoinContinuationChild(t, child)
	continuation, err := ProjectPublishedJoinContinuation(second, secondOriginal, grandchild)
	if err != nil {
		t.Fatal(err)
	}
	_, ref, _ := timeridentity.ParseJoinHandle(continuation.ChildCommand().Payload.Interface().(map[string]any))
	if ref.StageEntry().OriginRunID != first.Command.RunID || ref.StageEntry().OriginRunID == second.Command.RunID {
		t.Fatalf("repeated fork replaced original origin: %+v", ref.StageEntry())
	}
	event, err := continuation.Event(activityidentity.ForkLineageEventID(grandchild.RunID, secondOriginal.ID()), "selection:again")
	if err != nil {
		t.Fatal(err)
	}
	lineage, ok := event.SelectedForkLineage()
	if !ok || lineage.SourceRunID() != second.Command.RunID || lineage.SourceEventID() != secondOriginal.ID() {
		t.Fatalf("immediate event lineage conflated with retained stage origin: %+v", lineage)
	}
}

func TestPublishedJoinContinuationPreservesEntityAcrossDistinctRunScopes(t *testing.T) {
	source, original, child := publishedJoinContinuationFixture(t, "orders", timeridentity.TimerHandleJoinComplete, executionmode.Live, false, false)
	handle, ref, _ := timeridentity.ParseJoinHandle(child.Payload.Interface().(map[string]any))
	entry := ref.StageEntry()
	entry.EntityID = source.Command.EntityID
	ref, err := ref.Declaration().BindStageEntry(entry, ref.Generation())
	if err != nil {
		t.Fatal(err)
	}
	child = publishedJoinContinuationCommand(t, child, ref, handle.Kind())
	continuation, err := ProjectPublishedJoinContinuation(source, original, child)
	if err != nil || !continuation.Present() {
		t.Fatalf("same entity in a distinct run refused: %+v err=%v", continuation, err)
	}
	event, err := continuation.Event(activityidentity.ForkLineageEventID(child.RunID, original.ID()), "selection:shared-entity")
	if err != nil || event.RunID() == source.Command.RunID || event.EntityID() != source.Command.EntityID {
		t.Fatalf("child publication changed run-scoped entity meaning: %+v err=%v", event, err)
	}
}

func TestPublishedJoinContinuationRejectsNonObjectPayloadsWithoutPanic(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `"text"`, `7`, `true`} {
		for _, side := range []string{"source", "child"} {
			t.Run(side+"/"+raw, func(t *testing.T) {
				source, original, child := publishedJoinContinuationFixture(t, "", timeridentity.TimerHandleJoinComplete, executionmode.Live, false, false)
				payload, err := canonicaljson.Decode([]byte(raw))
				if err != nil {
					t.Fatal(err)
				}
				if side == "source" {
					source.Command.Payload = payload
				} else {
					child.Payload = payload
				}
				if got, err := ProjectPublishedJoinContinuation(source, original, child); err == nil || got.Present() {
					t.Fatalf("non-object payload accepted: %+v err=%v", got, err)
				}
			})
		}
	}
}

func TestPublishedJoinContinuationValidateEventOwnsLineageAndImmutableFrame(t *testing.T) {
	for _, fault := range []string{"exact", "prepared_target", "destination", "source_run", "source_event", "authority", "lineage_task", "event_task", "producer", "created", "numeric_payload", "routing_source", "event_id", "class", "mode", "type", "depth"} {
		t.Run(fault, func(t *testing.T) {
			source, original, child := publishedJoinContinuationFixture(t, "", timeridentity.TimerHandleJoinComplete, executionmode.Live, false, true)
			continuation, err := ProjectPublishedJoinContinuation(source, original, child)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := continuation.Event(activityidentity.ForkLineageEventID(child.RunID, original.ID()), "selection:exact")
			if err != nil {
				t.Fatal(err)
			}
			facts := publishedOccurrenceTestFacts(expected)
			destination, sourceRun, sourceEvent, stamp, task, mode := child.RunID, source.Command.RunID, original.ID(), "selection:exact", child.TaskID, child.ExecutionMode
			switch fault {
			case "prepared_target":
				facts.Envelope = events.EnvelopeForTargetRoute(facts.Envelope, events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/one", EntityID: uuid.NewString()})
			case "destination":
				destination = uuid.NewString()
			case "source_run":
				sourceRun = uuid.NewString()
			case "source_event":
				sourceEvent = uuid.NewString()
			case "authority":
				stamp = "selection:other"
			case "lineage_task":
				task = "other-task"
			case "event_task":
				facts.TaskID = "other-task"
			case "producer":
				facts.Producer.ID = stamp
			case "created":
				facts.CreatedAt = source.CurrentEventAdmittedAt
			case "numeric_payload":
				facts.Payload = []byte(strings.Replace(string(facts.Payload), `"attempt":7`, `"attempt":7.0`, 1))
			case "routing_source":
				entity := uuid.NewString()
				facts.RoutingSource, _ = events.NewRootRoutingSource(entity)
				facts.Envelope = events.EventEnvelope{EntityID: entity}
			case "event_id":
				facts.ID = uuid.NewString()
			case "mode":
				mode = executionmode.Mock
			case "type":
				facts.Type = "platform.join_timeout"
			case "depth":
				facts.ChainDepth = 1
			}
			lineage, err := events.NewSelectedForkLineage(destination, sourceRun, sourceEvent, stamp, task, mode)
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Facts: facts, Lineage: lineage})
			if fault == "class" {
				candidate, err = events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: child.RunID})
			}
			if err != nil {
				t.Fatalf("substitution fixture: %v", err)
			}
			err = continuation.ValidateEvent(candidate, "selection:exact")
			if fault == "exact" || fault == "prepared_target" {
				if err != nil {
					t.Fatalf("immutable publication refused: %v", err)
				}
			} else if err == nil {
				t.Fatal("substituted selected publication accepted")
			}
		})
	}
}
