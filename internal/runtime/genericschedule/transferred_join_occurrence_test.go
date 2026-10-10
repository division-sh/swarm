package genericschedule

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/uuid"
)

func transferredJoinOccurrenceFixture(t *testing.T, flow string, kind timeridentity.TimerHandleKind, mode executionmode.Mode) (joinruntime.Activation, TransferredJoinOccurrence, events.Event) {
	t.Helper()
	source, original, child := publishedJoinContinuationFixture(t, flow, kind, mode, true, true)
	continuation, err := ProjectPublishedJoinContinuation(source, original, child)
	if err != nil {
		t.Fatal(err)
	}
	record, err := continuation.RetainedPublication("selection:first")
	if err != nil {
		t.Fatal(err)
	}
	handle, ref, _ := timeridentity.ParseJoinHandle(child.Payload.Interface().(map[string]any))
	join, err := joinruntime.NewActivation(ref, []string{"item"}, nil, child.Due.Absolute.Add(-time.Hour), child.Due.Absolute)
	if err != nil {
		t.Fatal(err)
	}
	if kind == timeridentity.TimerHandleJoinComplete {
		join.Close(joinruntime.CloseReasonComplete, true, false)
	}
	join, err = join.WithTimerHandle(handle, child.Due.Absolute)
	if err != nil {
		t.Fatal(err)
	}
	join.TransferredPublication = &record
	occurrence, err := NewTransferredJoinOccurrence(join, record)
	if err != nil {
		t.Fatal(err)
	}
	if hash, err := occurrence.Command.ImmutableHash(); err != nil {
		t.Fatal(err)
	} else if want, err := child.ImmutableHash(); err != nil || hash != want {
		t.Fatalf("factory changed the exact arrival arm: got=%s want=%s err=%v", hash, want, err)
	}
	event, err := continuation.Event(record.EventID, record.AuthorityStamp)
	if err != nil {
		t.Fatal(err)
	}
	return join, occurrence, event
}

func TestTransferredJoinOccurrenceProjectsWithoutSourcePublication(t *testing.T) {
	for _, flow := range []string{"", "orders"} {
		for _, kind := range []timeridentity.TimerHandleKind{timeridentity.TimerHandleJoinComplete, timeridentity.TimerHandleJoinTimeout} {
			for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
				t.Run(flow+"/"+string(kind)+"/"+string(mode), func(t *testing.T) {
					_, source, original := transferredJoinOccurrenceFixture(t, flow, kind, mode)
					before, err := source.EvidenceDigest()
					if err != nil {
						t.Fatal(err)
					}
					if err := source.ValidateEvent(original); err != nil {
						t.Fatal(err)
					}
					child := publishedJoinContinuationChild(t, source.Command)
					projected, err := source.Project(child, "selection:second")
					if err != nil {
						t.Fatal(err)
					}
					if projected.Publication.SourceRunID != source.Command.RunID || projected.Publication.SourceEventID != source.Publication.EventID ||
						projected.Publication.EventID != activityidentity.ForkLineageEventID(child.RunID, source.Publication.EventID) {
						t.Fatalf("projection lost immediate source coordinates: %+v", projected.Publication)
					}
					if continuation, err := ProjectTransferredJoinContinuation(source, events.Event{}, child); err == nil || continuation.Present() {
						t.Fatal("coordinate-only projection admitted an absent source publication")
					}
					continuation, err := ProjectTransferredJoinContinuation(source, original, child)
					if err != nil {
						t.Fatal(err)
					}
					record, err := continuation.RetainedPublication("selection:second")
					if err != nil || record != projected.Publication {
						t.Fatalf("actual continuation differs from historical projection: %+v err=%v", record, err)
					}
					event, err := continuation.Event(record.EventID, record.AuthorityStamp)
					if err != nil || projected.ValidateEvent(event) != nil || !event.CreatedAt().Equal(original.CreatedAt()) || !event.Producer().Equal(original.Producer()) {
						t.Fatalf("retransfer changed retained publication frame: %+v err=%v", event, err)
					}
					if _, admitted := event.PayloadAdmission(); admitted {
						t.Fatal("expected frame acquired schema admission")
					}
					_, beforeRef, _ := timeridentity.ParseJoinHandle(source.Command.Payload.Interface().(map[string]any))
					_, afterRef, _ := timeridentity.ParseJoinHandle(projected.Command.Payload.Interface().(map[string]any))
					if beforeRef.StageEntry().OriginRunID != afterRef.StageEntry().OriginRunID {
						t.Fatal("immediate publication lineage replaced original stage-entry provenance")
					}
					if after, err := source.EvidenceDigest(); err != nil || after != before {
						t.Fatal("projection changed retained source evidence")
					}
				})
			}
		}
	}
}

func TestTransferredJoinPublicationPersistsAndClearsOnForkReference(t *testing.T) {
	join, occurrence, _ := transferredJoinOccurrenceFixture(t, "orders", timeridentity.TimerHandleJoinComplete, executionmode.Mock)
	raw, err := json.Marshal(join)
	if err != nil {
		t.Fatal(err)
	}
	var decoded joinruntime.Activation
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil || !reflect.DeepEqual(decoded, join) {
		t.Fatalf("transferred join round trip changed evidence: %+v err=%v", decoded, err)
	}
	if !decoded.OutcomePending || decoded.OutcomeFired || decoded.CloseReason != joinruntime.CloseReasonComplete {
		t.Fatal("retained publication consumed the pending join outcome")
	}
	decoded.TransferredPublication.AuthorityStamp = "selection:clone"
	if join.TransferredPublication.AuthorityStamp != occurrence.Publication.AuthorityStamp {
		t.Fatal("marshaled clone shares retained publication storage")
	}
	buckets := map[string]map[string]any{}
	if err := joinruntime.Store(buckets, join); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := joinruntime.Load(buckets, join.JoinRef().Node(), join.Key())
	if err != nil || !found || !reflect.DeepEqual(loaded, join) {
		t.Fatalf("typed state readback lost transferred evidence: %+v found=%v err=%v", loaded, found, err)
	}
	child := publishedJoinContinuationChild(t, occurrence.Command)
	_, childRef, _ := timeridentity.ParseJoinHandle(child.Payload.Interface().(map[string]any))
	forked, err := join.WithForkReference(childRef)
	if err != nil || forked.TransferredPublication != nil || join.TransferredPublication == nil {
		t.Fatalf("fork retained source publication or mutated original: %+v err=%v", forked, err)
	}
	projected, err := occurrence.Project(child, "selection:child")
	if err != nil {
		t.Fatal(err)
	}
	forked.TransferredPublication = &projected.Publication
	if _, err := NewTransferredJoinOccurrence(forked, projected.Publication); err != nil {
		t.Fatal(err)
	}
	foreign := projected.Publication
	foreign.AuthorityStamp = "selection:other"
	if _, err := NewTransferredJoinOccurrence(forked, foreign); err == nil {
		t.Fatal("factory replaced the retained publication coordinates")
	}
}

func TestTransferredJoinOccurrenceRejectsForgedCommand(t *testing.T) {
	for _, field := range []string{"run", "entity", "flow", "routing", "task", "schedule", "event", "owner", "reply", "due", "mode", "payload_null", "payload_array", "payload_extra"} {
		t.Run(field, func(t *testing.T) {
			_, occurrence, _ := transferredJoinOccurrenceFixture(t, "orders", timeridentity.TimerHandleJoinComplete, executionmode.Live)
			switch field {
			case "run":
				occurrence.Command.RunID = uuid.NewString()
			case "entity":
				occurrence.Command.EntityID = uuid.NewString()
			case "flow":
				occurrence.Command.FlowInstance = "orders/foreign"
			case "routing":
				occurrence.Command.RoutingSource, _ = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
					FlowID: "orders", FlowInstance: occurrence.Command.FlowInstance, EntityID: uuid.NewString(),
				})
			case "task":
				occurrence.Command.TaskID = "foreign-task"
			case "schedule":
				occurrence.Command.ScheduleKey = "foreign-schedule"
			case "event":
				occurrence.Command.EventType = "platform.join_timeout"
			case "owner":
				occurrence.Command.OwnerID = "foreign-owner"
			case "reply":
				occurrence.Command.ReplyContext = uuid.NewString()
			case "due":
				occurrence.Command.Due = DelayDue(time.Second)
			case "mode":
				occurrence.Command.ExecutionMode = executionmode.Mock
			case "payload_null":
				occurrence.Command.Payload, _ = canonicaljson.Decode([]byte(`null`))
			case "payload_array":
				occurrence.Command.Payload, _ = canonicaljson.Decode([]byte(`[]`))
			case "payload_extra":
				payload := occurrence.Command.Payload.Interface().(map[string]any)
				payload["foreign"] = true
				var err error
				occurrence.Command.Payload, err = canonicaljson.FromGo(payload)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := occurrence.Validate(); err == nil {
				t.Fatal("forged exported occurrence command accepted")
			}
			if _, err := occurrence.Event(); err == nil {
				t.Fatal("forged command emitted an expected event")
			}
			if _, err := occurrence.EvidenceDigest(); err == nil {
				t.Fatal("forged command acquired an evidence digest")
			}
		})
	}
}

func TestTransferredJoinOccurrenceRejectsChangedChildCorrespondence(t *testing.T) {
	for _, field := range []string{"due", "payload_extra", "origin", "cause", "event", "occurrence", "transition", "declaration", "kind", "generation_attempt", "generation_loop"} {
		t.Run(field, func(t *testing.T) {
			_, source, original := transferredJoinOccurrenceFixture(t, "orders", timeridentity.TimerHandleJoinComplete, executionmode.Live)
			child := publishedJoinContinuationChild(t, source.Command)
			handle, ref, _ := timeridentity.ParseJoinHandle(child.Payload.Interface().(map[string]any))
			entry, generation, declaration, kind := ref.StageEntry(), ref.Generation(), ref.Declaration(), handle.Kind()
			switch field {
			case "due":
				child.Due = AbsoluteDue(child.Due.Absolute.Add(time.Second))
			case "origin":
				entry.OriginRunID = source.Command.RunID
			case "cause":
				entry.Cause = "gate"
			case "event":
				entry.EventID = uuid.NewString()
			case "occurrence":
				entry.OccurrenceID = "foreign-occurrence"
			case "transition":
				entry.TransitionID = "foreign-transition"
			case "declaration":
				var err error
				declaration, err = timeridentity.NewJoinRef(ref.Node(), "other.event", ref.Stage(), ref.JoinID())
				if err != nil {
					t.Fatal(err)
				}
			case "kind":
				kind = timeridentity.TimerHandleJoinTimeout
			case "generation_attempt":
				generation.Attempt++
			case "generation_loop":
				generation.LoopID = "foreign-loop"
			}
			ref, err := declaration.BindStageEntry(entry, generation)
			if err != nil {
				t.Fatal(err)
			}
			child = publishedJoinContinuationCommand(t, child, ref, kind)
			if field == "payload_extra" {
				payload := child.Payload.Interface().(map[string]any)
				payload["foreign"] = true
				child.Payload, err = canonicaljson.FromGo(payload)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := child.Validate(); err != nil && field != "payload_extra" {
				t.Fatalf("self-consistent hostile child fixture: %v", err)
			}
			if _, err := source.Project(child, "selection:child"); err == nil {
				t.Fatal("historical projection accepted changed child correspondence")
			}
			if continuation, err := ProjectTransferredJoinContinuation(source, original, child); err == nil || continuation.Present() {
				t.Fatal("event-consuming continuation accepted changed child correspondence")
			}
		})
	}
}

func TestTransferredJoinOccurrenceEvidenceBindsPublicationAndOrdinaryFrame(t *testing.T) {
	_, source, original := transferredJoinOccurrenceFixture(t, "orders", timeridentity.TimerHandleJoinTimeout, executionmode.Mock)
	before, err := source.EvidenceDigest()
	if err != nil {
		t.Fatal(err)
	}
	changed := source
	changed.Publication.AuthorityStamp = "selection:other"
	if after, err := changed.EvidenceDigest(); err != nil || after == before {
		t.Fatal("evidence digest omitted selection authority")
	}
	changed = source
	changed.Publication.SourceEventID = uuid.NewString()
	changed.Publication.EventID = activityidentity.ForkLineageEventID(source.Command.RunID, changed.Publication.SourceEventID)
	if after, err := changed.EvidenceDigest(); err != nil || after == before {
		t.Fatal("evidence digest omitted immediate source publication")
	}
	payload, err := occurrencePublicationPayload(source.Command)
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
		Facts: occurrenceEventFacts(source.Command, source.Publication.EventID, source.Command.Due.Absolute, payload),
		RunID: source.Command.RunID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ordinary.Envelope(), original.Envelope()) || !reflect.DeepEqual(ordinary.Payload(), original.Payload()) {
		t.Fatal("transferred event changed ordinary occurrence envelope or numeric payload projection")
	}
}

func TestTransferredJoinPublicationRejectsHostileCoordinates(t *testing.T) {
	for _, fault := range []string{"empty_record", "empty_event", "wrong_event", "nil_source", "same_run", "source_spelling", "source_event_spelling", "missing_authority", "authority_whitespace", "mode"} {
		t.Run(fault, func(t *testing.T) {
			join, occurrence, _ := transferredJoinOccurrenceFixture(t, "", timeridentity.TimerHandleJoinTimeout, executionmode.Live)
			changed := occurrence.Publication
			switch fault {
			case "empty_record":
				changed = joinruntime.TransferredPublication{}
			case "empty_event":
				changed.EventID = ""
			case "wrong_event":
				changed.EventID = uuid.NewString()
			case "nil_source":
				changed.SourceRunID = uuid.Nil.String()
			case "same_run":
				changed.SourceRunID = occurrence.Command.RunID
			case "source_spelling":
				changed.SourceRunID = " " + changed.SourceRunID
			case "source_event_spelling":
				changed.SourceEventID = " " + changed.SourceEventID
			case "missing_authority":
				changed.AuthorityStamp = ""
			case "authority_whitespace":
				changed.AuthorityStamp = " " + changed.AuthorityStamp
			case "mode":
				changed.ExecutionMode = "invalid"
			}
			if err := changed.Validate(join.JoinRef()); err == nil {
				t.Fatal("hostile publication coordinates accepted")
			}
			join.TransferredPublication = &changed
			if err := join.Validate(); err == nil {
				t.Fatal("join validation ignored hostile retained coordinates")
			}
		})
	}
	_, occurrence, _ := transferredJoinOccurrenceFixture(t, "orders", timeridentity.TimerHandleJoinTimeout, executionmode.Live)
	_, ref, _ := timeridentity.ParseJoinHandle(occurrence.Command.Payload.Interface().(map[string]any))
	if occurrence.Publication.Validate(timeridentity.JoinRef{}) == nil || occurrence.Publication.Validate(ref.Declaration()) == nil {
		t.Fatal("publication coordinates accepted without an exact arrival stage entry")
	}
	entry := ref.StageEntry()
	entry.RunID = uuid.Nil.String()
	ref, err := ref.Declaration().BindStageEntry(entry, ref.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if occurrence.Publication.Validate(ref) == nil {
		t.Fatal("publication accepted a nil current run UUID")
	}
}

func TestTransferredJoinOccurrenceValidateEventRejectsSubstitutions(t *testing.T) {
	for _, fault := range []string{"exact", "prepared_target", "destination", "source_run", "source_event", "authority", "lineage_task", "event_task", "producer", "created", "numeric_payload", "routing_source", "event_id", "class", "mode", "type", "depth"} {
		t.Run(fault, func(t *testing.T) {
			_, occurrence, expected := transferredJoinOccurrenceFixture(t, "", timeridentity.TimerHandleJoinComplete, executionmode.Live)
			facts := publishedOccurrenceTestFacts(expected)
			destination, sourceRun, sourceEvent := occurrence.Command.RunID, occurrence.Publication.SourceRunID, occurrence.Publication.SourceEventID
			stamp, task, mode := occurrence.Publication.AuthorityStamp, occurrence.Command.TaskID, occurrence.Command.ExecutionMode
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
				facts.CreatedAt = facts.CreatedAt.Add(time.Second)
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
				candidate, err = events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: occurrence.Command.RunID})
			}
			if err != nil {
				t.Fatal(err)
			}
			validation := occurrence.ValidateEvent(candidate)
			if fault == "exact" || fault == "prepared_target" {
				if validation != nil {
					t.Fatal(validation)
				}
			} else if validation == nil {
				t.Fatal("substituted transferred publication accepted")
			}
		})
	}
}
