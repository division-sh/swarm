package genericschedule

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func TestForkJoinRequestExpectedPreservesCapturedSchedule(t *testing.T) {
	for _, flow := range []string{".", "orders"} {
		for _, shape := range []struct {
			name      string
			timeout   bool
			cancelled bool
		}{
			{name: "pending_completion"},
			{name: "cancelled_completion", cancelled: true},
			{name: "pending_timeout", timeout: true},
			{name: "cancelled_timeout", timeout: true, cancelled: true},
		} {
			for _, inherited := range []bool{false, true} {
				origin := "local_origin"
				if inherited {
					origin = "inherited_origin"
				}
				t.Run(flow+"/"+shape.name+"/"+origin, func(t *testing.T) {
					request := forkJoinRequestFixture(t, flow, shape.timeout, shape.cancelled, inherited)
					for _, kind := range []forkpoint.Kind{forkpoint.Event, forkpoint.DeploymentRevision, forkpoint.RunStart} {
						t.Run(string(kind), func(t *testing.T) {
							r := request
							r.PointKind = kind
							if kind != forkpoint.Event {
								r.PointEventID = ""
							}
							before := r
							if err := r.Validate(); err != nil {
								t.Fatal(err)
							}
							childID := uuid.NewString()
							actual, err := r.Expected(childID)
							if err != nil {
								t.Fatal(err)
							}
							hash, err := r.Child.ImmutableHash()
							if err != nil {
								t.Fatal(err)
							}
							want := runtimegenericschedule.Activation{
								ID: childID, Command: r.Child, ImmutableHash: hash, AdmittedAt: r.BornAt,
								InitialDueAt: r.Source.InitialDueAt, CurrentDueAt: r.Source.CurrentDueAt, Status: r.Source.Status,
								ForkJoinOrigin: &runtimegenericschedule.ForkJoinOrigin{
									SourceActivationID: r.Source.ID, SourceRunID: r.Source.Command.RunID,
									PointKind: r.PointKind, PointRevision: r.PointRevision, PointEventID: r.PointEventID,
									SourceAdmittedAt: r.Source.AdmittedAt, Owner: runtimegenericschedule.ForkJoinReconstructionOwner,
								},
							}
							if shape.cancelled {
								want.CancelCause, want.CancelledAt = r.Source.CancelCause, r.BornAt
							}
							if err := want.Validate(); err != nil {
								t.Fatal(err)
							}
							if !reflect.DeepEqual(actual, want) || !actual.InitialDueAt.Before(actual.AdmittedAt) {
								t.Fatalf("child changed the overdue absolute due, disposition, or immutable command: got=%+v want=%+v", actual, want)
							}
							if !reflect.DeepEqual(r, before) {
								t.Fatal("validation/projection changed the captured source or admitted child command")
							}
						})
					}
				})
			}
		}
	}
}

func TestForkJoinRequestRejectsCorruption(t *testing.T) {
	for _, flow := range []string{".", "orders"} {
		for _, mutation := range []struct {
			name string
			edit func(*ForkJoinRequest)
		}{
			{"source_identity", func(r *ForkJoinRequest) { r.Source.ID = "not-a-uuid" }},
			{"source_hash", func(r *ForkJoinRequest) { r.Source.ImmutableHash = "different" }},
			{"source_current_due", func(r *ForkJoinRequest) { r.Source.CurrentDueAt = r.Source.CurrentDueAt.Add(time.Second) }},
			{"source_clock", func(r *ForkJoinRequest) { r.Source.ClockSuspension = &runtimegenericschedule.ClockSuspension{} }},
			{"source_parked", func(r *ForkJoinRequest) { r.Source.Status = runtimegenericschedule.StatusParked }},
			{"source_cancellation_without_cause", func(r *ForkJoinRequest) {
				r.Source.Status, r.Source.CancelledAt = runtimegenericschedule.StatusCancelled, r.Source.AdmittedAt
			}},
			{"source_cancellation_after_child_birth", func(r *ForkJoinRequest) {
				r.Source.Status, r.Source.CancelCause, r.Source.CancelledAt = runtimegenericschedule.StatusCancelled, "join_stage_exit", r.BornAt.Add(time.Microsecond)
			}},
			{"child_key", func(r *ForkJoinRequest) { r.Child.ScheduleKey = "different" }},
			{"child_task", func(r *ForkJoinRequest) { r.Child.TaskID = "different" }},
			{"child_owner", func(r *ForkJoinRequest) { r.Child.OwnerID = "different-runtime" }},
			{"child_event_type", func(r *ForkJoinRequest) { r.Child.EventType = "platform.join_timeout" }},
			{"zero_revision", func(r *ForkJoinRequest) { r.PointRevision = 0 }},
			{"negative_revision", func(r *ForkJoinRequest) { r.PointRevision = -1 }},
			{"unknown_cut_kind", func(r *ForkJoinRequest) { r.PointKind = forkpoint.Kind("latest") }},
			{"missing_cut_event", func(r *ForkJoinRequest) { r.PointEventID = "" }},
			{"invalid_cut_event", func(r *ForkJoinRequest) { r.PointEventID = "not-a-uuid" }},
			{"start_cut_with_event", func(r *ForkJoinRequest) { r.PointKind = forkpoint.RunStart }},
			{"revision_cut_with_event", func(r *ForkJoinRequest) { r.PointKind = forkpoint.DeploymentRevision }},
			{"missing_birth", func(r *ForkJoinRequest) { r.BornAt = time.Time{} }},
			{"noncanonical_birth", func(r *ForkJoinRequest) { r.BornAt = r.BornAt.Add(time.Nanosecond) }},
			{"non_utc_birth", func(r *ForkJoinRequest) { r.BornAt = r.BornAt.In(time.FixedZone("offset", 3600)) }},
			{"birth_before_source", func(r *ForkJoinRequest) { r.BornAt = r.Source.AdmittedAt.Add(-time.Microsecond) }},
		} {
			t.Run(flow+"/"+mutation.name, func(t *testing.T) {
				r := forkJoinRequestFixture(t, flow, false, false, false)
				mutation.edit(&r)
				forkJoinRequireRequestRefusal(t, r)
			})
		}
	}
}

func TestForkJoinRequestRejectsSelfConsistentForeignEvidence(t *testing.T) {
	commandRefusals := map[string]bool{
		"source_run_context_independent": true,
		"source_run_not_canonical":       true,
		"source_entity_owner":            true,
		"source_entry_run":               true,
		"source_entry_entity":            true,
		"child_source_run":               true,
		"child_run":                      true,
		"child_entity_owner":             true,
		"child_entry_run":                true,
		"child_entry_entity":             true,
	}
	for _, flow := range []string{".", "orders"} {
		for _, mutation := range []struct {
			name string
			edit func(*testing.T, *ForkJoinRequest)
		}{
			{"source_run_context_independent", func(t *testing.T, r *ForkJoinRequest) { r.Source.Command.RunID = uuid.NewString() }},
			{"source_run_not_canonical", func(t *testing.T, r *ForkJoinRequest) {
				r.Source.Command.RunID = "A0000000-0000-4000-8000-000000000001"
			}},
			{"source_entity_owner", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinChangeEntityOwner(t, &r.Source.Command)
			}},
			{"source_entry_run", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinEditEntry(t, &r.Source.Command, func(e *timeridentity.StageEntryRef) { e.RunID = uuid.NewString() })
			}},
			{"source_entry_entity", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinEditEntry(t, &r.Source.Command, func(e *timeridentity.StageEntryRef) { e.EntityID = uuid.NewString() })
			}},
			{"child_source_run", func(t *testing.T, r *ForkJoinRequest) { r.Child.RunID = r.Source.Command.RunID }},
			{"child_run", func(t *testing.T, r *ForkJoinRequest) { r.Child.RunID = uuid.NewString() }},
			{"child_entity_owner", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinChangeEntityOwner(t, &r.Child)
			}},
			{"child_entry_run", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinEditEntry(t, &r.Child, func(e *timeridentity.StageEntryRef) { e.RunID = uuid.NewString() })
			}},
			{"child_entry_entity", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinEditEntry(t, &r.Child, func(e *timeridentity.StageEntryRef) { e.EntityID = uuid.NewString() })
			}},
			{"child_origin_missing", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinEditEntry(t, &r.Child, func(e *timeridentity.StageEntryRef) { e.OriginRunID = "" })
			}},
			{"child_origin_foreign", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinEditEntry(t, &r.Child, func(e *timeridentity.StageEntryRef) { e.OriginRunID = uuid.NewString() })
			}},
			{"child_origin_reset_to_parent", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinEditEntry(t, &r.Child, func(e *timeridentity.StageEntryRef) { e.OriginRunID = r.Source.Command.RunID })
			}},
			{"child_mode", func(t *testing.T, r *ForkJoinRequest) { r.Child.ExecutionMode = executionmode.Mock }},
			{"child_due", func(t *testing.T, r *ForkJoinRequest) {
				r.Child.Due = runtimegenericschedule.AbsoluteDue(r.BornAt.Add(time.Hour))
			}},
			{"child_delay_basis", func(t *testing.T, r *ForkJoinRequest) { r.Child.Due = runtimegenericschedule.DelayDue(time.Hour) }},
			{"child_reply", func(t *testing.T, r *ForkJoinRequest) { r.Child.ReplyContext = uuid.NewString() }},
			{"source_reply", func(t *testing.T, r *ForkJoinRequest) { r.Source.Command.ReplyContext = uuid.NewString() }},
			{"child_handle_kind", func(t *testing.T, r *ForkJoinRequest) {
				_, ref := forkJoinCommandHandle(t, r.Child)
				handle, err := timeridentity.JoinTimeoutHandle(ref)
				if err != nil {
					t.Fatal(err)
				}
				forkJoinSetHandle(t, &r.Child, handle)
			}},
			{"child_join_declaration", func(t *testing.T, r *ForkJoinRequest) {
				_, ref := forkJoinCommandHandle(t, r.Child)
				declaration, err := timeridentity.NewJoinRef(ref.Node(), ref.HandlerEvent(), ref.Stage(), "different-join")
				if err != nil {
					t.Fatal(err)
				}
				forkJoinBindCommandRef(t, &r.Child, declaration, ref.StageEntry(), ref.Generation())
			}},
			{"child_handler_declaration", func(t *testing.T, r *ForkJoinRequest) {
				_, ref := forkJoinCommandHandle(t, r.Child)
				declaration, err := timeridentity.NewJoinRef(ref.Node(), "other.completed", ref.Stage(), ref.JoinID())
				if err != nil {
					t.Fatal(err)
				}
				forkJoinBindCommandRef(t, &r.Child, declaration, ref.StageEntry(), ref.Generation())
			}},
			{"child_stage_declaration", func(t *testing.T, r *ForkJoinRequest) {
				_, ref := forkJoinCommandHandle(t, r.Child)
				entry := ref.StageEntry()
				entry.Stage = "other-stage"
				declaration, err := timeridentity.NewJoinRef(ref.Node(), ref.HandlerEvent(), entry.Stage, ref.JoinID())
				if err != nil {
					t.Fatal(err)
				}
				forkJoinBindCommandRef(t, &r.Child, declaration, entry, ref.Generation())
			}},
			{"child_node_declaration", func(t *testing.T, r *ForkJoinRequest) {
				_, ref := forkJoinCommandHandle(t, r.Child)
				node := identitytest.RootNode(t, "other-join-node")
				if flow != "." {
					node = identitytest.FlowNode(t, flow, "other-join-node")
				}
				declaration, err := timeridentity.NewJoinRef(node, ref.HandlerEvent(), ref.Stage(), ref.JoinID())
				if err != nil {
					t.Fatal(err)
				}
				forkJoinBindCommandRef(t, &r.Child, declaration, ref.StageEntry(), ref.Generation())
			}},
			{"child_entry_cause", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinEditEntry(t, &r.Child, func(e *timeridentity.StageEntryRef) {
					e.Cause, e.EventID, e.OccurrenceID, e.TransitionID = "delivery", uuid.NewString(), "accepted-delivery", "selected-transition"
				})
			}},
			{"child_entry_event", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinDeliveryEntries(t, r)
				forkJoinEditEntry(t, &r.Child, func(e *timeridentity.StageEntryRef) { e.EventID = uuid.NewString() })
			}},
			{"child_entry_occurrence", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinDeliveryEntries(t, r)
				forkJoinEditEntry(t, &r.Child, func(e *timeridentity.StageEntryRef) { e.OccurrenceID = "another-delivery" })
			}},
			{"child_entry_transition", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinDeliveryEntries(t, r)
				forkJoinEditEntry(t, &r.Child, func(e *timeridentity.StageEntryRef) { e.TransitionID = "another-transition" })
			}},
			{"prepared_source", func(t *testing.T, r *ForkJoinRequest) { forkJoinStampPreparedSource(r) }},
			{"cancelled_prepared_source", func(t *testing.T, r *ForkJoinRequest) {
				r.Source.Status, r.Source.CancelCause, r.Source.CancelledAt = runtimegenericschedule.StatusCancelled, "join_stage_exit", r.Source.AdmittedAt.Add(time.Minute)
				forkJoinStampPreparedSource(r)
			}},
			{"fired_source", func(t *testing.T, r *ForkJoinRequest) {
				forkJoinStampPreparedSource(r)
				r.Source.Status = runtimegenericschedule.StatusFired
				r.Source.FiredAt, r.Source.AcceptedAt = r.Source.CurrentEventAdmittedAt, r.Source.CurrentEventAdmittedAt
			}},
			{"failed_source", func(t *testing.T, r *ForkJoinRequest) {
				r.Source.Status, r.Source.FailedAt = runtimegenericschedule.StatusFailed, r.Source.AdmittedAt.Add(time.Minute)
				r.Source.Failure = runtimegenericschedule.Failure{Code: "failed", Message: "retained failure"}
			}},
		} {
			t.Run(flow+"/"+mutation.name, func(t *testing.T) {
				r := forkJoinRequestFixture(t, flow, false, false, true)
				mutation.edit(t, &r)
				if commandRefusals[mutation.name] {
					if r.Source.Command.Validate() == nil && r.Child.Validate() == nil {
						t.Fatal("canonical command admission accepted mismatched arrival ownership")
					}
					forkJoinRequireRequestRefusal(t, r)
					return
				}
				forkJoinHashSource(t, &r.Source)
				// A coherent command/hash is not evidence of source/child correspondence.
				if err := r.Source.Validate(); err != nil {
					t.Fatalf("hostile source must remain self-consistent: %v", err)
				}
				if err := r.Child.Validate(); err != nil {
					t.Fatalf("hostile child must remain self-consistent: %v", err)
				}
				forkJoinRequireRequestRefusal(t, r)
			})
		}
	}
}

func TestForkJoinRequestExpectedRejectsInvalidChildID(t *testing.T) {
	r := forkJoinRequestFixture(t, ".", false, false, false)
	for _, childID := range []string{"", "not-a-uuid"} {
		if _, err := r.Expected(childID); err == nil {
			t.Fatalf("invalid child activation ID %q admitted", childID)
		}
	}
}

func TestForkJoinRequestExpectedPreservesMockMode(t *testing.T) {
	for _, flow := range []string{".", "orders"} {
		for _, timeout := range []bool{false, true} {
			r := forkJoinRequestFixture(t, flow, timeout, false, false)
			r.Source.Command.ExecutionMode, r.Child.ExecutionMode = executionmode.Mock, executionmode.Mock
			forkJoinHashSource(t, &r.Source)
			before := r.Source
			child, err := r.Expected(uuid.NewString())
			if err != nil || child.Command.ExecutionMode != executionmode.Mock || !reflect.DeepEqual(before, r.Source) {
				t.Fatalf("captured mock mode changed or required live execution: child=%+v err=%v", child, err)
			}
		}
	}
}

func TestForkJoinRequestExpectedBindsOriginInEvidenceDigest(t *testing.T) {
	r := forkJoinRequestFixture(t, ".", false, false, false)
	childID := uuid.NewString()
	child, err := r.Expected(childID)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := child.EvidenceDigest()
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		name string
		edit func(*runtimegenericschedule.ForkJoinOrigin)
	}{
		{"source_activation", func(o *runtimegenericschedule.ForkJoinOrigin) { o.SourceActivationID = uuid.NewString() }},
		{"source_run", func(o *runtimegenericschedule.ForkJoinOrigin) { o.SourceRunID = uuid.NewString() }},
		{"point_kind", func(o *runtimegenericschedule.ForkJoinOrigin) { o.PointKind, o.PointEventID = forkpoint.RunStart, "" }},
		{"point_revision", func(o *runtimegenericschedule.ForkJoinOrigin) { o.PointRevision++ }},
		{"point_event", func(o *runtimegenericschedule.ForkJoinOrigin) { o.PointEventID = uuid.NewString() }},
		{"source_admission", func(o *runtimegenericschedule.ForkJoinOrigin) {
			o.SourceAdmittedAt = o.SourceAdmittedAt.Add(-time.Microsecond)
		}},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			changed := child.Canonical()
			mutation.edit(changed.ForkJoinOrigin)
			actual, err := changed.EvidenceDigest()
			if err != nil || actual == digest || changed.ImmutableHash != child.ImmutableHash {
				t.Fatalf("origin escaped exact readback despite unchanged command hash: digest=%q err=%v", actual, err)
			}
		})
	}
	ordinary := child
	ordinary.ForkJoinOrigin = nil
	ordinaryDigest, err := ordinary.EvidenceDigest()
	if err != nil || ordinaryDigest == digest {
		t.Fatalf("an ordinary same-command row is not inherited history: digest=%q err=%v", ordinaryDigest, err)
	}
	invalid := child.Canonical()
	invalid.ForkJoinOrigin.Owner = "unrelated-owner"
	if _, err := invalid.EvidenceDigest(); err == nil {
		t.Fatal("invalid lineage owner produced an admissible evidence digest")
	}
	if !reflect.DeepEqual(child.ForkJoinOrigin, &runtimegenericschedule.ForkJoinOrigin{
		SourceActivationID: r.Source.ID, SourceRunID: r.Source.Command.RunID,
		PointKind: r.PointKind, PointRevision: r.PointRevision, PointEventID: r.PointEventID,
		SourceAdmittedAt: r.Source.AdmittedAt, Owner: runtimegenericschedule.ForkJoinReconstructionOwner,
	}) {
		t.Fatal("canonicalization/foreign-origin checks changed retained child provenance")
	}
}

func TestForkJoinRequestExpectedRetainsEarliestOriginAndImmediateSource(t *testing.T) {
	firstRequest := forkJoinRequestFixture(t, ".", false, true, false)
	first, err := firstRequest.Expected(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	before := first.Canonical()
	nextRun := uuid.NewString()
	_, ref := forkJoinCommandHandle(t, first.Command)
	entry := ref.StageEntry()
	projection, err := runfork.ProjectEntityOwnership(first.Command.RunID, nextRun, entry.EntityID, entry.InstancePath)
	if err != nil {
		t.Fatal(err)
	}
	route, err := runfork.ProjectExecutionRoute(first.Command.RunID, nextRun, entry.FlowScope,
		flowidentity.StoredRoute(entry.FlowScope, entry.InstanceID, entry.InstancePath))
	if err != nil {
		t.Fatal(err)
	}
	entry.RunID, entry.EntityID, entry.InstanceID, entry.InstancePath = nextRun, projection.Fork.EntityID, route.InstanceID, route.InstancePath
	child := first.Command
	child.RunID, child.EntityID = nextRun, entry.EntityID
	child.RoutingSource, err = runfork.ProjectProducerOwnership(first.Command.RunID, nextRun, first.Command.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	forkJoinBindCommandRef(t, &child, ref.Declaration(), entry, ref.Generation())
	r := ForkJoinRequest{
		Source: first, Child: child, PointKind: forkpoint.DeploymentRevision, PointRevision: 11, BornAt: first.AdmittedAt.Add(time.Hour),
	}
	second, err := r.Expected(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	_, secondRef := forkJoinCommandHandle(t, second.Command)
	if second.ForkJoinOrigin == nil || second.ForkJoinOrigin.SourceActivationID != first.ID || second.ForkJoinOrigin.SourceRunID != first.Command.RunID ||
		!second.ForkJoinOrigin.SourceAdmittedAt.Equal(first.AdmittedAt) || second.ForkJoinOrigin.PointKind != r.PointKind || second.ForkJoinOrigin.PointRevision != r.PointRevision || second.ForkJoinOrigin.PointEventID != "" ||
		secondRef.StageEntry().OriginRunID != firstRequest.Source.Command.RunID || second.CancelCause != first.CancelCause || !second.CancelledAt.Equal(r.BornAt) ||
		!second.InitialDueAt.Equal(first.InitialDueAt) || !reflect.DeepEqual(first, before) {
		t.Fatalf("second fork overwrote earliest origin, retained immediate source, or canceled due: child=%+v", second)
	}
}

func forkJoinRequestFixture(t *testing.T, flow string, timeout, cancelled, inherited bool) ForkJoinRequest {
	t.Helper()
	arm := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	sourceRun, childRun := uuid.NewString(), uuid.NewString()
	entityID := sourceRun
	route := flowidentity.StoredRoute(".", sourceRun, sourceRun)
	node := identitytest.RootNode(t, "join-node")
	if flow != "." {
		entityID = uuid.NewString()
		route = flowidentity.StoredRoute(flow, "order-1", flow+"/order-1")
		node = identitytest.FlowNode(t, flow, "join-node")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(sourceRun, route)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := workflowlifecycle.NewInitialEntry(route, identity.NormalizeEntityID(entityID), "awaiting", executionmode.Live, arm)
	if err != nil {
		t.Fatal(err)
	}
	entry, enters, err := effect.StageEntry(owner)
	if err != nil || !enters {
		t.Fatalf("canonical construction entry: enters=%t err=%v", enters, err)
	}
	if inherited {
		entry.OriginRunID = uuid.NewString()
	}
	ref, err := timeridentity.NewJoinRef(node, "item.completed", "awaiting", "shared")
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	members := []string{}
	fireAt := time.Time{}
	if timeout {
		members, fireAt = []string{"member-a"}, arm.Add(time.Hour)
	}
	join, err := joinruntime.NewActivation(ref, members, nil, arm, fireAt)
	if err != nil {
		t.Fatal(err)
	}
	if !timeout {
		join.Close(joinruntime.CloseReasonComplete, true, false)
		join, err = join.WithTimerHandle(join.TimerHandle(), arm)
		if err != nil {
			t.Fatal(err)
		}
	}
	command, err := runtimegenericschedule.WorkflowJoinAdmission(join, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	source := runtimegenericschedule.Activation{
		ID: uuid.NewString(), Command: command, AdmittedAt: arm,
		InitialDueAt: command.Due.Absolute, CurrentDueAt: command.Due.Absolute, Status: runtimegenericschedule.StatusActive,
	}
	if cancelled {
		source.Status, source.CancelCause, source.CancelledAt = runtimegenericschedule.StatusCancelled, "join_stage_exit", arm.Add(time.Minute)
		if timeout {
			source.CancelCause = "join_closed"
		}
	}
	forkJoinHashSource(t, &source)
	projection, err := runfork.ProjectEntityOwnership(sourceRun, childRun, entityID, route.InstancePath)
	if err != nil {
		t.Fatal(err)
	}
	childRoute, err := runfork.ProjectExecutionRoute(sourceRun, childRun, flow, route)
	if err != nil {
		t.Fatal(err)
	}
	childEntry := entry
	childEntry.RunID, childEntry.EntityID = childRun, projection.Fork.EntityID
	childEntry.FlowScope, childEntry.InstanceID, childEntry.InstancePath = childRoute.ScopeKey, childRoute.InstanceID, childRoute.InstancePath
	if childEntry.OriginRunID == "" {
		childEntry.OriginRunID = sourceRun
	}
	childRef, err := ref.Declaration().BindStageEntry(childEntry, ref.Generation())
	if err != nil {
		t.Fatal(err)
	}
	childJoin, err := join.WithForkReference(childRef)
	if err != nil {
		t.Fatal(err)
	}
	child, err := runtimegenericschedule.WorkflowJoinAdmission(childJoin, source.Command.ExecutionMode)
	if err != nil {
		t.Fatal(err)
	}
	r := ForkJoinRequest{Source: source, Child: child, PointKind: forkpoint.Event, PointRevision: 7, PointEventID: uuid.NewString(), BornAt: arm.Add(2 * time.Hour)}
	if err := r.Validate(); err != nil {
		t.Fatalf("canonical request fixture: %v", err)
	}
	return r
}

func forkJoinRequireRequestRefusal(t *testing.T, r ForkJoinRequest) {
	t.Helper()
	before := r
	if err := r.Validate(); err == nil {
		t.Fatal("corrupt fork join request admitted")
	}
	child, err := r.Expected(uuid.NewString())
	if err == nil || !reflect.DeepEqual(child, runtimegenericschedule.Activation{}) {
		t.Fatalf("invalid request produced a child projection: child=%+v err=%v", child, err)
	}
	if !reflect.DeepEqual(r, before) {
		t.Fatal("refusal changed source or child evidence")
	}
}

func forkJoinHashSource(t *testing.T, source *runtimegenericschedule.Activation) {
	t.Helper()
	hash, err := source.Command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	source.ImmutableHash = hash
}

func forkJoinCommandHandle(t *testing.T, command runtimegenericschedule.AdmissionCommand) (timeridentity.TimerHandle, timeridentity.JoinRef) {
	t.Helper()
	handle, ref, found := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
	if !found {
		t.Fatal("fixture lost its canonical arrival handle")
	}
	return handle, ref
}

func forkJoinSetHandle(t *testing.T, command *runtimegenericschedule.AdmissionCommand, handle timeridentity.TimerHandle) {
	t.Helper()
	payload, err := canonicaljson.FromGo(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	command.ScheduleKey, command.TaskID, command.EventType, command.Payload = handle.TaskID(), handle.TaskID(), handle.EventType(), payload
}

func forkJoinBindCommandRef(t *testing.T, command *runtimegenericschedule.AdmissionCommand, declaration timeridentity.JoinRef, entry timeridentity.StageEntryRef, generation attemptgeneration.Generation) {
	t.Helper()
	handle, _ := forkJoinCommandHandle(t, *command)
	ref, err := declaration.BindStageEntry(entry, generation)
	if err != nil {
		t.Fatal(err)
	}
	if handle.Kind() == timeridentity.TimerHandleJoinTimeout {
		handle, err = timeridentity.JoinTimeoutHandle(ref)
	} else {
		handle, err = timeridentity.JoinCompleteHandle(ref)
	}
	if err != nil {
		t.Fatal(err)
	}
	forkJoinSetHandle(t, command, handle)
}

func forkJoinEditEntry(t *testing.T, command *runtimegenericschedule.AdmissionCommand, edit func(*timeridentity.StageEntryRef)) {
	t.Helper()
	_, ref := forkJoinCommandHandle(t, *command)
	entry := ref.StageEntry()
	edit(&entry)
	forkJoinBindCommandRef(t, command, ref.Declaration(), entry, ref.Generation())
}

func forkJoinStampPreparedSource(r *ForkJoinRequest) {
	r.Source.CurrentEventID = runtimegenericschedule.OccurrenceEventID(r.Source.ID, r.Source.CurrentDueAt)
	r.Source.CurrentEventAdmittedAt = r.Source.AdmittedAt.Add(time.Minute)
}

func forkJoinChangeEntityOwner(t *testing.T, command *runtimegenericschedule.AdmissionCommand) {
	t.Helper()
	_, ref := forkJoinCommandHandle(t, *command)
	command.EntityID = uuid.NewString()
	var err error
	if ref.FlowPath() == "." {
		command.RoutingSource, err = events.NewRootRoutingSource(command.EntityID)
	} else {
		command.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
			FlowID: ref.FlowPath(), FlowInstance: command.FlowInstance, EntityID: command.EntityID,
		})
	}
	if err != nil {
		t.Fatal(err)
	}
}

func forkJoinDeliveryEntries(t *testing.T, r *ForkJoinRequest) {
	t.Helper()
	eventID := uuid.NewString()
	for _, command := range []*runtimegenericschedule.AdmissionCommand{&r.Source.Command, &r.Child} {
		forkJoinEditEntry(t, command, func(e *timeridentity.StageEntryRef) {
			e.Cause, e.EventID, e.OccurrenceID, e.TransitionID = "delivery", eventID, "accepted-delivery", "selected-transition"
		})
	}
	forkJoinHashSource(t, &r.Source)
	if err := r.Validate(); err != nil {
		t.Fatalf("canonical retained delivery entries: %v", err)
	}
}
