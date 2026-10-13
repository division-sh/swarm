package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func runForkReplyPlanFixture(t *testing.T, state replycontext.State, root, fieldless bool) (runfork.RunForkPlan, string, replycontext.Record) {
	t.Helper()
	sourceRun, childRun, request := uuid.NewString(), uuid.NewString(), uuid.NewString()
	born := time.Unix(100, 0).UTC()
	owner := flowidentity.Instance{
		TemplateID: "review/final", ScopeKey: "review/final", InstanceID: "final", InstancePath: "review/key/final",
		EntityID: flowidentity.EntityID("review/key/final"), HasStoredPath: true,
		ParentEntityID: sourceRun, ParentRoute: flowidentity.ParentRoute{FlowID: ".", EntityID: sourceRun, FlowInstance: sourceRun},
	}
	if root {
		owner = flowidentity.Stored(nil, ".", sourceRun, sourceRun, sourceRun, "")
	}
	initial := pipeline.WorkflowInstance{
		WorkflowVersion: "v1", Mode: "static", Status: "active", EntityType: "record",
		CurrentState: "initial", StageDefined: true, EnteredStageAt: born, CreatedAt: born, Fields: map[string]any{"value": int64(7)},
	}
	if fieldless {
		initial.EntityType, initial.Fields = "", nil
	}
	constructed := selectedWorkflowConstructionRecordFixture(t, sourceRun, "bundle-v2:sha256:"+strings.Repeat("a", 64), owner, initial, executionmode.Live)
	entry := timeridentity.StageEntryRef{
		RunID: sourceRun, FlowScope: owner.ScopeKey, InstanceID: owner.InstanceID, InstancePath: owner.InstancePath, EntityID: owner.EntityID,
		Stage: "awaiting", Cause: "delivery", EventID: request, OccurrenceID: "original-return-arm", TransitionID: "original-transition",
	}
	node, err := identity.AdmitExecutableNodeDeclaration(owner.ScopeKey, "collector")
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := timeridentity.NewJoinRef(node, "result", entry.Stage, "reply-return")
	if err != nil {
		t.Fatal(err)
	}
	bound, err := declaration.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	early, err := timeridentity.NewJoinRef(node, "later", "later", "early-return")
	if err != nil {
		t.Fatal(err)
	}
	newer := entry
	newer.Stage, newer.EventID, newer.OccurrenceID, newer.TransitionID = "later", uuid.NewString(), "newer-arm", "newer-transition"
	bookkeeping := map[string]any{}
	if err := workflowlifecycle.StoreStageEntry(bookkeeping, newer); err != nil {
		t.Fatal(err)
	}
	record := replycontext.Record{
		RunID: sourceRun, RequestEventID: request, RequesterFlowID: owner.ScopeKey, RequestOutputPin: "request", ReplyInputPin: "return",
		SourceRunID: sourceRun, SourceRequestEventID: request,
		ProviderFlowID: "worker", ProviderInputPin: "request", ProviderOutputPin: "return",
		Origin:               events.RouteIdentity{FlowID: owner.ScopeKey, EntityID: owner.EntityID, FlowInstance: owner.InstancePath},
		ReturnJoins:          []events.JoinAdmissionReceipt{{Ref: bound, Disposition: events.JoinAdmissionBound}, {Ref: early, Disposition: events.JoinAdmissionEarly}},
		RequestCorrelationID: "item-42", CorrelationKey: "item.id", State: state, CreatedAt: born, UpdatedAt: born.Add(time.Second),
	}
	record.ID = replycontext.DeterministicID(request, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, record.Origin)
	history := []string{request}
	if state == replycontext.StateTerminal {
		record.AcceptedReplyEventID = uuid.NewString()
		terminalAt := born.Add(time.Second)
		record.TerminalAt = &terminalAt
		history = append(history, record.AcceptedReplyEventID)
	}
	record = record.Normalized()
	plan := (runfork.RunForkPlan{
		SourceRunID: sourceRun, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: request, Revision: 1},
		Entities: []runfork.RunForkEntityState{{EntityID: owner.EntityID, CurrentState: "later", Bookkeeping: bookkeeping, Fields: initial.Fields,
			MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
				Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
				FlowInstance: owner.InstancePath, FlowTemplate: owner.TemplateID, EntityType: initial.EntityType, FlowConfig: constructed.Config,
				InitialMaterialization: constructed.InitialMaterialization, CreatedAt: born, UpdatedAt: born,
			}}},
		ReplyContexts: []replycontext.Record{record},
	}).WithHistoricalEvents(1, history)
	return plan, childRun, record
}

func runForkReplyCapturedChildPlan(t *testing.T, source runfork.RunForkPlan, childRun string) runfork.RunForkPlan {
	t.Helper()
	records, err := projectRunForkReplyContexts(source, childRun)
	if err != nil {
		t.Fatal(err)
	}
	entities := make([]runfork.RunForkEntityState, 0, len(source.Entities))
	for _, entity := range source.Entities {
		constructor, err := ProjectRunForkConstructionReceipt(source, childRun, entity, time.Unix(200, 0).UTC())
		if err != nil {
			t.Fatal(err)
		}
		if constructor.CreationEvent != nil {
			t.Fatal("context-only model fixture cannot carry a construction event")
		}
		raw, err := pipeline.EncodeFlowConstructionReceipt(constructor)
		if err != nil {
			t.Fatal(err)
		}
		metadata := *entity.MaterializationMetadata
		projection, err := runfork.ProjectEntityOwnership(source.SourceRunID, childRun, entity.EntityID, metadata.FlowInstance)
		if err != nil {
			t.Fatal(err)
		}
		bookkeeping, accumulator, _, err := projectRunForkEntityExecutionState(entity, source.SourceRunID, childRun, projection)
		if err != nil {
			t.Fatal(err)
		}
		entity.EntityID, entity.Bookkeeping, entity.Accumulator = projection.Fork.EntityID, bookkeeping, accumulator
		metadata.FlowInstance, metadata.FlowTemplate = constructor.Identity.InstancePath, constructor.Identity.TemplateID
		metadata.InitialMaterialization = raw
		entity.MaterializationMetadata = &metadata
		entities = append(entities, entity)
	}
	// Model-only B-cut capture: constructor and reply rows, without copied events.
	return (runfork.RunForkPlan{
		SourceRunID: childRun, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 2},
		Entities: entities, ReplyContexts: records,
	}).WithHistoricalEvents(2, nil)
}

func runForkReplyEnclosingPlanFixture(t *testing.T, state replycontext.State) (runfork.RunForkPlan, string, replycontext.Record, replycontext.Record) {
	t.Helper()
	plan, childRun, source := runForkReplyPlanFixture(t, state, false, true)
	enclosing := source
	enclosing.ID, enclosing.RequestEventID = "enclosing:"+source.ID, uuid.NewString()
	enclosing.SourceRequestEventID = enclosing.RequestEventID
	enclosing.ProviderFlowID, enclosing.RequestCorrelationID = source.RequesterFlowID, "outer-item-42"
	enclosing.ReturnJoins = nil
	enclosing.State, enclosing.AcceptedReplyEventID, enclosing.TerminalAt = replycontext.StateOpen, "", nil
	if err := source.BindEnclosing(enclosing); err != nil {
		t.Fatal(err)
	}
	// Deliberately child-first to exercise the materializer's row dependency order.
	plan.ReplyContexts = []replycontext.Record{source, enclosing}
	return plan, childRun, source, enclosing
}

func TestRunForkReplyProjectionPreservesOpenTerminalAndOldReturn(t *testing.T) {
	for _, state := range []replycontext.State{replycontext.StateOpen, replycontext.StateTerminal} {
		for _, root := range []bool{false, true} {
			for _, fieldless := range []bool{false, true} {
				t.Run(string(state)+map[bool]string{false: "/keyed", true: "/root"}[root]+map[bool]string{false: "/fields", true: "/fieldless"}[fieldless], func(t *testing.T) {
					plan, childRun, source := runForkReplyPlanFixture(t, state, root, fieldless)
					before := source
					child, err := projectRunForkReplyContext(plan, childRun, source)
					if err != nil {
						t.Fatal(err)
					}
					if child.ID != deterministicRunForkReplyContextID(childRun, source.ID) || child.RunID != childRun ||
						child.RequestEventID != deterministicRunForkReplayEventID(childRun, source.RequestEventID) || child.State != source.State ||
						child.SourceRunID != source.SourceRunID || child.SourceRequestEventID != source.SourceRequestEventID ||
						child.EnclosingReplyContextID != source.EnclosingReplyContextID ||
						child.RequestCorrelationID != source.RequestCorrelationID || child.CorrelationKey != source.CorrelationKey ||
						child.RequesterFlowID != source.RequesterFlowID || child.ProviderFlowID != source.ProviderFlowID ||
						child.RequestOutputPin != source.RequestOutputPin || child.ReplyInputPin != source.ReplyInputPin ||
						child.ProviderInputPin != source.ProviderInputPin || child.ProviderOutputPin != source.ProviderOutputPin ||
						!child.CreatedAt.Equal(source.CreatedAt) || !child.UpdatedAt.Equal(source.UpdatedAt) {
						t.Fatalf("reply facts lost: source=%+v child=%+v", source, child)
					}
					if state == replycontext.StateTerminal && (child.AcceptedReplyEventID != deterministicRunForkReplayEventID(childRun, source.AcceptedReplyEventID) ||
						child.TerminalAt == source.TerminalAt || !child.TerminalAt.Equal(*source.TerminalAt)) {
						t.Fatal("accepted reply was reopened, lost or aliased")
					}
					if state == replycontext.StateOpen && (child.AcceptedReplyEventID != "" || child.TerminalAt != nil) {
						t.Fatal("open reply acquired a terminal outcome")
					}
					for i, receipt := range source.ReturnJoins {
						projected := child.ReturnJoins[i]
						if receipt.Disposition == events.JoinAdmissionEarly {
							if !reflect.DeepEqual(receipt, projected) {
								t.Fatal("early return acquired an arm")
							}
							continue
						}
						entry := projected.Ref.StageEntry()
						if entry.RunID != childRun || entry.OriginRunID != plan.SourceRunID || entry.OccurrenceID != "original-return-arm" ||
							entry.Stage != "awaiting" || !receipt.Ref.Declaration().Equal(projected.Ref.Declaration()) || entry.EntityID != child.Origin.EntityID || entry.InstancePath != child.Origin.FlowInstance {
							t.Fatal("reply return retargeted a newer arm or changed its exact owner")
						}
					}
					carriage, err := projectRunForkConstructionContext(plan, childRun, events.DeliveryContext{Reply: &events.ReplyContextRef{ID: source.ID}, Joins: source.ReturnJoins})
					if err != nil || carriage.Reply.ID != child.ID || !reflect.DeepEqual(carriage.Joins, child.ReturnJoins) || !reflect.DeepEqual(source, before) {
						t.Fatalf("construction carriage diverged or source changed: %v", err)
					}
				})
			}
		}
	}
}

func TestRunForkReplyProjectionPreservesUnkeyedCorrelation(t *testing.T) {
	plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
	source.CorrelationKey, source.RequestCorrelationID = "", source.RequestEventID
	plan.ReplyContexts[0] = source
	child, err := projectRunForkReplyContext(plan, childRun, source)
	if err != nil || child.RequestCorrelationID != source.RequestEventID || child.RequestCorrelationID == child.RequestEventID {
		t.Fatalf("original correlation regenerated: %+v err=%v", child, err)
	}
}

func TestRunForkReplyProjectionRejectsUncapturedAndCorruptBindings(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*runfork.RunForkPlan, *replycontext.Record)
	}{
		{"missing_state", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.State = "" }},
		{"invalid_request", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.RequestEventID = "not-an-event" }},
		{"missing_request", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.RequestEventID = "" }},
		{"zero_request", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.RequestEventID = uuid.Nil.String() }},
		{"noncanonical_request", func(_ *runfork.RunForkPlan, r *replycontext.Record) {
			r.RequestEventID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
		}},
		{"invalid_accepted", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.AcceptedReplyEventID = "not-an-event" }},
		{"missing_accepted", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.AcceptedReplyEventID = "" }},
		{"zero_accepted", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.AcceptedReplyEventID = uuid.Nil.String() }},
		{"noncanonical_accepted", func(_ *runfork.RunForkPlan, r *replycontext.Record) {
			r.AcceptedReplyEventID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
		}},
		{"wrong_run", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.RunID = uuid.NewString() }},
		{"wrong_origin", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.Origin.EntityID = uuid.NewString() }},
		{"wrong_scope", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.Origin.FlowID = "sibling" }},
		{"uncaptured", func(p *runfork.RunForkPlan, _ *replycontext.Record) { p.ReplyContexts = nil }},
		{"duplicate", func(p *runfork.RunForkPlan, r *replycontext.Record) { p.ReplyContexts = append(p.ReplyContexts, *r) }},
		{"unknown_history", func(p *runfork.RunForkPlan, _ *replycontext.Record) { p.ForkPoint.Revision++ }},
		{"missing_history", func(p *runfork.RunForkPlan, _ *replycontext.Record) { *p = p.WithHistoricalEvents(0, nil) }},
		{"invalid_fork_point", func(p *runfork.RunForkPlan, _ *replycontext.Record) { p.ForkPoint.Kind = "invalid" }},
		{"missing_constructor", func(p *runfork.RunForkPlan, _ *replycontext.Record) {
			p.Entities[0].MaterializationMetadata.InitialMaterialization = nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateTerminal, false, false)
			test.mutate(&plan, &source)
			if len(plan.ReplyContexts) == 1 {
				plan.ReplyContexts[0] = source
			}
			if _, err := projectRunForkReplyContext(plan, childRun, source); err == nil {
				t.Fatal("uncaptured/corrupt reply authority admitted")
			}
		})
	}
}

func TestRunForkReplyProjectionRequiresCompleteCapturedRecord(t *testing.T) {
	for _, test := range []struct {
		name  string
		alter func(replycontext.Record) replycontext.Record
	}{
		{"logical_request", func(r replycontext.Record) replycontext.Record { r.RequestEventID = uuid.NewString(); return r }},
		{"original_run", func(r replycontext.Record) replycontext.Record { r.SourceRunID = uuid.NewString(); return r }},
		{"original_request", func(r replycontext.Record) replycontext.Record { r.SourceRequestEventID = uuid.NewString(); return r }},
		{"enclosing", func(r replycontext.Record) replycontext.Record {
			r.EnclosingReplyContextID = uuid.NewString()
			return r
		}},
		{"correlation", func(r replycontext.Record) replycontext.Record { r.RequestCorrelationID = "other"; return r }},
		{"pairing", func(r replycontext.Record) replycontext.Record { r.ProviderOutputPin = "other"; return r }},
		{"return", func(r replycontext.Record) replycontext.Record { r.ReturnJoins = nil; return r }},
		{"accepted", func(r replycontext.Record) replycontext.Record { r.AcceptedReplyEventID = uuid.NewString(); return r }},
		{"created", func(r replycontext.Record) replycontext.Record { r.CreatedAt = r.CreatedAt.Add(time.Second); return r }},
		{"updated", func(r replycontext.Record) replycontext.Record { r.UpdatedAt = r.UpdatedAt.Add(time.Second); return r }},
		{"terminal_time", func(r replycontext.Record) replycontext.Record {
			at := r.UpdatedAt.Add(time.Second)
			r.TerminalAt = &at
			return r
		}},
		{"reopened", func(r replycontext.Record) replycontext.Record {
			r.State, r.AcceptedReplyEventID, r.TerminalAt = replycontext.StateOpen, "", nil
			return r
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, childRun, captured := runForkReplyPlanFixture(t, replycontext.StateTerminal, false, true)
			changed := test.alter(captured)
			if err := changed.Validate(); err != nil {
				t.Fatalf("fixture must reach captured equality validation: %v", err)
			}
			if _, err := projectRunForkReplyContext(plan, childRun, changed); err == nil {
				t.Fatal("valid but uncaptured record acquired fixed-cut reply authority")
			}
		})
	}
}

func TestRunForkReplyProjectionRejectsInvalidRunOwnership(t *testing.T) {
	for _, variant := range []string{"same_run", "invalid_child", "zero_child", "noncanonical_child", "invalid_source", "zero_source"} {
		t.Run(variant, func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
			switch variant {
			case "same_run":
				childRun = plan.SourceRunID
			case "invalid_child":
				childRun = "not-a-run"
			case "zero_child":
				childRun = uuid.Nil.String()
			case "noncanonical_child":
				childRun = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
			case "invalid_source":
				plan.SourceRunID = "not-a-run"
			case "zero_source":
				plan.SourceRunID = uuid.Nil.String()
			}
			if _, err := projectRunForkReplyContext(plan, childRun, source); err == nil {
				t.Fatal("inexact or same-run ownership acquired child reply authority")
			}
		})
	}
}

func TestRunForkReplyContextOnlyReforkProjectionModel(t *testing.T) {
	for _, state := range []replycontext.State{replycontext.StateOpen, replycontext.StateTerminal} {
		for _, root := range []bool{false, true} {
			t.Run(string(state)+map[bool]string{false: "/keyed", true: "/root"}[root], func(t *testing.T) {
				originalPlan, bRun, original := runForkReplyPlanFixture(t, state, root, true)
				bPlan := runForkReplyCapturedChildPlan(t, originalPlan, bRun)
				if history, admitted := bPlan.HistoricalEventIDs(bPlan.ForkPoint.Revision); !admitted || len(history) != 0 {
					t.Fatal("B-cut fixture must admit an empty physical event inventory")
				}
				b := bPlan.ReplyContexts[0]
				cRun := uuid.NewString()
				c, err := projectRunForkReplyContext(bPlan, cRun, b)
				if err != nil {
					t.Fatal(err)
				}
				if c.ID != deterministicRunForkReplyContextID(cRun, b.ID) || c.RunID != cRun ||
					c.RequestEventID != deterministicRunForkReplayEventID(cRun, b.RequestEventID) ||
					c.SourceRunID != original.SourceRunID || c.SourceRequestEventID != original.SourceRequestEventID ||
					c.State != state || c.RequestCorrelationID != original.RequestCorrelationID || c.CorrelationKey != original.CorrelationKey ||
					!c.CreatedAt.Equal(original.CreatedAt) || !c.UpdatedAt.Equal(original.UpdatedAt) || len(c.ReturnJoins) != len(original.ReturnJoins) {
					t.Fatalf("context-only refork changed captured authority: B=%+v C=%+v", b, c)
				}
				wantOrigin := b.Origin
				if root {
					wantOrigin.EntityID, wantOrigin.FlowInstance = cRun, cRun
				}
				if c.Origin != wantOrigin {
					t.Fatal("context-only refork lost captured constructor origin")
				}
				if state == replycontext.StateTerminal {
					if c.AcceptedReplyEventID != deterministicRunForkReplayEventID(cRun, b.AcceptedReplyEventID) ||
						c.TerminalAt == b.TerminalAt || !c.TerminalAt.Equal(*original.TerminalAt) {
						t.Fatal("context-only terminal refork reopened or lost the accepted outcome")
					}
				} else if c.AcceptedReplyEventID != "" || c.TerminalAt != nil {
					t.Fatal("context-only open refork acquired a terminal outcome")
				}
				for i, receipt := range c.ReturnJoins {
					if receipt.Disposition == events.JoinAdmissionEarly {
						if !reflect.DeepEqual(receipt, original.ReturnJoins[i]) {
							t.Fatal("context-only refork armed an early return")
						}
						continue
					}
					entry := receipt.Ref.StageEntry()
					if entry.RunID != cRun || entry.OriginRunID != originalPlan.SourceRunID || entry.Stage != "awaiting" ||
						entry.OccurrenceID != "original-return-arm" || entry.EntityID != c.Origin.EntityID || entry.InstancePath != c.Origin.FlowInstance ||
						!receipt.Ref.Declaration().Equal(original.ReturnJoins[i].Ref.Declaration()) {
						t.Fatal("context-only refork selected a newer return arm or changed provenance")
					}
				}
				tx, mock := startSnapshotTransaction(t)
				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(cRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
				owner := &runForkReplyOwnerProbe{}
				admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: state == replycontext.StateOpen})
				_, err = materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, bPlan, cRun, false, admission)
				if err != nil || len(owner.written) != 1 || !sameRunForkReplyRecord(owner.written[0], c) {
					t.Fatalf("context-only model materialization/readback failed: %v", err)
				}
			})
		}
	}
}

func TestRunForkReplyEnclosingChildMappingAndParentFirstWritesModel(t *testing.T) {
	for _, state := range []replycontext.State{replycontext.StateOpen, replycontext.StateTerminal} {
		t.Run(string(state), func(t *testing.T) {
			plan, childRun, source, enclosing := runForkReplyEnclosingPlanFixture(t, state)
			projected, err := projectRunForkReplyContexts(plan, childRun)
			if err != nil || len(projected) != 2 {
				t.Fatalf("enclosing model projection failed: %v", err)
			}
			parent, child := projected[0], projected[1]
			if parent.ID != deterministicRunForkReplyContextID(childRun, enclosing.ID) || parent.EnclosingReplyContextID != "" ||
				child.ID != deterministicRunForkReplyContextID(childRun, source.ID) || child.EnclosingReplyContextID != parent.ID ||
				child.SourceRunID != source.SourceRunID || child.SourceRequestEventID != source.SourceRequestEventID ||
				parent.SourceRunID != enclosing.SourceRunID || parent.SourceRequestEventID != enclosing.SourceRequestEventID {
				t.Fatal("enclosing mapping changed original evidence or exact captured parent identity")
			}
			tx, mock := startSnapshotTransaction(t)
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
			owner := &runForkReplyOwnerProbe{}
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true})
			_, err = materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, false, admission)
			if err != nil || !reflect.DeepEqual(owner.written, projected) {
				t.Fatalf("model owner did not write/read exact parents before children: %v", err)
			}
			bPlan := runForkReplyCapturedChildPlan(t, plan, childRun)
			cRun := uuid.NewString()
			reforked, err := projectRunForkReplyContexts(bPlan, cRun)
			if err != nil || len(reforked) != 2 {
				t.Fatalf("enclosing context-only refork failed: %v", err)
			}
			if reforked[1].EnclosingReplyContextID != reforked[0].ID ||
				reforked[0].ID != deterministicRunForkReplyContextID(cRun, parent.ID) ||
				reforked[1].EnclosingReplyContextID == deterministicRunForkReplyContextID(cRun, enclosing.ID) ||
				reforked[1].SourceRunID != source.SourceRunID || reforked[1].SourceRequestEventID != source.SourceRequestEventID {
				t.Fatal("refork resolved an ancestor instead of the exact immediate-source enclosing row")
			}
			bPlan.ReplyContexts[1].EnclosingReplyContextID = enclosing.ID
			if _, err := projectRunForkReplyContexts(bPlan, cRun); err == nil {
				t.Fatal("uncaptured ancestor enclosing reference became immediate-source authority")
			}
		})
	}
}

func TestRunForkReplyEnclosingProjectionRejectsInvalidCapturedChainsBeforeWriter(t *testing.T) {
	for _, variant := range []string{"missing", "foreign_run", "foreign_provider", "same", "cyclic", "duplicate"} {
		t.Run(variant, func(t *testing.T) {
			plan, childRun, source, enclosing := runForkReplyEnclosingPlanFixture(t, replycontext.StateOpen)
			switch variant {
			case "missing":
				plan.ReplyContexts = []replycontext.Record{source}
			case "foreign_run":
				plan.ReplyContexts[1].RunID = uuid.NewString()
			case "foreign_provider":
				plan.ReplyContexts[1].ProviderFlowID = "sibling"
			case "same":
				plan.ReplyContexts[0].EnclosingReplyContextID = source.ID
			case "cyclic":
				plan.ReplyContexts[0].ProviderFlowID = enclosing.RequesterFlowID
				plan.ReplyContexts[1].EnclosingReplyContextID = source.ID
			case "duplicate":
				plan.ReplyContexts = append(plan.ReplyContexts, enclosing)
			}
			_, err := projectRunForkReplyContexts(plan, childRun)
			if err == nil {
				t.Fatal("invalid enclosing chain acquired projected reply authority")
			}
			if variant == "cyclic" && !strings.Contains(err.Error(), "cyclic") {
				t.Fatalf("cycle fixture failed before dependency-cycle validation: %v", err)
			}
			tx, _ := startSnapshotTransaction(t)
			owner := &runForkReplyOwnerProbe{}
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true})
			got, err := materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, false, admission)
			if err == nil || len(owner.written) != 0 || !reflect.DeepEqual(got, admission) {
				t.Fatal("invalid enclosing chain reached the writer or discharged admission")
			}
		})
	}
}

// This owner is a model probe, not a native integrity or served-execution proof.
type runForkReplyOwnerProbe struct {
	records  map[string]replycontext.Record
	written  []replycontext.Record
	writeErr error
	loadErr  error
	alter    func(replycontext.Record) replycontext.Record
}

func (p *runForkReplyOwnerProbe) CreateWithinTransaction(_ context.Context, _ *mutationprotocol.Attempt, record replycontext.Record) error {
	if p.writeErr != nil {
		return p.writeErr
	}
	if record.EnclosingReplyContextID != "" {
		enclosing, found := p.records[record.EnclosingReplyContextID]
		if !found || enclosing.RunID != record.RunID || enclosing.ProviderFlowID != record.RequesterFlowID {
			return errors.New("probe requires the exact enclosing row before its child")
		}
	}
	p.written = append(p.written, record)
	if p.records == nil {
		p.records = make(map[string]replycontext.Record)
	}
	p.records[record.ID] = record
	return nil
}

func (p *runForkReplyOwnerProbe) LoadWithinTransaction(_ context.Context, _ *sql.Tx, id string) (replycontext.Record, error) {
	if p.loadErr != nil {
		return replycontext.Record{}, p.loadErr
	}
	record, found := p.records[id]
	if !found {
		return replycontext.Record{}, replycontext.ErrNotFound
	}
	if p.alter != nil {
		record = p.alter(record)
	}
	return record, nil
}

func TestRunForkReplyRootProjectionRequiresExactCapturedConstructorOrigin(t *testing.T) {
	for _, variant := range []string{"origin_scope", "origin_entity", "origin_instance", "constructor_run"} {
		t.Run(variant, func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateOpen, true, true)
			source.ReturnJoins = nil
			switch variant {
			case "origin_scope":
				source.Origin.FlowID = "sibling"
			case "origin_entity":
				source.Origin.EntityID = uuid.NewString()
			case "origin_instance":
				source.Origin.FlowInstance = uuid.NewString()
			case "constructor_run":
				plan.SourceRunID = uuid.NewString()
				source.RunID = plan.SourceRunID
			}
			if err := source.Validate(); err != nil {
				t.Fatalf("fixture must reach captured constructor validation: %v", err)
			}
			plan.ReplyContexts[0] = source
			if _, err := projectRunForkReplyContext(plan, childRun, source); err == nil {
				t.Fatal("root row acquired sibling or foreign constructor origin authority")
			}
		})
	}
}

func TestRunForkReplyMaterializationDischargesOnlyAfterExactReadback(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite_argument", true: "postgres_argument"}[postgres], func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
			child, err := projectRunForkReplyContext(plan, childRun, source)
			if err != nil {
				t.Fatal(err)
			}
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true, ActiveSession: true})
			before := append([]runfork.RunForkUnsupportedBlocker(nil), admission.UnsupportedBlockers...)
			tx, mock := startSnapshotTransaction(t)
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			owner := &runForkReplyOwnerProbe{}
			got, err := materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, postgres, admission)
			if err != nil || len(owner.written) != 1 || !sameRunForkReplyRecord(owner.written[0], child) {
				t.Fatalf("materialized reply/readback failed: %v", err)
			}
			if len(got.UnsupportedBlockers) != 1 || got.UnsupportedBlockers[0].Code != runfork.RunForkBlockerSessionHistoryUnproven ||
				!reflect.DeepEqual(admission.UnsupportedBlockers, before) || got.StateOnlyExecutionReady || !got.ReplayResumeFactsPresent {
				t.Fatalf("other restrictions weakened or input mutated: %+v", got)
			}
			for _, disposition := range got.Dispositions {
				if disposition.Fact == runfork.RunForkReplayResumeFactOpenReplyContext && (disposition.BlockerCode != "" || disposition.Disposition != runfork.RunForkReplayResumeDispositionReconstruct) {
					t.Fatal("materialized open reply still refused")
				}
			}
		})
	}
}

func TestRunForkReplyReadbackRejectsLifecycleAndBindingCorruption(t *testing.T) {
	for _, state := range []replycontext.State{replycontext.StateOpen, replycontext.StateTerminal} {
		for _, test := range []struct {
			name  string
			alter func(replycontext.Record) replycontext.Record
		}{
			{"correlation", func(r replycontext.Record) replycontext.Record { r.RequestCorrelationID = "other"; return r }},
			{"pairing", func(r replycontext.Record) replycontext.Record { r.ProviderOutputPin = "other"; return r }},
			{"state", func(r replycontext.Record) replycontext.Record { r.State = ""; return r }},
			{"time", func(r replycontext.Record) replycontext.Record { r.UpdatedAt = r.UpdatedAt.Add(time.Second); return r }},
			{"return", func(r replycontext.Record) replycontext.Record { r.ReturnJoins = nil; return r }},
			{"accepted", func(r replycontext.Record) replycontext.Record { r.AcceptedReplyEventID = uuid.NewString(); return r }},
			{"owner_run", func(r replycontext.Record) replycontext.Record { r.RunID = uuid.NewString(); return r }},
			{"logical_request", func(r replycontext.Record) replycontext.Record { r.RequestEventID = uuid.NewString(); return r }},
			{"original_run", func(r replycontext.Record) replycontext.Record { r.SourceRunID = uuid.NewString(); return r }},
			{"original_request", func(r replycontext.Record) replycontext.Record { r.SourceRequestEventID = uuid.NewString(); return r }},
			{"enclosing", func(r replycontext.Record) replycontext.Record {
				r.EnclosingReplyContextID = uuid.NewString()
				return r
			}},
			{"origin", func(r replycontext.Record) replycontext.Record { r.Origin.FlowInstance = "sibling/one"; return r }},
			{"created", func(r replycontext.Record) replycontext.Record { r.CreatedAt = r.CreatedAt.Add(time.Second); return r }},
			{"terminal_time", func(r replycontext.Record) replycontext.Record {
				at := r.UpdatedAt.Add(time.Second)
				r.TerminalAt = &at
				return r
			}},
		} {
			t.Run(string(state)+"/"+test.name, func(t *testing.T) {
				plan, childRun, source := runForkReplyPlanFixture(t, state, false, true)
				child, err := projectRunForkReplyContext(plan, childRun, source)
				if err != nil {
					t.Fatal(err)
				}
				tx, mock := startSnapshotTransaction(t)
				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
				owner := &runForkReplyOwnerProbe{records: map[string]replycontext.Record{child.ID: child}, alter: test.alter}
				admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: state == replycontext.StateOpen})
				got, err := requireMaterializedRunForkReplyContexts(context.Background(), tx, owner, plan, childRun, false, admission)
				if err == nil || !reflect.DeepEqual(got, admission) || len(owner.written) != 0 {
					t.Fatal("reply readback repaired corruption or discharged admission")
				}
			})
		}
	}
}

func TestRunForkReplyTerminalMaterializationPreservesAcceptedOutcomeBothDialectArguments(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite_argument", true: "postgres_argument"}[postgres], func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateTerminal, true, true)
			child, err := projectRunForkReplyContext(plan, childRun, source)
			if err != nil {
				t.Fatal(err)
			}
			tx, mock := startSnapshotTransaction(t)
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			owner := &runForkReplyOwnerProbe{}
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{})
			got, err := materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, postgres, admission)
			if err != nil || !reflect.DeepEqual(got, admission) || len(owner.written) != 1 || !sameRunForkReplyRecord(owner.written[0], child) {
				t.Fatalf("settled reply was reopened or lost: %v", err)
			}
		})
	}
}

func TestRunForkReplyReadbackRefusesInventoryGapOrExtra(t *testing.T) {
	for _, count := range []int{0, 2} {
		plan, childRun, _ := runForkReplyPlanFixture(t, replycontext.StateOpen, true, true)
		tx, mock := startSnapshotTransaction(t)
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
		admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true})
		got, err := requireMaterializedRunForkReplyContexts(context.Background(), tx, &runForkReplyOwnerProbe{}, plan, childRun, false, admission)
		if err == nil || !reflect.DeepEqual(got, admission) {
			t.Fatal("inexact inventory discharged source reply refusal")
		}
	}
}

func TestRunForkReplyReadbackPreservesInventoryAndOwnerFailures(t *testing.T) {
	for _, variant := range []string{"inventory_error", "missing_row", "load_error"} {
		t.Run(variant, func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
			child, err := projectRunForkReplyContext(plan, childRun, source)
			if err != nil {
				t.Fatal(err)
			}
			tx, mock := startSnapshotTransaction(t)
			failure := errors.New("reply readback refused")
			owner := &runForkReplyOwnerProbe{records: map[string]replycontext.Record{child.ID: child}}
			inventory := mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun)
			if variant == "inventory_error" {
				inventory.WillReturnError(failure)
			} else {
				inventory.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
				if variant == "missing_row" {
					owner.records, failure = nil, replycontext.ErrNotFound
				} else {
					owner.loadErr = failure
				}
			}
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true})
			got, err := requireMaterializedRunForkReplyContexts(context.Background(), tx, owner, plan, childRun, false, admission)
			if !errors.Is(err, failure) || !reflect.DeepEqual(got, admission) || len(owner.written) != 0 {
				t.Fatal("reply readback failure became repair, a write or continuation authority")
			}
		})
	}
}

func TestRunForkReplyMaterializationReadbackFailureKeepsAdmission(t *testing.T) {
	plan, childRun, _ := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
	tx, mock := startSnapshotTransaction(t)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	failure := errors.New("reply owner readback refused")
	owner := &runForkReplyOwnerProbe{loadErr: failure}
	admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true})
	got, err := materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, false, admission)
	if !errors.Is(err, failure) || len(owner.written) != 1 || !reflect.DeepEqual(got, admission) {
		t.Fatal("model write without exact readback discharged reply admission")
	}
}

func TestRunForkReplyMaterializationRejectsCorruptCapturedAuthorityBeforeWriter(t *testing.T) {
	for _, corruption := range []string{"origin", "source_run", "missing_constructor", "foreign_constructor", "malformed_constructor", "invalid_request", "invalid_accepted", "missing_history"} {
		t.Run(corruption, func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateTerminal, false, true)
			switch corruption {
			case "origin":
				source.Origin.FlowID = "sibling"
			case "source_run":
				source.RunID = childRun
			case "missing_constructor":
				plan.Entities[0].MaterializationMetadata.InitialMaterialization = nil
			case "foreign_constructor":
				foreign, _, _ := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
				plan.Entities[0].MaterializationMetadata.InitialMaterialization = foreign.Entities[0].MaterializationMetadata.InitialMaterialization
			case "malformed_constructor":
				plan.Entities[0].MaterializationMetadata.InitialMaterialization = []byte(`{}`)
			case "invalid_request":
				source.RequestEventID = uuid.Nil.String()
			case "invalid_accepted":
				source.AcceptedReplyEventID = uuid.Nil.String()
			case "missing_history":
				plan = plan.WithHistoricalEvents(0, nil)
			}
			plan.ReplyContexts[0] = source
			tx, _ := startSnapshotTransaction(t)
			owner := &runForkReplyOwnerProbe{}
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{})
			got, err := materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, false, admission)
			if err == nil || !reflect.DeepEqual(got, admission) || len(owner.written) != 0 {
				t.Fatal("corrupt captured row or constructor acquired child reply authority")
			}
		})
	}
}

func TestRunForkReplyMaterializationPreservesCancellationAndWriterFailure(t *testing.T) {
	plan, childRun, _ := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
	admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := materializeRunForkReplyContexts(ctx, nil, nil, nil, plan, childRun, false, admission); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if _, err := requireMaterializedRunForkReplyContexts(ctx, nil, nil, plan, childRun, false, admission); !errors.Is(err, context.Canceled) {
		t.Fatalf("readback cancellation lost: %v", err)
	}
	tx, _ := startSnapshotTransaction(t)
	failure := errors.New("reply owner refused")
	owner := &runForkReplyOwnerProbe{writeErr: failure}
	got, err := materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, false, admission)
	if !errors.Is(err, failure) || !reflect.DeepEqual(got, admission) || len(owner.written) != 0 {
		t.Fatal("writer refusal became reply continuation authority")
	}
}
