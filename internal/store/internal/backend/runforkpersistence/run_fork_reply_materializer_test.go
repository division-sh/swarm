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
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
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
		{"invalid_accepted", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.AcceptedReplyEventID = "not-an-event" }},
		{"wrong_run", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.RunID = uuid.NewString() }},
		{"wrong_origin", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.Origin.EntityID = uuid.NewString() }},
		{"wrong_scope", func(_ *runfork.RunForkPlan, r *replycontext.Record) { r.Origin.FlowID = "sibling" }},
		{"uncaptured", func(p *runfork.RunForkPlan, _ *replycontext.Record) { p.ReplyContexts = nil }},
		{"duplicate", func(p *runfork.RunForkPlan, r *replycontext.Record) { p.ReplyContexts = append(p.ReplyContexts, *r) }},
		{"unknown_history", func(p *runfork.RunForkPlan, _ *replycontext.Record) { p.ForkPoint.Revision++ }},
		{"out_of_cut", func(p *runfork.RunForkPlan, _ *replycontext.Record) { *p = p.WithHistoricalEvents(1, nil) }},
		{"missing_constructor", func(p *runfork.RunForkPlan, _ *replycontext.Record) {
			p.Entities[0].MaterializationMetadata.InitialMaterialization = nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateTerminal, false, false)
			test.mutate(&plan, &source)
			if err := source.Validate(); err == nil && len(plan.ReplyContexts) == 1 {
				plan.ReplyContexts[0] = source
			}
			if _, err := projectRunForkReplyContext(plan, childRun, source); err == nil {
				t.Fatal("uncaptured/corrupt reply authority admitted")
			}
		})
	}
}

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

func runForkReplyEventRecord(t *testing.T, plan runfork.RunForkPlan, childRunID, sourceEventID, eventID, flowID string, origin *events.RouteIdentity) eventrecord.Record {
	t.Helper()
	var routing events.RoutingSource
	var err error
	if origin == nil {
		routing, err = events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: "worker/one", EntityID: flowidentity.EntityID("worker/one")})
	} else if origin.FlowID == "." {
		routing, err = events.NewRootRoutingSource(childRunID)
	} else {
		routing, err = events.NewConcreteTemplateInstanceRoutingSource(*origin)
	}
	if err != nil {
		t.Fatal(err)
	}
	lineage, err := events.NewSelectedForkLineage(childRunID, plan.SourceRunID, sourceEventID, runfork.RunForkDeliveryEventReplayOwner, "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), FlowID: flowID, EventKey: "request.reply",
		SchemaDigest: "sha256:" + strings.Repeat("b", 64), SchemaClass: events.PayloadSchemaAuthored,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := events.NewPayloadAdmission([]byte(`{"correlation":"item-42"}`), admission)
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Lineage: lineage, Facts: events.EventFacts{
		ID: eventID, Type: "request.reply", Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: runfork.RunForkDeliveryEventReplayOwner},
		Payload: payload.Payload(), RoutingSource: routing, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, routing.Route()),
		CreatedAt: time.Unix(200, 0).UTC(), ExecutionMode: executionmode.Live,
	}})
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, payload)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteHistoricalRunForkReplay, events.ConnectEvaluationLedger{})
	if err != nil {
		t.Fatal(err)
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func expectRunForkReplyEvents(t *testing.T, mock sqlmock.Sqlmock, postgres bool, plan runfork.RunForkPlan, source, child replycontext.Record) {
	t.Helper()
	request := runForkReplyEventRecord(t, plan, child.RunID, source.RequestEventID, child.RequestEventID, child.RequesterFlowID, &child.Origin)
	mock.ExpectQuery(`FROM events e`).WithArgs(child.RequestEventID).WillReturnRows(selectedInputRecordRows(request, !postgres, true))
	if child.State == replycontext.StateTerminal {
		reply := runForkReplyEventRecord(t, plan, child.RunID, source.AcceptedReplyEventID, child.AcceptedReplyEventID, child.ProviderFlowID, nil)
		mock.ExpectQuery(`FROM events e`).WithArgs(child.AcceptedReplyEventID).WillReturnRows(selectedInputRecordRows(reply, !postgres, true))
	}
}

func TestRunForkReplyChildProducerRequiresCanonicalRoot(t *testing.T) {
	plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateOpen, true, true)
	child, err := projectRunForkReplyContext(plan, childRun, source)
	if err != nil {
		t.Fatal(err)
	}
	record := runForkReplyEventRecord(t, plan, childRun, source.RequestEventID, child.RequestEventID, child.RequesterFlowID, &child.Origin)
	admitted, err := record.Decode()
	if err != nil {
		t.Fatal(err)
	}
	event := admitted.Event()
	if event.RoutingSource().Kind() != events.RoutingSourceRoot || event.RoutingSource().Route().FlowID != "" {
		t.Fatal("root fixture contradicts canonical routing owner")
	}
	if err := validateRunForkReplyChildProducer(event, ".", nil); err != nil {
		t.Fatalf("canonical root provider rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		flowID string
		origin events.RouteIdentity
	}{
		{"declaration", "sibling", child.Origin},
		{"origin_scope", ".", events.RouteIdentity{FlowID: "sibling", EntityID: childRun, FlowInstance: childRun}},
		{"origin_entity", ".", events.RouteIdentity{FlowID: ".", EntityID: plan.SourceRunID, FlowInstance: childRun}},
		{"origin_instance", ".", events.RouteIdentity{FlowID: ".", EntityID: childRun, FlowInstance: plan.SourceRunID}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateRunForkReplyChildProducer(event, test.flowID, &test.origin); err == nil {
				t.Fatal("root producer acquired sibling or source origin authority")
			}
		})
	}
	foreignRoot := uuid.NewString()
	foreignOrigin := events.RouteIdentity{FlowID: ".", EntityID: foreignRoot, FlowInstance: foreignRoot}
	foreign := runForkReplyEventRecord(t, plan, foreignRoot, source.RequestEventID, child.RequestEventID, ".", &foreignOrigin)
	foreign.RunID = childRun
	foreignAdmitted, err := foreign.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRunForkReplyChildProducer(foreignAdmitted.Event(), ".", nil); err == nil {
		t.Fatal("foreign root event acquired child provider authority")
	}
}

func TestRunForkReplyMaterializationDischargesOnlyAfterExactReadback(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres], func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
			child, err := projectRunForkReplyContext(plan, childRun, source)
			if err != nil {
				t.Fatal(err)
			}
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true, ActiveSession: true})
			before := append([]runfork.RunForkUnsupportedBlocker(nil), admission.UnsupportedBlockers...)
			tx, mock := startSnapshotTransaction(t)
			expectRunForkReplyEvents(t, mock, postgres, plan, source, child)
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			expectRunForkReplyEvents(t, mock, postgres, plan, source, child)
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
		} {
			t.Run(string(state)+"/"+test.name, func(t *testing.T) {
				plan, childRun, source := runForkReplyPlanFixture(t, state, false, true)
				child, err := projectRunForkReplyContext(plan, childRun, source)
				if err != nil {
					t.Fatal(err)
				}
				tx, mock := startSnapshotTransaction(t)
				mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
				expectRunForkReplyEvents(t, mock, false, plan, source, child)
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

func TestRunForkReplyTerminalMaterializationPreservesAcceptedOutcomeBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres], func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateTerminal, true, true)
			child, err := projectRunForkReplyContext(plan, childRun, source)
			if err != nil {
				t.Fatal(err)
			}
			tx, mock := startSnapshotTransaction(t)
			expectRunForkReplyEvents(t, mock, postgres, plan, source, child)
			mock.ExpectQuery(`SELECT COUNT\(\*\) FROM reply_contexts WHERE run_id`).WithArgs(childRun).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			expectRunForkReplyEvents(t, mock, postgres, plan, source, child)
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

func TestRunForkReplyMaterializationRejectsWrongProducerOrLineage(t *testing.T) {
	for _, corruption := range []string{"producer", "lineage", "child_run", "accepted_missing"} {
		t.Run(corruption, func(t *testing.T) {
			plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateTerminal, false, true)
			child, err := projectRunForkReplyContext(plan, childRun, source)
			if err != nil {
				t.Fatal(err)
			}
			request := runForkReplyEventRecord(t, plan, childRun, source.RequestEventID, child.RequestEventID, child.RequesterFlowID, &child.Origin)
			switch corruption {
			case "producer":
				request.SourceRoute = []byte(`{"flow_id":"sibling","flow_instance":"sibling/one","entity_id":"` + flowidentity.EntityID("sibling/one") + `"}`)
			case "lineage":
				request.SelectedForkSourceRunID = uuid.NewString()
			case "child_run":
				request.RunID = plan.SourceRunID
			}
			tx, mock := startSnapshotTransaction(t)
			mock.ExpectQuery(`FROM events e`).WithArgs(child.RequestEventID).WillReturnRows(selectedInputRecordRows(request, true, true))
			if corruption == "accepted_missing" {
				mock.ExpectQuery(`FROM events e`).WithArgs(child.AcceptedReplyEventID).WillReturnRows(selectedInputRecordRows(eventrecord.Record{}, true, false))
			}
			owner := &runForkReplyOwnerProbe{}
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{})
			if _, err := materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, false, admission); err == nil || len(owner.written) != 0 {
				t.Fatal("wrong request/reply producer or lineage acquired child reply authority")
			}
		})
	}
}

func TestRunForkReplyMaterializationRejectsMissingChildRequestBeforeWriter(t *testing.T) {
	plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
	child, err := projectRunForkReplyContext(plan, childRun, source)
	if err != nil {
		t.Fatal(err)
	}
	tx, mock := startSnapshotTransaction(t)
	mock.ExpectQuery(`FROM events e`).WithArgs(child.RequestEventID).WillReturnRows(selectedInputRecordRows(eventrecord.Record{}, true, false))
	owner := &runForkReplyOwnerProbe{}
	admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true})
	got, err := materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, false, admission)
	if err == nil || !reflect.DeepEqual(got, admission) || len(owner.written) != 0 {
		t.Fatal("missing child request became source fallback, a write or reply authority")
	}
}

func TestRunForkReplyMaterializationPreservesCancellationAndWriterFailure(t *testing.T) {
	plan, childRun, source := runForkReplyPlanFixture(t, replycontext.StateOpen, false, true)
	admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := materializeRunForkReplyContexts(ctx, nil, nil, nil, plan, childRun, false, admission); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	child, err := projectRunForkReplyContext(plan, childRun, source)
	if err != nil {
		t.Fatal(err)
	}
	tx, mock := startSnapshotTransaction(t)
	expectRunForkReplyEvents(t, mock, false, plan, source, child)
	failure := errors.New("reply owner refused")
	owner := &runForkReplyOwnerProbe{writeErr: failure}
	got, err := materializeRunForkReplyContexts(context.Background(), tx, &mutationprotocol.Attempt{}, owner, plan, childRun, false, admission)
	if !errors.Is(err, failure) || !reflect.DeepEqual(got, admission) || len(owner.written) != 0 {
		t.Fatal("writer refusal became reply continuation authority")
	}
}
