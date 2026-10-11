package runforkreadiness

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecore "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestAdmissionSealsInheritedTimerSelectionAndRecordedOwnership(t *testing.T) {
	for _, flowID := range []string{".", "consumer"} {
		for _, recurring := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/recurring=%t", flowID, recurring), func(t *testing.T) {
				req := inheritedTimerAdmissionRequest(t, flowID, recurring)
				admitted, err := Admit(req)
				if err != nil {
					t.Fatal(err)
				}
				if err := admitted.ValidateAgainst(req.Binding); err != nil {
					t.Fatal(err)
				}
				firstChild, secondChild := uuid.NewString(), uuid.NewString()
				first := projectedTimerAdmissionFixture(t, req, firstChild)
				want, err := admitted.SelectInheritedWorkflowTimer(first)
				if err != nil || want == nil {
					t.Fatalf("first child timer = %#v, %v", want, err)
				}
				if want.Ref.DeclarationRevision == first.Ref.DeclarationRevision || want.OwnerAgent != "selected-timer-owner" {
					t.Fatal("selection lost canonical selected effects")
				}
				expected := first.Canonical()
				expected.Ref.DeclarationRevision = want.Ref.DeclarationRevision
				expected.OwnerAgent = "selected-timer-owner"
				expected.EventType = "timer.selected"
				if flowID != "." {
					expected.EventType = flowID + "/timer.selected"
				}
				if !reflect.DeepEqual(*want, expected) || !want.FireAt.Before(want.CreatedAt) {
					t.Fatal("selected delay rearmed or changed fixed-cut timer facts")
				}
				before := want.Canonical()
				want.Payload[0], want.OwnerAgent = ' ', "caller-mutated"
				second := projectedTimerAdmissionFixture(t, req, secondChild)
				bundle, _ := semanticview.Bundle(req.Source)
				bundle.Semantics.Timers = nil
				req.Plan.WorkflowTimers[0].Payload[0] = ' '
				req.Plan.Entities[0].MaterializationMetadata.InitialMaterialization[0] = ' '
				again, err := admitted.SelectInheritedWorkflowTimer(first)
				if err != nil || again == nil || !reflect.DeepEqual(*again, before) {
					t.Fatalf("sealed timer changed through external mutation: %#v, %v", again, err)
				}
				other, err := admitted.SelectInheritedWorkflowTimer(second)
				if err != nil || other == nil || other.RunID != secondChild || other.OwnerAgent != before.OwnerAgent ||
					other.Ref.DeclarationRevision != before.Ref.DeclarationRevision || other.EventType != before.EventType {
					t.Fatalf("seal reserved a child ID or reread source: %#v, %v", other, err)
				}
			})
		}
	}
}

func TestAdmissionInheritedTimerSelectionRejectsProjectionDrift(t *testing.T) {
	for _, removed := range []bool{false, true} {
		t.Run(fmt.Sprintf("removed=%t", removed), func(t *testing.T) {
			req := inheritedTimerAdmissionRequest(t, "consumer", true)
			if removed {
				bundle, _ := semanticview.Bundle(req.Source)
				bundle.Semantics.Timers = nil
				refreshTimerAdmissionSource(t, &req)
			}
			admitted, err := Admit(req)
			if err != nil {
				t.Fatal(err)
			}
			projected := projectedTimerAdmissionFixture(t, req, uuid.NewString())
			got, err := admitted.SelectInheritedWorkflowTimer(projected)
			if err != nil || (got == nil) != removed {
				t.Fatalf("exact timer selection = %#v, %v", got, err)
			}
			for _, test := range []struct {
				name   string
				change func(*pipeline.WorkflowTimerActivation)
			}{
				{"unsealed_timer", func(a *pipeline.WorkflowTimerActivation) { a.SourceTimerID = uuid.NewString() }},
				{"declaration", func(a *pipeline.WorkflowTimerActivation) { a.Ref.DeclarationKey = "foreign" }},
				{"activation_id", func(a *pipeline.WorkflowTimerActivation) { a.Ref.ActivationID = uuid.NewString() }},
				{"source_revision", func(a *pipeline.WorkflowTimerActivation) { a.Ref.DeclarationRevision = "foreign" }},
				{"cause", func(a *pipeline.WorkflowTimerActivation) {
					a.Ref.Cause = timeridentity.WorkflowTimerActivationCauseInitial
				}},
				{"source_run", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromRunID = uuid.NewString() }},
				{"same_run", func(a *pipeline.WorkflowTimerActivation) { a.RunID = req.Plan.SourceRunID }},
				{"entity", func(a *pipeline.WorkflowTimerActivation) { a.EntityID = uuid.NewString() }},
				{"route", func(a *pipeline.WorkflowTimerActivation) { a.Route.InstanceID = "other" }},
				{"effect_owner", func(a *pipeline.WorkflowTimerActivation) { a.OwnerAgent = "forged-selected-owner" }},
				{"effect_event", func(a *pipeline.WorkflowTimerActivation) { a.EventType = "consumer/timer.forged" }},
				{"mode", func(a *pipeline.WorkflowTimerActivation) { a.ExecutionMode = executionmode.Mock }},
				{"later_occurrence", func(a *pipeline.WorkflowTimerActivation) { a.FireAt = a.FireAt.Add(time.Hour) }},
				{"changed_original_arm", func(a *pipeline.WorkflowTimerActivation) { a.SourceArmedAt = a.SourceArmedAt.Add(-time.Hour) }},
				{"changed_interval", func(a *pipeline.WorkflowTimerActivation) { a.RecurrenceInterval = time.Hour / 2 }},
				{"rewritten_payload", func(a *pipeline.WorkflowTimerActivation) { a.Payload = []byte(`{"value":7.0}`) }},
				{"point_revision", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointRevision++ }},
				{"point_event", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromEventID = uuid.NewString() }},
				{"reconstruction_owner", func(a *pipeline.WorkflowTimerActivation) { a.ReconstructionOwner = "other-owner" }},
				{"already_fired", func(a *pipeline.WorkflowTimerActivation) { a.FiredAt = a.FireAt.Add(-time.Hour) }},
				{"inactive", func(a *pipeline.WorkflowTimerActivation) { a.Status = "cancelled" }},
			} {
				t.Run(test.name, func(t *testing.T) {
					bad := projected.Canonical()
					test.change(&bad)
					if got, err := admitted.SelectInheritedWorkflowTimer(bad); err == nil || got != nil {
						t.Fatalf("changed projection returned selected/removal authority: %#v, %v", got, err)
					}
				})
			}
			if _, err := (Admission{}).SelectInheritedWorkflowTimer(projected); err == nil {
				t.Fatal("zero admission selected timer effects")
			}
		})
	}
}

func TestAdmissionInheritedTimerSelectionPreservesLoopCorrespondence(t *testing.T) {
	req := inheritedTimerAdmissionRequest(t, "consumer", false)
	source, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(req.Plan.WorkflowTimers[0])
	if err != nil {
		t.Fatal(err)
	}
	activation, err := loopruntime.New(source.RunID, source.EntityID, "consumer", "loop", "revision", "loop-start", req.Plan.Entities[0].CurrentState, 3, source.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	source.Ref.Generation = activation.Generation()
	req.Plan.WorkflowTimers[0] = source.PersistenceRecord()
	req.Plan.Entities[0].Accumulator = map[string]any{loopruntime.BucketKey: map[string]any{activation.Key(): activation}}
	admitted, err := Admit(req)
	if err != nil {
		t.Fatal(err)
	}
	childID := uuid.NewString()
	projected := projectedTimerAdmissionFixture(t, req, childID)
	wantGeneration, err := loopruntime.ForkGeneration(source.Ref.Generation, childID, source.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	req.Plan.Entities[0].Accumulator[loopruntime.BucketKey] = map[string]any{}
	got, err := admitted.SelectInheritedWorkflowTimer(projected)
	if err != nil || got == nil || got.Ref.Generation != wantGeneration || got.Ref.Generation == source.Ref.Generation {
		t.Fatalf("loop projection lost sealed exact correspondence: %#v, %v", got, err)
	}
	bad := projected.Canonical()
	bad.Ref.Generation = source.Ref.Generation
	if _, err := admitted.SelectInheritedWorkflowTimer(bad); err == nil {
		t.Fatal("source generation substituted for exact child generation")
	}
	if _, err := Admit(req); err == nil {
		t.Fatal("timer generation admitted without recorded loop ownership")
	}
}

func TestAdmissionInheritedTimerSelectionBindsFixedRecordsAndTypedCuts(t *testing.T) {
	for _, kind := range []forkpoint.Kind{forkpoint.Event, forkpoint.RunStart, forkpoint.DeploymentRevision} {
		t.Run(string(kind), func(t *testing.T) {
			req := inheritedTimerAdmissionRequest(t, ".", false)
			eventID := req.Plan.ForkPoint.EventID
			req.Plan.ForkPoint = runfork.RunForkPoint{Kind: kind, Revision: 7}
			if kind == forkpoint.Event {
				req.Plan.ForkPoint.EventID = eventID
			}
			source, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(req.Plan.WorkflowTimers[0])
			if err != nil {
				t.Fatal(err)
			}
			source.SourceTimerID, source.ForkedFromRunID = uuid.NewString(), uuid.NewString()
			source.ForkedFromPointKind, source.ForkedFromPointRevision = forkpoint.RunStart, 3
			source.SourceArmedAt, source.ReconstructionOwner = source.CreatedAt.Add(-time.Hour), "run_fork_workflow_timer"
			req.Plan.WorkflowTimers[0] = source.PersistenceRecord()
			admitted, err := Admit(req)
			if err != nil {
				t.Fatal(err)
			}
			projected := projectedTimerAdmissionFixture(t, req, uuid.NewString())
			got, err := admitted.SelectInheritedWorkflowTimer(projected)
			if err != nil || got == nil || got.ForkedFromPointKind != kind || got.ForkedFromPointRevision != 7 || got.ForkedFromEventID != req.Plan.ForkPoint.EventID || !got.SourceArmedAt.Equal(source.SourceArmedAt) {
				t.Fatalf("typed cut = %#v, %v", got, err)
			}
			original := req.Plan.WorkflowTimers[0]
			for _, change := range []string{"payload", "due", "owner", "omitted", "duplicate"} {
				t.Run(change, func(t *testing.T) {
					binding := req.Binding
					binding.Plan.WorkflowTimers = append(binding.Plan.WorkflowTimers[:0:0], original)
					switch change {
					case "payload":
						binding.Plan.WorkflowTimers[0].Payload = []byte(`{"value":7.0}`)
					case "due":
						binding.Plan.WorkflowTimers[0].FireAt = original.FireAt.Add(time.Hour)
					case "owner":
						binding.Plan.WorkflowTimers[0].OwnerAgent = "foreign-owner"
					case "omitted":
						binding.Plan.WorkflowTimers = nil
					case "duplicate":
						binding.Plan.WorkflowTimers = append(binding.Plan.WorkflowTimers, original)
					}
					if err := admitted.ValidateAgainst(binding); err == nil {
						t.Fatal("changed fixed timer records retained readiness admission")
					}
				})
			}
		})
	}
}

func TestAdmissionInheritedTimerSelectionRequiresRecordedConstructorAndActiveCut(t *testing.T) {
	for _, change := range []string{"missing_receipt", "foreign_header", "foreign_source", "inactive"} {
		t.Run(change, func(t *testing.T) {
			req := inheritedTimerAdmissionRequest(t, "consumer", false)
			projected := projectedTimerAdmissionFixture(t, req, uuid.NewString())
			switch change {
			case "missing_receipt":
				req.Plan.Entities[0].MaterializationMetadata.InitialMaterialization = nil
			case "foreign_header":
				req.Plan.Entities[0].MaterializationMetadata.FlowConfig = []byte(`{"instance_id":"foreign","flow_path":"consumer/item"}`)
			case "foreign_source":
				req.Plan.WorkflowTimers[0].RunID = uuid.NewString()
			case "inactive":
				req.Plan.WorkflowTimers[0].Status = "cancelled"
			}
			admitted, err := Admit(req)
			if change != "inactive" {
				if err == nil {
					t.Fatal("unrecorded/foreign timer construction admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := admitted.SelectInheritedWorkflowTimer(projected); err == nil {
				t.Fatal("terminal fixed-cut timer rearmed through active projection")
			}
		})
	}
}

func inheritedTimerAdmissionRequest(t *testing.T, flowID string, recurring bool) AdmissionRequest {
	t.Helper()
	req := templateAdmissionRequest(t)
	req.Plan.ForkPoint.Kind = forkpoint.Event
	ids := []string{uuid.NewString(), uuid.NewString()}
	req.Plan.ForkPoint.EventID = ids[1]
	req.Plan = req.Plan.WithHistoricalEvents(7, ids)
	req.SourceModes = make(map[string]executionmode.Mode, len(ids))
	for i := range req.Plan.PendingWork {
		req.Plan.PendingWork[i].EventID = ids[i]
		req.SourceModes[ids[i]] = executionmode.Live
	}
	if flowID == "." {
		root := flowidentity.Stored(req.Source, ".", req.Plan.SourceRunID, req.Plan.SourceRunID, req.Plan.SourceRunID, "")
		graph, found := semanticview.WorkflowStageTopology(req.Source, ".")
		if !found {
			t.Fatal("fixture lacks root lifecycle")
		}
		stage, err := graph.InitialStoredStage()
		if err != nil {
			t.Fatal(err)
		}
		entered := *req.Plan.Entities[0].EnteredStateAt
		contract, _ := entityruntime.ResolveForFlow(req.Source, ".")
		req.Plan.Entities = append(req.Plan.Entities, runfork.RunForkEntityState{
			EntityID: root.EntityID, CurrentState: stage.ID(), EnteredStateAt: &entered,
			MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
				Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
				FlowTemplate: ".", FlowInstance: root.InstancePath, Mode: "static", EntityType: contract.EntityType,
				FlowConfig: selectedAgentHeaderConfig(t, root), InitialMaterialization: selectedConstructionReceipt(t, req.Source, req.Plan.SourceRunID, root, nil, entered),
			},
		})
	}
	entity := req.Plan.Entities[0]
	if flowID == "." {
		entity = req.Plan.Entities[len(req.Plan.Entities)-1]
	}
	var route flowidentity.Route
	if flowID == "." {
		route = flowidentity.Route{ScopeKey: ".", InstanceID: req.Plan.SourceRunID, InstancePath: req.Plan.SourceRunID}
	} else {
		route = flowidentity.Route{ScopeKey: "consumer", InstanceID: "item", InstancePath: "consumer/item"}
	}
	armed := entity.EnteredStateAt.UTC().Truncate(time.Microsecond)
	routing, err := events.NewRootRoutingSource(entity.EntityID)
	if flowID != "." {
		routing, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: route.InstancePath, EntityID: entity.EntityID})
	}
	if err != nil {
		t.Fatal(err)
	}
	timer := pipeline.WorkflowTimerActivation{
		Ref:   timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "stage:" + flowID + ":timeout", DeclarationRevision: "original-declaration-revision", Cause: timeridentity.WorkflowTimerActivationCauseEvent},
		RunID: req.Plan.SourceRunID, EntityID: entity.EntityID, Route: route, RoutingSource: routing,
		OwnerAgent: "original-timer-owner", EventType: "timer.original", ExecutionMode: executionmode.Live,
		Payload: []byte(`{"value":7}`), FireAt: armed.Add(time.Hour), CreatedAt: armed, Status: "active", Recurring: recurring,
	}
	if flowID != "." {
		timer.EventType = flowID + "/timer.original"
	}
	if recurring {
		timer.RecurrenceInterval = time.Hour
	}
	if err := timer.Validate(); err != nil {
		t.Fatal(err)
	}
	req.Plan.WorkflowTimers = []pipeline.WorkflowTimerActivationPersistenceRecord{timer.PersistenceRecord()}
	bundle, _ := semanticview.Bundle(req.Source)
	bundle.Semantics.Timers = append(bundle.Semantics.Timers, contracts.WorkflowTimerContract{
		ID: "timeout", FlowID: flowID, StageOwned: true, Stage: entity.CurrentState,
		Owner: "selected-timer-owner", Event: "timer.selected", Delay: "2h", Recurring: recurring,
	})
	view := bundle.FlowTree.ByID[flowID]
	if view.Events == nil {
		view.Events = make(map[string]contracts.EventCatalogEntry)
	}
	view.Events["timer.selected"] = contracts.EventCatalogEntry{}
	refreshTimerAdmissionSource(t, &req)
	return req
}

func refreshTimerAdmissionSource(t *testing.T, req *AdmissionRequest) {
	t.Helper()
	bundle, found := semanticview.Bundle(req.Source)
	if !found {
		t.Fatal("fixture source is not compiled")
	}
	hash, err := contracts.BundleHash(bundle)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := correlation.NewSourceArtifactFact(hash)
	if err != nil {
		t.Fatal(err)
	}
	effective, err := runtimecore.AdmitEffectiveSourceProjection(runtimecore.EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	req.Source, req.SourceArtifactFact, req.EffectiveSourceIdentity = effective.Source(), fact, effective.Identity()
	req.FrontierAdmission, err = runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan: req.Plan, Source: req.Source, ContractSelection: req.ContractSelection,
	})
	if err != nil {
		t.Fatal(err)
	}
	req.RecipientPlanning = runfork.RunForkSelectedContractRecipientPlanning{Owner: runfork.RunForkSelectedContractRecipientPlanningOwner}
	for _, event := range req.FrontierAdmission.FrontierEvents {
		req.RecipientPlanning.RecipientPlanEvents = append(req.RecipientPlanning.RecipientPlanEvents, runfork.RunForkSelectedContractRecipientPlanEvent{
			SourceEventID: event.SourceEventID, EventName: event.EventName, Recipients: event.DerivedRecipients,
		})
	}
}

func projectedTimerAdmissionFixture(t *testing.T, req AdmissionRequest, childRunID string) pipeline.WorkflowTimerActivation {
	t.Helper()
	source, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(req.Plan.WorkflowTimers[0])
	if err != nil {
		t.Fatal(err)
	}
	constructed, _, found, err := runforkadmission.FixedConstructionForRoute(req.Source, req.Plan, source.Route)
	if err != nil || !found {
		t.Fatalf("fixture construction: %v, found=%t", err, found)
	}
	childIdentity, err := runfork.ProjectConstructionIdentity(source.RunID, childRunID, constructed)
	if err != nil {
		t.Fatal(err)
	}
	child := source.Canonical()
	child.RunID, child.EntityID, child.Route = childRunID, childIdentity.EntityID, childIdentity.Route()
	child.RoutingSource, err = runfork.ProjectProducerOwnership(source.RunID, childRunID, source.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	if source.Ref.Generation.Valid() {
		child.Ref.Generation, err = loopruntime.ForkGeneration(source.Ref.Generation, childRunID, child.EntityID)
		if err != nil {
			t.Fatal(err)
		}
	}
	child.Ref.ActivationID = timeridentity.WorkflowTimerActivationID("fork", source.RunID, source.Ref.ActivationID, childRunID,
		string(req.Plan.ForkPoint.Kind), strconv.FormatInt(req.Plan.ForkPoint.Revision, 10), req.Plan.ForkPoint.EventID)
	child.CreatedAt, child.FiredAt = source.CreatedAt.Add(3*time.Hour), time.Time{}
	child.SourceTimerID, child.ForkedFromRunID = source.Ref.ActivationID, source.RunID
	child.ForkedFromPointKind, child.ForkedFromPointRevision, child.ForkedFromEventID = req.Plan.ForkPoint.Kind, req.Plan.ForkPoint.Revision, req.Plan.ForkPoint.EventID
	child.SourceArmedAt, child.ReconstructionOwner = source.CreatedAt, "run_fork_workflow_timer"
	if !source.SourceArmedAt.IsZero() {
		child.SourceArmedAt = source.SourceArmedAt
	}
	child = child.Canonical()
	if err := child.Validate(); err != nil {
		t.Fatal(err)
	}
	return child
}
