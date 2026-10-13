package pipeline

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestSelectInheritedWorkflowTimerRetainsArmedFactsAndSelectedEffects(t *testing.T) {
	for _, flowID := range []string{".", "child"} {
		for _, recurring := range []bool{false, true} {
			for _, cause := range []timeridentity.WorkflowTimerActivationCause{
				timeridentity.WorkflowTimerActivationCauseInitial,
				timeridentity.WorkflowTimerActivationCauseEvent,
				timeridentity.WorkflowTimerActivationCauseTransition,
			} {
				t.Run(fmt.Sprintf("%s/recurring=%t/%s", flowID, recurring, cause), func(t *testing.T) {
					bundle, inherited, header := inheritedWorkflowTimerSelectionFixture(t, flowID, recurring)
					inherited.Ref.Cause = cause
					inherited.Ref.Generation = attemptgeneration.Generation{
						FlowID: flowID, LoopID: "loop", ActivationID: "loop-activation",
						RevisionField: "revision", RevisionID: "revision-1", Attempt: 2,
					}
					before := inherited.Canonical()
					bundle.Semantics.Timers[0].Owner = "selected-owner"
					bundle.Semantics.Timers[0].Event = "timer.selected"
					selected := semanticview.Wrap(bundle)
					declarations := append([]runtimecontracts.WorkflowTimerContract(nil), selected.WorkflowTimers()...)
					got, err := SelectInheritedWorkflowTimer(selected, inherited, header)
					if err != nil || got == nil {
						t.Fatalf("selection = %#v, %v", got, err)
					}
					expected := before.Canonical()
					expected.Ref.DeclarationRevision, err = workflowTimerDeclarationRevision(selected, declarations[0])
					if err != nil {
						t.Fatal(err)
					}
					expected.OwnerAgent = "selected-owner"
					expected.EventType = "timer.selected"
					if flowID != "." {
						expected.EventType = flowID + "/" + expected.EventType
					}
					if !reflect.DeepEqual(*got, expected) || got.Ref.DeclarationRevision == inherited.Ref.DeclarationRevision {
						t.Fatalf("selection changed retained facts or lost selected effects:\n got %#v\nwant %#v", *got, expected)
					}
					if !reflect.DeepEqual(inherited, before) || !reflect.DeepEqual(selected.WorkflowTimers(), declarations) {
						t.Fatal("selection mutated its inherited activation or selected declarations")
					}
					got.Payload[0] = ' '
					if !reflect.DeepEqual(inherited.Payload, before.Payload) {
						t.Fatal("selected payload aliases the inherited activation")
					}
				})
			}
		}
	}
}

func TestSelectInheritedWorkflowTimerChangedDelayDoesNotRearm(t *testing.T) {
	for _, recurring := range []bool{false, true} {
		t.Run(fmt.Sprintf("recurring=%t", recurring), func(t *testing.T) {
			bundle, inherited, header := inheritedWorkflowTimerSelectionFixture(t, ".", recurring)
			bundle.Semantics.Timers[0].Delay = "2h"
			got, err := SelectInheritedWorkflowTimer(semanticview.Wrap(bundle), inherited, header)
			if err != nil || got == nil {
				t.Fatalf("selection = %#v, %v", got, err)
			}
			if got.Ref.DeclarationRevision == inherited.Ref.DeclarationRevision {
				t.Fatal("changed delay did not update the canonical declaration revision")
			}
			expected := inherited.Canonical()
			expected.Ref.DeclarationRevision = got.Ref.DeclarationRevision
			if !reflect.DeepEqual(*got, expected) {
				t.Fatalf("changed delay rearmed or changed the occurrence lattice:\n got %#v\nwant %#v", *got, expected)
			}
			if !got.FireAt.Before(got.CreatedAt) || !got.SourceArmedAt.Equal(inherited.SourceArmedAt) {
				t.Fatal("selection reset the overdue inherited clock to child birth")
			}
		})
	}
}

func TestInheritedInitialEntryTimerReusesExactArm(t *testing.T) {
	for _, flowID := range []string{".", "child"} {
		t.Run(flowID, func(t *testing.T) {
			bundle, inherited, header := inheritedWorkflowTimerSelectionFixture(t, flowID, false)
			bundle.Semantics.Timers[0].Delay = "2h"
			source := semanticview.Wrap(bundle)
			selected, err := SelectInheritedWorkflowTimer(source, inherited, header)
			if err != nil || selected == nil {
				t.Fatalf("select fixed-cut arm: %v", err)
			}
			want := selected.Canonical()
			for i := 0; i < 2; i++ {
				got, found, err := inheritedInitialEntryTimer(source, header, []WorkflowTimerActivation{want}, want.Ref.DeclarationKey, want.Ref.Generation)
				if err != nil || !found || !reflect.DeepEqual(got, want) {
					t.Fatalf("initial entry reused or rearmed incorrectly: got=%+v found=%t err=%v", got, found, err)
				}
			}
			if _, _, err := inheritedInitialEntryTimer(source, header, []WorkflowTimerActivation{want, want}, want.Ref.DeclarationKey, want.Ref.Generation); err == nil {
				t.Fatal("duplicate inherited arms silently selected a winner")
			}
			if _, _, err := inheritedInitialEntryTimer(source, header, []WorkflowTimerActivation{inherited}, want.Ref.DeclarationKey, want.Ref.Generation); err == nil {
				t.Fatal("unmaterialized selected declaration revision was accepted")
			}
			ordinary := want.Canonical()
			ordinary.SourceTimerID = ""
			if _, found, err := inheritedInitialEntryTimer(source, header, []WorkflowTimerActivation{ordinary}, want.Ref.DeclarationKey, want.Ref.Generation); err != nil || found {
				t.Fatalf("ordinary arm borrowed inherited selection: found=%t err=%v", found, err)
			}
		})
	}
}

func TestSelectInheritedWorkflowTimerRemovedDeclaration(t *testing.T) {
	for _, flowID := range []string{".", "child"} {
		t.Run(flowID, func(t *testing.T) {
			bundle, inherited, header := inheritedWorkflowTimerSelectionFixture(t, flowID, false)
			bundle.Semantics.Timers = nil
			got, err := SelectInheritedWorkflowTimer(semanticview.Wrap(bundle), inherited, header)
			if err != nil || got != nil {
				t.Fatalf("removed declaration selection = %#v, %v, want nil without error", got, err)
			}
		})
	}
}

func TestSelectInheritedWorkflowTimerRefusesMismatchedOwnerAndSource(t *testing.T) {
	for _, removed := range []bool{false, true} {
		for _, test := range []struct {
			name   string
			change func(*testing.T, *semanticview.Source, *WorkflowTimerActivation, *WorkflowInstance)
		}{
			{"nil_source", func(_ *testing.T, s *semanticview.Source, _ *WorkflowTimerActivation, _ *WorkflowInstance) { *s = nil }},
			{"source_missing_owner", func(t *testing.T, s *semanticview.Source, _ *WorkflowTimerActivation, _ *WorkflowInstance) {
				bundle, found := semanticview.Bundle(*s)
				if !found {
					t.Fatal("missing fixture source")
				}
				delete(bundle.FlowTree.ByID, "child")
			}},
			{"source_changed_scope", func(t *testing.T, s *semanticview.Source, _ *WorkflowTimerActivation, _ *WorkflowInstance) {
				bundle, found := semanticview.Bundle(*s)
				if !found {
					t.Fatal("missing fixture source")
				}
				bundle.FlowTree.ByID["child"].Path = "other"
			}},
			{"unknown_template", func(_ *testing.T, _ *semanticview.Source, _ *WorkflowTimerActivation, h *WorkflowInstance) {
				h.WorkflowName = "other"
			}},
			{"foreign_entity", func(_ *testing.T, _ *semanticview.Source, _ *WorkflowTimerActivation, h *WorkflowInstance) {
				h.EntityID = "foreign-entity"
			}},
			{"foreign_path", func(_ *testing.T, _ *semanticview.Source, _ *WorkflowTimerActivation, h *WorkflowInstance) {
				h.StorageRef = "other"
			}},
			{"foreign_instance", func(_ *testing.T, _ *semanticview.Source, _ *WorkflowTimerActivation, h *WorkflowInstance) {
				h.InstanceID = "other"
			}},
			{"missing_parent", func(_ *testing.T, _ *semanticview.Source, _ *WorkflowTimerActivation, h *WorkflowInstance) {
				h.ParentFlowInstance = ""
			}},
			{"foreign_parent_run", func(_ *testing.T, _ *semanticview.Source, _ *WorkflowTimerActivation, h *WorkflowInstance) {
				h.ParentFlowInstance = "99999999-9999-4999-8999-999999999999"
			}},
			{"foreign_run", func(_ *testing.T, _ *semanticview.Source, a *WorkflowTimerActivation, _ *WorkflowInstance) {
				a.RunID = "99999999-9999-4999-8999-999999999999"
			}},
			{"foreign_scope", func(_ *testing.T, _ *semanticview.Source, a *WorkflowTimerActivation, _ *WorkflowInstance) {
				a.Route.ScopeKey = "other"
			}},
			{"noncanonical_header", func(_ *testing.T, _ *semanticview.Source, _ *WorkflowTimerActivation, h *WorkflowInstance) {
				h.WorkflowName += " "
			}},
			{"inactive", func(_ *testing.T, _ *semanticview.Source, a *WorkflowTimerActivation, _ *WorkflowInstance) {
				a.Status = workflowTimerStatusCancelled
			}},
			{"invalid_arm", func(_ *testing.T, _ *semanticview.Source, a *WorkflowTimerActivation, _ *WorkflowInstance) {
				a.SourceArmedAt = time.Time{}
			}},
			{"root_provenance_for_child", func(t *testing.T, _ *semanticview.Source, a *WorkflowTimerActivation, _ *WorkflowInstance) {
				var err error
				a.RoutingSource, err = events.NewRootRoutingSource(a.EntityID)
				if err != nil {
					t.Fatal(err)
				}
				a.EventType = "timer.elapsed"
			}},
			{"foreign_flow_provenance", func(t *testing.T, _ *semanticview.Source, a *WorkflowTimerActivation, _ *WorkflowInstance) {
				var err error
				a.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{
					FlowID: "other", FlowInstance: a.Route.InstancePath, EntityID: a.EntityID,
				})
				if err != nil {
					t.Fatal(err)
				}
				a.EventType = "other/timer.elapsed"
			}},
		} {
			t.Run(fmt.Sprintf("removed=%t/%s", removed, test.name), func(t *testing.T) {
				bundle, inherited, header := inheritedWorkflowTimerSelectionFixture(t, "child", false)
				if removed {
					bundle.Semantics.Timers = nil
				}
				selected := semanticview.Wrap(bundle)
				test.change(t, &selected, &inherited, &header)
				if got, err := SelectInheritedWorkflowTimer(selected, inherited, header); err == nil || got != nil {
					t.Fatalf("invalid owner/source selection = %#v, %v", got, err)
				}
			})
		}
	}
}

func TestSelectInheritedWorkflowTimerUsesCanonicalTopologyAndEventAdmission(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*runtimecontracts.WorkflowContractBundle)
	}{
		{"recurring_bounded_loop", func(b *runtimecontracts.WorkflowContractBundle) {
			b.Semantics.Loops = []runtimecontracts.WorkflowLoopPlan{{FlowID: "child", ID: "loop", RegionStages: []string{"waiting"}}}
		}},
		{"foreign_event_scope", func(b *runtimecontracts.WorkflowContractBundle) { b.Semantics.Timers[0].Event = "other/timer.elapsed" }},
		{"missing_selected_owner", func(b *runtimecontracts.WorkflowContractBundle) { b.Semantics.Timers[0].Owner = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle, inherited, header := inheritedWorkflowTimerSelectionFixture(t, "child", true)
			test.change(bundle)
			if got, err := SelectInheritedWorkflowTimer(semanticview.Wrap(bundle), inherited, header); err == nil || got != nil {
				t.Fatalf("invalid selected declaration = %#v, %v", got, err)
			}
		})
	}
}

func inheritedWorkflowTimerSelectionFixture(
	t *testing.T,
	flowID string,
	recurring bool,
) (*runtimecontracts.WorkflowContractBundle, WorkflowTimerActivation, WorkflowInstance) {
	t.Helper()
	root := &runtimecontracts.FlowContractView{
		Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Schema: runtimecontracts.FlowSchemaDocument{Name: "root"},
		Children: []runtimecontracts.FlowContractView{{
			Path: "child", Paths: runtimecontracts.FlowContractPaths{FlowPath: "child"},
			Schema: runtimecontracts.FlowSchemaDocument{Name: "child"},
			Events: map[string]runtimecontracts.EventCatalogEntry{"timer.elapsed": {}, "timer.selected": {}},
		}},
	}
	child := &root.Children[0]
	child.Parent = root
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &root.Schema,
		FlowTree: runtimecontracts.FlowTree{
			Root: root, ByID: map[string]*runtimecontracts.FlowContractView{".": root, "child": child},
			ByPath: map[string]*runtimecontracts.FlowContractView{".": root, "child": child},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{".": root.Schema, "child": child.Schema},
		Semantics: runtimecontracts.WorkflowSemanticView{Timers: []runtimecontracts.WorkflowTimerContract{{
			ID: "timeout", FlowID: flowID, Stage: "waiting", StageOwned: true,
			Owner: "source-owner", Event: "timer.elapsed", Delay: "1h", Recurring: recurring,
		}}},
	}
	source := semanticview.Wrap(bundle)
	inherited := inheritedWorkflowTimerForTest(t)
	constructed := runtimeflowidentity.Stored(source, ".", inherited.RunID, inherited.RunID, runtimeflowidentity.EntityID(inherited.RunID), "")
	if flowID != "." {
		var err error
		constructed, err = runtimeflowidentity.KeylessChild(source, constructed, flowID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := constructed.ValidateConstruction(source, inherited.RunID); err != nil {
		t.Fatal(err)
	}
	inherited.EntityID, inherited.Route = constructed.EntityID, constructed.Route()
	declaration := bundle.Semantics.Timers[0]
	inherited.Ref.DeclarationKey = declaration.SemanticKey()
	var err error
	inherited.Ref.DeclarationRevision, err = workflowTimerDeclarationRevision(source, declaration)
	if err != nil {
		t.Fatal(err)
	}
	inherited.RoutingSource, _, err = workflowTimerDeclarationSourceEvent(source, inherited.EntityID, inherited.Route.InstancePath, declaration)
	if err != nil {
		t.Fatal(err)
	}
	inherited.OwnerAgent, inherited.EventType = declaration.Owner, declaration.Event
	if flowID != "." {
		inherited.EventType = flowID + "/" + inherited.EventType
	}
	inherited.Payload = []byte(`{"retained": true, "value": 7}`)
	inherited.Recurring = recurring
	if recurring {
		inherited.RecurrenceInterval = time.Hour
	}
	inherited = inherited.Canonical()
	if err := inherited.Validate(); err != nil {
		t.Fatal(err)
	}
	header := WorkflowInstance{
		InstanceID: constructed.InstanceID, StorageRef: constructed.InstancePath, EntityID: constructed.EntityID,
		WorkflowName: constructed.TemplateID, ParentFlowID: constructed.ParentRoute.FlowID,
		ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
		CurrentState: "done",
	}
	return bundle, inherited, header
}
