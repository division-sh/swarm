package runforkreadiness

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimecore "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

func TestAdmissionSealsCompleteProjectionAndResolvedAgentAuthority(t *testing.T) {
	for _, change := range []string{"omitted_state", "omitted_association", "wrong_instance_key", "omitted_agent", "different_valid_revision", "different_plan", "blueprint_config", "flow_config", "original_source", "original_modes", "original_metadata", "original_model_aliases"} {
		t.Run(change, func(t *testing.T) {
			req := templateAdmissionRequest(t)
			admitted, err := Admit(req)
			if err != nil {
				t.Fatal(err)
			}
			before, err := admitted.Projection()
			if err != nil {
				t.Fatal(err)
			}
			if len(before.States) != 1 || len(before.States[0].Agents) == 0 || len(before.States[0].SourceEvents) != 2 {
				t.Fatalf("fixture lacks complete template authority: %+v", before.States)
			}
			copy, err := admitted.Projection()
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "omitted_state":
				copy.States = nil
			case "omitted_association":
				copy.States[0].SourceEvents = copy.States[0].SourceEvents[:1]
			case "wrong_instance_key":
				copy.States[0].Route.InstanceID = "foreign"
			case "omitted_agent":
				copy.States[0].Agents = nil
			case "different_valid_revision":
				copy.States[0].Agents[0].ConfigRevision = strings.Repeat("b", 64)
			case "different_plan":
				copy.States[0].Agents[0].Plan.Route.InstancePath = "consumer/foreign"
			case "blueprint_config":
				copy.Blueprints[0].Config.Model = "foreign"
			case "flow_config":
				copy.Flows[0].ActivationVariables["instance_id"] = "foreign"
			case "original_source":
				bundle, _ := semanticview.Bundle(req.Source)
				bundle.Platform = contracts.PlatformSpecDocument{}
			case "original_modes":
				req.SourceModes["event-b"] = executionmode.Mock
			case "original_metadata":
				req.Plan.Entities[0].MaterializationMetadata.EntityType = "foreign"
			case "original_model_aliases":
				req.ModelOptions.ModelAliases["regular"]["anthropic"] = "foreign"
			}
			after, err := admitted.Projection()
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("admitted projection mutated through %s: %v", change, err)
			}
		})
	}
}

func Test2376AdmissionSealsCompiledExecutionIdentity(t *testing.T) {
	req := templateAdmissionRequest(t)
	bundle, _ := semanticview.Bundle(req.Source)
	want, err := contracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := Admit(req)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Semantics.Version = "parent-or-distribution-version"
	identity, err := admitted.ExecutionIdentity()
	if err != nil || identity != want {
		t.Fatalf("admitted execution pins changed through source mutation: %+v %v", identity, err)
	}
	identity.WorkflowVersion = "caller-selected"
	again, err := admitted.ExecutionIdentity()
	if err != nil || again != want {
		t.Fatal("returned identity mutated the sealed owner")
	}
	if _, err := (Admission{}).ExecutionIdentity(); err == nil {
		t.Fatal("zero admission elected execution pins")
	}
}

func TestAdmissionRejectsIncompleteOrForeignBindings(t *testing.T) {
	for _, change := range []string{"zero", "foreign_run", "foreign_revision", "foreign_entity", "foreign_artifact", "foreign_effective_source", "missing_event", "missing_recipient", "duplicate_event", "changed_mode"} {
		t.Run(change, func(t *testing.T) {
			req := templateAdmissionRequest(t)
			admitted, err := Admit(req)
			if err != nil {
				t.Fatal(err)
			}
			if err := admitted.ValidateAgainst(req.Binding); err != nil {
				t.Fatalf("exact binding: %v", err)
			}
			switch change {
			case "zero":
				admitted = Admission{}
			case "foreign_run":
				req.Plan.SourceRunID = uuid.NewString()
			case "foreign_revision":
				req.Plan.ForkPoint.Revision++
			case "foreign_entity":
				req.Plan.Entities[0].EntityID = uuid.NewString()
			case "foreign_artifact":
				req.SourceArtifactFact, err = correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("f", 64))
			case "foreign_effective_source":
				req.EffectiveSourceIdentity = Binding{}.EffectiveSourceIdentity
			case "missing_event":
				req.RecipientPlanning.RecipientPlanEvents = req.RecipientPlanning.RecipientPlanEvents[:1]
			case "missing_recipient":
				req.RecipientPlanning.RecipientPlanEvents[0].Recipients = nil
			case "duplicate_event":
				req.RecipientPlanning.RecipientPlanEvents[1] = req.RecipientPlanning.RecipientPlanEvents[0]
			case "changed_mode":
				req.SourceModes["event-b"] = executionmode.Mock
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := admitted.ValidateAgainst(req.Binding); err == nil {
				t.Fatal("tampered binding was admitted")
			}
			if change == "missing_event" || change == "missing_recipient" || change == "duplicate_event" || change == "foreign_artifact" || change == "foreign_effective_source" {
				if _, err := Admit(req); err == nil {
					t.Fatal("constructor sealed incomplete or foreign authority")
				}
			}
		})
	}
}

func TestAdmissionPreservesLoaderAdmittedEffectiveSourceContext(t *testing.T) {
	req := templateAdmissionRequest(t)
	bundle, _ := semanticview.Bundle(req.Source)
	packs, err := packadmission.FromBundle(bundle)
	if err != nil || len(packs.ChannelPlans) == 0 {
		t.Fatalf("fixture requires actual admitted channel context: %v", err)
	}
	profiled, err := runtimecore.AdmitEffectiveSourceProjection(runtimecore.EffectiveSourceProjectionRequest{
		Source: req.Source, SourceArtifactFact: req.SourceArtifactFact, ChannelPlans: packs.ChannelPlans,
	})
	if err != nil {
		t.Fatal(err)
	}
	unprofiled := req.EffectiveSourceIdentity
	if profiled.Identity().Equal(unprofiled) {
		t.Fatal("fixture did not vary effective source context")
	}
	for _, name := range []string{"unprofiled", "profiled"} {
		t.Run(name, func(t *testing.T) {
			candidate := req
			other := profiled.Identity()
			if name == "profiled" {
				candidate.Source = profiled.Source()
				candidate.EffectiveSourceIdentity = profiled.Identity()
				other = unprofiled
			}
			admitted, err := Admit(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if err := admitted.ValidateAgainst(candidate.Binding); err != nil {
				t.Fatalf("original loader identity was not retained: %v", err)
			}
			candidate.EffectiveSourceIdentity = other
			if err := admitted.ValidateAgainst(candidate.Binding); err == nil {
				t.Fatal("different valid effective source context accepted at consumption")
			}
		})
	}
}

func TestAdmissionRequiresPositiveSealForEmptyProjection(t *testing.T) {
	if _, err := Admit(AdmissionRequest{}); err == nil {
		t.Fatal("empty request admitted")
	}
	req := templateAdmissionRequest(t)
	req.Plan.PendingWork = nil
	req.Plan.Entities = nil
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: req.Plan, Source: req.Source, ContractSelection: req.ContractSelection})
	if err != nil {
		t.Fatal(err)
	}
	req.FrontierAdmission = frontier
	req.RecipientPlanning.RecipientPlanEvents = nil
	req.SourceModes = nil
	admitted, err := Admit(req)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := admitted.Projection()
	if err != nil || len(projection.States) != 0 || len(projection.Blueprints) != 0 || len(projection.Flows) != 0 {
		t.Fatalf("lawful empty projection: %+v, %v", projection, err)
	}
	if err := admitted.ValidateAgainst(req.Binding); err != nil {
		t.Fatal(err)
	}
	if err := (Admission{}).ValidateAgainst(req.Binding); err == nil {
		t.Fatal("zero admission substituted for positive empty seal")
	}
}

func TestSelectedReadinessRequiresCompleteConstruction(t *testing.T) {
	for _, change := range []string{"exact", "imported_state", "missing_config", "malformed_config", "foreign_route_config", "unknown_config_control", "missing_stage", "missing_entry_clock", "unknown_mode", "wrong_mode", "missing_flow_template", "wrong_flow_template"} {
		t.Run(change, func(t *testing.T) {
			req := templateAdmissionRequest(t)
			entity := &req.Plan.Entities[0]
			metadata := entity.MaterializationMetadata
			switch change {
			case "imported_state":
				metadata.Source = runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState
			case "missing_config":
				metadata.FlowConfig = nil
			case "malformed_config":
				metadata.FlowConfig = []byte(`{}`)
			case "foreign_route_config":
				metadata.FlowConfig = []byte(strings.ReplaceAll(string(metadata.FlowConfig), "consumer/item", "consumer/foreign"))
			case "unknown_config_control":
				metadata.FlowConfig = append([]byte(`{"unknown_runtime_control":true,`), metadata.FlowConfig[1:]...)
			case "missing_stage":
				entity.CurrentState = ""
			case "missing_entry_clock":
				entity.EnteredStateAt = nil
			case "unknown_mode":
				metadata.Mode = "unknown"
			case "wrong_mode":
				metadata.Mode = "static"
			case "missing_flow_template":
				metadata.FlowTemplate = ""
			case "wrong_flow_template":
				metadata.FlowTemplate = "foreign"
			}
			_, err := Project(req.Plan, req.Source, req.RecipientPlanning, req.SourceModes, req.ModelOptions)
			if (err == nil) != (change == "exact") {
				t.Fatalf("construction admission %s: %v", change, err)
			}
		})
	}
}

func TestAdmissionSealsResolvedModelRevision(t *testing.T) {
	req := templateAdmissionRequest(t)
	first, err := Admit(req)
	if err != nil {
		t.Fatal(err)
	}
	req.ModelOptions.ModelAliases["regular"]["anthropic"] = "another-model"
	second, err := Admit(req)
	if err != nil {
		t.Fatal(err)
	}
	var revisions []string
	for _, admission := range []Admission{first, second} {
		projection, err := admission.Projection()
		if err != nil {
			t.Fatal(err)
		}
		if len(projection.States) != 1 || len(projection.Blueprints) != 1 || len(projection.States[0].Agents) != 1 {
			t.Fatal("fixture lacks exact resolved agent projection")
		}
		blueprint := projection.Blueprints[0]
		revision, err := manager.AgentConfigPlanRevision(blueprint.Config, blueprint.Identity)
		if err != nil || revision != projection.States[0].Agents[0].ConfigRevision {
			t.Fatalf("sealed expectation does not describe actual resolved blueprint: %v", err)
		}
		revisions = append(revisions, revision)
	}
	if revisions[0] == revisions[1] {
		t.Fatal("changed resolved model did not change the admitted agent revision")
	}
}

func TestAdmissionRetainsExactRunlessPromptWithoutPersistingIt(t *testing.T) {
	req := templateAdmissionRequest(t)
	admitted, err := Admit(req)
	if err != nil {
		t.Fatal(err)
	}
	first, err := admitted.Projection()
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Blueprints) != 1 || len(first.Flows) != 1 || len(first.Flows[0].Agents) != 1 {
		t.Fatal("fixture requires the same actor in both prospective projections")
	}
	blueprint := first.Blueprints[0]
	want, err := blueprint.Config.ProviderPrompt(agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}
	wantText, err := want.Text()
	if err != nil || wantText == "" {
		t.Fatalf("missing admitted prompt: %v", err)
	}
	var decoded Projection
	if err := json.Unmarshal(admitted.sealed.projection, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Blueprints[0].Config.Prompt.Empty() || !decoded.Flows[0].Agents[0].Config.Prompt.Empty() {
		t.Fatal("derived runtime prompt leaked into durable JSON")
	}
	first.Blueprints[0].Config.Prompt = agentintent.DerivedPrompt{}
	first.Flows[0].Agents[0].Config.Prompt = agentintent.DerivedPrompt{}
	second, err := admitted.Projection()
	if err != nil {
		t.Fatal(err)
	}
	for _, actor := range []manager.AgentMaterializationBlueprint{second.Blueprints[0], second.Flows[0].Agents[0]} {
		if actor.Identity != blueprint.Identity || !actor.Config.Identity.IsZero() {
			t.Fatal("prompt restoration changed prospective identity")
		}
		got, err := actor.Config.ProviderPrompt(agentintent.RuntimeEnvironmentContext())
		if err != nil {
			t.Fatal(err)
		}
		text, err := got.Text()
		if err != nil || text != wantText {
			t.Fatalf("admitted prompt changed: %v", err)
		}
	}
	delete(admitted.sealed.prompts, blueprint.Identity)
	if _, err := admitted.Projection(); err == nil {
		t.Fatal("missing prompt was reconstructed or silently accepted")
	}
}

func templateAdmissionRequest(t *testing.T) AdmissionRequest {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{Consumer: canonicalrouting.TemplateInstanceAgentConsumer})
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{PlatformPackBase: packfixture.EmbeddedBase(t), AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
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
	runID, entityID := uuid.NewString(), uuid.NewString()
	graph, found := semanticview.WorkflowStageTopology(effective.Source(), "consumer")
	if !found {
		t.Fatal("fixture has no admitted consumer lifecycle")
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	entered := time.Now().UTC()
	selectedRoot := flowidentity.Stored(effective.Source(), ".", runID, runID, flowidentity.EntityID(runID), "")
	constructed, err := flowidentity.KeyedChild(effective.Source(), selectedRoot, "consumer", "item")
	if err != nil {
		t.Fatal(err)
	}
	constructed.EntityID = entityID
	plan := runfork.RunForkPlan{SourceRunID: runID, ForkPoint: runfork.RunForkPoint{EventID: "event-b", Revision: 7}, Entities: []runfork.RunForkEntityState{{
		EntityID: entityID, CurrentState: initial.ID(), EnteredStateAt: &entered, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
			Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
			FlowTemplate: "consumer", Mode: "template", StageDefined: graph.StageCount() != 0,
			EntityType: "deployment", FlowInstance: "consumer/item",
			FlowConfig: selectedAgentHeaderConfig(t, constructed),
		},
	}}}
	plan = plan.WithHistoricalEvents(7, []string{"event-a", "event-b"})
	target, err := events.NewExistingEntityTarget(events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/item", EntityID: entityID})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"event-a", "event-b"} {
		plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{EventID: id, EventName: "consumer/item/deploy.done", Classification: runfork.RunForkPendingClassificationPending,
			RoutingSource: eventtest.ConcreteTemplateRoutingSource("consumer", "consumer/item", entityID), FlowInstance: "consumer/item", DeliveryRoute: events.DeliveryRoute{Target: target}})
	}
	selection := runfork.RunForkContractSelection{Mode: "selected_contracts"}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: effective.Source(), ContractSelection: selection})
	if err != nil {
		t.Fatal(err)
	}
	planning := runfork.RunForkSelectedContractRecipientPlanning{Owner: runfork.RunForkSelectedContractRecipientPlanningOwner}
	for _, event := range frontier.FrontierEvents {
		planning.RecipientPlanEvents = append(planning.RecipientPlanEvents, runfork.RunForkSelectedContractRecipientPlanEvent{SourceEventID: event.SourceEventID, EventName: event.EventName, Recipients: event.DerivedRecipients})
	}
	req := AdmissionRequest{Binding: Binding{Plan: plan, SourceArtifactFact: fact, EffectiveSourceIdentity: effective.Identity(), ContractSelection: selection,
		FrontierAdmission: frontier, RecipientPlanning: planning, SourceModes: map[string]executionmode.Mode{"event-a": executionmode.Live, "event-b": executionmode.Live}}, Source: effective.Source()}
	req.ModelOptions.LLMBackend = "anthropic"
	req.ModelOptions.ExecutionPosture = executionposture.Live
	req.ModelOptions.ModelAliases = map[string]map[string]string{"regular": {"anthropic": "model"}}
	return req
}
