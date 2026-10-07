package tools_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type sparseEntityToolStore interface {
	tools.EntityPersistence
	entityToolImportOwner
	EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
	LoadRunDebugReport(context.Context, string, operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error)
}

type entityToolImportOwner interface {
	SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
}

type entityToolImportFixture struct {
	owner    entityToolImportOwner
	source   semanticview.Source
	selected any
	pipeline *pipeline.PipelineCoordinator
}

type entityToolImportFixtureKey struct{}

type entityToolPipelineModule struct{ source semanticview.Source }

func (m entityToolPipelineModule) SemanticSource() semanticview.Source  { return m.source }
func (entityToolPipelineModule) WorkflowNodes() []pipeline.WorkflowNode { return nil }
func (entityToolPipelineModule) GuardRegistry() pipeline.GuardRegistry  { return nil }

type entityToolPipelineBus struct {
	pipeline.WorkflowDeliveryRuntime
}

func (entityToolPipelineBus) Publish(context.Context, events.Event) error {
	return errors.New("unexpected fixture publication")
}
func (entityToolPipelineBus) PublishDirect(context.Context, events.Event, []string) error {
	return errors.New("unexpected fixture publication")
}
func (entityToolPipelineBus) ResolveSubscribedRecipients(string) []string                { return nil }
func (entityToolPipelineBus) LogRuntime(context.Context, pipeline.RuntimeLogEntry) error { return nil }
func (entityToolPipelineBus) EngineDispatcher() engine.PostCommitDispatcher {
	return entityToolPipelineDispatcher{}
}

type entityToolPipelineDispatcher struct{}

func (entityToolPipelineDispatcher) DispatchPostCommit(context.Context, []engine.EmitIntent) error {
	return errors.New("unexpected fixture dispatch")
}

func newEntityToolPipeline(t *testing.T, selected any, source semanticview.Source) *pipeline.PipelineCoordinator {
	t.Helper()
	bus := entityToolPipelineBus{}
	pc := pipeline.NewPipelineCoordinatorWithOptions(bus, pipeline.PipelineCoordinatorOptions{
		ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
		Module: entityToolPipelineModule{source: source}, Persistence: pipeline.NewWorkflowPersistence(selected.(pipeline.WorkflowPersistenceOwner)),
		DeliveryStore: selected.(deliverylifecycle.Store), DeadLetters: selected.(deadletters.AcknowledgedRecorder),
		PipelineObligations: selected.(interface {
			PipelineObligations() pipelineobligation.Store
		}).PipelineObligations(),
		DecisionCards: selected.(decisioncard.Store), ProposedEffects: selected.(decisioncard.ProposedEffectStore), HumanTasks: selected.(decisioncard.HumanTaskStore),
		DecisionCardDraftExpiry: selected.(pipeline.DecisionCardDraftExpiry), HumanTaskExpiry: selected.(pipeline.HumanTaskExpiry),
		DeliveryRuntime: bus, RunLifecycle: selected.(runlifecycle.OperationOwner),
	})
	if pc == nil {
		t.Fatal("constructed entity fixture requires the selected pipeline")
	}
	return pc
}

func entityToolFixtureActor(t *testing.T, source semanticview.Source, actor models.AgentConfig, runID, flowID, flowInstance string) models.AgentConfig {
	t.Helper()
	actor.FlowID = flowID
	declaration, ok := semanticview.ResolveAgentDeclaration(source, actor)
	if !ok {
		t.Fatalf("fixture actor %s requires its exact declaration in %s", actor.ID, flowID)
	}
	plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
	if err != nil {
		t.Fatal(err)
	}
	name, err := plan.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	route := agentidentity.RootRoute()
	if flowID != "." {
		route, err = agentidentity.PresentRoute(flowID, flowidentity.LogicalInstanceID(flowInstance), flowInstance)
		if err != nil {
			t.Fatal(err)
		}
	}
	actor.Identity, err = agentidentity.New(runID, name, route)
	if err != nil {
		t.Fatal(err)
	}
	actor.ID, actor.FlowPath = name.AgentID, actor.Identity.FlowInstance()
	return actor
}

func withEntityToolFixtureRoute(t *testing.T, ctx context.Context, flowInstance string) context.Context {
	t.Helper()
	fixture := ctx.Value(entityToolImportFixtureKey{}).(entityToolImportFixture)
	actor, ok := tools.ActorFromContext(ctx)
	if !ok {
		t.Fatal("fixture route requires actor context")
	}
	actor = entityToolFixtureActor(t, fixture.source, actor, correlation.RunIDFromContext(ctx), actor.FlowID, flowInstance)
	return tools.WithActor(ctx, actor)
}

func constructEntityToolFixture(t *testing.T, ctx context.Context, selected any, pc *pipeline.PipelineCoordinator, source semanticview.Source, flowID, flowInstance, entityID, stage string, fields map[string]any, at time.Time) flowidentity.RunScopedFlowInstance {
	t.Helper()
	owner := flowidentity.RunScopedFlowInstance{RunID: correlation.RunIDFromContext(ctx), Route: flowidentity.Route{ScopeKey: flowID, InstanceID: flowidentity.LogicalInstanceID(flowInstance), InstancePath: flowInstance}}
	ctx = effects.WithExecutionMode(ctx, effects.ExecutionModeLive)
	initial, lifecycle, err := pc.PrepareInitialEntryLifecycle(ctx, owner, pipeline.WorkflowInstance{
		WorkflowName: flowID, WorkflowVersion: source.WorkflowVersion(), StorageRef: flowInstance, InstanceID: owner.Route.InstanceID, EntityID: entityID,
		EntityType:   func() string { contract, _ := entityruntime.ResolveForFlow(source, flowID); return contract.EntityType }(),
		CurrentState: stage, StageDefined: true, Fields: fields, CreatedAt: at, EnteredStageAt: at,
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	command, err := flowactivationfixture.Command(ctx, initial, lifecycle, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged {
		t.Fatalf("construct entity tool fixture: acknowledged=%v err=%v", committed.Acknowledged, err)
	}
	if committed.Created {
		if err := pc.FinalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
			t.Fatal(err)
		}
	}
	return owner
}

func seedEntityToolSourceRun(t *testing.T, selected any, bundle *contracts.WorkflowContractBundle) context.Context {
	t.Helper()
	if bundle == nil || bundle.SourceArtifact == nil {
		t.Fatal("entity persistence fixture requires loaded source")
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	ctx := authoractivity.WithScope(context.Background(), authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
	ctx = effects.WithDifferentOwner(ctx, effects.OwnerBuildTestInfrastructure)
	ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, entityToolTestRunID), fact)
	fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: entityToolTestRunID, Artifact: bundle.SourceArtifact}
	switch selected.(type) {
	case *store.PostgresStore:
		runlifecyclefixture.RequirePostgres(t, ctx, storetest.DatabaseForTest(selected), fixture)
	case *store.SQLiteRuntimeStore:
		runlifecyclefixture.RequireSQLite(t, ctx, storetest.DatabaseForTest(selected), fixture)
	default:
		t.Fatalf("unsupported entity test backend %T", selected)
	}
	owner, ok := selected.(entityToolImportOwner)
	if !ok {
		t.Fatalf("entity test backend %T has no scenario import owner", selected)
	}
	source := semanticview.Wrap(bundle)
	return context.WithValue(ctx, entityToolImportFixtureKey{}, entityToolImportFixture{owner: owner, source: source, selected: selected, pipeline: newEntityToolPipeline(t, selected, source)})
}

func TestEntitySparseGeneratedToolMutation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			actor := models.AgentConfig{ExecutionMode: "live", ID: "writer", Role: "writer"}
			bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
				"work": {
					SchemaYAML: "name: work\ninstance: fixture_key\nstages:\n  queued: {}\n  done: {final: true}\n",
					TypesYAML:  "types:\n  Profile:\n    name: text\n    note: text?\n",
					EntitiesYAML: `
work:
  label: text?
  profile: Profile?
  left: text?
  right:
    type: text?
    equal_to: left
  token:
    type: text
    immutable: true
  seeded:
    type: integer
    initial: 7
`,
					AgentsYAML: `
writer:
  role: writer
  intent: {inline: "Maintain accurate work records."}
  entity_writes:
    work:
      save: [label, profile, left, right, token]
`,
				},
			})
			var persistence sparseEntityToolStore
			var db *sql.DB
			if backend == "sqlite" {
				s := newSQLiteRuntimeToolStoreForTest(t)
				persistence, db = s, storetest.DatabaseForTest(s)
			} else {
				s := newPostgresHumanTaskToolStoreForTest(t)
				persistence, db = s, storetest.DatabaseForTest(s)
			}
			runID, entityID := uuid.NewString(), uuid.NewString()
			fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			ctx := authoractivity.WithScope(context.Background(), authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
			ctx = effects.WithDifferentOwner(ctx, effects.OwnerBuildTestInfrastructure)
			ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), fact)
			if _, err := persistence.EnsureSourceArtifact(ctx, bundle.SourceArtifact); err != nil {
				t.Fatal(err)
			}
			fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, BundleHash: fact.BundleHash()}
			if backend == "sqlite" {
				runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
			} else {
				runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
			}
			source := semanticview.Wrap(bundle)
			if problems := tools.ValidateGeneratedToolSchemaClosureForSource(source); len(problems) != 0 {
				t.Fatalf("source-loaded sparse entity tool schemas rejected: %v", problems)
			}
			contract, ok := entityruntime.ResolveForFlow(source, "work")
			if !ok {
				t.Fatal("import fixture requires work contract")
			}
			fields, err := entityruntime.Initialize(contract, map[string]any{"left": "paired", "right": "paired", "token": "fixed"})
			if err != nil {
				t.Fatal(err)
			}
			pc := newEntityToolPipeline(t, persistence, source)
			owner := constructEntityToolFixture(t, ctx, persistence, pc, source, "work", "work/one", entityID, "queued", fields, time.Now().UTC())
			actor = entityToolFixtureActor(t, source, actor, runID, "work", "work/one")
			inbound := eventtest.PersistedChildForProducer(
				uuid.NewString(), events.EventType("work.ready"), eventtest.Producer(events.EventProducerNode, "creator"), "", []byte(`{}`), 0,
				runID, uuid.NewString(), events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "work/one"), time.Now().UTC(),
			)
			storetest.CommitSemanticEvent(t, ctx, persistence, inbound)
			ctx = tools.WithActor(bus.WithInboundEvent(ctx, inbound), actor)
			exec := tools.NewExecutorWithOptions(nil, tools.ExecutorOptions{EntityStore: persistence, EntityWriter: pc, WorkflowSource: source})
			identity := tools.EntityIdentity{RunID: runID, EntityID: entityID}
			read := func() map[string]any {
				t.Helper()
				row, found, err := persistence.LoadEntityState(ctx, identity)
				if err != nil || !found {
					t.Fatalf("load: found=%v err=%v", found, err)
				}
				return row
			}
			whole, err := exec.Execute(ctx, "read_work", map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			fields = whole.(map[string]any)["fields"].(map[string]any)
			if _, present := fields["label"]; present {
				t.Fatalf("read fabricated label: %#v", fields)
			}
			if testNumericValue(fields["seeded"]) != 7 {
				t.Fatalf("creation lost explicit initial: %#v", fields)
			}
			assertSparseToolContinuation(t, whole, fields)
			if _, err := exec.Execute(ctx, "read_work_label", map[string]any{}); err == nil {
				t.Fatal("unassigned typed field read returned a fabricated value")
			}
			for _, tc := range []struct {
				name  string
				value any
			}{
				{"update_work_profile_name", "no parent"},
				{"save_work_left", "unpaired"},
				{"save_work_token", "changed"},
				{"save_work_label", nil},
				{"save_work_profile", map[string]any{"note": "required name absent"}},
			} {
				before := read()
				if _, err := exec.Execute(ctx, tc.name, map[string]any{"value": tc.value}); err == nil {
					t.Fatalf("%s accepted %#v", tc.name, tc.value)
				}
				if after := read(); !reflect.DeepEqual(before, after) {
					t.Fatalf("rejected %s changed stored snapshot:\nbefore=%#v\nafter=%#v", tc.name, before, after)
				}
			}
			for _, tc := range []struct {
				name  string
				value any
			}{
				{"save_work_label", ""},
				{"save_work_left", "paired"},
				{"save_work_profile", map[string]any{"name": "first", "note": "previous"}},
				{"save_work_profile", map[string]any{"name": "first"}},
				{"update_work_profile_name", "second"},
			} {
				if _, err := exec.Execute(ctx, tc.name, map[string]any{"value": tc.value}); err != nil {
					for cause := errors.Unwrap(err); cause != nil; cause = errors.Unwrap(cause) {
						t.Logf("cause: %v", cause)
					}
					t.Fatalf("%s: %v", tc.name, err)
				}
			}
			row := read()
			fields = row["fields"].(map[string]any)
			if label, exists := fields["label"]; !exists || label != "" {
				t.Fatalf("explicit empty value lost: %#v", fields)
			}
			profile := fields["profile"].(map[string]any)
			if profile["name"] != "second" {
				t.Fatalf("nested mutation lost: %#v", fields)
			}
			if _, exists := profile["note"]; exists {
				t.Fatalf("nested optional value fabricated: %#v", fields)
			}
			whole, err = exec.Execute(ctx, "read_work", map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			assertSparseToolContinuation(t, whole, fields)

			// Exercise the canonical writer directly, without optimistic tool checks.
			before := read()
			otherSource := semanticview.Wrap(loadWave1EntityToolBundle(t, actor, "other", "work", "", "work:\n  label: text?\n"))
			for _, update := range []pipeline.EntityFieldMutation{
				{Source: source, Mutation: entityruntime.Mutation{Target: "entity.token", Value: "changed"}},
				{Source: source, Mutation: entityruntime.Mutation{Target: "entity.left", Value: "unpaired"}},
				{Source: source, Mutation: entityruntime.Mutation{Target: "entity.label", Value: nil}},
				{Mutation: entityruntime.Mutation{Target: "entity.label", Value: "unbound"}},
				{Source: otherSource, Mutation: entityruntime.Mutation{Target: "entity.label", Value: "wrong source"}},
			} {
				update.RunID, update.EntityID = runID, entityID
				update.Owner, update.FlowID = owner, "work"
				update.Writer = mutationlog.Writer{Type: "agent", ID: actor.ID, HandlerStep: "save_entity_field"}
				if _, err := pc.ApplyEntityFieldMutation(ctx, update); err == nil {
					t.Fatalf("canonical writer accepted invalid update: %#v", update)
				}
			}
			if !reflect.DeepEqual(before, read()) {
				t.Fatal("canonical writer rejection changed state or revision")
			}
			// Inspect actual persisted history on both stores. Only PostgreSQL's
			// existing internal debug reader exposes these records; neither store
			// has a public entity.history RPC.
			var mutations []operatorread.RunDebugMutation
			for _, row := range storetest.ObserveEntityMutationHistory(t, ctx, persistence, runID) {
				mutations = append(mutations, row.RunDebugMutation)
			}
			if backend == "postgres" {
				report, err := persistence.LoadRunDebugReport(ctx, runID, operatorread.RunDebugQueryOptions{MutationLimit: 100})
				if err != nil {
					t.Fatal(err)
				}
				if len(report.Mutations) != len(mutations) {
					t.Fatalf("debug history omitted records: got %d want %d", len(report.Mutations), len(mutations))
				}
				mutations = report.Mutations
			}
			labels, initials, withNote, withoutNote, nestedNames := 0, 0, 0, 0, 0
			for _, mutation := range mutations {
				if mutation.EntityID != entityID || mutation.Domain != "authored_field" {
					continue
				}
				switch mutation.Path {
				case "label":
					labels++
					if mutation.WriterType != "agent" || mutation.WriterID != actor.ID || mutation.HandlerStep != "save_entity_field" {
						t.Fatalf("label history lost attribution: %+v", mutation)
					}
					if string(mutation.NewValue) != `""` {
						t.Fatalf("history lost explicit empty label: %+v", mutation)
					}
				case "seeded":
					initials++
					if string(mutation.NewValue) != "7" {
						t.Fatalf("history changed explicit initial: %+v", mutation)
					}
				case "profile":
					if mutation.WriterType != "agent" || mutation.WriterID != actor.ID || mutation.HandlerStep != "save_entity_field" {
						t.Fatalf("profile history lost attribution: %+v", mutation)
					}
					var value, previous map[string]any
					if err := json.Unmarshal(mutation.NewValue, &value); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(mutation.OldValue, &previous); err != nil {
						t.Fatal(err)
					}
					// Canonical evidence describes the declared-root effect, not
					// the retired logger's synthetic dotted-key projection.
					if note, present := value["note"]; present {
						if note != "previous" || value["name"] != "first" || len(value) != 2 || len(previous) != 0 {
							t.Fatalf("history fabricated note: %+v", mutation)
						}
						withNote++
					} else if value["name"] == "first" {
						if len(value) != 1 || !reflect.DeepEqual(previous, map[string]any{"name": "first", "note": "previous"}) {
							t.Fatalf("whole replacement history changed old/new effect: %+v", mutation)
						}
						withoutNote++
					} else if value["name"] == "second" {
						if len(value) != 1 || !reflect.DeepEqual(previous, map[string]any{"name": "first"}) {
							t.Fatalf("nested update history changed old/new effect: %+v", mutation)
						}
						nestedNames++
					} else {
						t.Fatalf("unexpected profile effect: %+v", mutation)
					}
				}
			}
			if labels != 1 || initials != 1 || withNote != 1 || withoutNote != 1 || nestedNames != 1 {
				t.Fatalf("history presence counts label=%d initial=%d with_note=%d without_note=%d nested_name=%d", labels, initials, withNote, withoutNote, nestedNames)
			}
		})
	}
}

func assertSparseToolContinuation(t *testing.T, result any, wantFields map[string]any) {
	t.Helper()
	raw, err := json.Marshal([]map[string]any{{"name": "read_work", "ok": true, "result": result}})
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := agentframe.NewToolContinuation("agent-frame:v1:"+uuid.NewString(), raw)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := continuation.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := agentframe.DecodeToolContinuation(durable)
	if err != nil {
		t.Fatal(err)
	}
	var batch []struct {
		Result struct {
			Fields map[string]any `json:"fields"`
		} `json:"result"`
	}
	if err := json.Unmarshal(decoded.ToolResult(), &batch); err != nil || len(batch) != 1 {
		t.Fatalf("decode canonical tool result: %v", err)
	}
	// Compare serialized JSON because the persistence reader and execution
	// normalizer intentionally use different in-memory integer representations.
	want, _ := json.Marshal(wantFields)
	got, _ := json.Marshal(batch[0].Result.Fields)
	if string(got) != string(want) {
		t.Fatalf("tool continuation changed field presence: got=%s want=%s", got, want)
	}
}
