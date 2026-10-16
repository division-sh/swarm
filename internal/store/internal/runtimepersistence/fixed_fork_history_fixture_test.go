package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

// These are native fixed-history witnesses, not handler/boot eligibility proofs.
// Their sources and plans are admitted normally; only the five closed shapes
// may consume the original creating event's historical birth occurrence.
type fixedForkHistoryFixture struct {
	ctx          context.Context
	selected     any
	construction ConversationForkConstruction
	entityID     string
	runID        string
}

func prepareFixedForkHistoryFixture(t *testing.T, selected agentFixtureFlowStore, witness, runID, entityID string, event events.Event) fixedForkHistoryFixture {
	t.Helper()
	if witness == "route-timer-blocker" {
		// Row34 is component-prepared/native-committed, not a manager or
		// executable-attachment proof (reviewer-e 6202595494).
		ctx := correlation.WithRunID(testAuthorActivityContext(), runID)
		requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: event.CreatedAt().Add(-time.Minute)})
		command, err := flowactivationfixture.Command(ctx, pipeline.WorkflowInstance{
			InstanceID: "1", StorageRef: "flow-a/1", EntityID: entityID, EntityType: "default",
			WorkflowName: "flow-a", WorkflowVersion: "1", Mode: "static", CurrentState: "ready", StageDefined: true,
			Fields: map[string]any{}, CreatedAt: event.CreatedAt(), EnteredStageAt: event.CreatedAt(),
		}, pipeline.WorkflowLifecycleMutationPlan{}, event.CreatedAt())
		if err != nil {
			t.Fatal(err)
		}
		command.Plan.CreatingInput = pipeline.FlowConstructionInput{EventID: event.ID(), Input: "fork.state"}
		if err := command.Validate(); err != nil {
			t.Fatal(err)
		}
		return fixedForkHistoryFixture{ctx: ctx, selected: selected, runID: runID, entityID: entityID,
			construction: ConversationForkConstruction{Child: command.Plan, CreatingEvent: event}}
	}
	flow, instance, entityType, stage, keyField, key := "flow-a", "1", "", "", "name", ""
	fields := map[string]any{}
	switch witness {
	case "exact-facts":
		entityType, stage, key = "fork_entity", "ready", "Snapshot Entity"
		fields["name"] = key
	case "materializer-state":
		entityType, stage, key = "historical_entity", "queued", "Before Name"
		fields = map[string]any{"title": "before-title", "slug": "before-slug", "name": key,
			"gates": map[string]any{"review": "authored-review"}, "accumulator": map[string]any{"total": "authored-total"},
			"bookkeeping": map[string]any{"activation": "manual"}}
	case "unsupported-delivery":
		entityType, stage, keyField, key = "default", "ready", "fixture_key", "1"
		fields[keyField] = key
	case "source-metadata":
		flow, instance, entityType, stage, key = "state-flow", "at-T", "validation_case", "pending", "Fork Point Name"
		fields["name"] = key
	case "typed-collision":
		flow, instance, entityType, stage, keyField, key = "state-flow", "at-T", "source_case", "pending", "entity_type", "field_case"
		fields[keyField] = key
	default:
		t.Fatalf("unknown fixed fork witness %q", witness)
	}
	var names []string
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	var declarations strings.Builder
	fmt.Fprintf(&declarations, "%s:\n", entityType)
	for _, name := range names {
		if name == keyField {
			fmt.Fprintf(&declarations, "  %s: text\n", name)
			continue
		}
		raw, err := json.Marshal(fields[name])
		if err != nil {
			t.Fatal(err)
		}
		kind := "text"
		if _, object := fields[name].(map[string]any); object {
			kind = "json"
		}
		fmt.Fprintf(&declarations, "  %s: {type: %s, initial: %s, _unused_reason: Fixed historical fork witness.}\n", name, kind, raw)
	}
	input := string(event.Type())
	eventSchema := fmt.Sprintf("%s:\n  %s: text?\n", input, keyField)
	if witness == "exact-facts" {
		eventSchema += "  matrix: boolean?\n  items: json?\n"
	}
	bundle := loadLifecyclePersistenceFixtureForTest(t, map[string]string{
		"schema.yaml":           "name: fixed-fork-history\n",
		flow + "/schema.yaml":   fmt.Sprintf("name: %s\ninstance: %s\nstages:\n  %s: {}\n  done: {final: true}\npins:\n  inputs: [%s]\n", flow, keyField, stage, input),
		flow + "/entities.yaml": declarations.String(),
		flow + "/events.yaml":   eventSchema,
	})
	ctx := correlation.WithRunID(testAuthorActivityContextForBundle(bundle.SourceArtifact.BundleHash()), runID)
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
		Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: event.CreatedAt().Add(-time.Minute),
		Artifact: bundle.SourceArtifact, BundleHash: bundle.SourceArtifact.BundleHash(),
	})
	workflows := configureAgentFixtureFlowLifecycle(t, selected, &sqliteFlowActivationBus{}, bundle)
	source := semanticview.Wrap(bundle)
	planner := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
		BaseContext: ctx, SemanticSource: source, SourceArtifactFact: mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash()),
		WorkflowInstances: workflows, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	root := flowidentity.Stored(source, ".", runID, runID, runID, "")
	parent, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: root, OccurredAt: event.CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	child, err := flowidentity.KeyedChild(source, root, flow, instance)
	if err != nil {
		t.Fatal(err)
	}
	child.EntityID = entityID
	plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: source, Instance: child, ConstructorInput: input, ResolvedKey: key,
		TriggerEvent: event, OccurredAt: event.CreatedAt(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if name, ok := fields["name"].(string); ok {
		plan.Instance.Name = name
	}
	if slug, ok := fields["slug"].(string); ok {
		plan.Instance.Slug = slug
	}
	if witness == "materializer-state" {
		plan.Instance.Gates = map[string]bool{"review": true}
		if plan.Instance.Bookkeeping == nil {
			plan.Instance.Bookkeeping = map[string]any{}
		}
		plan.Instance.Bookkeeping["activation"] = "standing"
		plan.Instance.StateBuckets = map[string]any{"total": int64(7)}
	}
	if _, err := plan.PersistenceRecord(); err != nil {
		t.Fatal(err)
	}
	return fixedForkHistoryFixture{ctx: ctx, selected: selected, runID: runID, entityID: entityID,
		construction: ConversationForkConstruction{Parent: &parent, Child: plan, CreatingEvent: event}}
}

func commitFixedForkHistoryFixture(ctx context.Context, attempt *mutationprotocol.Attempt, f fixedForkHistoryFixture) (pipeline.CommittedFlowInstanceActivation, error) {
	var prepared runtimebus.PreparedPublishEvent
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var found bool
		var err error
		switch selected := f.selected.(type) {
		case *PostgresStore:
			prepared, found, err = selected.eventPostgresOwner.LoadPreparedPublishEventTx(ctx, tx, f.construction.CreatingEvent.ID())
		case *SQLiteRuntimeStore:
			prepared, found, err = selected.eventSQLiteOwner.LoadPreparedPublishEventTx(ctx, tx, f.construction.CreatingEvent.ID())
		}
		if err != nil || !found {
			return errors.Join(err, fmt.Errorf("fixed construction requires its persisted creating event"))
		}
		return nil
	}); err != nil {
		return pipeline.CommittedFlowInstanceActivation{}, err
	}
	switch selected := f.selected.(type) {
	case *PostgresStore:
		return selected.pipelinePostgresOwner.CommitFixedForkConstructionForTest(ctx, attempt, f.construction.Parent, f.construction.Child, prepared.Event)
	case *SQLiteRuntimeStore:
		return selected.pipelineSQLiteOwner.CommitFixedForkConstructionForTest(ctx, attempt, f.construction.Parent, f.construction.Child, prepared.Event)
	default:
		return pipeline.CommittedFlowInstanceActivation{}, fmt.Errorf("fixed fork fixture requires native selected store, got %T", f.selected)
	}
}

func persistFixedForkHistoryFixture(t *testing.T, f fixedForkHistoryFixture) {
	t.Helper()
	if err := runSelectedFixtureMutation(f.ctx, f.selected, "fixed fork creating occurrence", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		if err := commitSemanticEventFixtureWithRoutesStoryTx(ctx, f.selected.(eventCommitTxStore), attempt, f.construction.CreatingEvent, nil); err != nil {
			return err
		}
		_, err := commitFixedForkHistoryFixture(ctx, attempt, f)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func fixedForkBusinessEntity(t *testing.T, plan runfork.RunForkPlan, f fixedForkHistoryFixture) runfork.RunForkEntityState {
	t.Helper()
	if len(plan.Entities) != 2 {
		t.Fatalf("fixed native snapshot must retain one fieldless parent and one business child: %+v", plan.Entities)
	}
	var business runfork.RunForkEntityState
	parent, child := 0, 0
	for _, entity := range plan.Entities {
		metadata := entity.MaterializationMetadata
		if metadata == nil || metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance || len(metadata.InitialMaterialization) == 0 {
			t.Fatalf("native snapshot lost construction provenance: %+v", entity)
		}
		switch entity.EntityID {
		case f.runID:
			parent++
			if metadata.ParentInstance != "" || metadata.EntityType != "" || metadata.FlowInstance != f.runID || len(entity.Fields) != 0 || entity.CurrentState == nil || *entity.CurrentState != "pending" {
				t.Fatalf("fixed structural root changed its fieldless identity: %+v", entity)
			}
		case f.entityID:
			child++
			if metadata.ParentInstance != f.runID || metadata.InstanceKey != f.construction.Child.Instance.InstanceKey {
				t.Fatalf("fixed business child changed its admitted parent/key: %+v", entity)
			}
			business = entity
		default:
			t.Fatalf("fixed snapshot acquired an unrelated member: %+v", entity)
		}
	}
	if parent != 1 || child != 1 {
		t.Fatalf("fixed snapshot membership parent=%d child=%d", parent, child)
	}
	return business
}

func TestFixedForkHistoricalConstructionWitnessesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, witness := range []struct{ name, event string }{
			{"exact-facts", "matrix.event"}, {"materializer-state", "fork.before"},
			{"unsupported-delivery", "fork.pending"}, {"source-metadata", "fork.no_event_flow"},
			{"typed-collision", "fork.conflicting_entity_type"},
			{"route-timer-blocker", "fork.state"},
		} {
			t.Run(backend.name+"/"+witness.name, func(t *testing.T) {
				opened := backend.open(t)
				run, entity := uuid.NewString(), uuid.NewString()
				at := time.Unix(1700000500, 0).UTC()
				if witness.name == "route-timer-blocker" {
					at = time.Unix(1700000210, 0).UTC()
				}
				event := semanticEventRecordFixture(uuid.NewString(), run, events.EventType(witness.event), eventtest.Producer(events.EventProducerPlatform, "test"), []byte(`{}`), semanticEventRecordFixtureEnvelope(entity, ""), at)
				if witness.name == "exact-facts" {
					event = eventtest.ExistingRunRootIngress(event.ID(), events.EventType(witness.event), "revision-matrix", "", []byte(`{"matrix":true,"items":["matrix"]}`), 0, run, events.EventEnvelope{Scope: events.EventScopeGlobal}, at)
				}
				f := prepareFixedForkHistoryFixture(t, opened.store.(agentFixtureFlowStore), witness.name, run, entity, event)
				before := snapshotForkHistoricalExecutionTables(t, opened.db, backend.name == "postgres")
				rollback := errors.New("fixed construction rollback after real native commit body")
				write := func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
					if err := commitSemanticEventFixtureWithRoutesStoryTx(ctx, opened.store, attempt, event, nil); err != nil {
						return err
					}
					_, err := commitFixedForkHistoryFixture(ctx, attempt, f)
					return err
				}
				if err := runSelectedFixtureMutation(f.ctx, opened.store, "fixed construction rollback", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
					if err := write(ctx, attempt); err != nil {
						return err
					}
					return rollback
				}); !errors.Is(err, rollback) {
					t.Fatalf("native rollback did not reach its discriminator: %v", err)
				}
				if after := snapshotForkHistoricalExecutionTables(t, opened.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
					t.Fatal("fixed construction rollback leaked header, receipt, history or publication")
				}
				if err := runSelectedFixtureMutation(f.ctx, opened.store, "fixed construction positive", write); err != nil {
					t.Fatal(err)
				}
				history, err := ObserveEntityMutationHistoryForTest(f.ctx, opened.store, run)
				if err != nil || len(history) == 0 {
					t.Fatalf("fixed construction lost native births: %+v %v", history, err)
				}
				for _, mutation := range history {
					if !mutation.CreatedAt.Equal(at) {
						t.Fatalf("fixed construction birth moved off the creating occurrence: %+v", mutation)
					}
				}
				plans := []pipeline.FlowInstanceActivationPlan{f.construction.Child}
				if f.construction.Parent != nil {
					plans = append(plans, *f.construction.Parent)
				}
				for _, plan := range plans {
					record, err := plan.PersistenceRecord()
					if err != nil {
						t.Fatal(err)
					}
					stored, found, err := opened.store.(pipeline.WorkflowInstancePersistenceReader).LoadWorkflowInstance(f.ctx, record.Identity)
					if err != nil || !found || stored.EntityID != plan.Instance.EntityID || stored.CurrentState != plan.Instance.CurrentState || !stored.CreatedAt.Equal(at) {
						t.Fatalf("fixed construction lost exact parent/child header: %+v found=%v err=%v", stored, found, err)
					}
				}
				readiness, err := opened.store.(pipeline.DynamicFlowRuntimeReadinessPersistence).InspectDynamicFlowRuntimeReadinessForSource(f.ctx, mustStoreTestSourceArtifactFact(f.construction.Child.Readiness.BundleHash))
				if err != nil || len(readiness.SourceTransitionRequired) != 0 || len(readiness.CurrentCompleted)+len(readiness.CurrentPending) != len(plans) {
					t.Fatalf("fixed construction lost exact native readiness membership: %+v %v", readiness, err)
				}
				seen := map[string]string{}
				for _, member := range append(readiness.CurrentCompleted, readiness.CurrentPending...) {
					if member.Plan.RunID != run || member.Plan.BundleHash != f.construction.Child.Readiness.BundleHash || seen[member.InstancePath] != "" {
						t.Fatalf("fixed readiness changed source/run or duplicated a member: %+v", member)
					}
					seen[member.InstancePath] = member.Plan.Identity.EntityID
				}
				wantMembers := map[string]string{f.construction.Child.Identity.InstancePath: entity}
				if f.construction.Parent != nil {
					wantMembers[run] = run
				}
				if !reflect.DeepEqual(seen, wantMembers) {
					t.Fatalf("fixed readiness identities = %v", seen)
				}
				state, err := f.construction.Child.PersistenceRecord()
				if err != nil {
					t.Fatal(err)
				}
				later := state.State
				later.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
				later.ExpectedState, later.ExpectedRevision = later.CurrentState, 1
				later.Fields = []byte(`{"ordinary_later":true}`)
				later.UpdatedAt = at.Add(time.Hour)
				start := time.Now().UTC().Truncate(time.Microsecond)
				if _, err := opened.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: later}); err != nil {
					t.Fatal(err)
				}
				end := time.Now().UTC()
				if backend.name == "sqlite" {
					start, end = later.UpdatedAt, later.UpdatedAt
				}
				ordinary, err := ObserveEntityMutationHistoryForTest(f.ctx, opened.store, run)
				if err != nil {
					t.Fatal(err)
				}
				writes := 0
				for _, mutation := range ordinary {
					if mutation.EntityID == entity && mutation.Domain == "authored_field" && mutation.Path == "ordinary_later" {
						writes++
						if string(mutation.NewValue) != "true" || mutation.CreatedAt.Before(start) || mutation.CreatedAt.After(end) {
							t.Fatalf("ordinary subsequent write inherited the fixed birth clock: %+v, want [%v,%v]", mutation, start, end)
						}
					}
				}
				if writes != 1 {
					t.Fatalf("ordinary subsequent write count=%d, want1", writes)
				}
			})
		}
	}
}

func TestFixedForkHistoricalOccurrenceRefusalsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, witness := range []struct{ name, event string }{
			{"exact-facts", "matrix.event"}, {"materializer-state", "fork.before"},
			{"unsupported-delivery", "fork.pending"}, {"source-metadata", "fork.no_event_flow"},
			{"typed-collision", "fork.conflicting_entity_type"},
			{"route-timer-blocker", "fork.state"},
		} {
			t.Run(backend.name+"/"+witness.name, func(t *testing.T) {
				opened := backend.open(t)
				run, entity := uuid.NewString(), uuid.NewString()
				at := time.Unix(1700000500, 0).UTC()
				if witness.name == "route-timer-blocker" {
					at = time.Unix(1700000210, 0).UTC()
				}
				event := semanticEventRecordFixture(uuid.NewString(), run, events.EventType(witness.event), eventtest.Producer(events.EventProducerPlatform, "test"), []byte(`{}`), semanticEventRecordFixtureEnvelope(entity, ""), at)
				if witness.name == "exact-facts" {
					event = eventtest.ExistingRunRootIngress(event.ID(), events.EventType(witness.event), "revision-matrix", "", []byte(`{"matrix":true,"items":["matrix"]}`), 0, run, events.EventEnvelope{Scope: events.EventScopeGlobal}, at)
				}
				f := prepareFixedForkHistoryFixture(t, opened.store.(agentFixtureFlowStore), witness.name, run, entity, event)
				record, err := f.construction.Child.PersistenceRecord()
				if err != nil {
					t.Fatal(err)
				}
				before := snapshotForkHistoricalExecutionTables(t, opened.db, backend.name == "postgres")
				var ended *mutationprotocol.Attempt
				var retained *privatemutationlog.ConstructionOccurrence
				if err := runSelectedFixtureMutation(f.ctx, opened.store, "capture fixed occurrence", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
					ended = attempt
					var err error
					retained, err = privatemutationlog.FixedForkConstructionOccurrenceForTest(ctx, attempt, f.construction.Parent, f.construction.Child, event)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if err := retained.ValidateBirthOccurrence(f.ctx, ended, record.State); err == nil || err.Error() != "mutation attempt is not active" {
					t.Fatalf("completed fixed attempt retained authority: %v", err)
				}
				for _, name := range []string{"unknown_path", "mixed_state", "mixed_type", "wrong_payload", "wrong_parent", "extra_child", "added_key", "initial_fields", "internal_gates", "internal_accumulator", "internal_bookkeeping", "wrong_event", "event_time", "wrong_run", "wrong_source", "wrong_entity", "wrong_time", "wrong_operation", "wrong_attempt", "changed_after_mint", "wrong_birth_value"} {
					t.Run(name, func(t *testing.T) {
						if err := runSelectedFixtureMutation(f.ctx, opened.store, "fixed occurrence refusal", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
							child, parent := f.construction.Child, f.construction.Parent
							if parent != nil {
								copy := *parent
								parent = &copy
							}
							child.Instance.Fields = maps.Clone(child.Instance.Fields)
							creatingEvent := event
							mintRefusal := true
							switch name {
							case "unknown_path":
								child.Identity.InstancePath = "unknown/1"
							case "mixed_state":
								child.Instance.CurrentState = "queued"
								if f.construction.Child.Instance.CurrentState == "queued" {
									child.Instance.CurrentState = "ready"
								}
							case "mixed_type":
								child.Instance.EntityType = "source_case"
								if f.construction.Child.Instance.EntityType == "source_case" {
									child.Instance.EntityType = "fork_entity"
								}
							case "wrong_payload":
								child.Instance.Fields["unlisted"] = true
							case "wrong_parent":
								if parent == nil {
									copy := child
									parent = &copy
								}
								parent.Identity.EntityID = uuid.NewString()
							case "added_key":
								child.Instance.InstanceKey = "unlisted"
							case "initial_fields":
								child.Instance.InitialFieldValues = map[string]any{"unlisted": true}
							case "internal_gates":
								child.Instance.Gates = map[string]bool{"unlisted": true}
							case "internal_accumulator":
								child.Instance.StateBuckets = map[string]any{"unlisted": true}
							case "internal_bookkeeping":
								child.Instance.Bookkeeping = map[string]any{"unlisted": true}
							case "extra_child":
								child.Children = []pipeline.FlowInstanceActivationPlan{child}
							case "wrong_event":
								creatingEvent = events.Event{}
							case "event_time":
								child.OccurredAt = at.Add(time.Second)
							default:
								mintRefusal = false
							}
							occurrence, err := privatemutationlog.FixedForkConstructionOccurrenceForTest(ctx, attempt, parent, child, creatingEvent)
							if mintRefusal {
								if err == nil || occurrence != nil {
									t.Fatalf("%s admitted an unenumerated construction: %v", name, err)
								}
								return nil
							}
							if err != nil {
								return err
							}
							state, writeCtx := record.State, ctx
							want := ""
							switch name {
							case "wrong_run":
								writeCtx, want = correlation.WithRunID(ctx, uuid.NewString()), "belongs to another attempt/run/source"
							case "wrong_source":
								writeCtx, want = correlation.WithSourceArtifactFact(ctx, mustStoreTestSourceArtifactFact("bundle-v2:sha256:"+strings.Repeat("a", 64))), "belongs to another attempt/run/source"
							case "wrong_entity":
								state.EntityID, want = uuid.NewString(), "does not match its admitted entity/birth"
							case "wrong_time":
								state.UpdatedAt, want = at.Add(time.Second), "requires its exact birth operation/time"
							case "wrong_operation":
								state.Transition, want = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion, "requires its exact birth operation/time"
							case "wrong_attempt":
								occurrence, want = retained, "belongs to another attempt/run/source"
							case "changed_after_mint":
								child.Instance.Fields["unlisted"] = true
								if err := occurrence.ValidateBirthOccurrence(ctx, attempt, state); err != nil {
									t.Fatalf("caller mutation changed copied evidence: %v", err)
								}
								changed, err := child.PersistenceRecord()
								if err != nil {
									return err
								}
								state, want = changed.State, "does not match its admitted entity/birth"
							case "wrong_birth_value":
								want = "refuses a non-birth or repeated mutation"
							}
							// Nil owner forces every refusal to precede native writes.
							err = privatemutationlog.InsertConstructionEntityStateDiff(writeCtx, attempt, nil, state, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{Fields: map[string]any{"unlisted": true}}, runtimemutationlog.Writer{Type: "platform", ID: "workflow_engine", HandlerStep: "create"}, occurrence, backend.name == "postgres")
							if err == nil || !strings.Contains(err.Error(), want) {
								t.Fatalf("%s refusal = %v, want %s", name, err, want)
							}
							return nil
						}); err != nil {
							t.Fatal(err)
						}
						if after := snapshotForkHistoricalExecutionTables(t, opened.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
							t.Fatalf("%s refusal changed native source, receipt, history or publication", name)
						}
					})
				}
				t.Run("repeated_birth", func(t *testing.T) {
					rollback := errors.New("roll back real birth after repeated-use refusal")
					err := runSelectedFixtureMutation(f.ctx, opened.store, "fixed occurrence repeated birth", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
						occurrence, err := privatemutationlog.FixedForkConstructionOccurrenceForTest(ctx, attempt, f.construction.Parent, f.construction.Child, event)
						if err != nil {
							return err
						}
						var previous, next runtimemutationlog.EntityStateProjection
						if err := json.Unmarshal(record.State.InitialFields, &previous.Fields); err != nil {
							return err
						}
						for _, bucket := range []struct {
							raw    []byte
							target *map[string]any
						}{{record.State.Fields, &next.Fields}, {record.State.Bookkeeping, &next.Bookkeeping}, {record.State.Gates, &next.Gates}, {record.State.Accumulator, &next.Accumulator}} {
							if err := json.Unmarshal(bucket.raw, bucket.target); err != nil {
								return err
							}
						}
						next.CurrentState = runtimemutationlog.PresentLifecycleState(record.State.CurrentState)
						var owner privatemutationlog.ActiveRunSourceOwner
						switch selected := opened.store.(type) {
						case *PostgresStore:
							owner = selected.pipelinePostgresOwner
						case *SQLiteRuntimeStore:
							owner = selected.pipelineSQLiteOwner
						}
						if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
							switch selected := opened.store.(type) {
							case *PostgresStore:
								_, err := selected.pipelinePostgresOwner.RequireActiveSourceTx(ctx, tx, run)
								return err
							case *SQLiteRuntimeStore:
								_, err := selected.pipelineSQLiteOwner.RequireActiveSourceTx(ctx, tx, run)
								return err
							}
							return fmt.Errorf("unexpected fixed witness backend %T", opened.store)
						}); err != nil {
							return err
						}
						writer := runtimemutationlog.Writer{Type: "platform", ID: "workflow_engine", HandlerStep: "create"}
						if err := privatemutationlog.InsertConstructionEntityStateDiff(ctx, attempt, owner, record.State, previous, next, writer, occurrence, backend.name == "postgres"); err != nil {
							return err
						}
						if err := privatemutationlog.InsertConstructionEntityStateDiff(ctx, attempt, owner, record.State, previous, next, writer, occurrence, backend.name == "postgres"); err == nil || !strings.Contains(err.Error(), "refuses a non-birth or repeated mutation") {
							t.Fatalf("repeated birth = %v", err)
						}
						return rollback
					})
					if !errors.Is(err, rollback) {
						t.Fatalf("repeated birth did not reach its exact refusal: %v", err)
					}
					if after := snapshotForkHistoricalExecutionTables(t, opened.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
						t.Fatal("repeated-birth rollback leaked native evidence")
					}
				})
			})
		}
	}
}
