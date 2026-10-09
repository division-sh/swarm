package runforkpersistence

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func TestRunForkSnapshotOwnershipMetadataAuthority(t *testing.T) {
	const entityID = "entity-1"
	metadata := runForkRevisionEntityMetadata{EntityID: entityID, FlowInstance: "owner/one", EntityType: "case", Slug: "case-one", Name: "Case One", ConstructionKind: "imported_state"}
	for _, flows := range [][]string{{"context/one", "context/two"}, {"context/two", "context/one"}, {"owner/one", "owner/one"}, {"", ""}} {
		t.Run("event_context_not_owner/"+strings.Join(flows, ","), func(t *testing.T) {
			snapshot := &runForkRevisionSnapshot{EntityMetadata: []runForkRevisionEntityMetadata{metadata}}
			for _, flow := range flows {
				snapshot.Events = append(snapshot.Events, runForkRevisionEvent{EntityID: entityID, FlowInstance: flow})
			}
			got, message, ok := loadRunForkMaterializedEntitySnapshotMetadata(snapshot, runfork.RunForkEntityState{EntityID: entityID})
			want := runfork.RunForkMaterializedEntitySnapshotMetadata{
				Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState,
				FlowInstance: metadata.FlowInstance, EntityType: metadata.EntityType, Slug: metadata.Slug, Name: metadata.Name,
			}
			if !ok || !reflect.DeepEqual(got, want) {
				t.Fatalf("metadata = %#v, admitted=%t message=%q; want %#v", got, ok, message, want)
			}
		})
	}
	for _, name := range []string{"missing", "blank_flow", "blank_type", "duplicate_identical", "duplicate_flow", "duplicate_type"} {
		t.Run("missing_blank_payload/"+name, func(t *testing.T) {
			snapshot := &runForkRevisionSnapshot{
				EntityMetadata: []runForkRevisionEntityMetadata{metadata},
				Events:         []runForkRevisionEvent{{EntityID: entityID, FlowInstance: "event/repair"}},
			}
			switch name {
			case "missing":
				snapshot.EntityMetadata = nil
			case "blank_flow":
				snapshot.EntityMetadata[0].FlowInstance = " "
			case "blank_type":
				snapshot.EntityMetadata[0].EntityType = " "
			default:
				second := metadata
				if name == "duplicate_flow" {
					second.FlowInstance = "other/one"
				}
				if name == "duplicate_type" {
					second.EntityType = "other"
				}
				snapshot.EntityMetadata = append(snapshot.EntityMetadata, second)
			}
			entity := runfork.RunForkEntityState{EntityID: entityID, Fields: map[string]any{"entity_type": "payload-type", "flow_instance": "payload/repair"}}
			if got, _, ok := loadRunForkMaterializedEntitySnapshotMetadata(snapshot, entity); ok {
				t.Fatalf("invalid owner metadata admitted: %#v", got)
			}
		})
	}
}

func TestRunForkConstructionOrderConsumesRecordedParents(t *testing.T) {
	root := runfork.RunForkEntityState{EntityID: "actual-root", MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
		Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance, FlowInstance: "actual-root-path",
	}}
	child := runfork.RunForkEntityState{EntityID: "actual-child", MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
		Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance, FlowInstance: "unrelated-path-shape", ParentInstance: root.MaterializationMetadata.FlowInstance,
	}}
	input := []runfork.RunForkEntityState{child, root}
	before := append([]runfork.RunForkEntityState(nil), input...)
	ordered, err := runForkEntitiesInConstructionOrder(input)
	if err != nil || len(ordered) != 2 || ordered[0].EntityID != root.EntityID || ordered[1].EntityID != child.EntityID || !reflect.DeepEqual(input, before) {
		t.Fatalf("historical ordering lost actual parent or mutated input: %+v %v", ordered, err)
	}
	for _, variant := range []string{"absent_parent", "cyclic_parent", "duplicate_path", "missing_metadata"} {
		t.Run(variant, func(t *testing.T) {
			changedRoot, changedChild := *root.MaterializationMetadata, *child.MaterializationMetadata
			changed := []runfork.RunForkEntityState{{EntityID: child.EntityID, MaterializationMetadata: &changedChild}, {EntityID: root.EntityID, MaterializationMetadata: &changedRoot}}
			switch variant {
			case "absent_parent":
				changedChild.ParentInstance = "absent"
			case "cyclic_parent":
				changedRoot.ParentInstance = changedChild.FlowInstance
			case "duplicate_path":
				changedRoot.FlowInstance = changedChild.FlowInstance
			case "missing_metadata":
				changed[0].MaterializationMetadata = nil
			}
			if ordered, err := runForkEntitiesInConstructionOrder(changed); err == nil || ordered != nil {
				t.Fatalf("accepted unproven historical ancestry %s: %+v %v", variant, ordered, err)
			}
		})
	}
}

func TestRunForkSnapshotMetadataRejectsDuplicateOwnersBeforeAttachment(t *testing.T) {
	for _, name := range []string{"duplicate_entities", "duplicate_metadata", "unreferenced_duplicate_metadata"} {
		t.Run(name, func(t *testing.T) {
			entity := runfork.RunForkEntityState{EntityID: "entity-1"}
			entities := []runfork.RunForkEntityState{entity}
			metadata := runForkRevisionEntityMetadata{EntityID: entity.EntityID, FlowInstance: "owner/one", EntityType: "case"}
			snapshot := &runForkRevisionSnapshot{EntityMetadata: []runForkRevisionEntityMetadata{metadata}}
			if name == "duplicate_entities" {
				entities = append(entities, entity)
			} else {
				if name == "unreferenced_duplicate_metadata" {
					metadata.EntityID = "unreferenced"
					snapshot.EntityMetadata = append(snapshot.EntityMetadata, metadata)
				}
				snapshot.EntityMetadata = append(snapshot.EntityMetadata, metadata)
			}
			got, _, err := attachRunForkMaterializedEntitySnapshotMetadata(snapshot, entities)
			blocker, fact, blocked := runForkReplayResumeBlockerFromError(err)
			if !blocked || blocker.Code != runfork.RunForkBlockerEntitySnapshotMetadataUnproven || fact != runfork.RunForkReplayResumeFactEntityStateSnapshot || got != nil {
				t.Fatalf("duplicate admission = %#v, %v; want named refusal without partial projection", got, err)
			}
			if !reflect.DeepEqual(entities[0], entity) {
				t.Fatal("metadata admission mutated caller entities")
			}
		})
	}
}

func TestRunForkMaterializerMetadataRejectsInvalidOwnerCollection(t *testing.T) {
	for _, name := range []string{"duplicate_identical", "duplicate_conflicting", "blank_entity", "missing_metadata", "wrong_owner", "obsolete_source", "blank_source", "blank_flow", "blank_type"} {
		t.Run(name, func(t *testing.T) {
			meta := runfork.RunForkMaterializedEntitySnapshotMetadata{
				Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState,
				FlowInstance: "owner/one", EntityType: "case",
			}
			entity := runfork.RunForkEntityState{EntityID: "entity-1", MaterializationMetadata: &meta}
			plan := runfork.RunForkPlan{Entities: []runfork.RunForkEntityState{entity}}
			switch name {
			case "duplicate_identical", "duplicate_conflicting":
				other := meta
				if name == "duplicate_conflicting" {
					other.FlowInstance = "other/one"
				}
				plan.Entities = append(plan.Entities, runfork.RunForkEntityState{EntityID: entity.EntityID, MaterializationMetadata: &other})
			case "blank_entity":
				plan.Entities[0].EntityID = " "
			case "missing_metadata":
				plan.Entities[0].MaterializationMetadata = nil
			case "wrong_owner":
				meta.Owner = "other"
			case "obsolete_source":
				meta.Source = "source_event"
			case "blank_source":
				meta.Source = ""
			case "blank_flow":
				meta.FlowInstance = " "
			case "blank_type":
				meta.EntityType = " "
			}
			got, err := loadRunForkEntityMetadata(plan)
			blocker, fact, blocked := runForkReplayResumeBlockerFromError(err)
			if !blocked || blocker.Code != runfork.RunForkBlockerEntitySnapshotMetadataUnproven || fact != runfork.RunForkReplayResumeFactEntityStateSnapshot || got != nil {
				t.Fatalf("invalid metadata collection accepted: %#v, %v", got, err)
			}
		})
	}
}

func TestRunForkSnapshotMetadataDoesNotRetainCallerSuppliedAuthority(t *testing.T) {
	entity := runfork.RunForkEntityState{EntityID: "entity-1", MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
		Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState,
		FlowInstance: "caller/one", EntityType: "caller-type",
	}}
	got, admission, err := attachRunForkMaterializedEntitySnapshotMetadata(&runForkRevisionSnapshot{}, []runfork.RunForkEntityState{entity})
	if err != nil || len(got) != 1 || got[0].MaterializationMetadata != nil || len(admission.Blockers) != 1 || admission.Blockers[0].Code != runfork.RunForkBlockerEntitySnapshotMetadataUnproven {
		t.Fatalf("missing fixed metadata retained caller authority: %#v %#v %v", got, admission, err)
	}
	if entity.MaterializationMetadata.FlowInstance != "caller/one" {
		t.Fatal("caller metadata mutated")
	}
}

func TestRunForkSnapshotMetadataRejectsBlankHistoricalEntityContract(t *testing.T) {
	entityID := "entity-1"
	snapshot := &runForkRevisionSnapshot{EntityMetadata: []runForkRevisionEntityMetadata{{
		EntityID: entityID, FlowInstance: "review/one", EntityType: "  ", ConstructionKind: "imported_state",
	}}}

	_, message, ok := loadRunForkMaterializedEntitySnapshotMetadata(snapshot, runfork.RunForkEntityState{EntityID: entityID})
	if ok {
		t.Fatal("blank historical entity contract was admitted")
	}
	if !strings.Contains(message, "imported state has no entity type") {
		t.Fatalf("blank historical entity contract error = %q", message)
	}
}

func TestRunForkSnapshotMetadataKeepsConstructedFieldlessHeader(t *testing.T) {
	for _, tc := range []struct {
		name, member string
		accepted     bool
	}{
		{"null_fieldless", `,"entity_type":null`, true},
		{"declared_type", `,"entity_type":"case"`, true},
		{"missing_type", "", false},
		{"blank_type", `,"entity_type":""`, false},
		{"whitespace_type", `,"entity_type":" "`, false},
	} {
		t.Run("persisted_type/"+tc.name, func(t *testing.T) {
			const runID = "10000000-0000-0000-0000-000000000001"
			const entityID = "20000000-0000-0000-0000-000000000001"
			snapshot := &runForkRevisionSnapshot{RunID: runID, Revision: 1}
			stamp := runForkHistoricalFactContext{RunID: runID, Family: runforkrevision.FamilyEntityMetadata, Key: entityID, FirstRevision: 1, Revision: 1}
			raw := []byte(`{"entity_id":"` + entityID + `","construction_kind":"constructed"` + tc.member + `}`)
			before := *snapshot
			err := appendRunForkHistoricalFact(snapshot, stamp, raw)
			if (err == nil) != tc.accepted {
				t.Fatalf("persisted field type admission: accepted=%t err=%v", tc.accepted, err)
			}
			if !tc.accepted && !reflect.DeepEqual(before, *snapshot) {
				t.Fatal("rejected field type changed the snapshot")
			}
		})
	}
	for _, stageDefined := range []bool{false, true} {
		for _, change := range []string{"exact", "missing_config", "missing_clock", "missing_entry_clock", "missing_update_clock", "missing_status", "contradictory_stage", "terminal_without_clock", "unknown_kind"} {
			t.Run(fmt.Sprintf("staged_%t/%s", stageDefined, change), func(t *testing.T) {
				at := time.Now().UTC()
				fact := runForkRevisionEntityMetadata{EntityID: "flow-1", FlowInstance: "review", FlowConfig: []byte(`{}`), ConstructionKind: "constructed", StageDefined: stageDefined,
					CreatedAt: at, UpdatedAt: at, EnteredStateAt: at, CurrentState: "pending", Status: "active", FlowTemplate: "review", Mode: "static"}
				switch change {
				case "missing_config":
					fact.FlowConfig = nil
				case "missing_clock":
					fact.CreatedAt = time.Time{}
				case "missing_entry_clock":
					fact.EnteredStateAt = time.Time{}
				case "missing_update_clock":
					fact.UpdatedAt = time.Time{}
				case "missing_status":
					fact.Status = ""
				case "contradictory_stage":
					fact.CurrentState = "foreign"
				case "terminal_without_clock":
					fact.Status = "terminated"
				case "unknown_kind":
					fact.ConstructionKind = ""
				}
				got, reason, accepted := loadRunForkMaterializedEntitySnapshotMetadata(&runForkRevisionSnapshot{EntityMetadata: []runForkRevisionEntityMetadata{fact}}, runfork.RunForkEntityState{EntityID: fact.EntityID, CurrentState: "pending"})
				if accepted != (change == "exact") {
					t.Fatalf("constructed fieldless metadata: %+v accepted=%t reason=%s", got, accepted, reason)
				}
				if accepted && (got.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance || got.EntityType != "" || got.StageDefined != stageDefined) {
					t.Fatalf("historical header facts changed: %+v", got)
				}
			})
		}
	}
}

func TestRunForkEntityIdentityProjectsCanonicalRootToForkRun(t *testing.T) {
	const sourceRunID = "11111111-1111-4111-8111-111111111111"
	const forkRunID = "22222222-2222-4222-8222-222222222222"

	identity, err := projectRunForkEntityIdentity(sourceRunID, forkRunID, sourceRunID, sourceRunID)
	if err != nil {
		t.Fatal(err)
	}
	if identity.EntityID != forkRunID || identity.FlowInstance != forkRunID {
		t.Fatalf("projected root identity = %#v, want fork run identity", identity)
	}

	nested, err := projectRunForkEntityIdentity(sourceRunID, forkRunID, "33333333-3333-4333-8333-333333333333", "review/one")
	if err != nil {
		t.Fatal(err)
	}
	if nested.EntityID != "33333333-3333-4333-8333-333333333333" || nested.FlowInstance != "review/one" {
		t.Fatalf("projected nested identity = %#v, want unchanged", nested)
	}
}

func TestRunForkEntityIdentityRejectsContradictoryRootFacts(t *testing.T) {
	const sourceRunID = "11111111-1111-4111-8111-111111111111"
	const forkRunID = "22222222-2222-4222-8222-222222222222"

	for name, facts := range map[string][2]string{
		"root entity with non-root route": {sourceRunID, "review/one"},
		"non-root entity with root route": {"33333333-3333-4333-8333-333333333333", sourceRunID},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := projectRunForkEntityIdentity(sourceRunID, forkRunID, facts[0], facts[1]); err == nil {
				t.Fatal("contradictory root identity was admitted")
			}
		})
	}
}

func TestRunForkSelectedContractSourceEventProjectsRootWithoutWorkflowState(t *testing.T) {
	const sourceRunID = "11111111-1111-4111-8111-111111111111"
	const forkRunID = "22222222-2222-4222-8222-222222222222"
	source, err := events.NewRootRoutingSource(sourceRunID)
	if err != nil {
		t.Fatal(err)
	}

	projected, err := runfork.ProjectSelectedContractSourceEvent(sourceRunID, forkRunID, runfork.RunForkSelectedContractSourceEvent{
		SourceEventID: "33333333-3333-4333-8333-333333333333", RoutingSource: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	if projected.RoutingSource.Route().EntityID != forkRunID {
		t.Fatalf("projected source event = %#v, want fork-local root identity", projected)
	}
}
