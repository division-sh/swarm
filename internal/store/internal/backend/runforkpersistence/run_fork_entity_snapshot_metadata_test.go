package runforkpersistence

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestRunForkSnapshotOwnershipMetadataAuthority(t *testing.T) {
	const entityID = "entity-1"
	metadata := runForkRevisionEntityMetadata{EntityID: entityID, FlowInstance: "owner/one", EntityType: "case", Slug: "case-one", Name: "Case One"}
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
			if !ok || got != want {
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
		EntityID: entityID, FlowInstance: "review/one", EntityType: "  ",
	}}}

	_, message, ok := loadRunForkMaterializedEntitySnapshotMetadata(snapshot, runfork.RunForkEntityState{EntityID: entityID})
	if ok {
		t.Fatal("blank historical entity contract was admitted")
	}
	if !strings.Contains(message, "cannot prove source-at-revision flow_instance/entity_type metadata") {
		t.Fatalf("blank historical entity contract error = %q", message)
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
